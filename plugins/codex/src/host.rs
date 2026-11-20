use crate::{
    events::{self, EventConfig, EventHandle, TurnFacts},
    mcp::{
        McpReadback, cloud_hostname_from_profile, compute_disables, compute_owner_disables,
        compute_plugin_disables, merge_status_page, parse_status_page, policy_from_profile,
        require_minimal_own_server, synthetic_apps_disable, synthetic_swarm_owner_disables,
    },
    native::{
        NativeClient, NativeLaunch, NativeProcess,
        types::{
            NativeClientInfo, NativeCollaborationMode, NativeCollaborationModeKind,
            NativeCollaborationSettings, NativeInitializeCapabilities, NativeInitializeParams,
            NativeInitializeResponse, NativeModelEntry, NativeSandboxMode, NativeTextUserInput,
            NativeThreadConfigOverrides, NativeThreadResumeParams, NativeThreadStartParams,
            NativeTurnInterruptParams, NativeTurnStartParams, NativeTurnStartResponse,
            NativeTurnSteerParams, NativeTurnSteerResponse, NativeUserInput, ThreadReadback,
            ThreadStartReadback,
        },
    },
    observe::{self, ObserverSession},
    route::CodexRoute,
};
use eliot_core::{
    ContextOutcome, Error, InjectArgs, InjectPoint, McpPolicy, NativeStoreBinding, Result,
    SecretPolicy,
};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, DiscoverParams,
    InitializeParams, MAX_EVENT_BYTES, MAX_TEXT_BYTES, ObserverAttachParams, ObserverDetachParams,
    ObserverSnapshotParams, Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::{ProfileStoreParams, stream_sources},
    session::{
        attach_reply_diagnostics, begin_stop, decode, ensure_dedicated_reader,
        ensure_reader_harness, initialize_fields_valid, is_passed_env, is_secret_name,
        native_no_write, not_initialized, optional_profile_text, parse_resume_subject, paths_equal,
        register_native_process, rejected_control, request_reply_ids,
        target_environment as shared_target_environment, unknown_control, unknown_snapshot,
        validate_binding, validate_identity, validate_source_plan, with_claimed_context,
        with_context_points,
    },
    skill,
    start_classification::SettledStartFailure,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior, interval},
};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const LIVE_POLL_INTERVAL: Duration = Duration::from_secs(1);
const LIVE_POLL_DEADLINE: Duration = Duration::from_secs(2);
const TURN_PAGE_SIZE: u32 = 1000;
const MAX_TURN_PAGES: usize = 16;
const ITEM_PAGE_SIZE: u32 = 1000;
const MAX_ITEM_PAGES: usize = 16;

#[derive(Default)]
struct CodexPlugin {
    initialized: Option<Initialized>,
    session: Option<AgentSession>,
    failed_start: Option<FailedStart>,
    failed_attach: Option<FailedObserverAttach>,
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
    outcome: SettledStartFailure,
}

struct FailedObserverAttach {
    params: ObserverAttachParams,
    outcome: Value,
}

impl FailedObserverAttach {
    fn new(params: &ObserverAttachParams, outcome: Value) -> Self {
        Self {
            params: params.clone(),
            outcome,
        }
    }

    fn replay(&self, params: &ObserverAttachParams) -> Result<Value> {
        let exact = serde_json::to_value(&self.params)
            .and_then(|retained| serde_json::to_value(params).map(|params| retained == params))
            .map_err(|_| {
                Error::new(
                    "internal",
                    "Codex observer attach binding could not be compared",
                )
            })?;
        if exact {
            return Ok(self.outcome.clone());
        }
        Err(Error::new(
            "id_conflict",
            "Codex observer attach retry changed its retained binding",
        ))
    }
}

fn is_proven_observer_start_failure(error: &Error) -> bool {
    error.code == "native_start_failed"
}

/// A scope refusal is raised before any pinned reader spawns or thread is
/// read, after only the read-only store probe ran and was cleaned up, so it
/// proves the refused attach had no native effect.
fn is_proven_scope_refusal(error: &Error) -> bool {
    error.code == "scope_mismatch"
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
    /// Native-selected model id (requested or the `isDefault` native
    /// default): what thread and turn wire fields actually carry. The
    /// requested value (present or absent) is reported once in the start
    /// result; the effective value is this selection plus native readback.
    selected_model: String,
    /// Pinned reasoning effort, or `None` when the profile omitted it and
    /// the native/provider default is preserved by omitting the wire field.
    selected_effort: Option<String>,
    /// Actual native readback effort from thread start/resume. Used only to
    /// preserve the native selection through the full collaboration-settings
    /// replacement when the profile effort is omitted; `requested` stays
    /// absent and the normal turn `effort` field stays omitted.
    native_effort: Option<String>,
    collaboration_mode: Option<NativeCollaborationModeKind>,
    secrets: Vec<String>,
    native: NativeProcess,
    native_client: NativeClient,
    events: EventHandle,
    current_turn: Option<(String, String)>,
    stop_command_id: Option<String>,
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

/// Validated Codex start plan: every frozen-profile, workspace and launch
/// input the native-effect phases need, checked before any native I/O.
/// A refusal here is proven-no-effect: no child was spawned.
struct ValidatedStartPlan {
    resume_subject: Option<eliot_plugin::session::ResumeSubject>,
    route: CodexRoute,
    frozen_store: NativeStoreBinding,
    requested_model: Option<String>,
    daemon_model: Option<String>,
    requested_effort: Option<String>,
    collaboration_mode: Option<NativeCollaborationModeKind>,
    permissions: String,
    approval_policy: String,
    sandbox_mode: NativeSandboxMode,
    mcp_policy: McpPolicy,
    mcp_allow: Vec<String>,
    mcp_cloud_hostname: Option<String>,
    workspace: PathBuf,
    program: OsString,
    environment: Vec<(OsString, OsString)>,
    secrets: Vec<String>,
    metadata_secrets: Vec<String>,
    data_dir: PathBuf,
    swarm_executable: PathBuf,
    mcp_cwd: PathBuf,
    mcp_server_name: String,
    native_subagents: Option<String>,
    credential: String,
    profile: serde_json::Map<String, Value>,
}

/// Dispatched Codex thread with its owned session: the native effect has
/// happened exactly once and its identity is proven; readiness settlement
/// takes over from here.
struct DispatchedThread {
    session: AgentSession,
    selected_model: String,
    selected_effort: Option<String>,
    readback: ThreadStartReadback,
}

/// Outcome of the dispatch phase: either a live dispatched thread ready for
/// readiness settlement, or a settled pre-dispatch report when the worker
/// never reached thread dispatch.
enum DispatchOutcome {
    Dispatched(Box<DispatchedThread>),
    Settled(Value),
}

/// Validate the frozen Codex start plan before any native I/O: resume
/// subject, source plan, profile, route, store binding, workspace, program
/// and launch environment. No child is spawned on this path.
fn validate_start_plan(
    params: &AgentStartParams,
    initialized: &Initialized,
) -> Result<ValidatedStartPlan> {
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
                "Codex",
            )
        })
        .transpose()
        .map_err(|mut error| {
            if error.code == "unknown" {
                error.code = "native_readback".to_owned();
            }
            error
        })?;
    validate_source_plan(
        "Codex",
        &["session", "turn", "message", "tool", "request"],
        params.source_plan.as_ref(),
    )?;
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
    let native_subagents = profile
        .get("native_subagents")
        .and_then(Value::as_str)
        .map(str::to_owned);
    // An omitted model selects the harness native default (`isDefault`)
    // at start; an explicit model must match exactly or fail honestly
    // with no substitution. Requested-absent stays null in readback.
    // The daemon carries its frozen choice separately in `daemon_model`,
    // so a model-less launch never becomes an explicit request.
    let requested_model: Option<String> = profile
        .get("model")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    let daemon_model: Option<String> = profile
        .get("daemon_model")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    // A caller request and a daemon freeze never name different models:
    // the freeze only fills an omitted request.
    if let (Some(requested), Some(frozen)) = (&requested_model, &daemon_model)
        && requested != frozen
    {
        return Err(Error::new(
            "profile_error",
            "Codex frozen daemon model does not match the requested model",
        ));
    }
    let requested_effort = optional_profile_text(profile, "effort", "Codex")?;
    let collaboration_mode =
        match optional_profile_text(profile, "collaboration_mode", "Codex")?.as_deref() {
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
    let approval_policy = optional_profile_text(profile, "approval_policy", "Codex")?
        .unwrap_or_else(|| "never".to_owned());
    if !matches!(approval_policy.as_str(), "never" | "on-request") {
        return Err(Error::new(
            "profile_error",
            "Codex approval_policy must be never or on-request",
        ));
    }
    let (mcp_policy, mcp_allow) = policy_from_profile(profile)?;
    let mcp_cloud_hostname = cloud_hostname_from_profile(profile);
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
    let data_dir = environment
        .iter()
        .find(|(name, _)| name.to_string_lossy().eq_ignore_ascii_case("ELIOT_HOME"))
        .map(|(_, value)| PathBuf::from(value))
        .filter(|path| path.is_absolute() && path.is_dir())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "Codex Agent MCP requires an absolute owned Eliot data directory",
            )
        })?;
    let swarm_executable = std::env::current_exe().map_err(|_| {
        Error::new(
            "platform_io",
            "active Swarm executable could not be resolved for the Codex Agent MCP",
        )
    })?;
    let mcp_cwd = std::fs::canonicalize(&workspace).map_err(|_| {
        Error::new(
            "profile_error",
            "Codex Agent workspace could not be resolved for its MCP cwd",
        )
    })?;
    let start_command = ulid::Ulid::from_string(&params.command_id)
        .map_err(|_| Error::new("invalid", "Codex start command id is not a ULID"))?
        .to_string()
        .to_ascii_lowercase();
    let mcp_server_name = format!("eliot_swarm_{start_command}");
    Ok(ValidatedStartPlan {
        resume_subject,
        route,
        frozen_store,
        requested_model,
        daemon_model,
        requested_effort,
        collaboration_mode,
        permissions,
        approval_policy,
        sandbox_mode,
        mcp_policy,
        mcp_allow,
        mcp_cloud_hostname,
        workspace,
        program,
        environment,
        secrets,
        metadata_secrets,
        data_dir,
        swarm_executable,
        mcp_cwd,

        mcp_server_name,
        native_subagents,
        credential: initialized.credential.clone(),
        profile: profile.clone(),
    })
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
    let mut native_endpoint_retired = false;
    let mut native_retirement = Ok(());
    let mut live_ticks = interval(LIVE_POLL_INTERVAL);
    live_ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut pending_request = None;
    loop {
        let request = if let Some(request) = pending_request.take() {
            Some(request)
        } else {
            tokio::select! {
                biased;
                () = eliot_plugin::native_lifecycle::wait_for_native_exit(
                    plugin.session.as_mut().map(|session| &mut session.native.transport)
                ) => {
                    if let Some(session) = plugin.session.as_mut() {
                        native_retirement = eliot_plugin::native_lifecycle::drain_native_exit(
                            &mut session.native.transport,
                            async {
                                session.events.drain_native().await?;
                                session.events.close().await;
                                Ok(())
                            },
                        ).await;
                    }
                    native_endpoint_retired = true;
                    break;
                }
                request = requests.recv() => request,
                _ = live_ticks.tick(), if plugin.observer.as_ref().is_some_and(ObserverSession::live_poll_enabled) => {
                    let initialized = plugin
                        .initialized
                        .as_ref()
                        .ok_or_else(|| not_initialized("Codex"))?;
                    let observer = plugin
                        .observer
                        .as_mut()
                        .ok_or_else(|| not_initialized("Codex"))?;
                    let until = Instant::now() + LIVE_POLL_DEADLINE;
                    // Each operation uses this deadline. Finish any publication
                    // before dispatching another command on the same observer.
                    let _ = observer.poll_live(&client, &initialized.credential, until).await;
                    continue;
                }
            }
        };
        let Some(request) = request else { break };
        let is_shutdown = request.method == "shutdown";
        let result = if request.method == "agent.snapshot" {
            match eliot_plugin::preemptible_snapshot(
                &mut requests,
                plugin.dispatch(&request, &client),
            )
            .await
            {
                eliot_plugin::SnapshotDispatch::Ready(result) => result,
                eliot_plugin::SnapshotDispatch::Preempted(next) => {
                    let preempted = client
                        .respond(
                            request.id.clone(),
                            Err(Error::new(
                                "busy",
                                "status snapshot was preempted by a host request",
                            )),
                            Instant::now() + RPC_TIMEOUT,
                        )
                        .await;
                    if preempted.is_err() {
                        break;
                    }
                    let Some(next) = next else { break };
                    pending_request = Some(next);
                    continue;
                }
            }
        } else {
            plugin.dispatch(&request, &client).await
        };
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

    if !completed_shutdown && !native_endpoint_retired {
        plugin.shutdown_owned().await;
    }
    reader.abort();
    let _ = reader.await;
    if native_endpoint_retired {
        native_retirement
    } else if completed_shutdown {
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
            "model.list" => self.model_list(&request.params).await,
            "quota.read" => self.quota_read(&request.params).await,
            "native.archive" => self.native_archive(&request.params).await,
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
        attach_reply_diagnostics(
            result,
            &request.method,
            "Codex",
            |error| session.native_client.diagnostic_error(error),
            |_| Error::new("unknown", ""),
        )
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams = decode(raw, "Codex plugin initialize params are invalid")?;
        if !initialize_fields_valid(&params) {
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
        ensure_dedicated_reader(
            self.session.is_some()
                || self.failed_start.is_some()
                || self.failed_attach.is_some()
                || self.observer.is_some(),
            "Codex",
            "discovery",
        )?;
        let params: DiscoverParams = decode(raw, "Codex discover params are invalid")?;
        let (launch, secrets, metadata_secrets) =
            self.reader_launch(&params.profile, &params.target_environment)?;
        crate::discovery::discover(params, launch, secrets, metadata_secrets).await
    }

    /// Read-only native model catalogue for `harness.models`. A dedicated
    /// reader plugin instance pages `model/list` with no thread, no input
    /// and no inference; the default entry is the native `isDefault`.
    async fn model_list(&self, raw: &Value) -> Result<Value> {
        ensure_dedicated_reader(
            self.session.is_some() || self.failed_start.is_some() || self.observer.is_some(),
            "Codex",
            "model.list",
        )?;
        let profile: Value = decode(raw, "Codex model.list params are invalid")?;
        let snapshot = profile.get("profile").unwrap_or(&profile);
        ensure_reader_harness(snapshot, "codex", "Codex", "model.list")?;
        let (launch, secrets, metadata_secrets) = self.reader_launch(snapshot, &json!({}))?;
        let entries = crate::discovery::list_models(
            launch,
            &metadata_secrets,
            &secrets,
            tokio::time::Instant::now() + crate::discovery::READ_TIMEOUT,
        )
        .await?;
        let default = entries
            .iter()
            .find(|entry| entry.is_default)
            .map(|entry| entry.id.clone());
        let models: Vec<Value> = entries
            .into_iter()
            .map(|entry| {
                json!({
                    "id": entry.id,
                    "display_name": eliot_core::api::harness::native_model_display_name(&entry.display_name),
                    "default": entry.is_default,
                    "efforts": entry.supported_reasoning_efforts.into_iter().map(|effort| effort.reasoning_effort).collect::<Vec<_>>(),
                    "default_effort": entry.default_reasoning_effort,
                })
            })
            .collect();
        Ok(json!({"models": models, "default": default}))
    }

    /// Read-only native account quota for `harness.quota`. A dedicated
    /// reader plugin instance calls `account/rateLimits/read` (plus
    /// `account/usage/read` when useful) with no thread, no input and no
    /// inference. Only quota signals are projected into `details`; account
    /// identity (including any email) is never read or returned.
    async fn quota_read(&self, raw: &Value) -> Result<Value> {
        ensure_dedicated_reader(
            self.session.is_some() || self.failed_start.is_some() || self.observer.is_some(),
            "Codex",
            "quota.read",
        )?;
        let params = eliot_plugin::quota::decode(raw)?;
        let snapshot = &params.profile;
        ensure_reader_harness(snapshot, "codex", "Codex", "quota.read")?;
        let (launch, secrets, metadata_secrets) =
            self.reader_launch(snapshot, &params.environment)?;
        let details = crate::discovery::read_quota(
            launch,
            &metadata_secrets,
            &secrets,
            tokio::time::Instant::now() + crate::discovery::READ_TIMEOUT,
        )
        .await?;
        Ok(json!({"details": details}))
    }

    /// Archive one thread through the documented `thread/archive` method. A
    /// dedicated reader plugin instance performs the call with no turn and
    /// no inference; raw session files are never touched. Used by Swarm
    /// cleanup for terminal Codex Agents; every other harness reports no
    /// native archive surface and keeps its sessions.
    async fn native_archive(&self, raw: &Value) -> Result<Value> {
        ensure_dedicated_reader(
            self.session.is_some() || self.failed_start.is_some() || self.observer.is_some(),
            "Codex",
            "native.archive",
        )?;
        let profile = raw.get("profile").unwrap_or(raw);
        ensure_reader_harness(profile, "codex", "Codex", "native.archive")?;
        let native_ref = raw
            .get("native_ref")
            .and_then(Value::as_str)
            .filter(|reference| {
                !reference.is_empty()
                    && reference.len() <= 512
                    && !reference.chars().any(char::is_control)
            })
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    "Codex native.archive needs a native session reference",
                )
            })?;
        let (launch, secrets, metadata_secrets) = self.reader_launch(profile, &json!({}))?;
        let binding =
            frozen_store_binding(profile.as_object().ok_or_else(|| {
                Error::new("profile_error", "archive requires a frozen profile")
            })?)?;
        crate::discovery::archive_thread(
            launch,
            &binding,
            native_ref.to_owned(),
            &metadata_secrets,
            &secrets,
            tokio::time::Instant::now() + crate::discovery::READ_TIMEOUT,
        )
        .await?;
        Ok(json!({"archived": true}))
    }

    fn reader_launch(
        &self,
        profile: &Value,
        environment: &Value,
    ) -> Result<(NativeLaunch, Vec<String>, Vec<String>)> {
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Codex"))?;
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
        let native_subagents = profile.get("native_subagents").and_then(Value::as_str);
        let launch = NativeLaunch {
            program,
            args: route.args_with_native_subagents(native_subagents)?,
            cwd: PathBuf::from(&initialized.instance_dir),
            environment: variables,
        };
        Ok((launch, secrets, metadata_secrets))
    }

    async fn profile_store(&mut self, raw: &Value) -> Result<Value> {
        if self.session.is_some()
            || self.failed_start.is_some()
            || self.failed_attach.is_some()
            || self.observer.is_some()
        {
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
        let profile_object = params.profile.as_object().ok_or_else(|| {
            Error::new(
                "profile_error",
                "Codex profile.store requires a frozen profile object",
            )
        })?;
        let (mcp_policy, mcp_allow) = policy_from_profile(profile_object)?;
        let mcp_cloud_hostname = cloud_hostname_from_profile(profile_object);
        let controller_executable = std::env::current_exe().ok();
        let controller_executable = controller_executable
            .as_ref()
            .and_then(|path| path.to_str());
        let binding = crate::discovery::profile_store(
            &launch,
            &params.target_environment,
            &metadata_secrets,
            &diagnostic_secrets,
            mcp_policy == McpPolicy::Minimal,
            &mcp_allow,
            controller_executable,
            mcp_cloud_hostname.as_deref(),
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
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Codex",
        )?;
        if self.session.is_some() || self.failed_start.is_some() {
            return Err(Error::new(
                "bad_state",
                "Codex observation requires a dedicated plugin instance",
            ));
        }
        observe::validate_attach_binding(&params)?;
        if let Some(failed) = &self.failed_attach {
            return failed.replay(&params);
        }
        if let Some(observer) = &self.observer {
            return observer.repeat_attach(&params);
        }
        let (launch, secrets, metadata_secrets) = match self
            .reader_launch(&params.profile, &params.target_environment)
        {
            Ok(launch) => launch,
            Err(_) => {
                let result = observe::failed_attach_result(&params, "observer_preparation_failed");
                self.failed_attach = Some(FailedObserverAttach::new(&params, result.clone()));
                return Ok(result);
            }
        };
        if let Err(error) = observe::validate_attach_preparation(&params, &secrets) {
            if error.code == "frame_too_large" {
                let result = observe::failed_attach_result(&params, "observer_preparation_failed");
                self.failed_attach = Some(FailedObserverAttach::new(&params, result.clone()));
                return Ok(result);
            }
            return Err(error);
        }
        let failure = FailedObserverAttach::new(
            &params,
            observe::failed_attach_result(&params, "native_start_failed"),
        );
        // A scope refusal proves no native effect: only the read-only store
        // probe ran. It retains a preparation failure for exact-binding replay.
        let scope_failure = FailedObserverAttach::new(
            &params,
            observe::failed_attach_result(&params, "observer_preparation_failed"),
        );
        let attach = ObserverSession::attach(
            params,
            launch,
            secrets,
            metadata_secrets,
            Instant::now() + crate::discovery::READ_TIMEOUT,
        )
        .await;
        match attach {
            Ok((observer, result)) => {
                self.observer = Some(observer);
                Ok(result)
            }
            Err(error) if is_proven_observer_start_failure(&error) => {
                self.failed_attach = Some(failure);
                Ok(self
                    .failed_attach
                    .as_ref()
                    .expect("failed attach was just retained")
                    .outcome
                    .clone())
            }
            Err(error) if is_proven_scope_refusal(&error) => {
                self.failed_attach = Some(scope_failure);
                Ok(self
                    .failed_attach
                    .as_ref()
                    .expect("scope refusal was just retained")
                    .outcome
                    .clone())
            }
            Err(error) => Err(error),
        }
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
        if self.observer.is_some() || self.failed_attach.is_some() {
            return Err(Error::new(
                "unsupported",
                "an observer cannot start a controlled session",
            ));
        }
        let params: AgentStartParams = decode(raw, "Codex agent.start params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Codex",
        )?;
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
                return failed.outcome.replay();
            }
            return Err(Error::new(
                "bad_state",
                "Codex plugin already settled a different start command",
            ));
        }
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Codex"))?;
        let plan = validate_start_plan(&params, initialized)?;
        let session = self.spawn_owned_session(&plan, &params, peer).await?;

        let dispatched = self.dispatch_thread_start(session, &params, &plan).await?;
        // Readiness and input settlement live in settle_start_readiness, unless
        // dispatch settled a pre-thread report: effective readback,
        // frozen-profile match, ready commitment, result.
        match dispatched {
            DispatchOutcome::Dispatched(thread) => {
                self.settle_start_readiness(
                    thread.session,
                    &params,
                    &plan,
                    thread.readback,
                    thread.selected_model,
                    thread.selected_effort,
                )
                .await
            }
            DispatchOutcome::Settled(value) => Ok(value),
        }
    }
    /// Spawn the owned Codex worker from the validated plan: enumerate the
    /// worker-visible MCP surface, compute transport-carrying disables,
    /// launch, spawn and register the native process. The returned session
    /// owns its child; a registration failure retains the settled outcome.
    async fn spawn_owned_session(
        &mut self,
        plan: &ValidatedStartPlan,
        params: &AgentStartParams,
        peer: &PeerClient,
    ) -> Result<AgentSession> {
        // Both policies enumerate the worker-visible native MCP surface,
        // then suppress with transport-carrying `enabled=false` overrides.
        // A bare `enabled=false` breaks the native bootstrap for
        // project/plugin names (verified on 0.167.1), so each disable
        // re-emits the enumerated `command`/`url`. Plugin-backed builtin
        // servers never appear in `mcp_servers`, so each suppressed plugin
        // takes its own `plugins.<id>.enabled=false` override, and the
        // ChatGPT-backed Apps runtime (likewise absent from `mcp_servers`)
        // takes a synthetic transport-carrying disable. Minimal fresh
        // launches reuse the surface frozen with store selection before
        // dispatch (allow-listed servers persist there by name only, and
        // an allow-listed owner-identity entry is rejected at freeze
        // time); minimal resumes enumerate afresh against the retained
        // store. Inherit enumerates afresh in both cases and keeps the
        // additive behavior except owner Swarm entries: every enumerated
        // entry that carries the owner Swarm identity by transport is
        // always suppressed, so a worker can never talk to the owner's
        // daemon with the owner's identity in either policy, whatever the
        // entry is named. An allow-listed name that matches the identity
        // is rejected at launch instead of re-admitted.
        let controller_executable = plan.swarm_executable.to_str();
        let (mcp_disables, plugin_disables) = if plan.mcp_policy == McpPolicy::Minimal {
            let surface = if plan.resume_subject.is_none() {
                crate::discovery::frozen_mcp_surface(&plan.profile, &params.command_id)?
            } else {
                let probe = NativeLaunch {
                    program: plan.program.clone(),
                    args: plan
                        .route
                        .args_with_native_subagents(plan.native_subagents.as_deref())?,
                    cwd: plan.workspace.clone(),
                    environment: plan.environment.clone(),
                };
                crate::discovery::read_effective_mcp_surface(
                    &probe,
                    &plan.frozen_store,
                    &plan.metadata_secrets,
                    &plan.secrets,
                    Instant::now() + RPC_TIMEOUT,
                )
                .await
                .map_err(|error| {
                    pre_dispatch_read_error(
                        error,
                        "profile_error",
                        "Codex minimal MCP server enumeration failed before worker spawn",
                    )
                })?
            };
            let mut disables = compute_disables(
                &surface.servers,
                &plan.mcp_allow,
                &plan.mcp_server_name,
                &plan.metadata_secrets,
                controller_executable,
                plan.mcp_cloud_hostname.as_deref(),
            )?;
            if let Some(apps) = synthetic_apps_disable(&plan.mcp_allow) {
                disables.push(apps);
            }
            let plugins = compute_plugin_disables(&surface.plugins, &plan.mcp_allow)?;
            for stub in synthetic_swarm_owner_disables() {
                if !disables.iter().any(|kept| kept.name == stub.name) {
                    disables.push(stub);
                }
            }
            (disables, plugins)
        } else {
            let probe = NativeLaunch {
                program: plan.program.clone(),
                args: plan
                    .route
                    .args_with_native_subagents(plan.native_subagents.as_deref())?,
                cwd: plan.workspace.clone(),
                environment: plan.environment.clone(),
            };
            let surface = crate::discovery::read_effective_mcp_surface(
                &probe,
                &plan.frozen_store,
                &plan.metadata_secrets,
                &plan.secrets,
                Instant::now() + RPC_TIMEOUT,
            )
            .await
            .map_err(|error| {
                pre_dispatch_read_error(
                    error,
                    "profile_error",
                    "Codex MCP server enumeration failed before worker spawn",
                )
            })?;
            let mut disables = compute_owner_disables(
                &surface.servers,
                &plan.mcp_server_name,
                controller_executable,
                plan.mcp_cloud_hostname.as_deref(),
                &plan.metadata_secrets,
            )?;
            for stub in synthetic_swarm_owner_disables() {
                if !disables.iter().any(|kept| kept.name == stub.name) {
                    disables.push(stub);
                }
            }
            (disables, Vec::new())
        };
        let launch = NativeLaunch {
            program: plan.program.clone(),
            args: plan.route.args_with_agent_mcp(
                &plan.mcp_server_name,
                &plan.swarm_executable,
                &plan.data_dir,
                &plan.mcp_cwd,
                &mcp_disables,
                &plugin_disables,
                plan.native_subagents.as_deref(),
            )?,
            cwd: plan.workspace.clone(),
            environment: plan.environment.clone(),
        };
        let launch = crate::discovery::pinned_launch_for_binding(&launch, &plan.frozen_store)?;
        let mut native =
            NativeProcess::spawn(launch, SecretPolicy::new(plan.secrets.iter().cloned())).await?;
        let frames = native.take_frames()?;
        let stream = native.stream_state();
        let native_client = native.client();
        let events = events::spawn(
            frames,
            stream,
            native_client.clone(),
            peer.clone(),
            EventConfig {
                credential: plan.credential.clone(),
                agent_id: params.agent_id.clone(),
                generation: params.generation,
                secrets: plan.secrets.clone(),
                native_scope: String::new(),
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
            selected_model: String::new(),
            selected_effort: None,
            native_effort: None,
            collaboration_mode: plan.collaboration_mode,
            secrets: plan.secrets.clone(),
            native,
            native_client,
            events,
            current_turn: None,
            stop_command_id: None,
        };

        if let Err(error) = register_native_process(
            peer,
            &plan.credential,
            &session.agent_id,
            session.generation,
            session.native.pid(),
            "Codex",
            "app-server",
            "thread",
            RPC_TIMEOUT,
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
                agent_id: params.agent_id.clone(),
                generation: params.generation,
                command_id: params.command_id.clone(),
                outcome: SettledStartFailure::Uncertain(error.clone()),
            });
            return Err(error);
        }
        Ok(session)
    }

    /// Dispatch thread start or resume on the spawned worker: native
    /// initialize, resume scope and preflight, thread dispatch and exact
    /// thread-identity readback. Uncertain native effects are kept
    /// unknown and never replayed.
    async fn dispatch_thread_start(
        &mut self,
        mut session: AgentSession,
        params: &AgentStartParams,
        plan: &ValidatedStartPlan,
    ) -> Result<DispatchOutcome> {
        let (selected_model, selected_effort, native_scope) = match Self::initialize_native(
            &session.native_client,
            &plan.workspace,
            plan.requested_model.as_deref(),
            plan.daemon_model.as_deref(),
            plan.requested_effort.as_deref(),
            plan.collaboration_mode.is_some(),
            NativeStartContext {
                route: &plan.route,
                store: &plan.frozen_store,
                metadata_secrets: &plan.metadata_secrets,
            },
        )
        .await
        {
            Ok((model, effort, scope)) => (model, effort, scope),
            Err(error) => {
                let error = discard_failed_start(&mut session, error).await;
                return self
                    .settle_pre_dispatch_outcome(
                        session,
                        params,
                        error,
                        "owned app-server cleanup is unconfirmed; no thread or input was sent",
                    )
                    .map(DispatchOutcome::Settled);
            }
        };
        session.selected_model = selected_model.clone();
        session.selected_effort = selected_effort.clone();
        session.native_scope = native_scope;

        if let Some(subject) = &plan.resume_subject {
            if subject.native_scope != session.native_scope {
                let error = discard_failed_start(
                    &mut session,
                    Error::new(
                        "profile_error",
                        "Codex resume subject is outside the authenticated native scope",
                    ),
                )
                .await;
                return self
                    .settle_pre_dispatch_outcome(
                        session,
                        params,
                        error,
                    "owned app-server cleanup is unconfirmed; no thread/resume or input was sent",
                    )
                    .map(DispatchOutcome::Settled);
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
                // Only prerequisite reads ran; native thread effects were not dispatched.
                let error = pre_dispatch_read_error(error, "native_readback", &message);
                let error = discard_failed_start(&mut session, error).await;
                return self
                    .settle_pre_dispatch_outcome(
                        session,
                        params,
                        error,
                    "owned app-server cleanup is unconfirmed; no thread/resume or input was sent",
                    )
                    .map(DispatchOutcome::Settled);
            }
        }

        // rust-v0.167.1 Config::multi_agent_version_override requires agents.enabled=false
        // and multi_agent_v2=false: model metadata can bypass multi_agent=false alone.
        // Apply the documented dotted config overrides to every start and resume,
        // including profiles that preserve the native reasoning-effort default.
        let thread_config = NativeThreadConfigOverrides::from_profile(
            selected_effort.clone(),
            plan.native_subagents.as_deref(),
        );
        let (thread_method, params_value) = if let Some(subject) = &plan.resume_subject {
            let params = NativeThreadResumeParams {
                thread_id: subject.native_ref.clone(),
                exclude_turns: true,
                model: Some(selected_model.clone()),
                model_provider: Some(plan.route.provider().to_owned()),
                cwd: Some(plan.workspace.to_string_lossy().into_owned()),
                sandbox: Some(plan.sandbox_mode),
                approval_policy: Some(json!(plan.approval_policy.clone())),
                config: thread_config,
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
                cwd: Some(plan.workspace.to_string_lossy().into_owned()),
                model: Some(selected_model.clone()),
                model_provider: Some(plan.route.provider().to_owned()),
                sandbox: Some(plan.sandbox_mode),
                approval_policy: Some(json!(plan.approval_policy.clone())),
                config: thread_config,
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
                let (code, message) = if plan.resume_subject.is_some() {
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
                    agent_id: params.agent_id.clone(),
                    generation: params.generation,
                    command_id: params.command_id.clone(),
                    outcome: SettledStartFailure::Uncertain(error.clone()),
                });
                return Err(error);
            }
            Err(error) if native_no_write(&error.code) => {
                let message = format!("Codex {thread_method} was not dispatched");
                let code = error.code.clone();
                let error = contextual_error(error, code, message);
                let error = discard_failed_start(&mut session, error).await;
                self.failed_start = Some(FailedStart {
                    agent_id: params.agent_id.clone(),
                    generation: params.generation,
                    command_id: params.command_id.clone(),
                    outcome: SettledStartFailure::Uncertain(error.clone()),
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
                    && plan
                        .resume_subject
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
        // Remember the actual native selection for collaboration settings
        // replacement (see send_command); effective reporting uses this
        // same readback value, never a fabricated effort.
        session.native_effort = readback.reasoning_effort.clone();
        if plan.resume_subject.is_some()
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
        Ok(DispatchOutcome::Dispatched(Box::new(DispatchedThread {
            session,
            selected_model,
            selected_effort,
            readback,
        })))
    }

    /// Settle start readiness for the dispatched thread: deliver the
    /// coordination skill, read back the MCP inventory, verify the
    /// effective profile against the frozen plan, commit ready and report.
    async fn settle_start_readiness(
        &mut self,
        mut session: AgentSession,
        params: &AgentStartParams,
        plan: &ValidatedStartPlan,
        readback: ThreadStartReadback,
        selected_model: String,
        selected_effort: Option<String>,
    ) -> Result<Value> {
        // Expose the daemon-materialized coordination skill before any
        // input is sent: without it the Agent would start believing it has
        // no skill. The roots replace nothing on disk; a failed delivery
        // fails the start like any rejected dispatch.
        if let Err(error) = deliver_swarm_skill(&session.native_client, &plan.data_dir).await {
            let error = discard_failed_start(&mut session, error).await;
            self.failed_start = Some(FailedStart {
                agent_id: params.agent_id.clone(),
                generation: params.generation,
                command_id: params.command_id.clone(),
                outcome: SettledStartFailure::Uncertain(error.clone()),
            });
            return Err(error);
        }
        // Requested vs observed MCP surface: read the thread-scoped
        // `mcpServerStatus/list` inventory now that the owned thread is
        // loaded (pre-thread runtimes are unavailable per the installed
        // schema, so this read cannot move earlier). Minimal fails closed
        // here when the surface is unverifiable, shows an enabled
        // non-allowed server, or omits the Agent-scoped server; inherit
        // performs no inventory readback and reports the set as unknown
        // instead of echoing the requested set. No input has been sent: the
        // thread is empty, and a failed start settles deterministically
        // like any rejected dispatch.
        let mcp_readback = if plan.mcp_policy == McpPolicy::Minimal {
            match Self::read_mcp_status(
                &session.native_client,
                &readback.thread.id,
                &plan.mcp_allow,
                &plan.mcp_server_name,
                &plan.metadata_secrets,
            )
            .await
            {
                Ok(inventory) => inventory,
                Err(error) => {
                    let error = discard_failed_start(&mut session, error).await;
                    self.failed_start = Some(FailedStart {
                        agent_id: params.agent_id.clone(),
                        generation: params.generation,
                        command_id: params.command_id.clone(),
                        outcome: SettledStartFailure::Uncertain(error.clone()),
                    });
                    return Err(error);
                }
            }
        } else {
            // Inherit keeps the exact previous startup path: no inventory
            // RPCs. Each inventory RPC owns up to the whole outer
            // `agent.start` budget, so even an optional read regresses
            // default startup; the set stays unknown instead.
            McpReadback::unverified()
        };
        // Only a requested effort is verified/enforced. An omitted effort
        // leaves the native default in `effective.effort`, reported
        // separately from `requested.effort` (null).
        let effort_verified = plan.route.requires_chatgpt()
            && selected_effort.is_some()
            && readback
                .reasoning_effort
                .as_deref()
                .is_some_and(|effort| Some(effort) == selected_effort.as_deref());
        // The thread request always names the selected model (the requested
        // one, or the catalogue default), so its echo proves the native
        // admission of that model.
        let model_verified = readback.model == selected_model;
        // Names only from the native inventory; `None` stays null so an
        // unobserved surface is never reported as an observed one.
        let mcp_servers = mcp_readback
            .servers
            .map(|servers| Value::Array(servers.into_iter().map(Value::String).collect()))
            .unwrap_or(Value::Null);
        let effective = json!({
            "model": readback.model.clone(),
            "model_provider": readback.model_provider.clone(),
            "cwd": readback.cwd.clone(),
            "permissions": readback.sandbox.clone(),
            "approval_policy": readback.approval_policy.clone(),
            "effort": readback.reasoning_effort.clone(),
            "model_verified": model_verified,
            "effort_verified": effort_verified,
            "mcp_policy": plan.mcp_policy.as_str(),
            "mcp_servers": mcp_servers,
            "mcp_servers_verified": mcp_readback.verified,
        });
        let effective = with_context_points(
            effective,
            &[InjectPoint::NextInput, InjectPoint::ActiveTurn],
        );
        // An omitted request accepts the native-selected model as
        // effective; an explicit request must match exactly.
        let settings_match = plan.requested_model
            .as_deref()
            .is_none_or(|requested| readback.model == requested)
            && readback.model == selected_model
            && readback.model_provider == plan.route.provider()
            && paths_equal(Path::new(&readback.cwd), &plan.workspace)
            && sandbox_matches(&readback.sandbox, &plan.permissions, &plan.workspace)
            && readback_approval_matches(&readback.approval_policy, &plan.approval_policy)
            // Enforce the exact effective effort only if one was requested;
            // an omitted effort accepts and reports the native readback.
            && (selected_effort.is_none()
                || readback
                    .reasoning_effort
                    .as_deref()
                    .is_none_or(|effort| Some(effort) == selected_effort.as_deref()));
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
                "model": plan.requested_model.clone(),
                "effort": selected_effort,
                "cwd": plan.workspace.to_string_lossy(),
                "permissions": plan.permissions.clone(),
                "approval_policy": plan.approval_policy.clone(),
                "mcp_policy": plan.mcp_policy.as_str(),
                "mcp_allow": plan.mcp_allow.clone(),
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

    /// Settle a pre-effect start failure after `discard_failed_start`.
    /// `session` is the owned post-discard session (phase Stopped or Unknown).
    /// Unconfirmed cleanup retains the controller with unknown; confirmed
    /// cleanup returns the authoritative failed-start value for exact replay
    /// (a residual unclassified pre-dispatch read failure becomes
    /// native_readback: no thread/resume/input was dispatched, but it is not a
    /// model refusal).
    fn settle_pre_dispatch_outcome(
        &mut self,
        session: AgentSession,
        params: &AgentStartParams,
        error: Error,
        unconfirmed_context: &str,
    ) -> Result<Value> {
        if session.phase == SessionPhase::Unknown {
            let error = unconfirmed_pre_dispatch_error(error, unconfirmed_context);
            self.session = Some(session);
            return Err(error);
        }
        let error = if error.code == "unknown" {
            let message = format!(
                "Codex pre-dispatch metadata read failed before thread dispatch: {}",
                error.message
            );
            pre_dispatch_read_error(error, "native_readback", &message)
        } else {
            error
        };
        let error = eliot_plugin::session::classify_start_failure(error, "codex", "prepare");
        let value = eliot_plugin::session::failed_start_value(params, error);
        self.failed_start = Some(FailedStart {
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            command_id: params.command_id.clone(),
            outcome: SettledStartFailure::ProvenNoEffect(value.clone()),
        });
        Ok(value)
    }

    async fn initialize_native(
        client: &NativeClient,
        workspace: &Path,
        requested_model: Option<&str>,
        daemon_model: Option<&str>,
        requested_effort: Option<&str>,
        experimental_api: bool,
        context: NativeStartContext<'_>,
    ) -> Result<(String, Option<String>, String)> {
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
                    // Only prerequisite reads ran; no thread or input was dispatched.
                    pre_dispatch_read_error(
                        error,
                        "profile_error",
                        "Codex app-server initialize failed",
                    )
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
                // Only prerequisite reads ran; no thread or input was dispatched.
                pre_dispatch_read_error(
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
        .await
        .map_err(|error| {
            // Only prerequisite reads ran; no thread or input was dispatched.
            if error.code == "unknown" {
                pre_dispatch_read_error(
                    error,
                    "native_scope_unverified",
                    "Codex native store selection could not be verified",
                )
            } else {
                error
            }
        })?;
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
                    // Only prerequisite reads ran; no thread or input was dispatched.
                    pre_dispatch_read_error(error, "profile_error", "Codex account/read failed")
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
                // Only prerequisite reads ran; no thread or input was dispatched.
                pre_dispatch_read_error(
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

        // Requested vs observed MCP surface is read after thread dispatch
        // (see `read_mcp_status`): the installed `mcpServerStatus/list`
        // reports per-thread runtime state, and without a loaded thread
        // every entry's status is unavailable, so a pre-thread read cannot
        // prove anything about the isolated surface. Inherit performs no
        // inventory readback at all and reports the set as unknown instead
        // of echoing the requested set.
        if !context.route.requires_chatgpt() {
            let requested = requested_model.ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "Codex Responses route requires an explicit model id",
                )
            })?;
            return Ok((
                requested.to_owned(),
                context.route.optional_effort(requested_effort)?,
                native_scope,
            ));
        }

        // The daemon freezes the native default in `daemon_model` before any
        // session spawns; an omitted model with no frozen value means the
        // daemon's live read failed, so fall back to the native `isDefault`
        // entry exactly as a daemon freeze would. `requested` stays null in
        // readback; the frozen id selects by `id` only.
        let model = discover_model(client, requested_model, daemon_model).await?;
        let effort = match requested_effort {
            Some(effort)
                if model
                    .supported_reasoning_efforts
                    .iter()
                    .any(|supported| supported.reasoning_effort == effort) =>
            {
                Some(effort.to_owned())
            }
            Some(_) => {
                return Err(Error::new(
                    "profile_error",
                    "Codex profile effort is not supported by the exact selected model",
                ));
            }
            // Omitted effort: preserve the native default by omitting the
            // wire field. The catalog lookup above still proves the
            // selected model exists; no medium/xhigh value is fabricated.
            None => None,
        };
        Ok((model.id.clone(), effort, native_scope))
    }

    /// Read the native MCP inventory for requested-vs-observed reporting,
    /// scoped to the owned thread. The installed `mcpServerStatus/list`
    /// reports per-thread runtime state (null/unavailable without a loaded
    /// thread per the 0.167.1 schema), so only a thread-scoped read can
    /// prove which servers are actually present and enabled; callers run
    /// this after thread/start|resume and before any input. Names only:
    /// tool/resource payloads are never retained. A page that echoes a
    /// known credential fails the read; truncation of the page loop leaves
    /// the inventory incomplete. The inventory itself is a read, so a failed
    /// read stays deterministic: no input was dispatched and the caller
    /// settles the empty thread without replaying an uncertain effect.
    async fn read_mcp_status(
        client: &NativeClient,
        thread_id: &str,
        allow: &[String],
        own_mcp_server: &str,
        metadata_secrets: &[String],
    ) -> Result<McpReadback> {
        let mut accumulated: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut incomplete = true;
        for _ in 0..crate::mcp::MAX_MCP_STATUS_PAGES {
            let mut params = json!({
                "detail": "toolsAndAuthOnly",
                "limit": 100,
                "threadId": thread_id,
            });
            if let Some(cursor) = cursor.as_deref() {
                params["cursor"] = Value::String(cursor.to_owned());
            }
            let response = client
                .call("mcpServerStatus/list", params, Instant::now() + RPC_TIMEOUT)
                .await
                .map_err(|error| {
                    // The inventory is a read: no input was dispatched, so a
                    // failed read stays deterministic.
                    pre_dispatch_read_error(
                        error,
                        "native_readback",
                        "Codex mcpServerStatus/list read failed; no input was sent",
                    )
                })?;
            let page = parse_status_page(&response, allow, own_mcp_server)?;
            let names = Value::Array(page.effective.iter().cloned().map(Value::String).collect());
            if SecretPolicy::new(metadata_secrets.iter().cloned()).contains_value(&names) {
                return Err(Error::new(
                    "secret_in_payload",
                    "Codex MCP inventory contains a known secret",
                ));
            }
            cursor = merge_status_page(&mut accumulated, &mut seen, page, true)?;
            if cursor.is_none() {
                incomplete = false;
                break;
            }
        }
        if incomplete {
            return Err(Error::new(
                "native_readback",
                "Codex MCP inventory is paginated beyond its bound",
            ));
        }
        // The completed thread-scoped inventory must show Eliot's own
        // Agent-scoped server as present and enabled; an empty inventory or
        // a disabled own entry proves nothing and fails closed here, before
        // any input. The empty thread is retained like any stopped Agent's
        // thread; it never receives input.
        require_minimal_own_server(&accumulated, own_mcp_server)?;
        Ok(McpReadback::verified(accumulated))
    }

    async fn command(&mut self, raw: &Value) -> Result<Value> {
        if self.observer.is_some() || self.failed_attach.is_some() {
            return Err(Error::new(
                "unsupported",
                "an observer cannot issue native control commands",
            ));
        }
        let params: AgentCommandParams = decode(raw, "Codex agent.command params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Codex",
        )?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            "steer" => self.steer(params).await,
            "inject" => self.inject_active_turn(params).await,
            "interrupt" => self.interrupt(params).await,
            "approve" | "deny" | "answer" => self.reply_request(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Codex plugin command verb is not implemented",
            )),
        }
    }

    /// Build one native `turn/start` request from the effective model and
    /// effort. Omitted effort omits the wire field (schema: effort
    /// optional, anyOf ReasoningEffort|null), preserving the native
    /// default. Native replaces the full collaboration settings when the
    /// mode is present, so a null effort there would clear the
    /// native-configured selection: carry the actual native readback
    /// instead; no arbitrary reasoning is forced, the requested effort
    /// stays absent and the normal turn `effort` field above stays omitted.
    fn turn_start_params(
        thread_id: String,
        input: Vec<NativeUserInput>,
        model: String,
        effort: Option<String>,
        collaboration_mode: Option<NativeCollaborationModeKind>,
        native_effort: Option<String>,
    ) -> NativeTurnStartParams {
        NativeTurnStartParams {
            thread_id,
            input,
            cwd: None,
            model: Some(model.clone()),
            effort: effort.clone(),
            collaboration_mode: collaboration_mode.map(|mode| NativeCollaborationMode {
                mode,
                settings: NativeCollaborationSettings {
                    model,
                    reasoning_effort: effort.or(native_effort),
                    developer_instructions: None,
                },
            }),
            sandbox_policy: None,
            client_user_message_id: None,
        }
    }

    async fn send_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Codex",
        )?;
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
        let event_status = session.events.status();
        if let Some(reason) = event_status.failure {
            return Err(Error::new("unknown", reason)
                .with_where("plugin.codex.command.send")
                .with_origin(
                    eliot_core::FailureOrigin::Swarm,
                    "send gate refused on event-source failure",
                ));
        }
        if event_status.active_turn_id.is_some() {
            return Err(Error::new("busy", "Codex already has an active turn")
                .with_where("plugin.codex.command.send")
                .with_origin(
                    eliot_core::FailureOrigin::Swarm,
                    "send gate refused: native turn already active",
                ));
        }
        let combined = with_claimed_context(text, &params.context, "Codex")?;
        let command_id = params.command_id.clone();
        // A retained `configure` overlay rides the per-turn fields the
        // installed schema accepts; without one the frozen session values
        // apply unchanged.
        let model = params
            .model_override
            .clone()
            .unwrap_or_else(|| session.selected_model.clone());
        let effort = params
            .effort_override
            .clone()
            .or_else(|| session.selected_effort.clone());
        // What the native turn request actually carries: the requested
        // override, or the native readback when the profile (and the
        // overlay) left effort absent. Reported through the turn association
        // only under a `configure` overlay; plain sends keep their exact
        // event shape.
        let sent_model = params.config_revision.is_some().then(|| model.clone());
        let sent_effort = params
            .config_revision
            .is_some()
            .then(|| effort.clone().or_else(|| session.native_effort.clone()))
            .flatten();
        let turn_params = Self::turn_start_params(
            session
                .thread_id
                .clone()
                .ok_or_else(|| Error::new("unknown", "Codex thread identity is unresolved"))?,
            vec![NativeUserInput::Text(NativeTextUserInput::text(combined))],
            model.clone(),
            effort.clone(),
            session.collaboration_mode,
            session.native_effort.clone(),
        );
        let encoded = serde_json::to_value(turn_params)
            .map_err(|_| Error::new("internal", "Codex turn/start params could not be encoded"))?;
        let response = session
            .native_client
            .call("turn/start", encoded, Instant::now() + RPC_TIMEOUT)
            .await;
        let response = match response {
            Ok(value) => value,
            Err(error) if native_no_write(&error.code) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Codex turn/start was not dispatched",
                }));
            }
            Err(error) if error.code == "native_rejected" => {
                return Ok(json!({"state": "rejected", "reason": "Codex rejected turn/start"}));
            }
            Err(error) if error.code == "frame_too_large" => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Codex turn/start frame exceeded the native transport bound before dispatch",
                }));
            }
            Err(_) => {
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
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "Codex turn/start returned no exact turn identity",
                    ));
                }
            };
        let turn_id = started.turn.id;
        session.current_turn = Some((command_id.clone(), turn_id.clone()));
        if session
            .events
            .associate_turn(
                command_id.clone(),
                turn_id.clone(),
                params.context.clone(),
                sent_model.clone(),
                sent_effort.clone(),
            )
            .await
            .is_err()
        {
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                "Codex turn was accepted but turn.started was not acknowledged",
            ));
        }
        // Echo what the native request carried when a `configure` overlay
        // applied, so the settled outcome reads back the effective values
        // alongside the native turn association. Unchanged without one.
        let mut result = json!({"state": "applied", "turn_id": turn_id});
        if let Some(revision) = params.config_revision {
            result["config_revision"] = json!(revision);
            result["model"] = json!(model);
            result["effort"] = json!(sent_effort);
        }
        Ok(result)
    }

    async fn steer(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Codex",
        )?;
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
        if session.phase != SessionPhase::Ready
            || session.events.status().active_turn_id.as_deref() != Some(turn_id)
        {
            let result = rejected_control("Codex steer does not match a healthy active turn");
            return Ok(result);
        }
        let Some(thread_id) = session.thread_id.clone() else {
            let result = unknown_control("Codex thread identity is unresolved");
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
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Codex turn/steer outcome is unknown");
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
                            return Ok(result);
                        }
                    };
                let result = json!({
                    "state": "applied",
                    "command_id": params.command_id,
                    "turn_id": steer.turn_id,
                });
                Ok(result)
            }
        }
    }

    /// Direct `active_turn` inject: one command is one native turn/steer
    /// effect on the captured exact turn. The applied ack carries the typed
    /// context outcome; rejected/unknown results settle unchanged.
    async fn inject_active_turn(&mut self, params: AgentCommandParams) -> Result<Value> {
        let args: InjectArgs = serde_json::from_value(params.args.clone())
            .map_err(|_| Error::new("invalid", "inject args are malformed"))?;
        if args.point != InjectPoint::ActiveTurn {
            return Err(Error::new(
                "unsupported",
                "Codex inject point is not available on the active turn",
            ));
        }
        let consumer_id = params.command_id.clone();
        let mut result = self.steer(params).await?;
        if result.get("state").and_then(Value::as_str) == Some("applied") {
            result["context_outcome"] = serde_json::to_value(ContextOutcome {
                mechanism: "codex.turn/steer".to_owned(),
                application_point: InjectPoint::ActiveTurn,
                consumer_id,
            })
            .map_err(|_| Error::new("internal", "context outcome could not be encoded"))?;
        }
        Ok(result)
    }

    async fn interrupt(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Codex",
        )?;
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
            return Ok(rejected_control(
                "turn/interrupt does not match the exact active native turn",
            ));
        }
        let Some(thread_id) = session.thread_id.clone() else {
            let result = unknown_control("Codex thread identity is unresolved");
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
                let result = rejected_control(&format!(
                    "Codex turn/interrupt was not applied: {}",
                    error.message
                ));
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Codex turn/interrupt outcome is unknown");
                Ok(result)
            }
            Ok(_) => {
                let result = json!({
                    "state": "applied",
                    "command_id": params.command_id,
                    "turn_id": turn_id,
                });
                Ok(result)
            }
        }
    }

    async fn reply_request(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Codex Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Codex",
        )?;
        let (turn_id, request_id) = request_reply_ids(&params, "Codex")?;
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
            Ok(result) => Ok(result),
            Err(error) if native_no_write(&error.code) => {
                let result = rejected_control("Codex request reply was not dispatched");
                Ok(result)
            }
            Err(error) if error.code == "unknown" => {
                Ok(unknown_control("Codex request reply outcome is unknown"))
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "invalid" | "unsupported" | "secret_in_payload" | "stale_turn" | "not_found"
                ) =>
            {
                let result = rejected_control(&error.message);
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
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Codex",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Codex Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Codex",
        )?;
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
        // The daemon binds an explicit requested turn in Store. If the
        // association acknowledgement was lost, the adapter can still
        // translate its one current native turn for the exact command.
        let turn_id = params.turn_id.clone().or_else(|| {
            session
                .current_turn
                .as_ref()
                .filter(|(command, _)| command == &params.command_id)
                .map(|(_, turn)| turn.clone())
        });
        if turn_id.is_none()
            && session.start_command_id != params.command_id
            && session.stop_command_id.as_deref() != Some(params.command_id.as_str())
        {
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
        // This is a fresh, read-only native proof for the exact binding. The
        // daemon may use the marker as evidence provenance; it is never a
        // settlement signal and carries no retained plugin projection.
        let turn_evidence = last_turn.as_ref().map(|turn| {
            if turn.terminal {
                "thread/read+thread/turns/list+thread/items/list"
            } else {
                "thread/read+thread/turns/list"
            }
        });
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
            "turn_evidence": turn_evidence,
        }))
    }

    async fn stop(&mut self, raw: &Value) -> Result<Value> {
        if self.observer.is_some() || self.failed_attach.is_some() {
            return Err(Error::new(
                "unsupported",
                "an observer cannot stop the native subject",
            ));
        }
        let params: AgentStopParams = decode(raw, "Codex agent.stop params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Codex",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Codex Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Codex",
        )?;
        // Latch once before native I/O. The router owns interrupt/wait and
        // daemon force custody; this adapter only performs the graceful
        // native close and verifies the event/source drain.
        if begin_stop(
            &mut session.stop_command_id,
            session.phase == SessionPhase::Stopped,
            &params.command_id,
            "Codex",
        )? {
            return Ok(json!({"state": "applied"}));
        }
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("unknown", "stop lost its retained native session"))?;
        terminate_session(session, false).await?;
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
    let deadline = Instant::now() + RPC_TIMEOUT;
    let process_exit = if force {
        // Startup/plugin-shutdown cleanup retains its separate forceful path.
        session.native.terminate(true, deadline).await
    } else {
        // Agent stop must never escalate to a child kill; daemon Job custody
        // owns force termination after this graceful close is unresolved.
        session
            .native
            .transport
            .close_gracefully(deadline, true)
            .await
    };
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

async fn discard_unstarted(session: &mut AgentSession) {
    session.events.close().await;
    session.phase = match session
        .native
        .terminate(true, Instant::now() + RPC_TIMEOUT)
        .await
    {
        Ok(_) => SessionPhase::Stopped,
        Err(_) => SessionPhase::Unknown,
    };
}

/// Expose the daemon-materialized `eliot-swarm` skill to the owned
/// app-server process. `skills/extraRoots/set` replaces the process-level
/// extra roots used to discover standalone skills without persisting them
/// (installed 0.167.1 app-server schema; connection-wide, after
/// initialize). The plugin owns one app-server per Agent generation, so
/// the roots are effectively per-session; the checkout and the global
/// skill locations stay untouched.
async fn deliver_swarm_skill(native_client: &NativeClient, data_dir: &Path) -> Result<()> {
    let roots = skill::skill_root(data_dir).join("skills");
    let roots = roots
        .to_str()
        .ok_or_else(|| Error::new("invalid", "Codex skill roots path is not UTF-8"))?;
    native_client
        .call(
            "skills/extraRoots/set",
            json!({ "extraRoots": [roots] }),
            Instant::now() + RPC_TIMEOUT,
        )
        .await
        .map_err(|error| {
            Error::new(
                "skill_unavailable",
                format!("Codex skill roots were not delivered: {}", error.message),
            )
        })?;
    Ok(())
}

async fn discard_failed_start(session: &mut AgentSession, error: Error) -> Error {
    discard_unstarted(session).await;
    session.native_client.diagnostic_error(error)
}

/// Pure native catalogue selection shared by start and `model.list`.
/// An explicit request matches exactly (`id` and `model`) or fails
/// honestly with no substitution; a daemon-frozen model-less launch matches
/// by `id` only, preserving the native `isDefault` acceptance (no
/// `model ==` check); a cold omitted request uses the native `isDefault`
/// entry or fails honestly when the catalogue names none.
fn select_launch_model(
    entries: &[NativeModelEntry],
    requested_model: Option<&str>,
    daemon_model: Option<&str>,
) -> Result<NativeModelEntry> {
    if let Some(requested) = requested_model {
        return entries
            .iter()
            .find(|model| model.id == requested && model.model == requested)
            .cloned()
            .ok_or_else(|| {
                eliot_plugin::start_classification::harness_unavailable_model(
                    "codex",
                    "model_list",
                    "The exact requested Codex model is unavailable in this account catalogue",
                )
            });
    }
    if let Some(frozen) = daemon_model.filter(|frozen| !frozen.trim().is_empty()) {
        return entries
            .iter()
            .find(|model| model.id == frozen)
            .cloned()
            .ok_or_else(|| {
                eliot_plugin::start_classification::harness_unavailable_model(
                    "codex",
                    "model_list",
                    "Codex model/list no longer contains the daemon-frozen default model; use an explicit model",
                )
            });
    }
    entries
        .iter()
        .find(|model| model.is_default)
        .cloned()
        .ok_or_else(|| {
            eliot_plugin::start_classification::harness_unavailable_model(
                "codex",
                "model_list",
                "Codex model/list reported no default model; use an explicit model",
            )
        })
}

/// Bounded launch catalogue: per-page `RPC_TIMEOUT` capped by an overall
/// launch budget, so two slow pages fit their individual budgets.
const LAUNCH_CATALOGUE_BUDGET: std::time::Duration = std::time::Duration::from_secs(120);

async fn discover_model(
    client: &NativeClient,
    requested_model: Option<&str>,
    daemon_model: Option<&str>,
) -> Result<NativeModelEntry> {
    // Only prerequisite reads ran; no thread or input was dispatched.
    let (entries, truncated) = crate::discovery::collect_model_pages(
        client,
        RPC_TIMEOUT,
        Instant::now() + LAUNCH_CATALOGUE_BUDGET,
        || Error::new("internal", "Codex model/list params could not be encoded"),
        |error| {
            pre_dispatch_read_error(
                error,
                "profile_error",
                "Codex model/list did not return a catalogue page",
            )
        },
        || {
            Error::new(
                "profile_error",
                "Codex model/list page did not match the installed schema",
            )
        },
        || {
            Error::new(
                "profile_error",
                "Codex model/list returned a repeated or invalid cursor",
            )
        },
    )
    .await?;
    if truncated {
        return Err(Error::new(
            "profile_error",
            "Codex model/list exceeded the bounded pagination limit",
        ));
    }
    // End of catalogue: select from all pages at once so an omitted
    // request sees the default wherever it is paged. Requested
    // selection still fails honestly with no substitution.
    select_launch_model(&entries, requested_model, daemon_model)
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

/// Shared error re-coding, kept under the local path for the sibling
/// modules that reach it through this host.
pub(crate) use eliot_plugin::session::contextual_error;

/// Recode a pre-dispatch read failure to its deterministic code after proven
/// no native effect. Only prerequisite reads ran; no thread/resume/input was
/// dispatched. Unlike `contextual_error` (which preserves unknown for uncertain
/// writes), this forcibly recodes even transport-unknown reads, because the
/// read proved no native write. Callers retain failed_start only after
/// confirmed cleanup; unconfirmed custody retains session+unknown separately.
fn pre_dispatch_read_error(error: Error, code: &str, message: &str) -> Error {
    error.with_code(code).with_message(message)
}

/// Recode any pre-dispatch failure to unknown when owned cleanup is
/// unconfirmed. No thread/resume/input was dispatched, but the child may still
/// hold custody, so the session is retained and the daemon must not treat this
/// as settled.
fn unconfirmed_pre_dispatch_error(error: Error, context: &str) -> Error {
    let message = format!("{}; {context}", error.message);
    error.with_code("unknown").with_message(message)
}

fn target_environment(
    environment: Option<&Value>,
    pass_env: Option<&Value>,
) -> Result<TargetEnvironment> {
    let shared = shared_target_environment(environment, pass_env, "Codex")?;
    // Only selector contributions are public store metadata. Never
    // remove by value: a credential may have the same bytes as a path.
    let mut metadata_secrets = Vec::new();
    for (name, value) in &shared.variables {
        let name = name.to_string_lossy();
        let selector = ["CODEX_HOME", "CODEX_SQLITE_HOME"]
            .into_iter()
            .any(|selector| {
                if cfg!(windows) {
                    name.eq_ignore_ascii_case(selector)
                } else {
                    name.as_ref() == selector
                }
            });
        if !selector
            && !value.is_empty()
            && (is_secret_name(name.as_ref()) || is_passed_env(name.as_ref(), pass_env))
        {
            metadata_secrets.push(value.to_string_lossy().into_owned());
        }
    }
    Ok(TargetEnvironment {
        variables: shared.variables,
        secrets: shared.secrets,
        metadata_secrets,
    })
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
    let observed = BTreeMap::from([
        ("message".to_owned(), "native_read".to_owned()),
        ("session".to_owned(), "native_read".to_owned()),
        ("tool".to_owned(), "native_read".to_owned()),
        ("turn".to_owned(), "native_read".to_owned()),
    ]);
    PluginManifest::per_agent(
        "codex",
        env!("CARGO_PKG_VERSION"),
        "0.161.0",
        &[
            "launch",
            "resume",
            "send",
            "steer",
            "interrupt",
            "approve",
            "deny",
            "answer",
            "inject",
            "stop",
        ],
        eliot_plugin::protocol::ManifestFeatures {
            discover: true,
            observe: true,
            start_requires_input: false,
            native_children: true,
        },
        BTreeMap::from([
            (
                "controlled".to_owned(),
                stream_sources(&["message", "request", "session", "tool", "turn"]),
            ),
            ("observed".to_owned(), observed),
        ]),
        "responses",
    )
}
