use crate::{
    events::{self, EventConfig, EventHandle, EventStatus, InboxRecord, SteerVerdict, TurnFacts},
    native::{
        AuthSetup, AuthTransfer, HttpOutcome, IntegrationSelector, OpenCodeHttp, OwnedServer,
        ProviderFragment, SWARM_MCP_SERVER_NAME, ServerLaunch,
        models::{MAX_MODEL_ID_BYTES, ReaderContext, run_model_metadata, run_models},
        server::{provider_config, validate_fragment_environment},
        transfer_selected_integration,
        types::{
            ConfigEntry, ConfigMcpServer, InboxUser, McpServerReadback, ModelRef, NativeModelEntry,
            PermissionRule, SessionInfo,
        },
    },
};
use eliot_core::{ContextItem, Error, FailureOrigin, InjectPoint, McpPolicy, Result};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::{AgentModelsParams, ModelListParams, ModelListPart},
    session::{
        ResumeSubject, TargetEnvironment, decode, ensure_dedicated_reader, ensure_reader_harness,
        native_no_write, not_initialized, optional_profile_text, parse_resume_subject, paths_equal,
        register_native_process, split_provider_model, target_environment, unknown_snapshot,
        validate_binding, validate_identity, validate_source_plan, with_claimed_context,
        with_context_points,
    },
    start_classification::SettledStartFailure,
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};
use tokio::{
    sync::mpsc,
    time::{Instant, timeout_at},
};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Total bound for one agent.start dispatch, from admission to the ready,
/// failed, or unknown response. The daemon allows 90 s for the agent.start
/// round trip ([`DAEMON_START_TIMEOUT`], `START_TIMEOUT` in
/// crates/eliot-daemon/src/plugin_host/host.rs); this envelope answers
/// inside it with [`START_SETTLE_RESERVE`] left for the daemon's Job
/// retirement and settlement. Every pre-session phase (auth transfer, serve
/// spawn and readiness, subscribe, registration, session create) shares this
/// one envelope instead of stacking private budgets, and every start-failure
/// cleanup (event close plus owned-server terminate and diagnostics) is
/// clamped to it too: a loaded cold start gets room without ever outliving
/// the daemon budget (which would settle the launch unknown while the plugin
/// is still working, losing the late reply). A failure before any native
/// session or prompt exists settles as a definite failed start with a typed
/// reason, never unknown, except an uncertain auth import (the isolated
/// credential store may already have been written and reconciliation
/// failed): that stays unknown per the authentication contract. Unknown
/// otherwise stays reserved for real uncertainty about a native effect.
const START_ENVELOPE: std::time::Duration = std::time::Duration::from_secs(75);

/// Bound for one start-failure cleanup (event close plus owned-server
/// terminate and diagnostics), always clamped to [`START_ENVELOPE`]: a
/// refusal at the edge of the envelope fails the cleanup fast instead of
/// stacking another 30 s past the daemon budget. 10 s matches the serve
/// readiness cleanup in `native/server.rs`: enough to kill and reap a wedged
/// child, never enough to outlive the daemon budget. The post-create share
/// splits into one native DELETE round-trip plus the event close and server
/// terminate, each with its own deadline inside this reserve.
const START_CLEANUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

/// Post-create DELETE share of the cleanup reserve: one native
/// session-delete round-trip. The delete deadline always ends a full
/// shutdown share before the envelope, so the server shutdown below keeps
/// its own deadline even when the delete starts at the cutoff edge.
const START_DELETE_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

/// Post-create shutdown share of the cleanup reserve: the event close plus
/// the server terminate together fit inside it.
const START_SHUTDOWN_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

/// Time reserved after the envelope for the daemon's Job retirement and
/// settlement: the envelope, cleanup included, always ends this far before
/// the daemon 90 s agent.start budget, so a refusal at the edge of the
/// budget settles failed and leaves `starting` instead of racing the daemon
/// deadline (which would drop the reply, record unknown, and pin the Agent).
const START_SETTLE_RESERVE: std::time::Duration = std::time::Duration::from_secs(15);

/// Daemon agent.start round-trip budget, mirrored from
/// crates/eliot-daemon/src/plugin_host/host.rs (`START_TIMEOUT`): the start
/// envelope plus the settlement reserve must fit inside it.
const DAEMON_START_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(90);

/// Clamp one start-phase budget to the dispatch envelope: the phase gets its
/// full budget when the envelope has room, otherwise whatever is left. A
/// spent envelope fails the phase fast instead of stacking another 30 s.
fn phase_deadline(envelope: Instant, budget: std::time::Duration) -> Instant {
    (Instant::now() + budget).min(envelope)
}

/// Duration form of the same clamp for callees that take a timeout instead
/// of a deadline. A spent envelope yields zero, so the phase fails fast.
fn phase_budget(envelope: Instant, budget: std::time::Duration) -> std::time::Duration {
    phase_deadline(envelope, budget).saturating_duration_since(Instant::now())
}

/// Cutoff for the create request and response and all checks before readiness.
/// DELETE and server shutdown keep separate deadlines inside the reserve.
fn create_cutoff(envelope: Instant) -> Instant {
    envelope
        .checked_sub(START_CLEANUP_BUDGET)
        .unwrap_or_else(Instant::now)
}

fn create_deadline(cutoff: Instant) -> Instant {
    phase_deadline(cutoff, RPC_TIMEOUT)
}

/// Post-create DELETE deadline: the delete share, always ending a full
/// shutdown share before the envelope so the server shutdown below keeps
/// its own deadline even when the delete starts at the cutoff edge.
fn delete_deadline(envelope: Instant) -> Instant {
    phase_deadline(
        envelope
            .checked_sub(START_SHUTDOWN_BUDGET)
            .unwrap_or_else(Instant::now),
        START_DELETE_BUDGET,
    )
}

/// Where a start failure happened relative to the first native session or
/// prompt write. Everything up to (but excluding) an admitted session/create
/// proves no native session or prompt could exist yet: the owned server may
/// not even be up, and the only writes so far are credential preparation
/// under isolated roots. At or after the create write the effect is real or
/// uncertain, so those failures keep the historical error settlement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartPhase {
    /// Credential transfer, serve spawn, or serve readiness: no HTTP write
    /// has run and no server is necessarily up. The transfer may already
    /// have touched the isolated credential store, so an uncertain transfer
    /// outcome (code `unknown`) is not proven-no-effect even though no
    /// native session or prompt exists.
    Spawn,
    /// Event subscription: read-only, before session create.
    Subscribe,
    /// Daemon-side native-process registration, before session create.
    Register,
    /// Native catalogue verification, before session create: the isolated
    /// server answers for the expected provider/model without creating state.
    Verify,
    /// Session/create refused or provably never dispatched.
    CreateRefused,
    /// Resume refused before the retained session was touched (wrong scope
    /// or proven absent): this generation created nothing and the retained
    /// session, if any, is untouched.
    ResumeRefused,
    /// Session/create may have been admitted; later readbacks and commits.
    /// Only constructed in tests to pin the boundary: production never
    /// converts a post-create failure into a proven-no-effect value.
    #[allow(dead_code)]
    PostCreate,
}

/// Whether a failure in this start phase with this error code proves no
/// native session or prompt exists and no uncertain native effect is
/// outstanding, so the launch settles as a definite failed start instead of
/// unknown. This is the regression anchor for the pre-session rule: every
/// phase before the first create write maps here, and no post-create phase
/// may, with one exception. A `Spawn` failure carrying code `unknown` is the
/// uncertain auth import: the isolated credential store may already have
/// been written and reconciliation failed, so it stays unknown per the
/// authentication contract and never converts to proven-no-effect. Definite
/// refusals before the import (any other code) still settle failed.
fn start_point_for_phase(
    phase: StartPhase,
    code: &str,
) -> eliot_plugin::start_classification::StartPoint {
    use eliot_plugin::start_classification::StartPoint;
    if phase == StartPhase::Spawn && code == "unknown" {
        return StartPoint::TransportLost;
    }
    match phase {
        StartPhase::Spawn => StartPoint::SpawnRefused,
        StartPhase::Subscribe | StartPhase::Register | StartPhase::Verify => {
            StartPoint::BeforeSpawn
        }
        StartPhase::CreateRefused | StartPhase::ResumeRefused => {
            StartPoint::NativeRefusalBeforeDispatch
        }
        StartPhase::PostCreate => StartPoint::AfterDispatch,
    }
}

fn start_failure_is_proven_no_effect(phase: StartPhase, code: &str) -> bool {
    eliot_plugin::start_classification::is_proven_no_effect(start_point_for_phase(phase, code))
}

/// One pre-session failure: where it happened and the triggering error. The
/// caller converts this into the authoritative proven-no-effect failed start
/// instead of an error, so the daemon settles the launch failed and retires
/// the plugin instead of pinning an unknown Agent.
struct PhaseFailure {
    phase: StartPhase,
    error: Box<Error>,
}

/// Typed reason for one proven-no-effect failed start. Static per phase plus
/// the triggering error code (a fixed literal, never a payload): enough to
/// tell a slow cold start from a refused selector without risking secrets,
/// endpoints, or native text in the persisted outcome.
impl StartPhase {
    /// Stable pre-session location for failed-start outcome values:
    /// `plugin.opencode.start.<phase>`. The credential transfer shares the
    /// `spawn` phase but reports its own `auth_transfer` step in the reason.
    fn as_str(self) -> &'static str {
        match self {
            StartPhase::Spawn => "spawn",
            StartPhase::Subscribe => "subscribe",
            StartPhase::Register => "register",
            StartPhase::Verify => "verify",
            StartPhase::CreateRefused => "create_refused",
            StartPhase::ResumeRefused => "resume_refused",
            StartPhase::PostCreate => "post_create",
        }
    }
}

fn presession_reason(phase: StartPhase, code: &str) -> String {
    let what = match phase {
        StartPhase::Spawn => "OpenCode serve failed to start",
        StartPhase::Verify => "OpenCode native catalogue verification failed",
        StartPhase::Subscribe => "OpenCode event subscription failed",
        StartPhase::Register => "OpenCode native-process registration failed",
        StartPhase::CreateRefused => "OpenCode session/create was not admitted",
        StartPhase::ResumeRefused => "OpenCode resume did not start",
        StartPhase::PostCreate => "OpenCode start failed after session/create",
    };
    eliot_plugin::start_classification::format_start_reason(
        what,
        code,
        "no native session or prompt was dispatched by this generation",
    )
}

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
    outcome: SettledStartFailure,
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
    Retention,
    Process,
    EndAck,
}

impl StopStage {
    fn as_str(self) -> &'static str {
        match self {
            Self::Begin => "begin",
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
    stop_command_id: Option<String>,
    resources_proved_closed: bool,
    stop_end_ack_proved: bool,
}

/// A start request that passed admission: decoded params plus the validated
/// frozen profile, workspace, child environment, selected native auth
/// integration (cloud path) or native provider fragment (fragment path), and
/// redaction secrets. Exactly one of `auth_selector` and `provider_fragment`
/// is set: the fragment path never imports a canonical credential.
struct ParsedStart {
    params: AgentStartParams,
    /// Frozen native-subagent switch (`allow` or `deny`): `deny` stores a
    /// per-session `subagent` deny ruleset on the session (verified by
    /// readback on create and resume) and keeps the `task` deny in the
    /// isolated per-Agent config as a second layer.
    native_subagents: Option<String>,
    /// Requested `provider/model`, or `None` when the profile omitted it and
    /// the configured native default applies. Requested stays absent
    /// separately from the effective readback.
    requested_model: Option<String>,
    /// Split model ref, present exactly when a model was requested.
    model_ref: Option<ModelRef>,
    /// Inherited-overlay realignment target for a resume: the retained
    /// effective model the session already runs. `None` on launch and on
    /// resumes without an overlay; the daemon resolves it from the resume
    /// command's retained revision.
    override_model: Option<String>,
    /// Split override ref, present exactly when an override was carried.
    override_model_ref: Option<ModelRef>,
    /// Frozen profile effort (`Model.Ref.variant`), or `None` for the
    /// configured native default.
    requested_effort: Option<String>,
    /// Inherited-overlay effort for a resume, resolved like
    /// `override_model`.
    override_effort: Option<String>,
    requested_agent: Option<String>,
    /// Frozen worker MCP policy. The owned serve never sees the owner's
    /// global config (isolated per-Agent `XDG_CONFIG_HOME`/`XDG_DATA_HOME`),
    /// so both policies serve the Agent-scoped server; the policy is still
    /// read and reported so the record stays truthful.
    requested_mcp_policy: McpPolicy,
    /// Frozen configured cloud MCP hostname for the owner-identity HTTP
    /// arm. `None` when the daemon configures no cloud endpoint (or the
    /// snapshot predates it): the HTTP arm stays inert.
    mcp_cloud_hostname: Option<String>,
    workspace: PathBuf,
    program: OsString,
    environment: Vec<(OsString, OsString)>,
    auth_selector: Option<IntegrationSelector>,
    provider_fragment: Option<ProviderFragment>,
    secrets: Vec<String>,
    credential: String,
    instance_dir: String,
    store_root: String,
    resume: Option<ResumeSubject>,
    swarm_executable: PathBuf,
    swarm_data_dir: PathBuf,
}

/// Outcome of start admission: a replayed start response, or a parsed
/// request ready to launch.
enum StartAdmission {
    Replay(Result<Value>),
    New(Box<ParsedStart>),
}

/// Outcome of start phase 4: either a created session ready for the
/// readiness publish, or a pre-session failure that settled
/// as a definite failed start (no native session or prompt was ever
/// dispatched, so the daemon settles failed, never unknown). The session is
/// boxed: it dwarfs the failed value, and the size gap would trip the
/// large-variant lint.
enum CreateOutcome {
    Created(Box<AgentSession>, SessionInfo, Value),
    ProvenFailed(Value),
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

fn is_preemptible_read(method: &str) -> bool {
    matches!(method, "agent.snapshot" | "agent.models")
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
    let mut native_endpoint_retired = false;
    let mut native_retirement = Ok(());
    let mut pending_request = None;
    loop {
        let request = if let Some(request) = pending_request.take() {
            Some(request)
        } else {
            tokio::select! {
                biased;
                () = async {
                    match plugin.session.as_mut() {
                        Some(session) => session.server.wait_for_process_exit().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(session) = plugin.session.as_mut() {
                        let deadline = Instant::now() + RPC_TIMEOUT;
                        native_retirement = timeout_at(deadline, async {
                            session.events.drain_native().await?;
                            session.events.close().await;
                            Ok(())
                        }).await.unwrap_or_else(|_| Err(Error::new("unknown", "OpenCode native event tail settlement timed out")));
                        session.server.finish_diagnostics(deadline).await;
                    }
                    native_endpoint_retired = true;
                    break;
                }
                request = requests.recv() => request,
            }
        };
        let Some(request) = request else {
            break;
        };
        let is_shutdown = request.method == "shutdown";
        let result = if is_preemptible_read(&request.method) {
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
                                format!("{} read was preempted by a host request", request.method),
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
            "OpenCode plugin input closed before shutdown completed",
        ))
    }
}

impl OpenCodePlugin {
    async fn dispatch(&mut self, request: &RpcRequest, client: &PeerClient) -> Result<Value> {
        let result = match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "model.list" => self.model_list(&request.params).await,
            "quota.read" => crate::quota::read(&request.params).await,
            "agent.start" => self.start(&request.params, client).await,
            "agent.command" => self.command(&request.params).await,
            "agent.models" => self.session_models(&request.params).await,
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
                    // The attached error names the plugin method and the
                    // value's own reason: an empty harness tail stays honest
                    // (empty text), but the outcome still says which plugin
                    // method produced it and with what reason.
                    let context = Error::new(
                        "unknown",
                        value
                            .get("reason")
                            .and_then(Value::as_str)
                            .filter(|reason| !reason.trim().is_empty())
                            .map(str::to_owned)
                            .unwrap_or_else(|| {
                                format!("OpenCode {} produced an uncertain outcome", request.method)
                            }),
                    )
                    .with_where(format!("plugin.opencode.command.{}", request.method));
                    let diagnostics =
                        serde_json::to_value(session.server.diagnostic_error(context)).map_err(
                            |_| Error::new("internal", "OpenCode diagnostics could not be encoded"),
                        )?;
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
            outcome: SettledStartFailure::Uncertain(error.clone()),
        });
        error
    }

    /// One read-only `harness.models` phase: `models --standalone` ids or
    /// `api model.list --standalone` metadata. The daemon gives each phase
    /// its own reader Job and proves family closure before overlaying results.
    async fn model_list(&self, raw: &Value) -> Result<Value> {
        ensure_dedicated_reader(
            self.session.is_some() || self.failed_start.is_some(),
            "OpenCode",
            "model.list",
        )?;
        let params: ModelListParams = decode(raw, "OpenCode model.list params are invalid")?;
        let snapshot = &params.profile;
        ensure_reader_harness(snapshot, "opencode", "OpenCode", "model.list")?;
        let program = snapshot
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|cmd| !cmd.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "OpenCode model.list requires a discovered native cmd",
                )
            })?;
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("OpenCode"))?;
        let base =
            Path::new(&initialized.instance_dir).join(eliot_plugin::protocol::MODEL_READER_DIR);
        if !base.is_dir() {
            return Err(Error::new(
                "native_spawn",
                "OpenCode model reader has no host-owned directory",
            ));
        }
        let workspace = params
            .workspace
            .as_deref()
            .map_or(base.as_path(), Path::new);
        let program = OsStr::new(program);
        let TargetEnvironment {
            variables: environment,
            ..
        } = target_environment(
            params.target_environment.as_ref(),
            snapshot.get("pass_env"),
            "OpenCode",
        )?;
        let profile = snapshot
            .as_object()
            .expect("harness already proved the profile is an object");
        let model = optional_profile_text(profile, "model", "OpenCode")?;
        let fragment = start_provider_fragment(profile)?;
        if let Some(fragment) = fragment.as_ref() {
            validate_fragment_environment(fragment, &environment)?;
        }
        let until = Instant::now() + RPC_TIMEOUT;
        let config_home = base.join("config");
        let data_home = base.join("data");
        let auth = if fragment.is_some() || profile.get("auth_integration").is_some() {
            start_auth_selector(profile, fragment.is_some())?
        } else {
            None
        };
        let isolated = auth.is_some() || fragment.is_some();
        if isolated {
            let empty_config = base.join("auth-config");
            let config_dir = config_home.join("opencode");
            for dir in [&empty_config, &config_dir, &data_home] {
                std::fs::create_dir_all(dir).map_err(|_| {
                    Error::new(
                        "native_spawn",
                        "OpenCode model reader could not prepare its private stores",
                    )
                })?;
            }
            std::fs::write(
                config_dir.join("opencode.jsonc"),
                provider_config(model.as_deref(), fragment.as_ref()).to_string(),
            )
            .map_err(|_| {
                Error::new(
                    "native_spawn",
                    "OpenCode model reader could not write its provider config",
                )
            })?;
            if let Some(selector) = auth.as_ref() {
                transfer_selected_integration(
                    &AuthTransfer {
                        program,
                        selector,
                        workspace,
                        export_environment: &[],
                        canonical_config_home: &empty_config,
                        import_environment: &environment,
                        isolated_config_home: &config_home,
                        isolated_data_home: &data_home,
                        reuse_existing: false,
                    },
                    until,
                )
                .await?;
            }
        }
        let context = ReaderContext {
            workspace,
            environment: &environment,
            xdg: isolated.then_some((config_home.as_path(), data_home.as_path())),
        };
        let models: Vec<Value> = match params.part {
            ModelListPart::Models => run_models(program, until, &context)
                .await?
                .into_iter()
                .map(|id| json!({"id": id, "default": false}))
                .collect(),
            ModelListPart::Variants => run_model_metadata(program, until, &context)
                .await?
                .into_iter()
                .map(|entry| {
                    json!({"id": entry.id, "default": false, "efforts": entry.variants, "display_name": entry.display_name})
                })
                .collect(),
        };
        let provenance = if isolated { "preview" } else { "canonical" };
        Ok(json!({"models": models, "default": Value::Null, "provenance": provenance}))
    }

    /// Retain a proven-no-effect start failure and build its failed-state
    /// result: no native session was created and no prompt was sent, so the
    /// launch is definitely failed, never unknown. The reason names the
    /// failed check in short non-secret words; it is persisted in the daemon
    /// outcome, so it must never carry endpoints, ids, values, or secrets.
    fn retain_proven_no_effect_start(
        &mut self,
        params: &AgentStartParams,
        phase: StartPhase,
        error: Error,
    ) -> Value {
        let error =
            eliot_plugin::session::classify_start_failure(error, "opencode", phase.as_str());
        let value = eliot_plugin::session::failed_start_value(params, error);
        self.failed_start = Some(FailedStart {
            agent_id: params.agent_id.clone(),
            generation: params.generation,
            command_id: params.command_id.clone(),
            outcome: SettledStartFailure::ProvenNoEffect(value.clone()),
        });
        value
    }

    /// Convert one pre-session failure into its retained failed-state result.
    /// By construction the failure happened before any native session or
    /// prompt could exist and carries no uncertain effect (the caller keeps
    /// uncertain auth imports on the error path), so the launch is definitely
    /// failed: the daemon settles failed, retires the plugin, and releases
    /// capacity on proven exit instead of pinning an unknown Agent that
    /// blocks drain.
    fn proven_no_effect_start(
        &mut self,
        params: &AgentStartParams,
        failure: PhaseFailure,
    ) -> Value {
        debug_assert!(start_failure_is_proven_no_effect(
            failure.phase,
            &failure.error.code
        ));
        let reason = presession_reason(failure.phase, &failure.error.code);
        self.retain_proven_no_effect_start(
            params,
            failure.phase,
            (*failure.error).with_message(reason),
        )
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        let _timing = crate::native::startup::Step::new("start_total");
        let parsed = match self.parse_start_request(raw)? {
            StartAdmission::Replay(outcome) => return outcome,
            StartAdmission::New(parsed) => parsed,
        };
        // One envelope for the whole dispatch: every pre-session phase
        // shares it, so a loaded cold start gets room without outliving the
        // daemon agent.start budget (which would settle the launch unknown
        // while this plugin is still working).
        let envelope = Instant::now() + START_ENVELOPE;
        debug_assert!(START_ENVELOPE + START_SETTLE_RESERVE <= DAEMON_START_TIMEOUT);
        let session = match Self::launch_start_session(peer, &parsed, envelope).await {
            Ok(session) => session,
            // Launch runs before any native session or prompt exists, so a
            // failure here is a definite failed start, never unknown, with
            // one exception: an uncertain auth import (code `unknown`) means
            // the isolated credential store may already have been written and
            // reconciliation failed, so it stays unknown per the
            // authentication contract and replays as an error.
            Err(failure) => {
                if start_failure_is_proven_no_effect(failure.phase, &failure.error.code) {
                    return Ok(self.proven_no_effect_start(&parsed.params, failure));
                }
                return Err(self.retain_failed_start(&parsed.params, *failure.error));
            }
        };
        let session = match self
            .register_start_session(peer, &parsed, session, envelope)
            .await
        {
            Ok(session) => session,
            // Registration runs before any native session or prompt exists.
            Err(failure) => return Ok(self.proven_no_effect_start(&parsed.params, failure)),
        };
        if parsed.resume.is_some() {
            return self.resume_start_session(*parsed, session, envelope).await;
        }
        match self
            .create_start_session(&parsed, session, envelope)
            .await?
        {
            CreateOutcome::Created(session, created, effective) => {
                self.ready_start_session(*parsed, *session, created, effective)
                    .await
            }
            // Pre-session failure: no native session or prompt was ever
            // dispatched, so this failed-state value (not an error) settles
            // the launch failed and lets the daemon retire the plugin and
            // release capacity on proven exit.
            CreateOutcome::ProvenFailed(value) => Ok(value),
        }
    }

    /// Start phase 1, admission: decode, replay guards, and frozen-profile
    /// validation. Failures here retain nothing: a corrected start may retry.
    fn parse_start_request(&self, raw: &Value) -> Result<StartAdmission> {
        let _timing = crate::native::startup::Step::new("frozen_admission");
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
        let requested_mcp_policy = policy_from_snapshot(profile)?;
        let native_subagents = profile
            .get("native_subagents")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mcp_cloud_hostname = eliot_core::parse_snapshot_cloud_hostname(profile);
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
        let (requested_model, model_ref, requested_effort) = start_model_ref(profile)?;
        // A resume that inherited a `configure` overlay carries its
        // effective model and effort as the native realignment target
        // (the effort selects the documented `Model.Ref.variant`).
        let (override_model, override_model_ref, override_effort) = start_config_override(&params)?;
        let requested_agent = optional_profile_text(profile, "agent", "OpenCode")?;
        // A fragment profile always carries an explicit model: a fragment
        // without a model fails closed instead of falling back to the native
        // default.
        let provider_fragment = start_provider_fragment(profile)?;
        let auth_selector = start_auth_selector(profile, provider_fragment.is_some())?;
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
        // Agent-scoped swarm MCP identity: the Agent's owned Eliot data root
        // for the MCP `--data-dir` argument (mirrors the Codex
        // `args_with_agent_mcp` guard), plus the active Swarm executable for
        // the server command (mirrors the Claude `swarm_binary` readback).
        // The token itself is never rendered: the MCP child inherits this
        // environment from the serve process, and `mcp --require-agent`
        // fails closed without it.
        let swarm_data_dir = environment
            .iter()
            .find(|(name, _)| name.to_string_lossy().eq_ignore_ascii_case("ELIOT_HOME"))
            .map(|(_, value)| PathBuf::from(value))
            .filter(|path| path.is_absolute() && path.is_dir())
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "OpenCode Agent MCP requires an absolute owned Eliot data directory",
                )
            })?;
        let swarm_executable = std::env::current_exe().map_err(|_| {
            Error::new(
                "platform_io",
                "active Swarm executable could not be resolved for the OpenCode Agent MCP",
            )
        })?;
        Ok(StartAdmission::New(Box::new(ParsedStart {
            params,
            native_subagents,
            requested_model,
            model_ref,
            override_model,
            override_model_ref,
            requested_effort,
            override_effort,
            requested_agent,
            requested_mcp_policy,
            mcp_cloud_hostname,
            workspace,
            program,
            environment,
            auth_selector,
            provider_fragment,
            secrets,
            credential: initialized.credential.clone(),
            instance_dir: initialized.instance_dir.clone(),
            store_root: initialized.store_root.clone(),
            resume,
            swarm_executable,
            swarm_data_dir,
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
                return Ok(Some(StartAdmission::Replay(failed.outcome.replay())));
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
    /// stream, and assemble the session. The caller retains any error. A
    /// spawn failure carries its provenance: an uncertain auth import (code
    /// `unknown`) settles unknown, definite refusals settle failed.
    /// The cloud path carries the selected native integration; the fragment
    /// path carries the native provider fragment and no canonical credential.
    async fn launch_start_session(
        peer: &PeerClient,
        parsed: &ParsedStart,
        envelope: Instant,
    ) -> std::result::Result<AgentSession, PhaseFailure> {
        let mut server = match OwnedServer::spawn(
            ServerLaunch {
                program: parsed.program.clone(),
                workspace: parsed.workspace.clone(),
                instance_dir: PathBuf::from(&parsed.instance_dir),
                store_root: PathBuf::from(&parsed.store_root),
                agent_id: parsed.params.agent_id.clone(),
                environment: parsed.environment.clone(),
                model: parsed.requested_model.clone(),
                auth: parsed.auth_selector.clone().map(|selector| AuthSetup {
                    selector,
                    reuse_existing: parsed.resume.is_some(),
                }),
                provider_fragment: parsed.provider_fragment.clone(),
                swarm_executable: parsed.swarm_executable.clone(),
                swarm_data_dir: parsed.swarm_data_dir.clone(),
                native_subagents: parsed.native_subagents.clone(),
            },
            &parsed.secrets,
            envelope,
        )
        .await
        {
            Ok(server) => server,
            // The spawn cleans up its own child on failure; no diagnostics
            // exist yet because no server was retained. The caller settles a
            // definite failed start, except an uncertain auth import (code
            // `unknown`): the isolated credential store may have been
            // written, so that stays unknown.
            Err(error) => {
                return Err(PhaseFailure {
                    phase: StartPhase::Spawn,
                    error: Box::new(error),
                });
            }
        };
        let http = server.http().clone();
        // Subscribe before session create/input: the stream must observe the
        // session's whole durable sequence starting at `session.created`.
        let subscribe_timing = crate::native::startup::Step::new("subscribe");
        let (frames, stream) = match http.subscribe(phase_deadline(envelope, RPC_TIMEOUT)).await {
            Ok(subscribed) => subscribed,
            Err(error) => {
                let _ = server
                    .terminate(true, phase_deadline(envelope, START_CLEANUP_BUDGET))
                    .await;
                return Err(PhaseFailure {
                    phase: StartPhase::Subscribe,
                    error: Box::new(server.diagnostic_error(error)),
                });
            }
        };
        drop(subscribe_timing);
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
            stop_command_id: None,
            resources_proved_closed: false,
            stop_end_ack_proved: false,
        };

        Ok(session)
    }

    /// Start phase 3, registration: prove the owned server runs under this
    /// Agent generation. Registration runs before any native session exists,
    /// so a failure is a definite failed start once the owned server is
    /// ended below: the generation Job reaps anything the terminate left
    /// behind, and the daemon retires the plugin on the failed value instead
    /// of pinning an unknown Agent that blocks drain.
    async fn register_start_session(
        &mut self,
        peer: &PeerClient,
        parsed: &ParsedStart,
        mut session: AgentSession,
        envelope: Instant,
    ) -> std::result::Result<AgentSession, PhaseFailure> {
        let _timing = crate::native::startup::Step::new("process_registration");
        if let Err(error) = register_native_process(
            peer,
            &parsed.credential,
            &session.agent_id,
            session.generation,
            session.server.pid(),
            "OpenCode",
            "server",
            "session",
            phase_budget(envelope, RPC_TIMEOUT),
        )
        .await
        {
            close_start_events(&session, envelope).await;
            let _ = session
                .server
                .terminate(true, phase_deadline(envelope, START_CLEANUP_BUDGET))
                .await;
            let error = session.server.diagnostic_error(error);
            return Err(PhaseFailure {
                phase: StartPhase::Register,
                error: Box::new(error),
            });
        }
        Ok(session)
    }

    /// Start phase 4, create: open the native session and verify its
    /// effective model and directory against the frozen profile. A readback
    /// failure settles only after proving deletion of the created session;
    /// otherwise the exact session and server remain available as unknown.
    ///
    /// Fragment path: the native provider and catalogue verification runs
    /// BEFORE `POST /api/session` creates any native session — right here at
    /// the top, after the owned server is up but before the first native
    /// write. Nothing has been dispatched at that point, so every
    /// verification failure (mismatch or unreadable readback) settles as a
    /// definite failed start (`ProvenFailed`, retained for a corrected
    /// retry), never as an unknown that would pin the Agent, keep the plugin
    /// alive, and hold capacity. Unknown is reserved for failures after a
    /// session or prompt may have been admitted. An unreachable custom
    /// endpoint is not a start failure: the catalogue still lists the model,
    /// the start succeeds, and the native turn surfaces the refusal.
    async fn create_start_session(
        &mut self,
        parsed: &ParsedStart,
        mut session: AgentSession,
        envelope: Instant,
    ) -> Result<CreateOutcome> {
        let _timing = crate::native::startup::Step::new("session_create_verify");
        session.native_scope = opencode_scope(session.server.data_dir());
        // Pre-create verification: no native session or prompt exists yet.
        // A fragment profile proves its provider through the native
        // `GET /api/provider/:id` readback; the exact pair is proven below
        // through the live catalogue like every other profile.
        if let (Some(fragment), Some(requested)) = (
            parsed.provider_fragment.as_ref(),
            parsed.requested_model.as_deref(),
        ) {
            let provider_id = requested.split('/').next().unwrap_or("");
            if !fragment.contains_key(provider_id) {
                let failure = Error::new(
                    "profile_error",
                    "OpenCode provider fragment does not define the requested provider",
                );
                discard_failed_start(&mut session, failure.clone(), envelope).await;
                return Ok(CreateOutcome::ProvenFailed(
                    self.retain_proven_no_effect_start(&parsed.params, StartPhase::Verify, failure),
                ));
            }
            if let Err(error) = session
                .http
                .get_provider(provider_id, phase_deadline(envelope, RPC_TIMEOUT))
                .await
            {
                let failure = Error::new(
                    "profile_error",
                    format!("OpenCode provider readback failed ({})", error.message),
                );
                discard_failed_start(&mut session, failure.clone(), envelope).await;
                return Ok(CreateOutcome::ProvenFailed(
                    self.retain_proven_no_effect_start(&parsed.params, StartPhase::Verify, failure),
                ));
            }
        }
        // An explicit model must prove membership in the live catalogue
        // before the create below: `Session.create` publishes its input
        // model unchanged and only resolves availability lazily at
        // inference, so the create echo alone never proves the
        // provider/model exists. A requested effort additionally selects
        // the documented `Model.Ref.variant` on the create, so the
        // catalogue must also prove the variant belongs to that exact
        // model row. A miss or an unreadable catalogue is a definite
        // failed start, never unknown.
        if let Some(requested) = parsed.requested_model.as_deref() {
            let requested_variant = parsed
                .model_ref
                .as_ref()
                .and_then(|model_ref| model_ref.variant.as_deref());
            let _timing = crate::native::startup::Step::new("model_catalogue");
            let catalogue = match session
                .http
                .list_native_models(phase_deadline(envelope, RPC_TIMEOUT))
                .await
            {
                Ok(catalogue) => catalogue,
                Err(error) => {
                    let failure = Error::new(
                        "native_readback",
                        format!(
                            "OpenCode start catalogue readback failed ({})",
                            error.message
                        ),
                    );
                    discard_failed_start(&mut session, failure.clone(), envelope).await;
                    return Ok(CreateOutcome::ProvenFailed(
                        self.retain_proven_no_effect_start(
                            &parsed.params,
                            StartPhase::Verify,
                            failure,
                        ),
                    ));
                }
            };
            if !catalogue_contains(&catalogue, requested) {
                let failure = Error::new(
                    "profile_error",
                    format!("OpenCode model {requested} is not in the native catalogue"),
                );
                discard_failed_start(&mut session, failure.clone(), envelope).await;
                return Ok(CreateOutcome::ProvenFailed(
                    self.retain_proven_no_effect_start(&parsed.params, StartPhase::Verify, failure),
                ));
            }
            if let Some(variant) = requested_variant {
                let known = catalogue_variants(&catalogue, requested)
                    .is_some_and(|variants| variants.iter().any(|known| known == variant));
                if !known {
                    let failure = Error::new(
                        "profile_error",
                        format!("OpenCode effort {variant} is not a variant of model {requested}"),
                    );
                    discard_failed_start(&mut session, failure.clone(), envelope).await;
                    return Ok(CreateOutcome::ProvenFailed(
                        self.retain_proven_no_effect_start(
                            &parsed.params,
                            StartPhase::Verify,
                            failure,
                        ),
                    ));
                }
            }
        }
        // Workspace-inherited project MCP layers: the owned serve already
        // merged them, so an owner-identity entry there must refuse the start
        // before the native session exists. Every policy, whatever the entry
        // is named.
        if let Err(error) = refuse_owner_project_mcp(
            &parsed.workspace,
            &parsed.swarm_executable,
            parsed.mcp_cloud_hostname.as_deref(),
        ) {
            let failure = error.clone();
            discard_failed_start(&mut session, error, envelope).await;
            return Ok(CreateOutcome::ProvenFailed(
                self.retain_proven_no_effect_start(
                    &parsed.params,
                    StartPhase::CreateRefused,
                    failure,
                ),
            ));
        }
        let title = format!(
            "eliot-{}-g{}",
            parsed.params.agent_id, parsed.params.generation
        );
        let directory = parsed.workspace.to_string_lossy().into_owned();
        // `deny` profiles establish the subagent denial as the session's own
        // ruleset, where it outranks every agent/project rule; `allow`
        // profiles create the session unchanged.
        let deny_native_subagents = parsed.native_subagents.as_deref() == Some("deny");
        let deny_permissions = deny_native_subagents.then(Self::subagent_deny_permissions);
        let cutoff = create_cutoff(envelope);
        // The location default is only a preview. Look it up before create,
        // and only when there is spare time ahead of the create request's
        // full RPC budget. Do not spend verification or cleanup time on it.
        let mut native_default_model = None;
        if parsed.requested_model.is_none()
            && let Some(preview_cutoff) = cutoff.checked_sub(RPC_TIMEOUT)
            && Instant::now() < preview_cutoff
            && let Ok(Some(default)) = session
                .http
                .default_model(phase_deadline(
                    preview_cutoff,
                    std::time::Duration::from_secs(5),
                ))
                .await
            && let (Some(provider), Some(id)) = (default.provider_id, default.id)
        {
            let label = format!("{provider}/{id}");
            if split_catalogue_model(&label) == Some((provider.as_str(), id.as_str())) {
                native_default_model = Some(label);
            }
        }
        let create_timing = crate::native::startup::Step::new("session_create");
        let created = match session
            .http
            .create_session(
                &title,
                parsed.model_ref.as_ref(),
                &directory,
                deny_permissions.as_deref(),
                create_deadline(cutoff),
            )
            .await
        {
            Ok(created) => created,
            Err(HttpOutcome::Rejected { reason, .. }) => {
                // Authoritative refusal: the server proves no session was
                // admitted, so this settles as a definite failed start.
                discard_unstarted(&mut session, envelope).await;
                return Ok(CreateOutcome::ProvenFailed(self.proven_no_effect_start(
                    &parsed.params,
                    PhaseFailure {
                        phase: StartPhase::CreateRefused,
                        error: Box::new(Error::new("rejected", reason).with_origin(
                            FailureOrigin::Harness,
                            "native session/create refused without upstream evidence",
                        )),
                    },
                )));
            }
            Err(HttpOutcome::NotDispatched { reason }) => {
                // Provably never dispatched: no byte left the socket, so no
                // native session exists and this is a definite failed start.
                discard_unstarted(&mut session, envelope).await;
                return Ok(CreateOutcome::ProvenFailed(self.proven_no_effect_start(
                    &parsed.params,
                    PhaseFailure {
                        phase: StartPhase::CreateRefused,
                        error: Box::new(Error::new("native_not_dispatched", reason)),
                    },
                )));
            }
            Err(HttpOutcome::Unknown { reason }) => {
                self.session = Some(session);
                return Err(Error::new(
                    "unknown",
                    format!(
                        "OpenCode session/create may have changed native state; it will not be repeated ({reason})"
                    ),
                )
                .with_where("plugin.opencode.start.create")
                .with_origin(
                    FailureOrigin::Harness,
                    "native session/create outcome uncertain; native state may exist",
                ));
            }
        };
        drop(create_timing);
        session.session_id = Some(created.id.clone());
        // Effective readback before readiness: the create response states the
        // exact model and location; a mismatch prevents `session.ready`.
        let effective_model = created
            .model
            .as_ref()
            .map(|model| format!("{}/{}", model.provider_id, model.id));
        // An omitted request accepts the configured native default as
        // effective; an explicit request must match exactly. A requested
        // effort must echo as the native variant.
        let (model_verified, model_source) =
            launch_model_readback(parsed.requested_model.as_deref(), created.model.as_ref());
        let effective_variant = created
            .model
            .as_ref()
            .and_then(|model| model.variant.clone());
        let effort_verified = parsed.requested_effort.as_deref().is_none_or(|effort| {
            effective_variant
                .as_deref()
                .is_some_and(|variant| variant == effort)
        });
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
        let mut effective = json!({
            "model": effective_model.clone(),
            "model_verified": model_verified,
            "model_source": model_source,
            "model_variant": effective_variant.clone(),
            "effort": effective_variant,
            "effort_verified": effort_verified,
            "cwd": created.location.as_ref().map(|location| location.directory.clone()),
        });
        if created.model.is_none()
            && let Some(default) = native_default_model
        {
            effective["native_default_model"] = json!(default);
        }
        // Post-create failures below keep their historical settlement: the
        // create may have admitted native state, so they stay errors (unknown
        // where ambiguous), never the pre-create failed value.
        // Agent-scoped MCP readback after the session exists: names plus
        // the connected verdict for the `eliot-swarm` entry. Observation
        // stays read-only and polled; the inventory gate below refuses
        // the start on an unreadable inventory or an unexpected or
        // owner-identity server.
        let mcp_timing = crate::native::startup::Step::new("mcp_inventory");
        let (mcp_servers, mcp_servers_verified, mcp_inventory) =
            observe_mcp_servers(&session.http, phase_deadline(cutoff, RPC_TIMEOUT)).await;
        drop(mcp_timing);
        if let Err(error) =
            gate_mcp_inventory(mcp_inventory.as_deref(), parsed.requested_mcp_policy)
        {
            return Err(self
                .fail_created_start(
                    &parsed.params,
                    session,
                    &created.id,
                    Error::new("profile_error", *error.message),
                    envelope,
                )
                .await);
        }
        // Effective-configuration proofs: the inventory gate above only sees
        // entry names and statuses. Managed, system and global layers merge
        // natively above the project files the pre-create scan sees
        // (managed settings load last and override everything), so the
        // winning documents must prove two things. First, no effective
        // `mcp.servers` entry carries the owner identity under ANY name: a
        // differently named owner transport in a managed layer escapes both
        // the file scan and the reserved-name inventory gate under
        // `inherit`. Second, a serving (`connected` or `pending`) managed
        // entry must still run the exact generated transport, since any
        // inherited layer can replace the transport under that name. The
        // gate above already refused an unreadable inventory, so the
        // winning documents always prove both here; an unreadable
        // effective config proves neither and fails the start closed with
        // the created session killed, exactly like the gate above.
        {
            let expected_command = managed_mcp_command(parsed);
            let proof = match session
                .http
                .get_config(phase_deadline(cutoff, RPC_TIMEOUT))
                .await
            {
                Ok(documents) => refuse_effective_owner_mcp(
                    &documents,
                    parsed.swarm_executable.to_str(),
                    parsed.mcp_cloud_hostname.as_deref(),
                )
                .and_then(|()| {
                    if mcp_inventory.as_deref().is_some_and(|servers| {
                        servers
                            .iter()
                            .any(|server| server.is_serving(SWARM_MCP_SERVER_NAME))
                    }) {
                        verify_managed_mcp_transport(&documents, &expected_command)
                    } else {
                        Ok(())
                    }
                }),
                Err(error) => Err(Error::new(
                    "profile_error",
                    format!(
                        "OpenCode managed MCP transport readback failed ({})",
                        error.message
                    ),
                )),
            };
            if let Err(error) = proof {
                return Err(self
                    .fail_created_start(
                        &parsed.params,
                        session,
                        &created.id,
                        Error::new("profile_error", *error.message),
                        envelope,
                    )
                    .await);
            }
        }
        effective["mcp_servers"] = mcp_servers;
        effective["mcp_servers_verified"] = json!(mcp_servers_verified);
        effective["mcp_policy"] = json!(parsed.requested_mcp_policy.as_str());
        // A project `subagent` allow would beat the isolated global config
        // under last-match-wins evaluation, so `deny` profiles prove the
        // stored session carries the wildcard deny as the last rule of its
        // entire ruleset with a fresh readback on this exact session (the
        // installed evaluator matches actions with wildcard patterns, so
        // any later rule — including a `*` allow — can beat the deny). The
        // readback runs against the pre-create cutoff, so the delete below
        // never inherits an expired deadline. A missing or unreadable denial
        // goes to cleanup before hooks bind: a proved removal fails the
        // start (an installed version that cannot store the ruleset can
        // never serve a `deny` profile), while an unproved removal keeps the
        // exact native identity in memory as unknown with the live server.
        // The same readback sets the verification flag.
        let subagents_verified = if deny_native_subagents {
            match Self::readback_subagent_deny(
                &session.http,
                &created.id,
                phase_deadline(cutoff, RPC_TIMEOUT),
            )
            .await
            {
                Ok(true) => true,
                Ok(false) => {
                    return Err(self
                        .fail_created_start(
                            &parsed.params,
                            session,
                            &created.id,
                            Error::new("profile_error", "OpenCode session deny ruleset is missing"),
                            envelope,
                        )
                        .await);
                }
                Err(error) => {
                    return Err(self
                        .fail_created_start(
                            &parsed.params,
                            session,
                            &created.id,
                            Error::new("native_readback", *error.message),
                            envelope,
                        )
                        .await);
                }
            }
        } else {
            true
        };
        effective["native_subagents"] = json!(
            parsed
                .native_subagents
                .clone()
                .unwrap_or_else(|| "allow".to_owned())
        );
        effective["native_subagents_verified"] = json!(subagents_verified);
        let hook_timing = crate::native::startup::Step::new("context_bind");
        let context_bound = bind_context_hooks(
            &session.http,
            &created.id,
            phase_deadline(cutoff, RPC_TIMEOUT),
        )
        .await;
        drop(hook_timing);
        let effective = if context_bound {
            with_context_points(
                effective,
                &[InjectPoint::NextInput, InjectPoint::TurnSafePoint],
            )
        } else {
            with_context_points(effective, &[InjectPoint::NextInput])
        };
        if (parsed.requested_model.is_some() && !model_verified) || !effort_verified || !location_ok
        {
            return Err(self
                .fail_created_start(
                    &parsed.params,
                    session,
                    &created.id,
                    Error::new(
                        "profile_error",
                        "OpenCode effective model, effort or session directory did not match the frozen profile",
                    ),
                    envelope,
                )
                .await);
        }
        Ok(CreateOutcome::Created(
            Box::new(session),
            created,
            effective,
        ))
    }

    /// Resume: reattach to the exact retained native session in this new
    /// generation. No prompt is ever replayed: the new server serves the
    /// same per-Agent store, the preflight proves the retained session
    /// exists, then proves the realignment target in the live catalogue
    /// and the effective MCP proof before checking the native pending legs.
    /// The model/agent realign to the frozen profile with effective readback,
    /// and readiness adopts the live edge.
    async fn resume_start_session(
        &mut self,
        parsed: ParsedStart,
        mut session: AgentSession,
        envelope: Instant,
    ) -> Result<Value> {
        let resume = parsed.resume.as_ref().expect("resume subject admitted");
        session.native_scope = opencode_scope(session.server.data_dir());
        if session.native_scope != resume.native_scope {
            // Wrong store: this generation created nothing and the retained
            // session, if any, is untouched, so the resume is definitely
            // failed, never unknown.
            discard_unstarted(&mut session, envelope).await;
            return Ok(self.proven_no_effect_start(
                &parsed.params,
                PhaseFailure {
                    phase: StartPhase::ResumeRefused,
                    error: Box::new(Error::new(
                        "profile_error",
                        "OpenCode resume subject is outside the authenticated native scope",
                    )),
                },
            ));
        }
        // The resumed serve merges the current project layer again, so the
        // same owner-identity scan runs before the retained session is
        // touched: a project entry added since launch refuses the resume.
        if let Err(error) = refuse_owner_project_mcp(
            &parsed.workspace,
            &parsed.swarm_executable,
            parsed.mcp_cloud_hostname.as_deref(),
        ) {
            return Err(self.fail_resume(&parsed, session, error, envelope).await);
        }
        let deadline = phase_deadline(envelope, RPC_TIMEOUT);
        let retained = match Self::resume_read_retained(&session, &resume.native_ref, deadline)
            .await
        {
            Ok(retained) => retained,
            // Proven absent: the retained session no longer exists, so
            // there is nothing to resume and this generation created
            // nothing. Any other read failure stays unknown: the retained
            // session may still exist.
            Err(error) if error.code == "not_found" => {
                discard_unstarted(&mut session, envelope).await;
                return Ok(self.proven_no_effect_start(
                    &parsed.params,
                    PhaseFailure {
                        phase: StartPhase::ResumeRefused,
                        error: Box::new(error),
                    },
                ));
            }
            Err(error) => return Err(self.fail_resume(&parsed, session, error, envelope).await),
        };
        session.session_id = Some(retained.id.clone());
        // Catalogue membership proves before any native interrupt or
        // setter: the quiesce below aborts a running retained execution,
        // so a catalogue miss must fail here with proven no effect
        // instead of aborting that turn and then failing.
        if let Err(error) =
            Self::resume_validate_catalogue(&parsed, &session, &retained, deadline).await
        {
            discard_unstarted(&mut session, envelope).await;
            return Ok(self.proven_no_effect_start(
                &parsed.params,
                PhaseFailure {
                    phase: StartPhase::ResumeRefused,
                    error: Box::new(error),
                },
            ));
        }
        // Effective MCP proof before the quiesce below can abort a running
        // retained execution: the settled inventory is gated and the
        // winning effective documents must carry no owner transport (plus
        // the exact managed transport while the Agent-scoped entry serves),
        // exactly like the create path. Nothing has been written yet, so a
        // miss or an unreadable readback fails here with proven no effect
        // instead of aborting that turn and then failing.
        let (mcp_servers, mcp_servers_verified) =
            match Self::resume_prove_mcp(&parsed, &session, deadline).await {
                Ok(proof) => proof,
                Err(error) => {
                    discard_unstarted(&mut session, envelope).await;
                    return Ok(self.proven_no_effect_start(
                        &parsed.params,
                        PhaseFailure {
                            phase: StartPhase::ResumeRefused,
                            error: Box::new(error),
                        },
                    ));
                }
            };
        if let Err(error) = Self::resume_verify_idle(&session, &retained.id, deadline).await {
            return Err(self.stash_resume_unknown(session, *error.message).await);
        }
        let effective = match Self::resume_configure(
            &parsed,
            &session,
            &retained.id,
            mcp_servers,
            mcp_servers_verified,
            deadline,
        )
        .await
        {
            Ok(effective) => effective,
            Err(error) if error.code == "unknown" => {
                return Err(self.stash_resume_unknown(session, *error.message).await);
            }
            Err(error) => return Err(self.fail_resume(&parsed, session, error, envelope).await),
        };
        let context_bound = bind_context_hooks(&session.http, &retained.id, deadline).await;
        let effective = if context_bound {
            with_context_points(
                effective,
                &[InjectPoint::NextInput, InjectPoint::TurnSafePoint],
            )
        } else {
            with_context_points(effective, &[InjectPoint::NextInput])
        };
        if let Err(error) = session
            .events
            .resume_ready(
                session.native_scope.clone(),
                retained.id.clone(),
                effective.clone(),
            )
            .await
        {
            return Err(self.stash_resume_unknown(session, *error.message).await);
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
                "effort": parsed.requested_effort,
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

    /// Resume catalogue gate: an explicit realignment target proves live
    /// catalogue membership BEFORE the retained session is touched. The
    /// quiesce interrupt below aborts a running retained execution, so a
    /// catalogue miss must fail here with proven no effect instead of
    /// aborting that turn and then failing. No native interrupt, model
    /// switch or agent switch has been dispatched at this point, so a
    /// miss is a profile error with proven no effect; an unreadable
    /// catalogue is a pre-write readback failure, also with proven no
    /// effect (the create path settles an unreadable catalogue the same
    /// way). The `resume_configure` setters later only realign to the
    /// target proven here.
    async fn resume_validate_catalogue(
        parsed: &ParsedStart,
        session: &AgentSession,
        retained: &SessionInfo,
        deadline: Instant,
    ) -> Result<()> {
        // The realignment target is the inherited overlay's effective
        // model and effort when a resume carries them (the retained
        // session already runs them), else the frozen request. An omitted
        // request keeps the retained server-selected model and variant. A
        // carried model without an effort means the server default.
        let target_model = parsed
            .override_model
            .as_deref()
            .or(parsed.requested_model.as_deref());
        let target_variant = if parsed.override_model.is_some() {
            parsed.override_effort.as_deref()
        } else {
            parsed
                .override_effort
                .as_deref()
                .or(parsed.requested_effort.as_deref())
        };
        if target_model.is_none() && target_variant.is_none() {
            return Ok(());
        }
        let catalogue = session
            .http
            .list_native_models(deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "native_readback",
                    format!(
                        "OpenCode resume catalogue readback failed ({})",
                        error.message
                    ),
                )
            })?;
        // The membership lookup uses the native fields, never the joined
        // display string: see `catalogue_entry_for`.
        let target_ref = parsed
            .override_model_ref
            .as_ref()
            .or(parsed.model_ref.as_ref());
        if let Some(target) = target_model {
            let Some(target_ref) = target_ref else {
                return Err(Error::new(
                    "source_integrity",
                    "OpenCode resume model is missing its native ref",
                ));
            };
            if !catalogue_contains_for(&catalogue, &target_ref.provider_id, &target_ref.id) {
                return Err(Error::new(
                    "profile_error",
                    format!("OpenCode model {target} is not in the native catalogue"),
                ));
            }
        }
        if let Some(variant) = target_variant {
            // The native fields stay apart end to end: the validation ref
            // is the overlay/frozen ref when a model is carried, else the
            // retained native ref verbatim. Joining provider "a/b" plus
            // model "c" into "a/b/c" and re-splitting would look up
            // provider "a", model "b/c" instead.
            let validation_ref = parsed
                .override_model_ref
                .as_ref()
                .or(parsed.model_ref.as_ref())
                .or(retained.model.as_ref())
                .ok_or_else(|| {
                    Error::new(
                        "profile_error",
                        "OpenCode resume configure effort needs a session model",
                    )
                })?;
            let known =
                catalogue_variants_for(&catalogue, &validation_ref.provider_id, &validation_ref.id)
                    .is_some_and(|variants| variants.iter().any(|known| known == variant));
            if !known {
                // Display form only for the message; the lookup above uses
                // the native fields, never this string.
                let validation_model =
                    format!("{}/{}", validation_ref.provider_id, validation_ref.id);
                return Err(Error::new(
                    "profile_error",
                    format!(
                        "OpenCode effort {variant} is not a variant of model {validation_model}"
                    ),
                ));
            }
        }
        Ok(())
    }

    /// Resume MCP proof: gate the settled inventory and prove the winning
    /// effective documents carry no owner transport (plus the exact
    /// generated transport while the Agent-scoped entry serves), BEFORE
    /// the retained session is touched. The quiesce interrupt below aborts
    /// a running retained execution, so an unexpected or owner-identity
    /// server must fail here with proven no effect instead of aborting
    /// that turn and then failing. An unreadable inventory or effective
    /// config is a pre-write readback failure (`native_readback`), also
    /// with proven no effect; a proven owner hit, transport mismatch or
    /// unexpected serving server is a `profile_error`. Returns the
    /// observed inventory names plus the connected verdict for the
    /// effective record.
    async fn resume_prove_mcp(
        parsed: &ParsedStart,
        session: &AgentSession,
        deadline: Instant,
    ) -> Result<(Value, bool)> {
        // Same polled readback as the create path: the resumed server
        // re-spawns the Agent-scoped entry for this generation, and the
        // gate below refuses an unreadable inventory.
        let (mcp_servers, mcp_servers_verified, mcp_inventory) =
            observe_mcp_servers(&session.http, deadline).await;
        gate_mcp_inventory(mcp_inventory.as_deref(), parsed.requested_mcp_policy)?;
        // Same proofs as the create path: no effective `mcp.servers` entry
        // carries the owner identity under any name (managed layers merge
        // above the project files), and a serving (`connected` or
        // `pending`) managed entry still runs the exact generated
        // transport. Nothing has been written yet, so an unreadable
        // readback refuses here instead of staying unknown.
        let documents = session.http.get_config(deadline).await.map_err(|error| {
            Error::new(
                "native_readback",
                format!(
                    "OpenCode managed MCP transport readback failed ({})",
                    error.message
                ),
            )
        })?;
        refuse_effective_owner_mcp(
            &documents,
            parsed.swarm_executable.to_str(),
            parsed.mcp_cloud_hostname.as_deref(),
        )?;
        if mcp_inventory.as_deref().is_some_and(|servers| {
            servers
                .iter()
                .any(|server| server.is_serving(SWARM_MCP_SERVER_NAME))
        }) {
            verify_managed_mcp_transport(&documents, &managed_mcp_command(parsed))?;
        }
        Ok((mcp_servers, mcp_servers_verified))
    }

    /// Resume configure: realign the retained session's model and agent
    /// to the frozen profile, then prove the effective values with a fresh
    /// `session.get`. The location must be the workspace; the requested
    /// agent must be installed (the switch validates nothing itself). Live
    /// catalogue membership was already proven by the pre-quiesce
    /// `resume_validate_catalogue` gate, and the effective MCP proof by
    /// the pre-quiesce `resume_prove_mcp` gate, so no catalogue, inventory
    /// or effective-config read happens here after the interrupt: the
    /// caller threads that proof through as the `mcp_servers` pair.
    async fn resume_configure(
        parsed: &ParsedStart,
        session: &AgentSession,
        session_id: &str,
        mcp_servers: Value,
        mcp_servers_verified: bool,
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
        // `deny` profiles prove the retained session still carries the
        // wildcard deny as the last rule of its entire ruleset: a location
        // reload never clears stored session rules, but a session created
        // before the launch path set them is repaired here by preserving the
        // stored rules and appending the deny as the final entry, then
        // re-read. The verification flag below comes from the exact
        // session's final readback, so an already-correct retained session
        // reports true without a repair. An unrepairable, unreadable or
        // contradicted denial fails the resume: the session must not run
        // without it.
        let deny_native_subagents = parsed.native_subagents.as_deref() == Some("deny");
        if deny_native_subagents && !OpenCodeHttp::session_subagent_deny_verified(&current) {
            let existing = current.permissions.clone().unwrap_or_default();
            Self::resume_repair_subagent_deny(&session.http, session_id, &existing, deadline)
                .await?;
            let repaired = session
                .http
                .get_session(session_id, deadline)
                .await
                .map_err(|error| {
                    Error::new(
                        "unknown",
                        format!(
                            "OpenCode resume subagent denial readback is unresolved ({})",
                            error.message
                        ),
                    )
                })?;
            if !OpenCodeHttp::session_subagent_deny_verified(&repaired) {
                return Err(Error::new(
                    "unknown",
                    "OpenCode retained session contradicts the applied subagent denial",
                ));
            }
        }
        // The realignment target is the inherited overlay's effective
        // model and effort when a resume carries them (the retained
        // session already runs them), else the frozen request. An omitted
        // request keeps the retained server-selected model and variant. A
        // carried model without an effort means the server default.
        let target_model = parsed
            .override_model
            .as_deref()
            .or(parsed.requested_model.as_deref());
        let target_variant = if parsed.override_model.is_some() {
            parsed.override_effort.as_deref()
        } else {
            parsed
                .override_effort
                .as_deref()
                .or(parsed.requested_effort.as_deref())
        };
        let target_ref = parsed
            .override_model_ref
            .as_ref()
            .or(parsed.model_ref.as_ref());
        // Membership was proven by the pre-quiesce catalogue gate
        // (`resume_validate_catalogue` runs before any interrupt or
        // setter): the create echo is not membership proof, so neither is
        // the resume echo, and re-checking here would run after the
        // interrupt. The setters below plus the `session.get` effective
        // readback only realign to that already-proven target.
        let model_needed = target_model.is_some_and(|target| {
            current
                .model
                .as_ref()
                .is_none_or(|model| !model_ref_is(target, model))
        });
        let variant_needed = target_variant.is_some_and(|target| {
            current
                .model
                .as_ref()
                .and_then(|model| model.variant.as_deref())
                != Some(target)
        });
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
        if model_needed || variant_needed {
            let mut model_ref = match target_ref.cloned() {
                Some(model_ref) => model_ref,
                // An effort-only overlay on a session the server started
                // without reporting a model keeps the retained model and
                // only selects the variant.
                None => current.model.clone().ok_or_else(|| {
                    Error::new(
                        "source_integrity",
                        "OpenCode resume needs a model ref for its retained target model",
                    )
                })?,
            };
            model_ref.variant = target_variant.map(str::to_owned);
            Self::resume_switch(
                session
                    .http
                    .switch_model(session_id, &model_ref, deadline)
                    .await,
                "model",
            )?;
        }
        // The single shared echo verification proves the session runs
        // the realignment target (overlay or frozen), never a reverted
        // value. An omitted request keeps the server default and only
        // re-reads.
        let effective = match target_ref {
            Some(target) => {
                Self::readback_session_model(
                    &session.http,
                    session_id,
                    target,
                    target_variant,
                    deadline,
                    "resume configure",
                )
                .await?
            }
            None => session
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
                })?,
        };
        let model_verified = true;
        let agent_verified = parsed
            .requested_agent
            .as_ref()
            .is_none_or(|agent| effective.agent.as_ref() == Some(agent));
        if !agent_verified {
            return Err(Error::new(
                "unknown",
                "OpenCode effective agent contradicts the applied configuration",
            ));
        }
        let effective_variant = effective
            .model
            .as_ref()
            .and_then(|model| model.variant.clone());
        let effort_verified = target_variant.is_none_or(|target| {
            effective_variant
                .as_deref()
                .is_some_and(|variant| variant == target)
        });
        if target_variant.is_some() && !effort_verified {
            return Err(Error::new(
                "unknown",
                "OpenCode effective effort contradicts the applied configuration",
            ));
        }
        // The verification flag comes from this exact session's final
        // readback: the switches above never clear stored permissions, so a
        // repaired or already-correct denial still verifies here, and a
        // denial the switches somehow lost fails closed before readiness.
        let subagents_verified = if deny_native_subagents {
            let verified = OpenCodeHttp::session_subagent_deny_verified(&effective);
            if !verified {
                return Err(Error::new(
                    "unknown",
                    "OpenCode retained session contradicts the applied subagent denial",
                ));
            }
            true
        } else {
            true
        };
        let mut effective_value = json!({
            "model": effective.model.as_ref().map(|model| format!("{}/{}", model.provider_id, model.id)),
            "model_verified": model_verified,
            "model_variant": effective_variant.clone(),
            "effort": effective_variant,
            "effort_verified": effort_verified,
            "agent": effective.agent.clone(),
            "agent_verified": agent_verified,
            "cwd": effective.location.as_ref().map(|location| location.directory.clone()),
        });
        effective_value["native_subagents"] = json!(
            parsed
                .native_subagents
                .clone()
                .unwrap_or_else(|| "allow".to_owned())
        );
        effective_value["native_subagents_verified"] = json!(subagents_verified);
        // Fragment path: the regenerated server config must still carry the
        // frozen provider. The live catalogue proves the retained pair; a
        // miss fails the resume for a corrected retry.
        if let Some(fragment) = parsed.provider_fragment.as_ref()
            && let Some(model) = effective.model.as_ref()
        {
            let provider_id = model.provider_id.clone();
            if !fragment.contains_key(&provider_id) {
                return Err(Error::new(
                    "profile_error",
                    "OpenCode provider fragment does not define the retained provider",
                ));
            }
        }
        // The effective MCP proof ran pre-quiesce (`resume_prove_mcp`): no
        // inventory or effective-config read happens here after the
        // interrupt or setters, only the proven pair joins the record.
        effective_value["mcp_servers"] = mcp_servers;
        effective_value["mcp_servers_verified"] = json!(mcp_servers_verified);
        effective_value["mcp_policy"] = json!(parsed.requested_mcp_policy.as_str());
        Ok(effective_value)
    }

    /// Re-read the session and prove it runs `target`: the single echo
    /// verification behind every verified model switch. Resume realign
    /// and mid-run configure share it, so exactly one place turns a switch
    /// into verified effective state; a lost readback or an echo that
    /// contradicts the target is `unknown` (the switch may have applied
    /// without echoing). `context` names the caller (`resume configure`,
    /// `configure`). The switch dispatch itself stays per-caller: resume
    /// maps every write failure to `unknown` (an earlier setter in the
    /// same realign may have applied), while configure maps a proven
    /// no-effect refusal to its `rejected` value. The target is the native
    /// ref, never a joined string: re-splitting a `provider/model` display
    /// form would change the target when the provider ID contains a slash.
    async fn readback_session_model(
        http: &OpenCodeHttp,
        session_id: &str,
        target: &ModelRef,
        target_variant: Option<&str>,
        deadline: Instant,
        context: &str,
    ) -> Result<SessionInfo> {
        let echoed = http
            .get_session(session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode {context} readback is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        if echoed
            .model
            .as_ref()
            .is_none_or(|model| !session_model_is(target, model))
        {
            return Err(Error::new(
                "unknown",
                format!("OpenCode {context} model contradicts the applied configuration"),
            ));
        }
        let echoed_variant = echoed
            .model
            .as_ref()
            .and_then(|model| model.variant.as_deref());
        if target_variant.is_some_and(|target| echoed_variant != Some(target)) {
            return Err(Error::new(
                "unknown",
                format!("OpenCode {context} effort contradicts the applied configuration"),
            ));
        }
        Ok(echoed)
    }

    /// The frozen `deny` switch as a per-session ruleset: one wildcard
    /// `subagent` deny as the last rule of the entire ruleset. The
    /// installed server merges session rules after every agent/project rule
    /// with wildcard action matching and last-match-wins evaluation, so this
    /// survives a project `subagent` allow that would beat the isolated
    /// global config under the same ordering. Denies are checked before
    /// saved allow rules, so a user-saved allow cannot lift it either.
    fn subagent_deny_permissions() -> Vec<PermissionRule> {
        vec![PermissionRule {
            action: "subagent".to_owned(),
            resource: "*".to_owned(),
            effect: "deny".to_owned(),
        }]
    }

    /// Prove the stored session carries the deny ruleset with a fresh
    /// `session.get`. A lost readback is `native_readback`, never unknown:
    /// the read itself proves nothing was written.
    async fn readback_subagent_deny(
        http: &OpenCodeHttp,
        session_id: &str,
        deadline: Instant,
    ) -> Result<bool> {
        let info = http
            .get_session(session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "native_readback",
                    format!(
                        "OpenCode subagent denial readback failed ({})",
                        error.message
                    ),
                )
            })?;
        Ok(OpenCodeHttp::session_subagent_deny_verified(&info))
    }

    /// Repair a retained session that predates the per-session deny ruleset:
    /// `PATCH` the preserved stored rules with the wildcard deny appended as
    /// the final entry, then the caller re-reads. Preserving keeps unrelated
    /// session restrictions; appending as final keeps the deny winning under
    /// last-match-wins evaluation. A refusal proves the installed server
    /// cannot establish the denial, so `deny` profiles fail closed; anything
    /// ambiguous after the write is `unknown` (the ruleset may have been
    /// stored).
    async fn resume_repair_subagent_deny(
        http: &OpenCodeHttp,
        session_id: &str,
        existing: &[PermissionRule],
        deadline: Instant,
    ) -> Result<()> {
        let deny = OpenCodeHttp::with_subagent_deny(existing);
        match http
            .update_session_permissions(session_id, &deny, deadline)
            .await
        {
            Ok(()) => Ok(()),
            Err(HttpOutcome::Rejected { reason, .. }) => Err(Error::new(
                "profile_error",
                format!("OpenCode retained session cannot carry the subagent denial ({reason})"),
            )),
            Err(HttpOutcome::Unknown { reason }) | Err(HttpOutcome::NotDispatched { reason }) => {
                Err(Error::new(
                    "unknown",
                    format!("OpenCode resume subagent denial repair is unresolved ({reason})"),
                ))
            }
        }
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
        envelope: Instant,
    ) -> Error {
        let error = discard_failed_start(&mut session, error, envelope).await;
        // Resume-phase native failures keep their harness origin: the
        // readback or configuration the native side refused or left
        // unresolved. Only the location is filled when missing.
        let mut error = error.with_where("plugin.opencode.start.resume");
        if error.origin.is_none() {
            error = error.with_origin(
                FailureOrigin::Harness,
                "resume-phase native readback or configuration failed or stayed unresolved",
            );
        }
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
        .with_where("plugin.opencode.start.resume")
        .with_origin(
            FailureOrigin::Harness,
            "retained native session state unresolved after resume readback",
        )
    }

    /// Every post-create failure funnels through here: the created session
    /// may have been admitted, so the delete is proved first. A proved
    /// removal stops the server inside the shutdown share and settles failed;
    /// an unproved removal retains the exact session in memory as unknown,
    /// with the server still live. The create is never repeated.
    async fn fail_created_start(
        &mut self,
        params: &AgentStartParams,
        mut session: AgentSession,
        native_ref: &str,
        error: Error,
        envelope: Instant,
    ) -> Error {
        let (proved, kill_note) =
            delete_created_session_proved(&mut session, native_ref, envelope).await;
        if proved {
            let message = format!("{}; {kill_note}", error.message);
            let error = session.server.diagnostic_error(error.with_message(message));
            discard_created_start(&mut session, envelope).await;
            return self.retain_failed_start(params, error);
        }
        // The known id was bound at create. Keep it with the live server
        // and stream for readback, without committing session.ready.
        session.phase = SessionPhase::Unknown;
        self.session = Some(session);
        created_start_unknown(native_ref, &error.message, &kill_note)
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
        let _timing = crate::native::startup::Step::new("ready_ack");
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
            )
            .with_where("plugin.opencode.start.ready")
            .with_origin(
                FailureOrigin::Harness,
                "native session.ready acknowledgement unobserved; native session is known",
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
                "effort": parsed.requested_effort,
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
            "configure" => self.configure_command(params).await,
            "steer" => self.steer_command(params).await,
            "interrupt" => self.interrupt_command(params).await,
            "approve" | "deny" | "answer" => self.reply_command(params).await,
            _ => Err(Error::new(
                "unsupported",
                "OpenCode plugin command verb is not implemented",
            )),
        }
    }

    /// Read-only live model catalogue for the owned session: the exact
    /// `GET /api/model` set the start, resume and configure gates admit
    /// against, so an Agent-scoped `harness.models` read lists precisely
    /// what this session accepts. A missing or unready session is `bad_state`.
    async fn session_models(&self, raw: &Value) -> Result<Value> {
        let params: AgentModelsParams = decode(raw, "OpenCode agent.models params are invalid")?;
        let session = self.session.as_ref().ok_or_else(|| {
            Error::new(
                "bad_state",
                "OpenCode session catalogue needs a live Agent session",
            )
        })?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "OpenCode",
        )?;
        if self.failed_start.is_some()
            || session.phase != SessionPhase::Ready
            || native_io_blocked(session)
        {
            return Err(Error::new(
                "bad_state",
                "OpenCode session catalogue is not ready",
            ));
        }
        let catalogue = session
            .http
            .list_native_models(Instant::now() + std::time::Duration::from_secs(5))
            .await?;
        let mut result = session_catalog_json(&catalogue);
        result["agent_id"] = json!(session.agent_id);
        result["generation"] = json!(session.generation);
        Ok(result)
    }

    /// Native `configure`: switch the retained session's model and/or
    /// effort through the installed `POST /api/session/{id}/model`
    /// setter (the effort selects the documented `Model.Ref.variant`)
    /// and prove the switch with a fresh `session.get`. A requested
    /// model or variant outside the live native catalogue is rejected
    /// before any write, never substituted. A refused write proves no
    /// effect (`rejected`); anything ambiguous after a write may have
    /// been admitted (`unknown`). The session that already runs the
    /// requested model and variant skips the write and verifies by
    /// readback, so a retried configure after an uncertain outcome is
    /// idempotent.
    async fn configure_command(&mut self, params: AgentCommandParams) -> Result<Value> {
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
        // Pre-write validation failures reject with proof of no effect
        // (nothing has been written yet), so the settings lane stays free.
        // Only an ambiguous outcome at or after the first write is unknown.
        let requested = match parse_configure_model(&params.args) {
            Ok(request) => request,
            Err(error) if matches!(error.code.as_str(), "invalid" | "unsupported") => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": error.message,
                }));
            }
            Err(error) => return Err(error),
        };
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode session identity is unresolved"))?;
        let deadline = Instant::now() + RPC_TIMEOUT;
        // The installed server validates nothing (a bogus model 204s and
        // echoes back), so the live catalogue gates the write: a miss
        // proves nothing was written and rejects honestly. A catalogue
        // read failure is a readback, never an uncertain write.
        let catalogue = session
            .http
            .list_native_models(deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "native_readback",
                    format!(
                        "OpenCode configure catalogue readback failed ({})",
                        error.message
                    ),
                )
            })?;
        if let Some(model) = requested.model.as_deref()
            && !catalogue_contains(&catalogue, model)
        {
            return Ok(json!({
                "state": "rejected",
                "reason": format!("OpenCode model {model} is not in the native catalogue"),
            }));
        }
        let current = session
            .http
            .get_session(&session_id, deadline)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode configure session read is unresolved ({})",
                        error.message
                    ),
                )
            })?;
        let current_variant = current
            .model
            .as_ref()
            .and_then(|model| model.variant.clone());
        // The switch target keeps the native fields apart end to end: an
        // explicit request re-splits its already catalogue-proven pair,
        // while an effort-only configure reuses the retained native ref
        // verbatim. Routing the retained model through a joined
        // `provider/model` string and back would re-split at the first
        // slash, silently changing the selected model when the native
        // provider ID itself contains a slash.
        let Some(target_ref) =
            configure_switch_ref(requested.model.as_deref(), current.model.as_ref())?
        else {
            return Ok(json!({
                "state": "rejected",
                "reason": "OpenCode configure effort needs a session model",
            }));
        };
        // Display form only: messages and the response echo the frozen
        // pair text; the effort gate below, the switch and its echo
        // verification use the native fields, never this string. Joining
        // provider "a/b" plus model "c" into "a/b/c" and re-splitting
        // would look up provider "a", model "b/c" instead.
        let target_model = format!("{}/{}", target_ref.provider_id, target_ref.id);
        if let Some(effort) = requested.effort.as_deref() {
            let known = catalogue_variants_for(&catalogue, &target_ref.provider_id, &target_ref.id)
                .is_some_and(|variants| variants.iter().any(|known| known == effort));
            if !known {
                return Ok(json!({
                    "state": "rejected",
                    "reason": format!(
                        "OpenCode effort {effort} is not a variant of model {target_model}"
                    ),
                }));
            }
        }
        // A model switch without an effort runs the server default
        // variant; otherwise the retained variant carries over.
        let target_variant = requested.effort.clone().or(if requested.model.is_some() {
            None
        } else {
            current_variant.clone()
        });
        let switch_needed = current.model.as_ref().is_none_or(|model| {
            model.provider_id != target_ref.provider_id || model.id != target_ref.id
        }) || target_variant.as_deref() != current_variant.as_deref();
        if switch_needed {
            let mut model_ref = target_ref.clone();
            model_ref.variant = target_variant.clone();
            match session
                .http
                .switch_model(&session_id, &model_ref, deadline)
                .await
            {
                Ok(()) => {}
                Err(HttpOutcome::Rejected { reason, .. }) => {
                    return Ok(json!({
                        "state": "rejected",
                        "reason": format!("OpenCode refused the model switch ({reason})"),
                    }));
                }
                Err(HttpOutcome::NotDispatched { reason }) => {
                    return Ok(json!({
                        "state": "rejected",
                        "reason": format!("OpenCode model switch was not dispatched ({reason})"),
                    }));
                }
                Err(HttpOutcome::Unknown { reason }) => {
                    return Err(Error::new(
                        "unknown",
                        format!("OpenCode model switch outcome is unresolved ({reason})"),
                    ));
                }
            }
        }
        // The single shared echo verification: only a readback that
        // names the requested model and variant proves the switch.
        Self::readback_session_model(
            &session.http,
            &session_id,
            &target_ref,
            target_variant.as_deref(),
            deadline,
            "configure",
        )
        .await?;
        let mut response = json!({
            "state": "applied",
            "model": target_model,
            "model_verified": true,
        });
        if let Some(effort) = requested.effort {
            response["effort"] = Value::String(effort);
            response["effort_verified"] = Value::Bool(true);
        }
        Ok(response)
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
        let text = Self::admit_send_command(session, &params).await?;
        let command_id = params.command_id;
        // One deadline for admission plus association: the HTTP 200 only
        // proves inbox admission; the send applies at the exact
        // input-to-execution association on the event stream.
        let deadline = Instant::now() + RPC_TIMEOUT;
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode session identity is unresolved"))?;
        let inbox = match Self::prompt_session(session, &session_id, &text, deadline).await? {
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

    /// Send phase 1, admission: validate input and check idle state
    /// without native writes. Read the event task's status through the
    /// control queue, serialized behind the terminal commit: the watch
    /// snapshot still shows the finished turn when a send follows its
    /// `turn.completed` immediately.
    async fn admit_send_command(
        session: &mut AgentSession,
        params: &AgentCommandParams,
    ) -> Result<String> {
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
        with_claimed_context(text, &params.context, "OpenCode")
    }

    /// Send phase 2, prompt write: admit one input natively. A known rejection
    /// settles `rejected`; anything uncertain latches the session unknown.
    async fn prompt_session(
        session: &mut AgentSession,
        session_id: &str,
        text: &str,
        deadline: Instant,
    ) -> Result<PromptAdmission> {
        let inbox = match session.http.prompt(session_id, text, deadline).await {
            Ok(inbox) => inbox,
            Err(HttpOutcome::Rejected { reason, .. })
            | Err(HttpOutcome::NotDispatched { reason }) => {
                return Ok(PromptAdmission::Rejected(
                    json!({"state": "rejected", "reason": reason}),
                ));
            }
            Err(HttpOutcome::Unknown { .. }) => {
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    "OpenCode prompt may have applied; the input will not be repeated",
                ));
            }
        };
        if inbox.session_id != session_id || inbox.id.trim().is_empty() {
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
            session.phase = SessionPhase::Unknown;
            return Err(Error::new("unknown", message));
        }
        match session
            .events
            .await_association(&command_id, deadline)
            .await
        {
            Ok(turn_id) => Ok(json!({"state": "applied", "turn_id": turn_id})),
            Err(_) => {
                session.events.abandon_association(command_id.clone()).await;
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
        let command_id = params.command_id;
        let turn_id = params.turn_id;
        let Some(turn_id) = turn_id else {
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
                return Err(Error::new(
                    "unknown",
                    "OpenCode interrupt turn check is unresolved",
                ));
            }
        };
        if !live {
            return Err(Error::new(
                "native_not_dispatched",
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
                Ok(result)
            }
            Ok(false) => {
                let live = match session.events.check_turn(turn_id).await {
                    Ok(live) => live,
                    // The write went out but the gate is unresolved (the
                    // event task is gone): unknown, never stale.
                    Err(_) => {
                        return Err(Error::new(
                            "unknown",
                            "OpenCode interrupt turn check is unresolved",
                        ));
                    }
                };
                if live {
                    Err(Error::new(
                        "unknown",
                        "OpenCode server reports no running execution but the turn is still active",
                    ))
                } else {
                    Ok(json!({
                        "state": "rejected",
                        "command_id": command_id,
                        "turn_id": turn_id,
                        "reason": "OpenCode interrupt named a finished turn",
                    }))
                }
            }
            Err(HttpOutcome::Rejected { reason, .. } | HttpOutcome::NotDispatched { reason }) => {
                let result = json!({
                    "state": "rejected",
                    "command_id": command_id,
                    "turn_id": turn_id,
                    "reason": reason,
                });
                Ok(result)
            }
            Err(HttpOutcome::Unknown { reason }) => Err(Error::new(
                "unknown",
                format!("OpenCode interrupt outcome is unresolved ({reason})"),
            )),
        }
    }

    /// Steer the exact active turn: one `session.prompt` write with
    /// explicit `delivery: steer`, then the event task proves the input
    /// was delivered while the exact turn stayed active. Native joins
    /// such an input to the running turn at a step boundary; the write
    /// never opens a turn here (no send-lane association) and never emulates
    /// interruption. A finished turn is
    /// stale without a write; anything that cannot prove consumption is
    /// unknown, and a missed input is cancelled by exact id with removal
    /// proven by inbox-list absence plus a stream-observed cancel —
    /// anything unproven latches the session unknown so no later send
    /// dispatches behind the unresolved input.
    async fn steer_command(&mut self, params: AgentCommandParams) -> Result<Value> {
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
        let command_id = params.command_id;
        let turn_id = params.turn_id;
        let text = match params.args.get("text").and_then(Value::as_str) {
            Some(text) => text.to_owned(),
            None => {
                return Err(Error::new("invalid", "OpenCode steer requires args.text"));
            }
        };
        if text.len() > MAX_TEXT_BYTES {
            return Err(Error::new(
                "frame_too_large",
                "OpenCode input exceeds 512 KiB",
            ));
        }
        let Some(turn_id) = turn_id else {
            return Err(Error::new(
                "stale_turn",
                "OpenCode steer requires the exact turn id",
            ));
        };
        let live = match session.events.check_turn(&turn_id).await {
            Ok(live) => live,
            // The gate itself is unresolved (the event task is gone): the
            // turn may still be live, so this is unknown, never stale.
            Err(_) => {
                return Err(Error::new(
                    "unknown",
                    "OpenCode steer turn check is unresolved",
                ));
            }
        };
        if !live {
            return Err(Error::new(
                "native_not_dispatched",
                "OpenCode steer named a finished turn",
            ));
        }
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode session identity is unresolved"))?;
        let deadline = Instant::now() + RPC_TIMEOUT;
        let inbox = match session
            .http
            .prompt_with_delivery(&session_id, &text, Some("steer"), deadline)
            .await
        {
            Ok(inbox) => {
                if inbox.session_id != session_id || inbox.id.trim().is_empty() {
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "OpenCode steer answered for another session",
                    ));
                }
                inbox
            }
            Err(HttpOutcome::Rejected { reason, .. })
            | Err(HttpOutcome::NotDispatched { reason }) => {
                let result = json!({
                    "state": "rejected",
                    "command_id": command_id,
                    "turn_id": turn_id,
                    "reason": reason,
                });
                return Ok(result);
            }
            Err(HttpOutcome::Unknown { reason }) => {
                // The input may have been admitted without an inbox id to
                // watch or cancel: latch unknown like the send path and
                // never re-send (the control binding stays).
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    format!("OpenCode steer outcome is unresolved ({reason})"),
                ));
            }
        };
        let verdict = match session
            .events
            .watch_steer(command_id.clone(), turn_id.clone(), inbox.id.clone())
            .await
        {
            Ok(verdict) => verdict,
            Err(_) => {
                return Err(Self::settle_steer_unproven(
                    session,
                    &session_id,
                    &inbox.id,
                    "OpenCode steer was admitted but its consumption cannot be proven",
                )
                .await);
            }
        };
        match timeout_at(deadline, verdict).await {
            Ok(Ok(SteerVerdict::Applied)) => {
                let result = json!({
                    "state": "applied",
                    "command_id": command_id,
                    "turn_id": turn_id,
                });
                Ok(result)
            }
            Ok(Ok(SteerVerdict::Missed)) => Err(Self::settle_steer_unproven(
                session,
                &session_id,
                &inbox.id,
                "OpenCode steer missed its turn; the input did not join the exact turn",
            )
            .await),
            Ok(Err(_)) | Err(_) => {
                session.events.abandon_steer(command_id.clone()).await;
                Err(Self::settle_steer_unproven(
                    session,
                    &session_id,
                    &inbox.id,
                    "OpenCode steer consumption is unproven",
                )
                .await)
            }
        }
    }

    /// Settle a steer whose exact-turn consumption is unproven: cancel the
    /// exact inbox id (best effort; the native 204 also covers the
    /// no-op), then prove removal before letting the session continue.
    /// Removal is proven only by absence from the inbox-list readback
    /// plus a stream-observed cancel with no delivery. Anything else —
    /// still pending, delivered into later work, failed readback, or an
    /// unanswered stream record — latches the session unknown so the
    /// router cannot dispatch a later send or Goal behind the unresolved
    /// native input. The steer itself is always unknown.
    async fn settle_steer_unproven(
        session: &mut AgentSession,
        session_id: &str,
        inbox_id: &str,
        message: &str,
    ) -> Error {
        let deadline = Instant::now() + RPC_TIMEOUT;
        let _ = session
            .http
            .cancel_inbox(session_id, inbox_id, deadline)
            .await;
        let removed = Self::steer_input_removed(session, session_id, inbox_id).await;
        if removed {
            Error::new("unknown", message.to_owned())
        } else {
            session.phase = SessionPhase::Unknown;
            Error::new(
                "unknown",
                format!("{message}; the session is latched unknown behind the unresolved input"),
            )
        }
    }

    /// Removal proof for one steered inbox id: absent from the pending
    /// list, never delivered on the stream, and cancelled on the stream.
    /// Every other combination (including any failed read) is unproven.
    async fn steer_input_removed(session: &AgentSession, session_id: &str, inbox_id: &str) -> bool {
        let pending = match session
            .http
            .list_inbox(session_id, Instant::now() + RPC_TIMEOUT)
            .await
        {
            Ok(items) => items,
            Err(_) => return false,
        };
        if pending
            .iter()
            .any(|item| item.get("id").and_then(Value::as_str) == Some(inbox_id))
        {
            return false;
        }
        match session.events.check_inbox(inbox_id).await {
            Ok(InboxRecord {
                delivered: false,
                cancelled: true,
            }) => true,
            Ok(_) | Err(_) => false,
        }
    }

    /// Reply to one exact retained native request. The event task stages
    /// the choice against the current offer and performs the native write.
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
        let (Some(turn_id), Some(request_id)) = (
            params.turn_id,
            params.request_id.filter(|id| !id.is_empty()),
        ) else {
            return Err(Error::new(
                "not_found",
                "OpenCode reply requires the exact turn and request ids",
            ));
        };
        match session
            .events
            .reply_request(
                params.command_id,
                request_id,
                turn_id,
                params.verb,
                params.args,
            )
            .await
        {
            Ok(result) => Ok(result),
            Err(error) => {
                // These native offer guards ran before the reply write.
                if matches!(
                    error.code.as_str(),
                    "not_found" | "stale_turn" | "rejected" | "busy"
                ) || native_no_write(&error.code)
                {
                    return Ok(json!({
                        "state": "rejected",
                        "reason": error.code,
                        "message": error.message,
                    }));
                }
                Err(Error::new(
                    "unknown",
                    format!("OpenCode reply outcome is unresolved ({})", error.message),
                ))
            }
        }
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
        let deadline = Instant::now() + RPC_TIMEOUT;
        if !session
            .http
            .get_session(&request.session_id, deadline)
            .await
            .is_ok_and(|native| native.id == request.session_id)
        {
            return unknown_snapshot(
                &params,
                "OpenCode session readback did not prove the exact owned session",
            );
        }
        let native_turn = if let Some(input) = event_status.turn_input.as_ref().filter(|input| {
            request.turn_id.as_deref() == Some(input.turn_id.as_str())
                && params.command_id == input.command_id
                && event_status
                    .active_turn_id
                    .as_ref()
                    .is_none_or(|active| active == &input.turn_id)
        }) {
            events::read_turn_messages(
                &session.http,
                &request.session_id,
                &input.inbox_id,
                deadline,
            )
            .await
            .ok()
            .filter(|readback| {
                !matches!(readback, events::TurnReadback::Running)
                    || event_status.active_turn_id.as_deref() == Some(input.turn_id.as_str())
            })
        } else {
            None
        };
        let turn_evidence = native_turn
            .as_ref()
            .map(|_| "GET /api/session/{id}+GET /api/session/{id}/message");
        let active_turn = if matches!(&native_turn, Some(events::TurnReadback::Terminal(_))) {
            None
        } else {
            event_status.active_turn_id.clone()
        };
        let last_turn = match native_turn {
            Some(events::TurnReadback::Running) => json!({
                "command_id": params.command_id,
                "turn_id": request.turn_id,
                "status": "inProgress",
                "terminal": false,
            }),
            Some(events::TurnReadback::Terminal(terminal)) => {
                json!({
                    "command_id": params.command_id,
                    "turn_id": request.turn_id,
                    "status": terminal.outcome,
                    "terminal": true,
                    "final_text": terminal.final_text,
                    "native_error": terminal.idle_error.map(|error| error.message),
                })
            }
            None => match Self::snapshot_turn_facts(
                &event_status,
                &params,
                request.turn_id.as_deref(),
                &session.start_command_id,
                session.stop_command_id.as_deref(),
            ) {
                Ok(facts) => serde_json::to_value(facts).map_err(|_| {
                    Error::new("internal", "OpenCode turn snapshot could not be encoded")
                })?,
                Err(reason) => return unknown_snapshot(&params, reason),
            },
        };
        let state = if active_turn.is_some() {
            "busy"
        } else {
            "idle"
        };
        let result_known = last_turn["final_text"].is_string();
        Ok(json!({
            "agent_id": params.agent_id,
            "generation": params.generation,
            "command_id": params.command_id,
            "state": state,
            "native_scope": session.native_scope,
            "native_ref": request.session_id,
            "active_turn": active_turn,
            "last_turn": last_turn,
            "turn_evidence": turn_evidence,
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
        let turn_id = params.turn_id.as_deref().map(str::to_owned);
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

    /// The requested turn from the committed stream fold. Only the native
    /// session's start and stop commands can request session-level facts.
    fn snapshot_turn_facts(
        event_status: &EventStatus,
        params: &AgentSnapshotParams,
        turn_id: Option<&str>,
        start_command_id: &str,
        stop_command_id: Option<&str>,
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
            None if params.command_id == start_command_id
                || stop_command_id == Some(params.command_id.as_str()) =>
            {
                Ok(event_status.last_turn.clone())
            }
            None => Err("OpenCode command has no exact native turn association"),
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
        // Latch once before native I/O. An uncertain close is never replayed.
        session.stop_command_id = Some(params.command_id.clone());
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("unknown", "stop lost its retained native session"))?;
        // The router owns interrupt and the wait. The adapter closes the
        // native source while the daemon owns process custody.
        terminate_session(session, false).await?;
        Ok(json!({"state": "applied"}))
    }

    async fn shutdown(&mut self) -> Result<Value> {
        if let Some(session) = self.session.as_mut() {
            // Shutdown with proved resource closure ends the owned path
            // without replaying native control. An already-acked End is
            // likewise never repeated.
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
/// transfer prepares. Required on the cloud path and nonsecret; the
/// capacity-only account field is never consulted for authentication. On
/// the adopted local path the selector must be absent: no canonical
/// credential is ever imported there.
fn start_auth_selector(
    profile: &serde_json::Map<String, Value>,
    is_fragment: bool,
) -> Result<Option<IntegrationSelector>> {
    let raw = profile
        .get("auth_integration")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if is_fragment {
        if raw.is_some() {
            return Err(Error::new(
                "profile_error",
                "OpenCode provider fragment profiles must not name a canonical auth_integration",
            ));
        }
        return Ok(None);
    }
    let raw = raw.ok_or_else(|| {
        Error::new(
            "profile_error",
            "OpenCode profile requires an explicit auth_integration selector",
        )
    })?;
    IntegrationSelector::new(raw)
        .map(Some)
        .map_err(|error| error.with_code("profile_error"))
}

/// The frozen native `provider.<id>` fragment: an object whose keys are
/// provider ids and whose values are written as-is into the isolated
/// `opencode.jsonc` under `providers`. Only the `provider` table is admitted;
/// any fragment without a model fails closed at admission.
fn start_provider_fragment(
    profile: &serde_json::Map<String, Value>,
) -> Result<Option<ProviderFragment>> {
    let Some(fragment) = profile.get("provider") else {
        return Ok(None);
    };
    let Some(map) = fragment.as_object() else {
        return Err(Error::new(
            "profile_error",
            "OpenCode provider fragment must be an object",
        ));
    };
    if map.is_empty() {
        return Err(Error::new(
            "profile_error",
            "OpenCode provider fragment must name at least one provider",
        ));
    }
    if !profile
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| !model.trim().is_empty())
    {
        return Err(Error::new(
            "profile_error",
            "OpenCode provider fragment requires an explicit model",
        ));
    }
    if profile.contains_key("auth_integration") {
        return Err(Error::new(
            "profile_error",
            "OpenCode provider fragment profiles must not name a canonical auth_integration",
        ));
    }
    let mut out = ProviderFragment::new();
    for (id, value) in map {
        if id.trim().is_empty() || !value.is_object() {
            return Err(Error::new(
                "profile_error",
                "OpenCode provider fragment entries must be objects",
            ));
        }
        out.insert(id.clone(), value.clone());
    }
    Ok(Some(out))
}

/// Read the frozen worker MCP policy back. The owned serve is already
/// isolated from the owner's global config, so both policies serve the
/// Agent-scoped server unless the workspace project layer widens it; the
/// policy is still read (malformed snapshots fail closed) and reported.
/// OpenCode has no selective-inclusion mapping, so a non-empty allow list
/// fails closed instead of being silently dropped. The project layer is
/// scanned for owner-identity entries before the session exists, and the
/// settled inventory is gated (see `gate_mcp_inventory`).
fn policy_from_snapshot(profile: &serde_json::Map<String, Value>) -> Result<McpPolicy> {
    let policy = McpPolicy::parse_snapshot(profile)
        .map_err(|error| Error::new("profile_error", format!("OpenCode {}", error.message)))?;
    let allow = eliot_core::parse_snapshot_allow_list(profile)
        .map_err(|error| Error::new("profile_error", format!("OpenCode {}", error.message)))?;
    if !allow.is_empty() {
        return Err(Error::new(
            "unsupported",
            "OpenCode mcp_allow has no selective-inclusion mapping",
        ));
    }
    Ok(policy)
}

/// How long the effective MCP readback waits for the installed inventory
/// to settle on the Agent-scoped server. MCP servers connect
/// asynchronously after session create (observed up to ~10s on the
/// installed 2.2.7 server); a single instant query almost always races
/// them and reports an empty surface. Polling is bounded well under one
/// RPC budget so neither the create nor the resume path can stall on it.
const MCP_SETTLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);
const MCP_SETTLE_FIRST_POLL: std::time::Duration = std::time::Duration::from_millis(50);
const MCP_SETTLE_MAX_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// Polled Agent-scoped MCP readback for the effective record:
/// `GET /api/mcp` (`mcp.list`) after the session exists, re-polled until
/// the Agent-scoped server reports `connected` (or a terminal
/// non-connected status, or the settle cap). Names come from the installed
/// inventory when a query succeeds (`null` when none does, never
/// fabricated); `mcp_servers_verified` is true only on `connected`, and
/// the inventory gate refuses the start when no query succeeded, so a
/// `null` record never passes as verified. Only
/// names and the status word are retained — never the failure text, which
/// can carry local paths. A non-connected surface alone never fails the
/// start here: the inventory gate decides, refusing an unreadable
/// inventory (without it the unexpected-server gate and the
/// effective-transport proof cannot run) as well as unexpected or
/// owner-identity servers.
async fn observe_mcp_servers(
    http: &OpenCodeHttp,
    deadline: Instant,
) -> (Value, bool, Option<Vec<McpServerReadback>>) {
    let cap = (Instant::now() + MCP_SETTLE_TIMEOUT).min(deadline);
    let mut poll = MCP_SETTLE_FIRST_POLL;
    let mut last: Option<Vec<McpServerReadback>> = None;
    loop {
        let read_timing = crate::native::startup::Step::new("mcp_inventory_read");
        let readback = http.list_mcp_servers(cap).await;
        drop(read_timing);
        if let Ok(servers) = readback {
            match mcp_settle(&servers) {
                McpSettle::Connected => return (mcp_names(&servers), true, Some(servers)),
                // A terminal non-connected status settles the observation
                // early: without a restart the entry cannot become serving.
                McpSettle::Terminal => return (mcp_names(&servers), false, Some(servers)),
                McpSettle::KeepPolling => {
                    last = Some(servers);
                }
            }
        }
        if Instant::now() >= cap {
            break;
        }
        // Hot MCP startup should not pay a whole second after the first
        // pending observation. Back off for cold starts, clamping both
        // the native reads and sleeps to the same settle/start deadline.
        tokio::time::sleep_until((Instant::now() + poll).min(cap)).await;
        poll = (poll * 2).min(MCP_SETTLE_MAX_POLL);
    }
    match last {
        Some(servers) => (mcp_names(&servers), false, Some(servers)),
        None => (Value::Null, false, None),
    }
}

/// Refuse the start when any workspace-inherited project MCP layer
/// contributes the owner Swarm identity under any name. The owned serve
/// already merged those layers, so the scan runs before the native session
/// exists (create) or before the retained session is touched (resume), in
/// every policy. No project file is read beyond the documented names, and
/// none is ever edited.
fn refuse_owner_project_mcp(
    workspace: &std::path::Path,
    controller_executable: &std::path::Path,
    cloud_mcp_hostname: Option<&str>,
) -> Result<()> {
    let entries = crate::project_mcp::scan_project_mcp(workspace)?;
    let hits = crate::project_mcp::find_owner_entries(
        &entries,
        controller_executable.to_str(),
        cloud_mcp_hostname,
    );
    if let Some(hit) = hits.first() {
        return Err(Error::new(
            "profile_error",
            crate::project_mcp::owner_refusal_reason(hit),
        ));
    }
    Ok(())
}

/// Refuse the start when any effective `mcp.servers` entry carries the
/// owner Swarm identity under any name. The pre-create file scan only sees
/// project layers, but managed, system, global and integration layers also
/// merge natively (managed settings load last and override everything in
/// `packages/core/src/config.ts`), so a differently named owner transport
/// there escapes both that scan and the reserved-name inventory gate under
/// `inherit`. The winning documents prove its absence. The Agent-scoped
/// entry (`swarm mcp --require-agent`) never matches: it is not the owner
/// identity.
fn refuse_effective_owner_mcp(
    documents: &[ConfigEntry],
    controller_executable: Option<&str>,
    cloud_mcp_hostname: Option<&str>,
) -> Result<()> {
    for document in documents {
        let Some(servers) = document
            .info
            .as_ref()
            .and_then(|info| info.mcp.as_ref())
            .map(|mcp| &mcp.servers)
        else {
            continue;
        };
        for (name, server) in servers {
            let owned = match server.command.as_deref() {
                Some([head, tail @ ..]) => eliot_core::is_owner_swarm_server(
                    Some(head.as_str()),
                    Some(tail),
                    server.url.as_deref(),
                    controller_executable,
                    cloud_mcp_hostname,
                ),
                _ => eliot_core::is_owner_swarm_server(
                    None,
                    None,
                    server.url.as_deref(),
                    controller_executable,
                    cloud_mcp_hostname,
                ),
            };
            if owned {
                let origin = document.path.as_deref().unwrap_or("effective config");
                return Err(Error::new(
                    "profile_error",
                    format!(
                        "OpenCode effective MCP layer contributes the owner Swarm MCP server {name:?} from {origin}"
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Exclusive inventory gate for the worker MCP surface, applied after the
/// settle observation on both the create and resume paths. An unreadable
/// inventory refuses the start: without it neither the unexpected-server
/// gate nor the effective-transport proof below can run, so absence of
/// evidence must fail closed, never widen the worker by default.
/// Otherwise:
///
/// minimal refuses any serving (`connected` or `pending`) server besides
/// the Agent-scoped entry, so a project layer that defeats minimal fails
/// the start instead of widening the worker. inherit refuses serving
/// entries with reserved Swarm names (the project file scan already
/// refused transport matches; this is the backstop for layers it cannot
/// see). `mcp_servers_verified` stays true only after this gate passes
/// with the Agent-scoped entry connected.
fn gate_mcp_inventory(servers: Option<&[McpServerReadback]>, policy: McpPolicy) -> Result<()> {
    let Some(servers) = servers else {
        return Err(Error::new(
            "native_readback",
            "OpenCode MCP inventory read failed (no native inventory query succeeded)",
        ));
    };
    for server in servers {
        if !matches!(server.status.status.as_str(), "connected" | "pending") {
            continue;
        }
        if server.name == SWARM_MCP_SERVER_NAME {
            continue;
        }
        if policy == McpPolicy::Minimal {
            return Err(Error::new(
                "profile_error",
                format!(
                    "OpenCode minimal MCP found an unexpected serving native server {:?}",
                    server.name
                ),
            ));
        }
        if eliot_core::is_swarm_reserved_server_name(&server.name) {
            return Err(Error::new(
                "profile_error",
                format!(
                    "OpenCode inherit MCP found a reserved Swarm native server {:?}",
                    server.name
                ),
            ));
        }
    }
    Ok(())
}

/// The managed Agent-scoped MCP transport the plugin generated: the exact
/// local stdio command argv for `SWARM_MCP_SERVER_NAME`, rebuilt from the
/// frozen start admission (the same values rendered into the per-Agent
/// config document by `agent_config_json`).
fn managed_mcp_command(parsed: &ParsedStart) -> Vec<String> {
    vec![
        parsed.swarm_executable.to_string_lossy().into_owned(),
        "--data-dir".to_owned(),
        parsed.swarm_data_dir.to_string_lossy().into_owned(),
        "mcp".to_owned(),
        "--require-agent".to_owned(),
    ]
}

/// The winning effective `mcp.servers` transport for the managed
/// Agent-scoped entry: the highest-precedence effective document defining
/// it wins, mirroring the native fold where later documents replace earlier
/// ones for the same server name. `None` when no effective document defines
/// the entry at all.
fn winning_managed_mcp_server(documents: &[ConfigEntry]) -> Option<&ConfigMcpServer> {
    documents
        .iter()
        .filter(|entry| entry.entry_type == "document")
        .filter_map(|entry| {
            entry
                .info
                .as_ref()
                .and_then(|info| info.mcp.as_ref())
                .and_then(|mcp| mcp.servers.get(SWARM_MCP_SERVER_NAME))
        })
        .next_back()
}

/// Prove the serving managed entry still runs the exact generated
/// transport: a same-name entry from any inherited layer (ancestor direct
/// file, `.opencode/` project directory, or machine scope outside the file
/// scan) replaces the managed document entry natively, while the inventory
/// gate only sees the name and status. The winning definition must be the
/// local stdio shape with exactly the generated argv, no URL, and no
/// environment override; anything else refuses the start. Only the
/// transport/auth-identity fields decide; cosmetic keys never do.
fn verify_managed_mcp_transport(
    documents: &[ConfigEntry],
    expected_command: &[String],
) -> Result<()> {
    let server = winning_managed_mcp_server(documents).ok_or_else(|| {
        Error::new(
            "profile_error",
            "OpenCode managed MCP entry has no effective transport",
        )
    })?;
    let transport_ok = server
        .server_type
        .as_deref()
        .is_none_or(|kind| kind == "local")
        && server.command.as_deref() == Some(expected_command)
        && server.url.as_deref().is_none_or(str::is_empty)
        && server
            .environment
            .as_ref()
            .is_none_or(std::collections::HashMap::is_empty);
    if !transport_ok {
        return Err(Error::new(
            "profile_error",
            format!(
                "OpenCode serving {:?} entry does not run the managed Agent transport",
                SWARM_MCP_SERVER_NAME
            ),
        ));
    }
    Ok(())
}

/// Settle decision for one installed-inventory observation: the poll
/// stops on `Connected` (verified) or `Terminal` (unverified now) and
/// continues on `KeepPolling` (absent or still connecting).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum McpSettle {
    Connected,
    Terminal,
    KeepPolling,
}

fn mcp_settle(servers: &[McpServerReadback]) -> McpSettle {
    if servers
        .iter()
        .any(|server| server.is_connected(SWARM_MCP_SERVER_NAME))
    {
        return McpSettle::Connected;
    }
    if servers.iter().any(|server| {
        server.name == SWARM_MCP_SERVER_NAME
            && matches!(
                server.status.status.as_str(),
                "failed" | "disabled" | "needs_auth"
            )
    }) {
        return McpSettle::Terminal;
    }
    McpSettle::KeepPolling
}

/// Installed-inventory server names for the effective record.
fn mcp_names(servers: &[McpServerReadback]) -> Value {
    json!(
        servers
            .iter()
            .map(|server| server.name.as_str())
            .collect::<Vec<&str>>()
    )
}

// Native provider fragments never call the custom endpoint directly:
// readiness is the OpenCode native catalogue (`GET /api/provider`,
// `GET /api/model`), and an unreachable endpoint surfaces as the native
// turn error, never as a start failure.
/// A configure model/effort request: at least one of the two is present.
/// A requested model switches the session model; a requested effort
/// selects the documented `Model.Ref.variant` on that switch. Model and
/// effort travel together: a model switch without an effort runs the
/// server default variant.
#[derive(Debug)]
struct ConfigureModelRequest {
    model: Option<String>,
    effort: Option<String>,
}

/// The requested configure model and/or effort. Unknown fields refuse the
/// whole command; a missing or malformed member is `invalid`. A null
/// effort is absent; a null or empty model is invalid when present.
fn parse_configure_model(args: &Value) -> Result<ConfigureModelRequest> {
    let args = args
        .as_object()
        .ok_or_else(|| Error::new("invalid", "OpenCode configure args must be a JSON object"))?;
    for key in args.keys() {
        if !matches!(key.as_str(), "model" | "effort") {
            return Err(Error::new(
                "unsupported",
                format!("configure does not support {key}"),
            ));
        }
    }
    let model = match args.get("model") {
        None => None,
        Some(value) => {
            let model = value
                .as_str()
                .filter(|model| !model.trim().is_empty())
                .ok_or_else(|| {
                    Error::new("invalid", "OpenCode configure requires a non-empty model")
                })?;
            split_catalogue_model(model).ok_or_else(|| {
                Error::new(
                    "invalid",
                    "OpenCode configure model must be provider/model with non-empty slash segments and no variant suffix",
                )
            })?;
            Some(model.to_owned())
        }
    };
    let effort = match args.get("effort") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let text = value.as_str().ok_or_else(|| {
                Error::new("invalid", "OpenCode configure effort must be a string")
            })?;
            if text.is_empty() {
                None
            } else {
                if !valid_catalogue_variant(text) {
                    return Err(Error::new(
                        "invalid",
                        "OpenCode configure effort must be an exact variant id",
                    ));
                }
                Some(text.to_owned())
            }
        }
    };
    if model.is_none() && effort.is_none() {
        return Err(Error::new(
            "invalid",
            "OpenCode configure requires a model and/or an effort",
        ));
    }
    Ok(ConfigureModelRequest { model, effort })
}

/// Live `GET /api/model` entries in `model.list` response shape with
/// `session` provenance. Entries without a usable provider/model pair
/// are skipped, never guessed; the endpoint names no default.
fn session_catalog_json(catalogue: &[NativeModelEntry]) -> Value {
    let mut ids = HashSet::new();
    let models: Vec<Value> = catalogue
        .iter()
        .filter_map(|entry| {
            let provider = entry.provider_id.as_deref()?;
            let native_id = entry.id.as_deref()?;
            let id = format!("{provider}/{native_id}");
            if split_catalogue_model(&id)? != (provider, native_id)
                || !ids.insert((provider, native_id))
            {
                return None;
            }
            let efforts = catalogue_entry_variants(entry);
            let display_name = entry
                .name
                .as_deref()
                .and_then(eliot_core::api::harness::native_model_display_name)
                .map(str::to_owned);
            Some(json!({
                "id": id,
                "default": false,
                "efforts": efforts,
                "display_name": display_name,
            }))
        })
        .collect();
    json!({"models": models, "default": Value::Null, "provenance": "session"})
}

fn catalogue_entry_for<'a>(
    catalogue: &'a [NativeModelEntry],
    provider_id: &str,
    id: &str,
) -> Option<&'a NativeModelEntry> {
    catalogue.iter().find(
        |entry| match (entry.provider_id.as_deref(), entry.id.as_deref()) {
            (Some(entry_provider), Some(entry_id)) => {
                entry_provider == provider_id && entry_id == id
            }
            _ => false,
        },
    )
}

/// The live native catalogue row for one exact `provider/model` id, split
/// at the first slash. See [`catalogue_entry_for`]: callers holding the
/// native fields already must use that entry point instead of joining.
fn catalogue_entry<'a>(
    catalogue: &'a [NativeModelEntry],
    requested: &str,
) -> Option<&'a NativeModelEntry> {
    let (provider, id) = split_catalogue_model(requested)?;
    catalogue_entry_for(catalogue, provider, id)
}

fn split_catalogue_model(model: &str) -> Option<(&str, &str)> {
    if model.len() > MAX_MODEL_ID_BYTES {
        return None;
    }
    split_provider_model(model)
}

/// Whether an installed `Model.Ref` names exactly the requested
/// `provider/model` pair: both fields compared independently, never
/// trimmed or joined. See [`catalogue_entry`].
fn model_ref_is(requested: &str, model: &ModelRef) -> bool {
    split_provider_model(requested)
        .is_some_and(|(provider, id)| model.provider_id == provider && model.id == id)
}

/// Verification describes the session's selected identity. Inference
/// availability is learned only from a turn using that identity.
fn launch_model_readback(
    requested: Option<&str>,
    selected: Option<&ModelRef>,
) -> (bool, &'static str) {
    match requested {
        Some(requested) => (
            selected.is_some_and(|model| model_ref_is(requested, model)),
            "requested",
        ),
        None => (selected.is_some(), "native_default"),
    }
}

/// Whether an installed `Model.Ref` is exactly the switch target: both
/// native fields compared independently, never joined or re-split. A
/// joined `provider/model` display string would re-split at the first
/// slash, so a slash in the native provider ID compares against another
/// provider here instead of matching. See [`catalogue_entry`].
fn session_model_is(target: &ModelRef, model: &ModelRef) -> bool {
    target.provider_id == model.provider_id && target.id == model.id
}

/// The configure switch target: an explicit request re-splits its
/// already catalogue-proven pair, while an effort-only configure reuses
/// the retained native ref verbatim (`None` when the session reports no
/// model). The retained ref never routes through a joined
/// `provider/model` string: re-splitting would change the selected model
/// when the native provider ID itself contains a slash.
fn configure_switch_ref(
    requested: Option<&str>,
    current: Option<&ModelRef>,
) -> Result<Option<ModelRef>> {
    match requested {
        Some(wanted) => Ok(Some(split_model(wanted).ok_or_else(|| {
            Error::new(
                "source_integrity",
                "OpenCode configure model is not provider/model with non-empty slash segments",
            )
        })?)),
        None => Ok(current.cloned()),
    }
}

/// Whether the live native catalogue carries the exact native
/// provider/model pair. See [`catalogue_entry_for`].
fn catalogue_contains_for(catalogue: &[NativeModelEntry], provider_id: &str, id: &str) -> bool {
    catalogue_entry_for(catalogue, provider_id, id).is_some()
}

/// Whether the live native catalogue carries the exact requested
/// `provider/model` id. See [`catalogue_entry`].
fn catalogue_contains(catalogue: &[NativeModelEntry], requested: &str) -> bool {
    catalogue_entry(catalogue, requested).is_some()
}

/// The live native catalogue's selectable variant ids for one exact native
/// provider/model pair (`Model.Variant` ids in wire order). `None` when
/// the entry is absent or reports no variants member: unknown, never an
/// empty wardrobe. See [`catalogue_entry_for`].
fn catalogue_variants_for(
    catalogue: &[NativeModelEntry],
    provider_id: &str,
    id: &str,
) -> Option<Vec<String>> {
    catalogue_entry_for(catalogue, provider_id, id).and_then(catalogue_entry_variants)
}

/// The live native catalogue's selectable variant ids for one exact
/// `provider/model` id (`Model.Variant` ids in wire order). `None` when
/// the entry is absent or reports no variants member: unknown, never an
/// empty wardrobe.
fn catalogue_variants(catalogue: &[NativeModelEntry], requested: &str) -> Option<Vec<String>> {
    catalogue_entry(catalogue, requested).and_then(catalogue_entry_variants)
}

fn catalogue_entry_variants(entry: &NativeModelEntry) -> Option<Vec<String>> {
    entry.variants.as_ref().map(|variants| {
        let mut seen = HashSet::new();
        variants
            .iter()
            .filter_map(|variant| {
                let id = variant.id.as_deref()?;
                (valid_catalogue_variant(id) && seen.insert(id)).then(|| id.to_owned())
            })
            .collect()
    })
}

fn valid_catalogue_variant(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && !id.chars().any(|ch| ch.is_control() || ch.is_whitespace())
}

/// A frozen effort value: the trimmed string, non-empty, at most 128
/// bytes, no controls. The live catalogue gate proves membership; this
/// only rejects unusable shapes before any native I/O.
fn checked_effort(raw: Option<&str>, code: &'static str) -> Result<Option<String>> {
    let Some(value) = raw.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if value.len() > 128 || value.chars().any(char::is_control) {
        return Err(Error::new(
            code,
            "OpenCode effort must be non-empty, at most 128 characters and contain no controls",
        ));
    }
    Ok(Some(value.to_owned()))
}

/// The inherited-overlay realignment target for a resume: the retained
/// effective model and effort the session already runs. `None` on launch
/// and on resumes without an overlay; the daemon resolves both from the
/// resume command's retained revision, so a value that no longer splits
/// is retained-state corruption, never a profile verdict.
fn start_config_override(
    params: &AgentStartParams,
) -> Result<(Option<String>, Option<ModelRef>, Option<String>)> {
    let effort = checked_effort(params.effort_override.as_deref(), "source_integrity")?;
    let Some(model) = params.model_override.as_deref() else {
        return Ok((None, None, effort));
    };
    let mut model_ref = split_model(model).ok_or_else(|| {
        Error::new(
            "source_integrity",
            "OpenCode retained configure model is not provider/model with non-empty slash segments and no variant suffix",
        )
    })?;
    model_ref.variant = effort.clone();
    Ok((Some(model.to_owned()), Some(model_ref), effort))
}

/// The frozen profile model: an exact `provider/id` selection plus its
/// native reference, with the frozen `effort` as the documented
/// `Model.Ref.variant`. Both absent when the profile omitted them and the
/// configured native default applies.
fn start_model_ref(
    profile: &serde_json::Map<String, Value>,
) -> Result<(Option<String>, Option<ModelRef>, Option<String>)> {
    let requested_model: Option<String> = profile
        .get("model")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    let effort = checked_effort(
        profile.get("effort").and_then(Value::as_str),
        "profile_error",
    )?;
    let Some(requested) = requested_model else {
        if effort.is_some() {
            return Err(Error::new(
                "profile_error",
                "OpenCode effort without a model has no native mapping",
            ));
        }
        return Ok((None, None, None));
    };
    let mut model_ref = split_model(&requested).ok_or_else(|| {
        Error::new(
            "profile_error",
            "OpenCode profile model must be provider/model with non-empty slash segments and no variant suffix",
        )
    })?;
    model_ref.variant = effort.clone();
    Ok((Some(requested), Some(model_ref), effort))
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

/// Preserve the exact native session for resume and drain its event source.
/// The daemon owns the server's process exit after an ordinary stop.
async fn terminate_session(session: &mut AgentSession, force: bool) -> Result<()> {
    let deadline = Instant::now() + RPC_TIMEOUT;
    // A retention/readback failure never skips the bounded owned-resource
    // cleanup below. The semantic error is preserved separately, then
    // process closure and source End run without replaying the unknown read.
    let mut stage = StopStage::Begin;
    let _ = note_stage(&mut stage, StopStage::Begin);
    let mut semantic_error = false;
    if session.events.begin_stop().await.is_err() {
        semantic_error = true;
    } else {
        let _ = note_stage(&mut stage, StopStage::Retention);
        let retained = match session.session_id.clone() {
            Some(session_id) if !session_id.is_empty() => {
                match session.http.get_session(&session_id, deadline).await {
                    Ok(info) => info.id == session_id,
                    Err(_) => false,
                }
            }
            _ => false,
        };
        if !retained {
            semantic_error = true;
        }
    }
    if !force {
        let drained = session.events.drain_stop().await;
        if drained.is_ok() {
            session.stop_end_ack_proved = true;
        }
        if semantic_error || drained.is_err() {
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
        return Ok(());
    }
    finish_stop_cleanup(session, stage, semantic_error, force, deadline).await
}

async fn finish_stop_cleanup(
    session: &mut AgentSession,
    mut stage: StopStage,
    semantic_error: bool,
    force: bool,
    deadline: Instant,
) -> Result<()> {
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

/// Close the start event stream inside the shared envelope: the raw close
/// waits up to a full event deadline on its own, so clamp it to the cleanup
/// budget instead of stacking it past the daemon budget.
async fn close_start_events(session: &AgentSession, envelope: Instant) {
    let _ = timeout_at(
        phase_deadline(envelope, START_CLEANUP_BUDGET),
        session.events.close(),
    )
    .await;
}

/// Discard a server that never admitted a session or prompt: the event close
/// and the terminate/diagnostic wait share one cleanup budget clamped to the
/// start envelope, so a refusal at the edge of the budget settles fast
/// inside the daemon deadline instead of stacking a fresh 30 s past it
/// (which would lose the reply and pin the Agent in `starting`).
async fn discard_unstarted(session: &mut AgentSession, envelope: Instant) {
    close_start_events(session, envelope).await;
    let _ = session
        .server
        .terminate(true, phase_deadline(envelope, START_CLEANUP_BUDGET))
        .await;
    session.phase = SessionPhase::Stopped;
}

async fn discard_failed_start(
    session: &mut AgentSession,
    error: Error,
    envelope: Instant,
) -> Error {
    discard_unstarted(session, envelope).await;
    session.server.diagnostic_error(error)
}

/// Discard a server after a proved created-session delete: the event close
/// and the terminate share one shutdown deadline inside the reserve, so the
/// pair can never stack past the daemon budget after the delete's own share.
async fn discard_created_start(session: &mut AgentSession, envelope: Instant) {
    let stop_by = phase_deadline(envelope, START_SHUTDOWN_BUDGET);
    let _ = timeout_at(stop_by, session.events.close()).await;
    let _ = session.server.terminate(true, stop_by).await;
    session.phase = SessionPhase::Stopped;
}

/// Proved variant of the created-session delete: `Ok` means the native side
/// proved the session gone (removed or already gone) and the caller may stop
/// the server and settle failed. `Err` means removal is unproved (refused
/// or unconfirmed): the stable store may still retain the session, so the
/// caller retains the exact native identity in memory as unknown and keeps
/// the server live. The delete runs on its own deadline ending a full
/// shutdown share before the envelope, never the shutdown's deadline.
async fn delete_created_session_proved(
    session: &mut AgentSession,
    session_id: &str,
    envelope: Instant,
) -> (bool, String) {
    match session
        .http
        .delete_session(session_id, delete_deadline(envelope))
        .await
    {
        Ok(true) => (true, "the created native session was removed".to_owned()),
        Ok(false) => (
            true,
            "the created native session was already gone".to_owned(),
        ),
        Err(HttpOutcome::Rejected { reason, .. }) => (
            false,
            format!("the created native session kill was refused ({reason})"),
        ),
        Err(HttpOutcome::Unknown { reason } | HttpOutcome::NotDispatched { reason }) => (
            false,
            format!("the created native session kill is unconfirmed ({reason})"),
        ),
    }
}

fn created_start_unknown(native_ref: &str, reason: &str, removal: &str) -> Error {
    Error::new(
        "unknown",
        format!("OpenCode session {native_ref} is unresolved ({reason}; {removal})"),
    )
    .with_where("plugin.opencode.start.create")
    .with_origin(
        FailureOrigin::Harness,
        "created native session state unresolved after start readback",
    )
}

/// Prove the v1 context-hook binding for one exact session. Only a
/// verified `version == 1` match may advertise `turn_safe_point`; every
/// refusal, mismatch or transport loss degrades to `next_input` only
/// without failing readiness. `bind` is idempotent per session id, so
/// an uncertain attempt may be repeated boundedly within this deadline.
async fn bind_context_hooks(
    http: &crate::native::OpenCodeHttp,
    session_id: &str,
    deadline: Instant,
) -> bool {
    for _ in 0..3 {
        if Instant::now() >= deadline {
            return false;
        }
        match http.eliot_context_bind(session_id, deadline).await {
            Ok(output) => {
                return crate::context_hooks::bind_verified(&output, session_id);
            }
            Err(HttpOutcome::Rejected { .. }) => return false,
            Err(HttpOutcome::Unknown { .. } | HttpOutcome::NotDispatched { .. }) => {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    }
    false
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
        harness_version: "2.2.7".to_owned(),
        available: true,
        mode: "per_agent".to_owned(),
        verbs: vec![
            "launch".to_owned(),
            "resume".to_owned(),
            "send".to_owned(),
            "configure".to_owned(),
            "models".to_owned(),
            "steer".to_owned(),
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
    use super::{
        DAEMON_START_TIMEOUT, McpSettle, OpenCodePlugin, PhaseFailure, START_CLEANUP_BUDGET,
        START_ENVELOPE, START_SETTLE_RESERVE, SWARM_MCP_SERVER_NAME, StartAdmission, StartPhase,
        catalogue_contains, catalogue_entry, catalogue_variants, mcp_names, mcp_settle,
        opencode_manifest, opencode_scope, parse_configure_model, phase_budget, phase_deadline,
        policy_from_snapshot, presession_reason, split_model, start_auth_selector,
        start_config_override, start_failure_is_proven_no_effect, start_model_ref,
        start_provider_fragment, verify_managed_mcp_transport, winning_managed_mcp_server,
    };
    use crate::native::server::READINESS_TIMEOUT;
    use crate::native::types::{
        ConfigEntry, ConfigInfo, ConfigMcp, McpServerReadback, McpStatusReadback, NativeModelEntry,
    };
    use eliot_core::{AgentStartParams, McpPolicy, Result};
    use eliot_plugin::session::{
        ResumeSubject, optional_profile_text, parse_resume_subject, validate_source_plan,
    };
    use serde_json::json;

    #[test]
    fn turnless_snapshot_binds_start_and_stop_commands_only() {
        use super::{AgentSnapshotParams, EventStatus, TurnFacts};

        let status = EventStatus {
            last_turn: Some(TurnFacts {
                command_id: "send-1".to_owned(),
                turn_id: "turn-1".to_owned(),
                status: None,
                final_text: None,
                terminal: true,
            }),
            ..Default::default()
        };
        let mut params = AgentSnapshotParams {
            agent_id: "a1".to_owned(),
            generation: 1,
            command_id: "start".to_owned(),
            turn_id: None,
        };
        for command_id in ["start", "stop"] {
            params.command_id = command_id.to_owned();
            let turn =
                OpenCodePlugin::snapshot_turn_facts(&status, &params, None, "start", Some("stop"))
                    .unwrap()
                    .unwrap();
            assert_eq!(turn.turn_id, "turn-1");
        }
        params.command_id = "send-1".to_owned();
        assert!(
            OpenCodePlugin::snapshot_turn_facts(&status, &params, None, "start", Some("stop"))
                .is_err()
        );
    }

    fn mcp_fixture(name: &str, status: &str) -> McpServerReadback {
        McpServerReadback {
            name: name.to_owned(),
            status: McpStatusReadback {
                status: status.to_owned(),
                error: None,
            },
        }
    }

    #[test]
    fn session_catalog_matches_configure_identity_rules() {
        let live: Vec<NativeModelEntry> = serde_json::from_value(json!([
            {"providerID": "opencode", "id": "deepseek-v4.1-flash-free",
             "name": "DeepSeek V4.1 Flash Free",
             "variants": [{"id": "low"}, {"id": "low"}, {"id": "has space"}, {"id": " low"}, {"id": ""}]},
            {"providerID": "openrouter", "id": "meta/muse-spark-open",
             "name": "Muse Spark Open"},
            {"providerID": "opencode-go", "id": "  ", "name": "Blank"},
            {"providerID": "opencode ", "id": "padded", "name": "Padded"},
            {"providerID": "openrouter/meta", "id": "muse-spark-open", "variants": [{"id": "wrong"}]},
            {"providerID": "bad provider", "id": "m", "name": "Space"},
            {"id": "no-provider", "name": "Missing"},
        ]))
        .expect("live fixture decodes");
        let reply = super::session_catalog_json(&live);
        assert_eq!(
            reply.get("provenance").and_then(serde_json::Value::as_str),
            Some("session")
        );
        assert!(reply.get("default").unwrap().is_null());
        let models = reply
            .get("models")
            .and_then(serde_json::Value::as_array)
            .expect("models array");
        assert_eq!(models.len(), 2);
        assert_eq!(
            models[0].get("id").and_then(serde_json::Value::as_str),
            Some("opencode/deepseek-v4.1-flash-free")
        );
        assert_eq!(
            models[0]
                .get("display_name")
                .and_then(serde_json::Value::as_str),
            Some("DeepSeek V4.1 Flash Free")
        );
        assert_eq!(models[0].get("efforts").unwrap(), &json!(["low"]));
        assert_eq!(
            models[1].get("id").and_then(serde_json::Value::as_str),
            Some("openrouter/meta/muse-spark-open")
        );
        assert!(models[1].get("efforts").unwrap().is_null());
        assert!(catalogue_contains(&live, "openrouter/meta/muse-spark-open"));
        assert!(!catalogue_contains(&live, "opencode/padded"));
        assert_eq!(
            catalogue_variants(&live, "opencode/deepseek-v4.1-flash-free"),
            Some(vec!["low".to_owned()])
        );
        assert!(parse_configure_model(&json!({"effort": " low"})).is_err());
    }

    #[tokio::test]
    async fn an_interrupt_preempts_a_stalled_agent_models_read() {
        assert!(super::is_preemptible_read("agent.models"));
        let (sender, mut requests) = tokio::sync::mpsc::channel(1);
        let read = async {
            let interrupt = eliot_plugin::RpcRequest {
                jsonrpc: "2.0".into(),
                id: json!(2),
                method: "agent.command".into(),
                params: json!({"verb": "interrupt"}),
            };
            assert!(
                sender.try_send(interrupt).is_ok(),
                "enqueue interrupt during GET"
            );
            std::future::pending::<()>().await
        };
        let settled = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            eliot_plugin::preemptible_snapshot(&mut requests, read),
        )
        .await
        .unwrap();
        let eliot_plugin::SnapshotDispatch::Preempted(Some(interrupt)) = settled else {
            panic!("interrupt must preempt the read");
        };
        assert_eq!(interrupt.params["verb"], "interrupt");
    }

    #[test]
    fn absent_default_echo_is_unverified_without_substituting_a_model() {
        let selected = split_model("opencode-go/iceman-contributor-free").unwrap();
        assert_eq!(
            super::launch_model_readback(None, None),
            (false, "native_default")
        );
        assert_eq!(
            super::launch_model_readback(None, Some(&selected)),
            (true, "native_default")
        );
        assert_eq!(
            super::launch_model_readback(Some("opencode/other"), Some(&selected)),
            (false, "requested")
        );
    }

    #[test]
    fn frozen_policy_reads_snapshot_and_rejects_allow() {
        use serde_json::json;
        // Keyless snapshots keep their recorded meaning, `inherit`; new
        // freezes always record the key explicitly.
        let empty = serde_json::Map::new();
        assert_eq!(policy_from_snapshot(&empty).unwrap(), McpPolicy::Inherit);
        let minimal = json!({"mcp": "minimal"});
        assert_eq!(
            policy_from_snapshot(minimal.as_object().unwrap()).unwrap(),
            McpPolicy::Minimal
        );
        let inherit = json!({"mcp": "inherit"});
        assert_eq!(
            policy_from_snapshot(inherit.as_object().unwrap()).unwrap(),
            McpPolicy::Inherit
        );
        let allowed = json!({"mcp": "minimal", "mcp_allow": ["owner-search"]});
        assert_eq!(
            policy_from_snapshot(allowed.as_object().unwrap())
                .unwrap_err()
                .code,
            "unsupported"
        );
        let broken = json!({"mcp": "isolated"});
        assert!(policy_from_snapshot(broken.as_object().unwrap()).is_err());
    }

    #[test]
    fn mcp_settle_verifies_only_a_connected_agent_server() {
        // Absent or still-connecting inventory keeps polling; a serving
        // entry verifies at once; a terminal refusal settles unverified.
        // Shapes mirror `GET /api/mcp` on installed 2.2.7.
        assert_eq!(mcp_settle(&[]), McpSettle::KeepPolling);
        assert_eq!(
            mcp_settle(&[mcp_fixture(SWARM_MCP_SERVER_NAME, "pending")]),
            McpSettle::KeepPolling
        );
        assert_eq!(
            mcp_settle(&[mcp_fixture("owner-search", "connected")]),
            McpSettle::KeepPolling
        );
        assert_eq!(
            mcp_settle(&[mcp_fixture(SWARM_MCP_SERVER_NAME, "failed")]),
            McpSettle::Terminal
        );
        assert_eq!(
            mcp_settle(&[mcp_fixture(SWARM_MCP_SERVER_NAME, "disabled")]),
            McpSettle::Terminal
        );
        assert_eq!(
            mcp_settle(&[
                mcp_fixture("owner-search", "connected"),
                mcp_fixture(SWARM_MCP_SERVER_NAME, "connected"),
            ]),
            McpSettle::Connected
        );
        let servers = vec![
            mcp_fixture("owner-search", "connected"),
            mcp_fixture(SWARM_MCP_SERVER_NAME, "connected"),
        ];
        assert_eq!(mcp_names(&servers), json!(["owner-search", "eliot-swarm"]));
    }
    fn config_document(servers: serde_json::Value) -> ConfigEntry {
        ConfigEntry {
            entry_type: "document".to_owned(),
            path: Some("config/opencode/opencode.jsonc".to_owned()),
            info: Some(ConfigInfo {
                model: None,
                mcp: Some(ConfigMcp {
                    servers: serde_json::from_value(servers).expect("servers decode"),
                }),
            }),
        }
    }

    fn managed_command() -> Vec<String> {
        ["swarm", "--data-dir", "data", "mcp", "--require-agent"]
            .iter()
            .map(|arg| (*arg).to_owned())
            .collect()
    }

    fn managed_server(command: Vec<String>) -> serde_json::Value {
        let mut servers = serde_json::Map::new();
        servers.insert(
            SWARM_MCP_SERVER_NAME.to_owned(),
            json!({"type": "local", "command": command}),
        );
        serde_json::Value::Object(servers)
    }

    #[test]
    fn managed_mcp_transport_proof_requires_the_winning_entry() {
        let expected = managed_command();
        // No effective document defines the entry: nothing proves the
        // serving transport, so the start must not claim it.
        assert!(winning_managed_mcp_server(&[]).is_none());
        assert!(verify_managed_mcp_transport(&[], &expected).is_err());
        // An unrelated table is not a proof either.
        let other = vec![config_document(json!({"other": {"type": "local"}}))];
        assert!(winning_managed_mcp_server(&other).is_none());
        assert!(verify_managed_mcp_transport(&other, &expected).is_err());
        // The exact generated transport verifies.
        let managed = vec![config_document(managed_server(expected.clone()))];
        assert!(verify_managed_mcp_transport(&managed, &expected).is_ok());
        // A same-name replacement transport under any inherited layer
        // refuses, even though the inventory gate only sees the name.
        let replaced = vec![config_document(managed_server(vec![
            "evil".to_owned(),
            "--data-dir".to_owned(),
            "data".to_owned(),
            "mcp".to_owned(),
            "--require-agent".to_owned(),
        ]))];
        assert!(verify_managed_mcp_transport(&replaced, &expected).is_err());
        // Native precedence wins: later documents replace earlier ones, so
        // the proof follows the same winning entry the server serves.
        let shadowed = vec![
            config_document(managed_server(vec!["evil".to_owned()])),
            config_document(managed_server(expected.clone())),
        ];
        assert!(verify_managed_mcp_transport(&shadowed, &expected).is_ok());
        let shadowing = vec![
            config_document(managed_server(expected.clone())),
            config_document(managed_server(vec!["evil".to_owned()])),
        ];
        assert!(verify_managed_mcp_transport(&shadowing, &expected).is_err());
        // A remote URL, an environment override, or a non-local type under
        // the managed name is not the managed transport.
        for servers in [
            json!({"eliot-swarm": {"type": "remote", "url": "https://x.example/mcp"}}),
            json!({"eliot-swarm": {"command": expected.clone(), "url": "https://x.example/mcp"}}),
            json!({"eliot-swarm": {"type": "local", "command": expected.clone(), "environment": {"A": "b"}}}),
        ] {
            assert!(verify_managed_mcp_transport(&[config_document(servers)], &expected).is_err());
        }
        // A missing `type` still verifies when the argv matches exactly:
        // `mcp add` writes it, hand-merged layers may omit it.
        let untyped = vec![config_document(
            json!({"eliot-swarm": {"command": expected}}),
        )];
        assert!(verify_managed_mcp_transport(&untyped, &expected).is_ok());
        // Non-document entries never contribute a winning transport.
        let directory = vec![ConfigEntry {
            entry_type: "directory".to_owned(),
            path: Some("workspace".to_owned()),
            info: None,
        }];
        assert!(winning_managed_mcp_server(&directory).is_none());
    }

    #[test]
    fn effective_owner_transport_refuses_any_name() {
        // No effective servers: nothing to refuse.
        assert!(super::refuse_effective_owner_mcp(&[], None, None).is_ok());
        // The managed Agent-scoped entry is never the owner identity.
        let managed = vec![config_document(managed_server(managed_command()))];
        assert!(super::refuse_effective_owner_mcp(&managed, None, None).is_ok());
        // A differently named owner stdio transport in a managed layer
        // refuses, even though the inventory gate only sees names.
        let evil = vec![config_document(
            json!({"night-bridge": {"type": "local", "command": ["swarm", "mcp"]}}),
        )];
        let error = super::refuse_effective_owner_mcp(&evil, None, None).expect_err("owner hit");
        assert_eq!(error.code, "profile_error");
        assert!(error.message.contains("night-bridge"));
        // A swarm-named entry WITHOUT the Agent scope is the owner too.
        let renamed = vec![config_document(
            json!({"eliot-swarm": {"type": "local", "command": ["swarm", "mcp"]}}),
        )];
        assert!(super::refuse_effective_owner_mcp(&renamed, None, None).is_err());
        // Unrelated transports pass.
        let quiet = vec![config_document(
            json!({"helper": {"type": "local", "command": ["npm", "run", "server"]}}),
        )];
        assert!(super::refuse_effective_owner_mcp(&quiet, None, None).is_ok());
    }
    use std::path::PathBuf;

    #[test]
    fn unreadable_inventory_refuses_the_start_in_every_policy() {
        // Absence of evidence must fail closed: without a readable
        // inventory neither the unexpected-server gate nor the
        // effective-transport proof can run, so the start refuses in
        // every policy instead of widening the worker by default.
        for policy in [McpPolicy::Minimal, McpPolicy::Inherit] {
            let error = super::gate_mcp_inventory(None, policy).expect_err("unreadable refuses");
            assert_eq!(error.code, "native_readback");
        }
        // A readable empty inventory still passes: nothing serves.
        for policy in [McpPolicy::Minimal, McpPolicy::Inherit] {
            assert!(super::gate_mcp_inventory(Some(&[]), policy).is_ok());
        }
        // The connected Agent-scoped entry alone passes in every policy.
        let agent = [mcp_fixture(SWARM_MCP_SERVER_NAME, "connected")];
        for policy in [McpPolicy::Minimal, McpPolicy::Inherit] {
            assert!(super::gate_mcp_inventory(Some(&agent), policy).is_ok());
        }
    }

    const FAMILIES: &[&str] = &["session", "turn", "message", "request"];

    #[test]
    fn configure_model_parsing_accepts_model_and_effort() {
        // One exact provider/model id and/or one effort configures; a
        // null effort is absent, unknown fields refuse the whole command,
        // never partially apply.
        let only_model = parse_configure_model(&json!({"model": "opencode/kimi"})).unwrap();
        assert_eq!(only_model.model.as_deref(), Some("opencode/kimi"));
        assert!(only_model.effort.is_none());
        let nested = parse_configure_model(&json!({"model": "p/a/b"})).unwrap();
        assert_eq!(nested.model.as_deref(), Some("p/a/b"));
        let only_effort = parse_configure_model(&json!({"effort": "low"})).unwrap();
        assert!(only_effort.model.is_none());
        assert_eq!(only_effort.effort.as_deref(), Some("low"));
        let both =
            parse_configure_model(&json!({"model": "opencode/kimi", "effort": "low"})).unwrap();
        assert_eq!(both.model.as_deref(), Some("opencode/kimi"));
        assert_eq!(both.effort.as_deref(), Some("low"));
        let nulled =
            parse_configure_model(&json!({"model": "opencode/kimi", "effort": null})).unwrap();
        assert_eq!(nulled.model.as_deref(), Some("opencode/kimi"));
        assert!(nulled.effort.is_none(), "null effort is absent");
        let blanked =
            parse_configure_model(&json!({"model": "opencode/kimi", "effort": ""})).unwrap();
        assert_eq!(blanked.model.as_deref(), Some("opencode/kimi"));
        assert!(blanked.effort.is_none(), "blank effort is absent");
        for (args, code) in [
            (json!({"model": "opencode/kimi", "bogus": 1}), "unsupported"),
            (json!({}), "invalid"),
            (json!({"model": ""}), "invalid"),
            (json!({"model": "noslash"}), "invalid"),
            (json!({"model": "/empty-provider"}), "invalid"),
            (json!({"model": "trailing/"}), "invalid"),
            (json!({"model": "p/a#b"}), "invalid"),
            (json!({"model": null}), "invalid"),
            (json!({"effort": 5}), "invalid"),
            (json!({"effort": ""}), "invalid"),
            (json!({"effort": "   "}), "invalid"),
        ] {
            let error = parse_configure_model(&args).unwrap_err();
            assert_eq!(error.code, code, "configure args must be refused honestly");
        }
    }

    #[test]
    fn configure_catalogue_matches_exact_provider_model_ids() {
        use crate::native::types::NativeModelVariant;
        let catalogue = vec![
            NativeModelEntry {
                id: Some("kimi".to_owned()),
                provider_id: Some("opencode".to_owned()),
                limit: None,
                name: None,
                variants: Some(vec![
                    NativeModelVariant {
                        id: Some("low".to_owned()),
                    },
                    NativeModelVariant {
                        id: Some("high".to_owned()),
                    },
                ]),
            },
            NativeModelEntry {
                id: None,
                provider_id: Some("opencode".to_owned()),
                limit: None,
                name: None,
                variants: None,
            },
            NativeModelEntry {
                id: Some("kimi".to_owned()),
                provider_id: Some("other".to_owned()),
                limit: None,
                name: None,
                variants: Some(vec![NativeModelVariant {
                    id: Some("sibling".to_owned()),
                }]),
            },
        ];
        assert!(catalogue_contains(&catalogue, "opencode/kimi"));
        assert!(catalogue_contains(&catalogue, "other/kimi"));
        assert!(!catalogue_contains(&catalogue, "opencode/other"));
        assert!(!catalogue_contains(&[], "opencode/kimi"));
        assert_eq!(
            catalogue_variants(&catalogue, "opencode/kimi"),
            Some(vec!["low".to_owned(), "high".to_owned()])
        );
        // The shared row is an exact provider/model pair: an identical
        // model id under another provider never stands in, so neither the
        // membership gate nor the variant gate can read the wrong row.
        let row = catalogue_entry(&catalogue, "other/kimi").expect("exact pair row");
        assert_eq!(row.provider_id.as_deref(), Some("other"));
        assert_eq!(
            catalogue_variants(&catalogue, "other/kimi"),
            Some(vec!["sibling".to_owned()])
        );
        assert!(catalogue_variants(&catalogue, "opencode/other").is_none());
        assert!(catalogue_variants(&[], "opencode/kimi").is_none());
        // Field-by-field without trimming (installed
        // `SessionRunnerModel.select` compares `providerID` and `id`
        // independently with `===`): padded native ids never equal the
        // frozen pair, and padded requests never split.
        let padded = vec![NativeModelEntry {
            id: Some("kimi ".to_owned()),
            provider_id: Some(" opencode".to_owned()),
            limit: None,
            name: None,
            variants: None,
        }];
        assert!(!catalogue_contains(&padded, "opencode/kimi"));
        assert!(!catalogue_contains(&catalogue, "opencode/kimi "));
        assert!(!catalogue_contains(&catalogue, " opencode/kimi"));
        let echo = crate::native::types::ModelRef {
            id: "kimi".to_owned(),
            provider_id: "opencode".to_owned(),
            variant: None,
        };
        assert!(super::model_ref_is("opencode/kimi", &echo));
        let padded_echo = crate::native::types::ModelRef {
            id: "kimi ".to_owned(),
            provider_id: "opencode".to_owned(),
            variant: None,
        };
        assert!(!super::model_ref_is("opencode/kimi", &padded_echo));
    }

    #[test]
    fn configure_switch_ref_keeps_slash_provider_intact() {
        // A slash in the native provider ID must survive an effort-only
        // configure: the retained ref is reused verbatim instead of
        // round-tripping through a joined `provider/model` string, which
        // would re-split at the first slash and select another provider.
        let retained = crate::native::types::ModelRef {
            id: "c".to_owned(),
            provider_id: "a/b".to_owned(),
            variant: Some("low".to_owned()),
        };
        let target = super::configure_switch_ref(None, Some(&retained))
            .expect("effort-only keeps the retained ref")
            .expect("session reports a model");
        assert_eq!(target.provider_id, "a/b");
        assert_eq!(target.id, "c");
        // The old join/re-split path selects provider "a", model id "b/c"
        // for the same retained session: this documents the regression the
        // verbatim reuse closes.
        let joined = format!("{}/{}", retained.provider_id, retained.id);
        let misparsed = split_model(&joined).expect("display form splits");
        assert_ne!(
            (misparsed.provider_id.as_str(), misparsed.id.as_str()),
            (target.provider_id.as_str(), target.id.as_str()),
            "joining and re-splitting the retained pair must not select",
        );
        // An explicit request still re-splits its own catalogue-proven pair.
        let explicit = super::configure_switch_ref(Some("opencode/kimi"), None)
            .expect("explicit splits")
            .expect("explicit target");
        assert_eq!(explicit.provider_id, "opencode");
        assert_eq!(explicit.id, "kimi");
        // Neither a request nor a retained model means no target.
        assert!(
            super::configure_switch_ref(None, None)
                .expect("absent target is not an error")
                .is_none()
        );
    }

    #[test]
    fn session_echo_compares_native_fields_without_joining() {
        // The echo verification compares provider and model id as
        // independent fields: a slash in the provider ID matches the same
        // slash provider, never the first-slash re-split. The variant is
        // checked separately by the readback, so it never affects this.
        let target = crate::native::types::ModelRef {
            id: "c".to_owned(),
            provider_id: "a/b".to_owned(),
            variant: None,
        };
        let same = crate::native::types::ModelRef {
            id: "c".to_owned(),
            provider_id: "a/b".to_owned(),
            variant: Some("low".to_owned()),
        };
        assert!(super::session_model_is(&target, &same));
        let resplit = crate::native::types::ModelRef {
            id: "b/c".to_owned(),
            provider_id: "a".to_owned(),
            variant: None,
        };
        assert!(!super::session_model_is(&target, &resplit));
        let other_provider = crate::native::types::ModelRef {
            id: "c".to_owned(),
            provider_id: "other".to_owned(),
            variant: None,
        };
        assert!(!super::session_model_is(&target, &other_provider));
    }

    #[test]
    fn slash_provider_catalogue_lookups_use_native_fields() {
        // A native provider ID containing a slash must look up by fields,
        // never by joining to "a/b/c" and re-splitting at the first slash
        // (provider "a", model "b/c"). Covers the configure effort gate
        // and the resume catalogue check alike.
        use crate::native::types::NativeModelVariant;
        let catalogue = vec![
            NativeModelEntry {
                id: Some("c".to_owned()),
                provider_id: Some("a/b".to_owned()),
                limit: None,
                name: None,
                variants: Some(vec![NativeModelVariant {
                    id: Some("low".to_owned()),
                }]),
            },
            NativeModelEntry {
                id: Some("b/c".to_owned()),
                provider_id: Some("a".to_owned()),
                limit: None,
                name: None,
                variants: Some(vec![NativeModelVariant {
                    id: Some("high".to_owned()),
                }]),
            },
        ];
        // Native-field lookups select the slash-provider row verbatim.
        assert!(super::catalogue_contains_for(&catalogue, "a/b", "c"));
        assert_eq!(
            super::catalogue_variants_for(&catalogue, "a/b", "c"),
            Some(vec!["low".to_owned()])
        );
        // The joined display form re-splits onto the other row, which is
        // why the gates must never join before looking up.
        assert_eq!(
            super::catalogue_variants(&catalogue, "a/b/c"),
            Some(vec!["high".to_owned()])
        );
        // Configure effort verification: an effort-only configure reuses
        // the retained slash-provider ref, so its wardrobe is ["low"].
        let retained = crate::native::types::ModelRef {
            id: "c".to_owned(),
            provider_id: "a/b".to_owned(),
            variant: None,
        };
        let target = super::configure_switch_ref(None, Some(&retained))
            .expect("effort-only keeps the retained ref")
            .expect("session reports a model");
        let known = super::catalogue_variants_for(&catalogue, &target.provider_id, &target.id)
            .is_some_and(|variants| variants.iter().any(|known| known == "low"));
        assert!(known, "slash-provider effort must match its own row");
        let wrong = super::catalogue_variants_for(&catalogue, &target.provider_id, &target.id)
            .is_some_and(|variants| variants.iter().any(|known| known == "high"));
        assert!(
            !wrong,
            "slash-provider effort must not read the re-split row"
        );
        // Resume catalogue check: the overlay-absent validation ref is the
        // retained native pair, so the same field lookup applies.
        let validation: Option<&crate::native::types::ModelRef> = Some(&retained);
        let row = validation.and_then(|model| {
            super::catalogue_entry_for(&catalogue, &model.provider_id, &model.id)
        });
        assert!(row.is_some_and(|entry| entry.provider_id.as_deref() == Some("a/b")));
        assert!(super::catalogue_contains_for(&catalogue, "a", "b/c"));
        assert!(!super::catalogue_contains_for(&catalogue, "a/b", "b/c"));
    }

    #[test]
    fn resume_override_resolves_the_retained_effective_model() {
        let mut params = AgentStartParams::new("a1", 1, "c1");
        let (model, model_ref, effort) = start_config_override(&params).unwrap();
        assert!(model.is_none());
        assert!(model_ref.is_none());
        assert!(effort.is_none());
        params.model_override = Some("opencode/kimi".to_owned());
        params.effort_override = Some("low".to_owned());
        let (model, model_ref, effort) = start_config_override(&params).unwrap();
        assert_eq!(model.as_deref(), Some("opencode/kimi"));
        assert_eq!(effort.as_deref(), Some("low"));
        let model_ref = model_ref.expect("override ref");
        assert_eq!(model_ref.variant.as_deref(), Some("low"));
        // An effort-only overlay carries no model ref.
        params.model_override = None;
        let (model, model_ref, effort) = start_config_override(&params).unwrap();
        assert!(model.is_none());
        assert!(model_ref.is_none());
        assert_eq!(effort.as_deref(), Some("low"));
        params.model_override = Some("noslash".to_owned());
        assert_eq!(
            start_config_override(&params).unwrap_err().code,
            "source_integrity",
            "a retained override that no longer splits is corruption"
        );
        params.model_override = None;
        params.effort_override = Some("   ".to_owned());
        let (_, _, effort) = start_config_override(&params).unwrap();
        assert!(effort.is_none(), "blank override effort is absent");
    }

    #[test]
    fn manifest_advertises_the_connected_pr2_verbs() {
        let manifest = opencode_manifest();
        assert_eq!(manifest.name, "opencode");
        assert_eq!(manifest.protocol, "eliot-plugin/1");
        assert_eq!(manifest.harness_version, "2.2.7");
        assert!(manifest.available);
        assert_eq!(manifest.mode, "per_agent");
        assert_eq!(
            manifest.verbs,
            [
                "launch",
                "resume",
                "send",
                "configure",
                "models",
                "steer",
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
        let (frozen, model, effort) = start_model_ref(profile.as_object().unwrap()).unwrap();
        assert_eq!(frozen.as_deref(), Some(requested));
        assert!(effort.is_none());
        let model = model.expect("requested model splits");
        assert_eq!(model.provider_id, "openrouter");
        assert_eq!(model.id, "stealth/crouton-alpha");
        assert_eq!(model.variant, None);
        assert_eq!(
            serde_json::to_value(&model).unwrap(),
            json!({"providerID":"openrouter", "id":"stealth/crouton-alpha"})
        );
        assert_eq!(format!("{}/{}", model.provider_id, model.id), requested);
        // A frozen effort selects the documented variant on the create.
        let profile = json!({"model": requested, "effort": "xhigh"});
        let (frozen, model, effort) = start_model_ref(profile.as_object().unwrap()).unwrap();
        assert_eq!(frozen.as_deref(), Some(requested));
        assert_eq!(effort.as_deref(), Some("xhigh"));
        let model = model.expect("requested model splits");
        assert_eq!(model.variant.as_deref(), Some("xhigh"));
        assert_eq!(
            serde_json::to_value(&model).unwrap(),
            json!({"providerID":"openrouter", "id":"stealth/crouton-alpha", "variant":"xhigh"})
        );
        // Omitted model defers to the configured native default: requested
        // stays absent separately from the effective readback.
        let absent = json!({});
        let (frozen, model, effort) = start_model_ref(absent.as_object().unwrap()).unwrap();
        assert!(frozen.is_none(), "requested absent");
        assert!(model.is_none(), "no ref without a request");
        assert!(effort.is_none(), "no effort without a request");
        assert_eq!(
            start_model_ref(json!({"effort": "low"}).as_object().unwrap())
                .unwrap_err()
                .code,
            "profile_error",
            "effort without a model has no native mapping"
        );
        assert_eq!(
            start_model_ref(
                json!({"model": requested, "effort": ""})
                    .as_object()
                    .unwrap()
            )
            .unwrap()
            .2,
            None,
            "blank effort is absent"
        );
        // Empty is absent (native default), not malformed: the daemon
        // profile path still rejects empty models before the plugin.
        for malformed in [
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

    fn fragment_profile(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        value.as_object().expect("object").clone()
    }

    fn fragment_doc() -> serde_json::Value {
        serde_json::json!({
            "k173d-fixture": {
                "package": "@opencode/ai/providers/openai-compatible",
                "settings": {
                    "baseURL": "http://127.0.0.1:1234/v1",
                    "apiKey": "{env:K173D_PROOF_KEY}",
                },
                "models": {
                    "test-model": { "name": "test-model", "tools": true },
                },
            },
        })
    }

    #[test]
    fn fragment_parses_as_is_and_rejects_mixed_or_partial() {
        let profile = fragment_profile(serde_json::json!({
            "model": "k173d-fixture/test-model",
            "provider": fragment_doc(),
        }));
        let fragment = start_provider_fragment(&profile)
            .expect("fragment")
            .expect("present");
        assert!(fragment.contains_key("k173d-fixture"));
        // Cloud profiles carry no fragment.
        let cloud = fragment_profile(
            serde_json::json!({ "model": "openrouter/m", "auth_integration": "openrouter" }),
        );
        assert!(start_provider_fragment(&cloud).expect("cloud").is_none());
        // Mixed auth refuses.
        let mixed = fragment_profile(serde_json::json!({
            "model": "k173d-fixture/test-model",
            "provider": fragment_doc(),
            "auth_integration": "openrouter",
        }));
        assert_eq!(
            start_provider_fragment(&mixed).expect_err("mixed").code,
            "profile_error"
        );
        // Missing model refuses.
        let no_model = fragment_profile(serde_json::json!({ "provider": fragment_doc() }));
        assert_eq!(
            start_provider_fragment(&no_model)
                .expect_err("no model")
                .code,
            "profile_error"
        );
        // Empty fragment refuses.
        let empty = fragment_profile(
            serde_json::json!({ "model": "k173d-fixture/test-model", "provider": {} }),
        );
        assert_eq!(
            start_provider_fragment(&empty).expect_err("empty").code,
            "profile_error"
        );
    }

    #[test]
    fn fragment_auth_selector_is_absent_while_cloud_requires_a_selector() {
        let cloud = fragment_profile(serde_json::json!({ "auth_integration": "openrouter" }));
        assert!(start_auth_selector(&cloud, false).expect("cloud").is_some());
        assert_eq!(
            start_auth_selector(&fragment_profile(serde_json::json!({})), false)
                .err()
                .expect("missing cloud selector")
                .code,
            "profile_error"
        );
        let mixed = fragment_profile(serde_json::json!({ "auth_integration": "openrouter" }));
        assert_eq!(
            start_auth_selector(&mixed, true)
                .err()
                .expect("fragment mixed auth")
                .code,
            "profile_error"
        );
        assert!(
            start_auth_selector(&fragment_profile(serde_json::json!({})), true)
                .expect("fragment none")
                .is_none()
        );
    }

    #[test]
    fn fragment_environment_requires_non_empty_reference() {
        use std::ffi::OsString;
        let mut fragment = crate::native::server::ProviderFragment::new();
        fragment.insert(
            "k173d-fixture".to_owned(),
            serde_json::json!({ "settings": { "apiKey": "{env:K173D_PROOF_KEY}" } }),
        );
        let empty: Vec<(OsString, OsString)> = Vec::new();
        assert!(crate::native::server::validate_fragment_environment(&fragment, &empty).is_err());
        let env = vec![(OsString::from("K173D_PROOF_KEY"), OsString::from("secret"))];
        assert!(crate::native::server::validate_fragment_environment(&fragment, &env).is_ok());
    }

    #[test]
    fn proven_no_effect_start_returns_failed_state_and_replays() {
        let params = AgentStartParams::new("agent-1", 7, "0199-start-1");
        let mut plugin = OpenCodePlugin::default();
        let value = plugin.retain_proven_no_effect_start(
            &params,
            StartPhase::Verify,
            eliot_core::Error::new("native_readback", "native catalogue unreachable"),
        );
        assert_eq!(value["state"], "failed");
        assert_eq!(value["agent_id"], "agent-1");
        assert_eq!(value["generation"], 7);
        assert_eq!(value["command_id"], "0199-start-1");
        assert_eq!(value["reason"], "native catalogue unreachable");
        match plugin
            .start_replay_admission(&params)
            .expect("admission")
            .expect("replay")
        {
            StartAdmission::Replay(Ok(replayed)) => assert_eq!(replayed, value),
            StartAdmission::Replay(Err(_)) => panic!("proven failure must replay a value"),
            StartAdmission::New(_) => panic!("settled start must replay"),
        }
        let other = AgentStartParams::new("agent-1", 7, "0199-start-2");
        match plugin.start_replay_admission(&other) {
            Err(error) => assert_eq!(error.code, "bad_state"),
            Ok(_) => panic!("a different start must be refused"),
        }
    }

    /// Pre-session classification: every phase before the first create write
    /// proves no native session or prompt exists and settles definite failed,
    /// except a Spawn failure carrying code `unknown` (the uncertain auth
    /// import, which may already have written the isolated credential store).
    /// Only the post-create phase otherwise stays uncertain. Adding a phase
    /// forces an explicit entry here.
    #[test]
    fn presession_phases_classify_failed_before_native_session() {
        for phase in [
            StartPhase::Spawn,
            StartPhase::Subscribe,
            StartPhase::Register,
            StartPhase::Verify,
            StartPhase::CreateRefused,
            StartPhase::ResumeRefused,
        ] {
            for code in [
                "native_spawn",
                "native_readback",
                "rejected",
                "profile_error",
            ] {
                assert!(
                    start_failure_is_proven_no_effect(phase, code),
                    "pre-session failure must be proven-no-effect: {phase:?} {code}"
                );
            }
        }
        assert!(!start_failure_is_proven_no_effect(
            StartPhase::Spawn,
            "unknown"
        ));
        assert!(!start_failure_is_proven_no_effect(
            StartPhase::PostCreate,
            "native_spawn"
        ));
        assert!(!start_failure_is_proven_no_effect(
            StartPhase::PostCreate,
            "unknown"
        ));
    }

    /// Uncertain auth import stays unknown: a Spawn failure with code
    /// `unknown` (the import may already have written the isolated credential
    /// store and reconciliation failed) is never proven-no-effect, while
    /// every definite refusal before the import settles failed.
    #[test]
    fn spawn_uncertain_import_stays_unknown() {
        assert!(!start_failure_is_proven_no_effect(
            StartPhase::Spawn,
            "unknown"
        ));
        for code in [
            "native_spawn",
            "native_readback",
            "profile_error",
            "invalid",
        ] {
            assert!(
                start_failure_is_proven_no_effect(StartPhase::Spawn, code),
                "definite refusal before the import must settle failed: {code}"
            );
        }
        // The uncertain spawn retains the error for replay instead of
        // converting to a failed-state value.
        let params = AgentStartParams::new("agent-9", 1, "0199-uncertain");
        let mut plugin = OpenCodePlugin::default();
        let error = plugin.retain_failed_start(&params, eliot_core::Error::new("unknown", "probe"));
        assert_eq!(error.code, "unknown");
        match plugin
            .start_replay_admission(&params)
            .expect("admission")
            .expect("replay")
        {
            StartAdmission::Replay(Err(replayed)) => assert_eq!(replayed.code, "unknown"),
            StartAdmission::Replay(Ok(_)) => panic!("uncertain failure must replay an error"),
            StartAdmission::New(_) => panic!("settled start must replay"),
        }
    }

    /// The readiness deadline is the real-signal bound and the start envelope
    /// always fits the daemon agent.start budget: the plugin, cleanup
    /// included, answers ready, failed, or unknown before the daemon retires
    /// the Job and settles, so a refusal at the edge of the budget settles
    /// failed and leaves `starting` instead of racing the daemon deadline.
    #[test]
    fn readiness_and_envelope_bounds_fit_daemon_start_budget() {
        assert!(
            READINESS_TIMEOUT <= START_ENVELOPE,
            "readiness must fit inside the start envelope"
        );
        assert!(
            START_CLEANUP_BUDGET <= START_ENVELOPE,
            "failure cleanup must fit inside the start envelope"
        );
        assert!(
            START_ENVELOPE + START_SETTLE_RESERVE <= DAEMON_START_TIMEOUT,
            "envelope must leave Job-retirement and settlement margin"
        );
    }

    #[test]
    fn late_create_leaves_the_full_cleanup_reserve() {
        let cutoff = super::Instant::now() + std::time::Duration::from_secs(1);
        let envelope = cutoff + START_CLEANUP_BUDGET;
        assert_eq!(super::create_cutoff(envelope), cutoff);
        assert_eq!(super::create_deadline(cutoff), cutoff);
        assert_eq!(
            envelope - cutoff,
            super::START_DELETE_BUDGET + super::START_SHUTDOWN_BUDGET
        );
    }

    #[test]
    fn unproved_created_session_removal_reports_unknown_with_exact_id() {
        let error = super::created_start_unknown(
            "ses_exact",
            "OpenCode session policy verification failed",
            "the created native session kill is unconfirmed",
        );
        assert_eq!(error.code, "unknown");
        assert_eq!(
            error.message.as_str(),
            "OpenCode session ses_exact is unresolved (OpenCode session policy verification failed; the created native session kill is unconfirmed)"
        );
    }

    /// Phase deadlines share the envelope: full budget with room left, the
    /// remainder when spent, and fail-fast zero for duration-based callees.
    #[test]
    fn phase_deadlines_clamp_to_the_dispatch_envelope() {
        let roomy = super::Instant::now() + std::time::Duration::from_secs(3600);
        let capped = phase_deadline(roomy, std::time::Duration::from_secs(30));
        assert!(capped > super::Instant::now());
        assert!(capped <= super::Instant::now() + std::time::Duration::from_secs(30));
        let spent = super::Instant::now();
        assert_eq!(
            phase_deadline(spent, std::time::Duration::from_secs(30)),
            spent
        );
        assert_eq!(
            phase_budget(spent, std::time::Duration::from_secs(30)),
            std::time::Duration::ZERO
        );
    }

    /// Failure cleanup at the edge of the budget fails fast: with a spent
    /// envelope the cleanup budget is zero, so a refusal at the envelope
    /// edge settles the failed value at once instead of stacking another
    /// 30 s past the daemon deadline (which would lose the reply and pin
    /// the Agent in `starting`).
    #[test]
    fn spent_envelope_fails_failure_cleanup_fast() {
        let spent = super::Instant::now();
        assert_eq!(phase_deadline(spent, START_CLEANUP_BUDGET), spent);
        assert_eq!(
            phase_budget(spent, START_CLEANUP_BUDGET),
            std::time::Duration::ZERO
        );
    }

    /// Proven reasons are typed by phase plus triggering code: static text
    /// only, so a persisted failed outcome can never carry native text,
    /// endpoints, or secrets.
    #[test]
    fn presession_reasons_name_phase_and_code() {
        assert_eq!(
            presession_reason(StartPhase::Spawn, "native_spawn"),
            "OpenCode serve failed to start (native_spawn); no native session or prompt was dispatched by this generation"
        );
        assert_eq!(
            presession_reason(StartPhase::CreateRefused, "rejected"),
            "OpenCode session/create was not admitted (rejected); no native session or prompt was dispatched by this generation"
        );
        for code in ["native_spawn", "unknown", "profile_error"] {
            let reason = presession_reason(StartPhase::Subscribe, code);
            assert!(reason.contains(code));
        }
    }

    /// Every pre-session phase converts to the retained failed value through
    /// the same converter the start dispatch uses.
    #[test]
    fn every_presession_phase_converts_to_a_retained_failed_value() {
        for phase in [
            StartPhase::Spawn,
            StartPhase::Subscribe,
            StartPhase::Register,
            StartPhase::Verify,
            StartPhase::CreateRefused,
            StartPhase::ResumeRefused,
        ] {
            let params = AgentStartParams::new("agent-9", 1, "0199-presession");
            let mut plugin = OpenCodePlugin::default();
            let value = plugin.proven_no_effect_start(
                &params,
                PhaseFailure {
                    phase,
                    error: Box::new(
                        eliot_core::Error::new("native_spawn", "probe")
                            .with_where("plugin.opencode.start.spawn")
                            .with_origin(
                                eliot_core::FailureOrigin::Environment,
                                "OS refused native spawn",
                            ),
                    ),
                },
            );
            assert_eq!(value["state"], "failed");
            assert_eq!(value["agent_id"], "agent-9");
            assert_eq!(value["origin"], "environment");
            assert_eq!(value["where"], "plugin.opencode.start.spawn");
            assert_eq!(value["origin_evidence"], "OS refused native spawn");
            assert_eq!(value["command_id"], "0199-presession");
            match plugin
                .start_replay_admission(&params)
                .expect("admission")
                .expect("replay")
            {
                StartAdmission::Replay(Ok(replayed)) => assert_eq!(replayed, value),
                StartAdmission::Replay(Err(_)) => panic!("proven failure must replay a value"),
                StartAdmission::New(_) => panic!("settled start must replay"),
            }
        }
    }

    #[test]
    fn refused_model_at_start_is_failed_with_harness_origin() {
        let params = AgentStartParams::new("agent-1", 7, "0199-model-ghost");
        let mut plugin = OpenCodePlugin::default();
        let error = eliot_plugin::start_classification::harness_unavailable_model(
            "opencode",
            "create_refused",
            "model is missing",
        );
        let value = plugin.retain_proven_no_effect_start(&params, StartPhase::CreateRefused, error);
        assert_eq!(value["state"], "failed");
        assert_eq!(value["origin"], "harness");
        assert!(start_failure_is_proven_no_effect(
            StartPhase::CreateRefused,
            "native_model_unavailable"
        ));
        assert!(eliot_plugin::start_classification::is_proven_no_effect(
            eliot_plugin::start_classification::StartPoint::NativeRefusalBeforeDispatch
        ));
    }
}
