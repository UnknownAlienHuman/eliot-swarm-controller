use crate::{
    events::{EventConfig, EventHandle, EventStatus, InboxRecord, SteerVerdict, TurnFacts},
    native::{
        AuthSetup, BackendKind, HttpOutcome, IntegrationSelector, LocalProvider, OpenCodeHttp,
        OwnedServer, SWARM_MCP_SERVER_NAME, ServerLaunch,
        models::{run_model_variants, run_models},
        types::{InboxUser, McpServerReadback, ModelRef, NativeModelEntry, SessionInfo},
    },
};
use eliot_core::{ContextItem, Error, FailureOrigin, InjectPoint, McpPolicy, Result};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    session::{
        ResumeSubject, TargetEnvironment, decode, known_command, native_no_write, not_initialized,
        optional_profile_text, parse_resume_subject, paths_equal, register_native_process,
        split_provider_model, target_environment, unknown_snapshot, validate_binding,
        validate_identity, validate_source_plan, with_claimed_context, with_context_points,
    },
};
use reqwest::{redirect, retry};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
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
/// child, never enough to outlive the daemon budget.
const START_CLEANUP_BUDGET: std::time::Duration = std::time::Duration::from_secs(10);

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
    /// Local backend verification, before session create: the adopted
    /// provider answers for the expected model without creating state.
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
        StartPhase::Verify => "OpenCode local backend verification failed",
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
    outcome: SettledStartFailure,
}

/// How one retained start failure replays. `Uncertain` keeps the historical
/// error replay (the daemon settles by error code, and a native effect may
/// have happened). `ProvenNoEffect` carries the failed-state result value:
/// no native session or prompt was ever dispatched, so the launch is
/// definitely failed and the daemon retires the plugin and releases capacity
/// on proven exit instead of pinning an unknown Agent.
enum SettledStartFailure {
    Uncertain(Error),
    ProvenNoEffect(Value),
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
/// integration (cloud path) or adopted local provider (local path), and
/// redaction secrets. Exactly one of `auth_selector` and `local` is set:
/// the local path never imports a canonical credential.
struct ParsedStart {
    params: AgentStartParams,
    /// Frozen native-subagent switch (`allow` or `deny`): `deny` disables
    /// the `task` tool in the isolated per-Agent config.
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
    local: Option<LocalProvider>,
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

    /// Read-only model list for `harness.models`: the native `opencode
    /// models` catalogue (`provider/model` ids verbatim) with the
    /// selectable `Model.Variant` ids overlaid as `efforts` from the
    /// installed `api model.list` operation (`GET /api/model`). No
    /// session, no model turn and no inference. A failed variant read
    /// degrades to the ids-only catalogue; an omitted model still leaves
    /// the configured native default, read back from the session at
    /// launch.
    async fn model_list(&self, raw: &Value) -> Result<Value> {
        if self.session.is_some() || self.failed_start.is_some() {
            return Err(Error::new(
                "bad_state",
                "OpenCode model.list requires a dedicated reader plugin instance",
            ));
        }
        let profile: Value = decode(raw, "OpenCode model.list params are invalid")?;
        let snapshot = profile.get("profile").unwrap_or(&profile);
        if snapshot.get("harness").and_then(Value::as_str) != Some("opencode") {
            return Err(Error::new(
                "profile_error",
                "OpenCode model.list received another harness",
            ));
        }
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
        let ids = run_models(std::ffi::OsStr::new(program), Instant::now() + RPC_TIMEOUT).await?;
        let variants =
            run_model_variants(std::ffi::OsStr::new(program), Instant::now() + RPC_TIMEOUT)
                .await
                .ok();
        let models: Vec<Value> = ids
            .into_iter()
            .map(|id| {
                let mut entry = serde_json::Map::from_iter([
                    ("id".to_owned(), Value::String(id.clone())),
                    ("default".to_owned(), Value::Bool(false)),
                ]);
                if let Some(overlay) = variants
                    .as_ref()
                    .and_then(|overlay| overlay.iter().find(|candidate| candidate.id == id))
                {
                    entry.insert(
                        "efforts".to_owned(),
                        Value::Array(
                            overlay
                                .variants
                                .iter()
                                .map(|variant| Value::String(variant.clone()))
                                .collect(),
                        ),
                    );
                }
                Value::Object(entry)
            })
            .collect();
        Ok(json!({"models": models, "default": Value::Null}))
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
        if model_ref
            .as_ref()
            .is_some_and(|model_ref| model_ref.variant.is_some())
            && has_local_fields(profile)
        {
            return Err(Error::new(
                "profile_error",
                "OpenCode effort has no adopted-backend mapping",
            ));
        }
        // A local-backend profile always carries an explicit model (the
        // eliot-local segment): local fields without a model fail closed
        // instead of falling back to the native default.
        let local = match model_ref.as_ref() {
            Some(model_ref) => parse_local_provider(profile, model_ref)?,
            None => {
                if has_local_fields(profile) {
                    return Err(Error::new(
                        "profile_error",
                        "OpenCode local backend requires an explicit model with the dedicated eliot-local provider segment",
                    ));
                }
                None
            }
        };
        let auth_selector = start_auth_selector(profile, local.is_some())?;
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
            local,
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
                return Ok(Some(StartAdmission::Replay(match &failed.outcome {
                    SettledStartFailure::Uncertain(error) => Err(error.clone()),
                    SettledStartFailure::ProvenNoEffect(value) => Ok(value.clone()),
                })));
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
    /// The cloud path carries the selected native integration; the local
    /// path carries the adopted provider and no canonical credential.
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
                local: parsed.local.clone(),
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
    /// mismatch best-effort deletes the created session and surfaces the
    /// kill result; no session is kept.
    ///
    /// Adopted local path: the provider, catalogue, limits and backend
    /// inventory verification runs BEFORE `POST /api/session` creates any
    /// native session — right here at the top, after the owned server is up
    /// but before the first native write. Nothing has been dispatched at
    /// that point, so every verification failure (mismatch, unreachable
    /// backend, or unreadable readback) settles as a definite failed start
    /// (`ProvenFailed`, retained for a corrected retry), never as an unknown
    /// that would pin the Agent, keep the plugin alive, and hold capacity.
    /// Unknown is reserved for failures after a session or prompt may have
    /// been admitted.
    async fn create_start_session(
        &mut self,
        parsed: &ParsedStart,
        mut session: AgentSession,
        envelope: Instant,
    ) -> Result<CreateOutcome> {
        let _timing = crate::native::startup::Step::new("session_create_verify");
        session.native_scope = opencode_scope(session.server.data_dir());
        // Pre-create verification: no native session or prompt exists yet.
        let local_fragment = if let Some(local) = parsed.local.as_ref() {
            let backend_key = local_key_value(&parsed.environment, local.api_key_env.as_deref());
            match verify_local_effective(
                &session.http,
                local,
                backend_key,
                phase_deadline(envelope, RPC_TIMEOUT),
            )
            .await
            {
                Ok(fragment) => Some(fragment),
                Err(error) => {
                    let failure = error.clone();
                    discard_failed_start(&mut session, error, envelope).await;
                    return Ok(CreateOutcome::ProvenFailed(
                        self.retain_proven_no_effect_start(
                            &parsed.params,
                            StartPhase::Verify,
                            failure,
                        ),
                    ));
                }
            }
        } else {
            None
        };
        // A requested effort selects the documented `Model.Ref.variant` on
        // the create below: the installed server echoes even bogus
        // switches, so the live catalogue proves the variant belongs to
        // the requested model before the session exists. A miss or an
        // unreadable catalogue is a definite failed start, never unknown.
        if let Some(requested) = parsed.requested_model.as_deref()
            && let Some(variant) = parsed
                .model_ref
                .as_ref()
                .and_then(|model_ref| model_ref.variant.as_deref())
        {
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
            let known = catalogue_variants(&catalogue, requested)
                .is_some_and(|variants| variants.iter().any(|known| known == variant));
            if !known {
                let failure = Error::new(
                    "profile_error",
                    format!("OpenCode effort {variant} is not a variant of model {requested}"),
                );
                discard_failed_start(&mut session, failure.clone(), envelope).await;
                return Ok(CreateOutcome::ProvenFailed(
                    self.retain_proven_no_effect_start(&parsed.params, StartPhase::Verify, failure),
                ));
            }
        }
        // Workspace project MCP layer: the owned serve already merged it,
        // so an owner-identity entry there must refuse the start before the
        // native session exists. Every policy, whatever the entry is named.
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
        let created = match session
            .http
            .create_session(
                &title,
                parsed.model_ref.as_ref(),
                &directory,
                phase_deadline(envelope, RPC_TIMEOUT),
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
        session.session_id = Some(created.id.clone());
        // Effective readback before readiness: the create response states the
        // exact model and location; on any mismatch no `session.ready` is
        // published and no session is kept.
        let effective_model = created
            .model
            .as_ref()
            .map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim()));
        // An omitted request accepts the configured native default as
        // effective; an explicit request must match exactly. A requested
        // effort must echo as the native variant.
        let model_verified = parsed
            .requested_model
            .as_deref()
            .is_none_or(|requested| effective_model.as_deref() == Some(requested));
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
            "model_variant": effective_variant.clone(),
            "effort": effective_variant,
            "effort_verified": effort_verified,
            "cwd": created.location.as_ref().map(|location| location.directory.clone()),
        });
        // The adopted provider fragment was proven before the session
        // existed; merge it into the effective readback here. Post-create
        // failures below keep their historical settlement: the create may
        // have admitted native state, so they stay errors (unknown where
        // ambiguous), never the pre-create failed value.
        if let Some(fragment) = local_fragment {
            effective["local"] = fragment;
        }
        // Agent-scoped MCP readback after the session exists: names plus
        // Agent-scoped MCP readback after the session exists: names plus
        // the connected verdict for the `eliot-swarm` entry. Observation
        // stays read-only and best-effort; the inventory gate below may
        // still refuse the start on an unexpected or owner-identity server.
        let (mcp_servers, mcp_servers_verified, mcp_inventory) =
            observe_mcp_servers(&session.http, phase_deadline(envelope, RPC_TIMEOUT)).await;
        if let Err(error) =
            gate_mcp_inventory(mcp_inventory.as_deref(), parsed.requested_mcp_policy)
        {
            let kill_note = delete_created_session(&mut session, &created.id, envelope).await;
            let error = discard_failed_start(
                &mut session,
                Error::new("profile_error", format!("{}; {kill_note}", error.message)),
                envelope,
            )
            .await;
            return Err(self.retain_failed_start(&parsed.params, error));
        }
        effective["mcp_servers"] = mcp_servers;
        effective["mcp_servers_verified"] = json!(mcp_servers_verified);
        effective["mcp_policy"] = json!(parsed.requested_mcp_policy.as_str());
        let context_bound = bind_context_hooks(
            &session.http,
            &created.id,
            phase_deadline(envelope, RPC_TIMEOUT),
        )
        .await;
        let effective = if context_bound {
            with_context_points(
                effective,
                &[InjectPoint::NextInput, InjectPoint::TurnSafePoint],
            )
        } else {
            with_context_points(effective, &[InjectPoint::NextInput])
        };
        if !model_verified || !effort_verified || !location_ok {
            let kill_note = delete_created_session(&mut session, &created.id, envelope).await;
            let error = discard_failed_start(
                &mut session,
                Error::new(
                    "profile_error",
                    format!(
                        "OpenCode effective model, effort or session directory did not match the frozen profile; {kill_note}"
                    ),
                ),
                envelope,
            )
            .await;
            return Err(self.retain_failed_start(&parsed.params, error));
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
    /// exists and is idle, the model/agent realign to the frozen profile
    /// with an effective readback, and readiness adopts the live edge.
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
        if let Err(error) = Self::resume_quiesce(&session, &retained.id, deadline).await {
            return Err(self.stash_resume_unknown(session, *error.message).await);
        }
        if let Err(error) = Self::resume_verify_idle(&session, &retained.id, deadline).await {
            return Err(self.stash_resume_unknown(session, *error.message).await);
        }
        let effective = match Self::resume_configure(&parsed, &session, &retained.id, deadline)
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
        let model_needed = target_model.is_some_and(|target| {
            current
                .model
                .as_ref()
                .map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim()))
                != Some(target.to_owned())
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
        let effective = match target_model {
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
        let mut effective_value = json!({
            "model": effective.model.as_ref().map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim())),
            "model_verified": model_verified,
            "model_variant": effective_variant.clone(),
            "effort": effective_variant,
            "effort_verified": effort_verified,
            "agent": effective.agent.clone(),
            "agent_verified": agent_verified,
            "cwd": effective.location.as_ref().map(|location| location.directory.clone()),
        });
        // Adopted local path: the regenerated server config must still pin
        // the frozen provider. A known mismatch fails the resume for a
        // corrected retry; transport ambiguity stays unknown.
        if let Some(local) = parsed.local.as_ref() {
            let backend_key = local_key_value(&parsed.environment, local.api_key_env.as_deref());
            match verify_local_effective(&session.http, local, backend_key, deadline).await {
                Ok(fragment) => {
                    effective_value["local"] = fragment;
                }
                Err(error) => return Err(error),
            }
        }
        // Same best-effort MCP readback as the create path: the resumed
        // server re-spawns the Agent-scoped entry for this generation.
        let (mcp_servers, mcp_servers_verified, mcp_inventory) =
            observe_mcp_servers(&session.http, deadline).await;
        gate_mcp_inventory(mcp_inventory.as_deref(), parsed.requested_mcp_policy)?;
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
    /// no-effect refusal to its `rejected` value.
    async fn readback_session_model(
        http: &OpenCodeHttp,
        session_id: &str,
        target: &str,
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
        let echoed_model = echoed
            .model
            .as_ref()
            .map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim()));
        if echoed_model.as_deref() != Some(target) {
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
        let current_model = current
            .model
            .as_ref()
            .map(|model| format!("{}/{}", model.provider_id.trim(), model.id.trim()));
        let current_variant = current
            .model
            .as_ref()
            .and_then(|model| model.variant.clone());
        let Some(target_model) = requested.model.clone().or(current_model.clone()) else {
            return Ok(json!({
                "state": "rejected",
                "reason": "OpenCode configure effort needs a session model",
            }));
        };
        if let Some(effort) = requested.effort.as_deref() {
            let known = catalogue_variants(&catalogue, &target_model)
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
        let switch_needed = Some(target_model.as_str()) != current_model.as_deref()
            || target_variant.as_deref() != current_variant.as_deref();
        if switch_needed {
            let mut model_ref = split_model(&target_model).ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "OpenCode retained session model is not provider/model with non-empty slash segments",
                )
            })?;
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
            &target_model,
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

    /// Steer the exact active turn: one `session.prompt` write with
    /// explicit `delivery: steer`, then the event task proves the input
    /// was delivered while the exact turn stayed active. Native joins
    /// such an input to the running turn at a step boundary; the write
    /// never opens a turn here (`command_turns` untouched, no send-lane
    /// association) and never emulates interruption. A finished turn is
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
        let admitted = match Self::admit_control_command(session, &params, false)? {
            ControlAdmission::Replay(value) => return Ok(value),
            ControlAdmission::New {
                command_id,
                turn_id,
                ..
            } => (command_id, turn_id),
        };
        let (command_id, turn_id) = admitted;
        let text = match params.args.get("text").and_then(Value::as_str) {
            Some(text) => text.to_owned(),
            None => {
                Self::abandon_control(session, &command_id);
                return Err(Error::new("invalid", "OpenCode steer requires args.text"));
            }
        };
        if text.len() > MAX_TEXT_BYTES {
            Self::abandon_control(session, &command_id);
            return Err(Error::new(
                "frame_too_large",
                "OpenCode input exceeds 512 KiB",
            ));
        }
        let Some(turn_id) = turn_id else {
            Self::abandon_control(session, &command_id);
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
                Self::abandon_control(session, &command_id);
                return Err(Error::new(
                    "unknown",
                    "OpenCode steer turn check is unresolved",
                ));
            }
        };
        if !live {
            Self::abandon_control(session, &command_id);
            return Err(Error::new(
                "stale_turn",
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
                    session.unknown_commands.insert(command_id.clone());
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
                Self::settle_control(session, &command_id, result.clone());
                return Ok(result);
            }
            Err(HttpOutcome::Unknown { reason }) => {
                // The input may have been admitted without an inbox id to
                // watch or cancel: latch unknown like the send path and
                // never re-send (the control binding stays).
                session.unknown_commands.insert(command_id.clone());
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
                    &command_id,
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
                Self::settle_control(session, &command_id, result.clone());
                Ok(result)
            }
            Ok(Ok(SteerVerdict::Missed)) => Err(Self::settle_steer_unproven(
                session,
                &session_id,
                &inbox.id,
                &command_id,
                "OpenCode steer missed its turn; the input did not join the exact turn",
            )
            .await),
            Ok(Err(_)) | Err(_) => {
                session.events.abandon_steer(command_id.clone()).await;
                Err(Self::settle_steer_unproven(
                    session,
                    &session_id,
                    &inbox.id,
                    &command_id,
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
        command_id: &str,
        message: &str,
    ) -> Error {
        let deadline = Instant::now() + RPC_TIMEOUT;
        let _ = session
            .http
            .cancel_inbox(session_id, inbox_id, deadline)
            .await;
        let removed = Self::steer_input_removed(session, session_id, inbox_id).await;
        session.unknown_commands.insert(command_id.to_owned());
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
/// transfer prepares. Required on the cloud path and nonsecret; the
/// capacity-only account field is never consulted for authentication. On
/// the adopted local path the selector must be absent: no canonical
/// credential is ever imported there.
fn start_auth_selector(
    profile: &serde_json::Map<String, Value>,
    is_local: bool,
) -> Result<Option<IntegrationSelector>> {
    let raw = profile
        .get("auth_integration")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty());
    if is_local {
        if raw.is_some() {
            return Err(Error::new(
                "profile_error",
                "OpenCode local backend profiles must not name a canonical auth_integration",
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

/// Dedicated local provider ID without a canonical alias. The frozen model
/// on an adopted-backend profile must use this provider segment; the daemon
/// freezes it, and this check keeps a hand-built snapshot honest.
const LOCAL_PROVIDER_ID: &str = "eliot-local";

/// Expected native package rendering the adopted OpenAI-compatible route.
const LOCAL_PROVIDER_PACKAGE: &str = "@opencode/ai/providers/openai-compatible";

/// The frozen adopted local provider: `provider_base_url`, `backend`,
/// `context_limit` and `output_limit` are all required with an optional
/// Any adopted-backend field present means the profile intends the local
/// path, with or without an explicit model; the caller rejects the
/// model-absent combination as a profile error.
fn has_local_fields(profile: &serde_json::Map<String, Value>) -> bool {
    profile.contains_key("provider_base_url")
        || profile.contains_key("backend")
        || profile.contains_key("api_key_env")
        || profile.contains_key("context_limit")
        || profile.contains_key("output_limit")
        || profile.contains_key("provider_package")
}

/// `api_key_env` reference (absent means explicit no-auth mode). Key values
/// stay runtime-only; only the env name is frozen. Any local field without
/// the full set fails closed instead of falling back to another auth mode.
fn parse_local_provider(
    profile: &serde_json::Map<String, Value>,
    model_ref: &ModelRef,
) -> Result<Option<LocalProvider>> {
    if !has_local_fields(profile) {
        return Ok(None);
    }
    if model_ref.provider_id != LOCAL_PROVIDER_ID {
        return Err(Error::new(
            "profile_error",
            "OpenCode local backend profiles must use the dedicated eliot-local provider segment",
        ));
    }
    if profile.contains_key("auth_integration") {
        return Err(Error::new(
            "profile_error",
            "OpenCode local backend profiles must not name a canonical auth_integration",
        ));
    }
    let base_url = profile
        .get("provider_base_url")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "OpenCode local backend requires an explicit provider_base_url",
            )
        })?
        .trim()
        .to_owned();
    if base_url.len() > 2048
        || base_url.chars().any(char::is_control)
        || (!base_url.starts_with("http://") && !base_url.starts_with("https://"))
    {
        return Err(Error::new(
            "profile_error",
            "OpenCode local backend provider_base_url must be an absolute http(s) endpoint URL",
        ));
    }
    let backend_raw = profile
        .get("backend")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "OpenCode local backend requires an explicit backend kind",
            )
        })?;
    let backend = BackendKind::parse(backend_raw).ok_or_else(|| {
        Error::new(
            "profile_error",
            "OpenCode local backend kind must be strata, unsloth, lmstudio, llama.cpp, ollama, vllm or generic",
        )
    })?;
    let limit = |key: &str| {
        profile
            .get(key)
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    format!("OpenCode local backend requires a positive {key}"),
                )
            })
    };
    let context_limit = limit("context_limit")?;
    let output_limit = limit("output_limit")?;
    let api_key_env = match profile.get("api_key_env") {
        None | Some(Value::Null) => None,
        Some(Value::String(name)) if !name.trim().is_empty() => Some(name.trim().to_owned()),
        _ => {
            return Err(Error::new(
                "profile_error",
                "OpenCode local backend api_key_env must be a non-empty environment name",
            ));
        }
    };
    let package = profile
        .get("provider_package")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(LOCAL_PROVIDER_PACKAGE)
        .to_owned();
    if package != LOCAL_PROVIDER_PACKAGE {
        return Err(Error::new(
            "profile_error",
            "OpenCode local backend provider package contradicts the installed native schema",
        ));
    }
    Ok(Some(LocalProvider {
        provider_id: model_ref.provider_id.clone(),
        api_model_id: model_ref.id.clone(),
        base_url,
        api_key_env,
        context_limit,
        output_limit,
        backend: backend.as_str().to_owned(),
        package,
    }))
}

/// Compare endpoint spellings after trimming trailing slashes. The frozen
/// and observed values must name the same route; project overrides that
/// retarget the base URL are a profile mismatch, never silently adopted.
fn normalize_endpoint(value: &str) -> String {
    value.trim().trim_end_matches('/').to_owned()
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
const MCP_SETTLE_POLL: std::time::Duration = std::time::Duration::from_secs(1);

/// Best-effort Agent-scoped MCP readback for the effective record:
/// `GET /api/mcp` (`mcp.list`) after the session exists, re-polled until
/// the Agent-scoped server reports `connected` (or a terminal
/// non-connected status, or the settle cap). Names come from the installed
/// inventory when a query succeeds (`null` when none does, never
/// fabricated); `mcp_servers_verified` is true only on `connected`. Only
/// names and the status word are retained — never the failure text, which
/// can carry local paths. A non-connected or unreadable surface never
/// fails the start: this harness keeps `inherit` only, so the applied
/// per-Agent config is the qualified record and this observation is
/// reported honestly.
async fn observe_mcp_servers(
    http: &OpenCodeHttp,
    deadline: Instant,
) -> (Value, bool, Option<Vec<McpServerReadback>>) {
    let cap =
        Instant::now() + MCP_SETTLE_TIMEOUT.min(deadline.saturating_duration_since(Instant::now()));
    let mut last: Option<Vec<McpServerReadback>> = None;
    loop {
        if let Ok(servers) = http.list_mcp_servers(deadline).await {
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
        tokio::time::sleep(MCP_SETTLE_POLL).await;
    }
    match last {
        Some(servers) => (mcp_names(&servers), false, Some(servers)),
        None => (Value::Null, false, None),
    }
}

/// Refuse the start when the workspace project MCP layer contributes the
/// owner Swarm identity under any name. The owned serve already merged
/// that layer, so the scan runs before the native session exists (create)
/// or before the retained session is touched (resume), in every policy.
/// No project file is read beyond the documented names, and none is
/// ever edited.
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

/// Exclusive inventory gate for the worker MCP surface, applied after the
/// settle observation on both the create and resume paths. An unreadable
/// inventory reports unverified but never refuses: absence of evidence is
/// not an unexpected server. Otherwise:
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
        return Ok(());
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

/// Resolve the adopted backend key value (runtime-only) from the launch
/// environment for the passive inventory read. A missing or empty value
/// yields `None` so the read runs unauthenticated only when the profile
/// selected no-auth mode; it never fabricates an empty-string credential.
fn local_key_value<'a>(
    environment: &'a [(OsString, OsString)],
    name: Option<&str>,
) -> Option<&'a str> {
    let name = name?;
    environment
        .iter()
        .find_map(|(key, value)| {
            let matches = if cfg!(windows) {
                key.to_string_lossy().eq_ignore_ascii_case(name)
            } else {
                key.to_string_lossy() == *name
            };
            matches.then(|| value.to_str()).flatten()
        })
        .filter(|value| !value.trim().is_empty())
}

/// Passive adopted-backend inventory: one bounded `GET {base_url}/models`
/// with the scoped key reference only. This never generates text, never
/// sends `?reload=1` (which can unload running llama.cpp models), and never
/// touches load/switch selectors. `None` reports unavailable evidence
/// honestly (listing disabled or unreachable); `Some(ids)` lets the caller
/// reject a known mismatch against the frozen API model id.
async fn fetch_backend_inventory(
    base_url: &str,
    backend: BackendKind,
    api_key: Option<&str>,
    deadline: Instant,
) -> Option<Vec<String>> {
    const MAX_INVENTORY_BYTES: usize = 64 * 1024;
    let url = format!(
        "{}{}",
        base_url.trim_end_matches('/'),
        backend.inventory_path()
    );
    if url.len() > 4096 {
        return None;
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(redirect::Policy::none())
        .retry(retry::never())
        .build()
        .ok()?;
    let mut request = client.get(url);
    if let Some(key) = api_key.filter(|key| !key.trim().is_empty()) {
        request = request.bearer_auth(key);
    }
    let mut response = tokio::time::timeout_at(deadline, request.send())
        .await
        .ok()?
        .ok()?;
    if response.status() != reqwest::StatusCode::OK {
        return None;
    }
    if response
        .content_length()
        .is_some_and(|length| length > MAX_INVENTORY_BYTES as u64)
    {
        return None;
    }
    let mut body = Vec::new();
    loop {
        let chunk = tokio::time::timeout_at(deadline, response.chunk())
            .await
            .ok()?
            .ok()?;
        match chunk {
            Some(part) => {
                if body.len().saturating_add(part.len()) > MAX_INVENTORY_BYTES {
                    return None;
                }
                body.extend_from_slice(&part);
            }
            None => break,
        }
    }
    let value: Value = serde_json::from_slice(&body).ok()?;
    // OpenAI-compatible list shape `{object: "list", data: [{id}]}`;
    // tolerate a bare array of `{id}` entries too.
    let items = if let Some(data) = value.get("data").and_then(Value::as_array) {
        data.clone()
    } else {
        value.as_array()?.clone()
    };
    Some(
        items
            .iter()
            .filter_map(|item| item.get("id").and_then(Value::as_str).map(str::to_owned))
            .collect(),
    )
}

/// Verify the effective adopted provider before inference: the exact
/// provider identity, package and base URL from `GET /api/provider`, the
/// frozen API model id selected locally from `GET /api/model`, and the
/// frozen limits against observed ones, plus the backend's own passive
/// inventory. Every failure reason is short and non-secret: reasons are
/// persisted in the daemon outcome, so they never carry endpoints, ids,
/// values, or secrets. A known mismatch is `profile_error` (project
/// overrides must not retarget the route); transport ambiguity is
/// `unknown`. An unreachable backend is `profile_error`, never a silent
/// pass: the start cannot infer without it. Absent observed provider fields
/// report unavailable honestly instead of failing, and only nonsecret
/// observations enter the fragment — never a resolved key or raw
/// configuration.
async fn verify_local_effective(
    http: &OpenCodeHttp,
    local: &LocalProvider,
    backend_key: Option<&str>,
    deadline: Instant,
) -> Result<Value> {
    let provider = http
        .get_provider(&local.provider_id, deadline)
        .await
        .map_err(|_| Error::new("unknown", "adopted provider readback unresolved"))?;
    if let Some(observed) = provider.id.as_deref()
        && observed != local.provider_id
    {
        return Err(Error::new("profile_error", "effective provider mismatch"));
    }
    let mut unavailable = Vec::new();
    let observed_package = provider.package.clone();
    if let Some(observed) = observed_package.as_deref() {
        if observed != local.package {
            return Err(Error::new(
                "profile_error",
                "effective provider package mismatch",
            ));
        }
    } else {
        unavailable.push("package");
    }
    let observed_base = provider
        .settings
        .as_ref()
        .and_then(|settings| settings.base_url.clone());
    if let Some(observed) = observed_base.as_deref() {
        if normalize_endpoint(observed) != normalize_endpoint(&local.base_url) {
            return Err(Error::new(
                "profile_error",
                "effective provider endpoint mismatch",
            ));
        }
    } else {
        unavailable.push("baseURL");
    }
    let key_observed = provider
        .settings
        .as_ref()
        .is_some_and(|settings| settings.api_key.is_some());
    let models = http
        .list_native_models(deadline)
        .await
        .map_err(|_| Error::new("unknown", "adopted provider readback unresolved"))?;
    let entry = models
        .iter()
        .find(|model| model.id.as_deref() == Some(local.api_model_id.as_str()))
        .ok_or_else(|| Error::new("profile_error", "effective catalogue mismatch"))?;
    let mut limits_unavailable = Vec::new();
    if let Some(limit) = entry.limit.as_ref() {
        match limit.context {
            Some(observed) if observed != local.context_limit => {
                return Err(Error::new(
                    "profile_error",
                    "effective context limit mismatch",
                ));
            }
            None => limits_unavailable.push("context"),
            _ => {}
        }
        match limit.output {
            Some(observed) if observed != local.output_limit => {
                return Err(Error::new(
                    "profile_error",
                    "effective output limit mismatch",
                ));
            }
            None => limits_unavailable.push("output"),
            _ => {}
        }
    } else {
        limits_unavailable.push("limit");
    }
    let backend_kind = BackendKind::parse(&local.backend)
        .ok_or_else(|| Error::new("profile_error", "effective backend mismatch"))?;
    let inventory =
        match fetch_backend_inventory(&local.base_url, backend_kind, backend_key, deadline).await {
            None => {
                return Err(Error::new("profile_error", "adopted backend unreachable"));
            }
            Some(ids) if ids.iter().any(|id| id == &local.api_model_id) => "matched",
            Some(_) => {
                return Err(Error::new(
                    "profile_error",
                    "adopted backend inventory mismatch",
                ));
            }
        };
    Ok(json!({
        "backend": local.backend,
        "backend_hint": backend_kind.metadata_hint(),
        "provider_id": local.provider_id,
        "provider_package": local.package,
        "provider_base_url": local.base_url,
        "api_model_id": local.api_model_id,
        "context_limit": local.context_limit,
        "output_limit": local.output_limit,
        "key_mode": local.api_key_env.as_ref().map(|name| format!("env:{name}")).unwrap_or("none".to_owned()),
        "observed_package": observed_package,
        "observed_base_url": observed_base,
        "key_observed": key_observed,
        "catalogue_model_present": true,
        "limits_unavailable": limits_unavailable,
        "provider_unavailable": unavailable,
        "readback_complete": unavailable.is_empty() && limits_unavailable.is_empty(),
        "inventory": inventory,
    }))
}

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
            split_model(model).ok_or_else(|| {
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
            checked_effort(Some(text), "invalid")?
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

/// Whether the live native catalogue carries the exact requested
/// `provider/model` id. Entries missing either half never match; the
/// installed server echoes even bogus switches, so this gate (read
/// before any write) is what fails an unknown model honestly.
fn catalogue_contains(catalogue: &[NativeModelEntry], requested: &str) -> bool {
    catalogue.iter().any(
        |entry| match (entry.provider_id.as_deref(), entry.id.as_deref()) {
            (Some(provider), Some(id)) => format!("{}/{}", provider.trim(), id.trim()) == requested,
            _ => false,
        },
    )
}

/// The live native catalogue's selectable variant ids for one exact
/// `provider/model` id (`Model.Variant` ids in wire order). `None` when
/// the entry is absent or reports no variants member: unknown, never an
/// empty wardrobe.
fn catalogue_variants(catalogue: &[NativeModelEntry], requested: &str) -> Option<Vec<String>> {
    catalogue
        .iter()
        .find_map(
            |entry| match (entry.provider_id.as_deref(), entry.id.as_deref()) {
                (Some(provider), Some(id))
                    if format!("{}/{}", provider.trim(), id.trim()) == requested =>
                {
                    Some(entry.variants.as_ref().map(|variants| {
                        variants
                            .iter()
                            .filter_map(|variant| variant.id.clone())
                            .collect()
                    }))
                }
                _ => None,
            },
        )
        .flatten()
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

/// Best-effort delete of a created session for a failed start: returns the
/// kill note for the retained error. Never fails itself. The delete shares
/// the start envelope cleanup budget so a mismatch at the edge of the budget
/// still answers inside the daemon deadline.
async fn delete_created_session(
    session: &mut AgentSession,
    session_id: &str,
    envelope: Instant,
) -> String {
    match session
        .http
        .delete_session(session_id, phase_deadline(envelope, START_CLEANUP_BUDGET))
        .await
    {
        Ok(true) => "the created native session was removed".to_owned(),
        Ok(false) => "the created native session was already gone".to_owned(),
        Err(HttpOutcome::Rejected { reason, .. }) => {
            format!("the created native session kill was refused ({reason})")
        }
        Err(HttpOutcome::Unknown { reason }) | Err(HttpOutcome::NotDispatched { reason }) => {
            format!("the created native session kill is unconfirmed ({reason})")
        }
    }
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
        BackendKind, DAEMON_START_TIMEOUT, LocalProvider, McpSettle, OpenCodeHttp, OpenCodePlugin,
        PhaseFailure, START_CLEANUP_BUDGET, START_ENVELOPE, START_SETTLE_RESERVE,
        SWARM_MCP_SERVER_NAME, StartAdmission, StartPhase, catalogue_contains, catalogue_variants,
        fetch_backend_inventory, local_key_value, mcp_names, mcp_settle, normalize_endpoint,
        opencode_manifest, opencode_scope, parse_configure_model, parse_local_provider,
        phase_budget, phase_deadline, policy_from_snapshot, presession_reason, split_model,
        start_auth_selector, start_config_override, start_failure_is_proven_no_effect,
        start_model_ref, verify_local_effective,
    };
    use crate::native::server::READINESS_TIMEOUT;
    use crate::native::types::{McpServerReadback, McpStatusReadback, NativeModelEntry};
    use eliot_core::{AgentStartParams, McpPolicy, Result};
    use eliot_plugin::session::{
        ResumeSubject, optional_profile_text, parse_resume_subject, validate_source_plan,
    };
    use serde_json::json;

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
    use std::path::PathBuf;

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
                variants: None,
            },
        ];
        assert!(catalogue_contains(&catalogue, "opencode/kimi"));
        assert!(!catalogue_contains(&catalogue, "opencode/other"));
        assert!(!catalogue_contains(&catalogue, "other/kimi"));
        assert!(!catalogue_contains(&[], "opencode/kimi"));
        assert_eq!(
            catalogue_variants(&catalogue, "opencode/kimi"),
            Some(vec!["low".to_owned(), "high".to_owned()])
        );
        assert!(catalogue_variants(&catalogue, "opencode/other").is_none());
        assert!(catalogue_variants(&[], "opencode/kimi").is_none());
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

    fn local_profile(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
        value.as_object().expect("object").clone()
    }

    fn local_model_ref() -> crate::native::types::ModelRef {
        crate::native::types::ModelRef {
            id: "qwen3-8b".to_owned(),
            provider_id: "eliot-local".to_owned(),
            variant: None,
        }
    }

    #[test]
    fn local_provider_parses_frozen_fields_with_env_key_or_no_auth() {
        let profile = local_profile(json!({
            "model": "eliot-local/qwen3-8b",
            "provider_base_url": "http://127.0.0.1:1234/v1",
            "backend": "lmstudio",
            "context_limit": 131072,
            "output_limit": 8192,
        }));
        let local = parse_local_provider(&profile, &local_model_ref())
            .expect("no-auth local")
            .expect("present");
        assert_eq!(local.provider_id, "eliot-local");
        assert_eq!(local.api_model_id, "qwen3-8b");
        assert_eq!(local.base_url, "http://127.0.0.1:1234/v1");
        assert_eq!(local.backend, "lmstudio");
        assert_eq!(local.api_key_env, None);

        let profile = local_profile(json!({
            "provider_base_url": "http://127.0.0.1:8080/v1",
            "backend": "llama.cpp",
            "api_key_env": "LOCAL_LLM_KEY",
            "context_limit": 65536,
            "output_limit": 4096,
            "provider_package": "@opencode/ai/providers/openai-compatible",
        }));
        let local = parse_local_provider(&profile, &local_model_ref())
            .expect("env-key local")
            .expect("present");
        assert_eq!(local.api_key_env.as_deref(), Some("LOCAL_LLM_KEY"));

        // Cloud profiles carry no local provider.
        let profile =
            local_profile(json!({"model": "openrouter/m", "auth_integration": "openrouter"}));
        let cloud_ref = crate::native::types::ModelRef {
            id: "m".to_owned(),
            provider_id: "openrouter".to_owned(),
            variant: None,
        };
        assert!(
            parse_local_provider(&profile, &cloud_ref)
                .expect("cloud")
                .is_none()
        );
    }

    #[test]
    fn local_provider_rejects_mixed_partial_and_wrong_package() {
        // A canonical credential must never ride the local path.
        let profile = local_profile(json!({
            "provider_base_url": "http://127.0.0.1:8080",
            "backend": "ollama",
            "context_limit": 32768,
            "output_limit": 4096,
            "auth_integration": "openrouter",
        }));
        assert_eq!(
            parse_local_provider(&profile, &local_model_ref())
                .expect_err("mixed auth")
                .code,
            "profile_error"
        );
        // The dedicated provider segment is required.
        let foreign = crate::native::types::ModelRef {
            id: "m".to_owned(),
            provider_id: "openrouter".to_owned(),
            variant: None,
        };
        let profile = local_profile(json!({
            "provider_base_url": "http://127.0.0.1:8080",
            "backend": "ollama",
            "context_limit": 32768,
            "output_limit": 4096,
        }));
        assert_eq!(
            parse_local_provider(&profile, &foreign)
                .expect_err("foreign provider")
                .code,
            "profile_error"
        );
        // Partial local fields fail closed, never fall back.
        for value in [
            json!({"provider_base_url": "http://127.0.0.1:8080"}),
            json!({"backend": "ollama"}),
            json!({"context_limit": 32768}),
            json!({"output_limit": 4096}),
            json!({"provider_base_url": "", "backend": "ollama", "context_limit": 1, "output_limit": 1}),
            json!({"provider_base_url": "ftp://127.0.0.1:8080", "backend": "ollama", "context_limit": 1, "output_limit": 1}),
            json!({"provider_base_url": "http://127.0.0.1:8080", "backend": "unknown-rt", "context_limit": 1, "output_limit": 1}),
            json!({"provider_base_url": "http://127.0.0.1:8080", "backend": "ollama", "context_limit": 0, "output_limit": 1}),
            json!({"provider_base_url": "http://127.0.0.1:8080", "backend": "ollama", "context_limit": 1, "provider_package": "other/package"}),
        ] {
            assert!(
                parse_local_provider(&local_profile(value), &local_model_ref()).is_err(),
                "partial local must fail"
            );
        }
    }

    #[test]
    fn local_auth_selector_is_absent_while_cloud_requires_a_selector() {
        let cloud = local_profile(json!({"auth_integration": "openrouter"}));
        assert!(start_auth_selector(&cloud, false).expect("cloud").is_some());
        // `err()` (not `expect_err`) because the selector type deliberately
        // has no `Debug` so a caller bug cannot log whatever was passed in.
        assert_eq!(
            start_auth_selector(&local_profile(json!({})), false)
                .err()
                .expect("missing cloud selector")
                .code,
            "profile_error"
        );
        // Local refuses a canonical selector instead of importing it.
        let mixed = local_profile(json!({"auth_integration": "openrouter"}));
        assert_eq!(
            start_auth_selector(&mixed, true)
                .err()
                .expect("local mixed auth")
                .code,
            "profile_error"
        );
        assert!(
            start_auth_selector(&local_profile(json!({})), true)
                .expect("local none")
                .is_none()
        );
    }

    #[test]
    fn endpoint_normalization_trims_only_trailing_slashes() {
        assert_eq!(
            normalize_endpoint("http://127.0.0.1:1234/v1/"),
            "http://127.0.0.1:1234/v1"
        );
        assert_eq!(
            normalize_endpoint("http://127.0.0.1:1234/v1"),
            "http://127.0.0.1:1234/v1"
        );
    }

    #[test]
    fn backend_key_resolves_only_non_empty_runtime_values() {
        use std::ffi::OsString;
        let environment = vec![
            (
                OsString::from("LOCAL_LLM_KEY"),
                OsString::from("secret-value"),
            ),
            (OsString::from("EMPTY_KEY"), OsString::from("   ")),
        ];
        assert_eq!(
            local_key_value(&environment, Some("LOCAL_LLM_KEY")),
            Some("secret-value")
        );
        assert_eq!(local_key_value(&environment, Some("EMPTY_KEY")), None);
        assert_eq!(local_key_value(&environment, Some("MISSING")), None);
        assert_eq!(local_key_value(&environment, None), None);
    }

    /// A closed loopback port: binds are released before use, so connects
    /// are refused fast with no listener, no redirect and no leak.
    async fn closed_loopback_port() -> u16 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("closed-port bind");
        let port = listener.local_addr().expect("closed-port addr").port();
        drop(listener);
        port
    }

    /// A minimal stub of the owned OpenCode server: answers the two
    /// readback routes the adopted-backend gate needs, nothing else.
    async fn stub_owned_provider_server(
        provider_body: String,
        model_body: String,
    ) -> (tokio::task::JoinHandle<()>, u16) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub bind");
        let port = listener.local_addr().expect("stub addr").port();
        let handle = tokio::spawn(async move {
            for _ in 0..2 {
                let Ok((mut stream, _)) = listener.accept().await else {
                    return;
                };
                let mut head = Vec::new();
                let mut byte = [0u8; 1];
                loop {
                    use tokio::io::AsyncReadExt;
                    match stream.read(&mut byte).await {
                        Ok(0) => break,
                        Ok(_) => {
                            head.push(byte[0]);
                            if head.len() >= 4 && head[head.len() - 4..] == *b"\r\n\r\n" {
                                break;
                            }
                            if head.len() > 8192 {
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }
                let body = if head.starts_with(b"GET /api/provider/") {
                    &provider_body
                } else {
                    &model_body
                };
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                use tokio::io::AsyncWriteExt;
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        (handle, port)
    }

    fn local_fixture(base_url: &str) -> LocalProvider {
        LocalProvider {
            provider_id: "eliot-local".to_owned(),
            api_model_id: "test-model".to_owned(),
            base_url: base_url.to_owned(),
            api_key_env: None,
            context_limit: 1000,
            output_limit: 100,
            backend: "generic".to_owned(),
            package: "@opencode/ai/providers/openai-compatible".to_owned(),
        }
    }

    #[tokio::test]
    async fn backend_inventory_unreachable_reports_none() {
        let closed = closed_loopback_port().await;
        let base = format!("http://127.0.0.1:{closed}/v1");
        let deadline = super::Instant::now() + std::time::Duration::from_secs(10);
        // No listener: refused fast, with or without a key reference (the
        // key is never sent when the endpoint is unreachable).
        assert_eq!(
            fetch_backend_inventory(&base, BackendKind::Generic, None, deadline).await,
            None
        );
        assert_eq!(
            fetch_backend_inventory(
                &base,
                BackendKind::Generic,
                Some("LOCAL_TEST_KEY"),
                deadline
            )
            .await,
            None
        );
    }

    /// The live 20261114 finding: with no backend listening, the launch
    /// used to stash an unknown session (Agent stuck `starting`, plugin and
    /// children alive, capacity held). The verification gate that runs
    /// before any native session is created must fail definitely instead.
    #[tokio::test]
    async fn unreachable_backend_fails_before_any_native_session() {
        let closed = closed_loopback_port().await;
        let (stub, port) = stub_owned_provider_server(
            format!(
                "{{\"data\":{{\"id\":\"eliot-local\",\"package\":\"@opencode/ai/providers/openai-compatible\",\"settings\":{{\"baseURL\":\"http://127.0.0.1:{closed}/v1\"}}}}}}"
            ),
            "{\"data\":[{\"id\":\"test-model\",\"providerID\":\"eliot-local\",\"limit\":{\"context\":1000,\"output\":100}}]}".to_owned(),
        )
        .await;
        let dir = std::env::temp_dir();
        let http = OpenCodeHttp::new(port, "test-password".to_owned(), &dir).expect("stub client");
        let local = local_fixture(&format!("http://127.0.0.1:{closed}/v1"));
        let error = verify_local_effective(
            &http,
            &local,
            None,
            super::Instant::now() + std::time::Duration::from_secs(10),
        )
        .await
        .expect_err("unreachable backend must fail the start gate");
        assert_eq!(error.code, "profile_error");
        assert_eq!(error.message.as_str(), "adopted backend unreachable");
        // The short safe reason carries no endpoint, id, value or secret.
        assert!(!error.message.contains("127.0.0.1"));
        assert!(!error.message.contains("test-model"));
        stub.abort();
    }

    /// The healthy path is unchanged: a listening backend whose inventory
    /// lists the frozen model passes the gate with the fragment merged.
    #[tokio::test]
    async fn reachable_backend_with_matching_inventory_passes() {
        let backend = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("backend bind");
        let backend_port = backend.local_addr().expect("backend addr").port();
        let backend_task = tokio::spawn(async move {
            let Ok((mut stream, _)) = backend.accept().await else {
                return;
            };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            loop {
                use tokio::io::AsyncReadExt;
                match stream.read(&mut byte).await {
                    Ok(0) => break,
                    Ok(_) => {
                        head.push(byte[0]);
                        if head.len() >= 4 && head[head.len() - 4..] == *b"\r\n\r\n" {
                            break;
                        }
                        if head.len() > 8192 {
                            break;
                        }
                    }
                    Err(_) => break,
                }
            }
            let body =
                "{\"object\":\"list\",\"data\":[{\"id\":\"test-model\",\"object\":\"model\"}]}";
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            use tokio::io::AsyncWriteExt;
            let _ = stream.write_all(response.as_bytes()).await;
        });
        let (stub, port) = stub_owned_provider_server(
            format!(
                "{{\"data\":{{\"id\":\"eliot-local\",\"package\":\"@opencode/ai/providers/openai-compatible\",\"settings\":{{\"baseURL\":\"http://127.0.0.1:{backend_port}/v1\"}}}}}}"
            ),
            "{\"data\":[{\"id\":\"test-model\",\"providerID\":\"eliot-local\",\"limit\":{\"context\":1000,\"output\":100}}]}".to_owned(),
        )
        .await;
        let dir = std::env::temp_dir();
        let http = OpenCodeHttp::new(port, "test-password".to_owned(), &dir).expect("stub client");
        let local = local_fixture(&format!("http://127.0.0.1:{backend_port}/v1"));
        let fragment = verify_local_effective(
            &http,
            &local,
            None,
            super::Instant::now() + std::time::Duration::from_secs(10),
        )
        .await
        .expect("healthy backend passes");
        assert_eq!(fragment["inventory"], "matched");
        assert_eq!(
            fragment["provider_base_url"],
            format!("http://127.0.0.1:{backend_port}/v1")
        );
        stub.abort();
        backend_task.abort();
    }

    /// Proven-no-effect failures return the daemon-honored failed-state
    /// value (never an error): the same start replays the value without new
    /// native I/O, and any other start is refused.
    #[test]
    fn proven_no_effect_start_returns_failed_state_and_replays() {
        let params = AgentStartParams::new("agent-1", 7, "0199-start-1");
        let mut plugin = OpenCodePlugin::default();
        let value = plugin.retain_proven_no_effect_start(
            &params,
            StartPhase::Verify,
            eliot_core::Error::new("native_readback", "adopted backend unreachable"),
        );
        assert_eq!(value["state"], "failed");
        assert_eq!(value["agent_id"], "agent-1");
        assert_eq!(value["generation"], 7);
        assert_eq!(value["command_id"], "0199-start-1");
        assert_eq!(value["reason"], "adopted backend unreachable");
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
