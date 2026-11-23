use crate::{
    events::{self, EventConfig, EventHandle, EventStatus, InboxRecord, SteerVerdict, TurnFacts},
    native::{
        AuthSetup, AuthTransfer, HttpOutcome, IntegrationSelector, OpenCodeHttp, OwnedServer,
        SWARM_MCP_SERVER_NAME, ServerLaunch,
        models::{MAX_MODEL_ID_BYTES, ReaderContext, run_model_metadata, run_models},
        server::{
            ArchiveServerLaunch, provider_config, validate_local_provider,
            validate_local_provider_environment,
        },
        transfer_selected_integration,
        types::{
            ConfigEntry, ConfigMcpServer, InboxUser, McpServerReadback, ModelRef, NativeModelEntry,
            PermissionRule, SessionInfo,
        },
    },
};
use eliot_core::local_provider::LocalProvider;
use eliot_core::{ContextItem, Error, FailureOrigin, InjectPoint, McpPolicy, Result, SecretPolicy};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::{
        AgentModelsParams, ModelListParams, ModelListPart, NativeArchiveParams, stream_sources,
    },
    retry::{Retry, RetryOperation},
    session::{
        ModelSelection, ResumeSubject, TargetEnvironment, attach_reply_diagnostics, begin_stop,
        decode, ensure_dedicated_reader, ensure_reader_harness, initialize_fields_valid,
        native_no_write, not_initialized, optional_profile_text, parse_resume_subject, paths_equal,
        register_native_process, required_native_cmd, split_provider_model, target_environment,
        unknown_snapshot, validate_binding, validate_identity, validate_source_plan,
        with_claimed_context, with_context_points, with_model_catalogue_diagnostic,
    },
    settle::settle_unproven_input,
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
/// OpenCode can answer config readback before its provider inventory is
/// available. Its native catalogue fetch allows 10 seconds; leave time for
/// the provider reload while keeping the start cleanup reserve intact.
const START_CATALOGUE_READY_BUDGET: std::time::Duration = std::time::Duration::from_secs(12);

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

/// Total bound for one `native.archive` reader dispatch: the owned reader
/// server spawn/readiness, the store-scope proof, one session read, exhaustive
/// childless-list preflight, one DELETE and complete post-delete readback. The
/// shutdown share below is reserved after it, and the pair fits inside the
/// daemon's 25 s reader deadline; cancellation kills the helper and never replays.
const ARCHIVE_ENVELOPE: std::time::Duration = std::time::Duration::from_secs(18);

/// Spawn/readiness share of the archive envelope: the retained store is
/// already warm and no auth transfer runs, so readiness needs no credential
/// import time.
const ARCHIVE_SPAWN_BUDGET: std::time::Duration = std::time::Duration::from_secs(12);

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

/// One start already settled failed: the daemon retires this plugin
/// process and never re-sends agent.start.
struct FailedStart;

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionPhase {
    Ready,
    Unknown,
    Stopped,
}

fn native_io_blocked(session: &AgentSession) -> bool {
    session.resources_proved_closed || session.stop_end_ack_proved
}

/// Identity gate and retained-session binding shared by snapshot and stop.
fn retained_session<'a>(
    slot: &'a mut Option<AgentSession>,
    agent_id: &str,
    generation: i64,
    command_id: &str,
) -> Result<&'a mut AgentSession> {
    validate_identity(agent_id, generation, command_id, "OpenCode")?;
    let session = slot
        .as_mut()
        .ok_or_else(|| Error::new("not_found", "OpenCode Agent session is not retained"))?;
    validate_binding(
        &session.agent_id,
        session.generation,
        agent_id,
        generation,
        "OpenCode",
    )?;
    Ok(session)
}

/// Live command gate: the bound session must be Ready with native I/O open.
fn ready_session<'a>(
    slot: &'a mut Option<AgentSession>,
    params: &AgentCommandParams,
) -> Result<&'a mut AgentSession> {
    let session = slot
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
    Ok(session)
}

struct AgentSession {
    agent_id: String,
    generation: i64,
    start_command_id: String,
    phase: SessionPhase,
    session_id: Option<String>,
    native_scope: String,
    server: OwnedServer,
    http: crate::native::OpenCodeHttp,
    events: EventHandle,
    stop_command_id: Option<String>,
    resources_proved_closed: bool,
    stop_end_ack_proved: bool,
    local_provider: Option<LocalProvider>,
    secret_policy: SecretPolicy,
}

/// A start request that passed admission: decoded params plus the validated
/// frozen profile, workspace, child environment, selected native auth
/// integration (cloud path) or typed local provider, and
/// redaction secrets. Exactly one of `auth_selector` and `local_provider`
/// is set: local providers use environment-backed authentication.
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
    local_provider: Option<LocalProvider>,
    secrets: Vec<String>,
    credential: String,
    instance_dir: String,
    store_root: String,
    resume: Option<ResumeSubject>,
    swarm_executable: PathBuf,
    swarm_data_dir: PathBuf,
    swarm_mcp_environment: Vec<(OsString, OsString)>,
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
            "native.archive" => self.native_archive(&request.params).await,
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
        // The attached error names the plugin method and the value's own
        // reason: an empty harness tail stays honest (empty text), but the
        // outcome still says which plugin method produced it and with what
        // reason.
        attach_reply_diagnostics(
            result,
            &request.method,
            "OpenCode",
            |error| session.server.diagnostic_error(error),
            |value| {
                Error::new(
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
                .with_where(format!("plugin.opencode.command.{}", request.method))
            },
        )
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams =
            decode(raw, "OpenCode plugin initialize params are invalid")?;
        let store_root = params.store_root.clone().unwrap_or_default();
        if !initialize_fields_valid(&params)
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

    /// Settle a failed start: the settled marker refuses any second
    /// agent.start, and the daemon retires this plugin process.
    fn retain_failed_start(&mut self, error: Error) -> Error {
        self.failed_start = Some(FailedStart);
        error
    }

    /// Remove one terminal Swarm-created OpenCode session through the
    /// documented `DELETE /api/session/{id}` call. A dedicated reader plugin
    /// instance runs a short-lived owned server on the exact retained per-Agent
    /// store and directory, proves that store derives the retained native scope,
    /// and proves the exact target's presence against a complete inventory.
    /// An absent target is already archived; a present target with native
    /// children is refused because OpenCode DELETE recursively removes them.
    /// The stopped isolated reader has no live Agent turn. Raw store files are
    /// never touched.
    async fn native_archive(&self, raw: &Value) -> Result<Value> {
        ensure_dedicated_reader(
            self.session.is_some() || self.failed_start.is_some(),
            "OpenCode",
            "native.archive",
        )?;
        let params: NativeArchiveParams =
            decode(raw, "OpenCode native.archive params are invalid")?;
        ensure_reader_harness(&params.profile, "opencode", "OpenCode", "native.archive")?;
        if params.native_ref.is_empty()
            || params.native_ref.len() > 512
            || params.native_ref.chars().any(char::is_control)
        {
            return Err(Error::new(
                "invalid",
                "OpenCode native.archive needs a native session reference",
            ));
        }
        let agent_id = params
            .agent_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    "OpenCode native.archive needs the retained Agent id",
                )
            })?;
        let native_scope = params
            .native_scope
            .as_deref()
            .filter(|scope| !scope.is_empty())
            .ok_or_else(|| {
                Error::new(
                    "invalid",
                    "OpenCode native.archive needs the retained native scope",
                )
            })?;
        let workspace = params.workspace.as_deref().ok_or_else(|| {
            Error::new(
                "invalid",
                "OpenCode native.archive needs the retained Agent workspace",
            )
        })?;
        let program = required_native_cmd(
            &params.profile,
            "OpenCode native.archive requires a discovered native cmd",
        )?;
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("OpenCode"))?;
        let TargetEnvironment {
            variables: environment,
            mut secrets,
        } = target_environment(None, params.profile.get("pass_env"), "OpenCode")?;
        secrets.push(initialized.credential.clone());
        let envelope = Instant::now() + ARCHIVE_ENVELOPE;
        let mut server = OwnedServer::spawn_archive_reader(
            ArchiveServerLaunch {
                program: OsString::from(program),
                workspace: PathBuf::from(workspace),
                instance_dir: PathBuf::from(&initialized.instance_dir),
                store_root: PathBuf::from(&initialized.store_root),
                agent_id: agent_id.to_owned(),
                environment,
            },
            &secrets,
            phase_deadline(envelope, ARCHIVE_SPAWN_BUDGET),
        )
        .await?;
        let result =
            Self::archive_retained_session(&server, native_scope, &params.native_ref, envelope)
                .await;
        // The reader server is ours: end it on every path. An unconfirmed
        // termination after a successful delete leaves the outcome unknown,
        // never a claimed archive.
        let stopped = server
            .terminate(true, Instant::now() + START_SHUTDOWN_BUDGET)
            .await;
        match (result, stopped) {
            (Ok(value), Ok(())) => Ok(value),
            (Ok(_), Err(error)) => Err(server.diagnostic_error(Error::new(
                "unknown",
                format!(
                    "OpenCode native.archive server cleanup is unconfirmed ({})",
                    error.message
                ),
            ))),
            (Err(error), _) => Err(server.diagnostic_error(error)),
        }
    }

    /// Reconcile exact session reads with a complete unfiltered inventory on
    /// the reader serving the exact retained store. An absent target is
    /// archived independently of retained children. A present target with any
    /// native child is refused before recursive DELETE; after DELETE, proven
    /// target absence establishes this target's archive even if child rows
    /// remain. HTTP 400/401 and a proven child of a present target are
    /// definitive refusals; every other uncertain outcome stays unknown,
    /// never replayed.
    async fn archive_retained_session(
        server: &OwnedServer,
        native_scope: &str,
        native_ref: &str,
        envelope: Instant,
    ) -> Result<Value> {
        if opencode_scope(server.data_dir()) != native_scope {
            return Err(Error::new(
                "source_integrity",
                "OpenCode archive reader store differs from the retained native scope",
            ));
        }
        let http = server.http();
        let present = http
            .archive_session_present(native_ref, phase_deadline(envelope, RPC_TIMEOUT))
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode archive session readback is unconfirmed ({}); no replay",
                        error.message
                    ),
                )
            })?;
        let before = http
            .archive_session_inventory(native_ref, phase_deadline(envelope, RPC_TIMEOUT))
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "OpenCode complete childless-list proof failed ({}); no replay",
                        error.message
                    ),
                )
            })?;
        if before.target_present != present {
            return Err(Error::new(
                "unknown",
                "OpenCode exact session readback disagreed with the complete session list; no replay",
            ));
        }
        if !present {
            return Ok(json!({"archived": true}));
        }
        if before.has_children {
            return Err(Error::new(
                "native_archive_refused",
                "OpenCode session has native child sessions and was kept",
            ));
        }

        let delete_deadline = envelope
            .checked_sub(START_SHUTDOWN_BUDGET)
            .unwrap_or_else(Instant::now);
        match http.delete_session(native_ref, delete_deadline).await {
            Ok(_) => {
                let after = http
                    .archive_session_inventory(native_ref, phase_deadline(envelope, RPC_TIMEOUT))
                    .await
                    .map_err(|error| {
                        Error::new(
                            "unknown",
                            format!(
                                "OpenCode delete readback is unconfirmed ({}); no replay",
                                error.message
                            ),
                        )
                    })?;
                if after.target_present {
                    return Err(Error::new(
                        "unknown",
                        "OpenCode DELETE completed without proving the target absent; no replay",
                    ));
                }
                Ok(json!({"archived": true}))
            }
            Err(HttpOutcome::Rejected {
                status: Some(400 | 401),
                reason,
            }) => Err(Error::new(
                "native_archive_refused",
                format!("OpenCode definitively refused session delete ({reason})"),
            )),
            Err(HttpOutcome::Rejected { reason, .. }) => Err(Error::new(
                "unknown",
                format!("OpenCode delete refusal was not authoritative ({reason}); no replay"),
            )),
            Err(HttpOutcome::Unknown { reason }) => Err(Error::new(
                "unknown",
                format!("OpenCode session delete is unconfirmed ({reason}); no replay"),
            )),
            Err(HttpOutcome::NotDispatched { reason }) => Err(Error::new(
                "unknown",
                format!("OpenCode session delete was not dispatched ({reason}); no replay"),
            )),
        }
    }

    /// One read-only `harness.models` phase: `models --standalone` ids or
    /// `api model.list --standalone` metadata. The daemon gives each phase
    /// its own reader Job and proves family closure before overlaying results.
    async fn model_list(&self, raw: &Value) -> Result<Value> {
        let snapshot = eliot_plugin::session::model_catalogue_profile(
            raw,
            self.session.is_some() || self.failed_start.is_some(),
            "opencode",
            "OpenCode",
            true,
        )?;
        let params: ModelListParams = decode(raw, "OpenCode model.list params are invalid")?;
        let program = required_native_cmd(
            snapshot,
            "OpenCode model.list requires a discovered native cmd",
        )?;
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
        let fragment = start_local_provider(profile)?;
        if let Some(fragment) = fragment.as_ref() {
            validate_local_provider_environment(fragment, profile.get("pass_env"), &environment)?;
        }
        let until = Instant::now() + RPC_TIMEOUT;
        let config_home = base.join("config");
        let data_home = base.join("data");
        let auth = if fragment.is_some() || profile.get("auth_integration").is_some() {
            start_auth_selector(profile, fragment.is_some())?
        } else {
            None
        };
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
        let context = ReaderContext {
            workspace,
            environment: &environment,
            xdg: Some((config_home.as_path(), data_home.as_path())),
            local_provider: fragment.is_some(),
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
        Ok(json!({"models": models, "default": Value::Null, "provenance": "preview"}))
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
        self.failed_start = Some(FailedStart);
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
        let parsed = self.parse_start_request(raw)?;
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
            // authentication contract and returns as an error.
            Err(failure) => {
                if start_failure_is_proven_no_effect(failure.phase, &failure.error.code) {
                    return Ok(self.proven_no_effect_start(&parsed.params, failure));
                }
                return Err(self.retain_failed_start(*failure.error));
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

    /// Start phase 1, admission: the once-only guard, decode and
    /// frozen-profile validation. Failures here retain nothing: a corrected
    /// start may retry.
    fn parse_start_request(&self, raw: &Value) -> Result<Box<ParsedStart>> {
        let _timing = crate::native::startup::Step::new("frozen_admission");
        // The daemon sends agent.start exactly once per freshly spawned
        // plugin process: a bound or settled plugin never starts again.
        if self.session.is_some() || self.failed_start.is_some() {
            return Err(Error::new(
                "bad_state",
                "OpenCode plugin already received agent.start",
            ));
        }
        let params: AgentStartParams = decode(raw, "OpenCode agent.start params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "OpenCode",
        )?;
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
        let local_provider = start_local_provider(profile)?;
        let auth_selector = start_auth_selector(profile, local_provider.is_some())?;
        let (workspace, program) = start_paths(profile, &params)?;
        let TargetEnvironment {
            variables: environment,
            mut secrets,
        } = target_environment(
            params.target_environment.as_ref(),
            profile.get("pass_env"),
            "OpenCode",
        )?;
        let mcp_environment = eliot_plugin::session::agent_mcp_environment(
            params.mcp_environment.as_ref(),
            params.target_environment.as_ref(),
            params.pass_agent_identity_to_shell,
            &params.agent_id,
            params.generation,
            "OpenCode",
        )?;
        secrets.extend(mcp_environment.secrets.iter().cloned());
        secrets.push(initialized.credential.clone());
        // The native target environment carries no data root. The private
        // managed MCP config always receives its scoped root and identity.
        let swarm_data_dir = eliot_plugin::native_cli::unique_environment_value(
            &mcp_environment.variables,
            "ELIOT_HOME",
        )
        .map(PathBuf::from)
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
        let swarm_mcp_environment = mcp_environment.variables;
        Ok(Box::new(ParsedStart {
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
            local_provider,
            secrets,
            credential: initialized.credential.clone(),
            instance_dir: initialized.instance_dir.clone(),
            store_root: initialized.store_root.clone(),
            resume,
            swarm_executable,
            swarm_data_dir,
            swarm_mcp_environment,
        }))
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
            &["session", "turn", "message", "tool", "request"],
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
                local_provider: parsed.local_provider.clone(),
                owner_rules: parsed.params.owner_rules.clone(),
                frozen_pass_env: parsed
                    .params
                    .profile
                    .as_ref()
                    .and_then(|profile| profile.get("pass_env"))
                    .cloned(),
                swarm_executable: parsed.swarm_executable.clone(),
                swarm_data_dir: parsed.swarm_data_dir.clone(),
                swarm_environment: parsed.swarm_mcp_environment.clone(),
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
            phase: SessionPhase::Unknown,
            session_id: None,
            native_scope: String::new(),
            server,
            http,
            events,
            stop_command_id: None,
            resources_proved_closed: false,
            stop_end_ack_proved: false,
            local_provider: parsed.local_provider.clone(),
            secret_policy: SecretPolicy::new(&parsed.secrets),
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
    /// The catalogue contributes model limits and effort variants before
    /// native session creation. The session readback proves the selected
    /// identity. Inference can still report a native refusal on the turn.
    async fn create_start_session(
        &mut self,
        parsed: &ParsedStart,
        mut session: AgentSession,
        envelope: Instant,
    ) -> Result<CreateOutcome> {
        let _timing = crate::native::startup::Step::new("session_create_verify");
        session.native_scope = opencode_scope(session.server.data_dir());
        let mut model_catalogue = ModelCatalogueReadback::default();
        if let Some(requested) = parsed.requested_model.as_deref() {
            let requested_variant = parsed
                .model_ref
                .as_ref()
                .and_then(|model_ref| model_ref.variant.as_deref());
            let _timing = crate::native::startup::Step::new("model_catalogue");
            let deadline = phase_deadline(create_cutoff(envelope), START_CATALOGUE_READY_BUDGET);
            let catalogue = match session.http.list_native_models(deadline).await {
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
            let selection = ModelSelection::new(requested, catalogue_entry(&catalogue, requested));
            if let Err(failure) = validate_catalogue_effort(&selection, requested_variant) {
                discard_failed_start(&mut session, failure.clone(), envelope).await;
                return Ok(CreateOutcome::ProvenFailed(
                    self.retain_proven_no_effect_start(&parsed.params, StartPhase::Verify, failure),
                ));
            }
            model_catalogue = ModelCatalogueReadback::from_selection(&selection);
        }
        // Workspace-inherited project MCP layers: the owned serve already
        // merged them, so an owner-identity entry there must refuse the start
        // before the native session exists. Every policy, whatever the entry
        // is named.
        if parsed.local_provider.is_none()
            && let Err(error) = refuse_owner_project_mcp(
                &parsed.workspace,
                &parsed.swarm_executable,
                parsed.mcp_cloud_hostname.as_deref(),
            )
        {
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
        let deny_native_subagents = parsed.native_subagents.as_deref() != Some("allow");
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
        effective = model_catalogue.attach(effective);
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
            let expected_environment = managed_mcp_environment(parsed);
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
                        verify_managed_mcp_transport(
                            &documents,
                            &expected_command,
                            &expected_environment,
                        )
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
        effective["native_subagents"] = json!(if deny_native_subagents {
            "deny"
        } else {
            "allow"
        });
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
        if parsed.local_provider.is_none()
            && let Err(error) = refuse_owner_project_mcp(
                &parsed.workspace,
                &parsed.swarm_executable,
                parsed.mcp_cloud_hostname.as_deref(),
            )
        {
            return Err(self.fail_resume(session, error, envelope).await);
        }
        let deadline = phase_deadline(envelope, RPC_TIMEOUT);
        let retained =
            match Self::resume_read_retained(&session, &resume.native_ref, deadline).await {
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
                Err(error) => return Err(self.fail_resume(session, error, envelope).await),
            };
        session.session_id = Some(retained.id.clone());
        // Read model metadata before any retained-session configuration write.
        let model_catalogue =
            match Self::resume_validate_catalogue(&parsed, &session, &retained, deadline).await {
                Ok(limits) => limits,
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
        // Check settled MCP inventory and winning effective documents before
        // any retained-session write, so a refusal here has proven no effect.
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
            return Err(self.stash_resume_unknown(session, error).await);
        }
        let effective = match Self::resume_configure(
            &parsed,
            &session,
            &retained.id,
            mcp_servers,
            mcp_servers_verified,
            model_catalogue,
            deadline,
        )
        .await
        {
            Ok(effective) => effective,
            Err(error) if error.code == "native_rejected" => {
                let error = discard_failed_start(&mut session, error, envelope).await;
                return Ok(self.retain_proven_no_effect_start(
                    &parsed.params,
                    StartPhase::ResumeRefused,
                    error,
                ));
            }
            Err(error) if error.code == "unknown" => {
                return Err(self.stash_resume_unknown(session, error).await);
            }
            Err(error) => return Err(self.fail_resume(session, error, envelope).await),
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
            return Err(self.stash_resume_unknown(session, error).await);
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

    /// Read metadata for the resume target before any interrupt or setter.
    /// An unreadable catalogue or an unsupported listed variant settles with
    /// proven no effect. An omitted model row only contributes a diagnostic.
    async fn resume_validate_catalogue(
        parsed: &ParsedStart,
        session: &AgentSession,
        retained: &SessionInfo,
        deadline: Instant,
    ) -> Result<ModelCatalogueReadback> {
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
        // The fragment membership rule matches the create path before any
        // write: a model outside the fragment refuses here. Without scanning
        // workspace files, the resumed serve regenerates its configuration
        // under the same native project-config disable switch, so workspace
        // project layers stay unloaded for the session lifetime.
        if let (Some(provider), Some(target)) = (parsed.local_provider.as_ref(), target_model)
            && !local_provider_contains(provider, target)
        {
            return Err(Error::new(
                "profile_error",
                "OpenCode local provider does not declare the retained model",
            ));
        }
        if target_model.is_none() && target_variant.is_none() {
            return Ok(ModelCatalogueReadback::default());
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
        let target_ref = parsed
            .override_model_ref
            .as_ref()
            .or(parsed.model_ref.as_ref());
        let validation_ref = if target_model.is_some() {
            target_ref.ok_or_else(|| {
                Error::new(
                    "source_integrity",
                    "OpenCode resume model is missing its native ref",
                )
            })?
        } else {
            target_ref.or(retained.model.as_ref()).ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "OpenCode resume configure effort needs a session model",
                )
            })?
        };
        let id = format!("{}/{}", validation_ref.provider_id, validation_ref.id);
        let selection = ModelSelection::new(
            &id,
            catalogue_entry_for(&catalogue, &validation_ref.provider_id, &validation_ref.id),
        );
        validate_catalogue_effort(&selection, target_variant)?;
        Ok(ModelCatalogueReadback::from_selection(&selection))
    }

    /// Resume MCP proof: gate the settled inventory and prove the winning
    /// effective documents carry no owner transport (plus the exact
    /// generated transport while the Agent-scoped entry serves), BEFORE
    /// the retained session is touched. An unexpected or owner-identity
    /// server must fail here with proven no effect. An unreadable inventory or effective
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
            verify_managed_mcp_transport(
                &documents,
                &managed_mcp_command(parsed),
                &managed_mcp_environment(parsed),
            )?;
        }
        Ok((mcp_servers, mcp_servers_verified))
    }

    /// Resume configure: realign the retained session's model and agent
    /// to the frozen profile, then prove the effective values with a fresh
    /// `session.get`. The location must be the workspace; the requested
    /// agent must be installed (the switch validates nothing itself).
    /// Catalogue metadata and MCP proof come from the preflight reads.
    async fn resume_configure(
        parsed: &ParsedStart,
        session: &AgentSession,
        session_id: &str,
        mcp_servers: Value,
        mcp_servers_verified: bool,
        model_catalogue: ModelCatalogueReadback,
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
        let deny_native_subagents = parsed.native_subagents.as_deref() != Some("allow");
        let repair_needed =
            deny_native_subagents && !OpenCodeHttp::session_subagent_deny_verified(&current);
        if repair_needed {
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
                return Err(Self::resume_configure_error(
                    Error::new(
                        "profile_error",
                        format!("OpenCode profile agent {wanted} is not installed"),
                    ),
                    repair_needed,
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
        // Reaching this point proves each requested earlier setter returned success.
        let earlier_write = repair_needed || agent_needed;
        let switch_model = model_needed || variant_needed;
        if switch_model {
            let mut model_ref = match target_ref.cloned() {
                Some(model_ref) => model_ref,
                // An effort-only overlay on a session the server started
                // without reporting a model keeps the retained model and
                // only selects the variant.
                None => current.model.clone().ok_or_else(|| {
                    Self::resume_configure_error(
                        Error::new(
                            "source_integrity",
                            "OpenCode resume needs a model ref for its retained target model",
                        ),
                        earlier_write,
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
            )
            .map_err(|error| Self::resume_configure_error(error, earlier_write))?;
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
        let model_verified = effective.model.is_some();
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
        effective_value = model_catalogue.attach(effective_value);
        effective_value["native_subagents"] = json!(if deny_native_subagents {
            "deny"
        } else {
            "allow"
        });
        effective_value["native_subagents_verified"] = json!(subagents_verified);
        // The regenerated provider fragment still bounds the retained model.
        if let Some(provider) = parsed.local_provider.as_ref()
            && let Some(model) = effective.model.as_ref()
            && !provider.contains_model(&model.provider_id, &model.id)
        {
            return Err(Self::resume_configure_error(
                Error::new(
                    "profile_error",
                    "OpenCode local provider does not declare the retained model",
                ),
                earlier_write || switch_model,
            ));
        }
        // Keep the effective MCP proof read before the setters.
        effective_value["mcp_servers"] = mcp_servers;
        effective_value["mcp_servers_verified"] = json!(mcp_servers_verified);
        effective_value["mcp_policy"] = json!(parsed.requested_mcp_policy.as_str());
        Ok(effective_value)
    }

    fn resume_configure_error(error: Error, earlier_write: bool) -> Error {
        if !earlier_write || error.code == "unknown" {
            return error;
        }
        let message = format!(
            "OpenCode resume stopped after an accepted retained-session write ({})",
            error.message
        );
        error.with_code("unknown").with_message(message)
    }

    /// Re-read the session and prove it runs `target`: the single echo
    /// verification behind every verified model switch. Resume realign
    /// and mid-run configure share it, so exactly one place turns a switch
    /// into verified effective state; a lost readback or an echo that
    /// contradicts the target is `unknown` (the switch may have applied
    /// without echoing). `context` names the caller (`resume configure`,
    /// `configure`). The switch dispatch itself stays per-caller: resume
    /// preserves a definite model refusal unless an earlier setter applied;
    /// configure maps a proven no-effect refusal to its `rejected` value.
    /// The native ref preserves a provider ID that itself contains a slash.
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

    /// One resume setter: a 204 admits the write for readback. An
    /// authoritative model refusal preserves the native error.
    fn resume_switch(outcome: std::result::Result<(), HttpOutcome>, what: &str) -> Result<()> {
        match outcome {
            Ok(()) => Ok(()),
            Err(HttpOutcome::Rejected { reason, .. }) if what == "model" => {
                Err(Error::new("native_rejected", reason)
                    .with_where("plugin.opencode.start.resume_model")
                    .with_origin(
                        FailureOrigin::Harness,
                        "OpenCode resume model switch returned a native error",
                    ))
            }
            Err(HttpOutcome::Rejected { reason, .. })
            | Err(HttpOutcome::Unknown { reason })
            | Err(HttpOutcome::NotDispatched { reason }) => Err(Error::new(
                "unknown",
                format!("OpenCode resume {what} switch is unresolved ({reason})"),
            )),
        }
    }

    /// Discard the server after a resume failure that wrote nothing and
    /// settle the start failed.
    async fn fail_resume(
        &mut self,
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
        self.retain_failed_start(error)
    }

    /// Stash a resume-ambiguous session as unknown: the server and stream
    /// stay for the snapshot, and the resume is never repeated.
    async fn stash_resume_unknown(&mut self, session: AgentSession, error: Error) -> Error {
        self.session = Some(session);
        if error.origin.is_some() {
            return error;
        }
        let message = error.message;
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
            return self.retain_failed_start(error);
        }
        // The known id was bound at create. Keep it with the live server
        // and stream for readback, without committing session.ready.
        session.phase = SessionPhase::Unknown;
        self.session = Some(session);
        created_start_unknown(native_ref, &error.message, &kill_note)
    }

    /// Start phase 5, readiness: publish `session.ready` and return the
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
            "compact" => self.compact_command(params).await,
            "steer" => self.steer_command(params).await,
            "interrupt" => self.interrupt_command(params).await,
            "approve" | "deny" | "answer" => self.reply_command(params).await,
            _ => Err(Error::new(
                "unsupported",
                "OpenCode plugin command verb is not implemented",
            )),
        }
    }

    async fn compact_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        if params.turn_id.is_some() {
            return Err(Error::new(
                "native_not_dispatched",
                "OpenCode compaction requires an idle session",
            ));
        }
        let session = ready_session(&mut self.session, &params)?;
        let session_id = session
            .session_id
            .as_deref()
            .ok_or_else(|| Error::new("native_not_dispatched", "OpenCode session is not bound"))?;
        let terminal = session.events.begin_compaction(&params.command_id).await?;
        let deadline = Instant::now() + RPC_TIMEOUT;
        let receipt = match session.http.compact(session_id, deadline).await {
            Ok(receipt) => receipt,
            Err(HttpOutcome::Rejected { reason, .. } | HttpOutcome::NotDispatched { reason }) => {
                session.events.refuse_compaction().await?;
                return Ok(json!({"state": "rejected", "reason": reason}));
            }
            Err(HttpOutcome::Unknown { .. }) => {
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    "OpenCode compaction admission is unconfirmed",
                ));
            }
        };
        if receipt.session_id != session_id
            || receipt.id.trim().is_empty()
            || receipt.kind != "compaction"
            || receipt.delivery != "queue"
        {
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                "OpenCode compaction receipt differs from the request",
            ));
        }
        let applied = match timeout_at(deadline, terminal).await {
            Ok(Ok((input, applied))) if input == receipt.id => applied,
            _ => {
                // Native message history carries a terminal fact under the
                // inbox id even when the stream leaves the command uncertain.
                if !session.events.current_status().await.is_ok_and(|status| {
                    !status.compaction_in_flight && !status.source_unknown && !status.batch_unknown
                }) {
                    session.phase = SessionPhase::Unknown;
                }
                let Some(outcome) = session
                    .http
                    .compaction_outcome(
                        session_id,
                        &receipt.id,
                        Instant::now() + std::time::Duration::from_secs(5),
                    )
                    .await
                    .unwrap_or(None)
                else {
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "OpenCode compaction completion is unconfirmed",
                    ));
                };
                outcome
            }
        };
        Ok(json!({
            "state": if applied { "applied" } else { "failed" },
            "command_id": params.command_id,
            "compaction": {"completed": applied, "receipt_id": receipt.id},
        }))
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
        let session = ready_session(&mut self.session, &params)?;
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
        // The exact catalogue row supplies effort variants when available.
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
        // The same membership rule as the start and resume paths, before
        // either one writes: a model outside the frozen
        // fragment refuses here, never after quiescing or switching.
        if let (Some(provider), Some(model)) =
            (session.local_provider.as_ref(), requested.model.as_deref())
            && !local_provider_contains(provider, model)
        {
            return Ok(json!({
                "state": "rejected",
                "reason": "OpenCode local provider does not declare the requested model",
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
        // explicit request supplies its provider/model pair,
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
        let selection = ModelSelection::new(
            &target_model,
            catalogue_entry_for(&catalogue, &target_ref.provider_id, &target_ref.id),
        );
        if let Err(error) = validate_catalogue_effort(&selection, requested.effort.as_deref()) {
            return Ok(json!({"state": "rejected", "reason": error.message}));
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
                    return Ok(with_model_catalogue_diagnostic(
                        json!({
                            "state": "rejected",
                            "reason": reason,
                            "origin": FailureOrigin::Harness,
                            "origin_evidence": "OpenCode model switch returned a native error",
                            "where": "plugin.opencode.configure.switch_model",
                        }),
                        selection.catalogue_omits_model(),
                    ));
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
            "model": selection.id,
            "model_verified": true,
        });
        if let Some(effort) = requested.effort {
            response["effort"] = Value::String(effort);
            response["effort_verified"] = Value::Bool(true);
        }
        Ok(with_model_catalogue_diagnostic(
            response,
            selection.catalogue_omits_model(),
        ))
    }

    async fn send_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = ready_session(&mut self.session, &params)?;
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
        let session = if params.owner_control {
            let session = retained_session(
                &mut self.session,
                &params.agent_id,
                params.generation,
                &params.command_id,
            )?;
            if session.phase == SessionPhase::Stopped || native_io_blocked(session) {
                return Err(Error::new(
                    "native_not_dispatched",
                    "OpenCode native session is closed",
                ));
            }
            session
        } else {
            ready_session(&mut self.session, &params)?
        };
        let command_id = params.command_id;
        let Some(turn_id) = params.turn_id.as_deref() else {
            let status = session
                .events
                .current_status()
                .await
                .map_err(|error| error.with_code("native_not_dispatched"))?;
            return eliot_plugin::session::idle_interrupt_result(
                &command_id,
                params.owner_control
                    && session.phase == SessionPhase::Ready
                    && status.ready
                    && status.connected
                    && !status.stopped
                    && !status.compaction_in_flight
                    && status.active_command_id.is_none()
                    && status.active_turn_id.is_none()
                    && !status.source_unknown
                    && !status.batch_unknown
                    && status.failure.is_none(),
            );
        };
        if !params.owner_control {
            let live = session.events.check_turn(turn_id).await.map_err(|_| {
                Error::new("unknown", "OpenCode interrupt turn check is unresolved")
            })?;
            if !live {
                return Err(Error::new(
                    "native_not_dispatched",
                    "OpenCode interrupt named a finished turn",
                ));
            }
        }
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "OpenCode session identity is unresolved"))?;
        let outcome = session
            .http
            .interrupt(&session_id, Instant::now() + RPC_TIMEOUT)
            .await;
        Self::finish_interrupt(session, &command_id, turn_id, outcome).await
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
        let session = ready_session(&mut self.session, &params)?;
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
        let settlement = settle_unproven_input(removed, message);
        if settlement.latch_unknown {
            session.phase = SessionPhase::Unknown;
        }
        settlement.error
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
        let session = ready_session(&mut self.session, &params)?;
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
        let session = retained_session(
            &mut self.session,
            &params.agent_id,
            params.generation,
            &params.command_id,
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
                let mut turn = json!({
                    "command_id": params.command_id,
                    "turn_id": request.turn_id,
                    "status": terminal.outcome,
                    "terminal": true,
                    "final_text": null,
                    "native_error": terminal.idle_error.map(|error| error.message),
                });
                if let Some(text) = terminal.final_text {
                    eliot_plugin::session::attach_final_text(
                        &mut turn,
                        &session.secret_policy.redact_text(&text),
                    );
                }
                turn
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
        let session = retained_session(
            &mut self.session,
            &params.agent_id,
            params.generation,
            &params.command_id,
        )?;
        // Latch once before native I/O. An uncertain close is never replayed.
        if begin_stop(
            &mut session.stop_command_id,
            session.phase == SessionPhase::Stopped,
            &params.command_id,
            "OpenCode",
        )? {
            return Ok(json!({"state": "applied"}));
        }
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
        self.shutdown_paths(false).await
    }

    async fn shutdown_owned(&mut self) {
        let _ = self.shutdown_paths(true).await;
    }

    async fn shutdown_paths(&mut self, best_effort: bool) -> Result<Value> {
        eliot_plugin::session::shutdown_session(
            self.session.as_mut(),
            best_effort,
            async |session| {
                // Shutdown with proved resource closure ends the owned path
                // without replaying native control. An already-acked End is
                // likewise never repeated.
                if session.phase == SessionPhase::Stopped || session.resources_proved_closed {
                    Ok(())
                } else {
                    terminate_session(session, true).await
                }
            },
            async |session| {
                session.events.close().await;
            },
        )
        .await
    }
}

/// The frozen profile auth selector: the one explicitly selected native
/// integration (ID or name) whose canonical credential the pre-serve
/// transfer prepares. Required on the cloud path and nonsecret; the
/// capacity-only account field is never consulted for authentication. On
/// the fragment path the selector must be absent: no canonical
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
                "OpenCode local provider profiles cannot name a canonical auth_integration",
            ));
        }
        return Ok(None);
    }
    let Some(raw) = raw else {
        if let Some(migration) = retained_eliot_local_migration(profile) {
            return Err(migration);
        }
        return Err(Error::new(
            "profile_error",
            "OpenCode profile requires an explicit auth_integration selector",
        ));
    };
    IntegrationSelector::new(raw)
        .map(Some)
        .map_err(|error| error.with_code("profile_error"))
}

/// Older local-provider snapshots need a new profile before they can run.
fn retained_eliot_local_migration(profile: &serde_json::Map<String, Value>) -> Option<Error> {
    let has_old_fields = [
        "provider_base_url",
        "backend",
        "api_key_env",
        "context_limit",
        "output_limit",
        "provider_package",
    ]
    .iter()
    .any(|field| profile.contains_key(*field));
    let is_eliot_local = profile
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| model.split('/').next().unwrap_or("") == "eliot-local");
    if has_old_fields || is_eliot_local {
        Some(Error::new(
            "profile_error",
            "OpenCode retained provider configuration must migrate to local_provider with model <provider>/<model>",
        ))
    } else {
        None
    }
}

fn start_local_provider(profile: &serde_json::Map<String, Value>) -> Result<Option<LocalProvider>> {
    if profile.contains_key("provider") {
        return Err(Error::new(
            "profile_error",
            "OpenCode retained provider configuration must migrate to local_provider",
        ));
    }
    let Some(value) = profile.get("local_provider") else {
        return match retained_eliot_local_migration(profile) {
            Some(error) => Err(error),
            None => Ok(None),
        };
    };
    let provider: LocalProvider = serde_json::from_value(value.clone()).map_err(|_| {
        Error::new(
            "profile_error",
            "OpenCode retained local_provider has invalid fields",
        )
    })?;
    validate_local_provider(&provider, profile.get("pass_env"))?;
    if !profile
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|model| local_provider_contains(&provider, model))
    {
        return Err(Error::new(
            "profile_error",
            "OpenCode local provider profile must select one of its declared models",
        ));
    }
    if profile.contains_key("auth_integration") {
        return Err(Error::new(
            "profile_error",
            "OpenCode local provider profiles cannot name a canonical auth_integration",
        ));
    }
    Ok(Some(provider))
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
/// The backoff is the shared readiness-settle policy (50 ms doubling to
/// 1 s, clamped to this cap).
const MCP_SETTLE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(20);

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
    let mut retry = Retry::new(RetryOperation::ReadinessSettle);
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
        // pending observation. The shared readiness-settle backoff waits
        // from 50 ms up to 1 s, clamping both the native reads and the
        // sleeps to the same settle/start deadline.
        retry.wait_settle(cap).await;
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

fn local_provider_contains(provider: &LocalProvider, model: &str) -> bool {
    split_provider_model(model).is_some_and(|(id, model)| provider.contains_model(id, model))
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

fn managed_mcp_environment(parsed: &ParsedStart) -> std::collections::HashMap<String, String> {
    parsed
        .swarm_mcp_environment
        .iter()
        .map(|(name, value)| {
            (
                name.to_string_lossy().into_owned(),
                value.to_string_lossy().into_owned(),
            )
        })
        .collect()
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
/// local stdio shape with exactly the generated argv, no URL, and the
/// expected scoped Agent environment; anything else refuses the start.
fn verify_managed_mcp_transport(
    documents: &[ConfigEntry],
    expected_command: &[String],
    expected_environment: &std::collections::HashMap<String, String>,
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
        && (server.environment.as_ref() == Some(expected_environment)
            || (expected_environment.is_empty() && server.environment.is_none()));
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
/// requested pair, while an effort-only configure reuses
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

#[derive(Default)]
struct ModelCatalogueReadback {
    context: Option<i64>,
    output: Option<i64>,
    omits_model: bool,
}

impl ModelCatalogueReadback {
    fn from_selection(selection: &ModelSelection<'_, NativeModelEntry>) -> Self {
        let limits = selection
            .catalogue_entry
            .and_then(|entry| entry.limit.as_ref());
        Self {
            context: limits.and_then(|limit| limit.context),
            output: limits.and_then(|limit| limit.output),
            omits_model: selection.catalogue_omits_model(),
        }
    }

    fn attach(self, mut effective: Value) -> Value {
        effective["context_limit"] = json!(self.context);
        effective["output_limit"] = json!(self.output);
        with_model_catalogue_diagnostic(effective, self.omits_model)
    }
}

fn validate_catalogue_effort(
    selection: &ModelSelection<'_, NativeModelEntry>,
    effort: Option<&str>,
) -> Result<()> {
    if let Some(effort) = effort
        && !selection.allows_effort(|entry| {
            catalogue_entry_variants(entry)
                .is_none_or(|variants| variants.iter().any(|variant| variant == effort))
        })
    {
        return Err(Error::new(
            "profile_error",
            format!(
                "OpenCode effort {effort} is not a variant of model {}",
                selection.id
            ),
        ));
    }
    Ok(())
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
/// bytes, no controls. This checks the shape before any native I/O.
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
    use eliot_plugin::session::{RetainedStopStep, stop_retained_session};

    let deadline = Instant::now() + RPC_TIMEOUT;
    let result = stop_retained_session("OpenCode", force, async |step| match step {
        RetainedStopStep::Begin => session.events.begin_stop().await,
        RetainedStopStep::VerifyRetention => {
            if let Some(id) = session.session_id.as_deref().filter(|id| !id.is_empty())
                && session
                    .http
                    .get_session(id, deadline)
                    .await
                    .is_ok_and(|info| info.id == id)
            {
                Ok(())
            } else {
                Err(Error::new(
                    "unknown",
                    "OpenCode session retention was not confirmed",
                ))
            }
        }
        RetainedStopStep::Drain | RetainedStopStep::End => {
            let result = match step {
                RetainedStopStep::Drain => session.events.drain_stop().await,
                _ => session.events.end_stop().await,
            };
            if result.is_ok() {
                session.stop_end_ack_proved = true;
            }
            result
        }
        RetainedStopStep::Process => {
            let exit = session.server.terminate_leader(force, deadline).await;
            if exit.is_ok() {
                session.resources_proved_closed = true;
            }
            exit
        }
        RetainedStopStep::Diagnostics => {
            session.server.finish_diagnostics(deadline).await;
            Ok(())
        }
    })
    .await;
    session.phase = if result.is_ok() {
        SessionPhase::Stopped
    } else {
        SessionPhase::Unknown
    };
    result
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
    let mut retry =
        eliot_plugin::retry::Retry::new(eliot_plugin::retry::RetryOperation::SessionBinding);
    loop {
        if Instant::now() >= deadline {
            return false;
        }
        match http.eliot_context_bind(session_id, deadline).await {
            Ok(output) => {
                return crate::context_hooks::bind_verified(&output, session_id);
            }
            Err(HttpOutcome::Rejected { .. }) => return false,
            Err(HttpOutcome::Unknown { .. } | HttpOutcome::NotDispatched { .. }) => {
                if !retry
                    .wait(eliot_plugin::retry::RetryFact::Idempotent, Some(deadline))
                    .await
                {
                    return false;
                }
            }
        }
    }
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
    PluginManifest::per_agent(
        "opencode",
        env!("CARGO_PKG_VERSION"),
        "2.2.7",
        &[
            "launch",
            "resume",
            "send",
            "configure",
            "compact",
            "models",
            "steer",
            "interrupt",
            "approve",
            "deny",
            "answer",
            "stop",
        ],
        eliot_plugin::protocol::ManifestFeatures {
            discover: false,
            observe: false,
            start_requires_input: false,
            native_children: false,
        },
        BTreeMap::from([(
            "controlled".to_owned(),
            stream_sources(&["message", "request", "session", "tool", "turn"]),
        )]),
        "none",
    )
}
