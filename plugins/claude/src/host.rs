use crate::{
    events::{self, EventConfig, EventHandle, EventStatus, TurnOutcome},
    native::{
        ClaudeClient, ClaudeMcpConfigFile, ClaudeProcess,
        types::{InitializeReadback, OutboundUserInput, OutboundUserMessage},
    },
};
use eliot_core::{ContextItem, Error, InjectPoint, McpPolicy, Result, SecretPolicy};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    native_transport::NativeLaunch,
    protocol::stream_sources,
    session::{
        self, attach_reply_diagnostics, begin_stop, contextual_error, decode,
        ensure_dedicated_reader, initialize_fields_valid, native_no_write, not_initialized,
        optional_profile_text, parse_resume_subject, register_native_process, rejected_control,
        request_reply_ids, unknown_control, unknown_snapshot, validate_binding, validate_identity,
        validate_source_plan, with_claimed_context, with_context_points, with_safe_point_steer,
    },
    skill,
};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    path::{Path, PathBuf},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
// The daemon gives agent.command 30 s. Share one shorter budget across the
// input write and association, leaving time to return the actual native cause.
const SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(25);

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
    /// Retained after native exit and output drain; retirement does not
    /// depend on delivery of the start RPC response. Proven-no-effect
    /// starts never spawn, so this stays false for them: the daemon
    /// retires the plugin on the failed value instead.
    endpoint_exited: bool,
}

/// Where a start failure happened relative to the first native CLI spawn.
/// Every variant except `PostSpawn` proves no native process exists yet:
/// plan validation runs before any native resource, the MCP config write
/// and skill-root check only touch the private instance tree, the login
/// check runs bounded read-only probes (plus the CLI's own browser login,
/// whose failure leaves no session or prompt), and a failed spawn by
/// definition produced no child. `PostSpawn` is the boundary pin: anything
/// at or after a live child keeps its honest classification and never
/// converts. Only constructed in tests to pin the boundary: production
/// never converts a post-spawn failure into a proven-no-effect value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StartPhase {
    /// Frozen-profile/workspace/cmd/settings validation in `plan_start`,
    /// including the `inherit` owner-scan refusal, MCP policy checks, and
    /// the resume-scope check. Covers the cmd-resolution and settings
    /// pre-spawn refusals.
    Plan,
    /// Private per-session MCP config file creation (MCP config write).
    McpConfig,
    /// Private per-session file for Claude's append-system-prompt option.
    OwnerRules,
    /// Daemon-materialized skill-root availability check (skill
    /// materialization) before `--plugin-dir` is passed to the CLI.
    Skill,
    /// Read-only CLI auth status before any session.
    Auth,
    /// Owned CLI spawn failure with no child produced.
    Spawn,
    /// At or after a live native child. Never proven-no-effect.
    PostSpawn,
}

/// Whether a failure in this start phase proves no native process was
/// spawned by this generation, so the launch settles as a definite failed
/// start instead of unknown. This is the regression anchor for the
/// pre-spawn rule: every phase before the first spawn maps here, and no
/// post-spawn phase may. Unlike the OpenCode auth transfer, Claude has no
/// uncertain pre-spawn write: auth status, validation and private-tree
/// file preparation happen before any session effect, so all pre-spawn
/// codes (including `unknown`) settle failed. Adding a phase forces an
/// explicit entry in the classification test below.
fn start_point_for_phase(phase: StartPhase) -> eliot_plugin::start_classification::StartPoint {
    use eliot_plugin::start_classification::StartPoint;
    match phase {
        StartPhase::Plan
        | StartPhase::McpConfig
        | StartPhase::OwnerRules
        | StartPhase::Skill
        | StartPhase::Auth => StartPoint::BeforeSpawn,
        StartPhase::Spawn => StartPoint::SpawnRefused,
        StartPhase::PostSpawn => StartPoint::AfterDispatch,
    }
}

fn start_failure_is_proven_no_effect(phase: StartPhase, _code: &str) -> bool {
    eliot_plugin::start_classification::is_proven_no_effect(start_point_for_phase(phase))
}

/// One pre-spawn failure: where it happened and the triggering error. The
/// caller converts this into the authoritative proven-no-effect failed
/// start instead of an error, so the daemon settles the launch failed and
/// retires the plugin instead of pinning an unknown Agent.
struct PhaseFailure {
    phase: StartPhase,
    // Boxed: the diagnostics-carrying Error is large (clippy result_large_err).
    error: Box<Error>,
}

const INHERIT_MCP_CHECK: &str = "plugin.claude.start.plan.inherit_mcp";

/// Typed reason for one proven-no-effect failed start. Static per phase
/// plus the triggering error code. Use a fixed reason unless the refusal
/// carries an explicitly safe owner action.
fn prespawn_reason(phase: StartPhase, code: &str) -> String {
    let what = match phase {
        StartPhase::Plan => "Claude start refused pre-spawn",
        StartPhase::McpConfig => "Claude MCP config could not be prepared",
        StartPhase::OwnerRules => "Claude owner rules could not be prepared",
        StartPhase::Skill => "Claude skill root is unavailable",
        StartPhase::Auth => "Claude CLI authentication status is unavailable",
        StartPhase::Spawn => "Claude CLI failed to spawn",
        StartPhase::PostSpawn => "Claude start failed after native spawn",
    };
    eliot_plugin::start_classification::format_start_reason(
        what,
        code,
        "no native process was spawned by this generation",
    )
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SessionPhase {
    Ready,
    Unknown,
    Stopped,
}

/// Identity gate and retained-session binding shared by snapshot and stop.
fn retained_session<'a>(
    slot: &'a mut Option<AgentSession>,
    agent_id: &str,
    generation: i64,
    command_id: &str,
) -> Result<&'a mut AgentSession> {
    validate_identity(agent_id, generation, command_id, "Claude")?;
    let session = slot
        .as_mut()
        .ok_or_else(|| Error::new("not_found", "Claude Agent session is not retained"))?;
    validate_binding(
        &session.agent_id,
        session.generation,
        agent_id,
        generation,
        "Claude",
    )?;
    Ok(session)
}

struct AgentSession {
    agent_id: String,
    generation: i64,
    start_command_id: String,
    phase: SessionPhase,
    session_id: Option<String>,
    native_scope: String,
    native: ClaudeProcess,
    // Keep the private per-session MCP config alive with the owned CLI.
    mcp_config: Option<ClaudeMcpConfigFile>,
    native_client: ClaudeClient,
    events: EventHandle,
    pending_turn: PendingTurn,
    stop_command_id: Option<String>,
}

/// A send deadline ends the RPC wait, not the native input. Keep its exact
/// receiver until the event task commits an association or a definite refusal.
#[derive(Default)]
struct PendingTurn {
    pending: Option<TurnWaiter>,
}

struct TurnWaiter {
    outcome: oneshot::Receiver<TurnOutcome>,
}

impl PendingTurn {
    fn finish(&mut self, outcome: TurnOutcome) -> Value {
        self.pending
            .take()
            .expect("registered send owns its waiter");
        match outcome {
            TurnOutcome::Associated(turn_id) => {
                json!({"state": "applied", "turn_id": turn_id})
            }
            TurnOutcome::Rejected(reason) => rejected_control(&reason),
            TurnOutcome::Unknown(reason) => {
                send_unknown("claude_turn_association_unknown", &reason)
            }
        }
    }

    fn reconcile(&mut self) {
        let Some(pending) = self.pending.as_mut() else {
            return;
        };
        match pending.outcome.try_recv() {
            Ok(outcome) => {
                self.finish(outcome);
            }
            Err(oneshot::error::TryRecvError::Empty) => {}
            Err(oneshot::error::TryRecvError::Closed) => self.pending = None,
        }
    }
}

fn send_unknown(cause: &str, reason: &str) -> Value {
    json!({"state": "unknown", "cause": cause, "reason": reason})
}

/// Validated start inputs: the frozen profile, Agent-scoped MCP context, and
/// minted session id. Built before any spawn; every later start phase reads
/// from it.
struct StartPlan {
    workspace: PathBuf,
    instance_dir: PathBuf,
    data_root: PathBuf,
    swarm_binary: PathBuf,
    mcp_environment: Vec<(OsString, OsString)>,
    program: OsString,
    environment: Vec<(OsString, OsString)>,
    secrets: Vec<String>,
    config_dir: Option<String>,
    credential: String,
    session_id: String,
    /// Requested model id, or `None` when the profile omitted it and the
    /// CLI default applies (`--model` omitted; effective read back from
    /// initialize/stream).
    requested_model: Option<String>,
    requested_permission_mode: Option<String>,
    /// Frozen Claude Code `--effort` level (closed CLI vocabulary); absent
    /// keeps the CLI default.
    requested_effort: Option<String>,
    inherit_cloud_hostname: Option<String>,
    requested_mcp_policy: McpPolicy,
    requested_mcp_allow: Vec<String>,
    /// Frozen native-subagent switch: `deny` disables the native subagent
    /// tool for the session via `--disallowedTools Task`.
    native_subagents_deny: bool,
    resume_ref: Option<String>,
}

/// One unconfirmed stage of the shared uncommitted-session rollback.
/// The event consumer stays alive through termination and drain while
/// suppressed, then closes: closing first drops the frame queue and a
/// late trailing record overflows even with a confirmed child exit.
struct RollbackCleanup {
    stage: &'static str,
    error: Error,
}

/// Pure stage selection for the shared rollback: the first unconfirmed
/// stage wins, in execution order. Callers keep their own failure
/// classification and retained session state.
fn select_rollback_failure(
    begin_error: Option<Error>,
    exit_error: Option<Error>,
    drain_error: Option<Error>,
) -> Option<RollbackCleanup> {
    if let Some(error) = begin_error {
        Some(RollbackCleanup {
            stage: "begin_stop",
            error,
        })
    } else if let Some(error) = exit_error {
        Some(RollbackCleanup {
            stage: "terminate",
            error,
        })
    } else {
        drain_error.map(|error| RollbackCleanup {
            stage: "drain",
            error,
        })
    }
}

/// Serve one request, then continue the loop or end it, with whether a
/// shutdown reply completed.
enum Serve {
    Continue,
    End(bool),
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
    let mut native_endpoint_retired = false;
    let mut native_retirement = Ok(());
    let mut pending_request = None;
    loop {
        if plugin
            .failed_start
            .as_ref()
            .is_some_and(|failed| failed.endpoint_exited)
        {
            native_endpoint_retired = true;
            break;
        }
        let request = if let Some(request) = pending_request.take() {
            Some(request)
        } else {
            tokio::select! {
                biased;
                () = eliot_plugin::native_lifecycle::wait_for_native_exit(
                    plugin.session.as_mut().map(|session| &mut session.native.transport)
                ) => {
                    native_endpoint_retired = true;
                    None
                }
                request = requests.recv() => request,
            }
        };
        let Some(request) = request else {
            break;
        };
        let (serve, next) =
            serve_request_preemptible(&mut plugin, &client, &mut requests, request).await;
        if let Some(next) = next {
            pending_request = Some(next);
        }
        match serve {
            Serve::Continue => {}
            Serve::End(done) => {
                completed_shutdown = done;
                break;
            }
        }
    }

    if native_endpoint_retired {
        if let Some(session) = plugin.session.as_mut() {
            native_retirement = eliot_plugin::native_lifecycle::drain_native_exit(
                &mut session.native.transport,
                async {
                    session.events.drain_native().await?;
                    session.events.close().await;
                    Ok(())
                },
            )
            .await;
        }
    } else if !completed_shutdown {
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
            "Claude plugin input closed before shutdown completed",
        ))
    }
}

/// Serve one ordinary request: dispatch it, reply, and report whether the loop
/// must end.
async fn serve_request(
    plugin: &mut ClaudePlugin,
    client: &PeerClient,
    request: RpcRequest,
) -> Serve {
    let is_shutdown = request.method == "shutdown";
    let result = plugin.dispatch(&request, client).await;
    let response = client
        .respond(request.id, result, Instant::now() + RPC_TIMEOUT)
        .await;
    if is_shutdown {
        Serve::End(response.is_ok())
    } else if response.is_err() {
        Serve::End(false)
    } else {
        Serve::Continue
    }
}

/// Serve one ordinary request, preempting ONLY a status snapshot: a host
/// control arriving during snapshot dispatch answers the snapshot `busy`
/// and is retained for the next iteration. Every other method dispatches
/// to completion, so no mutating effect is dropped or canceled. A dead
/// transport ends the loop.
async fn serve_request_preemptible(
    plugin: &mut ClaudePlugin,
    client: &PeerClient,
    requests: &mut mpsc::Receiver<RpcRequest>,
    request: RpcRequest,
) -> (Serve, Option<RpcRequest>) {
    if request.method != "agent.snapshot" {
        return (serve_request(plugin, client, request).await, None);
    }
    match eliot_plugin::preemptible_snapshot(requests, plugin.dispatch(&request, client)).await {
        eliot_plugin::SnapshotDispatch::Ready(result) => {
            let response = client
                .respond(request.id, result, Instant::now() + RPC_TIMEOUT)
                .await;
            if response.is_err() {
                (Serve::End(false), None)
            } else {
                (Serve::Continue, None)
            }
        }
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
                (Serve::End(false), None)
            } else if let Some(next) = next {
                (Serve::Continue, Some(next))
            } else {
                (Serve::End(false), None)
            }
        }
    }
}

impl ClaudePlugin {
    /// Called only after native exit and reader cleanup succeeded. Keep this
    /// exited-start latch until the host loop retires, even if its RPC reply
    /// is late.
    fn retain_exited_start(&mut self, error: Error) -> Error {
        self.failed_start = Some(FailedStart {
            endpoint_exited: true,
        });
        error
    }

    /// Retain a proven-no-effect start failure and build its failed-state
    /// result: no native process was spawned and no prompt was sent, so the
    /// launch is definitely failed, never unknown. The reason names the
    /// failed check in short non-secret words; it is persisted in the daemon
    /// outcome, so it must never carry credentials or raw native output.
    fn retain_proven_no_effect_start(&mut self, params: &AgentStartParams, error: Error) -> Value {
        let value = eliot_plugin::session::failed_start_value(params, error);
        self.failed_start = Some(FailedStart {
            endpoint_exited: false,
        });
        value
    }

    /// Convert one pre-spawn failure into its retained failed-state result.
    /// By construction the failure happened before the native CLI was
    /// spawned and carries no uncertain effect, so the launch is definitely
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
        // Auth-phase messages are fixed owner-safe phrases (signed out, not
        // found at the profile's cmd, timed out, no JSON status): show the
        // specific one instead of a generic phase reason.
        let reason = if failure.phase == StartPhase::Auth {
            failure.error.message.to_string()
        } else {
            let mut reason = prespawn_reason(failure.phase, &failure.error.code);
            if failure.error.failure_where.as_deref() == Some(INHERIT_MCP_CHECK) {
                reason.push_str(
                    ". Set mcp = \"minimal\" in the Claude profile, or start an ad-hoc Claude chat",
                );
            }
            reason
        };
        let phase = match failure.phase {
            StartPhase::Plan => "plan",
            StartPhase::McpConfig => "mcp_config",
            StartPhase::OwnerRules => "owner_rules",
            StartPhase::Skill => "skill",
            StartPhase::Auth => "auth",
            StartPhase::Spawn => "spawn",
            StartPhase::PostSpawn => "post_spawn",
        };
        let mut error = *failure.error;
        if error.origin.is_none()
            && matches!(
                failure.phase,
                StartPhase::McpConfig
                    | StartPhase::OwnerRules
                    | StartPhase::Skill
                    | StartPhase::Auth
                    | StartPhase::Spawn
            )
        {
            error = error.with_origin(
                eliot_core::FailureOrigin::Environment,
                "native executable, private file or authentication status check failed",
            );
        }
        let error = eliot_plugin::session::classify_start_failure(error, "claude", phase);
        self.retain_proven_no_effect_start(params, error.with_message(reason))
    }

    async fn dispatch(&mut self, request: &RpcRequest, client: &PeerClient) -> Result<Value> {
        let result = match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "quota.read" => crate::quota::read(&request.params).await,
            "model.list" => self.model_list(&request.params).await,
            "agent.start" => self.start(&request.params, client).await,
            "agent.command" => self.command(request).await,
            "agent.snapshot" => self.snapshot(&request.params).await,
            "agent.stop" => self.stop(&request.params).await,
            "shutdown" => self.shutdown().await,
            _ => Err(Error::new(
                "unsupported",
                "Claude plugin method is not implemented",
            )),
        };
        self.post_process(&request.method, result)
    }

    /// Redact and attach diagnostics to one reply.
    fn post_process(&self, method: &str, result: Result<Value>) -> Result<Value> {
        let Some(session) = self.session.as_ref() else {
            return result;
        };
        attach_reply_diagnostics(
            result,
            method,
            "Claude",
            |error| session.native_client.diagnostic_error(error),
            |_| Error::new("unknown", ""),
        )
    }

    async fn model_list(&self, raw: &Value) -> Result<Value> {
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Claude"))?;
        ensure_dedicated_reader(
            self.session.is_some() || self.failed_start.is_some(),
            "Claude",
            "model.list",
        )?;
        crate::native::models::read(raw, Path::new(&initialized.instance_dir)).await
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams = decode(raw, "Claude plugin initialize params are invalid")?;
        if !initialize_fields_valid(&params) {
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
            &["session", "turn", "message", "request", "tool"],
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
        // An omitted model omits `--model` so the CLI default applies; the
        // effective model is read back from initialize/stream. Requested
        // stays absent separately from the effective readback. A resume
        // running under an inherited `configure` overlay realigns to the
        // overlay's effective model instead of the frozen snapshot.
        let requested_model: Option<String> = params
            .model_override
            .clone()
            .filter(|value| !value.trim().is_empty())
            .or_else(|| {
                profile
                    .get("model")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .map(str::to_owned)
            });
        // The daemon freezes the closed CLI permission-mode vocabulary here;
        // unset means the CLI-default posture.
        let requested_permission_mode =
            optional_profile_text(profile, "permission_mode", "Claude")?;
        let requested_effort = optional_profile_text(profile, "effort", "Claude")?;
        // Frozen worker MCP policy: inherit keeps the normal MCP precedence,
        // minimal is enforced with the installed `--strict-mcp-config` flag.
        let requested_mcp_policy = McpPolicy::parse_snapshot(profile)
            .map_err(|error| Error::new("profile_error", format!("Claude {}", error.message)))?;
        let requested_mcp_allow = eliot_core::parse_snapshot_allow_list(profile)
            .map_err(|error| Error::new("profile_error", format!("Claude {}", error.message)))?;
        check_mcp_allow_empty(&requested_mcp_allow)?;
        let native_subagents_deny =
            profile.get("native_subagents").and_then(Value::as_str) != Some("allow");
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
        let mcp_environment = session::agent_mcp_environment(
            params.mcp_environment.as_ref(),
            params.target_environment.as_ref(),
            params.pass_agent_identity_to_shell,
            &params.agent_id,
            params.generation,
            "Claude",
        )?;
        secrets.extend(mcp_environment.secrets.iter().cloned());
        check_effort_override(requested_effort.as_deref(), &environment)?;
        let data_root = ClaudeMcpConfigFile::require_agent_data_root(
            &mcp_environment.variables,
            &params.agent_id,
            params.generation,
        )?;
        let instance_dir = PathBuf::from(&initialized.instance_dir);
        if !instance_dir.is_absolute() || !instance_dir.is_dir() {
            return Err(Error::new(
                "not_allowed",
                "Claude MCP registration requires its private Agent instance directory",
            ));
        }
        let swarm_binary = std::env::current_exe().map_err(|_| {
            Error::new(
                "platform_io",
                "could not locate the current Eliot executable for Claude MCP",
            )
        })?;
        if !swarm_binary.is_file() {
            return Err(Error::new(
                "platform_io",
                "the current Eliot executable for Claude MCP is unavailable",
            ));
        }
        let inherit_cloud_hostname = if requested_mcp_policy == McpPolicy::Inherit {
            let hostname = eliot_core::parse_snapshot_cloud_hostname(profile);
            crate::owner_scan::refuse_owner_mcp_sources(
                &workspace,
                config_dir.as_deref(),
                environment.iter().find_map(|(name, value)| {
                    name.eq_ignore_ascii_case("ENABLE_CLAUDEAI_MCP_SERVERS")
                        .then(|| value.to_str())
                        .flatten()
                }),
                &swarm_binary,
                hostname.as_deref(),
            )
            .map_err(|error| error.with_where(INHERIT_MCP_CHECK))?;
            hostname
        } else {
            None
        };
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
            instance_dir,
            data_root,
            swarm_binary,
            mcp_environment: mcp_environment.variables,
            program,
            environment,
            secrets,
            config_dir,
            credential: initialized.credential.clone(),
            session_id,
            requested_model,
            requested_permission_mode,
            requested_effort,
            inherit_cloud_hostname,
            requested_mcp_policy,
            requested_mcp_allow,
            native_subagents_deny,
            resume_ref,
        })
    }

    /// Shared rollback for a session whose readiness never committed: begin
    /// stop, terminate the owned CLI, drain its output, then close the event
    /// consumer. Returns the first unconfirmed cleanup stage, if any.
    async fn rollback_uncommitted_session(session: &mut AgentSession) -> Option<RollbackCleanup> {
        let begin_error = session.events.begin_stop().await.err();
        let exit_error = session
            .native
            .terminate(false, Instant::now() + RPC_TIMEOUT)
            .await
            .err();
        let drain_error = session
            .native
            .await_output_drained(Instant::now() + RPC_TIMEOUT)
            .await
            .err();
        session.events.close().await;
        select_rollback_failure(begin_error, exit_error, drain_error)
    }

    /// Settle a start whose native process the host would not register. No
    /// session or input was sent; if the owned CLI's cleanup is unconfirmed
    /// the session is kept unknown instead of dropped. The event consumer
    /// stays alive through termination+drain (suppressed, so no readiness or
    /// input can commit), then closes: closing first drops the frame queue
    /// and a late trailing record overflows even with a confirmed child exit.
    async fn fail_unregistered_start(&mut self, mut session: AgentSession, error: Error) -> Error {
        if let Some(cleanup) = Self::rollback_uncommitted_session(&mut session).await {
            self.session = Some(session);
            let message = format!(
                "{}; owned CLI cleanup is unconfirmed at {} ({}): {}; no session or input was sent",
                error.message, cleanup.stage, cleanup.error.code, cleanup.error.message
            );
            return contextual_error(error, "unknown", message);
        }
        let error = session.native_client.diagnostic_error(error);
        self.retain_exited_start(error)
    }

    /// Discard a failed start and latch its native exit. A start
    /// whose cleanup is unconfirmed is `unknown`: the session is kept
    /// instead of dropped (like `fail_unregistered_start`). The event
    /// consumer stays alive through termination+drain (suppressed, so no
    /// readiness or input can commit), then closes. PR284's exact CI cause
    /// was dropped by `.is_err()`, so the close-before-drain race is
    /// source-proven but exact CI causality is not fully proven.
    async fn reject_start(&mut self, mut session: AgentSession, error: Error) -> Error {
        if let Some(cleanup) = Self::rollback_uncommitted_session(&mut session).await {
            session.phase = SessionPhase::Unknown;
            self.session = Some(session);
            let message = format!(
                "{}; owned CLI cleanup is unconfirmed at {} ({}): {}; session.ready was never committed",
                error.message, cleanup.stage, cleanup.error.code, cleanup.error.message
            );
            return contextual_error(error, "unknown", message);
        }
        // Cleanup proved native exit and reader drain. Keep the dispatched
        // initialize/resume effect unknown while the daemon separately proves
        // plugin custody exit and settles this starting generation.
        let error = if error.code == "unknown" {
            error.with_code(eliot_plugin::native_stdio::NATIVE_ENDPOINT_EXITED)
        } else {
            error
        };
        let error = session.native_client.diagnostic_error(error);
        self.retain_exited_start(error)
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
        plan: &StartPlan,
    ) -> Result<(AgentSession, Value)> {
        let callback_id = session.events.hook_callback_id().to_owned();
        let readback = match session
            .native_client
            .control(
                "initialize",
                json!({
                    "hooks": {
                        "PreToolUse": [{
                            "hookCallbackIds": [callback_id.clone()],
                            "timeout": events::TOOL_HOOK_TIMEOUT_SECONDS,
                        }],
                        "PostToolUse": [{
                            "hookCallbackIds": [callback_id],
                            "timeout": events::TOOL_HOOK_TIMEOUT_SECONDS,
                        }]
                    }
                }),
                Instant::now() + RPC_TIMEOUT,
            )
            .await
        {
            Ok(value) => value,
            // A lost initialize reply leaves the CLI state uncertain: never
            // recode unknown as rejected.
            Err(error) => {
                return Err(self
                    .reject_start(session, setter_error(error, "initialize"))
                    .await);
            }
        };
        let readback: InitializeReadback = match serde_json::from_value(readback) {
            Ok(readback) => readback,
            Err(_) => {
                return Err(self
                    .reject_start(
                        session,
                        Error::new(
                            "profile_error",
                            "Claude initialize readback did not match its envelope",
                        ),
                    )
                    .await);
            }
        };
        let catalogue_omits_model = plan
            .requested_model
            .as_deref()
            .is_some_and(|model| claude_model_selection(&readback, model).catalogue_omits_model());
        if readback.session_state.as_deref() != Some("idle") {
            return Err(self
                .reject_start(
                    session,
                    Error::new(
                        "profile_error",
                        "Claude initialize did not report an idle session",
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
                plan.requested_model.as_deref(),
                plan.requested_permission_mode.as_deref(),
                effective_mode.clone(),
            )
            .await
            {
                Ok(mode) => effective_mode = mode,
                Err(error) => {
                    return Err(self.reject_start(session, error).await);
                }
            }
        }
        if readback.hooks_applied != Some(true) {
            return Err(self
                .reject_start(
                    session,
                    Error::new(
                        "profile_error",
                        "Claude initialize did not apply the registered tool callbacks",
                    ),
                )
                .await);
        }
        // The CLI does not report the serving model at initialize. Only an
        // actual native assistant record supplies the effective model.
        // Requested vs effective permission mode is reported, never forced:
        // launching with `manual` reads back `default` on the installed CLI.
        // initialize itself exposes no MCP inventory record, so the
        // effective server set stays unknown (null, unverified) at readiness
        // for both policies instead of echoing the requested set. The first
        // post-input `system`/`init` record carries `mcp_servers` (name plus
        // status per server, verified on installed 2.1.321); the event task
        // retains that observed set and attaches it to the turn terminal, so
        // the reducer merges it into the effective set with verified true.
        // An absent or malformed init inventory stays unknown.
        let effective =
            start_effective_settings(&readback, effective_mode, plan.requested_mcp_policy);
        Ok((
            session,
            session::with_model_catalogue_diagnostic(effective, catalogue_omits_model),
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
            // A definite pre-transaction rejection (for example the daemon's
            // `secret_in_payload` refusal) proves the batch was never
            // committed, so the launch settles as definite `failed`, never
            // `Unknown`. Clean up the spawned CLI first; unconfirmed cleanup
            // stays unknown.
            if eliot_plugin::is_definite_batch_rejection(&error.code) {
                if Self::rollback_uncommitted_session(&mut session)
                    .await
                    .is_some()
                {
                    session.phase = SessionPhase::Unknown;
                    self.session = Some(session);
                    return Err(Error::new(
                        "unknown",
                        format!(
                            "Claude session.ready was definitely rejected ({}) but owned CLI cleanup is unconfirmed",
                            error.code
                        ),
                    ));
                }
                let reason = format!(
                    "Claude session.ready was definitely rejected ({}); the batch was never committed",
                    error.code
                );
                let error = error
                    .with_message(reason)
                    .with_where("plugin.claude.start.session_ready")
                    .with_origin(
                        eliot_core::FailureOrigin::Swarm,
                        "daemon rejected the session.ready batch before its transaction",
                    );
                return Ok(self.retain_proven_no_effect_start(params, error));
            }
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
                "effort": plan.requested_effort.clone(),
                "cwd": plan.workspace.to_string_lossy(),
                "mcp_policy": plan.requested_mcp_policy.as_str(),
                "mcp_allow": plan.requested_mcp_allow.clone(),
            },
            "effective": effective,
        });
        self.session = Some(session);
        Ok(result)
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        self.guard_start()?;
        let params: AgentStartParams = decode(raw, "Claude agent.start params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Claude",
        )?;
        // Every failure before the native CLI is spawned proves no native
        // effect exists, so it settles as a definite failed start (never
        // unknown) with a typed reason. Failures at or after spawn keep
        // their honest classification below.
        let plan = match self.plan_start(&params) {
            Ok(plan) => plan,
            Err(error) => {
                return Ok(self.proven_no_effect_start(
                    &params,
                    PhaseFailure {
                        phase: StartPhase::Plan,
                        error: Box::new(error),
                    },
                ));
            }
        };
        let session = match launch_session(&plan, &params, peer).await {
            Ok(session) => session,
            Err(failure) => {
                if start_failure_is_proven_no_effect(failure.phase, &failure.error.code) {
                    return Ok(self.proven_no_effect_start(&params, failure));
                }
                return Err(*failure.error);
            }
        };
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
            return Err(self.fail_unregistered_start(session, error).await);
        }

        let (session, effective) = self.read_effective_profile(session, &plan).await?;
        self.commit_readiness(session, &params, &plan, effective)
            .await
    }

    /// The daemon sends agent.start exactly once per freshly spawned plugin
    /// process: a bound or settled plugin never starts again.
    fn guard_start(&self) -> Result<()> {
        if self.session.is_some() || self.failed_start.is_some() {
            return Err(Error::new(
                "bad_state",
                "Claude plugin already received agent.start",
            ));
        }
        Ok(())
    }

    async fn command(&mut self, request: &RpcRequest) -> Result<Value> {
        let params: AgentCommandParams =
            decode(&request.params, "Claude agent.command params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Claude",
        )?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            "configure" => self.configure_command(params).await,
            "steer" => Err(Error::new(
                "native_not_dispatched",
                "Claude steer is delivered through turn_safe_point tool hooks",
            )),
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
        let context = params.context.clone();
        let text = match send_text(&params) {
            Ok(text) => text.to_owned(),
            Err(error) if error.code == "frame_too_large" => {
                return Ok(rejected_control(&error.message));
            }
            Err(error) => return Err(error),
        };
        // These are pre-write refusals. Generic `busy`/`unknown` errors after
        // daemon dispatch would incorrectly turn them into uncertain effects.
        session.pending_turn.reconcile();
        if let Err(error) = check_send_ready(session) {
            return Ok(rejected_control(&error.message));
        }
        let native_content = match with_claimed_context(&text, &context, "Claude") {
            Ok(native_content) => native_content,
            Err(error) if error.code == "frame_too_large" => {
                return Ok(rejected_control(&error.message));
            }
            Err(error) => return Err(error),
        };
        dispatch_send(session, &params.command_id, native_content, context).await
    }

    /// Change the selector only after confirming the owned CLI is idle.
    /// An applied reply proves selector acceptance and unchanged permission
    /// mode. The next bound assistant record identifies the serving model.
    async fn configure_command(&mut self, params: AgentCommandParams) -> Result<Value> {
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
                "Claude session is not ready for input",
            ));
        }
        if session.session_id.is_none() {
            return Err(Error::new(
                "unknown",
                "Claude session identity is unresolved",
            ));
        }
        session.pending_turn.reconcile();
        if let Some(reason) = configure_input_refusal(
            &session.events.status(),
            session.pending_turn.pending.is_some(),
        ) {
            return Ok(json!({"state": "rejected", "reason": reason}));
        }
        let model = match Self::parse_configure_model(&params.args) {
            Ok(model) => model,
            Err(error) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": error.message,
                }));
            }
        };
        let before = match read_configure_state(&session.native_client).await {
            Ok(before) => before,
            Err(_) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Claude could not confirm native idle state and permission mode",
                }));
            }
        };
        let selection = claude_model_selection(&before, &model);
        if let Err(error) = check_configure_preflight(&before, &selection) {
            return Ok(json!({"state": "rejected", "reason": error.message}));
        }
        if let Some(reason) = configure_input_refusal(
            &session.events.status(),
            session.pending_turn.pending.is_some(),
        ) {
            return Ok(json!({"state": "rejected", "reason": reason}));
        }
        match session
            .native_client
            .control(
                "set_model",
                json!({"model": model}),
                Instant::now() + RPC_TIMEOUT,
            )
            .await
        {
            Ok(_) => {
                let after = read_configure_state(&session.native_client).await.map_err(|error| {
                    contextual_error(
                        error,
                        "unknown",
                        "Claude accepted the model selector but permission mode could not be verified",
                    )
                })?;
                verify_configure_permission_mode(&before, &after)?;
                Ok(session::with_model_catalogue_diagnostic(
                    json!({
                        "state": "applied",
                        "accepted_model": model,
                        "model_verified": false,
                    }),
                    selection.catalogue_omits_model(),
                ))
            }
            Err(error) if error.code == "native_rejected" => {
                Ok(session::with_model_catalogue_diagnostic(
                    json!({
                        "state": "rejected",
                        "reason": error.message,
                        "origin": eliot_core::FailureOrigin::Harness,
                        "origin_evidence": "Claude set_model returned a native error",
                        "where": "plugin.claude.configure.set_model",
                    }),
                    selection.catalogue_omits_model(),
                ))
            }
            Err(error) if native_no_write(&error.code) => Ok(json!({
                "state": "rejected",
                "reason": "Claude set_model was not dispatched",
            })),
            Err(error) => Err(contextual_error(
                error,
                "unknown",
                "Claude set_model may have changed native state; it will not be repeated",
            )),
        }
    }

    /// The requested model of a `configure` dispatch. Admission already
    /// gated the fields; this re-checks shape so a malformed dispatch
    /// refuses before any native write. Claude has no live effort switch:
    /// admission refuses effort, so only a model ever reaches this path.
    fn parse_configure_model(args: &Value) -> Result<String> {
        match args.get("model").and_then(Value::as_str) {
            Some(model) if !model.trim().is_empty() => Ok(model.to_owned()),
            _ => Err(Error::new("invalid", "Claude configure needs a model")),
        }
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
        let status = session.events.status();
        if session.phase == SessionPhase::Stopped
            || !status.ready
            || status.stopped
            || session.native.transport.process_exit_observed()
            || session.session_id.is_none()
        {
            return Err(Error::new(
                "native_not_dispatched",
                "Claude has no established live session for interrupt",
            ));
        }
        if !params.owner_control && session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Claude session or event source outcome is unresolved",
            ));
        }
        if params.request_id.is_some() {
            return Err(Error::new(
                "invalid",
                "Claude interrupt does not address a native request",
            ));
        }
        let command_id = params.command_id.clone();
        let Some(turn_id) = params.turn_id.as_deref() else {
            session.pending_turn.reconcile();
            return eliot_plugin::session::idle_interrupt_result(
                &command_id,
                params.owner_control
                    && session.phase == SessionPhase::Ready
                    && status.active_command_id.is_none()
                    && status.active_turn_id.is_none()
                    && session.pending_turn.pending.is_none()
                    && !status.source_unknown
                    && !status.batch_unknown
                    && status.failure.is_none(),
            );
        };
        if status
            .active_turn_id
            .as_deref()
            .is_some_and(|active| active != turn_id)
        {
            return Err(Error::new(
                "native_not_dispatched",
                "Claude is running a different turn",
            ));
        }
        // The Owner can reach an established live session even when its turn
        // observation is uncertain. An ordinary interrupt requires the live gate.
        if !params.owner_control {
            if let Some(reason) = status.failure {
                return Err(Error::new("unknown", reason));
            }
            // The event task answers behind any queued terminal frame.
            let Some(already_interrupted) = session.events.turn_interrupt_status(turn_id).await?
            else {
                return Err(Error::new(
                    "stale_turn",
                    "Claude turn is not the active turn",
                ));
            };
            if already_interrupted {
                return Ok(json!({
                    "state": "applied", "command_id": command_id, "turn_id": turn_id,
                }));
            }
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
                    "reason": format!("Claude interrupt was not dispatched: {}", error.message),
                }));
            }
            Err(_) => {
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
                    "unknown",
                    "Claude interrupt may have applied; it will not be repeated",
                ));
            }
        };
        session.events.mark_interrupted(turn_id).await;
        let mut applied = json!({
            "state": "applied",
            "command_id": command_id,
            "turn_id": params.turn_id,
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
        let (turn_id, request_id) = request_reply_ids(&params, "Claude")?;
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
        Ok(result)
    }

    async fn snapshot(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentSnapshotParams = decode(raw, "Claude agent.snapshot params are invalid")?;
        let session = retained_session(
            &mut self.session,
            &params.agent_id,
            params.generation,
            &params.command_id,
        )?;
        // PR1 has no live native session poll: the snapshot is the committed
        // stream position (session/turn bindings plus the latest terminal
        // facts), never a fresh native query.
        if let Some(stopped) = stopped_stop_snapshot(session, &params) {
            return Ok(stopped);
        }
        let event_status = session.events.status();
        session.pending_turn.reconcile();
        live_session_snapshot(session, &params, &event_status)
    }

    async fn stop(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentStopParams = decode(raw, "Claude agent.stop params are invalid")?;
        let session = retained_session(
            &mut self.session,
            &params.agent_id,
            params.generation,
            &params.command_id,
        )?;
        // Latch once before native I/O. An uncertain cancel is never replayed.
        if begin_stop(
            &mut session.stop_command_id,
            session.phase == SessionPhase::Stopped,
            &params.command_id,
            "Claude",
        )? {
            return Ok(json!({"state": "applied"}));
        }
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("unknown", "stop lost its retained native session"))?;
        // The router owns interrupt/wait and daemon force custody. This
        // adapter only performs the graceful native close and verifies the
        // event/source drain; force custody remains in the daemon.
        terminate_session(session, false, true).await?;
        Ok(json!({"state": "applied"}))
    }

    async fn shutdown(&mut self) -> Result<Value> {
        self.shutdown_paths(false).await
    }

    async fn shutdown_owned(&mut self) {
        let _ = self.shutdown_paths(true).await;
    }

    async fn shutdown_paths(&mut self, best_effort: bool) -> Result<Value> {
        session::shutdown_session(
            self.session.as_mut(),
            best_effort,
            async |session| {
                if session.phase == SessionPhase::Stopped {
                    Ok(())
                } else {
                    terminate_session(session, true, false).await
                }
            },
            async |session| session.events.close().await,
        )
        .await
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
) -> Result<Value> {
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
    let last_turn = match snapshot_turn_facts(
        params,
        event_status,
        &session.start_command_id,
        session.stop_command_id.as_deref(),
    ) {
        Ok(last_turn) => last_turn,
        Err(reason) => return unknown_snapshot(params, reason),
    };
    let result_known = last_turn
        .as_ref()
        .is_some_and(|turn| turn.final_text.is_some());
    let process_exited = session.native.transport.process_exit_observed();
    let mut active_turn = event_status.active_turn_id.clone();
    let mut last_turn = serde_json::to_value(last_turn)
        .map_err(|_| Error::new("internal", "Claude turn snapshot could not be encoded"))?;
    let mut state = if event_status.batch_unknown || event_status.source_unknown {
        "unknown"
    } else if event_status.active_turn_id.is_some() {
        "busy"
    } else if event_status.ready {
        "idle"
    } else {
        "unknown"
    };
    let turn_command =
        (params.command_id != session.start_command_id).then_some(params.command_id.as_str());
    let evidence = params.turn_id.as_deref().and_then(|turn_id| {
        exact_turn_evidence(turn_id, turn_command, event_status, process_exited)
    });
    match evidence {
        Some(TurnEvidence::Running) => {
            last_turn = in_progress_turn_snapshot(params, turn_command, "running");
        }
        Some(TurnEvidence::Terminal) => {
            // The exact `result` record ended the turn; the CLI then waits
            // for input, or its process has since ended.
            state = if process_exited { "terminal" } else { "idle" };
        }
        Some(TurnEvidence::Exited) => {
            state = "lost";
            active_turn = None;
            last_turn = in_progress_turn_snapshot(params, turn_command, "lost");
        }
        None => {}
    }
    Ok(json!({
        "agent_id": params.agent_id.clone(),
        "generation": params.generation,
        "command_id": params.command_id.clone(),
        "state": state,
        "native_scope": session.native_scope.clone(),
        "native_ref": session_id,
        "active_turn": active_turn,
        "last_turn": last_turn,
        "batch_unknown": event_status.batch_unknown,
        "source_unknown": event_status.source_unknown,
        "process_exited": process_exited,
        "result_known": result_known,
        "turn_evidence": evidence.map(TurnEvidence::label),
    }))
}

fn in_progress_turn_snapshot(
    params: &AgentSnapshotParams,
    command_id: Option<&str>,
    status: &str,
) -> Value {
    let mut fact = json!({"turn_id": params.turn_id, "status": status, "terminal": false});
    if let Some(command_id) = command_id {
        fact["command_id"] = json!(command_id);
    }
    fact
}

/// What the owned stream-json reader and process waiter proved about the
/// exact requested turn. The CLI has no turn-status query: the reader's own
/// records are the native read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TurnEvidence {
    /// Bound turn, no `result` yet, healthy source and a live process.
    Running,
    /// The exact turn's `result` record mapped to a universal status.
    Terminal,
    /// The process exited and the drained stream held no `result`.
    Exited,
}

impl TurnEvidence {
    fn label(self) -> &'static str {
        match self {
            Self::Running => "stream-json active turn without result",
            Self::Terminal => "stream-json result",
            Self::Exited => "process exit with drained stream-json and no result",
        }
    }
}

fn exact_turn_evidence(
    turn_id: &str,
    command_id: Option<&str>,
    status: &EventStatus,
    process_exited: bool,
) -> Option<TurnEvidence> {
    if let Some(last) = status
        .last_turn
        .as_ref()
        .filter(|last| last.turn_id == turn_id)
    {
        return (last.command_id.as_deref() == command_id
            && last.terminal
            && last.status.is_some()
            && status.active_turn_id.is_none()
            && !status.background_continuation)
            .then_some(TurnEvidence::Terminal);
    }
    if status.active_turn_id.as_deref() != Some(turn_id)
        || status.active_command_id.as_deref() != command_id
        || status.stopped
    {
        return None;
    }
    if process_exited {
        return status.native_drained.then_some(TurnEvidence::Exited);
    }
    (status.ready && !status.source_unknown && !status.batch_unknown && status.failure.is_none())
        .then_some(TurnEvidence::Running)
}

fn snapshot_turn_facts<'a>(
    params: &AgentSnapshotParams,
    status: &'a EventStatus,
    start_command_id: &str,
    stop_command_id: Option<&str>,
) -> std::result::Result<Option<&'a events::TurnFacts>, &'static str> {
    let Some(turn_id) = params.turn_id.as_deref() else {
        return if params.command_id == start_command_id
            || stop_command_id == Some(params.command_id.as_str())
        {
            Ok(status
                .last_turn
                .as_ref()
                .filter(|last| last.command_id.is_some()))
        } else {
            Err("Claude command has no exact native turn association")
        };
    };
    // The start command identifies the session for an exact commandless
    // readback. It cannot identify a send or claim a turn as its own.
    let command_id = (params.command_id != start_command_id).then_some(params.command_id.as_str());
    let retained = status
        .last_turn
        .as_ref()
        .filter(|last| last.turn_id == turn_id && last.command_id.as_deref() == command_id);
    let active = status.active_turn_id.as_deref() == Some(turn_id)
        && status.active_command_id.as_deref() == command_id;
    if !active && retained.is_none() {
        return Err("Claude readback did not prove the exact requested turn");
    }
    Ok(retained)
}

async fn terminate_session(session: &mut AgentSession, force: bool, graceful: bool) -> Result<()> {
    let AgentSession {
        phase,
        mcp_config,
        native,
        events,
        ..
    } = session;
    let result = eliot_plugin::session::stop_owned_session(
        "Claude",
        None,
        events.begin_stop(),
        async {
            let process_exit = if graceful {
                native.close_gracefully(Instant::now() + RPC_TIMEOUT).await
            } else {
                native.terminate(force, Instant::now() + RPC_TIMEOUT).await
            };
            let drained = native
                .await_output_drained(Instant::now() + RPC_TIMEOUT)
                .await;
            (process_exit, drained)
        },
        events.end_stop(),
    )
    .await;
    if let Err(error) = result {
        *phase = SessionPhase::Unknown;
        return Err(error);
    }
    *phase = SessionPhase::Stopped;
    drop(mcp_config.take());
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

/// Pre-dispatch gates for a fresh send: a failed event source and an already
/// active or unassociated input. Every refusal here proves no native write.
fn check_send_ready(session: &AgentSession) -> Result<()> {
    if session.phase != SessionPhase::Ready {
        return Err(Error::new(
            "bad_state",
            "Claude session is not ready for input",
        ));
    }
    if session.session_id.is_none() {
        return Err(Error::new(
            "bad_state",
            "Claude session identity is unresolved",
        ));
    }
    let event_status = session.events.status();
    if let Some(reason) = event_status.failure {
        return Err(Error::new("unknown", reason));
    }
    // A self-started native turn with no send takes this input: Claude
    // merges it and the event task binds it to that exact turn.
    if event_status.active_turn_id.is_some() && !event_status.native_unbound {
        return Err(Error::new("busy", "Claude already has an active turn"));
    }
    if session.pending_turn.pending.is_some() {
        return Err(Error::new(
            "busy",
            "Claude prior input has no exact native turn association",
        ));
    }
    Ok(())
}

/// Mint the exact turn association before the write and register it with
/// the event task first: an early lifecycle can never miss its waiter, and
/// a lost association retains its exact waiter without a rewrite or a
/// session-wide latch. Only later native evidence may retire that waiter.
async fn dispatch_send(
    session: &mut AgentSession,
    command_id: &str,
    content: String,
    context: Vec<ContextItem>,
) -> Result<Value> {
    let deadline = Instant::now() + SEND_TIMEOUT;
    let turn_id = new_uuid();
    let outcome = match session
        .events
        .expect_turn(command_id.to_owned(), turn_id.clone(), context)
        .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            // No user record was written, even if registration itself lost
            // its reply. Withdraw only this never-written registration.
            session.events.cancel_turn(command_id.to_owned()).await;
            let result = rejected_control(&error.message);
            return Ok(result);
        }
    };
    session.pending_turn.pending = Some(TurnWaiter { outcome });
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
            content,
        },
    };
    if let Err(error) = session.native_client.send_user(input, deadline).await {
        if native_no_write(&error.code) {
            // Proven never dispatched: withdraw the pending turn before
            // answering, or every later send wedges `busy` behind it.
            session.events.cancel_turn(command_id.to_owned()).await;
            session.pending_turn.pending = None;
            let result = rejected_control(&error.message);
            return Ok(result);
        }
        let result = send_unknown("claude_input_write_unknown", &error.message);
        session.pending_turn.reconcile();
        return Ok(result);
    }
    let pending = session
        .pending_turn
        .pending
        .as_mut()
        .expect("send registered its waiter");
    let result = match tokio::time::timeout_at(deadline, &mut pending.outcome).await {
        Ok(Ok(outcome)) => return Ok(session.pending_turn.finish(outcome)),
        Ok(Err(_)) => {
            session.pending_turn.pending = None;
            send_unknown(
                "claude_turn_association_lost",
                "Claude event task closed without settling the input; it will not be repeated",
            )
        }
        Err(_) => send_unknown(
            "claude_turn_association_timeout",
            "Claude turn association was not observed before the deadline; the input will not be repeated",
        ),
    };
    Ok(result)
}

/// Spawn the owned CLI and bind the event task: the only start phase that
/// creates native resources. Every failure before the spawn (MCP config
/// write, skill-root check, auth status, and the spawn itself) returns a
/// `PhaseFailure` the caller converts into the proven-no-effect failed
/// start; a failure after the child exists keeps its honest error and the
/// child is ended before returning.
async fn launch_session(
    plan: &StartPlan,
    params: &AgentStartParams,
    peer: &PeerClient,
) -> std::result::Result<AgentSession, PhaseFailure> {
    // Read the selected CLI's authentication before starting a native session.
    crate::native::require_auth(&plan.program, &plan.workspace, &plan.environment)
        .await
        .map_err(|error| PhaseFailure {
            phase: StartPhase::Auth,
            error: Box::new(error),
        })?;
    let mcp_config = ClaudeMcpConfigFile::create(
        &plan.instance_dir,
        &plan.swarm_binary,
        &plan.data_root,
        &plan.mcp_environment,
    )
    .map_err(|error| PhaseFailure {
        phase: StartPhase::McpConfig,
        error: Box::new(error),
    })?;
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
    // Omit `--model` when the profile left it absent so the CLI default
    // applies; the effective model is read back from initialize/stream.
    if let Some(model) = plan.requested_model.as_deref() {
        argv.push(OsString::from("--model"));
        argv.push(OsString::from(model));
    }
    if let Some(mode) = plan.requested_permission_mode.as_deref() {
        argv.push(OsString::from("--permission-mode"));
        argv.push(OsString::from(mode));
    }
    if let Some(effort) = plan.requested_effort.as_deref() {
        argv.push(OsString::from("--effort"));
        argv.push(OsString::from(effort));
    }
    if let Some(rules) = &params.owner_rules {
        let path = plan.instance_dir.join("owner-rules.md");
        std::fs::write(&path, rules).map_err(|_| PhaseFailure {
            phase: StartPhase::OwnerRules,
            error: Box::new(Error::new(
                "platform_io",
                "Claude could not write the private owner rules file",
            )),
        })?;
        argv.push(OsString::from("--append-system-prompt-file"));
        argv.push(path.into_os_string());
    }
    argv.extend(native_subagents_argv(plan.native_subagents_deny));
    argv.extend(mcp_argv(
        plan.requested_mcp_policy,
        mcp_config.path().as_os_str(),
    ));
    if let Some(hostname) = &plan.inherit_cloud_hostname {
        // Native policy also covers remote servers fetched after preflight.
        // Per-scope deny entries merge, including with managed settings.
        argv.push(OsString::from("--settings"));
        argv.push(OsString::from(
            json!({
                "deniedMcpServers": [{"serverUrl": format!("*://{hostname}:*/*")}],
            })
            .to_string(),
        ));
    }
    // Expose the daemon-materialized coordination skill through the CLI's
    // own plugin mechanism: the materialized skill root doubles as the
    // plugin directory, so no checkout or global skill location changes.
    // A missing root refuses the start pre-spawn with its typed reason
    // instead of failing the spawn unknown.
    let skill_root = skill::skill_root(&plan.data_root);
    if !skill_root.is_dir() {
        return Err(PhaseFailure {
            phase: StartPhase::Skill,
            error: Box::new(Error::new(
                "unavailable",
                "Claude skill root is unavailable",
            )),
        });
    }
    argv.push(OsString::from("--plugin-dir"));
    argv.push(OsString::from(skill_root));
    let launch = NativeLaunch {
        program: plan.program.clone(),
        args: argv,
        cwd: plan.workspace.clone(),
        environment: plan.environment.clone(),
    };
    let mut native = ClaudeProcess::spawn(launch, SecretPolicy::new(plan.secrets.iter().cloned()))
        .await
        .map_err(|error| PhaseFailure {
            phase: StartPhase::Spawn,
            error: Box::new(error),
        })?;
    let frames = match native.take_frames() {
        Ok(frames) => frames,
        Err(error) => {
            let _ = native.terminate(false, Instant::now() + RPC_TIMEOUT).await;
            return Err(PhaseFailure {
                phase: StartPhase::PostSpawn,
                error: Box::new(error),
            });
        }
    };
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
            native: native_client.clone(),
            workspace: plan.workspace.clone(),
        },
    );
    Ok(AgentSession {
        agent_id: params.agent_id.clone(),
        generation: params.generation,
        start_command_id: params.command_id.clone(),
        phase: SessionPhase::Unknown,
        session_id: None,
        native_scope: claude_scope(plan.config_dir.as_deref()),
        native,
        mcp_config: Some(mcp_config),
        native_client,
        events,
        pending_turn: PendingTurn::default(),
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
    requested_model: Option<&str>,
    requested_mode: Option<&str>,
    current_mode: Option<String>,
) -> Result<Option<String>> {
    // An omitted request keeps the retained server-selected model: no
    // `set_model` realign runs, and the effective mode readback below stands.
    if let Some(requested) = requested_model {
        client
            .control(
                "set_model",
                json!({"model": requested}),
                Instant::now() + RPC_TIMEOUT,
            )
            .await
            .map_err(|error| setter_error(error, "set_model"))?;
    }
    let mut effective_mode = current_mode;
    if let Some(profile_mode) = requested_mode {
        let requested = native_resume_permission_mode(profile_mode);
        if effective_mode.as_deref() != Some(requested) {
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
                    "Claude set_permission_mode echoed a different native mode",
                ));
            }
            effective_mode = Some(requested.to_owned());
        }
    }
    Ok(effective_mode)
}

fn configure_input_refusal(
    status: &crate::events::EventStatus,
    pending_send: bool,
) -> Option<&str> {
    if let Some(reason) = status.failure.as_deref() {
        return Some(reason);
    }
    if status.active_turn_id.is_some() || status.background_continuation {
        return Some(
            "Claude already has an active turn or background continuation; configure between turns",
        );
    }
    pending_send.then_some(
        "Claude prior input has no exact native turn association; configure between turns",
    )
}

async fn read_configure_state(client: &ClaudeClient) -> Result<InitializeReadback> {
    // Repeated initialize without settings preserves the installed hooks and
    // reports the current native session state and permission mode.
    let result = client
        .control("initialize", json!({}), Instant::now() + RPC_TIMEOUT)
        .await?;
    serde_json::from_value(result).map_err(|_| {
        Error::new(
            "native_readback",
            "Claude configure readback did not match its envelope",
        )
    })
}

fn claude_model_selection<'a>(
    readback: &'a InitializeReadback,
    id: &'a str,
) -> session::ModelSelection<'a, crate::native::types::InitializeModel> {
    session::ModelSelection::new(
        id,
        readback
            .models
            .iter()
            .filter(|entry| {
                entry.value.as_deref() == Some(id) || entry.resolved_model.as_deref() == Some(id)
            })
            .max_by_key(|entry| entry.supports_auto_mode == Some(true)),
    )
}

fn check_configure_preflight(
    readback: &InitializeReadback,
    selection: &session::ModelSelection<'_, crate::native::types::InitializeModel>,
) -> Result<()> {
    if readback.session_state.as_deref() != Some("idle") {
        return Err(Error::new(
            "busy",
            "Claude native session must be idle to configure the model",
        ));
    }
    match readback.current_permission_mode.as_deref() {
        Some("auto")
            if selection
                .catalogue_entry
                .is_some_and(|entry| entry.supports_auto_mode != Some(true)) =>
        {
            Err(Error::new(
                "unsupported",
                "Claude did not confirm this model supports the current auto permission mode",
            ))
        }
        Some(mode) if !mode.trim().is_empty() => Ok(()),
        _ => Err(Error::new(
            "native_readback",
            "Claude did not report its current permission mode",
        )),
    }
}

fn verify_configure_permission_mode(
    before: &InitializeReadback,
    after: &InitializeReadback,
) -> Result<()> {
    if after.current_permission_mode.is_none()
        || after.current_permission_mode != before.current_permission_mode
    {
        return Err(Error::new(
            "unknown",
            "Claude accepted the model selector but its permission mode was not preserved and verified",
        ));
    }
    Ok(())
}

fn native_resume_permission_mode(profile_mode: &str) -> &str {
    match profile_mode {
        "manual" => "default",
        mode => mode,
    }
}

fn setter_error(error: Error, subtype: &str) -> Error {
    // Definite local non-admission: the control write provably never
    // reached the CLI, so the start is rejected, never a profile verdict.
    if subtype == "set_model" && error.code == "native_rejected" {
        error
            .with_where("plugin.claude.start.set_model")
            .with_origin(
                eliot_core::FailureOrigin::Harness,
                "Claude set_model returned a native error",
            )
    } else if native_no_write(&error.code) {
        contextual_error(
            error,
            "rejected",
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

/// Claude has no selective-inclusion mechanism: minimal is the Eliot-only
/// per-run config plus `--strict-mcp-config`, so a non-empty allow list
/// would be silently dropped. Reject it as unsupported at config load, so a
/// stale snapshot fails closed instead of losing explicitly allowed
/// integrations.
fn check_mcp_allow_empty(allow: &[String]) -> Result<()> {
    if allow.is_empty() {
        Ok(())
    } else {
        Err(Error::new(
            "unsupported",
            "Claude mcp_allow has no selective-inclusion mapping",
        ))
    }
}

/// MCP argv pair for the frozen policy. Minimal adds `--strict-mcp-config`
/// ahead of the per-run config so only it applies (installed CLI 2.1.325:
/// the flag ignores all other MCP configurations); inherit passes only the
/// additive per-run config and keeps the normal precedence, with owner
/// Swarm entries refused pre-spawn by the `owner_scan` gate (no per-server
/// disable or deny switch exists on 2.1.325: `--strict-mcp-config` is
/// all-or-nothing, `--setting-sources` selects whole sources, and `mcp`
/// offers no deny list).
fn mcp_argv(policy: McpPolicy, config_path: &std::ffi::OsStr) -> Vec<OsString> {
    let mut argv = Vec::with_capacity(3);
    if policy == McpPolicy::Minimal {
        argv.push(OsString::from("--strict-mcp-config"));
    }
    argv.push(OsString::from("--mcp-config"));
    argv.push(config_path.to_owned());
    argv
}

/// Per-session native-subagent mapping for the installed Claude Code CLI
/// (installed 2.1.278 exposes `--disallowedTools`; the `Task` tool launches
/// subagents, per `claude --help` and the official subagent docs): `deny`
/// passes `--disallowedTools Task`, `allow` adds nothing and preserves
/// today's behavior.
fn native_subagents_argv(deny: bool) -> Vec<OsString> {
    if !deny {
        return Vec::new();
    }
    vec![OsString::from("--disallowedTools"), OsString::from("Task")]
}

fn claude_manifest() -> PluginManifest {
    PluginManifest::per_agent(
        "claude",
        env!("CARGO_PKG_VERSION"),
        &[
            "launch",
            "resume",
            "send",
            "configure",
            "steer",
            "interrupt",
            "approve",
            "deny",
            "stop",
        ],
        eliot_plugin::protocol::ManifestFeatures {
            start_requires_input: false,
            native_children: false,
            discover: false,
            observe: false,
        },
        BTreeMap::from([(
            "controlled".to_owned(),
            stream_sources(&["message", "request", "session", "tool", "turn"]),
        )]),
        "none",
    )
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

/// Native scope names the CLI config identity: the hashed canonical
/// `CLAUDE_CONFIG_DIR` when the target environment sets one, else the CLI
/// default resolution, which the plugin must not guess at. The raw directory
/// is also a pass_env secret value on the daemon, and committed scope
/// metadata must never collide with it: the daemon rejects secret-bearing
/// immutable metadata, which used to wedge the launch unknown on its very
/// first batch. Same FNV-1a shape as the Codex home scope.
fn claude_scope(config_dir: Option<&str>) -> String {
    match config_dir.filter(|dir| !dir.trim().is_empty()) {
        Some(dir) => {
            let canonical = std::fs::canonicalize(dir).unwrap_or_else(|_| PathBuf::from(dir));
            let mut canonical = canonical.to_string_lossy().into_owned();
            if cfg!(windows) {
                canonical = canonical.to_lowercase();
            }
            let mut hash = 0xcbf29ce484222325u64;
            for byte in canonical.bytes() {
                hash ^= u64::from(byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
            format!("claude-config:{hash:016x}")
        }
        None => "claude-config:default".to_owned(),
    }
}

fn check_effort_override(
    requested: Option<&str>,
    environment: &[(OsString, OsString)],
) -> Result<()> {
    if let Some(requested) = requested
        && let Some((_, native)) = environment.iter().find(|(name, _)| {
            name == "CLAUDE_CODE_EFFORT_LEVEL"
                || (cfg!(windows) && name.eq_ignore_ascii_case("CLAUDE_CODE_EFFORT_LEVEL"))
        })
        && !native.is_empty()
        && native != requested
    {
        return Err(Error::new(
            "profile_error",
            "Claude requested effort conflicts with CLAUDE_CODE_EFFORT_LEVEL",
        ));
    }
    Ok(())
}

fn start_effective_settings(
    readback: &InitializeReadback,
    permission_mode: Option<String>,
    mcp_policy: McpPolicy,
) -> Value {
    // Initialize has no effective effort field. The environment can override
    // --effort, and a model or maxEffortLevel setting can cap the result.
    with_safe_point_steer(with_context_points(
        json!({
            "model": null,
            "effort": null,
            "effort_verified": false,
            "permission_mode": permission_mode,
            "session_state": readback.session_state,
            "pid": readback.pid,
            "mcp_policy": mcp_policy.as_str(),
            "mcp_servers": null,
            "mcp_servers_verified": false,
        }),
        &[InjectPoint::NextInput, InjectPoint::TurnSafePoint],
    ))
}

/// The shared start/input/stop commands plus the Claude-only interrupt,
/// request-reply, and steer commands.
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
