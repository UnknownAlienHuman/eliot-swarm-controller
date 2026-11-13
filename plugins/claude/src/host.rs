use crate::{
    events::{self, EventConfig, EventHandle, EventStatus, SteerOutcome, TurnOutcome},
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
    protocol::MAX_CREDENTIAL_BYTES,
    session::{
        self, contextual_error, decode, native_no_write, not_initialized, optional_profile_text,
        parse_resume_subject, register_native_process, rejected_control, unknown_control,
        unknown_snapshot, validate_binding, validate_identity, validate_source_plan,
        with_claimed_context, with_context_points,
    },
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    ffi::OsString,
    path::PathBuf,
};
use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The actual steer mechanism, reported on every applied steer ack: the
/// installed CLI has no named steer method, so the exact captured turn is
/// steered through its existing PreToolUse `additionalContext` hook response.
/// This is steer, never `active_turn` inject: that point stays unsupported.
const STEER_MECHANISM: &str = "claude.PreToolUse.additionalContext";

#[derive(Default)]
struct ClaudePlugin {
    initialized: Option<Initialized>,
    session: Option<AgentSession>,
    failed_start: Option<FailedStart>,
    deferred_steer: Option<DeferredSteer>,
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
    // Keep the private per-session MCP config alive with the owned CLI.
    mcp_config: Option<ClaudeMcpConfigFile>,
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
    steer_outcomes: HashMap<String, SteerRecord>,
    stop_command_id: Option<String>,
}

/// Validated start inputs: the frozen profile, Agent-scoped MCP context, and
/// minted session id. Built before any spawn; every later start phase reads
/// from it.
struct StartPlan {
    workspace: PathBuf,
    instance_dir: PathBuf,
    data_root: PathBuf,
    swarm_binary: PathBuf,
    program: OsString,
    environment: Vec<(OsString, OsString)>,
    secrets: Vec<String>,
    config_dir: Option<String>,
    credential: String,
    session_id: String,
    requested_model: String,
    requested_permission_mode: Option<String>,
    requested_mcp_policy: McpPolicy,
    requested_mcp_allow: Vec<String>,
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

/// Retained steer outcome for exact command-id replay. Applied is retained
/// only after the native hook response confirms the text rode it; a reused
/// id with another turn or text is an integrity error.
struct SteerRecord {
    turn_id: String,
    text: String,
    result: Value,
}

/// A steer whose exact-turn waiter is registered with the event task while
/// its RPC reply stays deferred: the same host loop keeps serving ordinary
/// control/status/shutdown requests and settles this waiter when it resolves
/// (hook confirmation, turn end, stop, stream loss) or its deadline passes.
/// At most one is ever deferred: the event task holds a single queue slot.
struct DeferredSteer {
    request_id: Value,
    command_id: String,
    turn_id: String,
    text: String,
    outcome: oneshot::Receiver<SteerOutcome>,
    deadline: Instant,
    cancel_ack: Option<oneshot::Receiver<bool>>,
}

/// What one dispatch produced: an immediate reply, or a deferred steer reply
/// the run loop settles once its waiter resolves.
enum DispatchOutcome {
    Ready(Result<Value>),
    Deferred(DeferredSteer),
}

/// What woke the run loop while a steer reply was deferred. A dropped
/// waiter sender (`None`) means the event task died without settling.
enum SteerWake {
    Request(Option<RpcRequest>),
    Outcome(Option<SteerOutcome>),
    Deadline,
    CancelAcknowledged(bool),
}

/// Serve one request (or settle one deferred steer), then continue the loop
/// or end it, with whether a shutdown reply completed.
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
    let mut pending_request = None;
    loop {
        let Some(mut waiter) = plugin.deferred_steer.take() else {
            let request = if let Some(request) = pending_request.take() {
                Some(request)
            } else {
                requests.recv().await
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
                Serve::Continue => continue,
                Serve::End(done) => {
                    completed_shutdown = done;
                    break;
                }
            }
        };
        // One steer reply is deferred: race ordinary requests against its
        // exact-turn settlement in this same task, so interrupt, stop,
        // snapshot, and shutdown are never blocked behind the hook wait.
        // The select resolves to a value first so no waiter borrow crosses
        // the settling code below.
        // A snapshot preempted on the previous iteration already holds its
        // preempting host request: serve it next without another recv, so
        // the retained control is never dropped.
        let wake = if let Some(pending) = pending_request.take() {
            SteerWake::Request(Some(pending))
        } else if let Some(cancel_ack) = waiter.cancel_ack.as_mut() {
            tokio::select! {
                request = requests.recv() => SteerWake::Request(request),
                outcome = &mut waiter.outcome => SteerWake::Outcome(outcome.ok()),
                acknowledged = cancel_ack => {
                    SteerWake::CancelAcknowledged(acknowledged.unwrap_or(false))
                },
            }
        } else {
            tokio::select! {
                request = requests.recv() => SteerWake::Request(request),
                outcome = &mut waiter.outcome => SteerWake::Outcome(outcome.ok()),
                _ = tokio::time::sleep_until(waiter.deadline) => SteerWake::Deadline,
            }
        };
        let serve = match wake {
            SteerWake::Request(request) => {
                let Some(request) = request else { break };
                plugin.deferred_steer = Some(waiter);
                let (serve, next) =
                    serve_request_preemptible(&mut plugin, &client, &mut requests, request).await;
                if let Some(next) = next {
                    pending_request = Some(next);
                }
                serve
            }
            SteerWake::Outcome(outcome) => {
                plugin.settle_deferred_steer(&client, waiter, outcome).await
            }
            SteerWake::Deadline => {
                if let Some(session) = plugin.session.as_ref() {
                    waiter.cancel_ack = Some(
                        session
                            .events
                            .request_cancel_steer(waiter.command_id.clone()),
                    );
                    plugin.deferred_steer = Some(waiter);
                    Serve::Continue
                } else {
                    plugin
                        .settle_deferred_deadline(&client, waiter, false)
                        .await
                }
            }
            SteerWake::CancelAcknowledged(acknowledged) => {
                plugin
                    .settle_deferred_deadline(&client, waiter, acknowledged)
                    .await
            }
        };
        match serve {
            Serve::Continue => {}
            Serve::End(done) => {
                completed_shutdown = done;
                break;
            }
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

/// Serve one ordinary request: dispatch it, reply unless it deferred a steer
/// waiter, and report whether the loop must end.
async fn serve_request(
    plugin: &mut ClaudePlugin,
    client: &PeerClient,
    request: RpcRequest,
) -> Serve {
    let is_shutdown = request.method == "shutdown";
    match plugin.dispatch(&request, client).await {
        DispatchOutcome::Ready(result) => {
            if is_shutdown && let Some(mut waiter) = plugin.deferred_steer.take() {
                // Shutdown serializes BeginStop behind any hook write that
                // may have just confirmed Applied: the outcome receiver is
                // now ready. Settle and reply the known deferred result
                // before the shutdown ack so Applied is not discarded with
                // the session; unknown only when truly empty/closed.
                let arrived = waiter.outcome.try_recv().ok();
                let settled = plugin.settle_deferred_steer(client, waiter, arrived).await;
                if matches!(settled, Serve::End(_)) {
                    return Serve::End(false);
                }
            }
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
        DispatchOutcome::Deferred(waiter) => {
            plugin.deferred_steer = Some(waiter);
            Serve::Continue
        }
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
        eliot_plugin::SnapshotDispatch::Ready(DispatchOutcome::Ready(result)) => {
            let response = client
                .respond(request.id, result, Instant::now() + RPC_TIMEOUT)
                .await;
            if response.is_err() {
                (Serve::End(false), None)
            } else {
                (Serve::Continue, None)
            }
        }
        eliot_plugin::SnapshotDispatch::Ready(DispatchOutcome::Deferred(waiter)) => {
            plugin.deferred_steer = Some(waiter);
            (Serve::Continue, None)
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

/// Answer one deferred steer reply; a dead transport ends the loop.
async fn respond_deferred(client: &PeerClient, request_id: Value, result: Result<Value>) -> Serve {
    match client
        .respond(request_id, result, Instant::now() + RPC_TIMEOUT)
        .await
    {
        Ok(()) => Serve::Continue,
        Err(_) => Serve::End(false),
    }
}

impl ClaudePlugin {
    async fn dispatch(&mut self, request: &RpcRequest, client: &PeerClient) -> DispatchOutcome {
        let result = match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "agent.start" => self.start(&request.params, client).await,
            "agent.command" => match self.command(request).await {
                DispatchOutcome::Ready(result) => result,
                deferred => return deferred,
            },
            "agent.snapshot" => self.snapshot(&request.params).await,
            "agent.stop" => self.stop(&request.params).await,
            "shutdown" => self.shutdown().await,
            _ => Err(Error::new(
                "unsupported",
                "Claude plugin method is not implemented",
            )),
        };
        DispatchOutcome::Ready(self.post_process(&request.method, result))
    }

    /// Redact and attach diagnostics to one reply, exactly as the serial
    /// path: immediate replies here, deferred steer replies at settle time.
    fn post_process(&self, method: &str, result: Result<Value>) -> Result<Value> {
        let Some(session) = self.session.as_ref() else {
            return result;
        };
        match result {
            Err(error) => Err(session.native_client.diagnostic_error(error)),
            Ok(mut value) => {
                if method == "agent.snapshot"
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
        // Frozen worker MCP policy: inherit keeps the normal MCP precedence,
        // minimal is enforced with the installed `--strict-mcp-config` flag.
        let requested_mcp_policy = McpPolicy::parse_snapshot(profile)
            .map_err(|error| Error::new("profile_error", format!("Claude {}", error.message)))?;
        let requested_mcp_allow = eliot_core::parse_snapshot_allow_list(profile)
            .map_err(|error| Error::new("profile_error", format!("Claude {}", error.message)))?;
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
        let data_root = ClaudeMcpConfigFile::require_agent_data_root(
            &environment,
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
            program,
            environment,
            secrets,
            config_dir,
            credential: initialized.credential.clone(),
            session_id,
            requested_model,
            requested_permission_mode,
            requested_mcp_policy,
            requested_mcp_allow,
            resume_ref,
        })
    }

    /// Settle a start whose native process the host would not register. No
    /// session or input was sent; if the owned CLI's cleanup is unconfirmed
    /// the session is kept unknown instead of dropped. The event consumer
    /// stays alive through termination+drain (suppressed, so no readiness or
    /// input can commit), then closes: closing first drops the frame queue
    /// and a late trailing record overflows even with a confirmed child exit.
    async fn fail_unregistered_start(
        &mut self,
        mut session: AgentSession,
        params: &AgentStartParams,
        error: Error,
    ) -> Error {
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
        if begin_error.is_some() || exit_error.is_some() || drain_error.is_some() {
            let (stage, cleanup) = if let Some(cleanup) = begin_error {
                ("begin_stop", cleanup)
            } else if let Some(cleanup) = exit_error {
                ("terminate", cleanup)
            } else {
                ("drain", drain_error.expect("drain failure"))
            };
            self.session = Some(session);
            let message = format!(
                "{}; owned CLI cleanup is unconfirmed at {} ({}): {}; no session or input was sent",
                error.message, stage, cleanup.code, cleanup.message
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
    /// instead of dropped (like `fail_unregistered_start`). The event
    /// consumer stays alive through termination+drain (suppressed, so no
    /// readiness or input can commit), then closes. PR284's exact CI cause
    /// was dropped by `.is_err()`, so the close-before-drain race is
    /// source-proven but exact CI causality is not fully proven.
    async fn reject_start(
        &mut self,
        mut session: AgentSession,
        params: &AgentStartParams,
        error: Error,
    ) -> Error {
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
        if begin_error.is_some() || exit_error.is_some() || drain_error.is_some() {
            let (stage, cleanup) = if let Some(cleanup) = begin_error {
                ("begin_stop", cleanup)
            } else if let Some(cleanup) = exit_error {
                ("terminate", cleanup)
            } else {
                ("drain", drain_error.expect("drain failure"))
            };
            session.phase = SessionPhase::Unknown;
            self.session = Some(session);
            let message = format!(
                "{}; owned CLI cleanup is unconfirmed at {} ({}): {}; session.ready was never committed",
                error.message, stage, cleanup.code, cleanup.message
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
        let callback_id = session.events.hook_callback_id().to_owned();
        let readback = match session
            .native_client
            .control(
                "initialize",
                json!({
                    "hooks": {
                        "PreToolUse": [{
                            "hookCallbackIds": [callback_id],
                            "timeout": 30,
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
        if readback.hooks_applied != Some(true) {
            return Err(self
                .reject_start(
                    session,
                    params,
                    Error::new(
                        "profile_error",
                        "Claude initialize did not apply the registered PreToolUse callback",
                    ),
                )
                .await);
        }
        // The CLI never reads the active model back in initialize (note 47):
        // membership in `models[]` above is the only proof, so
        // `effective.model` stays null at readiness and the requested id
        // travels under `requested`. It is populated later only by a model
        // reported on an actual native assistant record (message.completed).
        // Requested vs effective permission mode is reported, never forced:
        // launching with `manual` reads back `default` on the installed CLI.
        // The installed stream-json protocol (2.1.278) exposes no MCP
        // inventory record, so the effective server set stays unknown (null,
        // unverified) for both policies instead of echoing the requested set.
        let effective = with_context_points(
            json!({
                "model": null,
                "permission_mode": effective_mode,
                "session_state": readback.session_state,
                "pid": readback.pid,
                "mcp_policy": plan.requested_mcp_policy.as_str(),
                "mcp_servers": null,
                "mcp_servers_verified": false,
            }),
            &[InjectPoint::NextInput, InjectPoint::TurnSafePoint],
        );
        Ok((session, effective))
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
                "mcp_policy": plan.requested_mcp_policy.as_str(),
                "mcp_allow": plan.requested_mcp_allow.clone(),
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

    async fn command(&mut self, request: &RpcRequest) -> DispatchOutcome {
        let raw = &request.params;
        let params: AgentCommandParams =
            match decode(raw, "Claude agent.command params are invalid") {
                Ok(params) => params,
                Err(error) => return DispatchOutcome::Ready(Err(error)),
            };
        if let Err(error) = validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Claude",
        ) {
            return DispatchOutcome::Ready(Err(error));
        }
        match params.verb.as_str() {
            "send" => DispatchOutcome::Ready(self.send_command(params).await),
            "steer" => self.steer_command(request.id.clone(), params).await,
            "interrupt" => DispatchOutcome::Ready(self.interrupt_command(params).await),
            "approve" | "deny" => DispatchOutcome::Ready(self.reply_request(params).await),
            _ => DispatchOutcome::Ready(Err(Error::new(
                "unsupported",
                "Claude plugin command verb is not implemented",
            ))),
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
        let context = params.context.clone();
        let text = send_text(&params)?;
        if let Some(replay) = replay_send(session, &params.command_id, text) {
            return replay;
        }
        check_send_ready(session)?;
        let native_content = with_claimed_context(text, &context, "Claude")?;
        session
            .command_inputs
            .insert(params.command_id.clone(), text.to_owned());
        dispatch_send(session, &params.command_id, native_content, context).await
    }

    /// Steer the exact captured turn through the installed CLI's existing
    /// PreToolUse `additionalContext` hook response (it has no named steer
    /// method). The text queues behind any terminal frame and rides the next
    /// live hook callback for that turn only, combined after any FIFO hook
    /// context; applied is retained only after the native response confirms
    /// it. A turn that ends first drops the text (stale, never leaked into
    /// another turn); an uncertain write settles unknown without a replay.
    /// Registration returns immediately with the waiter deferred: the host
    /// loop keeps serving ordinary requests and settles the reply when the
    /// exact-turn waiter resolves, so this hook wait never blocks
    /// interrupt, stop, snapshot, or shutdown behind it.
    async fn steer_command(
        &mut self,
        request_id: Value,
        params: AgentCommandParams,
    ) -> DispatchOutcome {
        let ready = |result: Result<Value>| DispatchOutcome::Ready(result);
        if params.request_id.is_some() {
            return ready(Err(Error::new(
                "invalid",
                "Claude steer does not address a native request",
            )));
        }
        let turn_id = match params
            .turn_id
            .as_deref()
            .filter(|turn_id| !turn_id.trim().is_empty())
        {
            Some(turn_id) => turn_id,
            None => {
                return ready(Err(Error::new(
                    "stale_turn",
                    "Claude steer requires a captured turn id",
                )));
            }
        };
        let text = match params.args.get("text").and_then(Value::as_str) {
            Some(text) => text,
            None => {
                return ready(Err(Error::new(
                    "invalid",
                    "Claude steer requires args.text",
                )));
            }
        };
        {
            let session = match self.session.as_mut() {
                Some(session) => session,
                None => {
                    return ready(Err(Error::new(
                        "bad_state",
                        "Claude Agent session has not started",
                    )));
                }
            };
            if let Err(error) = validate_binding(
                &session.agent_id,
                session.generation,
                &params.agent_id,
                params.generation,
                "Claude",
            ) {
                return ready(Err(error));
            }
            // Replays first: a settled steer answers from retention even
            // while another steer reply is deferred or the session outcome is
            // unresolved, and a refused shape never overwrites a retained
            // outcome.
            if let Some(previous) = session.steer_outcomes.get(&params.command_id) {
                if previous.turn_id != turn_id || previous.text != text {
                    return ready(Err(Error::new(
                        "source_integrity",
                        "Claude command id was reused for another steer",
                    )));
                }
                return ready(Ok(previous.result.clone()));
            }
            // Proven no-effect: a fresh steer that never queues has no native
            // effect even when a prior session outcome is unresolved, so the
            // authoritative refusal is `rejected`, never Unknown.
            if session.phase != SessionPhase::Ready {
                return ready(Ok(rejected_control(
                    "Claude session or event source outcome is unresolved",
                )));
            }
            if text.trim().is_empty() {
                return ready(Err(Error::new("invalid", "Claude steer text is empty")));
            }
            if text.len() > MAX_TEXT_BYTES {
                let result = rejected_control("Claude steer input exceeds 512 KiB");
                session.steer_outcomes.insert(
                    params.command_id.clone(),
                    SteerRecord {
                        turn_id: turn_id.to_owned(),
                        text: text.to_owned(),
                        result: result.clone(),
                    },
                );
                return ready(Ok(result));
            }
        }
        // One deferred reply at a time: the event task holds the single
        // queue slot, so a second steer waits for this one's reply instead
        // of queueing behind an unanswered waiter.
        if self.deferred_steer.is_some() {
            // Proven no-effect: never queued, so the authoritative refusal is
            // `rejected`, not an error Core would classify Unknown. Transient:
            // a later command id may steer once this reply settles.
            return ready(Ok(rejected_control(
                "Claude already has a steer reply deferred",
            )));
        }
        let session = match self.session.as_mut() {
            Some(session) => session,
            None => {
                return ready(Err(Error::new(
                    "bad_state",
                    "Claude Agent session has not started",
                )));
            }
        };
        // Proven no-effect like the phase refusal above: the steer never
        // queues, so a prior event-source gap refuses it as `rejected` with
        // the exact gap reason instead of Unknown.
        if let Some(reason) = session.events.status().failure {
            return ready(Ok(rejected_control(&reason)));
        }
        // The event task answers behind any queued terminal frame, so a
        // steer racing completion refuses instead of queueing behind it.
        if !session.events.check_turn(turn_id).await {
            // Proven no-effect: never queued behind the finished turn.
            return ready(Ok(rejected_control("Claude turn is not the active turn")));
        }
        let outcome = match session
            .events
            .steer(
                params.command_id.clone(),
                turn_id.to_owned(),
                text.to_owned(),
            )
            .await
        {
            Ok(outcome) => outcome,
            // Actor busy/stale/`native_not_dispatched` prove pre-insertion
            // refusal (never queued; only register_steer emits these codes),
            // so they refuse as `rejected` with the exact reason.
            // Request-shape `invalid` and channel/write uncertainty keep their
            // error codes.
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "busy" | "stale_turn" | "native_not_dispatched"
                ) =>
            {
                return ready(Ok(rejected_control(&error.message)));
            }
            Err(error) => return ready(Err(error)),
        };
        DispatchOutcome::Deferred(DeferredSteer {
            request_id,
            command_id: params.command_id.clone(),
            turn_id: turn_id.to_owned(),
            text: text.to_owned(),
            outcome,
            deadline: Instant::now() + RPC_TIMEOUT,
            cancel_ack: None,
        })
    }

    /// Settle a deferred steer reply after its exact-turn waiter resolved.
    /// Retains and post-processes exactly like an immediate reply; the
    /// original command/turn binding travels unchanged into the retained
    /// record. A dead transport ends the loop.
    async fn settle_deferred_steer(
        &mut self,
        client: &PeerClient,
        waiter: DeferredSteer,
        arrived: Option<SteerOutcome>,
    ) -> Serve {
        let DeferredSteer {
            request_id,
            command_id,
            turn_id,
            text,
            ..
        } = waiter;
        let outcome = match arrived {
            Some(outcome) => outcome,
            None => {
                // Cancellation drops the queued sender before its ack is
                // observed. Retain the exact command/turn/text binding before
                // replying so replay cannot enqueue the same steer again.
                let result =
                    unknown_control("Claude steer outcome was lost; it will not be repeated");
                if let Some(session) = self.session.as_mut() {
                    session.steer_outcomes.insert(
                        command_id.clone(),
                        SteerRecord {
                            turn_id: turn_id.clone(),
                            text: text.clone(),
                            result: result.clone(),
                        },
                    );
                }
                let result = self.post_process("agent.command", Ok(result));
                return respond_deferred(client, request_id, result).await;
            }
        };
        let result = match outcome {
            SteerOutcome::Applied => Ok(json!({
                "state": "applied",
                "command_id": command_id.clone(),
                "turn_id": turn_id.clone(),
                "mechanism": STEER_MECHANISM,
            })),
            SteerOutcome::Rejected(reason) => Ok(rejected_control(&reason)),
            SteerOutcome::Unknown(reason) => Ok(unknown_control(&reason)),
            SteerOutcome::Stale => Ok(rejected_control(
                "Claude turn ended before the steer applied",
            )),
        };
        // Applied, rejected (including the stale-turn no-effect refusal),
        // and unknown settle into retention with their exact
        // command/turn/text binding.
        if let Ok(value) = result.as_ref()
            && let Some(session) = self.session.as_mut()
        {
            session.steer_outcomes.insert(
                command_id,
                SteerRecord {
                    turn_id,
                    text,
                    result: value.clone(),
                },
            );
        }
        let result = self.post_process("agent.command", result);
        respond_deferred(client, request_id, result).await
    }

    /// Settle after the deadline withdrawal was processed by the event task.
    /// The host kept both receivers live while continuing to serve controls;
    /// a confirmed steer outcome wins before a timeout is retained.
    async fn settle_deferred_deadline(
        &mut self,
        client: &PeerClient,
        mut waiter: DeferredSteer,
        cancellation_acknowledged: bool,
    ) -> Serve {
        // The event task serializes cancellation behind any callback write.
        // Its outcome sender runs before the cancellation acknowledgement, so
        // inspect the still-live receiver before retaining a timeout result.
        if let Ok(outcome) = waiter.outcome.try_recv() {
            return self
                .settle_deferred_steer(client, waiter, Some(outcome))
                .await;
        }
        let DeferredSteer {
            request_id,
            command_id,
            turn_id,
            text,
            ..
        } = waiter;
        if let Some(session) = self.session.as_mut() {
            let reason = if cancellation_acknowledged {
                "Claude steer was not confirmed before its deadline; it will not be repeated"
            } else {
                "Claude steer withdrawal was not confirmed before its deadline; it will not be repeated"
            };
            let result = unknown_control(reason);
            session.steer_outcomes.insert(
                command_id,
                SteerRecord {
                    turn_id,
                    text,
                    result: result.clone(),
                },
            );
            let result = self.post_process("agent.command", Ok(result));
            return respond_deferred(client, request_id, result).await;
        }
        let result = self.post_process(
            "agent.command",
            Err(Error::new(
                "bad_state",
                "Claude Agent session has not started",
            )),
        );
        respond_deferred(client, request_id, result).await
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
        .is_some_and(|turn| turn.final_text.is_some() && turn.final_text_unavailable.is_none());
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
    drop(session.mcp_config.take());
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

/// Pre-dispatch gates for a fresh send: a failed event source and an already
/// active turn.
fn check_send_ready(session: &AgentSession) -> Result<()> {
    let event_status = session.events.status();
    if let Some(reason) = event_status.failure {
        return Err(Error::new("unknown", reason));
    }
    if event_status.active_turn_id.is_some() {
        return Err(Error::new("busy", "Claude already has an active turn"));
    }
    Ok(())
}

/// Mint the exact turn association before the write and register it with
/// the event task first: an early lifecycle can never miss its waiter, and
/// a lost association latches unknown without a rewrite.
async fn dispatch_send(
    session: &mut AgentSession,
    command_id: &str,
    content: String,
    context: Vec<ContextItem>,
) -> Result<Value> {
    let turn_id = new_uuid();
    let outcome = match session
        .events
        .expect_turn(command_id.to_owned(), turn_id.clone(), context)
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
            content,
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
    let mcp_config =
        ClaudeMcpConfigFile::create(&plan.instance_dir, &plan.swarm_binary, &plan.data_root)?;
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
    argv.extend(mcp_argv(
        plan.requested_mcp_policy,
        mcp_config.path().as_os_str(),
    ));
    // The CLI keeps its own login (not the desktop app's): check it, and run
    // its browser login when it is logged out, before any native resource.
    crate::native::ensure_logged_in(&plan.program, &plan.workspace, &plan.environment).await?;
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
        mcp_config: Some(mcp_config),
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
        steer_outcomes: HashMap::new(),
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

fn native_resume_permission_mode(profile_mode: &str) -> &str {
    match profile_mode {
        "manual" => "default",
        mode => mode,
    }
}

fn setter_error(error: Error, subtype: &str) -> Error {
    // Definite local non-admission: the control write provably never
    // reached the CLI, so the start is rejected, never a profile verdict.
    if native_no_write(&error.code) {
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

/// MCP argv pair for the frozen policy. Minimal adds `--strict-mcp-config`
/// ahead of the per-run config so only it applies (installed CLI 2.1.278:
/// the flag ignores all other MCP configurations); inherit passes only the
/// additive per-run config and keeps the normal precedence.
fn mcp_argv(policy: McpPolicy, config_path: &std::ffi::OsStr) -> Vec<OsString> {
    let mut argv = Vec::with_capacity(3);
    if policy == McpPolicy::Minimal {
        argv.push(OsString::from("--strict-mcp-config"));
    }
    argv.push(OsString::from("--mcp-config"));
    argv.push(config_path.to_owned());
    argv
}

fn claude_manifest() -> PluginManifest {
    let controlled = BTreeMap::from([
        ("message".to_owned(), "stream".to_owned()),
        ("request".to_owned(), "stream".to_owned()),
        ("session".to_owned(), "stream".to_owned()),
        ("tool".to_owned(), "stream".to_owned()),
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
            "steer".to_owned(),
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

/// The shared start/input/stop commands plus the Claude-only interrupt,
/// request-reply, and steer commands.
fn known_command(session: &AgentSession, command_id: &str) -> bool {
    session::known_command(
        &session.start_command_id,
        &session.command_inputs,
        &session.stop_command_id,
        command_id,
    ) || session.interrupt_applied.contains_key(command_id)
        || session.reply_outcomes.contains_key(command_id)
        || session.steer_outcomes.contains_key(command_id)
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
    use super::{claude_manifest, claude_scope, mcp_argv, new_uuid, setter_error};
    use eliot_core::Error;
    use eliot_core::McpPolicy;
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
    fn mcp_argv_is_strict_only_for_minimal() {
        let config = std::ffi::OsStr::new("agent-mcp.json");
        assert_eq!(
            mcp_argv(McpPolicy::Inherit, config),
            vec![
                std::ffi::OsString::from("--mcp-config"),
                std::ffi::OsString::from("agent-mcp.json"),
            ]
        );
        assert_eq!(
            mcp_argv(McpPolicy::Minimal, config),
            vec![
                std::ffi::OsString::from("--strict-mcp-config"),
                std::ffi::OsString::from("--mcp-config"),
                std::ffi::OsString::from("agent-mcp.json"),
            ]
        );
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
                "steer".to_owned(),
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

    #[test]
    fn setter_non_admission_is_rejected_and_loss_stays_unknown() {
        for code in ["busy", "frame_too_large", "native_not_dispatched"] {
            let error = setter_error(Error::new(code, "write never sent"), "set_model");
            assert_eq!(error.code, "rejected", "code={code}");
            assert!(
                error.message.contains("set_model"),
                "message={}",
                error.message
            );
        }
        for code in ["unknown", "native_rejected", "internal"] {
            let error = setter_error(Error::new(code, "outcome lost"), "initialize");
            assert_eq!(error.code, "unknown", "code={code}");
        }
    }
}
