mod catalogue;
mod start;

use crate::{
    events::{EventHandle, EventStatus, TurnFacts, turn_native_error},
    native::{
        NativeClient, NativeLaunch, NativeProcess,
        requests::describe_request,
        types::{
            NativeApprovalListPendingParams, NativeApprovalListPendingResult,
            NativeClientCapabilities, NativeClientInfo, NativeInitializeParams,
            NativeItemCompletedNotification, NativeModelEffortVariants, NativeModelListParams,
            NativeModelListResult, NativeModelSelection, NativeSession, NativeSessionReadParams,
            NativeSessionReadResult, NativeSessionReasoningEffortChangedNotification,
            NativeSessionResumeParams, NativeSessionResumeResult, NativeSessionSetModelParams,
            NativeSessionSetModelResult, NativeSessionStartParams, NativeSessionStartResult,
            NativeTurnCancelParams, NativeTurnCancelResult, NativeTurnCompletedNotification,
            NativeTurnInputPart, NativeTurnStartParams, NativeTurnStartResult,
            NativeTurnStartedNotification, NativeTurnSteerParams, NativeTurnSteerResult,
            NativeTurnTerminal, NativeUnframedViewEvent, NativeViewPageParams,
            NativeViewPageResult, is_reasoning_effort_tier,
        },
    },
};
use eliot_core::{
    ContextOutcome, Error, FailureOrigin, InjectArgs, InjectPoint, Result, SecretPolicy,
    StderrSnapshot,
};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::stream_sources,
    session::{
        ModelSelection, attach_reply_diagnostics, begin_stop, decode, ensure_dedicated_reader,
        ensure_reader_harness, initialize_fields_valid, model_catalogue_profile, native_no_write,
        not_initialized, paths_equal, register_native_process, rejected_control, request_reply_ids,
        required_native_cmd, shutdown_session, stop_owned_session, target_environment,
        unknown_control, unknown_snapshot, validate_binding, validate_identity,
        with_claimed_context, with_context_points, with_model_catalogue_diagnostic,
    },
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, HashSet},
    ffi::OsString,
    path::Path,
};
use tokio::{sync::mpsc, time::Instant};

const RPC_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

fn request_deadline(received_at: Instant) -> Instant {
    received_at + RPC_TIMEOUT
}
const STEER_READ_DEADLINE: std::time::Duration = std::time::Duration::from_secs(10);

fn project_model_catalog(
    models: Vec<crate::native::types::NativeModelCatalogEntry>,
) -> (Vec<serde_json::Value>, Option<String>) {
    let default_id = models
        .iter()
        .find(|entry| entry.is_default == Some(true))
        .map(|entry| entry.model_id.clone());
    let projected = models
        .into_iter()
        .map(|entry| {
            let efforts = match entry.variants {
                Some(crate::native::types::NativeModelEffortVariants::Known(variants)) => {
                    Some(variants)
                }
                _ => None,
            };
            let is_default = entry.is_default == Some(true);
            serde_json::json!({
                "id": entry.model_id,
                "display_name": entry.display_label.as_deref().and_then(eliot_core::api::harness::native_model_display_name),
                "default": is_default,
                "efforts": efforts,
            })
        })
        .collect();
    (projected, default_id)
}

fn quota_read_error(error: Error) -> Error {
    if error.failure_where.as_deref() == Some("plugin.muse.native.response.method_not_found") {
        error
            .with_code("unsupported")
            .with_message("Installed Muse MSP does not support usage/read")
    } else {
        let detail = error.message.clone();
        error.with_message(format!("Muse usage/read did not return quota: {detail}"))
    }
}

/// MSP `usedPercent` contract (`SubscriptionUsageWindow`/`SubscriptionUsageWeekly`):
/// an integer >= 0 with no upper bound. Over-quota values above 100 are valid
/// measurements, never a schema mismatch; only a negative value rejects.
fn validate_usage_read_percent(used: i64) -> Result<()> {
    if used < 0 {
        return Err(Error::new(
            "profile_error",
            "Muse usage/read percent did not match the installed schema",
        ));
    }
    Ok(())
}

/// A failed read is safe to refuse before input dispatch. It is never a
/// terminal verdict, and never permission to change or replay the input.
fn validate_steer_readback(readback: Result<Value>, session_id: &str, turn_id: &str) -> Result<()> {
    let refusal = || {
        Error::new(
            "native_rejected",
            "Muse steer refused before dispatch: session/read did not prove a readable native log and the exact running turn; no turn/steer was sent",
        )
    };
    let value = readback.map_err(|_| refusal())?;
    let read: NativeSessionReadResult = serde_json::from_value(value).map_err(|_| refusal())?;
    if read.session.session_id != session_id
        || read.session.status != "running"
        || read.session.active_turn_id.as_deref() != Some(turn_id)
        || read.view_cursor.trim().is_empty()
        || read.history.unavailable_reason().is_some()
    {
        return Err(refusal());
    }
    Ok(())
}

/// Native `initialize` client identifier. The installed host requires a
/// machine identifier (`^[a-z0-9_]+$`); the launch path and the read-only
/// model.list reader send the same value through [`native_initialize_params`].
pub(super) const NATIVE_CLIENT_INFO_NAME: &str = "eliot";

/// Shared native `initialize` params for the launch path and the read-only
/// model.list reader: identical client identity on both paths.
pub(super) fn native_initialize_params() -> NativeInitializeParams {
    NativeInitializeParams {
        client_info: NativeClientInfo {
            name: NATIVE_CLIENT_INFO_NAME.to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            title: Some("Eliot".to_owned()),
        },
        // Eliot answers `userInput/request` through its answer/deny verbs;
        // `true` (the D-047 capable posture) has the host serve prompts
        // toward this client.
        capabilities: Some(NativeClientCapabilities {
            user_input_dialogs: Some(true),
        }),
    }
}
const STOP_SETTLEMENT_RESERVE: std::time::Duration = std::time::Duration::from_secs(5);
const STOP_RESPONSE_RESERVE: std::time::Duration = std::time::Duration::from_secs(5);
/// Time reserved at the end of an `agent.command` send budget to return
/// the typed rejection (k167b): proof reads share the request deadline
/// but must finish early enough that the rejection still settles
/// `rejected` instead of racing the daemon's 30 s `unknown`.
const SEND_REJECTION_RESERVE: std::time::Duration = std::time::Duration::from_secs(5);
/// Typed follow-up rejection while the host-scoped MCP startup-audit
/// latch stands (E47): MCP is disabled for this host's lifetime, so only
/// an Agent restart on a fresh native host can continue.
const MCP_AUDIT_DISABLED_REJECTION: &str = "Muse MCP startup audit failed; MCP is disabled for this runtime. Restart the Agent on the same worktree to continue with a fresh native host; turn/start was not dispatched.";
pub(crate) const VIEW_PAGE_SIZE: i64 = 1000;
pub(crate) const MAX_VIEW_PAGES: usize = 4;

#[derive(Default)]
struct MusePlugin {
    initialized: Option<Initialized>,
    session: Option<AgentSession>,
    failed_start: Option<FailedStart>,
}

enum HostLoopEvent {
    Request(Option<RpcRequest>),
    NativeExit,
}

struct Initialized {
    credential: String,
    instance_dir: String,
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

struct AgentSession {
    agent_id: String,
    generation: i64,
    start_command_id: String,
    phase: SessionPhase,
    session_id: Option<String>,
    native_scope: String,
    selected_effort: Option<String>,
    /// Session-verified model the owned native session currently runs:
    /// the start/resume readback, refreshed by `configure` after its
    /// `session/read` proof. Reported on turn association so the readback
    /// follows a live switch; omitted when never verified.
    selected_model: Option<String>,
    secrets: Vec<String>,
    native: NativeProcess,
    native_client: NativeClient,
    events: EventHandle,
    current_turn: Option<(String, String)>,
    stop_command_id: Option<String>,
    /// Kept until the native host accepts a turn with the bundled skill pointer.
    skill_pointer: Option<String>,
}

#[allow(
    dead_code,
    reason = "The host start submodule consumes this initialization record."
)]
struct InitializedNative {
    effort: Option<String>,
    scope: String,
    fingerprint: String,
    catalogue_omits_model: bool,
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

    let mut plugin = MusePlugin::default();
    let mut completed_shutdown = false;
    let mut native_endpoint_retired = false;
    let mut native_retirement = Ok(());
    let mut pending_request: Option<(RpcRequest, Option<Instant>)> = None;
    loop {
        let mut carried_stop_deadline = None;
        let event = if let Some((request, stop_deadline)) = pending_request.take() {
            carried_stop_deadline = stop_deadline;
            HostLoopEvent::Request(Some(request))
        } else {
            tokio::select! {
                biased;
                () = eliot_plugin::native_lifecycle::wait_for_native_exit(
                    plugin.session.as_mut().map(|session| &mut session.native.transport)
                ) => HostLoopEvent::NativeExit,
                request = requests.recv() => HostLoopEvent::Request(request),
            }
        };
        match event {
            HostLoopEvent::NativeExit => {
                if let Some(session) = plugin.session.as_mut() {
                    // Confirmed exit only: an unconfirmed wait error records
                    // nothing. Capture before the drain borrows the transport
                    // so the exit rides the event queue ahead of close.
                    let exit = session.native.transport.wait_for_process_exit().await;
                    let stderr = session.native.transport.stderr_snapshot();
                    let native_exit = exit
                        .ok()
                        .map(|status| native_exit_text(&status, &stderr, &session.secrets));
                    native_retirement = eliot_plugin::native_lifecycle::drain_native_exit(
                        &mut session.native.transport,
                        async {
                            session.events.drain_native().await?;
                            if let Some(text) = native_exit {
                                session.events.record_native_exit(text).await;
                            }
                            session.events.close().await;
                            Ok(())
                        },
                    )
                    .await;
                }
                native_endpoint_retired = true;
                break;
            }
            HostLoopEvent::Request(None) => break,
            HostLoopEvent::Request(Some(request)) => {
                let is_shutdown = request.method == "shutdown";
                let is_stop = request.method == "agent.stop";
                let deadline =
                    carried_stop_deadline.unwrap_or_else(|| request_deadline(Instant::now()));
                let result = if request.method == "agent.snapshot" {
                    match eliot_plugin::preemptible_snapshot(
                        &mut requests,
                        plugin.dispatch(&request, &client, deadline),
                    )
                    .await
                    {
                        eliot_plugin::SnapshotDispatch::Ready(result) => result,
                        eliot_plugin::SnapshotDispatch::Preempted(next) => {
                            let stop_deadline = next
                                .as_ref()
                                .filter(|next| next.method == "agent.stop")
                                .map(|_| request_deadline(Instant::now()));
                            let preempted = client
                                .respond(
                                    request.id.clone(),
                                    Err(Error::new(
                                        "busy",
                                        "status snapshot was preempted by a host request",
                                    )),
                                    stop_deadline.unwrap_or_else(|| Instant::now() + RPC_TIMEOUT),
                                )
                                .await;
                            if preempted.is_err() {
                                break;
                            }
                            let Some(next) = next else { break };
                            pending_request = Some((next, stop_deadline));
                            continue;
                        }
                    }
                } else {
                    plugin.dispatch(&request, &client, deadline).await
                };
                let response_deadline = if is_stop {
                    deadline
                } else {
                    Instant::now() + RPC_TIMEOUT
                };
                let response = client.respond(request.id, result, response_deadline).await;
                if is_shutdown {
                    completed_shutdown = response.is_ok();
                    break;
                }
                if response.is_err() {
                    break;
                }
            }
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
            "Muse plugin input closed before shutdown completed",
        ))
    }
}

impl MusePlugin {
    async fn dispatch(
        &mut self,
        request: &RpcRequest,
        client: &PeerClient,
        deadline: Instant,
    ) -> Result<Value> {
        let result = match request.method.as_str() {
            "initialize" => self.initialize(&request.params),
            "model.list" => self.model_list(&request.params).await,
            "quota.read" => self.quota_read(&request.params).await,
            "agent.start" => self.start(&request.params, client).await,
            "agent.command" => self.command(&request.params, deadline).await,
            "agent.snapshot" => self.snapshot(&request.params).await,
            "agent.stop" => self.stop(&request.params, deadline).await,
            "shutdown" => self.shutdown().await,
            _ => Err(Error::new(
                "unsupported",
                "Muse plugin method is not implemented",
            )),
        };
        let Some(session) = self.session.as_ref() else {
            return result;
        };
        attach_reply_diagnostics(
            result,
            &request.method,
            "Muse",
            |error| session.native_client.diagnostic_error(error),
            |_| Error::new("unknown", ""),
        )
    }

    fn initialize(&mut self, raw: &Value) -> Result<Value> {
        let params: InitializeParams = decode(raw, "Muse plugin initialize params are invalid")?;
        if !initialize_fields_valid(&params) {
            return Err(Error::new(
                "invalid",
                "Muse plugin initialize requires protocol, private instance directory, and credential",
            ));
        }
        if let Some(existing) = &self.initialized {
            if existing.credential != params.credential
                || existing.instance_dir != params.instance_dir
            {
                return Err(Error::new(
                    "bad_state",
                    "Muse plugin is already initialized for another instance",
                ));
            }
        } else {
            self.initialized = Some(Initialized {
                credential: params.credential,
                instance_dir: params.instance_dir,
            });
        }
        serde_json::to_value(muse_manifest())
            .map_err(|_| Error::new("internal", "Muse plugin manifest could not be encoded"))
    }

    /// Settle a failed start: the settled marker refuses any second
    /// agent.start, and the daemon retires this plugin process.
    fn retain_failed_start(&mut self, error: Error) -> Error {
        self.failed_start = Some(FailedStart);
        error
    }

    /// Retain a proven-no-effect start failure and build its failed-state
    /// result: no `session/start|resume` was dispatched and no input was
    /// sent, so the launch is definitely failed, never unknown. The reason
    /// names the failed check in short non-secret words; it is persisted in
    /// the daemon outcome, so it must never carry paths, values, or secrets.
    fn retain_proven_no_effect_start(&mut self, params: &AgentStartParams, error: Error) -> Value {
        debug_assert!(eliot_plugin::start_classification::is_proven_no_effect(
            eliot_plugin::start_classification::StartPoint::NativeRefusalBeforeDispatch
        ));
        let error = eliot_plugin::session::classify_start_failure(error, "muse", "prepare");
        let value = eliot_plugin::session::failed_start_value(params, error);
        self.failed_start = Some(FailedStart);
        value
    }

    async fn start(&mut self, raw: &Value, peer: &PeerClient) -> Result<Value> {
        // The daemon sends agent.start exactly once per freshly spawned
        // plugin process: a bound or settled plugin never starts again.
        if self.session.is_some() || self.failed_start.is_some() {
            return Err(Error::new(
                "bad_state",
                "Muse plugin already received agent.start",
            ));
        }
        let params: AgentStartParams = decode(raw, "Muse agent.start params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Muse",
        )?;
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Muse"))?;
        // Profile validation runs before any spawn: zero native I/O exists,
        // so a refusal here is proven-no-effect (failed, never unknown).
        let start::PreparedStart {
            resume_subject,
            requested_model,
            requested_effort,
            requested_approval,
            requested_permissions,
            workspace,
            launch,
            secrets,
        } = match start::prepare_profile(&params, initialized) {
            Ok(prepared) => prepared,
            Err(error) => {
                return Ok(self.retain_proven_no_effect_start(&params, error));
            }
        };
        let skill_pointer = match start::skill_pointer(&params) {
            Ok(pointer) => pointer,
            Err(error) => return Ok(self.retain_proven_no_effect_start(&params, error)),
        };
        let policy = SecretPolicy::new(secrets.iter());
        let mut host = match catalogue::CatalogueHost::open(
            &launch,
            Path::new(&initialized.instance_dir),
            &policy,
        )
        .await
        {
            Ok(host) => host,
            Err(error) if error.code == "unknown" => return Err(self.retain_failed_start(error)),
            Err(error) => return Ok(self.retain_proven_no_effect_start(&params, error)),
        };
        let initialized_native = match Self::validate_catalogue(
            &host,
            requested_model.as_deref(),
            requested_effort.as_deref(),
        ) {
            Ok(validated) => validated,
            Err(error) => {
                return match host.close().await {
                    Ok(()) => Ok(self.retain_proven_no_effect_start(&params, error)),
                    Err(cleanup) => Err(self.retain_failed_start(cleanup)),
                };
            }
        };
        let mut session = match start::bind_owned_session(
            &params,
            initialized,
            host.native,
            secrets,
            skill_pointer,
            peer,
        ) {
            Ok(session) => session,
            Err(error) => return Ok(self.retain_proven_no_effect_start(&params, error)),
        };

        if let Err(error) = register_native_process(
            peer,
            &initialized.credential,
            &session.agent_id,
            session.generation,
            session.native.pid(),
            "Muse",
            "session host",
            "session",
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
                    "{}; owned session host cleanup is unconfirmed; no session or input was sent",
                    error.message
                );
                return Err(contextual_error(error, "unknown", message));
            }
            // Registration failed but the child is terminated: no session was
            // dispatched, so this is proven-no-effect. Registration messages
            // are canned words plus a code, safe to persist as the reason.
            return Ok(self.retain_proven_no_effect_start(&params, error));
        }

        session.selected_effort = initialized_native.effort.clone();
        session.native_scope = initialized_native.scope;

        if let Some(subject) = &resume_subject {
            if subject.native_scope != session.native_scope {
                let error = discard_failed_start(
                    &mut session,
                    Error::new(
                        "profile_error",
                        "Muse resume subject is outside the authenticated native scope",
                    ),
                )
                .await;
                if session.phase == SessionPhase::Unknown {
                    let message = format!(
                        "{}; owned session host cleanup is unconfirmed; no session/resume or input was sent",
                        error.message
                    );
                    self.session = Some(session);
                    return Err(error.with_code("unknown").with_message(message));
                }
                // No `session/resume` was dispatched: proven-no-effect.
                return Ok(self.retain_proven_no_effect_start(&params, error));
            }
            session.session_id = Some(subject.native_ref.clone());
            if let Err(error) = verify_resume_preflight(
                &session.native_client,
                &subject.native_ref,
                subject.retained_turn.as_deref(),
            )
            .await
            {
                let message = format!(
                    "Muse resume preflight failed before session/resume dispatch: {}",
                    error.message
                );
                // Only prerequisite reads ran; native session effects were not dispatched.
                let error = error.with_code("native_readback").with_message(message);
                let error = discard_failed_start(&mut session, error).await;
                if session.phase == SessionPhase::Unknown {
                    let message = format!(
                        "{}; owned session host cleanup is unconfirmed; no session/resume or input was sent",
                        error.message
                    );
                    self.session = Some(session);
                    return Err(error.with_code("unknown").with_message(message));
                }
                // Only prerequisite reads ran: proven-no-effect.
                return Ok(self.retain_proven_no_effect_start(&params, error));
            }
        }

        // Params encoding runs before any dispatch: an (unreachable in
        // practice) encoding failure is still proven-no-effect.
        let (session_method, params_value) = if let Some(subject) = &resume_subject {
            let native_params = NativeSessionResumeParams {
                command_id: new_command_id(),
                session_id: subject.native_ref.clone(),
                exclude_items: Some(true),
            };
            let encoded = serde_json::to_value(native_params).map_err(|_| {
                Error::new(
                    "internal",
                    "Muse session/resume params could not be encoded",
                )
            });
            match encoded {
                Ok(value) => ("session/resume", value),
                Err(error) => {
                    return Ok(self.retain_proven_no_effect_start(&params, error));
                }
            }
        } else {
            let native_params = NativeSessionStartParams {
                command_id: new_command_id(),
                // Omitted model defers to the server default; the effective
                // id is read back from the session below.
                model_id: requested_model.clone(),
                workspace_root: Some(workspace.to_string_lossy().into_owned()),
                approval_mode: requested_approval.clone(),
            };
            let encoded = serde_json::to_value(native_params).map_err(|_| {
                Error::new("internal", "Muse session/start params could not be encoded")
            });
            match encoded {
                Ok(value) => ("session/start", value),
                Err(error) => {
                    return Ok(self.retain_proven_no_effect_start(&params, error));
                }
            }
        };
        let dispatched = self
            .dispatch_session_start(session, session_method, params_value)
            .await?;
        let start::DispatchedStart {
            mut session,
            response: response_value,
        } = dispatched;
        // A lost resume reply leaves the write unknown under the minted
        // command id; the resume is never dispatched a second time.
        let mut snapshot_pull: Option<bool> = None;
        let mut resume_pointers = Vec::new();
        let native_session = if resume_subject.is_some() {
            let mut resumed = match serde_json::from_value::<NativeSessionResumeResult>(
                response_value.clone(),
            ) {
                Ok(resumed)
                    if !resumed.session.session_id.trim().is_empty()
                        && resume_subject.as_ref().is_some_and(|subject| {
                            subject.native_ref == resumed.session.session_id
                        })
                        // excludeItems=true was requested: the served history
                        // must be excluded (mode "none"), never republished.
                        && resumed.history.mode == "none" =>
                {
                    resumed
                }
                _ => {
                    // The retained session id stays: a mismatched host
                    // identity (or served history) fails the resume without
                    // adopting the foreign identity.
                    self.session = Some(session);
                    return Err(Error::new(
                        "unknown",
                        format!(
                            "Muse {session_method} returned without the exact retained session identity and excluded history"
                        ),
                    ));
                }
            };
            // Resume does not emit session/started. Bind only this verified
            // response identity before readiness, including early health pushes
            // queued behind requests belonging to the retained generation.
            session.session_id = Some(resumed.session.session_id.clone());
            if let Err(error) = session
                .events
                .view_readback(
                    resumed.session.session_id.clone(),
                    resumed.history.unavailable_reason(),
                )
                .await
            {
                self.session = Some(session);
                return Err(error);
            }
            // Late-joiner pointers are pulled after readiness (below): the
            // full payloads arrive as re-issued server-to-client requests
            // right after this response, and the pull covers any re-issue the
            // stream loses. The re-issues buffer until a turn associates, so
            // adoption still registers before they apply.
            if !resumed.pending_requests.is_empty() {
                resume_pointers = std::mem::take(&mut resumed.pending_requests);
            }
            if resumed.session.status != "idle" {
                self.session = Some(session);
                return Err(Error::new(
                    "unknown",
                    "Muse session/resume did not prove the resumed session idle; no input was sent",
                ));
            }
            resumed.session
        } else {
            let started = match serde_json::from_value::<NativeSessionStartResult>(
                response_value.clone(),
            ) {
                Ok(started) if !started.session.session_id.trim().is_empty() => started,
                _ => {
                    if let Some(session_id) = response_value
                        .get("session")
                        .and_then(|session| session.get("sessionId"))
                        .and_then(Value::as_str)
                    {
                        session.session_id = Some(session_id.to_owned());
                    }
                    self.session = Some(session);
                    return Err(Error::new(
                        "unknown",
                        format!(
                            "Muse {session_method} returned without the exact expected session identity"
                        ),
                    ));
                }
            };
            if started.session.status != "idle" {
                session.session_id = Some(started.session.session_id.clone());
                self.session = Some(session);
                return Err(Error::new(
                    "unknown",
                    "Muse session/start did not prove the new session idle; no input was sent",
                ));
            }
            started.session
        };
        session.session_id = Some(native_session.session_id.clone());
        let effort_verified = self
            .apply_start_effort(
                &mut session,
                &native_session,
                initialized_native.effort.as_deref(),
            )
            .await?;
        // Model and approval alignment on resume: the frozen profile is the
        // source of truth, so a retained session running another model or
        // approval mode is realigned via the setters (launch already passes
        // both to `session/start`). Either setter runs idle — the resume
        // preflight proved it — so admission applies immediately and one
        // `session/read` confirms both.
        let model_set = self
            .align_resume_model(
                &mut session,
                &native_session,
                resume_subject.is_some(),
                requested_model.as_deref(),
            )
            .await?;
        let approval_set = self
            .align_resume_approval(
                &mut session,
                &native_session,
                resume_subject.is_some(),
                requested_approval.as_deref(),
            )
            .await?;
        let setters_ran = model_set || approval_set;
        // Effective readback: one `session/read` confirms both aligned values
        // (the `Session` object carries `modelId` and `approvalMode`); when
        // no setter ran, the start/resume result already states them.
        let (effective_model, effective_approval) =
            if setters_ran {
                let reread =
                    match read_owned_session(&session.native_client, &native_session.session_id)
                        .await
                    {
                        Ok(reread) => {
                            if let Some(reason) = reread.history.unavailable_reason() {
                                let loss = session
                                    .events
                                    .view_readback(reread.session.session_id.clone(), Some(reason))
                                    .await;
                                self.session = Some(session);
                                loss?;
                                return Err(Error::new(
                                    "unknown",
                                    "Muse settings readback view is unavailable",
                                ));
                            }
                            reread.session
                        }
                        Err(error) => {
                            let error = discard_failed_start(&mut session, error).await;
                            return Err(self.retain_failed_start(error));
                        }
                    };
                (
                    reread.model_id.clone(),
                    reread.approval_mode.map(|state| state.mode),
                )
            } else {
                (
                    native_session.model_id.clone(),
                    native_session
                        .approval_mode
                        .as_ref()
                        .map(|state| state.mode.clone()),
                )
            };
        // An omitted request accepts the server-selected model as
        // effective, but only when the server actually names one; an empty
        // or missing `modelId` never verifies. An explicit request must
        // match exactly. Retain it as the session-verified model so later
        // turn associations report the switch instead of the launch value.
        session.selected_model = effective_model.clone();
        let model_verified = match requested_model.as_deref() {
            None => effective_model
                .as_deref()
                .is_some_and(|model| !model.trim().is_empty()),
            Some(requested) => effective_model.as_deref() == Some(requested),
        };
        let approval_verified = requested_approval
            .as_deref()
            .is_some_and(|approval| effective_approval.as_deref() == Some(approval));
        let effective = json!({
            "model": effective_model.clone(),
            "model_provider": native_session.provider_id.clone(),
            "model_verified": model_verified,
            "cwd": native_session.workspace_root.clone(),
            "effort": initialized_native.effort.clone(),
            "effort_verified": effort_verified,
            "approval": effective_approval.clone(),
            "approval_verified": approval_verified,
            "permissions": Value::Null,
            "permissions_verified": false,
            "schema_fingerprint": initialized_native.fingerprint.clone(),
        });
        let effective =
            with_model_catalogue_diagnostic(effective, initialized_native.catalogue_omits_model);
        let effective = with_context_points(
            effective,
            &[InjectPoint::NextInput, InjectPoint::ActiveTurn],
        );
        // The effective settings are checked BEFORE readiness: on any
        // mismatch no `session.ready` is published and no session is kept.
        let effort_match = initialized_native.effort.is_none() || effort_verified;
        let approval_match = requested_approval.is_none() || approval_verified;
        let settings_match = effort_match
            && approval_match
            && model_verified
            && native_session
                .workspace_root
                .as_deref()
                .is_some_and(|root| paths_equal(Path::new(root), &workspace));
        if !settings_match {
            let error = discard_failed_start(
                &mut session,
                Error::new(
                    "profile_error",
                    "Muse effective model, workspace root, reasoning effort, or approval mode did not match the frozen profile",
                ),
            )
            .await;
            return Err(self.retain_failed_start(error));
        }
        session = self
            .publish_start_ready(session, &native_session, &effective)
            .await?;
        // Adoption is best-effort and never fails the resume: entries that
        // bind to a live turn open, the dead generation's orphans drop. It
        // runs after readiness so a corrupt snapshot entry gaps the source
        // without failing the resume itself.
        if !resume_pointers.is_empty() {
            snapshot_pull = Some(
                adopt_resume_snapshot(
                    &session.native_client,
                    &session.events,
                    native_session.session_id.as_str(),
                    &resume_pointers,
                )
                .await,
            );
        }
        let mut result = json!({
            "agent_id": params.agent_id,
            "generation": params.generation,
            "command_id": params.command_id,
            "state": "ready",
            "native_scope": session.native_scope.clone(),
            "native_ref": native_session.session_id,
            "requested": {
                "model": requested_model,
                "effort": initialized_native.effort,
                "approval": requested_approval,
                "permissions": requested_permissions,
                "cwd": workspace.to_string_lossy(),
            },
            "effective": effective,
        });
        // Whether the resume late-joiner pull landed. `failed` is visible but
        // never fatal: the re-issued stream requests still adopt the pointers.
        if let Some(pulled) = snapshot_pull
            && let Some(fields) = result.as_object_mut()
        {
            fields.insert(
                "snapshot_pull".to_owned(),
                Value::String(if pulled {
                    "ok".to_owned()
                } else {
                    "failed".to_owned()
                }),
            );
        }
        self.session = Some(session);
        Ok(result)
    }

    async fn command(&mut self, raw: &Value, deadline: Instant) -> Result<Value> {
        let params: AgentCommandParams = decode(raw, "Muse agent.command params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Muse",
        )?;
        match params.verb.as_str() {
            "send" => self.send_command(params, deadline).await,
            "configure" => self.configure_command(params).await,
            "steer" => self.steer_command(params).await,
            "inject" => self.inject_active_turn(params).await,
            "interrupt" => self.interrupt_command(params).await,
            "approve" | "deny" | "answer" => self.reply_request(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Muse plugin command verb is not implemented",
            )),
        }
    }

    /// Build one native `turn/start` request from the effective reasoning
    /// effort: the retained `configure` overlay or the frozen session
    /// value. The installed 1.4 schema carries effort per turn; the model
    /// switches live through `session/setModel` at configure time, and the
    /// association below reports the session-verified model so the readback
    /// follows the switch.
    fn turn_start_params(
        command_id: String,
        session_id: String,
        input: Vec<NativeTurnInputPart>,
        reasoning_effort: Option<String>,
    ) -> NativeTurnStartParams {
        NativeTurnStartParams {
            command_id,
            session_id,
            input,
            reasoning_effort,
        }
    }

    /// Switch the live session model through `session/setModel` (and the
    /// standing reasoning effort through `session/setReasoningEffort` when
    /// requested), between turns only. The switch runs only when no turn
    /// is active, so a mid-turn configure refuses before any write with
    /// proof of no effect. Each setter ack is verified
    /// against its readback (`session/read` for the model, the durable
    /// `session/reasoningEffortChanged` view event for the effort); a
    /// refusal or a provably-undispatched write refuses with its reason,
    /// and a lost or ambiguous outcome settles unknown without a replay,
    /// storing nothing.
    async fn configure_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Muse Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Muse",
        )?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Muse session or event source outcome is unresolved",
            ));
        }
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "Muse session identity is unresolved"))?;
        let requested = match parse_configure_request(&params.args) {
            Ok(request) => request,
            Err(error) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": error.message,
                }));
            }
        };
        let Some(requested_model) = requested.model else {
            return Ok(json!({
                "state": "rejected",
                "reason": "Muse configure needs a model; an effort-only change applies per turn without a native switch",
            }));
        };
        // Busy gate before any native write: an exact active turn means a
        // model call is in flight. The native would admit the selection
        // for the next model-call boundary; Eliot refuses instead so the
        // outcome stays exact.
        let event_status = match session.events.view_readback(session_id.clone(), None).await {
            Ok(status) => status,
            Err(_) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Muse native observation is unresolved; session/setModel was not dispatched",
                }));
            }
        };
        if let Some(reason) = event_status.failure {
            return Ok(json!({
                "state": "rejected",
                "reason": format!("{reason}; session/setModel was not dispatched"),
            }));
        }
        if event_status.active_turn_id.is_some() {
            return Ok(json!({
                "state": "rejected",
                "reason": "Muse already has an active turn; configure the model between turns",
            }));
        }
        let deadline = Instant::now() + RPC_TIMEOUT;
        // The exact catalogue row supplies effort variants when available.
        let catalog: NativeModelListResult = match session
            .native_client
            .call(
                "model/list",
                serde_json::to_value(NativeModelListParams {
                    session_id: Some(session_id.clone()),
                })
                .map_err(|_| {
                    Error::new("internal", "Muse model/list params could not be encoded")
                })?,
                deadline,
            )
            .await
        {
            Ok(response) => match serde_json::from_value(response) {
                Ok(catalog) => catalog,
                Err(_) => {
                    return Ok(json!({
                        "state": "rejected",
                        "reason": "Muse model/list result did not match the installed schema; session/setModel was not dispatched",
                    }));
                }
            },
            Err(_) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Muse configure catalogue readback failed; session/setModel was not dispatched",
                }));
            }
        };
        let selection = ModelSelection::new(
            &requested_model,
            catalog
                .models
                .iter()
                .find(|model| model.model_id == requested_model),
        );
        if let Some(effort) = requested.effort.as_deref()
            && !selection.allows_effort(|entry| match entry.variants.as_ref() {
                Some(NativeModelEffortVariants::Known(variants)) => {
                    variants.iter().any(|variant| variant == effort)
                }
                _ => true,
            })
        {
            return Ok(json!({
                "state": "rejected",
                "reason": format!("Muse effort {effort} is not a variant of model {requested_model}"),
            }));
        }
        let model_command = new_command_id();
        let model_response = match session
            .native_client
            .call(
                "session/setModel",
                serde_json::to_value(NativeSessionSetModelParams {
                    command_id: model_command.clone(),
                    session_id: session_id.clone(),
                    model: NativeModelSelection {
                        model_id: selection.id.to_owned(),
                    },
                })
                .map_err(|_| {
                    Error::new(
                        "internal",
                        "Muse session/setModel params could not be encoded",
                    )
                })?,
                deadline,
            )
            .await
        {
            Ok(value) => value,
            Err(error) if error.code == "native_rejected" => {
                return Ok(with_model_catalogue_diagnostic(
                    json!({
                        "state": "rejected",
                        "reason": error.message,
                        "origin": FailureOrigin::Harness,
                        "origin_evidence": "Muse session/setModel returned a native error",
                        "where": "plugin.muse.configure.set_model",
                    }),
                    selection.catalogue_omits_model(),
                ));
            }
            Err(error) if native_no_write(&error.code) => {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Muse session/setModel was not dispatched",
                }));
            }
            Err(error) => {
                return Err(contextual_error(
                    error,
                    "unknown",
                    "Muse session/setModel may have changed native state; it will not be repeated",
                ));
            }
        };
        check_configure_model_ack(model_response, &model_command)?;
        // Verify the switch against the owned session before reporting it:
        // the ack admits the selection for subsequent model calls, and the
        // readback proves which model the session now carries.
        let reread = read_owned_session(&session.native_client, &session_id)
            .await
            .map_err(|error| {
                Error::new(
                    "unknown",
                    format!(
                        "Muse session/read did not prove the model switch ({})",
                        error.message
                    ),
                )
            })?;
        if reread.session.model_id.as_deref() != Some(requested_model.as_str()) {
            return Err(Error::new(
                "unknown",
                format!("Muse session/read did not prove the model switch to {requested_model}"),
            ));
        }
        session.selected_model = Some(requested_model.clone());
        Ok(with_model_catalogue_diagnostic(
            json!({
                "state": "applied",
                "model": requested_model,
                "model_verified": true,
            }),
            selection.catalogue_omits_model(),
        ))
    }

    /// Re-prove an idle send gate with a fresh exact `session/read` (k167b):
    /// the exact session, status `idle`, an explicitly null `activeTurnId`
    /// (#427), and a healthy history envelope. On proof the actor latch is
    /// cleared and re-read. Returns the fresh actor status only when every
    /// step proved; any failure is not idle, so the caller rejects instead
    /// of dispatching. All three reads share `proof_deadline`.
    async fn reprove_idle_send_gate(
        session: &mut AgentSession,
        proof_deadline: Instant,
    ) -> Option<EventStatus> {
        let owned = session.session_id.clone()?;
        if !session_read_proves_owned_idle(&session.native_client, &owned, proof_deadline).await {
            return None;
        }
        if !session
            .events
            .reestablish_idle_at(owned.clone(), proof_deadline)
            .await
            .unwrap_or(false)
        {
            return None;
        }
        session
            .events
            .view_readback_at(owned, None, proof_deadline)
            .await
            .ok()
    }

    async fn send_command(
        &mut self,
        params: AgentCommandParams,
        deadline: Instant,
    ) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Muse Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Muse",
        )?;
        if session.phase != SessionPhase::Ready {
            return Err(Error::new(
                "unknown",
                "Muse session or event source outcome is unresolved",
            ));
        }
        let text = params
            .args
            .get("text")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::new("invalid", "Muse send requires args.text"))?;
        if text.len() > MAX_TEXT_BYTES {
            return Err(Error::new("frame_too_large", "Muse input exceeds 512 KiB"));
        }
        // One request budget (k167b): every proof read below shares the
        // dispatch `deadline`, and proof reads stop at `proof_deadline`
        // so the typed rejection still returns before any native write.
        let proof_deadline = deadline
            .checked_sub(SEND_REJECTION_RESERVE)
            .unwrap_or(deadline);
        // Host-scoped MCP posture (E47): the owned host failed its MCP
        // startup audit, so MCP stays disabled for that host's lifetime.
        // Reject before any readback or dispatch; only a fresh host (an
        // Agent restart) clears the latch.
        if session.events.status().mcp_startup_audit_failed {
            return Ok(rejected_control(MCP_AUDIT_DISABLED_REJECTION));
        }
        // A fresh send has no native effect yet. Serialize this check behind
        // the event actor's queued facts/terminal commit, rather than using
        // its potentially older watch snapshot. A durable terminal recovered
        // after projection loss does not restore the live source.
        let session_id = session
            .session_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "Muse session identity is unresolved"))?;
        let mut event_status = match session
            .events
            .view_readback_at(session_id, None, proof_deadline)
            .await
        {
            Ok(status) => status,
            Err(_) => match Self::reprove_idle_send_gate(session, proof_deadline).await {
                Some(status) => status,
                None => {
                    return Ok(rejected_control(
                        "Muse native observation is unresolved; turn/start was not dispatched. Restart the Agent on the same worktree to continue.",
                    ));
                }
            },
        };
        // k167b: a readback-settled terminal keeps the send gate until the
        // owned session proves idle with a fresh `session/read`, even when
        // the actor readback itself succeeded.
        if event_status.needs_idle_proof {
            match Self::reprove_idle_send_gate(session, proof_deadline).await {
                Some(fresh)
                    if !fresh.needs_idle_proof
                        && fresh.failure.is_none()
                        && !fresh.view_unavailable =>
                {
                    event_status = fresh;
                }
                _ => {
                    return Ok(rejected_control(
                        "Muse native observation is unresolved after a terminal; turn/start was not dispatched. Restart the Agent on the same worktree to continue.",
                    ));
                }
            }
        }
        if let Some(reason) = event_status.failure.clone() {
            if !crate::events::is_view_related_failure(&reason) {
                return Ok(rejected_control(&format!(
                    "{reason}; turn/start was not dispatched. Restart the Agent on the same worktree to continue when the source stays unresolved."
                )));
            }
            match Self::reprove_idle_send_gate(session, proof_deadline).await {
                Some(fresh)
                    if fresh.failure.is_none()
                        && !fresh.view_unavailable
                        && !fresh.needs_idle_proof =>
                {
                    event_status = fresh;
                }
                _ => {
                    return Ok(rejected_control(&format!(
                        "{reason}; turn/start was not dispatched. Restart the Agent on the same worktree to continue when the source stays unresolved."
                    )));
                }
            }
        }
        if event_status.active_turn_id.is_some()
            && Self::reprove_idle_send_gate(session, proof_deadline)
                .await
                .is_none()
        {
            return Ok(rejected_control(
                "Muse could not prove the native session idle; turn/start was not dispatched.",
            ));
        }
        // Never dispatch once the rejection reserve is spent: without room
        // to return the outcome the caller settles `unknown` past a
        // dispatch it cannot observe.
        if Instant::now() >= proof_deadline {
            return Ok(rejected_control(
                "Muse send budget elapsed before turn/start; turn/start was not dispatched. Retry the send.",
            ));
        }
        let send_text = match &session.skill_pointer {
            Some(pointer) => format!("{pointer}\n\n{text}"),
            None => text.to_owned(),
        };
        let combined = with_claimed_context(&send_text, &params.context, "Muse")?;
        let command_id = params.command_id.clone();
        // Mint the caller-selected idempotency handle before the write: for a
        // fresh turn this derives the turn id, and the ack authoritatively
        // confirms it. A lost reply leaves the outcome unknown; the input is
        // never written a second time under another id.
        let native_command_id = new_command_id();
        // A retained `configure` effort overlay rides the per-turn field
        // the installed schema accepts; without one the frozen session
        // value applies unchanged. A model override never reaches this
        // path: the model switch runs live through `session/setModel` at
        // configure time, so the session already carries the retained
        // revision's model. The association reports that session-verified
        // model alongside the carried effort.
        let effort = params
            .effort_override
            .clone()
            .or_else(|| session.selected_effort.clone());
        // What the native turn request actually carries, reported through
        // the turn association only under a `configure` overlay; plain
        // sends keep their exact event shape.
        let sent_effort = params
            .config_revision
            .is_some()
            .then(|| effort.clone())
            .flatten();
        let turn_params = Self::turn_start_params(
            native_command_id.clone(),
            session
                .session_id
                .clone()
                .ok_or_else(|| Error::new("unknown", "Muse session identity is unresolved"))?,
            vec![NativeTurnInputPart {
                part_type: "text".to_owned(),
                text: Some(combined),
            }],
            effort.clone(),
        );
        let encoded = serde_json::to_value(turn_params)
            .map_err(|_| Error::new("internal", "Muse turn/start params could not be encoded"))?;
        // One request budget (k167b): the native write shares the dispatch
        // deadline instead of minting a fresh timeout past it.
        let response = session
            .native_client
            .call("turn/start", encoded, deadline)
            .await;
        let response = match response {
            Ok(value) => value,
            Err(error) if native_no_write(&error.code) => {
                return Ok(rejected_control("Muse turn/start was not dispatched"));
            }
            Err(error) if error.code == "native_rejected" => {
                return Ok(rejected_control("Muse rejected turn/start"));
            }
            Err(error) if error.code == "frame_too_large" => {
                return Ok(rejected_control(
                    "Muse turn/start frame exceeded the native transport bound before dispatch",
                ));
            }
            Err(error) => {
                session.phase = SessionPhase::Unknown;
                return Err(contextual_error(
                    error,
                    "unknown",
                    "Muse turn/start may have applied; the input will not be repeated",
                ));
            }
        };
        // The ack is authoritative: take the turn id from it, verify it echoes
        // the exact submitted command, and accept only a started or queued
        // disposition.
        let started: NativeTurnStartResult =
            match serde_json::from_value::<NativeTurnStartResult>(response) {
                Ok(started)
                    if started.command_id == native_command_id
                        && !started.turn_id.trim().is_empty()
                        && matches!(started.disposition.as_str(), "started" | "queued")
                        && started.status == "accepted" =>
                {
                    started
                }
                Ok(steered)
                    if steered.command_id == native_command_id
                        && steered.disposition.as_str() == "steered" =>
                {
                    // A steered disposition means this input joined a live turn
                    // it did not create. PR1 holds single-flight and cannot
                    // associate that input exactly, so the command and the
                    // session latch unknown; the input is never resubmitted.
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "Muse turn/start steered the input into a turn it did not create",
                    ));
                }
                _ => {
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "Muse turn/start returned no exact turn identity",
                    ));
                }
            };
        let turn_id = started.turn_id;
        session.current_turn = Some((command_id.clone(), turn_id.clone()));
        if session
            .events
            .associate_turn(
                command_id.clone(),
                turn_id.clone(),
                params.context.clone(),
                sent_effort.clone(),
                session.selected_model.clone(),
            )
            .await
            .is_err()
        {
            session.phase = SessionPhase::Unknown;
            return Err(Error::new(
                "unknown",
                "Muse turn was accepted but turn.started was not acknowledged",
            ));
        }
        // The first turn carrying the skill text is now accepted and
        // acknowledged: later turns send plain input.
        session.skill_pointer = None;
        // Echo what the native request carried when a `configure` overlay
        // applied, so the settled outcome reads back the effective values
        // alongside the native turn association. Unchanged without one.
        let mut result = json!({"state": "applied", "turn_id": turn_id});
        if let Some(revision) = params.config_revision {
            result["config_revision"] = json!(revision);
            result["effort"] = json!(effort);
        }
        Ok(result)
    }

    async fn steer_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Muse Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Muse",
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
            .ok_or_else(|| Error::new("invalid", "Muse steer requires args.text"))?;
        if params.verb != "inject" && text.len() > MAX_TEXT_BYTES {
            return Ok(rejected_control("Muse steer input exceeds 512 KiB"));
        }
        if session.secrets.iter().any(|secret| text.contains(secret)) {
            return Err(Error::new(
                "secret_in_payload",
                "Muse steer contains a known credential",
            ));
        }
        if session.phase != SessionPhase::Ready || session.events.status().source_unknown {
            let result = rejected_control(
                "Muse steer refused before dispatch: native source is unavailable; no turn/steer was sent",
            );
            return Ok(result);
        }
        let Some(native_session) = session.session_id.clone() else {
            let result = rejected_control(
                "Muse steer refused before dispatch: session identity is unresolved",
            );
            return Ok(result);
        };
        // A healthy local turn binding cannot prove that Muse can still read
        // its origin log. Long-log origin failures can be admitted by steer
        // and then fail the native turn. Probe the read-only lane first; a
        // failed probe has no input effect and must never be sent as a steer.
        let readback = session
            .native_client
            .call(
                "session/read",
                serde_json::to_value(NativeSessionReadParams {
                    session_id: native_session.clone(),
                    exclude_items: Some(true),
                })
                .map_err(|_| {
                    Error::new("internal", "Muse steer read params could not be encoded")
                })?,
                Instant::now() + STEER_READ_DEADLINE,
            )
            .await;
        if let Ok(value) = &readback
            && let Ok(read) = serde_json::from_value::<NativeSessionReadResult>(value.clone())
            && read.session.session_id == native_session
            && let Some(reason) = read.history.unavailable_reason()
        {
            let _ = session
                .events
                .view_readback(native_session.clone(), Some(reason))
                .await;
        }
        if let Err(error) = validate_steer_readback(readback, &native_session, turn_id) {
            let result = rejected_control(&error.message);
            return Ok(result);
        }
        // The turn itself is gated authoritatively: the event task owns the
        // active turn and answers behind the terminal commit, so a steer
        // that arrives after the terminal batch never writes to a dead turn.
        let live = match session.events.check_turn(turn_id).await {
            Ok(live) => live,
            Err(error) => {
                let result = unknown_control(&error.message);
                return Ok(result);
            }
        };
        if !live {
            return Ok(rejected_control(
                "Muse turn/steer does not match the exact active turn",
            ));
        }
        if session.events.status().source_unknown {
            let result = rejected_control(
                "Muse steer refused before dispatch: native source became unavailable during readback",
            );
            return Ok(result);
        }
        // `expectedTurnId` always carries the exact active turn: input meant
        // for turn A can never leak into turn B, and the ack must confirm the
        // same turn absorbed it.
        let native_command_id = new_command_id();
        let steer_params = NativeTurnSteerParams {
            command_id: native_command_id.clone(),
            expected_turn_id: turn_id.to_owned(),
            input: vec![NativeTurnInputPart {
                part_type: "text".to_owned(),
                text: Some(text.to_owned()),
            }],
            session_id: native_session,
        };
        let encoded = match serde_json::to_value(steer_params) {
            Ok(encoded) => encoded,
            Err(_) => {
                let result = unknown_control("Muse turn/steer params could not be encoded");
                return Ok(result);
            }
        };
        let response = session
            .native_client
            .call("turn/steer", encoded, Instant::now() + RPC_TIMEOUT)
            .await;
        match response {
            Err(error)
                if native_no_write(&error.code)
                    || error.code == "native_rejected"
                    || error.code == "frame_too_large" =>
            {
                let mut result = rejected_control("Muse turn/steer was not applied");
                eliot_plugin::session::attach_failure_origin(
                    &mut result,
                    "muse",
                    error.origin_or_unknown(),
                    error
                        .origin_evidence
                        .as_deref()
                        .unwrap_or("native refusal lacks an origin signal"),
                    &eliot_core::SecretPolicy::new(session.secrets.iter()),
                );
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Muse turn/steer outcome is unknown");
                Ok(result)
            }
            Ok(value) => {
                let steered: NativeTurnSteerResult = match serde_json::from_value(value) {
                    Ok(steered) => steered,
                    Err(_) => {
                        let mut result = unknown_control("Muse turn/steer response was invalid");
                        eliot_plugin::session::attach_failure_origin(
                            &mut result,
                            "muse",
                            eliot_core::FailureOrigin::HarnessContractMismatch,
                            "docs/harnesses/muse.md#native-mapping: turn/steer response violates the installed result schema",
                            &eliot_core::SecretPolicy::new(session.secrets.iter()),
                        );
                        return Ok(result);
                    }
                };
                if steered.command_id != native_command_id
                    || steered.status != "accepted"
                    || steered.turn_id != turn_id
                {
                    let mut result =
                        unknown_control("Muse turn/steer was not absorbed by its turn");
                    eliot_plugin::session::attach_failure_origin(
                        &mut result,
                        "muse",
                        eliot_core::FailureOrigin::HarnessContractMismatch,
                        "docs/harnesses/muse.md#native-mapping: turn/steer result contradicts the submitted command, expected turn or accepted status",
                        &eliot_core::SecretPolicy::new(session.secrets.iter()),
                    );
                    return Ok(result);
                }
                let result = json!({
                    "state": "applied",
                    "command_id": params.command_id,
                    "turn_id": turn_id,
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
                "Muse inject point is not available on the active turn",
            ));
        }
        let consumer_id = params.command_id.clone();
        let mut result = self.steer_command(params).await?;
        if result.get("state").and_then(Value::as_str) == Some("applied") {
            result["context_outcome"] = serde_json::to_value(ContextOutcome {
                mechanism: "muse.turn/steer".to_owned(),
                application_point: InjectPoint::ActiveTurn,
                consumer_id,
            })
            .map_err(|_| Error::new("internal", "context outcome could not be encoded"))?;
        }
        Ok(result)
    }

    async fn interrupt_command(&mut self, params: AgentCommandParams) -> Result<Value> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("bad_state", "Muse Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Muse",
        )?;
        let turn_id = params
            .turn_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Error::new("stale_turn", "turn/cancel requires a captured turn id"))?;
        if params.request_id.is_some() {
            return Err(Error::new(
                "invalid",
                "turn/cancel does not accept request_id",
            ));
        }
        if session.phase == SessionPhase::Stopped {
            return Err(Error::new(
                "bad_state",
                "Muse plugin is stopping; no further input is accepted",
            ));
        }
        let event_status = session.events.status();
        if session.phase != SessionPhase::Ready || event_status.source_unknown {
            let reason = if event_status.view_unavailable {
                format!(
                    "Muse interrupt refused: native view unavailable ({}); the recorded source gap leaves the turn outcome unknown",
                    event_status
                        .view_unavailable_reason
                        .unwrap_or(crate::native::types::NativeHistoryNoneReason::Unknown)
                        .label(),
                )
            } else {
                "Muse interrupt does not match a healthy active turn".to_owned()
            };
            let result = rejected_control(&reason);
            return Ok(result);
        }
        // The turn itself is gated authoritatively: the event task owns the
        // active turn and answers behind the terminal commit, so an interrupt
        // that arrives after the terminal batch sees `stale_turn` instead of
        // sending a second native cancel.
        let live = match session.events.check_turn(turn_id).await {
            Ok(live) => live,
            Err(error) => {
                let result = unknown_control(&error.message);
                return Ok(result);
            }
        };
        if !live {
            return Ok(rejected_control(
                "Muse turn/cancel does not match the exact active turn",
            ));
        }
        let Some(native_session) = session.session_id.clone() else {
            let result = unknown_control("Muse session identity is unresolved");
            return Ok(result);
        };
        // The exact active turn is always named explicitly: halt running,
        // keep queue — queued submits are a different object (`turn/unqueue`)
        // and are never touched here. Admission is not the stop: the turn
        // ends when its `turn/completed` with terminal `cancelled` folds on
        // the stream.
        let native_command_id = new_command_id();
        let cancel_params = NativeTurnCancelParams {
            command_id: native_command_id.clone(),
            session_id: native_session,
            turn_id: Some(turn_id.to_owned()),
        };
        let encoded = match serde_json::to_value(cancel_params) {
            Ok(encoded) => encoded,
            Err(_) => {
                let result = unknown_control("Muse turn/cancel params could not be encoded");
                return Ok(result);
            }
        };
        let response = session
            .native_client
            .call("turn/cancel", encoded, Instant::now() + RPC_TIMEOUT)
            .await;
        match response {
            Err(error)
                if native_no_write(&error.code)
                    || error.code == "native_rejected"
                    || error.code == "frame_too_large" =>
            {
                let result = rejected_control("Muse turn/cancel was not applied");
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Muse turn/cancel outcome is unknown");
                Ok(result)
            }
            Ok(value) => {
                let cancelled: NativeTurnCancelResult = match serde_json::from_value(value) {
                    Ok(cancelled) => cancelled,
                    Err(_) => {
                        let result = unknown_control("Muse turn/cancel response was invalid");
                        return Ok(result);
                    }
                };
                if cancelled.command_id != native_command_id
                    || cancelled.status != "accepted"
                    || cancelled.turn_id != turn_id
                {
                    let result = unknown_control("Muse turn/cancel did not stop its exact turn");
                    return Ok(result);
                }
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
            .ok_or_else(|| Error::new("bad_state", "Muse Agent session has not started"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Muse",
        )?;
        let (turn_id, request_id) = request_reply_ids(&params, "Muse")?;
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
                let result = rejected_control("Muse request reply was not dispatched");
                Ok(result)
            }
            Err(error) if error.code == "unknown" => {
                Ok(unknown_control("Muse request reply outcome is unknown"))
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
        let params: AgentSnapshotParams = decode(raw, "Muse agent.snapshot params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Muse",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Muse Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Muse",
        )?;
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
                "turn_evidence": null,
                "batch_unknown": event_status.batch_unknown,
                "source_unknown": event_status.source_unknown,
            }));
        }
        let event_status = session.events.status();
        // Store binds an explicit turn. The one current native binding
        // covers readback if the association acknowledgement was lost.
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
            return unknown_snapshot(&params, "Muse command identity is not retained");
        }
        if session.phase == SessionPhase::Stopped {
            return unknown_snapshot(
                &params,
                "Muse native process is stopped; native readback is unavailable",
            );
        }
        let Some(session_id) = session
            .session_id
            .as_deref()
            .or(event_status.session_id.as_deref())
        else {
            return unknown_snapshot(&params, "Muse session identity was not acknowledged");
        };
        read_live_snapshot(session, &params, session_id, turn_id.as_deref()).await
    }

    async fn stop(&mut self, raw: &Value, deadline: Instant) -> Result<Value> {
        let params: AgentStopParams = decode(raw, "Muse agent.stop params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Muse",
        )?;
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("not_found", "Muse Agent session is not retained"))?;
        validate_binding(
            &session.agent_id,
            session.generation,
            &params.agent_id,
            params.generation,
            "Muse",
        )?;
        // Latch once before native I/O. The router owns interrupt/wait and
        // daemon force custody; this adapter only performs the graceful
        // native close and verifies the event/source drain.
        if begin_stop(
            &mut session.stop_command_id,
            session.phase == SessionPhase::Stopped,
            &params.command_id,
            "Muse",
        )? {
            return Ok(json!({"state": "applied"}));
        }
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| Error::new("unknown", "stop lost its retained native session"))?;
        terminate_session(session, false, deadline).await?;
        Ok(json!({"state": "applied"}))
    }

    async fn shutdown_paths(&mut self, best_effort: bool) -> Result<Value> {
        shutdown_session(
            self.session.as_mut(),
            best_effort,
            async |session| {
                if session.phase != SessionPhase::Stopped {
                    terminate_session(session, true, Instant::now() + RPC_TIMEOUT).await
                } else {
                    Ok(())
                }
            },
            async |session| session.events.close().await,
        )
        .await
    }

    async fn shutdown(&mut self) -> Result<Value> {
        self.shutdown_paths(false).await
    }

    async fn shutdown_owned(&mut self) {
        let _ = self.shutdown_paths(true).await;
    }
}

/// Endpoint-scoped native exit text for the snapshot readback: the confirmed
/// exit code plus the bounded redacted harness stderr tail when it says
/// anything. Never a turn verdict; an unknown turn next to this text names
/// the crash that ended observation.
fn native_exit_text(
    status: &std::process::ExitStatus,
    stderr: &StderrSnapshot,
    secrets: &[String],
) -> String {
    const MAX_TAIL_BYTES: usize = 2 * 1024;
    let mut text = match status.code() {
        Some(code) => format!("Muse native exited (code {code})"),
        None => "Muse native exited (code unknown)".to_owned(),
    };
    let policy = SecretPolicy::new(secrets.iter().cloned());
    let tail = policy.redact_text(stderr.text.trim());
    if !tail.is_empty() {
        let mut end = tail.len().min(MAX_TAIL_BYTES);
        while !tail.is_char_boundary(end) {
            end -= 1;
        }
        text.push_str("; stderr tail: ");
        text.push_str(&tail[..end]);
        if tail.len() > MAX_TAIL_BYTES || stderr.truncated {
            text.push('\u{2026}');
        }
    }
    text
}

async fn terminate_session(
    session: &mut AgentSession,
    force: bool,
    deadline: Instant,
) -> Result<()> {
    let settlement_deadline = deadline - STOP_RESPONSE_RESERVE;
    let native_deadline = settlement_deadline - STOP_SETTLEMENT_RESERVE;
    let result = {
        let events = &session.events;
        let native = &mut session.native;
        let begin = events.begin_stop();
        let native = async move {
            let process_exit = if force {
                native
                    .transport
                    .terminate_dropping_input(true, native_deadline)
                    .await
            } else {
                native
                    .transport
                    .close_gracefully(native_deadline, true)
                    .await
            };
            let drained = native.await_output_drained(native_deadline).await;
            (process_exit, drained)
        };
        let end = events.end_stop();
        stop_owned_session(
            "Muse",
            Some((native_deadline, settlement_deadline)),
            begin,
            native,
            end,
        )
        .await
    };
    session.phase = if result.is_ok() {
        SessionPhase::Stopped
    } else {
        SessionPhase::Unknown
    };
    result
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

async fn discard_failed_start(session: &mut AgentSession, error: Error) -> Error {
    discard_unstarted(session).await;
    session.native_client.diagnostic_error(error)
}

/// A `configure` model and/or effort request. Admission already gated the
/// fields against `ConfigureSupport`; this re-checks shape so a malformed
/// dispatch refuses before any native write.
#[derive(Debug)]
struct ConfigureRequest {
    model: Option<String>,
    effort: Option<String>,
}

fn parse_configure_request(args: &Value) -> Result<ConfigureRequest> {
    let model = args
        .get("model")
        .and_then(Value::as_str)
        .filter(|model| !model.trim().is_empty())
        .map(str::to_owned);
    let effort = args
        .get("effort")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let Some(effort) = effort.as_deref()
        && !is_reasoning_effort_tier(effort)
    {
        return Err(Error::new(
            "invalid",
            format!("Muse effort {effort} is not a supported reasoning tier"),
        ));
    }
    if model.is_none() && effort.is_none() {
        return Err(Error::new(
            "invalid",
            "Muse configure needs a model or an effort",
        ));
    }
    Ok(ConfigureRequest { model, effort })
}

impl MusePlugin {
    /// Read-only native model catalogue for `harness.models`. A dedicated
    /// reader instance queries `model/list` on a transient `muse serve`
    /// and refreshes only an empty snapshot. No session, no turn, no input
    /// and no inference. The native `isDefault` marker (when the installed
    /// host supplies it) projects as the catalogue default; absent or
    /// all-false stays unknown and is never invented by list order.
    async fn model_list(&self, raw: &Value) -> Result<Value> {
        let snapshot = model_catalogue_profile(
            raw,
            self.session.is_some() || self.failed_start.is_some(),
            "muse",
            "Muse",
            false,
        )?;
        let program =
            required_native_cmd(snapshot, "Muse model.list requires a discovered native cmd")?;
        let cwd = std::env::current_dir()
            .map_err(|_| Error::new("internal", "Muse model.list could not read its cwd"))?;
        let initialized = self
            .initialized
            .as_ref()
            .ok_or_else(|| not_initialized("Muse"))?;
        let environment = target_environment(
            raw.get("target_environment"),
            snapshot.get("pass_env"),
            "Muse",
        )?;
        let launch = NativeLaunch {
            program: OsString::from(program),
            args: vec![OsString::from("serve")],
            cwd,
            environment: environment.variables,
        };
        let policy = SecretPolicy::new(environment.secrets.iter().chain([&initialized.credential]));
        let mut host =
            catalogue::CatalogueHost::open(&launch, Path::new(&initialized.instance_dir), &policy)
                .await?;
        host.close().await?;
        if host.catalogue.models.is_empty() && host.refresh_failed() {
            return Err(host.catalogue_error(Error::new(
                "native_model_unavailable",
                "Muse model/list still returned an empty catalogue",
            )));
        }
        let (models, default_id) = project_model_catalog(host.catalogue.models);
        Ok(json!({"models": models, "default": default_id}))
    }

    /// Read-only native subscription usage for `harness.quota`. A dedicated
    /// reader instance spawns a transient `muse serve`, runs initialize
    /// plus `usage/read`, then terminates. No session, no turn, no input
    /// and no inference. Only quota signals are projected into `details`;
    /// `account/read` is never called, so no email, label or avatar ever
    /// enters the response, logs or Store.
    async fn quota_read(&self, raw: &Value) -> Result<Value> {
        ensure_dedicated_reader(
            self.session.is_some() || self.failed_start.is_some(),
            "Muse",
            "quota.read",
        )?;
        let params = eliot_plugin::quota::decode(raw)?;
        let snapshot = &params.profile;
        ensure_reader_harness(snapshot, "muse", "Muse", "quota.read")?;
        let program =
            required_native_cmd(snapshot, "Muse quota.read requires a discovered native cmd")?;
        let cwd = std::env::current_dir()
            .map_err(|_| Error::new("internal", "Muse quota.read could not read its cwd"))?;
        let deadline = Instant::now() + RPC_TIMEOUT;
        let mut native = NativeProcess::spawn(
            NativeLaunch {
                program: OsString::from(program),
                args: vec![OsString::from("serve")],
                cwd,
                environment: params
                    .environment
                    .as_object()
                    .into_iter()
                    .flatten()
                    .filter_map(|(key, value)| {
                        value
                            .as_str()
                            .map(|value| (OsString::from(key), OsString::from(value)))
                    })
                    .collect(),
            },
            SecretPolicy::new(
                params
                    .environment
                    .as_object()
                    .into_iter()
                    .flatten()
                    .filter_map(|(_, value)| value.as_str()),
            ),
        )
        .await?;
        let client = native.client();
        let result: Result<Value> = async {
            use crate::native::types::{
                NativeClientCapabilities, NativeClientInfo, NativeInitializeParams,
                NativeInitializeResult, NativeUsageReadParams, NativeUsageReadResult,
            };
            let init = NativeInitializeParams {
                client_info: NativeClientInfo {
                    name: "eliot_quota".to_owned(),
                    version: env!("CARGO_PKG_VERSION").to_owned(),
                    title: Some("Eliot quota".to_owned()),
                },
                capabilities: Some(NativeClientCapabilities {
                    user_input_dialogs: Some(true),
                }),
            };
            let init_value = serde_json::to_value(init).map_err(|_| {
                Error::new(
                    "internal",
                    "Muse quota.read initialize params could not be encoded",
                )
            })?;
            let _initialized: NativeInitializeResult = serde_json::from_value(
                client
                    .call("initialize", init_value, deadline)
                    .await
                    .map_err(|error| {
                        let detail = error.message.clone();
                        error.with_message(format!("Muse quota.read initialize failed: {detail}"))
                    })?,
            )
            .map_err(|_| {
                Error::new(
                    "profile_error",
                    "Muse quota.read initialize response was invalid",
                )
            })?;
            client
                .notify("initialized", None, deadline)
                .await
                .map_err(|error| {
                    let detail = error.message.clone();
                    error.with_message(format!(
                        "Muse quota.read initialized notify failed: {detail}"
                    ))
                })?;
            let response = client
                .call(
                    "usage/read",
                    serde_json::to_value(NativeUsageReadParams {}).map_err(|_| {
                        Error::new("internal", "Muse quota.read params could not be encoded")
                    })?,
                    deadline,
                )
                .await
                .map_err(quota_read_error)?;
            let read: NativeUsageReadResult = serde_json::from_value(response).map_err(|_| {
                Error::new(
                    "profile_error",
                    "Muse usage/read result did not match the installed schema",
                )
            })?;
            let details = match read.usage {
                Some(usage) => {
                    if usage.tier.trim().is_empty()
                        || usage.tier.len() > 128
                        || usage.tier.chars().any(char::is_control)
                    {
                        return Err(Error::new(
                            "profile_error",
                            "Muse usage/read tier did not match the installed schema",
                        ));
                    }
                    for used in [usage.window.used_percent, usage.weekly.used_percent] {
                        validate_usage_read_percent(used)?;
                    }
                    if usage.window.window_duration_mins <= 0
                        || usage.window.resets_at_ms < 0
                        || usage.weekly.resets_at_ms < 0
                        || usage.observed_at_ms < 0
                    {
                        return Err(Error::new(
                            "profile_error",
                            "Muse usage/read stamps did not match the installed schema",
                        ));
                    }
                    serde_json::to_value(&usage).map_err(|_| {
                        Error::new("internal", "Muse quota.read details could not be encoded")
                    })?
                }
                // Truthful absence: the host observed nothing. Supported
                // with an empty details object and a naming note; no numbers
                // are fabricated.
                None => serde_json::json!({}),
            };
            Ok(json!({"details": if details.as_object().is_some_and(|o|o.is_empty()) { details } else {json!({"usage":details})}}))
        }
        .await;
        let result = result.map_err(|error| client.diagnostic_error(error));
        let _ = native.terminate(false, Instant::now() + RPC_TIMEOUT).await;
        result
    }
}

async fn read_live_snapshot(
    session: &AgentSession,
    params: &AgentSnapshotParams,
    session_id: &str,
    turn_id: Option<&str>,
) -> Result<Value> {
    let raw = match read_owned_session_value(
        &session.native_client,
        session_id,
        Instant::now() + RPC_TIMEOUT,
    )
    .await
    {
        Ok(raw) => raw,
        Err(_) => {
            return view_unknown_snapshot(
                session,
                params,
                "Muse session/read did not prove the exact owned session",
            );
        }
    };
    let explicit_idle = raw.pointer("/session/activeTurnId") == Some(&Value::Null);
    let readback: NativeSessionReadResult = serde_json::from_value(raw)
        .map_err(|_| Error::new("native_readback", "Muse session/read response was invalid"))?;
    let last_turn = if let Some(turn_id) = turn_id {
        let exact_running = readback.session.status == "running"
            && readback.session.active_turn_id.as_deref() == Some(turn_id);
        if !(exact_running || (readback.session.status == "idle" && explicit_idle)) {
            return view_unknown_snapshot(
                session,
                params,
                "Muse session/read did not prove the exact turn was idle or active",
            );
        }
        match read_owned_turn(
            &session.native_client,
            session_id,
            turn_id,
            &params.command_id,
            readback.session.active_turn_id.as_deref(),
            &session.secrets,
        )
        .await
        {
            Ok(Some(facts)) => Some(facts),
            _ => {
                return view_unknown_snapshot(
                    session,
                    params,
                    "Muse readback did not prove the exact requested turn",
                );
            }
        }
    } else {
        None
    };
    let event_status = session.events.status();
    let result_known = last_turn
        .as_ref()
        .is_some_and(|turn| turn.final_text.is_some());
    let turn_evidence = last_turn.as_ref().map(|turn| {
        if turn.terminal {
            "session/read+view/page"
        } else {
            "session/read"
        }
    });
    Ok(json!({
        "agent_id": params.agent_id, "generation": params.generation,
        "command_id": params.command_id, "state": session_state(&readback.session),
        "native_scope": session.native_scope, "native_ref": session_id,
        "native_model": readback.session.model_id,
        "active_turn": readback.session.active_turn_id,
        "last_turn": last_turn, "turn_evidence": turn_evidence,
        "native_exit": event_status.native_exit,
        "batch_unknown": event_status.batch_unknown,
        "source_unknown": event_status.source_unknown || readback.history.unavailable_reason().is_some(),
        "result_known": result_known,
    }))
}

fn view_unknown_snapshot(
    session: &AgentSession,
    params: &AgentSnapshotParams,
    reason: &str,
) -> Result<Value> {
    let mut snapshot = unknown_snapshot(params, reason)?;
    snapshot["native_scope"] = json!(session.native_scope);
    snapshot["native_ref"] = json!(session.session_id);
    snapshot["active_turn"] = json!(session.events.status().active_turn_id);
    snapshot["turn_evidence"] = Value::Null;
    snapshot["native_exit"] = json!(session.events.status().native_exit);
    snapshot["batch_unknown"] = json!(session.events.status().batch_unknown);
    Ok(snapshot)
}

/// Fresh exact idle proof for the send gate (k167b): one raw
/// `session/read`, checked with #427's exact rule (exact session, status
/// `idle`, explicitly null `activeTurnId`, healthy history). Any failure —
/// transport, schema, or unproven state — is not idle: the caller rejects
/// instead of dispatching.
async fn session_read_proves_owned_idle(
    client: &NativeClient,
    session_id: &str,
    deadline: Instant,
) -> bool {
    if Instant::now() >= deadline {
        return false;
    }
    match read_owned_session_value(client, session_id, deadline).await {
        Ok(value) => crate::events::session_read_proves_idle(&value, session_id),
        Err(_) => false,
    }
}

async fn read_owned_session_value(
    client: &NativeClient,
    session_id: &str,
    deadline: Instant,
) -> Result<Value> {
    let value = client
        .call(
            "session/read",
            serde_json::to_value(NativeSessionReadParams {
                session_id: session_id.to_owned(),
                exclude_items: Some(true),
            })
            .map_err(|_| Error::new("internal", "Muse session/read params could not be encoded"))?,
            deadline,
        )
        .await?;
    let owned = value
        .get("session")
        .and_then(|session| session.get("sessionId"))
        .and_then(Value::as_str);
    if owned != Some(session_id) {
        return Err(Error::new(
            "source_integrity",
            "Muse session/read returned another native session",
        ));
    }
    Ok(value)
}

async fn read_owned_session_at(
    client: &NativeClient,
    session_id: &str,
    deadline: Instant,
) -> Result<NativeSessionReadResult> {
    let value = read_owned_session_value(client, session_id, deadline).await?;
    serde_json::from_value(value)
        .map_err(|_| Error::new("native_readback", "Muse session/read response was invalid"))
}

async fn read_owned_session(
    client: &NativeClient,
    session_id: &str,
) -> Result<NativeSessionReadResult> {
    read_owned_session_at(client, session_id, Instant::now() + RPC_TIMEOUT).await
}

/// One backward `view/page` fetch, newest records first. Every durable-view
/// scan in this file pages through this helper; callers fold their own
/// records from the page and loop on `next_cursor` when one page cannot
/// prove enough.
async fn read_backward_page(
    client: &NativeClient,
    session_id: &str,
    cursor: Option<String>,
) -> Result<NativeViewPageResult> {
    let params = NativeViewPageParams {
        session_id: session_id.to_owned(),
        limit: VIEW_PAGE_SIZE,
        direction: Some("backward".to_owned()),
        cursor,
    };
    serde_json::from_value(
        client
            .call(
                "view/page",
                serde_json::to_value(params).map_err(|_| {
                    Error::new("internal", "Muse view/page params could not be encoded")
                })?,
                Instant::now() + RPC_TIMEOUT,
            )
            .await?,
    )
    .map_err(|_| Error::new("native_readback", "Muse view/page response was invalid"))
}

fn check_configure_model_ack(response: Value, command_id: &str) -> Result<()> {
    let ack: NativeSessionSetModelResult = serde_json::from_value(response).map_err(|_| {
        Error::new(
            "unknown",
            "Muse session/setModel ack did not match the installed schema",
        )
    })?;
    if ack.command_id != command_id || ack.status != "accepted" {
        return Err(Error::new(
            "unknown",
            "Muse session/setModel did not acknowledge the exact submitted command",
        ));
    }
    Ok(())
}

/// Read back the standing session-default reasoning effort: the newest
/// `session/reasoningEffortChanged` view event for the exact session. The set
/// call's ack proves admission only, so the start path compares this readback
/// against the requested tier before readiness.
async fn read_effective_reasoning_effort(
    client: &NativeClient,
    session_id: &str,
) -> Result<Option<String>> {
    // The set just landed at the view head, so one backward page from the
    // head holds it; anything else writing here would be a foreign host.
    let response = read_backward_page(client, session_id, None).await?;
    for event in response.events.iter().rev() {
        if event.method.as_str() != "session/reasoningEffortChanged" {
            continue;
        }
        let changed: NativeSessionReasoningEffortChangedNotification =
            serde_json::from_value(event.params.clone()).map_err(|_| {
                Error::new(
                    "native_readback",
                    "Muse view session/reasoningEffortChanged did not match the 1.4 schema",
                )
            })?;
        if changed.session_id != session_id {
            continue;
        }
        return Ok(Some(changed.reasoning_effort));
    }
    Ok(None)
}

async fn verify_resume_preflight(
    client: &NativeClient,
    session_id: &str,
    retained_turn: Option<&str>,
) -> Result<()> {
    let readback = read_owned_session(client, session_id).await?;
    if let Some(reason) = readback.history.unavailable_reason() {
        return Err(Error::new(
            "native_readback",
            format!("Muse retained view is unavailable ({})", reason.label()),
        ));
    }
    let session = readback.session;
    // The newest turn lives at the view head, so one backward page proves
    // it; the retained no-turn case needs no scan because the session turn
    // counter proves absence exactly.
    let newest_turn = match retained_turn {
        Some(_) => read_latest_native_turn_status(client, session_id).await?,
        None => None,
    };
    validate_resume_preflight(
        &session,
        retained_turn,
        newest_turn
            .as_ref()
            .map(|(id, status)| (id.as_str(), status.as_str())),
    )
}

/// Accepting `idle`/`notLoaded` here is NOT proof the generation-1 host is
/// gone (muse.md: "`notLoaded` alone does not prove session closure"). Eliot
/// guarantees the old generation is terminal before this plugin ever runs:
/// the Store admits resume only for a Stopped/Failed/Lost generation with no
/// active turn (`crates/eliot-store/src/admission.rs`, resume arm), rechecks
/// that gate at dispatch (`crates/eliot-store/src/dispatch.rs`, resume arm),
/// and refuses while a stop is still in flight. The preflight only checks the
/// retained session is resumable by this new generation.
fn validate_resume_preflight(
    session: &NativeSession,
    retained_turn: Option<&str>,
    newest_turn: Option<(&str, &str)>,
) -> Result<()> {
    if !matches!(session.status.as_str(), "idle" | "notLoaded") {
        return Err(Error::new(
            "native_readback",
            "Muse retained session is active or has no resumable runtime status",
        ));
    }
    if session
        .active_turn_id
        .as_deref()
        .is_some_and(|turn| !turn.trim().is_empty())
    {
        return Err(Error::new(
            "native_readback",
            "Muse retained session still has an active turn",
        ));
    }
    match (retained_turn, newest_turn) {
        (Some(expected), Some((actual, status)))
            if actual == expected && terminal_turn_status(status) =>
        {
            Ok(())
        }
        (None, None) if session.turn_count == Some(0) => Ok(()),
        (None, None) => Err(Error::new(
            "native_readback",
            "Muse cannot prove the retained session ran no native turns",
        )),
        _ => Err(Error::new(
            "native_readback",
            "Muse newest native turn does not match the exact retained terminal turn",
        )),
    }
}

/// Newest native turn as `(turn_id, terminal status)` from one backward
/// view page, newest record first. A turn whose newest record is its start
/// reports `inProgress`, which never satisfies the terminal match.
async fn read_latest_native_turn_status(
    client: &NativeClient,
    session_id: &str,
) -> Result<Option<(String, String)>> {
    let response = read_backward_page(client, session_id, None).await?;
    for event in response.events.iter().rev() {
        match event.method.as_str() {
            "turn/completed" => {
                let completed: NativeTurnCompletedNotification =
                    serde_json::from_value(event.params.clone()).map_err(|_| {
                        Error::new(
                            "native_readback",
                            "Muse view turn/completed did not match the 1.4 schema",
                        )
                    })?;
                if completed.session_id != session_id {
                    continue;
                }
                let terminal = match completed.terminal {
                    NativeTurnTerminal::Completed => "completed",
                    NativeTurnTerminal::Failed => "failed",
                    NativeTurnTerminal::Cancelled => "cancelled",
                    NativeTurnTerminal::Unknown => {
                        return Err(Error::new(
                            "native_readback",
                            "Muse view turn/completed carried an unknown terminal",
                        ));
                    }
                };
                return Ok(Some((completed.turn_id, terminal.to_owned())));
            }
            "turn/started" => {
                let started: NativeTurnStartedNotification =
                    serde_json::from_value(event.params.clone()).map_err(|_| {
                        Error::new(
                            "native_readback",
                            "Muse view turn/started did not match the 1.4 schema",
                        )
                    })?;
                if started.session_id != session_id {
                    continue;
                }
                return Ok(Some((started.turn_id, "inProgress".to_owned())));
            }
            _ => {}
        }
    }
    Ok(None)
}

fn terminal_turn_status(status: &str) -> bool {
    matches!(status, "completed" | "failed" | "cancelled")
}

async fn read_owned_turn(
    client: &NativeClient,
    session_id: &str,
    turn_id: &str,
    command_id: &str,
    active_turn_id: Option<&str>,
    secrets: &[String],
) -> Result<Option<TurnFacts>> {
    if active_turn_id == Some(turn_id) {
        return Ok(Some(TurnFacts {
            command_id: command_id.to_owned(),
            turn_id: turn_id.to_owned(),
            status: Some("inProgress".to_owned()),
            final_text: None,
            native_error: None,
            terminal: false,
        }));
    }
    // The turn is not the live foreground turn. Prove its terminal state,
    // native failure text and final text from the durable-sourced view
    // records, newest first.
    let policy = SecretPolicy::new(secrets.iter().cloned());
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut terminal: Option<String> = None;
    let mut final_text: Option<String> = None;
    let mut native_error: Option<String> = None;
    for _ in 0..MAX_VIEW_PAGES {
        let response = read_backward_page(client, session_id, cursor.clone()).await?;
        for event in response.events.iter().rev() {
            observe_view_event(
                event,
                session_id,
                turn_id,
                &policy,
                &mut terminal,
                &mut final_text,
                &mut native_error,
            )?;
            if terminal.is_some() && final_text.is_some() {
                break;
            }
        }
        if terminal.is_some() && final_text.is_some() {
            break;
        }
        match response.next_cursor {
            Some(next) if !next.is_empty() && seen_cursors.insert(next.clone()) => {
                cursor = Some(next);
            }
            Some(_) => {
                return Err(Error::new("native_readback", "Muse view cursor repeated"));
            }
            None => break,
        }
    }
    let Some(status) = terminal else {
        return Ok(None);
    };
    Ok(Some(TurnFacts {
        command_id: command_id.to_owned(),
        turn_id: turn_id.to_owned(),
        status: Some(status),
        final_text,
        native_error,
        terminal: true,
    }))
}

/// Fold one durable-sourced view record into the turn readback. Records for
/// other sessions or turns are skipped; a malformed record for the exact
/// requested turn fails the readback instead of guessing.
fn observe_view_event(
    event: &NativeUnframedViewEvent,
    session_id: &str,
    turn_id: &str,
    policy: &SecretPolicy,
    terminal: &mut Option<String>,
    final_text: &mut Option<String>,
    native_error: &mut Option<String>,
) -> Result<()> {
    match event.method.as_str() {
        "turn/completed" => {
            let completed: NativeTurnCompletedNotification =
                serde_json::from_value(event.params.clone()).map_err(|_| {
                    Error::new(
                        "native_readback",
                        "Muse view turn/completed did not match the 1.4 schema",
                    )
                })?;
            if completed.session_id != session_id || completed.turn_id != turn_id {
                return Ok(());
            }
            if terminal.is_none() {
                *terminal = Some(match completed.terminal {
                    NativeTurnTerminal::Completed => "completed".to_owned(),
                    NativeTurnTerminal::Failed => "failed".to_owned(),
                    NativeTurnTerminal::Cancelled => "interrupted".to_owned(),
                    NativeTurnTerminal::Unknown => {
                        return Err(Error::new(
                            "native_readback",
                            "Muse view turn/completed carried an unknown terminal",
                        ));
                    }
                });
                // The durable view record carries the same failure text the
                // live terminal commits: keep the first record's text so the
                // readback surfaces the native reason an empty turn hides.
                if native_error.is_none() {
                    *native_error = turn_native_error(&completed, &completed.terminal, policy);
                }
            }
        }
        "item/completed" => {
            let completed: NativeItemCompletedNotification =
                serde_json::from_value(event.params.clone()).map_err(|_| {
                    Error::new(
                        "native_readback",
                        "Muse view item/completed did not match the 1.4 schema",
                    )
                })?;
            if completed.session_id != session_id
                || completed.item.kind != "agentMessage"
                || completed.item.turn_id.as_deref() != Some(turn_id)
            {
                return Ok(());
            }
            if final_text.is_none()
                && let Some(text) = completed.item.text.as_deref()
                && text.len() <= eliot_plugin::MAX_EVENT_BYTES
            {
                *final_text = Some(policy.redact_text(text));
            }
        }
        _ => {}
    }
    Ok(())
}

fn session_state(session: &NativeSession) -> &'static str {
    match session.status.as_str() {
        "idle" => "idle",
        "running" => "busy",
        _ => "unknown",
    }
}

/// Adopt a resume late-joiner pointer set. The observation counter is
/// captured before the pull, the pulled full params are described
/// (shape-validated; turn liveness is checked when the event path applies
/// them), and the event path adopts what still binds — guarded so a newer
/// stream resolution always wins over this stale snapshot. Returns whether
/// the pull itself landed; adoption never fails the resume either way.
async fn adopt_resume_snapshot(
    client: &NativeClient,
    events: &EventHandle,
    session_id: &str,
    pointers: &[crate::native::types::NativePendingRequestPointer],
) -> bool {
    let capture = events.status().observation;
    let expected = pointers
        .iter()
        .filter_map(|pointer| pointer.request_id())
        .filter(|id| !id.trim().is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let params = NativeApprovalListPendingParams {
        session_id: session_id.to_owned(),
    };
    let encoded = serde_json::to_value(params).unwrap_or(Value::Null);
    let pulled: Option<NativeApprovalListPendingResult> = client
        .call(
            "approval/listPending",
            encoded,
            Instant::now() + RPC_TIMEOUT,
        )
        .await
        .ok()
        .and_then(|value| serde_json::from_value(value).ok());
    let Some(pulled) = pulled else {
        let _ = events
            .adopt_pulled(Vec::new(), expected, capture, false)
            .await;
        return false;
    };
    let wanted = expected
        .iter()
        .map(String::as_str)
        .collect::<std::collections::HashSet<_>>();
    let mut entries = Vec::new();
    let mut snapshot_had_undecodable = false;
    for approval in &pulled.approvals {
        if !wanted.contains(approval.approval_id.as_str()) {
            continue;
        }
        let raw = serde_json::to_value(approval).unwrap_or(Value::Null);
        // The pull is session-scoped but the binding is still verified: a
        // foreign-session entry fails describe and is skipped, never adopted.
        // Skipped entries are reported (the event path gaps them) instead of
        // silently dropped; adoption still never fails the resume.
        match describe_request("approval/request", &raw, session_id, &approval.turn_id) {
            Ok(entry) => entries.push(entry),
            Err(_) => snapshot_had_undecodable = true,
        }
    }
    for prompt in &pulled.user_inputs {
        if !wanted.contains(prompt.user_input_id.as_str()) {
            continue;
        }
        let raw = serde_json::to_value(prompt).unwrap_or(Value::Null);
        match describe_request("userInput/request", &raw, session_id, &prompt.turn_id) {
            Ok(entry) => entries.push(entry),
            Err(_) => snapshot_had_undecodable = true,
        }
    }
    let _ = events
        .adopt_pulled(entries, expected, capture, snapshot_had_undecodable)
        .await;
    true
}

/// Shared contextual error, kept under the local path for the
/// `native::client` test that pins `crate::host::contextual_error`.
pub(crate) use eliot_plugin::session::contextual_error;

pub(crate) fn muse_scope(muse_home: &str) -> String {
    let mut hash = 0xcbf29ce484222325u64;
    let canonical_home = if cfg!(windows) {
        muse_home.to_lowercase()
    } else {
        muse_home.to_owned()
    };
    for byte in canonical_home.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    format!("muse-home:{hash:016x}")
}

/// Mint a caller-selected SS3.1.1 idempotency handle. A ULID and a UUIDv7
/// share the same 48-bit unix-millisecond prefix, so setting the version and
/// variant bits on fresh ULID bytes yields a valid UUIDv7 without a new
/// dependency; the randomness comes from the ULID generator.
pub(crate) fn new_command_id() -> String {
    let mut bytes = ulid::Ulid::new().0.to_be_bytes();
    bytes[6] = (bytes[6] & 0x0F) | 0x70;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
    let hex: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &hex[0..8],
        &hex[8..12],
        &hex[12..16],
        &hex[16..20],
        &hex[20..32]
    )
}

fn muse_manifest() -> PluginManifest {
    PluginManifest::per_agent(
        "muse",
        env!("CARGO_PKG_VERSION"),
        "1.4.0",
        &[
            "launch",
            "resume",
            "send",
            "configure",
            "steer",
            "interrupt",
            "approve",
            "deny",
            "answer",
            "inject",
            "stop",
        ],
        eliot_plugin::protocol::ManifestFeatures {
            discover: false,
            observe: false,
            start_requires_input: false,
            native_children: true,
        },
        BTreeMap::from([(
            "controlled".to_owned(),
            stream_sources(&["message", "request", "session", "tool", "turn"]),
        )]),
        "none",
    )
}

/// Native arguments for `muse serve`. Full access passes `--disable-sandbox`
/// at host startup; default/workspace-write retains host-default sandboxing.
fn muse_serve_args(permissions: &str) -> Vec<OsString> {
    let mut args = vec![OsString::from("serve")];
    if permissions == "danger-full-access" {
        args.push(OsString::from("--disable-sandbox"));
    }
    args
}
