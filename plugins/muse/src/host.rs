use crate::{
    events::{self, EventConfig, EventHandle, TurnFacts, rejected_control, unknown_control},
    native::{
        NativeClient, NativeLaunch, NativeProcess, native_no_write,
        requests::describe_request,
        types::{
            NativeApprovalListPendingParams, NativeApprovalListPendingResult,
            NativeClientCapabilities, NativeClientInfo, NativeInitializeParams,
            NativeInitializeResult, NativeItemCompletedNotification, NativeModelCatalogEntry,
            NativeModelEffortVariants, NativeModelListParams, NativeModelListResult,
            NativeModelSelection, NativeSession, NativeSessionReadParams, NativeSessionReadResult,
            NativeSessionReasoningEffortChangedNotification, NativeSessionResumeParams,
            NativeSessionResumeResult, NativeSessionSetApprovalModeParams,
            NativeSessionSetApprovalModeResult, NativeSessionSetModelParams,
            NativeSessionSetModelResult, NativeSessionSetReasoningEffortParams,
            NativeSessionSetReasoningEffortResult, NativeSessionStartParams,
            NativeSessionStartResult, NativeTurnCancelParams, NativeTurnCancelResult,
            NativeTurnCompletedNotification, NativeTurnInputPart, NativeTurnStartParams,
            NativeTurnStartResult, NativeTurnStartedNotification, NativeTurnSteerParams,
            NativeTurnSteerResult, NativeTurnTerminal, NativeUnframedViewEvent,
            NativeViewPageParams, NativeViewPageResult, is_approval_mode, is_reasoning_effort_tier,
        },
    },
};
use eliot_core::{Error, ProcessRecord, ProcessState, Result, SecretPolicy};
use eliot_plugin::{
    AgentCommandParams, AgentSnapshotParams, AgentStartParams, AgentStopParams, InitializeParams,
    MAX_TEXT_BYTES, Peer, PeerClient, PluginManifest, RpcRequest,
    protocol::MAX_CREDENTIAL_BYTES,
    session::{
        TargetEnvironment, target_environment, unknown_snapshot, validate_binding,
        validate_identity,
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
const VIEW_PAGE_SIZE: i64 = 1000;
const MAX_VIEW_PAGES: usize = 4;
const MAX_CONTROL_COMMANDS: usize = 32;

#[derive(Default)]
struct MusePlugin {
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
    selected_effort: Option<String>,
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

struct ResumeSubject {
    native_scope: String,
    native_ref: String,
    retained_turn: Option<String>,
}

struct InitializedNative {
    effort: Option<String>,
    scope: String,
    fingerprint: String,
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
            "Muse plugin input closed before shutdown completed",
        ))
    }
}

impl MusePlugin {
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
                "Muse plugin method is not implemented",
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
                    .map_err(|_| Error::new("internal", "Muse diagnostics could not be encoded"))?;
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
        let params: InitializeParams = decode(raw, "Muse plugin initialize params are invalid")?;
        if params.protocol != "eliot-plugin/1"
            || params.instance_dir.trim().is_empty()
            || params.credential.trim().is_empty()
            || params.credential.len() > MAX_CREDENTIAL_BYTES
        {
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
        let params: AgentStartParams = decode(raw, "Muse agent.start params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Muse",
        )?;
        if let Some(session) = &self.session {
            if session.agent_id == params.agent_id
                && session.generation == params.generation
                && session.start_command_id == params.command_id
            {
                return match session.phase {
                    SessionPhase::Ready => session
                        .start_result
                        .clone()
                        .ok_or_else(|| Error::new("unknown", "Muse start result was not retained")),
                    SessionPhase::Unknown => Err(Error::new(
                        "unknown",
                        "Muse session/start outcome is unresolved; it will not be repeated",
                    )),
                    SessionPhase::Stopped => Err(Error::new(
                        "bad_state",
                        "Muse Agent generation has already stopped",
                    )),
                };
            }
            return Err(Error::new(
                "stale_generation",
                "Muse plugin is already bound to another Agent generation",
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
                "Muse plugin already settled a different start command",
            ));
        }
        let initialized = self.initialized.as_ref().ok_or_else(not_initialized)?;
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "Muse start does not accept input; send it as a separate command",
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
            .ok_or_else(|| Error::new("profile_error", "Muse requires a frozen profile object"))?;
        let profile = frozen_profile
            .as_object()
            .ok_or_else(|| Error::new("profile_error", "Muse requires a frozen profile object"))?;
        if profile.get("harness").and_then(Value::as_str) != Some("muse") {
            return Err(Error::new(
                "profile_error",
                "Muse plugin received a profile for another harness",
            ));
        }
        let requested_model = profile
            .get("model")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Error::new("profile_error", "Muse profile requires an exact model id"))?
            .to_owned();
        let requested_effort = optional_profile_text(profile, "effort")?;
        if let Some(effort) = requested_effort.as_deref()
            && !is_reasoning_effort_tier(effort)
        {
            return Err(Error::new(
                "profile_error",
                "Muse profile effort is not a supported reasoning tier",
            ));
        }
        let requested_approval = optional_profile_text(profile, "approval")?;
        if let Some(mode) = requested_approval.as_deref()
            && !is_approval_mode(mode)
        {
            return Err(Error::new(
                "profile_error",
                "Muse profile approval is not a supported approval mode",
            ));
        }
        let workspace = params
            .workspace
            .as_deref()
            .filter(|path| !path.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| Error::new("profile_error", "Muse requires an existing workspace"))?;
        if !workspace.is_dir() {
            return Err(Error::new(
                "profile_error",
                "Muse workspace must be an existing directory",
            ));
        }
        // No PATH default: resolving `muse` would land on the `muse.cmd`
        // launcher wrapper, whose PID would orphan the real `muse serve`
        // host on stop. The frozen profile must name the native binary.
        let program = profile
            .get("cmd")
            .and_then(Value::as_str)
            .filter(|program| !program.trim().is_empty())
            .map(OsString::from)
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "Muse profile requires an explicit cmd naming one executable path",
                )
            })?;
        let TargetEnvironment {
            variables: environment,
            mut secrets,
        } = target_environment(
            params.target_environment.as_ref(),
            profile.get("pass_env"),
            "Muse",
        )?;
        secrets.push(initialized.credential.clone());
        // Sandbox posture is fixed at host birth (`muse serve` flags) and stays
        // the host default until a permissions mapping lands; approval mode is
        // profile-selected when the frozen profile carries one (server default
        // otherwise).
        let launch = NativeLaunch {
            program,
            args: vec![OsString::from("serve")],
            cwd: workspace.clone(),
            environment,
        };
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
            session_id: None,
            native_scope: String::new(),
            selected_effort: None,
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
                    "{}; owned session host cleanup is unconfirmed; no session or input was sent",
                    error.message
                );
                return Err(contextual_error(error, "unknown", message));
            }
            let error = session.native_client.diagnostic_error(error);
            return Err(self.retain_failed_start(&params, error));
        }

        let initialized_native = match Self::initialize_native(
            &session.native_client,
            &requested_model,
            requested_effort.as_deref(),
        )
        .await
        {
            Ok(initialized) => initialized,
            Err(error) => {
                let error = discard_failed_start(&mut session, error).await;
                return Err(self.retain_failed_start(&params, error));
            }
        };
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
                return Err(self.retain_failed_start(&params, error));
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
                let error = contextual_error(error, "native_readback", message);
                let error = discard_failed_start(&mut session, error).await;
                return Err(self.retain_failed_start(&params, error));
            }
        }

        let (session_method, params_value) = if let Some(subject) = &resume_subject {
            let params = NativeSessionResumeParams {
                command_id: new_command_id(),
                session_id: subject.native_ref.clone(),
                exclude_items: Some(true),
            };
            (
                "session/resume",
                serde_json::to_value(params).map_err(|_| {
                    Error::new(
                        "internal",
                        "Muse session/resume params could not be encoded",
                    )
                })?,
            )
        } else {
            let params = NativeSessionStartParams {
                command_id: new_command_id(),
                model_id: Some(requested_model.clone()),
                workspace_root: Some(workspace.to_string_lossy().into_owned()),
                approval_mode: requested_approval.clone(),
            };
            (
                "session/start",
                serde_json::to_value(params).map_err(|_| {
                    Error::new("internal", "Muse session/start params could not be encoded")
                })?,
            )
        };
        let response_value = match session
            .native_client
            .call(session_method, params_value, Instant::now() + RPC_TIMEOUT)
            .await
        {
            Ok(value) => value,
            Err(error) if error.code == "native_rejected" => {
                let (code, message) = if resume_subject.is_some() {
                    (
                        "not_found",
                        "Muse rejected resume of the exact retained session",
                    )
                } else {
                    (
                        "profile_error",
                        "Muse rejected session/start for the selected profile",
                    )
                };
                let error = contextual_error(error, code, message);
                let error = discard_failed_start(&mut session, error).await;
                return Err(self.retain_failed_start(&params, error));
            }
            Err(error) if native_no_write(&error.code) => {
                let message = format!("Muse {session_method} was not dispatched");
                let code = error.code.clone();
                let error = contextual_error(error, code, message);
                let error = discard_failed_start(&mut session, error).await;
                return Err(self.retain_failed_start(&params, error));
            }
            Err(error) => {
                self.session = Some(session);
                return Err(contextual_error(
                    error,
                    "unknown",
                    format!(
                        "Muse {session_method} may have changed native state; it will not be repeated"
                    ),
                ));
            }
        };
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
        // Apply the frozen profile effort as the session default before
        // readiness. The ack proves admission only (it echoes the command id,
        // not the tier), so the standing default is read back from the durable
        // view and compared; `effort_verified` holds only on an exact match.
        let mut effort_verified = false;
        if let Some(effort) = initialized_native.effort.as_deref() {
            let effort_command = new_command_id();
            let effort_params = NativeSessionSetReasoningEffortParams {
                command_id: effort_command.clone(),
                reasoning_effort: effort.to_owned(),
                session_id: native_session.session_id.clone(),
            };
            let effort_value = serde_json::to_value(effort_params).map_err(|_| {
                Error::new(
                    "internal",
                    "Muse session/setReasoningEffort params could not be encoded",
                )
            })?;
            let effort_response = match session
                .native_client
                .call(
                    "session/setReasoningEffort",
                    effort_value,
                    Instant::now() + RPC_TIMEOUT,
                )
                .await
            {
                Ok(value) => value,
                Err(error) if error.code == "native_rejected" => {
                    let error = discard_failed_start(
                        &mut session,
                        contextual_error(
                            error,
                            "profile_error",
                            "Muse session/setReasoningEffort did not admit the profile effort",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(&params, error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setReasoningEffort was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(&mut session, contextual_error(error, code, message))
                            .await;
                    return Err(self.retain_failed_start(&params, error));
                }
                Err(error) => {
                    let error = discard_failed_start(
                        &mut session,
                        contextual_error(
                            error,
                            "unknown",
                            "Muse session/setReasoningEffort may have changed native state; it will not be repeated",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(&params, error));
                }
            };
            let effort_ack: NativeSessionSetReasoningEffortResult =
                match serde_json::from_value(effort_response) {
                    Ok(ack) => ack,
                    Err(_) => {
                        let error = discard_failed_start(
                            &mut session,
                            Error::new(
                                "profile_error",
                                "Muse session/setReasoningEffort ack did not match the 1.4 schema",
                            ),
                        )
                        .await;
                        return Err(self.retain_failed_start(&params, error));
                    }
                };
            if effort_ack.command_id != effort_command || effort_ack.status != "accepted" {
                let error = discard_failed_start(
                    &mut session,
                    Error::new(
                        "profile_error",
                        "Muse session/setReasoningEffort did not acknowledge the exact submitted command",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(&params, error));
            }
            let effective_effort = match read_effective_reasoning_effort(
                &session.native_client,
                &native_session.session_id,
            )
            .await
            {
                Ok(effort) => effort,
                Err(error) => {
                    let error = discard_failed_start(&mut session, error).await;
                    return Err(self.retain_failed_start(&params, error));
                }
            };
            effort_verified = effective_effort.as_deref() == Some(effort);
        }
        // Model and approval alignment on resume: the frozen profile is the
        // source of truth, so a retained session running another model or
        // approval mode is realigned via the setters (launch already passes
        // both to `session/start`). Either setter runs idle — the resume
        // preflight proved it — so admission applies immediately and one
        // `session/read` confirms both.
        let mut setters_ran = false;
        if resume_subject.is_some()
            && native_session.model_id.as_deref() != Some(requested_model.as_str())
        {
            let model_command = new_command_id();
            let model_params = NativeSessionSetModelParams {
                command_id: model_command.clone(),
                session_id: native_session.session_id.clone(),
                model: NativeModelSelection {
                    model_id: requested_model.clone(),
                },
            };
            let model_value = serde_json::to_value(model_params).map_err(|_| {
                Error::new(
                    "internal",
                    "Muse session/setModel params could not be encoded",
                )
            })?;
            let model_response = match session
                .native_client
                .call(
                    "session/setModel",
                    model_value,
                    Instant::now() + RPC_TIMEOUT,
                )
                .await
            {
                Ok(value) => value,
                Err(error) if error.code == "native_rejected" => {
                    let error = discard_failed_start(
                        &mut session,
                        contextual_error(
                            error,
                            "profile_error",
                            "Muse session/setModel did not admit the profile model",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(&params, error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setModel was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(&mut session, contextual_error(error, code, message))
                            .await;
                    return Err(self.retain_failed_start(&params, error));
                }
                Err(error) => {
                    let error = discard_failed_start(
                        &mut session,
                        contextual_error(
                            error,
                            "unknown",
                            "Muse session/setModel may have changed native state; it will not be repeated",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(&params, error));
                }
            };
            let model_ack: NativeSessionSetModelResult =
                match serde_json::from_value(model_response) {
                    Ok(ack) => ack,
                    Err(_) => {
                        let error = discard_failed_start(
                            &mut session,
                            Error::new(
                                "profile_error",
                                "Muse session/setModel ack did not match the 1.4 schema",
                            ),
                        )
                        .await;
                        return Err(self.retain_failed_start(&params, error));
                    }
                };
            if model_ack.command_id != model_command || model_ack.status != "accepted" {
                let error = discard_failed_start(
                    &mut session,
                    Error::new(
                        "profile_error",
                        "Muse session/setModel did not acknowledge the exact submitted command",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(&params, error));
            }
            setters_ran = true;
        }
        if let Some(approval) = requested_approval.as_deref()
            && resume_subject.is_some()
            && native_session
                .approval_mode
                .as_ref()
                .is_none_or(|state| state.mode != approval)
        {
            let approval_command = new_command_id();
            let approval_params = NativeSessionSetApprovalModeParams {
                command_id: approval_command.clone(),
                session_id: native_session.session_id.clone(),
                mode: approval.to_owned(),
            };
            let approval_value = serde_json::to_value(approval_params).map_err(|_| {
                Error::new(
                    "internal",
                    "Muse session/setApprovalMode params could not be encoded",
                )
            })?;
            let approval_response = match session
                .native_client
                .call(
                    "session/setApprovalMode",
                    approval_value,
                    Instant::now() + RPC_TIMEOUT,
                )
                .await
            {
                Ok(value) => value,
                Err(error) if error.code == "native_rejected" => {
                    let error = discard_failed_start(
                        &mut session,
                        contextual_error(
                            error,
                            "profile_error",
                            "Muse session/setApprovalMode did not admit the profile approval mode",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(&params, error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setApprovalMode was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(&mut session, contextual_error(error, code, message))
                            .await;
                    return Err(self.retain_failed_start(&params, error));
                }
                Err(error) => {
                    let error = discard_failed_start(
                        &mut session,
                        contextual_error(
                            error,
                            "unknown",
                            "Muse session/setApprovalMode may have changed native state; it will not be repeated",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(&params, error));
                }
            };
            let approval_ack: NativeSessionSetApprovalModeResult =
                match serde_json::from_value(approval_response) {
                    Ok(ack) => ack,
                    Err(_) => {
                        let error = discard_failed_start(
                            &mut session,
                            Error::new(
                                "profile_error",
                                "Muse session/setApprovalMode ack did not match the 1.4 schema",
                            ),
                        )
                        .await;
                        return Err(self.retain_failed_start(&params, error));
                    }
                };
            if approval_ack.command_id != approval_command
                || approval_ack.status != "accepted"
                || approval_ack.effective_mode.mode != approval
            {
                let error = discard_failed_start(
                    &mut session,
                    Error::new(
                        "profile_error",
                        "Muse session/setApprovalMode did not acknowledge the requested mode",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(&params, error));
            }
            setters_ran = true;
        }
        // Effective readback: one `session/read` confirms both aligned values
        // (the `Session` object carries `modelId` and `approvalMode`); when
        // no setter ran, the start/resume result already states them.
        let (effective_model, effective_approval) =
            if setters_ran {
                let reread =
                    match read_owned_session(&session.native_client, &native_session.session_id)
                        .await
                    {
                        Ok(reread) => reread,
                        Err(error) => {
                            let error = discard_failed_start(&mut session, error).await;
                            return Err(self.retain_failed_start(&params, error));
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
        let model_verified = effective_model.as_deref() == Some(requested_model.as_str());
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
            "schema_fingerprint": initialized_native.fingerprint.clone(),
        });
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
            return Err(self.retain_failed_start(&params, error));
        }
        if let Err(error) = session
            .events
            .ready(
                session.native_scope.clone(),
                native_session.session_id.clone(),
                effective.clone(),
            )
            .await
        {
            session.phase = SessionPhase::Unknown;
            self.session = Some(session);
            return Err(Error::new(
                "unknown",
                format!(
                    "Muse session is known but session.ready was not acknowledged: {}",
                    error.message
                ),
            ));
        }
        session.phase = SessionPhase::Ready;
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
        session.start_result = Some(result.clone());
        self.session = Some(session);
        Ok(result)
    }

    async fn initialize_native(
        client: &NativeClient,
        requested_model: &str,
        requested_effort: Option<&str>,
    ) -> Result<InitializedNative> {
        let init_params = NativeInitializeParams {
            client_info: NativeClientInfo {
                name: "eliot".to_owned(),
                version: env!("CARGO_PKG_VERSION").to_owned(),
                title: Some("Eliot".to_owned()),
            },
            // A1 answers `userInput/request` through Eliot's answer/deny
            // verbs; `true` (the D-047 capable posture) has the host serve
            // prompts toward this client.
            capabilities: Some(NativeClientCapabilities {
                user_input_dialogs: Some(true),
            }),
        };
        let init_value = serde_json::to_value(init_params)
            .map_err(|_| Error::new("internal", "Muse initialize params could not be encoded"))?;
        let initialized: NativeInitializeResult = serde_json::from_value(
            client
                .call("initialize", init_value, Instant::now() + RPC_TIMEOUT)
                .await
                .map_err(|error| {
                    contextual_error(
                        error,
                        "profile_error",
                        "Muse session host initialize failed",
                    )
                })?,
        )
        .map_err(|_| {
            Error::new(
                "profile_error",
                "Muse session host initialize response was invalid",
            )
        })?;
        if initialized.muse_home.trim().is_empty() {
            return Err(Error::new(
                "profile_error",
                "Muse session host did not identify its data scope",
            ));
        }
        if initialized.platform_family.trim().is_empty()
            || initialized.platform_os.trim().is_empty()
            || initialized.user_agent.trim().is_empty()
            || initialized.server_info.version.trim().is_empty()
        {
            return Err(Error::new(
                "profile_error",
                "Muse session host initialize omitted runtime identity fields",
            ));
        }
        client
            .notify("initialized", None, Instant::now() + RPC_TIMEOUT)
            .await
            .map_err(|error| {
                contextual_error(
                    error,
                    "profile_error",
                    "Muse session host did not accept initialized",
                )
            })?;

        let model = discover_model(client, requested_model).await?;
        if let Some(effort) = requested_effort
            && let Some(NativeModelEffortVariants::Known(variants)) = model.variants.as_ref()
            && !variants.iter().any(|variant| variant == effort)
        {
            return Err(Error::new(
                "profile_error",
                "Muse profile effort is not supported by the exact selected model",
            ));
        }
        Ok(InitializedNative {
            effort: requested_effort.map(str::to_owned),
            scope: muse_scope(&initialized.muse_home),
            fingerprint: initialized.schema.fingerprint,
        })
    }

    async fn command(&mut self, raw: &Value) -> Result<Value> {
        let params: AgentCommandParams = decode(raw, "Muse agent.command params are invalid")?;
        validate_identity(
            &params.agent_id,
            params.generation,
            &params.command_id,
            "Muse",
        )?;
        match params.verb.as_str() {
            "send" => self.send_command(params).await,
            "steer" => self.steer_command(params).await,
            "interrupt" => self.interrupt_command(params).await,
            "approve" | "deny" | "answer" => self.reply_request(params).await,
            _ => Err(Error::new(
                "unsupported",
                "Muse plugin command verb is not implemented",
            )),
        }
    }

    async fn send_command(&mut self, params: AgentCommandParams) -> Result<Value> {
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
        if let Some(previous) = session.command_inputs.get(&params.command_id) {
            if previous != text {
                return Err(Error::new(
                    "source_integrity",
                    "Muse command id was reused with different input",
                ));
            }
            if let Some(turn_id) = session.command_turns.get(&params.command_id) {
                return Ok(json!({"state": "applied", "turn_id": turn_id}));
            }
            if session.unknown_commands.contains(&params.command_id) {
                return Err(Error::new(
                    "unknown",
                    "Muse turn/start outcome is unresolved and will not be repeated",
                ));
            }
            if session.rejected_commands.contains(&params.command_id) {
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Muse turn/start was known not to apply",
                }));
            }
            return Err(Error::new(
                "unknown",
                "Muse command was already dispatched without a retained exact outcome",
            ));
        }
        let event_status = session.events.status();
        if let Some(reason) = event_status.failure {
            return Err(Error::new("unknown", reason));
        }
        if event_status.active_turn_id.is_some() {
            return Err(Error::new("busy", "Muse already has an active turn"));
        }
        let command_id = params.command_id.clone();
        if session.command_inputs.len() >= 32 {
            return Err(Error::new(
                "unsupported",
                "Muse L6.4 per-generation command history bound has been reached",
            ));
        }
        session
            .command_inputs
            .insert(command_id.clone(), text.to_owned());
        // Mint the caller-selected idempotency handle before the write: for a
        // fresh turn this derives the turn id, and the ack authoritatively
        // confirms it. A lost reply leaves the outcome unknown; the input is
        // never written a second time under another id.
        let native_command_id = new_command_id();
        let turn_params = NativeTurnStartParams {
            command_id: native_command_id.clone(),
            session_id: session
                .session_id
                .clone()
                .ok_or_else(|| Error::new("unknown", "Muse session identity is unresolved"))?,
            input: vec![NativeTurnInputPart {
                part_type: "text".to_owned(),
                text: Some(text.to_owned()),
            }],
            reasoning_effort: session.selected_effort.clone(),
        };
        let encoded = serde_json::to_value(turn_params)
            .map_err(|_| Error::new("internal", "Muse turn/start params could not be encoded"))?;
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
                    "reason": "Muse turn/start was not dispatched",
                }));
            }
            Err(error) if error.code == "native_rejected" => {
                session.rejected_commands.insert(command_id);
                return Ok(json!({"state": "rejected", "reason": "Muse rejected turn/start"}));
            }
            Err(error) if error.code == "frame_too_large" => {
                session.rejected_commands.insert(command_id);
                return Ok(json!({
                    "state": "rejected",
                    "reason": "Muse turn/start frame exceeded the native transport bound before dispatch",
                }));
            }
            Err(_) => {
                session.unknown_commands.insert(command_id);
                session.phase = SessionPhase::Unknown;
                return Err(Error::new(
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
                    session.unknown_commands.insert(command_id);
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "Muse turn/start steered the input into a turn it did not create",
                    ));
                }
                _ => {
                    session.unknown_commands.insert(command_id);
                    session.phase = SessionPhase::Unknown;
                    return Err(Error::new(
                        "unknown",
                        "Muse turn/start returned no exact turn identity",
                    ));
                }
            };
        let turn_id = started.turn_id;
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
                "Muse turn was accepted but turn.started was not acknowledged",
            ));
        }
        Ok(json!({"state": "applied", "turn_id": turn_id}))
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
        if text.len() > MAX_TEXT_BYTES {
            return Ok(rejected_control("Muse steer input exceeds 512 KiB"));
        }
        if session.secrets.iter().any(|secret| text.contains(secret)) {
            return Err(Error::new(
                "secret_in_payload",
                "Muse steer contains a known credential",
            ));
        }
        if let Some(result) = control_replay(session, &params)? {
            return Ok(result);
        }
        if session.phase != SessionPhase::Ready || session.events.status().source_unknown {
            let result = rejected_control("Muse steer does not match a healthy active turn");
            settle_control(
                session,
                &params.command_id,
                ControlOutcome::Rejected(result.clone()),
            );
            return Ok(result);
        }
        // The turn itself is gated authoritatively: the event task owns the
        // active turn and answers behind the terminal commit, so a steer
        // that arrives after the terminal batch never writes to a dead turn.
        let live = match session.events.check_turn(turn_id).await {
            Ok(live) => live,
            Err(error) => {
                let result = unknown_control(&error.message);
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                return Ok(result);
            }
        };
        if !live {
            abandon_control(session, &params.command_id);
            return Err(Error::new(
                "stale_turn",
                "Muse turn/steer does not match the exact active turn",
            ));
        }
        let Some(native_session) = session.session_id.clone() else {
            let result = unknown_control("Muse session identity is unresolved");
            settle_control(session, &params.command_id, ControlOutcome::Unknown);
            return Ok(result);
        };
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
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
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
                let result = rejected_control("Muse turn/steer was not applied");
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Rejected(result.clone()),
                );
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Muse turn/steer outcome is unknown");
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                Ok(result)
            }
            Ok(value) => {
                let steered: NativeTurnSteerResult = match serde_json::from_value(value) {
                    Ok(steered) => steered,
                    Err(_) => {
                        let result = unknown_control("Muse turn/steer response was invalid");
                        settle_control(session, &params.command_id, ControlOutcome::Unknown);
                        return Ok(result);
                    }
                };
                if steered.command_id != native_command_id
                    || steered.status != "accepted"
                    || steered.turn_id != turn_id
                {
                    let result = unknown_control("Muse turn/steer was not absorbed by its turn");
                    settle_control(session, &params.command_id, ControlOutcome::Unknown);
                    return Ok(result);
                }
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
        if let Some(result) = control_replay(session, &params)? {
            return Ok(result);
        }
        if session.phase == SessionPhase::Stopped {
            abandon_control(session, &params.command_id);
            return Err(Error::new(
                "bad_state",
                "Muse plugin is stopping; no further input is accepted",
            ));
        }
        if session.phase != SessionPhase::Ready || session.events.status().source_unknown {
            let result = rejected_control("Muse interrupt does not match a healthy active turn");
            settle_control(
                session,
                &params.command_id,
                ControlOutcome::Rejected(result.clone()),
            );
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
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                return Ok(result);
            }
        };
        if !live {
            abandon_control(session, &params.command_id);
            return Err(Error::new(
                "stale_turn",
                "Muse turn/cancel does not match the exact active turn",
            ));
        }
        let Some(native_session) = session.session_id.clone() else {
            let result = unknown_control("Muse session identity is unresolved");
            settle_control(session, &params.command_id, ControlOutcome::Unknown);
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
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
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
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Rejected(result.clone()),
                );
                Ok(result)
            }
            Err(_) => {
                let result = unknown_control("Muse turn/cancel outcome is unknown");
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                Ok(result)
            }
            Ok(value) => {
                let cancelled: NativeTurnCancelResult = match serde_json::from_value(value) {
                    Ok(cancelled) => cancelled,
                    Err(_) => {
                        let result = unknown_control("Muse turn/cancel response was invalid");
                        settle_control(session, &params.command_id, ControlOutcome::Unknown);
                        return Ok(result);
                    }
                };
                if cancelled.command_id != native_command_id
                    || cancelled.status != "accepted"
                    || cancelled.turn_id != turn_id
                {
                    let result = unknown_control("Muse turn/cancel did not stop its exact turn");
                    settle_control(session, &params.command_id, ControlOutcome::Unknown);
                    return Ok(result);
                }
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
            .ok_or_else(|| {
                Error::new(
                    "stale_turn",
                    "Muse request reply needs its captured turn id",
                )
            })?;
        let request_id = params
            .request_id
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                Error::new(
                    "not_found",
                    "Muse request reply needs its captured request id",
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
                        let result = unknown_control("Muse request reply omitted its outcome");
                        settle_control(session, &params.command_id, ControlOutcome::Unknown);
                        return Ok(result);
                    }
                };
                settle_control(session, &params.command_id, outcome);
                Ok(result)
            }
            Err(error) if native_no_write(&error.code) => {
                let result = rejected_control("Muse request reply was not dispatched");
                settle_control(
                    session,
                    &params.command_id,
                    ControlOutcome::Rejected(result.clone()),
                );
                Ok(result)
            }
            Err(error) if error.code == "unknown" => {
                settle_control(session, &params.command_id, ControlOutcome::Unknown);
                Ok(unknown_control("Muse request reply outcome is unknown"))
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
                "Muse command has no proof for the requested native turn",
            );
        }
        if !known_command(session, &params.command_id) {
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
        let readback = match read_owned_session(&session.native_client, session_id).await {
            Ok(readback) => readback,
            Err(_) => {
                return unknown_snapshot(
                    &params,
                    "Muse session/read did not prove the exact owned session",
                );
            }
        };
        let native_state = session_state(&readback);
        if session.command_inputs.contains_key(&params.command_id) && turn_id.is_none() {
            return unknown_snapshot(&params, "Muse command has no exact native turn association");
        }
        let last_turn = if let Some(turn_id) = turn_id.as_deref() {
            match read_owned_turn(
                &session.native_client,
                session_id,
                turn_id,
                &params.command_id,
                readback.active_turn_id.as_deref(),
                &session.secrets,
            )
            .await
            {
                Ok(Some(facts)) => Some(facts),
                _ => {
                    return unknown_snapshot(
                        &params,
                        "Muse readback did not prove the exact requested turn",
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
            "native_ref": session_id,
            "active_turn": last_turn.as_ref().filter(|turn| !turn.terminal).map(|turn| turn.turn_id.clone()),
            "last_turn": last_turn,
            "batch_unknown": event_status.batch_unknown,
            "source_unknown": event_status.source_unknown,
            "result_known": result_known,
        }))
    }

    async fn stop(&mut self, raw: &Value) -> Result<Value> {
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
        if session.phase == SessionPhase::Stopped
            && session.stop_command_id.as_deref() == Some(params.command_id.as_str())
        {
            return Ok(json!({"state": "applied"}));
        }
        if let Some(existing) = &session.stop_command_id {
            if existing != &params.command_id {
                return Err(Error::new(
                    "source_integrity",
                    "Muse stop command id changed after dispatch",
                ));
            }
            return Err(Error::new(
                "unknown",
                "Muse stop outcome is unresolved and will not be repeated",
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
            "Muse process/output drain or session.ended outcome is unknown",
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
            "Muse session host PID is unavailable; no session or input was sent",
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
                "Muse session host process registration failed ({})",
                error.code
            );
            contextual_error(error, "process_registration", message)
        })?;
    let record: ProcessRecord = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "process_registration",
            "Muse host returned an invalid process.put acknowledgement",
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
            "Muse host did not verify the running session host under this Agent generation",
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

async fn discover_model(
    client: &NativeClient,
    requested_model: &str,
) -> Result<NativeModelCatalogEntry> {
    // `model/list` is a single snapshot call: v1 has no catalog pagination
    // and no catalog subscription.
    let response = client
        .call(
            "model/list",
            serde_json::to_value(NativeModelListParams { session_id: None }).map_err(|_| {
                Error::new("internal", "Muse model/list params could not be encoded")
            })?,
            Instant::now() + RPC_TIMEOUT,
        )
        .await
        .map_err(|error| {
            contextual_error(
                error,
                "profile_error",
                "Muse model/list did not return a catalogue",
            )
        })?;
    let catalog: NativeModelListResult = serde_json::from_value(response).map_err(|_| {
        Error::new(
            "profile_error",
            "Muse model/list result did not match the installed schema",
        )
    })?;
    catalog
        .models
        .into_iter()
        .find(|model| model.model_id == requested_model)
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "The exact requested Muse model is unavailable in this account catalogue",
            )
        })
}

async fn read_owned_session(client: &NativeClient, session_id: &str) -> Result<NativeSession> {
    let response: NativeSessionReadResult = serde_json::from_value(
        client
            .call(
                "session/read",
                serde_json::to_value(NativeSessionReadParams {
                    session_id: session_id.to_owned(),
                    exclude_items: Some(true),
                })
                .map_err(|_| {
                    Error::new("internal", "Muse session/read params could not be encoded")
                })?,
                Instant::now() + RPC_TIMEOUT,
            )
            .await?,
    )
    .map_err(|_| Error::new("native_readback", "Muse session/read response was invalid"))?;
    if response.session.session_id != session_id {
        return Err(Error::new(
            "source_integrity",
            "Muse session/read returned another native session",
        ));
    }
    Ok(response.session)
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

fn parse_resume_subject(
    resume: &Value,
    source_state: Option<&Value>,
    agent_id: &str,
    generation: i64,
) -> Result<ResumeSubject> {
    let resume = resume
        .as_object()
        .ok_or_else(|| Error::new("invalid", "Muse resume_from must be an object"))?;
    if resume.len() != 2
        || !resume.contains_key("native_scope")
        || !resume.contains_key("native_ref")
    {
        return Err(Error::new(
            "invalid",
            "Muse resume_from must contain only native_scope and native_ref",
        ));
    }
    let native_scope = resume
        .get("native_scope")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", "Muse resume_from requires native_scope"))?
        .to_owned();
    let native_ref = resume
        .get("native_ref")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| Error::new("invalid", "Muse resume_from requires native_ref"))?
        .to_owned();
    let source_state = source_state
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("unknown", "Muse resume requires retained native readback"))?;
    let retained_generation = source_state
        .get("retained_generation")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            Error::new(
                "unknown",
                "Muse resume readback omitted its retained generation",
            )
        })?;
    if retained_generation.checked_add(1) != Some(generation) {
        return Err(Error::new(
            "stale_generation",
            "Muse resume generation does not follow the retained source generation",
        ));
    }
    let retained_source = source_state
        .get("retained_source")
        .and_then(Value::as_object)
        .ok_or_else(|| Error::new("unknown", "Muse resume omitted retained source state"))?;
    if retained_source.get("agent_id").and_then(Value::as_str) != Some(agent_id)
        || retained_source.get("generation").and_then(Value::as_i64) != Some(retained_generation)
        || retained_source.get("source").and_then(Value::as_str) != Some("stream")
    {
        return Err(Error::new(
            "stale_generation",
            "Muse resume source state does not match the retained Agent generation",
        ));
    }
    let families = retained_source
        .get("families")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::new("unknown", "Muse resume source state omitted its families"))?;
    if ["session", "turn", "message"]
        .iter()
        .any(|family| !families.iter().any(|value| value.as_str() == Some(family)))
    {
        return Err(Error::new(
            "unknown",
            "Muse retained source did not own the required event families",
        ));
    }
    let current_turn = source_state
        .get("current_turn")
        .ok_or_else(|| Error::new("unknown", "Muse resume readback omitted current_turn"))?;
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
                    "Muse resume current_turn summary is incomplete",
                ));
            }
            Some(
                turn.get("turn_id")
                    .and_then(Value::as_str)
                    .filter(|value| !value.trim().is_empty())
                    .ok_or_else(|| {
                        Error::new(
                            "unknown",
                            "Muse resume current_turn omitted its exact turn id",
                        )
                    })?
                    .to_owned(),
            )
        }
        _ => {
            return Err(Error::new(
                "unknown",
                "Muse resume current_turn readback is malformed",
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
    session_id: &str,
    retained_turn: Option<&str>,
) -> Result<()> {
    let session = read_owned_session(client, session_id).await?;
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
            terminal: false,
        }));
    }
    // The turn is not the live foreground turn. Prove its terminal state and
    // final text from the durable-sourced view records, newest first.
    let policy = SecretPolicy::new(secrets.iter().cloned());
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut terminal: Option<String> = None;
    let mut final_text: Option<String> = None;
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
                "Muse control command id was reused with different binding",
            ));
        }
        return Ok(Some(match &previous.outcome {
            ControlOutcome::Applied(result) | ControlOutcome::Rejected(result) => result.clone(),
            ControlOutcome::Pending => unknown_control("Muse control command is already in flight"),
            ControlOutcome::Unknown => {
                unknown_control("Muse control command outcome is unresolved")
            }
        }));
    }
    if session.control_commands.len() >= MAX_CONTROL_COMMANDS {
        return Ok(Some(rejected_control(
            "Muse L6.4 per-generation control history bound has been reached",
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

/// Drop a control entry `control_replay` inserted before any native write.
/// A deterministic pre-write refusal (stale turn, stopping host) proves no
/// write happened, so the entry is removed instead of settled: the exact
/// command re-evaluates on retry (no stale `Pending` "already in flight"),
/// and refusals consume none of the 32 per-generation slots.
fn abandon_control(session: &mut AgentSession, command_id: &str) {
    session.control_commands.remove(command_id);
}

fn validate_source_plan(source_plan: Option<&Value>) -> Result<()> {
    let Some(plan) = source_plan.and_then(Value::as_object) else {
        return Err(Error::new(
            "unsupported",
            "Muse start requires an installed stream source plan",
        ));
    };
    if ["session", "turn", "message", "request"]
        .iter()
        .any(|family| plan.get(*family).and_then(Value::as_str) != Some("stream"))
    {
        return Err(Error::new(
            "unsupported",
            "Muse start requires stream ownership for session, turn, message, and request",
        ));
    }
    Ok(())
}

/// Shared contextual error, kept under the local path for the
/// `native::client` test that pins `crate::host::contextual_error`.
pub(crate) fn contextual_error(
    error: Error,
    code: impl Into<String>,
    message: impl Into<String>,
) -> Error {
    eliot_plugin::session::contextual_error(error, code, message)
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
            format!("Muse profile field {key} must be a non-empty string"),
        )),
    }
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
    let controlled = BTreeMap::from([
        ("message".to_owned(), "stream".to_owned()),
        ("request".to_owned(), "stream".to_owned()),
        ("session".to_owned(), "stream".to_owned()),
        ("turn".to_owned(), "stream".to_owned()),
    ]);
    PluginManifest {
        name: "muse".to_owned(),
        version: env!("CARGO_PKG_VERSION").to_owned(),
        protocol: "eliot-plugin/1".to_owned(),
        harness_version: "1.4.0".to_owned(),
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
            discover: false,
            observe: false,
            start_requires_input: false,
            native_children: false,
        },
        sources: BTreeMap::from([("controlled".to_owned(), controlled)]),
        proxy: "none".to_owned(),
    }
}

fn decode<T: serde::de::DeserializeOwned>(raw: &Value, message: &str) -> Result<T> {
    serde_json::from_value(raw.clone()).map_err(|_| Error::new("invalid", message))
}

fn not_initialized() -> Error {
    Error::new("bad_state", "Muse plugin has not been initialized")
}

#[cfg(test)]
mod tests {
    use super::{
        muse_manifest, muse_scope, new_command_id, validate_resume_preflight, validate_source_plan,
    };
    use crate::native::types::NativeSession;
    use serde_json::json;

    fn preflight_session(
        status: &str,
        active_turn: Option<&str>,
        turn_count: Option<i64>,
    ) -> NativeSession {
        NativeSession {
            session_id: "session-1".to_owned(),
            status: status.to_owned(),
            active_turn_id: active_turn.map(str::to_owned),
            model_id: Some("muse-spark-1.3-contributor".to_owned()),
            provider_id: Some("meta".to_owned()),
            workspace_root: Some("C:\\work".to_owned()),
            approval_mode: None,
            turn_count,
        }
    }

    #[test]
    fn command_id_is_a_uuidv7() {
        for _ in 0..16 {
            let id = new_command_id();
            assert_eq!(id.len(), 36);
            let parts: Vec<&str> = id.split('-').collect();
            assert_eq!(parts.len(), 5);
            assert_eq!(
                parts.iter().map(|part| part.len()).collect::<Vec<_>>(),
                [8, 4, 4, 4, 12]
            );
            assert!(
                id.chars()
                    .all(|char| char.is_ascii_hexdigit() || char == '-')
            );
            assert_eq!(&id[14..15], "7", "version nibble: {id}");
            assert!(
                matches!(&id[19..20], "8" | "9" | "a" | "b"),
                "variant nibble: {id}"
            );
        }
        assert_ne!(new_command_id(), new_command_id());
    }

    #[test]
    fn manifest_advertises_the_connected_verbs() {
        let manifest = muse_manifest();
        assert_eq!(manifest.name, "muse");
        assert_eq!(manifest.protocol, "eliot-plugin/1");
        assert_eq!(manifest.harness_version, "1.4.0");
        assert!(manifest.available);
        assert_eq!(manifest.mode, "per_agent");
        assert_eq!(
            manifest.verbs,
            [
                "launch",
                "resume",
                "send",
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
        assert!(validate_source_plan(None).is_err());
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream", "turn": "stream", "message": "stream",
                "request": "stream",
            })))
            .is_ok()
        );
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream", "turn": "stream", "message": "stream",
                "request": "stream", "tool": "stream",
            })))
            .is_ok()
        );
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream", "turn": "stream", "message": "stream",
            })))
            .is_err()
        );
        assert!(
            validate_source_plan(Some(&json!({
                "session": "stream", "turn": "stream",
            })))
            .is_err()
        );
    }

    #[test]
    fn native_scope_is_stable_per_home() {
        assert_eq!(muse_scope("C:\\Muse"), muse_scope("C:\\Muse"));
        assert_ne!(muse_scope("C:\\Muse"), muse_scope("C:\\Other"));
        assert!(muse_scope("C:\\Muse").starts_with("muse-home:"));
    }

    #[test]
    fn resume_preflight_requires_the_exact_retained_terminal_turn() {
        for status in ["idle", "notLoaded"] {
            let session = preflight_session(status, None, Some(1));
            for terminal in ["completed", "failed", "cancelled"] {
                assert!(
                    validate_resume_preflight(
                        &session,
                        Some("retained"),
                        Some(("retained", terminal)),
                    )
                    .is_ok(),
                    "status={status} terminal={terminal}"
                );
            }
        }
        for status in ["running", "unknown-status", ""] {
            let session = preflight_session(status, None, Some(1));
            assert!(
                validate_resume_preflight(
                    &session,
                    Some("retained"),
                    Some(("retained", "completed"))
                )
                .is_err(),
                "status={status}"
            );
        }
        let active = preflight_session("idle", Some("live-turn"), Some(1));
        assert!(
            validate_resume_preflight(&active, Some("retained"), Some(("retained", "completed")))
                .is_err()
        );
        let session = preflight_session("idle", None, Some(1));
        for newest in [
            None,
            Some(("other", "completed")),
            Some(("retained", "inProgress")),
            Some(("retained", "future-terminal")),
        ] {
            assert!(
                validate_resume_preflight(&session, Some("retained"), newest).is_err(),
                "newest={newest:?}"
            );
        }
    }

    #[test]
    fn resume_preflight_proves_the_no_turn_case_from_the_turn_counter() {
        let empty = preflight_session("idle", None, Some(0));
        assert!(validate_resume_preflight(&empty, None, None).is_ok());
        for turn_count in [None, Some(1)] {
            let unproven = preflight_session("idle", None, turn_count);
            assert!(
                validate_resume_preflight(&unproven, None, None).is_err(),
                "turn_count={turn_count:?}"
            );
        }
        let empty = preflight_session("idle", None, Some(0));
        assert!(validate_resume_preflight(&empty, None, Some(("foreign", "completed"))).is_err());
    }
}
