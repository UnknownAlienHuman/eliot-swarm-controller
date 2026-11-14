//! Existing Muse profile, transport, settings and readiness phases.
//! Start/resume uncertainty, registration and late endpoint retirement stay in the host.

use super::{
    AgentSession, Initialized, InitializedNative, MusePlugin, RPC_TIMEOUT, SessionPhase,
    contextual_error, discard_failed_start, discover_model, muse_scope, muse_serve_args,
    new_command_id, read_effective_reasoning_effort,
};
use crate::{
    events::{self, EventConfig},
    native::{
        NativeClient, NativeLaunch, NativeProcess,
        types::{
            NativeClientCapabilities, NativeClientInfo, NativeInitializeParams,
            NativeInitializeResult, NativeModelEffortVariants, NativeModelSelection, NativeSession,
            NativeSessionSetApprovalModeParams, NativeSessionSetApprovalModeResult,
            NativeSessionSetModelParams, NativeSessionSetModelResult,
            NativeSessionSetReasoningEffortParams, NativeSessionSetReasoningEffortResult,
            is_approval_mode, is_reasoning_effort_tier,
        },
    },
};
use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    AgentStartParams, PeerClient,
    session::{
        ResumeSubject, TargetEnvironment, native_no_write, optional_profile_text,
        parse_resume_subject, target_environment, validate_source_plan,
    },
};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    ffi::OsString,
    path::PathBuf,
};
use tokio::time::Instant;

pub(super) struct PreparedStart {
    pub(super) resume_subject: Option<ResumeSubject>,
    pub(super) requested_model: Option<String>,
    pub(super) requested_effort: Option<String>,
    pub(super) requested_approval: Option<String>,
    pub(super) requested_permissions: String,
    pub(super) workspace: PathBuf,
    pub(super) launch: NativeLaunch,
    pub(super) secrets: Vec<String>,
}

pub(super) fn prepare_profile(
    params: &AgentStartParams,
    initialized: &Initialized,
) -> Result<PreparedStart> {
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
                "Muse",
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
        "Muse",
        &["session", "turn", "message", "tool", "request"],
        params.source_plan.as_ref(),
    )?;
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
    // An omitted model selects the harness native default: the catalogue
    // must still prove reachable and non-empty (an empty catalogue serves
    // no default), and the effective server-selected model is read back.
    let requested_model: Option<String> = profile
        .get("model")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned);
    let requested_effort = optional_profile_text(profile, "effort", "Muse")?;
    if let Some(effort) = requested_effort.as_deref()
        && !is_reasoning_effort_tier(effort)
    {
        return Err(Error::new(
            "profile_error",
            "Muse profile effort is not a supported reasoning tier",
        ));
    }
    let requested_approval = optional_profile_text(profile, "approval", "Muse")?;
    if let Some(mode) = requested_approval.as_deref()
        && !is_approval_mode(mode)
    {
        return Err(Error::new(
            "profile_error",
            "Muse profile approval is not a supported approval mode",
        ));
    }
    let requested_permissions = optional_profile_text(profile, "permissions", "Muse")?
        .unwrap_or_else(|| "workspace-write".to_owned());
    if requested_permissions != "workspace-write" && requested_permissions != "danger-full-access" {
        return Err(Error::new(
            "profile_error",
            "Muse profile permissions must be workspace-write or danger-full-access",
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
    // Sandbox posture is fixed at host birth (`muse serve` flags):
    // canonical danger-full-access passes --disable-sandbox, while
    // workspace-write preserves the host-default sandbox posture.
    let launch = NativeLaunch {
        program,
        args: muse_serve_args(&requested_permissions),
        cwd: workspace.clone(),
        environment,
    };

    Ok(PreparedStart {
        resume_subject,
        requested_model,
        requested_effort,
        requested_approval,
        requested_permissions,
        workspace,
        launch,
        secrets,
    })
}

pub(super) async fn spawn_owned_session(
    params: &AgentStartParams,
    initialized: &Initialized,
    launch: NativeLaunch,
    secrets: Vec<String>,
    peer: &PeerClient,
) -> Result<AgentSession> {
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
    Ok(AgentSession {
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
    })
}

impl MusePlugin {
    pub(super) async fn initialize_native(
        client: &NativeClient,
        requested_model: Option<&str>,
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
        if let (Some(effort), Some(selected)) = (requested_effort, model.as_ref())
            && let Some(NativeModelEffortVariants::Known(variants)) = selected.variants.as_ref()
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

    pub(super) async fn apply_start_effort(
        &mut self,
        params: &AgentStartParams,
        session: &mut AgentSession,
        native_session: &NativeSession,
        requested_effort: Option<&str>,
    ) -> Result<bool> {
        // Apply the frozen profile effort as the session default before
        // readiness. The ack proves admission only (it echoes the command id,
        // not the tier), so the standing default is read back from the durable
        // view and compared; `effort_verified` holds only on an exact match.
        let mut effort_verified = false;
        if let Some(effort) = requested_effort {
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
                        session,
                        contextual_error(
                            error,
                            "profile_error",
                            "Muse session/setReasoningEffort did not admit the profile effort",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(params, error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setReasoningEffort was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(session, contextual_error(error, code, message)).await;
                    return Err(self.retain_failed_start(params, error));
                }
                Err(error) => {
                    let error = discard_failed_start(
                        session,
                        contextual_error(
                            error,
                            "unknown",
                            "Muse session/setReasoningEffort may have changed native state; it will not be repeated",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(params, error));
                }
            };
            let effort_ack: NativeSessionSetReasoningEffortResult =
                match serde_json::from_value(effort_response) {
                    Ok(ack) => ack,
                    Err(_) => {
                        let error = discard_failed_start(
                            session,
                            Error::new(
                                "profile_error",
                                "Muse session/setReasoningEffort ack did not match the 1.4 schema",
                            ),
                        )
                        .await;
                        return Err(self.retain_failed_start(params, error));
                    }
                };
            if effort_ack.command_id != effort_command || effort_ack.status != "accepted" {
                let error = discard_failed_start(
                    session,
                    Error::new(
                        "profile_error",
                        "Muse session/setReasoningEffort did not acknowledge the exact submitted command",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(params, error));
            }
            let effective_effort = match read_effective_reasoning_effort(
                &session.native_client,
                &native_session.session_id,
            )
            .await
            {
                Ok(effort) => effort,
                Err(error) => {
                    let error = discard_failed_start(session, error).await;
                    return Err(self.retain_failed_start(params, error));
                }
            };
            effort_verified = effective_effort.as_deref() == Some(effort);
        }

        Ok(effort_verified)
    }

    pub(super) async fn align_resume_model(
        &mut self,
        params: &AgentStartParams,
        session: &mut AgentSession,
        native_session: &NativeSession,
        resuming: bool,
        requested_model: Option<&str>,
    ) -> Result<bool> {
        let mut setters_ran = false;
        // An omitted request accepts the retained server-selected model.
        if let Some(requested) = requested_model
            && resuming
            && native_session.model_id.as_deref() != Some(requested)
        {
            let model_command = new_command_id();
            let model_params = NativeSessionSetModelParams {
                command_id: model_command.clone(),
                session_id: native_session.session_id.clone(),
                model: NativeModelSelection {
                    model_id: requested.to_owned(),
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
                        session,
                        contextual_error(
                            error,
                            "profile_error",
                            "Muse session/setModel did not admit the profile model",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(params, error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setModel was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(session, contextual_error(error, code, message)).await;
                    return Err(self.retain_failed_start(params, error));
                }
                Err(error) => {
                    let error = discard_failed_start(
                        session,
                        contextual_error(
                            error,
                            "unknown",
                            "Muse session/setModel may have changed native state; it will not be repeated",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(params, error));
                }
            };
            let model_ack: NativeSessionSetModelResult =
                match serde_json::from_value(model_response) {
                    Ok(ack) => ack,
                    Err(_) => {
                        let error = discard_failed_start(
                            session,
                            Error::new(
                                "profile_error",
                                "Muse session/setModel ack did not match the 1.4 schema",
                            ),
                        )
                        .await;
                        return Err(self.retain_failed_start(params, error));
                    }
                };
            if model_ack.command_id != model_command || model_ack.status != "accepted" {
                let error = discard_failed_start(
                    session,
                    Error::new(
                        "profile_error",
                        "Muse session/setModel did not acknowledge the exact submitted command",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(params, error));
            }
            setters_ran = true;
        }

        Ok(setters_ran)
    }

    pub(super) async fn align_resume_approval(
        &mut self,
        params: &AgentStartParams,
        session: &mut AgentSession,
        native_session: &NativeSession,
        resuming: bool,
        requested_approval: Option<&str>,
    ) -> Result<bool> {
        let mut setters_ran = false;
        if let Some(approval) = requested_approval
            && resuming
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
                        session,
                        contextual_error(
                            error,
                            "profile_error",
                            "Muse session/setApprovalMode did not admit the profile approval mode",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(params, error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setApprovalMode was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(session, contextual_error(error, code, message)).await;
                    return Err(self.retain_failed_start(params, error));
                }
                Err(error) => {
                    let error = discard_failed_start(
                        session,
                        contextual_error(
                            error,
                            "unknown",
                            "Muse session/setApprovalMode may have changed native state; it will not be repeated",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(params, error));
                }
            };
            let approval_ack: NativeSessionSetApprovalModeResult =
                match serde_json::from_value(approval_response) {
                    Ok(ack) => ack,
                    Err(_) => {
                        let error = discard_failed_start(
                            session,
                            Error::new(
                                "profile_error",
                                "Muse session/setApprovalMode ack did not match the 1.4 schema",
                            ),
                        )
                        .await;
                        return Err(self.retain_failed_start(params, error));
                    }
                };
            if approval_ack.command_id != approval_command
                || approval_ack.status != "accepted"
                || approval_ack.effective_mode.mode != approval
            {
                let error = discard_failed_start(
                    session,
                    Error::new(
                        "profile_error",
                        "Muse session/setApprovalMode did not acknowledge the requested mode",
                    ),
                )
                .await;
                return Err(self.retain_failed_start(params, error));
            }
            setters_ran = true;
        }

        Ok(setters_ran)
    }

    pub(super) async fn publish_start_ready(
        &mut self,
        mut session: AgentSession,
        native_session: &NativeSession,
        effective: &Value,
    ) -> Result<AgentSession> {
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

        Ok(session)
    }
}
