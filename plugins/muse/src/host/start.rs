//! Existing Muse profile, transport, settings and readiness phases.
//! Start/resume uncertainty, registration and late endpoint retirement stay in the host.

use super::{
    AgentSession, Initialized, InitializedNative, MusePlugin, RPC_TIMEOUT, SessionPhase,
    catalogue::CatalogueHost, contextual_error, discard_failed_start, muse_scope, muse_serve_args,
    new_command_id, read_effective_reasoning_effort,
};
use crate::{
    events::{self, EventConfig},
    native::{
        NativeLaunch, NativeProcess,
        types::{
            NativeModelEffortVariants, NativeModelSelection, NativeSession,
            NativeSessionSetApprovalModeParams, NativeSessionSetApprovalModeResult,
            NativeSessionSetModelParams, NativeSessionSetModelResult,
            NativeSessionSetReasoningEffortParams, NativeSessionSetReasoningEffortResult,
            is_approval_mode, is_reasoning_effort_tier,
        },
    },
};
use eliot_core::{Error, Result};
use eliot_plugin::session::ModelSelection;
use eliot_plugin::{
    AgentStartParams, PeerClient,
    session::{
        ResumeSubject, TargetEnvironment, native_no_write, optional_profile_text,
        parse_resume_subject, target_environment, validate_source_plan,
    },
    skill,
};
use serde_json::Value;
use std::{ffi::OsString, path::PathBuf};
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
    pub(super) model_hold_revision: Option<String>,
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
    if profile.get("native_subagents").and_then(Value::as_str) != Some("allow") {
        return Err(Error::new(
            "unsupported",
            "Muse cannot disable native subagents; set native_subagents = \"allow\" explicitly to launch",
        ));
    }
    // An omitted model selects the harness native default: the catalogue
    // must still prove reachable and non-empty (an empty catalogue serves
    // no default), and the effective server-selected model is read back.
    // A resume running under an inherited `configure` overlay realigns to
    // the overlay's effective values instead of the frozen snapshot.
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
    // Overlay effort belongs to turn/start, including after resume.
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
    let model_hold_revision = params
        .target_environment
        .as_ref()
        .and_then(Value::as_object)
        .and_then(crate::install::model_hold_revision);
    secrets.push(initialized.credential.clone());
    // Sandbox posture is fixed at host birth (`muse serve` flags):
    // canonical danger-full-access passes --disable-sandbox, while
    // workspace-write preserves the host-default sandbox posture.
    let launch = NativeLaunch {
        program,
        args: muse_serve_args(&requested_permissions, requested_model.as_deref()),
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
        model_hold_revision,
    })
}

pub(super) fn skill_pointer(params: &AgentStartParams) -> Result<String> {
    // Muse serves the skill on demand through a pointer in the first turn.
    // Its data root arrives through the scoped integration environment.
    let data_dir = params
        .mcp_environment
        .as_ref()
        .and_then(Value::as_object)
        .and_then(|environment| environment.get("ELIOT_HOME"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute() && path.is_dir())
        .ok_or_else(|| {
            Error::new(
                "not_allowed",
                "Muse skill delivery requires the Agent-bound Eliot data root",
            )
        })?;
    let skill_path = skill::skill_body_path(&data_dir);
    skill::read_skill_file(&skill_path).map_err(|_| {
        Error::new(
            "skill_unavailable",
            "Muse skill source is unavailable; the daemon did not materialize it",
        )
    })?;
    let skill_path = eliot_core::display_path(&skill_path.to_string_lossy());
    Ok(format!(
        "The inherited `swarm` MCP tool is available for coordination.\n\
         Read the skill for delegation, mail, claims, variables, or artifacts. Skip it for solo work.\n\
         Skill file: {skill_path}"
    ))
}

pub(super) fn bind_owned_session(
    params: &AgentStartParams,
    initialized: &Initialized,
    mut native: NativeProcess,
    secrets: Vec<String>,
    model_hold_revision: Option<String>,
    skill_pointer: String,
    peer: &PeerClient,
) -> Result<AgentSession> {
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
            model_hold_revision,
        },
    );
    Ok(AgentSession {
        agent_id: params.agent_id.clone(),
        generation: params.generation,
        start_command_id: params.command_id.clone(),
        phase: SessionPhase::Unknown,
        session_id: None,
        native_scope: String::new(),
        selected_effort: None,
        expected_model: None,
        secrets,
        native,
        native_client,
        events,
        current_turn: None,
        stop_command_id: None,
        skill_pointer: Some(skill_pointer),
    })
}
/// Dispatched session start or resume with its owned session: the dispatch
/// either proves a reply or settles the failure with the session retained
/// exactly as the start path requires (dropped when proven-no-effect,
/// kept unknown when a native effect may have happened).
pub(super) struct DispatchedStart {
    pub(super) session: AgentSession,
    pub(super) response: Value,
}

impl MusePlugin {
    /// Dispatch session start (or session resume) and classify the reply.
    /// A rejection or a proven-undispatched write discards the child and
    /// retains the failure; any other transport loss keeps the session
    /// unknown instead of retrying the dispatch.
    pub(super) async fn dispatch_session_start(
        &mut self,
        mut session: AgentSession,
        session_method: &str,
        params_value: Value,
    ) -> Result<DispatchedStart> {
        match session
            .native_client
            .call(session_method, params_value, Instant::now() + RPC_TIMEOUT)
            .await
        {
            Ok(response) => Ok(DispatchedStart { session, response }),
            Err(error) if error.code == "native_rejected" => {
                let error = error
                    .with_where(format!("plugin.muse.start.{session_method}"))
                    .with_origin(
                        eliot_core::FailureOrigin::Harness,
                        "Muse returned a native session error",
                    );
                let error = discard_failed_start(&mut session, error).await;
                Err(self.retain_failed_start(error))
            }
            Err(error) if native_no_write(&error.code) => {
                let message = format!("Muse {session_method} was not dispatched");
                let code = error.code.clone();
                let error = contextual_error(error, code, message);
                let error = discard_failed_start(&mut session, error).await;
                Err(self.retain_failed_start(error))
            }
            Err(error) => {
                let error = contextual_error(
                    error,
                    "unknown",
                    format!(
                        "Muse {session_method} may have changed native state; it will not be repeated"
                    ),
                );
                let error = self.retain_failed_start(error);
                self.session = Some(session);
                Err(error)
            }
        }
    }

    pub(super) fn validate_catalogue(
        host: &CatalogueHost,
        requested_model: Option<&str>,
        requested_effort: Option<&str>,
    ) -> Result<InitializedNative> {
        let initialized = &host.identity;
        if !initialized
            .granted_capabilities
            .iter()
            .any(|cap| cap == "rawLog")
        {
            return Err(Error::new(
                "unsupported",
                "Muse host does not expose rawLog for provider model verification",
            ));
        }
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
        if requested_model.is_none() && host.catalogue.models.is_empty() {
            return Err(host.catalogue_error(Error::new(
                "native_model_unavailable",
                "Muse model/list reported no usable default; use an explicit model",
            )));
        }
        let selection = requested_model.map(|id| {
            ModelSelection::new(
                id,
                host.catalogue
                    .models
                    .iter()
                    .find(|model| model.model_id == id),
            )
        });
        if let (Some(effort), Some(selection)) = (requested_effort, &selection)
            && !selection.allows_effort(|entry| match entry.variants.as_ref() {
                Some(NativeModelEffortVariants::Known(variants)) => {
                    variants.iter().any(|variant| variant == effort)
                }
                _ => true,
            })
        {
            return Err(Error::new(
                "profile_error",
                "Muse profile effort is not supported by the exact selected model",
            ));
        }
        Ok(InitializedNative {
            effort: requested_effort.map(str::to_owned),
            scope: muse_scope(&initialized.muse_home),
            fingerprint: initialized.schema.fingerprint.clone(),
            catalogue_omits_model: selection.is_some_and(|model| model.catalogue_omits_model()),
        })
    }

    pub(super) async fn apply_start_effort(
        &mut self,
        session: &mut AgentSession,
        native_session: &NativeSession,
        view_cursor: &str,
        requested_effort: Option<&str>,
    ) -> Result<Option<String>> {
        // MSP makes the default durable at the exact setter ack. Read the
        // fresh view suffix to detect a conflicting report before readiness.
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
                    return Err(self.retain_failed_start(error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setReasoningEffort was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(session, contextual_error(error, code, message)).await;
                    return Err(self.retain_failed_start(error));
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
                    return Err(self.retain_failed_start(error));
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
                        return Err(self.retain_failed_start(error));
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
                return Err(self.retain_failed_start(error));
            }
            return match read_effective_reasoning_effort(
                &session.native_client,
                &native_session.session_id,
                view_cursor,
            )
            .await
            {
                Ok(effort) => Ok(effort),
                Err(error) => {
                    let error = discard_failed_start(session, error).await;
                    Err(self.retain_failed_start(error))
                }
            };
        }

        Ok(None)
    }

    pub(super) async fn align_resume_model(
        &mut self,
        session: &mut AgentSession,
        native_session: &NativeSession,
        selection: Option<NativeModelSelection>,
    ) -> Result<bool> {
        // Resume's model override resolves a change of model through the
        // retained provider. When the label already matches, resume leaves
        // its stored route alone. Repair the contributor route for sessions
        // created before #949, which can retain the ID with no profile.
        if let Some(model) = selection {
            let model_command = new_command_id();
            let model_params = NativeSessionSetModelParams {
                command_id: model_command.clone(),
                session_id: native_session.session_id.clone(),
                model,
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
                        error.with_where("plugin.muse.start.set_model").with_origin(
                            eliot_core::FailureOrigin::Harness,
                            "Muse session/setModel returned a native error",
                        ),
                    )
                    .await;
                    return Err(self.retain_failed_start(error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setModel was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(session, contextual_error(error, code, message)).await;
                    return Err(self.retain_failed_start(error));
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
                    return Err(self.retain_failed_start(error));
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
                        return Err(self.retain_failed_start(error));
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
                return Err(self.retain_failed_start(error));
            }
            return Ok(true);
        }

        Ok(false)
    }

    pub(super) async fn align_resume_approval(
        &mut self,
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
                    return Err(self.retain_failed_start(error));
                }
                Err(error) if native_no_write(&error.code) => {
                    let message = "Muse session/setApprovalMode was not dispatched".to_owned();
                    let code = error.code.clone();
                    let error =
                        discard_failed_start(session, contextual_error(error, code, message)).await;
                    return Err(self.retain_failed_start(error));
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
                    return Err(self.retain_failed_start(error));
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
                        return Err(self.retain_failed_start(error));
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
                return Err(self.retain_failed_start(error));
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
