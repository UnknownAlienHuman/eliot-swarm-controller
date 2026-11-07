use std::{ffi::OsString, path::PathBuf};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    AgentStartParams,
    native_transport::NativeLaunch,
    session::{
        optional_profile_text, parse_resume_subject, target_environment, validate_source_plan,
    },
};
use serde_json::{Value, json};

use crate::{
    events::FAMILIES,
    native::{InitializeResult, SessionResult},
};

pub(crate) struct Plan {
    pub launch: NativeLaunch,
    pub policy: SecretPolicy,
    pub model: String,
    pub scope: String,
    pub resume: Option<String>,
    mode: Option<String>,
    skip_permissions: bool,
}

impl Plan {
    pub fn new(params: &AgentStartParams, credential: &str) -> Result<Self> {
        if params.initial.is_some() {
            return Err(Error::new(
                "unsupported",
                "Antigravity starts before input; submit a separate send",
            ));
        }
        validate_source_plan("Antigravity", FAMILIES, params.source_plan.as_ref())?;
        let profile = params
            .profile
            .as_ref()
            .and_then(Value::as_object)
            .ok_or_else(|| Error::new("profile_error", "Antigravity requires a frozen profile"))?;
        if profile.get("harness").and_then(Value::as_str) != Some("antigravity") {
            return Err(Error::new(
                "profile_error",
                "Antigravity received another harness profile",
            ));
        }
        if profile.get("effort").is_some_and(|value| !value.is_null()) {
            return Err(Error::new(
                "unsupported",
                "Antigravity effort is unconnected on this route",
            ));
        }
        let model = optional_profile_text(profile, "model", "Antigravity")?
            .ok_or_else(|| Error::new("profile_error", "Antigravity requires an exact model"))?;
        let program = optional_profile_text(profile, "cmd", "Antigravity")?.ok_or_else(|| {
            Error::new(
                "profile_error",
                "Antigravity requires an explicit executable",
            )
        })?;
        let cwd = params
            .workspace
            .as_deref()
            .map(PathBuf::from)
            .filter(|path| path.is_dir())
            .ok_or_else(|| {
                Error::new(
                    "profile_error",
                    "Antigravity requires an existing workspace",
                )
            })?;
        let mode = optional_profile_text(profile, "mode", "Antigravity")?;
        if mode
            .as_deref()
            .is_some_and(|mode| !matches!(mode, "plan" | "auto" | "accept-edits"))
        {
            return Err(Error::new(
                "profile_error",
                "Antigravity mode must be plan, auto, or accept-edits",
            ));
        }
        let skip_permissions = match profile.get("dangerously_skip_permissions") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(value)) => *value,
            _ => {
                return Err(Error::new(
                    "profile_error",
                    "Antigravity dangerously_skip_permissions must be boolean",
                ));
            }
        };
        let environment = target_environment(
            params.target_environment.as_ref(),
            profile.get("pass_env"),
            "Antigravity",
        )?;
        let mut secrets = environment.secrets;
        secrets.push(credential.to_owned());
        let scope = config_scope(&environment.variables)?;
        let resume = params
            .resume_from
            .as_ref()
            .map(|resume| {
                parse_resume_subject(
                    resume,
                    params.source_state.as_ref(),
                    &params.agent_id,
                    params.generation,
                    "Antigravity",
                )
            })
            .transpose()?;
        if resume
            .as_ref()
            .is_some_and(|subject| subject.native_scope != scope)
        {
            return Err(Error::new(
                "profile_error",
                "Antigravity resume scope differs from the selected config namespace",
            ));
        }
        let resume = resume.map(|subject| subject.native_ref);
        let args = native_args(&model, mode.as_deref(), skip_permissions, resume.as_deref());
        Ok(Self {
            launch: NativeLaunch {
                program: program.into(),
                args,
                cwd,
                environment: environment.variables,
            },
            policy: SecretPolicy::new(secrets),
            model,
            scope,
            resume,
            mode,
            skip_permissions,
        })
    }

    /// Verify the ACP `initialize` + `session/new|load` facts against the
    /// frozen plan: protocol 1, a non-empty session id (the exact retained
    /// id on resume), and the model/thought-level selectors the translation
    /// below depends on. Permissions and cwd are launch facts (the ACP
    /// server is spawned in the workspace with the profile environment);
    /// the session object itself echoes neither, so cwd is proven by the
    /// launch, not by a response field.
    pub fn check_session(&self, init: &InitializeResult, session: &SessionResult) -> Result<Value> {
        if init.protocol_version != 1
            || session.session_id.trim().is_empty()
            || self
                .resume
                .as_ref()
                .is_some_and(|expected| expected != &session.session_id)
        {
            return Err(Error::new(
                "unknown",
                "Antigravity ACP session did not prove the requested conversation",
            ));
        }
        if self.resume.is_some()
            && init.agent_capabilities.get("loadSession") != Some(&Value::Bool(true))
        {
            return Err(Error::new(
                "unknown",
                "Antigravity ACP agent does not advertise session load",
            ));
        }
        let options = session.config_options.as_ref().ok_or_else(|| {
            Error::new(
                "unknown",
                "Antigravity ACP session advertised no config options",
            )
        })?;
        for (required, category) in [("model", "model"), ("thought_level", "thought_level")] {
            let Some(option) = options.iter().find(|option| option.id == required) else {
                return Err(Error::new(
                    "unknown",
                    "Antigravity ACP session did not prove the requested model selectors",
                ));
            };
            if option.category.as_deref() != Some(category) || option.kind != "select" {
                return Err(Error::new(
                    "unknown",
                    "Antigravity ACP model selector did not match its advertised category",
                ));
            }
        }
        // Every required write value must be an advertised option value:
        // an unadvertised id is rejected here instead of sent and guessed.
        for (config_id, value) in self.config_writes() {
            let advertised = options
                .iter()
                .find(|option| option.id == config_id)
                .is_some_and(|option| {
                    option
                        .option_values()
                        .iter()
                        .any(|(advertised, _)| advertised == &value)
                });
            if !advertised {
                return Err(Error::new(
                    "unknown",
                    "Antigravity ACP session did not advertise a required config value",
                ));
            }
        }
        let (base_model, thought_level) = translate_model(&self.model);
        Ok(json!({
            "model": base_model,
            "requested_model": self.model,
            "thought_level": thought_level,
            "permission_mode": self.skip_permissions,
            "cwd": self.launch.cwd,
            "session_id": session.session_id,
            "mode": session.modes.as_ref().map(|modes| modes.current_mode_id.clone()),
        }))
    }

    /// The `session/set_config_option` pairs this plan requires, in wire
    /// order: base model first, then thought level when the requested model
    /// carried an effort suffix.
    pub fn config_writes(&self) -> Vec<(String, String)> {
        let (base_model, thought_level) = translate_model(&self.model);
        let mut writes = vec![("model".to_owned(), base_model)];
        if let Some(level) = thought_level {
            writes.push(("thought_level".to_owned(), level));
        }
        writes
    }

    pub fn resume(&self) -> Option<&str> {
        self.resume.as_deref()
    }

    pub fn mode(&self) -> Option<&str> {
        self.mode.as_deref()
    }

    pub fn requested(&self) -> Value {
        json!({"model": self.model, "cwd": self.launch.cwd, "mode": self.mode,
            "dangerously_skip_permissions": self.skip_permissions, "transport": "acp"})
    }
}

/// Bind the actual inherited/overridden config namespace. This names the
/// canonical environment selectors, not an invented native database root.
fn config_scope(environment: &[(OsString, OsString)]) -> Result<String> {
    let mut paths = std::collections::BTreeMap::new();
    for name in [
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
    ] {
        let selected = environment
            .iter()
            .find(|(key, _)| {
                if cfg!(windows) {
                    key.to_string_lossy().eq_ignore_ascii_case(name)
                } else {
                    key.as_os_str() == std::ffi::OsStr::new(name)
                }
            })
            .map(|(_, value)| value.clone())
            .or_else(|| std::env::var_os(name));
        if let Some(selected) = selected.filter(|value| !value.is_empty()) {
            let path = PathBuf::from(selected);
            let path = std::fs::canonicalize(&path).unwrap_or(path);
            let text = path.to_string_lossy().into_owned();
            paths.insert(
                name,
                if cfg!(windows) {
                    text.to_lowercase()
                } else {
                    text
                },
            );
        }
    }
    if paths.is_empty() {
        return Err(Error::new(
            "profile_error",
            "Antigravity config namespace is unavailable",
        ));
    }
    serde_json::to_string(&paths)
        .map(|paths| format!("antigravity-config:{paths}"))
        .map_err(|_| Error::new("internal", "Could not encode Antigravity config namespace"))
}

/// Translate a requested CLI model id into the ACP session vocabulary.
/// The ACP `model` selector carries base family ids only (observed:
/// `gemini-3.8-flash`, `gemini-4-argon`, …); the effort suffix lives in
/// the separate `thought_level` selector (`low|medium|high`). Only those
/// three suffixes translate; any other suffix stays part of the base id
/// and will be rejected by the server instead of guessed at.
pub(crate) fn translate_model(model: &str) -> (String, Option<String>) {
    for level in ["low", "medium", "high"] {
        if let Some(base) = model.strip_suffix(&format!("-{level}"))
            && !base.is_empty()
        {
            return (base.to_owned(), Some(level.to_owned()));
        }
    }
    (model.to_owned(), None)
}

fn native_args(
    _model: &str,
    _mode: Option<&str>,
    skip_permissions: bool,
    _resume: Option<&str>,
) -> Vec<OsString> {
    // One ACP stdio server per generation. Model, thought level, session
    // mode and resume are ACP session facts now (set_config_option /
    // set_mode / session load), not CLI flags; only the process-level
    // permission posture remains a launch flag.
    [
        OsString::from("acp"),
        OsString::from("--stdio"),
        format!("--dangerously-skip-permissions={skip_permissions}").into(),
    ]
    .to_vec()
}
