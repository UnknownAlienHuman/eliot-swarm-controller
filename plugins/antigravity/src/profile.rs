use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    AgentStartParams,
    native_transport::NativeLaunch,
    session::{
        optional_profile_text, parse_resume_subject, paths_equal, target_environment,
        validate_source_plan,
    },
};
use serde_json::{Value, json};

use crate::{events::FAMILIES, native::Init};

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

    pub fn check_init(&self, conversation: &str, init: &Init) -> Result<Value> {
        if conversation.trim().is_empty()
            || init.permission_mode.trim().is_empty()
            || init.model != self.model
            || !paths_equal(Path::new(&init.cwd), &self.launch.cwd)
            || self
                .resume
                .as_ref()
                .is_some_and(|expected| expected != conversation)
        {
            return Err(Error::new(
                "unknown",
                "Antigravity init did not prove the requested model, workspace, and conversation",
            ));
        }
        Ok(json!({"model": init.model, "permission_mode": init.permission_mode, "cwd": init.cwd}))
    }

    pub fn requested(&self) -> Value {
        json!({"model": self.model, "cwd": self.launch.cwd, "mode": self.mode,
            "dangerously_skip_permissions": self.skip_permissions})
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

fn native_args(
    model: &str,
    mode: Option<&str>,
    skip_permissions: bool,
    resume: Option<&str>,
) -> Vec<OsString> {
    let mut args: Vec<OsString> = [
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--model",
        model,
    ]
    .into_iter()
    .map(OsString::from)
    .collect();
    args.push(format!("--dangerously-skip-permissions={skip_permissions}").into());
    if let Some(mode) = mode {
        args.extend([OsString::from("--mode"), OsString::from(mode)]);
    }
    if let Some(resume) = resume {
        args.extend([OsString::from("--conversation"), OsString::from(resume)]);
    }
    args
}
