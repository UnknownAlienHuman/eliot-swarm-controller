use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::session::target_environment;
use serde_json::Value;

pub(crate) const ZED_COMMIT: &str = "761786a4ef7144de734e48cb2e8f99bc46b5061e";

pub(crate) struct Plan {
    pub program: PathBuf,
    pub model: String,
    pub cwd: PathBuf,
    pub env: Vec<(OsString, OsString)>,
    pub secrets: Vec<String>,
}

pub(crate) fn plan(
    profile: Option<&Value>,
    target: Option<&Value>,
    workspace: Option<&str>,
) -> Result<Plan> {
    let profile =
        profile.ok_or_else(|| Error::new("profile_error", "Zed requires a frozen profile"))?;
    if profile.get("harness").and_then(Value::as_str) != Some("zed") {
        return Err(Error::new(
            "profile_error",
            "Zed received another harness profile",
        ));
    }
    let cmd = profile
        .get("cmd")
        .and_then(Value::as_str)
        .map(Path::new)
        .filter(|path| host_path(path))
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "Zed requires an absolute zed-agent-host executable",
            )
        })?;
    let program = cmd
        .canonicalize()
        .ok()
        .filter(|path| host_path(path) && path.is_file())
        .ok_or_else(|| {
            Error::new(
                "profile_error",
                "Zed host path does not resolve to a zed-agent-host executable",
            )
        })?;
    let model = profile
        .get("model")
        .and_then(Value::as_str)
        .and_then(|model| model.strip_prefix("openai/"))
        .filter(|model| !model.is_empty() && !model.contains('/'))
        .ok_or_else(|| Error::new("profile_error", "Zed model must be openai/<native-model>"))?;
    let cwd = PathBuf::from(workspace.unwrap_or_default());
    if !cwd.is_absolute() || !cwd.is_dir() {
        return Err(Error::new(
            "profile_error",
            "Zed requires an existing absolute workspace",
        ));
    }
    let environment = target_environment(target, profile.get("pass_env"), "Zed")?;
    let env = environment
        .variables
        .into_iter()
        .filter(|(key, _)| key == "OPENAI_API_KEY")
        .collect();
    Ok(Plan {
        program,
        model: model.to_owned(),
        cwd,
        env,
        secrets: environment.secrets,
    })
}

fn host_path(path: &Path) -> bool {
    let expected = if cfg!(windows) {
        "zed-agent-host.exe"
    } else {
        "zed-agent-host"
    };
    path.is_absolute()
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.eq_ignore_ascii_case(expected))
}

pub(crate) fn policy(secrets: &[String], credential: &str) -> SecretPolicy {
    SecretPolicy::new(secrets.iter().map(String::as_str).chain([credential]))
}
