use std::path::{Path, PathBuf};

use serde_json::Value;
use swarm_contracts::runtime::RuntimeCommand;

/// A validated direct-child launch request. Only this module can construct the
/// fixed argv from a manager-owned RuntimeCommand; the Tokio spawner inherits
/// the verified module runner's non-killing process group/job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeLaunchSpec {
    pub(crate) executable: PathBuf,
    pub(crate) args: Vec<String>,
    pub(crate) cwd: PathBuf,
    pub(crate) stdin_piped: bool,
    pub(crate) stdout_piped: bool,
    pub(crate) stderr_piped: bool,
    pub(crate) shell: bool,
    pub(crate) kill_on_drop: bool,
}

pub fn build_launch_spec(
    executable: &Path,
    command: &RuntimeCommand,
) -> Result<NativeLaunchSpec, &'static str> {
    if command.method != "agent.open" {
        return Err("NATIVE_PROCESS_START_REQUIRES_AGENT_OPEN");
    }
    if !executable.is_absolute() || !bounded_path_text(executable) {
        return Err("NATIVE_EXECUTABLE_MUST_BE_ABSOLUTE");
    }
    #[cfg(windows)]
    if matches!(
        executable.extension().and_then(|extension| extension.to_str()),
        Some(extension) if extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
    ) {
        return Err("SHELL_WRAPPER_NOT_ALLOWED");
    }

    let options = command
        .route
        .get("native_options")
        .and_then(Value::as_object)
        .ok_or("NATIVE_OPTIONS_REQUIRED")?;
    let cwd = options
        .get("workspaceRoot")
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute() && bounded_path_text(path))
        .ok_or("WORKSPACE_ROOT_MUST_BE_ABSOLUTE")?;

    let requested_model = selected_model_id(&command.route)?;

    let mut args = vec![
        "--input-format".to_owned(),
        "stream-json".to_owned(),
        "--output-format".to_owned(),
        "stream-json".to_owned(),
    ];
    args.push("--model".to_owned());
    args.push(requested_model.to_owned());
    if let Some(effort) = options.get("reasoningEffort") {
        let effort = bounded_option_string(effort, 32).ok_or("INVALID_EFFORT")?;
        if !matches!(effort.as_str(), "low" | "medium" | "high") {
            return Err("INVALID_EFFORT");
        }
        args.push("--effort".to_owned());
        args.push(effort);
    }
    if let Some(agent) = options.get("agent") {
        args.push("--agent".to_owned());
        args.push(bounded_option_string(agent, 256).ok_or("INVALID_AGENT")?);
    }
    if let Some(resume_id) = command.input.get("resume_conversation_id") {
        if command.method != "agent.open" {
            return Err("RESUME_ONLY_ALLOWED_ON_OPEN");
        }
        args.push("--conversation".to_owned());
        args.push(bounded_option_string(resume_id, 512).ok_or("INVALID_RESUME_ID")?);
    }
    if options
        .get("dangerouslySkipPermissions")
        .and_then(Value::as_bool)
        == Some(true)
    {
        args.push("--dangerously-skip-permissions".to_owned());
    }

    Ok(NativeLaunchSpec {
        executable: executable.to_owned(),
        args,
        cwd,
        stdin_piped: true,
        stdout_piped: true,
        stderr_piped: true,
        shell: false,
        kill_on_drop: false,
    })
}

pub fn selected_model_id(route: &Value) -> Result<&str, &'static str> {
    let options = route
        .get("native_options")
        .and_then(Value::as_object)
        .ok_or("NATIVE_OPTIONS_REQUIRED")?;
    let model = options
        .get("modelId")
        .and_then(Value::as_str)
        .ok_or("MODEL_ID_REQUIRED")?;
    if model.trim().is_empty() || model.chars().count() > 256 || model.chars().any(char::is_control)
    {
        return Err("INVALID_MODEL_ID");
    }
    Ok(model)
}

fn bounded_option_string(value: &Value, max_chars: usize) -> Option<String> {
    let value = value
        .as_str()
        .filter(|value| !value.trim().is_empty() && !value.chars().any(char::is_control))?;
    if value.chars().count() > max_chars {
        return None;
    }
    Some(value.to_owned())
}

fn bounded_path_text(path: &std::path::Path) -> bool {
    path.to_str()
        .is_some_and(|value| value.len() <= 32 * 1024 && !value.chars().any(char::is_control))
}
