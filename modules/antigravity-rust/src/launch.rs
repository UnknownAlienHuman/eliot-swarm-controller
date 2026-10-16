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
    // The installed harness owns its model default. Pass --model only for an
    // exact explicit route override, mirroring the Node bridge's conditional
    // (bridge.mjs/bridge-v4.mjs omit --model when modelId is absent).
    if let Some(requested_model) = requested_model {
        args.push("--model".to_owned());
        args.push(requested_model.to_owned());
    }
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

/// Exact explicit route model override, or `None` when the route names no
/// model. An absent `modelId` is not an error: the launch omits `--model` and
/// the installed harness default applies. A present value must be the exact
/// bounded model string; empty, overlong, control-carrying, or non-string
/// values are rejected without guessing.
pub fn selected_model_id(route: &Value) -> Result<Option<&str>, &'static str> {
    let options = route
        .get("native_options")
        .and_then(Value::as_object)
        .ok_or("NATIVE_OPTIONS_REQUIRED")?;
    let Some(value) = options.get("modelId") else {
        return Ok(None);
    };
    let model = value.as_str().ok_or("INVALID_MODEL_ID")?;
    if model.trim().is_empty() || model.chars().count() > 256 || model.chars().any(char::is_control)
    {
        return Err("INVALID_MODEL_ID");
    }
    Ok(Some(model))
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

#[cfg(test)]
mod model_override_tests {
    use super::*;
    use serde_json::json;

    fn workspace_root() -> String {
        std::env::temp_dir()
            .to_str()
            .expect("test workspace root must be representable")
            .to_owned()
    }

    fn executable() -> PathBuf {
        std::env::temp_dir().join("agy-test-executable")
    }

    fn open_command(route: Value) -> RuntimeCommand {
        RuntimeCommand {
            operation_id: "op-open-model".to_owned(),
            method: "agent.open".to_owned(),
            created_at_ms: 1,
            binding_id: "binding-model".to_owned(),
            generation: 1,
            native_root_id: None,
            route,
            input: json!({}),
            input_sha256: None,
            target_input_sha256: None,
        }
    }

    fn route_with_model(model: Value) -> Value {
        let mut options = serde_json::Map::new();
        options.insert("workspaceRoot".to_owned(), Value::String(workspace_root()));
        options.insert("modelId".to_owned(), model);
        json!({"native_options": Value::Object(options)})
    }

    #[test]
    fn absent_model_id_omits_flag_for_native_default() {
        let route = json!({"native_options": {"workspaceRoot": workspace_root()}});
        assert_eq!(selected_model_id(&route).unwrap(), None);
        let spec = build_launch_spec(&executable(), &open_command(route)).unwrap();
        assert!(!spec.args.contains(&"--model".to_owned()));
        assert_eq!(
            &spec.args[..4],
            &[
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json"
            ]
        );
    }

    #[test]
    fn explicit_model_id_is_passed_exactly() {
        let route = route_with_model(json!("gemini-explicit-override"));
        assert_eq!(
            selected_model_id(&route).unwrap(),
            Some("gemini-explicit-override")
        );
        let spec = build_launch_spec(&executable(), &open_command(route)).unwrap();
        let position = spec
            .args
            .iter()
            .position(|arg| arg == "--model")
            .expect("explicit override must pass --model");
        assert_eq!(spec.args[position + 1], "gemini-explicit-override");
    }

    #[test]
    fn malformed_model_override_is_rejected_without_guessing() {
        for invalid in [
            json!(""),
            json!("   "),
            json!("bad\u{7}model"),
            json!("x".repeat(257)),
            json!(42),
            json!(Value::Null),
            json!({"id": "nested"}),
        ] {
            assert_eq!(
                selected_model_id(&route_with_model(invalid)).unwrap_err(),
                "INVALID_MODEL_ID"
            );
        }
        assert_eq!(
            selected_model_id(&json!({})).unwrap_err(),
            "NATIVE_OPTIONS_REQUIRED"
        );
    }
}
