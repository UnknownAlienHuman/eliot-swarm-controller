//! Zed v1.28.1 eval-cli invocation and artifact contract.

use std::{
    ffi::OsString,
    fs::File,
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{MAX_TEXT_BYTES, native_transport::NativeLaunch};
use serde::Deserialize;

pub const NATIVE_TIMEOUT: u64 = 600;
const RESULT_LIMIT: usize = 64 * 1024;
const THREAD_LIMIT: usize = 4 * 1024 * 1024;

// Keep unrelated profile values out of the native process. Zed authenticates
// all registered providers at startup, including providers it will not use.
const PROVIDER_ENV: &[(&str, &str)] = &[
    ("anthropic", "ANTHROPIC_API_KEY"),
    ("openai", "OPENAI_API_KEY"),
    ("google", "GEMINI_API_KEY"),
    ("google", "GOOGLE_AI_API_KEY"),
    ("ollama", "OLLAMA_API_KEY"),
];

pub fn supports_model(model: &str) -> bool {
    model
        .split_once('/')
        .is_some_and(|(provider, _)| PROVIDER_ENV.iter().any(|(id, _)| *id == provider))
}

pub fn command(
    program: &Path,
    workspace: &Path,
    root: &Path,
    model: &str,
    mut environment: Vec<(OsString, OsString)>,
) -> NativeLaunch {
    let provider = model.split_once('/').map(|(provider, _)| provider);
    environment.retain(|(name, _)| {
        name == "PATH"
            || PROVIDER_ENV
                .iter()
                .any(|(id, key)| Some(*id) == provider && name == *key)
    });
    environment.extend(
        [
            ("HOME", "home"),
            ("XDG_CONFIG_HOME", "config"),
            ("XDG_DATA_HOME", "data"),
            ("XDG_STATE_HOME", "state"),
            ("XDG_CACHE_HOME", "cache"),
            ("XDG_RUNTIME_DIR", "runtime"),
            ("TMPDIR", "tmp"),
        ]
        .map(|(name, dir)| (OsString::from(name), root.join(dir).into_os_string())),
    );
    // Disable automatic D-Bus session discovery and leave existing OS
    // desktop credentials outside this invocation's auth scope.
    environment.push((
        "DBUS_SESSION_BUS_ADDRESS".into(),
        format!("unix:path={}", root.join("unavailable-bus").display()).into(),
    ));
    NativeLaunch {
        program: program.into(),
        cwd: workspace.into(),
        args: vec![
            "--workdir".into(),
            workspace.into(),
            "--model".into(),
            model.into(),
            "--timeout".into(),
            NATIVE_TIMEOUT.to_string().into(),
            "--output-dir".into(),
            root.join("output").into_os_string(),
            "--no-staff".into(),
        ],
        environment,
    }
}

pub fn executable(program: &str) -> Result<(PathBuf, eliot_plugin::FileIdentity)> {
    let path = PathBuf::from(program);
    if !path.is_absolute() || path.file_name().and_then(|value| value.to_str()) != Some("eval-cli")
    {
        return Err(Error::new(
            "profile_error",
            "Zed requires a resolved eval-cli path",
        ));
    }
    let file = File::open(&path)
        .and_then(|file| {
            if file.metadata()?.is_file() {
                eliot_plugin::file_identity(&file)
            } else {
                Err(std::io::Error::other("expected a regular executable"))
            }
        })
        .map_err(|_| Error::new("profile_error", "Zed eval-cli identity could not be read"))?;
    Ok((path, file))
}

#[derive(Deserialize)]
struct Receipt {
    status: String,
    model: String,
}

pub fn terminal_status(exit: Option<i32>, result: &[u8], model: &str) -> Option<&'static str> {
    let receipt: Receipt = serde_json::from_slice(result).ok()?;
    if receipt.model != model {
        return None;
    }
    match (exit, receipt.status.as_str()) {
        (Some(0), "completed") => Some("completed"),
        (Some(1), "error") | (Some(2), "timeout") => Some("failed"),
        (Some(3), "interrupted") => Some("interrupted"),
        _ => None,
    }
}

pub async fn read_terminal(root: &Path, exit: Option<i32>, model: &str) -> Option<&'static str> {
    let path = root.join("output/result.json");
    let result = tokio::task::spawn_blocking(move || {
        eliot_plugin::read_native_artifact(&path, RESULT_LIMIT)
    })
    .await
    .ok()?
    .ok()?;
    terminal_status(exit, &result, model)
}

#[derive(Deserialize)]
struct Thread {
    messages: Vec<Message>,
}

#[derive(Deserialize)]
struct AgentMessage {
    content: Vec<Content>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Message {
    Agent {
        #[serde(rename = "Agent")]
        agent: AgentMessage,
    },
    Other(serde::de::IgnoredAny),
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Content {
    Text {
        #[serde(rename = "Text")]
        text: String,
    },
    Other(serde::de::IgnoredAny),
}

pub fn final_answer(thread: &[u8], policy: &SecretPolicy) -> Option<String> {
    let thread = serde_json::from_slice::<Thread>(thread).ok()?;
    let content = thread
        .messages
        .into_iter()
        .rev()
        .find_map(|message| match message {
            Message::Agent { agent } => Some(agent.content),
            Message::Other(_) => None,
        })?;
    let text = content
        .into_iter()
        .filter_map(|chunk| match chunk {
            Content::Text { text } => Some(text),
            Content::Other(_) => None,
        })
        .collect::<Vec<_>>()
        .join("\n");
    let text = policy.redact_text(&text);
    (!text.trim().is_empty() && text.len() <= MAX_TEXT_BYTES).then_some(text)
}

pub async fn read_answer(root: &Path, policy: &SecretPolicy) -> Option<String> {
    let path = root.join("output/thread.json");
    let thread = tokio::task::spawn_blocking(move || {
        eliot_plugin::read_native_artifact(&path, THREAD_LIMIT)
    })
    .await
    .ok()?
    .ok()?;
    final_answer(&thread, policy)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn zed_terminal_requires_matching_exit_status_and_model() {
        for (code, status, expected) in [
            (0, "completed", "completed"),
            (1, "error", "failed"),
            (2, "timeout", "failed"),
            (3, "interrupted", "interrupted"),
        ] {
            let bytes =
                serde_json::to_vec(&json!({"status": status, "model": "openai/test"})).unwrap();
            assert_eq!(
                terminal_status(Some(code), &bytes, "openai/test"),
                Some(expected)
            );
            assert_eq!(terminal_status(Some(code + 1), &bytes, "openai/test"), None);
            assert_eq!(terminal_status(Some(code), &bytes, "openai/other"), None);
            assert_eq!(terminal_status(None, &bytes, "openai/test"), None);
        }
        assert_eq!(terminal_status(Some(0), b"{}", "openai/test"), None);
    }

    #[test]
    fn zed_keeps_only_final_assistant_text() {
        let thread = json!({"messages": [
            {"User":{"content":[{"Text":"private prompt"}]}},
            {"Agent":{"content":[{"Text":"first"},{"ToolUse":{"tool":"other"}}]}},
            {"Agent":{"content":[{"Thinking":{"text":"private thought"}},{"Text":"answer secret-value"}]}}
        ]});
        let bytes = serde_json::to_vec(&thread).unwrap();
        let text = final_answer(&bytes, &SecretPolicy::new(["secret-value"])).unwrap();
        assert!(text.starts_with("answer "));
        assert!(!text.contains("secret-value"));
        assert!(!text.contains("first"));
    }

    #[test]
    fn zed_launch_uses_private_directories_and_explicit_provider_environment() {
        let program = Path::new("/bin/eval-cli");
        let workspace = Path::new("/project");
        let root = Path::new("/private");
        let launch = command(
            program,
            workspace,
            root,
            "openai/test",
            vec![
                ("HOME".into(), "/owner".into()),
                ("OPENAI_API_KEY".into(), "expected".into()),
                ("ANTHROPIC_API_KEY".into(), "excluded".into()),
            ],
        );
        assert!(!launch.args.contains(&"--instruction".into()));
        assert!(launch.args.contains(&root.join("output").into_os_string()));
        assert!(
            launch
                .environment
                .contains(&("HOME".into(), root.join("home").into_os_string()))
        );
        assert!(
            launch
                .environment
                .contains(&("OPENAI_API_KEY".into(), "expected".into()))
        );
        assert!(
            !launch
                .environment
                .iter()
                .any(|(name, _)| name == "ANTHROPIC_API_KEY")
        );
    }
}
