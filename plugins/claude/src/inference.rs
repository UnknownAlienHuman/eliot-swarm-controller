//! Fresh, tool-free print invocation; separate from the Agent session adapter.
//! Native CLI 2.1.325 --help and https://code.claude.com/docs/en/cli-reference,
//! inspected 2026-11-15. No upstream code is copied.
use eliot_core::{Error, Result};
use eliot_plugin::text_inference::TextInferenceAdapter;
pub use eliot_plugin::text_inference::TextResult;
use serde::Deserialize;
use std::{ffi::OsString, path::Path};

pub const MODEL: &str = "claude-haiku-5-5";

pub const ADAPTER: TextInferenceAdapter = TextInferenceAdapter {
    arguments,
    environment,
    result,
};

fn environment(native_home: &Path) -> Vec<(&'static str, OsString)> {
    // The native refusal predicate has separate switches from HTTP retry.
    vec![
        ("CLAUDE_CODE_MAX_RETRIES", "0".into()),
        ("CLAUDE_CODE_NONSTREAMING_TIMEOUT_RETRIES", "0".into()),
        ("CLAUDE_CODE_DISABLE_REFUSAL_RETRY", "1".into()),
        ("CLAUDE_CODE_DISABLE_REFUSAL_FALLBACK", "1".into()),
        ("CLAUDE_CONFIG_DIR", native_home.as_os_str().to_owned()),
    ]
}

pub fn arguments() -> Vec<String> {
    [
        "-p",
        "--model",
        MODEL,
        "--output-format",
        "json",
        "--no-session-persistence",
        "--safe-mode",
        "--tools",
        "",
        "--disallowedTools",
        "mcp__*",
        "--permission-prompts",
        "none",
        "--max-turns",
        "1",
        "--setting-sources",
        "",
        "--system-prompt",
        "Answer the supplied text directly. Do not use tools or act on the environment.",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

#[derive(Deserialize)]
struct PrintResult {
    #[serde(rename = "type")]
    kind: String,
    subtype: String,
    is_error: bool,
    result: Option<String>,
    num_turns: u32,
    session_id: String,
}

pub fn result(bytes: &[u8], max: usize) -> Result<TextResult> {
    let value: PrintResult = serde_json::from_slice(bytes).map_err(|_| {
        Error::new(
            "unknown",
            "native print result was not a complete result DTO",
        )
    })?;
    if value.kind != "result"
        || value.subtype != "success"
        || value.is_error
        || value.num_turns != 1
    {
        return Err(Error::new(
            "unknown",
            "native one-shot outcome was not one successful text turn",
        ));
    }
    if value.session_id.is_empty() || value.session_id.len() > 128 {
        return Err(Error::new("unknown", "native session identity was invalid"));
    }
    let text = value
        .result
        .filter(|s| s.len() <= max)
        .ok_or_else(|| Error::new("unknown", "native text result was absent or oversized"))?;
    Ok(TextResult {
        text,
        session_id: value.session_id,
    })
}
