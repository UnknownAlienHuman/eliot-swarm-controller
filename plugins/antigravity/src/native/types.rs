//! AGY 1.5.0 NDJSON, observed with gemini-3.8-flash-high on 2026-11-04.
//! Input/result have no request or turn id: correlation is single-flight,
//! confined to this uninterrupted process, never across a resumed generation.

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
#[serde(tag = "event")]
pub(crate) enum AgyFrame {
    #[serde(rename = "init")]
    Init { conversation_id: String, init: Init },
    #[serde(rename = "step_update")]
    StepUpdate { step_update: StepUpdate },
    #[serde(rename = "result")]
    Result { result: ResultRecord },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Init {
    pub model: String,
    pub cwd: String,
    pub permission_mode: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct StepUpdate {
    pub conversation_id: String,
    pub step_index: u64,
    pub state: String,
    pub step_type: String,
    pub tool_name: Option<String>,
    pub tool_info: Option<ToolInfo>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ToolInfo {
    pub error: Option<ToolError>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ToolError {
    pub r#type: String,
    pub message: String,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ResultRecord {
    pub conversation_id: String,
    pub status: String,
    pub response: String,
    pub error: Option<String>,
    pub finished_reason: Option<String>,
    pub num_turns: Option<u64>,
    pub usage: Option<Usage>,
    #[serde(default)]
    pub denied_actions: Vec<DeniedAction>,
}

impl ResultRecord {
    pub fn terminal_status(&self) -> eliot_core::Result<Option<&'static str>> {
        match self.status.as_str() {
            "SUCCESS" => Ok(Some("completed")),
            "ERROR" | "INVALID" => Ok(Some("failed")),
            "CANCELED" | "INTERRUPTED" => Ok(Some("cancelled")),
            "WAITING" | "RUNNING" => Ok(None),
            _ => Err(eliot_core::Error::new(
                "native_protocol",
                "Antigravity result status is unknown",
            )),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Usage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub thinking_tokens: u64,
    pub cache_read_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeniedAction {
    pub action: String,
    pub display_name: String,
}

#[derive(Serialize)]
pub(crate) struct UserInput<'a> {
    event: &'static str,
    message: UserMessage<'a>,
}

#[derive(Serialize)]
struct UserMessage<'a> {
    role: &'static str,
    content: &'a str,
}

impl<'a> UserInput<'a> {
    pub fn new(text: &'a str) -> Self {
        Self {
            event: "user",
            message: UserMessage {
                role: "user",
                content: text,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn observed_denial_does_not_turn_success_into_failure() {
        // Recorded stdout3.ndjson; only the conversation id and cwd are sanitized.
        let frames: Vec<AgyFrame> =
            include_str!("../../tests/fixtures/agy-1.5.0-soft-denial.ndjson")
                .lines()
                .map(|line| serde_json::from_str(line).expect("recorded frame"))
                .collect();
        let error = frames
            .iter()
            .find_map(|frame| match frame {
                AgyFrame::StepUpdate { step_update } if step_update.state == "ERROR" => {
                    step_update.tool_info.as_ref()?.error.as_ref()
                }
                _ => None,
            })
            .expect("recorded tool denial");
        assert!(error.message.contains("user denied permission"));
        let AgyFrame::Result { result } = frames.last().expect("result") else {
            panic!("result envelope")
        };
        assert_eq!(result.terminal_status().unwrap(), Some("completed"));
        assert_eq!(result.denied_actions[0].action, "command");
        assert_eq!(result.response, "");
        assert_eq!(result.usage.as_ref().unwrap().total_tokens, 9451);
    }

    #[test]
    fn input_has_exact_observed_envelope_and_json_escaping() {
        let text = "quote \" and newline\n and backslash \\";
        let raw = serde_json::to_value(UserInput::new(text)).unwrap();
        assert_eq!(
            raw,
            serde_json::json!({"event":"user","message":{"role":"user","content":text}})
        );
    }
}
