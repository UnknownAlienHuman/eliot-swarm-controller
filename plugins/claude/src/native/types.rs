//! Claude stream-json wire shapes for `claude -p`.
//!
//! Field names follow the controller-verified CLI behavior (note 47, installed
//! CLI 2.1.278): stdin/stdout NDJSON with `control_request`/`control_response`
//! envelopes, `user` input records, `command_lifecycle` association,
//! `assistant` text, `system` lines and the terminal `result` record. Records
//! stay open: unknown fields are ignored, required fields are checked by the
//! caller that consumes them.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Outbound stdin control envelope.
#[derive(Serialize)]
pub(crate) struct OutboundControlRequest {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub request_id: String,
    pub request: Value,
}

/// Outbound stdin user-input record. `uuid` (Eliot-minted) is the exact turn
/// association; the CLI echoes it as `command_lifecycle.command_uuid`.
#[derive(Serialize)]
pub(crate) struct OutboundUserInput {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub uuid: String,
    pub session_id: String,
    pub parent_tool_use_id: Option<String>,
    pub message: OutboundUserMessage,
}

#[derive(Serialize)]
pub(crate) struct OutboundUserMessage {
    pub role: &'static str,
    pub content: String,
}

/// Inbound `control_response` envelope. The reader routes on the raw
/// `type` first, so only the correlated body is decoded here.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ControlResponse {
    pub response: ControlResponseBody,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ControlResponseBody {
    pub subtype: String,
    pub request_id: String,
    #[serde(default)]
    pub response: Option<Value>,
}

/// Successful `initialize` readback: the effective profile.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct InitializeReadback {
    #[serde(default)]
    pub models: Vec<InitializeModel>,
    #[serde(default)]
    pub current_permission_mode: Option<String>,
    #[serde(default)]
    pub pid: Option<u32>,
    #[serde(default)]
    pub session_state: Option<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct InitializeModel {
    #[serde(default)]
    pub value: Option<String>,
    #[serde(rename = "resolvedModel", default)]
    pub resolved_model: Option<String>,
}

/// `command_lifecycle` association record.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CommandLifecycle {
    #[serde(default)]
    pub command_uuid: String,
    #[serde(default)]
    pub state: String,
}

/// One `assistant` content block; only `text` blocks carry reply text.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AssistantContentBlock {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub text: Option<String>,
}

/// Terminal `result` record. Bound to the exact active turn (single flight);
/// the record itself carries no command uuid.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ResultRecord {
    #[serde(default)]
    pub terminal_reason: String,
    #[serde(default)]
    pub session_id: Option<String>,
}

/// Extract concatenated text from an `assistant` record's content blocks.
pub(crate) fn assistant_text(record: &Value) -> String {
    let mut out = String::new();
    let blocks = record
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array);
    let Some(blocks) = blocks else {
        return out;
    };
    for block in blocks {
        let parsed: Result<AssistantContentBlock, _> = serde_json::from_value(block.clone());
        let Ok(parsed) = parsed else { continue };
        if parsed.kind == "text"
            && let Some(text) = parsed.text
            && !text.is_empty()
        {
            out.push_str(&text);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn assistant_text_keeps_only_text_blocks() {
        let record = json!({
            "type": "assistant",
            "message": {"content": [
                {"type": "text", "text": "hello "},
                {"type": "tool_use", "name": "Bash"},
                {"type": "text", "text": "world"},
                {"type": "text"},
            ]},
        });
        assert_eq!(assistant_text(&record), "hello world");
        assert_eq!(assistant_text(&json!({"type": "assistant"})), "");
    }

    #[test]
    fn initialize_readback_decodes_the_verified_shape() {
        let raw = json!({
            "models": [{"value": "opus", "resolvedModel": "claude-opus-4-6"}],
            "current_permission_mode": "default",
            "pid": 1234,
            "session_state": "idle",
            "account": {"uuid": "owner"},
        });
        let readback: InitializeReadback = serde_json::from_value(raw).expect("readback");
        assert_eq!(readback.models.len(), 1);
        assert_eq!(
            readback.models[0].resolved_model.as_deref(),
            Some("claude-opus-4-6")
        );
        assert_eq!(readback.current_permission_mode.as_deref(), Some("default"));
        assert_eq!(readback.pid, Some(1234));
        assert_eq!(readback.session_state.as_deref(), Some("idle"));
    }
}
