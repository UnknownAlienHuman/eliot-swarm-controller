//! Claude stream-json wire shapes for `claude -p`.
//!
//! Field names follow the controller-verified CLI behavior (note 47, installed
//! CLI 2.1.278): stdin/stdout NDJSON with `control_request`/`control_response`
//! envelopes, `user` input records, `command_lifecycle` association,
//! `assistant` text, `system` lines and the terminal `result` record. Records
//! stay open: unknown fields are ignored, required fields are checked by the
//! caller that consumes them.

use eliot_core::TurnUsage;
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

/// Outbound stdin `control_response` answering one CLI `control_request`
/// (the `can_use_tool` permission reply). The CLI asks on stdout; Eliot
/// answers on stdin with the documented SDK `PermissionResult`.
#[derive(Serialize)]
pub(crate) struct OutboundControlResponse<T> {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub response: OutboundControlResponseBody<T>,
}

#[derive(Serialize)]
pub(crate) struct OutboundControlResponseBody<T> {
    pub subtype: &'static str,
    pub request_id: String,
    pub response: T,
}

/// Documented SDK permission decision (camelCase wire names). Allow must
/// echo the asked `input` back as `updatedInput`; deny carries a bounded
/// message. Live verification is pending the owner login (note 47).
#[derive(Serialize)]
#[serde(tag = "behavior")]
pub(crate) enum PermissionResult {
    #[serde(rename = "allow")]
    Allow {
        #[serde(rename = "updatedInput")]
        updated_input: Value,
    },
    #[serde(rename = "deny")]
    Deny { message: String },
}

/// Inbound `can_use_tool` request body (the `request` object of a CLI
/// `control_request`). Only the decision fields are consumed; suggestions
/// and reasons stay out of the persisted details.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CanUseToolBody {
    #[serde(default)]
    pub subtype: String,
    #[serde(default)]
    pub tool_name: String,
    #[serde(default)]
    pub tool_use_id: Option<String>,
    #[serde(default)]
    pub input: Option<Value>,
}

/// Installed 2.1.278 `hook_callback` body. The native input and ids stay
/// private to this plugin; the event path uses only the bound Eliot turn and
/// a separately minted opaque consumer id.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct HookCallbackBody {
    #[serde(default)]
    pub subtype: String,
    #[serde(default)]
    pub callback_id: String,
    pub input: Value,
    #[serde(default)]
    pub tool_use_id: Option<String>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
}

/// Native withdrawal of a pending `control_request`; there is no reply to it.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ControlCancelRequest {
    #[serde(rename = "type")]
    pub kind: String,
    pub request_id: String,
}

/// `hook_callback` success payload. Empty means pass with no override.
#[derive(Clone, Debug, Default, Serialize)]
pub(crate) struct HookCallbackResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hook_specific_output: Option<PreToolUseHookOutput>,
}

impl HookCallbackResponse {
    pub fn with_context(additional_context: String) -> Self {
        Self {
            hook_specific_output: Some(PreToolUseHookOutput {
                hook_event_name: "PreToolUse",
                additional_context,
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PreToolUseHookOutput {
    pub hook_event_name: &'static str,
    pub additional_context: String,
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
    #[serde(default)]
    pub hooks_applied: Option<bool>,
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
    #[serde(default)]
    pub id: Option<String>,
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

/// Extract one turn's native token usage from a terminal `result` record's
/// `usage`. With persistent streaming input, `result.usage` covers only the
/// current user turn's main loop (subagents excluded); `result.modelUsage`
/// and `total_cost_usd` are cumulative/estimated and are never read here.
/// Only genuinely supplied nonnegative integer counters are mapped; missing
/// or malformed values stay null (never zero, never a sum), and an absent or
/// wholly malformed `usage` yields no usage at all so completion proceeds.
pub(crate) fn result_usage(record: &Value) -> Option<TurnUsage> {
    let usage = record.get("usage")?.as_object()?;
    let counter = |key: &str| {
        usage
            .get(key)
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0)
    };
    let input_tokens = counter("input_tokens");
    let output_tokens = counter("output_tokens");
    let cache_read_tokens = counter("cache_read_input_tokens");
    let cache_write_tokens = counter("cache_creation_input_tokens");
    if input_tokens.is_none()
        && output_tokens.is_none()
        && cache_read_tokens.is_none()
        && cache_write_tokens.is_none()
    {
        return None;
    }
    Some(TurnUsage {
        scope: Some("main_loop_turn".to_owned()),
        message_id: None,
        input_tokens,
        output_tokens,
        reasoning_tokens: None,
        thinking_tokens: None,
        cache_read_tokens,
        cache_write_tokens,
        total_tokens: None,
    })
}

/// Extract the model reported by an `assistant` record's message.
/// Only a non-empty string scalar is observed; missing/null is unobserved.
pub(crate) fn assistant_model(record: &Value) -> Option<String> {
    record
        .get("message")
        .and_then(|message| message.get("model"))
        .and_then(Value::as_str)
        .filter(|model| !model.trim().is_empty())
        .map(ToOwned::to_owned)
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

/// Tool-use ids in one assistant record. The event processor binds these
/// native invocation ids to the exact turn already associated by the stream.
pub(crate) fn assistant_tool_use_ids(record: &Value) -> Vec<String> {
    let Some(blocks) = record
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    blocks
        .iter()
        .filter_map(|block| serde_json::from_value::<AssistantContentBlock>(block.clone()).ok())
        .filter(|block| block.kind == "tool_use")
        .filter_map(|block| block.id)
        .filter(|id| !id.trim().is_empty())
        .collect()
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
    fn permission_reply_uses_the_documented_sdk_shape() {
        let allow = OutboundControlResponse {
            kind: "control_response",
            response: OutboundControlResponseBody {
                subtype: "success",
                request_id: "req-1".to_owned(),
                response: PermissionResult::Allow {
                    updated_input: json!({"command": "ls"}),
                },
            },
        };
        assert_eq!(
            serde_json::to_value(&allow).expect("allow reply"),
            json!({
                "type": "control_response",
                "response": {
                    "subtype": "success",
                    "request_id": "req-1",
                    "response": {"behavior": "allow", "updatedInput": {"command": "ls"}},
                },
            })
        );
        let deny = OutboundControlResponse {
            kind: "control_response",
            response: OutboundControlResponseBody {
                subtype: "success",
                request_id: "req-2".to_owned(),
                response: PermissionResult::Deny {
                    message: "Eliot denied this tool call.".to_owned(),
                },
            },
        };
        assert_eq!(
            serde_json::to_value(&deny).expect("deny reply")["response"]["response"],
            json!({"behavior": "deny", "message": "Eliot denied this tool call."})
        );
    }

    #[test]
    fn can_use_tool_decodes_the_documented_fields() {
        let raw = json!({
            "subtype": "can_use_tool",
            "tool_name": "Bash",
            "tool_use_id": "tool-1",
            "input": {"command": "ls"},
            "permission_suggestions": [],
            "decision_reason": "wants to list files",
        });
        let body: CanUseToolBody = serde_json::from_value(raw).expect("can_use_tool");
        assert_eq!(body.subtype, "can_use_tool");
        assert_eq!(body.tool_name, "Bash");
        assert_eq!(body.tool_use_id.as_deref(), Some("tool-1"));
        assert_eq!(body.input, Some(json!({"command": "ls"})));
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
