//! AGY 1.5.2 ACP (Agent Client Protocol v1) wire types, observed against the
//! pinned schema `v1-7ec5185c.json` and the installed `agy acp` server.
//!
//! The ACP server speaks JSON-RPC over stdio: requests are correlated by id
//! through the shared native transport, turn progress arrives as
//! `session/update` notifications, and the `session/prompt` response
//! (`stopReason`) is the terminal turn fact. There is no request or turn id
//! on the wire for a prompt: correlation is single-flight, confined to this
//! uninterrupted process and the exact Eliot turn prepared before dispatch,
//! never across a resumed generation.
//!
//! Hooks, steer and inject are NOT ACP methods (the pinned schema has no
//! such `x-method`); the later hook-callback increment will add its own
//! binding. This module stays strictly inside the pinned ACP surface.

use serde::Deserialize;
use serde_json::{Value, json};

/// `initialize` result. Only the negotiated version and the advertised
/// capability object are consumed; `authMethods` and `_meta` are never
/// decoded into owned state.
#[derive(Debug, Deserialize)]
pub(crate) struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    pub protocol_version: u64,
    #[serde(default, rename = "agentCapabilities")]
    pub agent_capabilities: Value,
}

/// `session/new` / `session/load` result.
#[derive(Debug, Deserialize)]
pub(crate) struct SessionResult {
    #[serde(rename = "sessionId")]
    pub session_id: String,
    #[serde(default)]
    pub modes: Option<ModeState>,
    #[serde(default)]
    #[serde(rename = "configOptions")]
    pub config_options: Option<Vec<ConfigOption>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ModeState {
    #[serde(rename = "currentModeId")]
    pub current_mode_id: String,
    #[serde(default, rename = "availableModes")]
    pub available_modes: Vec<SessionMode>,
}

/// `session/load` result. The pinned schema deliberately returns no
/// session id here: the caller retains the exact id from its correlated
/// request and validates it against the resume subject.
#[derive(Debug, Deserialize)]
pub(crate) struct LoadSessionResult {
    #[serde(default)]
    pub modes: Option<ModeState>,
    #[serde(default, rename = "configOptions")]
    pub config_options: Option<Vec<ConfigOption>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct SessionMode {
    pub id: String,
}

/// One `configOptions` entry. `options` stays a raw value because the schema
/// permits both a flat option array and grouped arrays; flattening happens
/// in [`ConfigOption::option_values`].
#[derive(Debug, Deserialize)]
pub(crate) struct ConfigOption {
    pub id: String,
    #[serde(default)]
    pub category: Option<String>,
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default, rename = "currentValue")]
    pub current_value: Value,
    #[serde(default)]
    pub options: Option<Value>,
}

impl ConfigOption {
    /// Flattened `(value, name)` pairs for a select option, in wire order.
    /// Grouped entries contribute their members with the group name folded
    /// into the label; structural surprises yield no pairs, never defaults.
    pub fn option_values(&self) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let Some(options) = self.options.as_ref().and_then(Value::as_array) else {
            return out;
        };
        for entry in options {
            if let (Some(value), Some(name)) = (
                entry.get("value").and_then(Value::as_str),
                entry.get("name").and_then(Value::as_str),
            ) {
                out.push((value.to_owned(), name.to_owned()));
            } else if let Some(members) = entry.get("options").and_then(Value::as_array) {
                let group = entry.get("name").and_then(Value::as_str).unwrap_or("");
                for member in members {
                    if let (Some(value), Some(name)) = (
                        member.get("value").and_then(Value::as_str),
                        member.get("name").and_then(Value::as_str),
                    ) {
                        out.push((
                            value.to_owned(),
                            if group.is_empty() {
                                name.to_owned()
                            } else {
                                format!("{group}: {name}")
                            },
                        ));
                    }
                }
            }
        }
        out
    }

    /// The current value as a plain string, when the option is a select.
    pub fn current_value_str(&self) -> Option<&str> {
        self.current_value.as_str()
    }
}

/// `session/set_config_option` result: the full echoed option set.
#[derive(Debug, Deserialize)]
pub(crate) struct SetConfigOptionResult {
    #[serde(rename = "configOptions")]
    pub config_options: Vec<ConfigOption>,
}

/// `session/prompt` result: the terminal turn fact.
#[derive(Debug, Deserialize)]
pub(crate) struct PromptResult {
    #[serde(rename = "stopReason")]
    pub stop_reason: String,
}

impl PromptResult {
    /// Map the pinned `StopReason` vocabulary onto Eliot turn statuses.
    /// `end_turn`/`max_tokens`/`max_turn_requests` end the turn normally,
    /// `cancelled` is the interrupt settlement, `refusal` fails the turn,
    /// and anything else is a native protocol violation.
    pub fn terminal_status(&self) -> eliot_core::Result<Option<&'static str>> {
        match self.stop_reason.as_str() {
            "end_turn" | "max_tokens" | "max_turn_requests" => Ok(Some("completed")),
            "cancelled" => Ok(Some("cancelled")),
            "refusal" => Ok(Some("failed")),
            _ => Err(eliot_core::Error::new(
                "native_protocol",
                "Antigravity prompt stop reason is unknown",
            )),
        }
    }
}

/// One classified `session/update` notification body.
#[derive(Debug)]
pub(crate) enum AcpUpdate {
    /// Assistant text chunk; appended to the current turn's final text.
    MessageChunk { text: String },
    /// Thought chunk: bookkeeping, never assistant output.
    ThoughtChunk,
    /// Echo of the user's own input: bookkeeping.
    UserChunk,
    /// A new tool call started.
    ToolCall {
        tool_call_id: String,
        title: String,
        status: String,
    },
    /// A tool call state change.
    ToolCallUpdate {
        tool_call_id: String,
        title: Option<String>,
        status: String,
    },
    /// Known metadata updates this adapter does not model (plan, mode,
    /// config, usage, notices, …): handled without input association.
    Ignored,
    /// A `sessionUpdate` kind outside the pinned vocabulary.
    Unknown(String),
}

/// Classify one `session/update` params object. Returns `None` when the
/// envelope itself (session id / update kind) is structurally invalid.
pub(crate) fn classify_update(params: &Value) -> Option<(String, AcpUpdate)> {
    let session_id = params.get("sessionId")?.as_str()?.to_owned();
    let update = params.get("update")?;
    let kind = update.get("sessionUpdate")?.as_str()?;
    let content_text = || {
        update
            .get("content")
            .and_then(|content| content.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    let tool_call_id = || {
        update
            .get("toolCallId")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned()
    };
    Some((
        session_id,
        match kind {
            "agent_message_chunk" => AcpUpdate::MessageChunk {
                text: content_text(),
            },
            "agent_thought_chunk" => AcpUpdate::ThoughtChunk,
            "user_message_chunk" => AcpUpdate::UserChunk,
            "tool_call" => AcpUpdate::ToolCall {
                tool_call_id: tool_call_id(),
                title: update
                    .get("title")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                status: update
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            },
            "tool_call_update" => AcpUpdate::ToolCallUpdate {
                tool_call_id: tool_call_id(),
                title: update
                    .get("title")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                status: update
                    .get("status")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
            },
            "plan"
            | "available_commands_update"
            | "current_mode_update"
            | "config_option_update"
            | "session_info_update"
            | "usage_update"
            | "notice"
            | "compaction_update"
            | "compaction_summary_chunk" => AcpUpdate::Ignored,
            other => AcpUpdate::Unknown(other.to_owned()),
        },
    ))
}

/// Build the exact `session/prompt` params for one text input.
pub(crate) fn prompt_params(session_id: &str, text: &str) -> Value {
    json!({
        "sessionId": session_id,
        "prompt": [{"type": "text", "text": text}],
    })
}
