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

use eliot_core::{SessionCost, SessionUsage};
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
    /// Context occupancy and cumulative session cost from `usage_update`:
    /// tokens currently in context, context-window capacity, and the
    /// optional cumulative session cost. Session-scoped facts, never
    /// per-turn input/output totals.
    SessionUsage {
        used: Option<i64>,
        size: Option<i64>,
        cost_amount: Option<f64>,
        cost_unit: Option<String>,
    },
    /// Known metadata updates this adapter does not model (plan, mode,
    /// config, notices, …): handled without input association.
    Ignored,
    /// A `sessionUpdate` kind outside the pinned vocabulary.
    Unknown(String),
}

/// One nonnegative `usage_update` token counter. Negative values and
/// non-integers stay unknown (null), never zero.
fn usage_counter(value: Option<&Value>) -> Option<i64> {
    value.and_then(Value::as_i64).filter(|value| *value >= 0)
}

/// One finite nonnegative `usage_update` cost amount, from a bare number or
/// a cost object's amount/value/total member. Anything else stays unknown.
fn usage_cost_amount(cost: Option<&Value>) -> Option<f64> {
    let amount = match cost {
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::Object(cost)) => ["amount", "value", "total"]
            .iter()
            .filter_map(|key| cost.get(*key))
            .filter_map(Value::as_f64)
            .next(),
        _ => None,
    };
    amount.filter(|amount| amount.is_finite() && *amount >= 0.0)
}

/// One verbatim `usage_update` cost unit, from a cost object's
/// currency/unit/code member. Absent or blank stays unknown, never invented.
fn usage_cost_unit(cost: Option<&Value>) -> Option<String> {
    cost.and_then(Value::as_object)
        .and_then(|cost| {
            ["currency", "unit", "currencyCode"]
                .iter()
                .filter_map(|key| cost.get(*key))
                .filter_map(Value::as_str)
                .next()
        })
        .filter(|unit| !unit.trim().is_empty())
        .map(str::to_owned)
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
            "usage_update" => AcpUpdate::SessionUsage {
                used: usage_counter(update.get("used")),
                size: usage_counter(update.get("size")),
                cost_amount: usage_cost_amount(update.get("cost")),
                cost_unit: usage_cost_unit(update.get("cost")),
            },
            "plan"
            | "available_commands_update"
            | "current_mode_update"
            | "config_option_update"
            | "session_info_update"
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

/// Merge one `usage_update` body into the retained session snapshot:
/// `used` replaces the retained context occupancy, `size` the retained
/// window capacity, and a supplied cost amount replaces the retained
/// cumulative cost with its verbatim unit. Members the update omits keep the
/// retained values; yields `None` when the update contributes nothing new.
pub(crate) fn session_usage_from_update(
    used: Option<i64>,
    size: Option<i64>,
    cost_amount: Option<f64>,
    cost_unit: Option<String>,
    previous: Option<&SessionUsage>,
) -> Option<SessionUsage> {
    let mut merged = previous.cloned().unwrap_or(SessionUsage {
        context_used_tokens: None,
        context_window_tokens: None,
        cumulative_tokens: None,
        cumulative_cost: None,
        source: None,
        observed_at: None,
    });
    let mut changed = false;
    if used.is_some_and(|used| merged.context_used_tokens != Some(used)) {
        merged.context_used_tokens = used;
        changed = true;
    }
    if size.is_some_and(|size| merged.context_window_tokens != Some(size)) {
        merged.context_window_tokens = size;
        changed = true;
    }
    if let Some(amount) = cost_amount {
        let cost = SessionCost {
            amount: Some(amount),
            unit: cost_unit,
        };
        if merged.cumulative_cost.as_ref() != Some(&cost) {
            merged.cumulative_cost = Some(cost);
            changed = true;
        }
    }
    if !changed {
        return None;
    }
    merged.source = Some("antigravity:usage_update".to_owned());
    merged.observed_at = None;
    Some(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn update(update: Value) -> (String, AcpUpdate) {
        classify_update(&json!({"sessionId": "session-1", "update": update}))
            .expect("update classifies")
    }

    #[test]
    fn usage_update_parses_occupancy_and_verbatim_cost() {
        let (session_id, parsed) = update(json!({
            "sessionUpdate": "usage_update",
            "used": 4096,
            "size": 200000,
            "cost": {"amount": 1.5, "currency": "USD"},
        }));
        assert_eq!(session_id, "session-1");
        let AcpUpdate::SessionUsage {
            used,
            size,
            cost_amount,
            cost_unit,
        } = parsed
        else {
            panic!("usage_update must parse as session usage");
        };
        assert_eq!(used, Some(4096));
        assert_eq!(size, Some(200000));
        assert_eq!(cost_amount, Some(1.5));
        assert_eq!(cost_unit.as_deref(), Some("USD"));
        // Bare numbers, negatives and unknown shapes stay tolerant.
        let (_, bare) = update(json!({"sessionUpdate": "usage_update", "used": 7, "cost": 2.0}));
        assert!(matches!(
            bare,
            AcpUpdate::SessionUsage {
                used: Some(7),
                size: None,
                cost_amount: Some(_),
                cost_unit: None,
            }
        ));
        let (_, negative) = update(json!({
            "sessionUpdate": "usage_update",
            "used": -1,
            "size": -2,
            "cost": {"amount": -0.5},
        }));
        assert!(matches!(
            negative,
            AcpUpdate::SessionUsage {
                used: None,
                size: None,
                cost_amount: None,
                cost_unit: _,
            }
        ));
    }

    #[test]
    fn usage_update_merges_occupancy_and_cost_without_turn_counters() {
        let merged = session_usage_from_update(
            Some(4096),
            Some(200000),
            Some(1.5),
            Some("USD".to_owned()),
            None,
        )
        .expect("occupancy merges");
        assert_eq!(merged.context_used_tokens, Some(4096));
        assert_eq!(merged.context_window_tokens, Some(200000));
        let cost = merged.cumulative_cost.as_ref().expect("cost");
        assert_eq!(cost.amount, Some(1.5));
        assert_eq!(cost.unit.as_deref(), Some("USD"));
        assert_eq!(merged.cumulative_tokens, None);
        assert_eq!(merged.source.as_deref(), Some("antigravity:usage_update"));
        // A repeat contributes nothing new, so it commits nothing.
        assert_eq!(
            session_usage_from_update(
                Some(4096),
                Some(200000),
                Some(1.5),
                Some("USD".to_owned()),
                Some(&merged)
            ),
            None
        );
        // An occupancy drop (compaction) replaces, never reads as spend.
        let compacted = session_usage_from_update(Some(1024), None, None, None, Some(&merged))
            .expect("compaction merges");
        assert_eq!(compacted.context_used_tokens, Some(1024));
        assert_eq!(compacted.context_window_tokens, Some(200000));
    }

    #[test]
    fn live_turn_without_usage_update_leaves_both_scopes_null() {
        // Live 2026-11-14: agy 1.5.3 + gemini-3.8-flash-high, one short
        // task (read task.txt, reply 42). The ACP session emitted
        // message/tool notifications but no `usage_update`, and the
        // `session/prompt` result carried only `stopReason`. Checked
        // native surfaces: prompt result (`usage`/`_meta` absent),
        // `session/update` stream (no `usage_update` kind), PreInvocation
        // hook input (conversation/invocation/step counts only).
        let prompt: PromptResult = serde_json::from_value(json!({
            "stopReason": "end_turn",
        }))
        .expect("real prompt result parses");
        assert_eq!(
            prompt.terminal_status().expect("known stop"),
            Some("completed")
        );
        // Checked-but-absent usage surfaces stay tolerated and still yield
        // no turn usage: unknown members are ignored, never mapped.
        let with_extras: PromptResult = serde_json::from_value(json!({
            "stopReason": "end_turn",
            "usage": {"inputTokens": 10, "outputTokens": 2},
            "_meta": {"usage": {"input": 10}},
        }))
        .expect("prompt extras stay tolerated");
        assert_eq!(
            with_extras.terminal_status().expect("known stop"),
            Some("completed")
        );

        // Real notification shapes from the live turn: assistant text 42
        // and the File task.txt tool call. None is a session-usage fact.
        let (_, message) = update(json!({
            "sessionUpdate": "agent_message_chunk",
            "content": {"text": "42"},
        }));
        assert!(matches!(message, AcpUpdate::MessageChunk { text } if text == "42"));
        let (_, tool) = update(json!({
            "sessionUpdate": "tool_call",
            "toolCallId": "tool-call_11475363737043308230",
            "title": "File task.txt",
            "status": "in_progress",
        }));
        assert!(matches!(tool, AcpUpdate::ToolCall { .. }));
        let (_, tool_update) = update(json!({
            "sessionUpdate": "tool_call_update",
            "toolCallId": "tool-call_11475363737043308230",
            "title": "File /C:/Development/Rust/qa-scratch/k4-workspace/task.txt",
            "status": "completed",
        }));
        assert!(matches!(tool_update, AcpUpdate::ToolCallUpdate { .. }));

        // No `usage_update` in the sequence, so no session snapshot merges:
        // both turn and session scopes stay null (never estimated).
        assert_eq!(
            session_usage_from_update(None, None, None, None, None),
            None
        );
    }
}
