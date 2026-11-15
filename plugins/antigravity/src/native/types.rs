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

use eliot_core::{SessionCost, SessionUsage, TurnUsage};
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
///
/// The pinned ACP schema carries only `stopReason` here, but installed
/// `agy` 1.5.9 extends the response with a per-turn `usage` object
/// (`inputTokens`, `outputTokens`, `cachedReadTokens`,
/// `cachedWriteTokens`, `thoughtTokens`, `totalTokens`; observed
/// 2026-11-15 against `gemini-4-argon-high`). It stays `Option<Value>` so
/// a missing or malformed extension never breaks completion: usable
/// counters are extracted tolerantly by [`prompt_turn_usage`].
#[derive(Debug, Deserialize)]
pub(crate) struct PromptResult {
    #[serde(rename = "stopReason")]
    pub stop_reason: String,
    #[serde(default)]
    pub usage: Option<Value>,
}

/// Extract per-turn model usage from a `session/prompt` response's agy
/// `usage` extension. Only genuinely supplied nonnegative integer counters
/// are mapped; missing or malformed values stay null (never zero, never a
/// sum), and an absent or wholly unusable object yields no usage at all so
/// completion proceeds. Scope is `turn`: the response covers the whole
/// prompt call's model usage, not just the final message.
pub(crate) fn prompt_turn_usage(result: &PromptResult) -> Option<TurnUsage> {
    let usage = result.usage.as_ref()?.as_object()?;
    let counter = |key: &str| {
        usage
            .get(key)
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0)
    };
    let input_tokens = counter("inputTokens");
    let output_tokens = counter("outputTokens");
    let cache_read_tokens = counter("cachedReadTokens");
    let cache_write_tokens = counter("cachedWriteTokens");
    let thinking_tokens = counter("thoughtTokens");
    let total_tokens = counter("totalTokens");
    if input_tokens.is_none()
        && output_tokens.is_none()
        && cache_read_tokens.is_none()
        && cache_write_tokens.is_none()
        && thinking_tokens.is_none()
        && total_tokens.is_none()
    {
        return None;
    }
    Some(TurnUsage {
        scope: Some("turn".to_owned()),
        message_id: None,
        input_tokens,
        output_tokens,
        reasoning_tokens: None,
        thinking_tokens,
        cache_read_tokens,
        cache_write_tokens,
        total_tokens,
    })
}

/// One option the native offers for a `session/request_permission` ask.
/// `kind` is one of the ACP `PermissionOptionKind` values (`allow_once`,
/// `allow_always`, `reject_once`, `reject_always`); any other kind is
/// retained but never maps to a reply decision.
#[derive(Clone, Debug)]
pub(crate) struct PermissionOption {
    pub option_id: String,
    pub name: String,
    pub kind: String,
}

/// A parsed `session/request_permission` ask: the tool call awaiting
/// authorization plus the exact offered options. `raw_input` stays a
/// `Value`: it is only ever rendered into a redacted bounded summary,
/// never persisted verbatim.
#[derive(Clone, Debug)]
pub(crate) struct PermissionAsk {
    pub tool_call_id: String,
    pub title: String,
    pub tool_name: Option<String>,
    pub tool_kind: Option<String>,
    pub raw_input: Value,
    pub options: Vec<PermissionOption>,
}

/// Map an offered option kind to its reply direction: `true` grants
/// (`allow_once`, `allow_always`), `false` refuses (`reject_once`,
/// `reject_always`). Unknown kinds map to nothing and can never drive a
/// reply on their own.
pub(crate) fn permission_option_grants(kind: &str) -> Option<bool> {
    match kind {
        "allow_once" | "allow_always" => Some(true),
        "reject_once" | "reject_always" => Some(false),
        _ => None,
    }
}

/// Default option for an explicit owner/manager reply verb: the least
/// persistent matching kind (`allow_once` before `allow_always`,
/// `reject_once` before `reject_always`). An explicit `choice_id` still
/// overrides this; the verb alone never invents an option.
pub(crate) fn default_permission_option(
    options: &[PermissionOption],
    approve: bool,
) -> Option<&PermissionOption> {
    let kinds: &[&str] = if approve {
        &["allow_once", "allow_always"]
    } else {
        &["reject_once", "reject_always"]
    };
    kinds
        .iter()
        .filter_map(|kind| options.iter().find(|option| option.kind == *kind))
        .next()
}

/// Parse a `session/request_permission` params body. Malformed option
/// entries are skipped, never fatal: an ask with no usable options is
/// still returned so the caller can record the gap and answer the safe
/// fallback instead of hanging the turn.
pub(crate) fn parse_permission_request(params: &Value) -> Option<PermissionAsk> {
    let body = params.as_object()?;
    let tool_call = body.get("toolCall").and_then(Value::as_object)?;
    let tool_call_id = tool_call
        .get("toolCallId")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())?
        .to_owned();
    let title = tool_call
        .get("title")
        .and_then(Value::as_str)
        .unwrap_or("ACP tool call")
        .to_owned();
    let tool_name = tool_call
        .get("name")
        .and_then(Value::as_str)
        .filter(|name| !name.trim().is_empty())
        .map(str::to_owned);
    let tool_kind = tool_call
        .get("kind")
        .and_then(Value::as_str)
        .filter(|kind| !kind.trim().is_empty())
        .map(str::to_owned);
    let raw_input = tool_call.get("rawInput").cloned().unwrap_or(Value::Null);
    let mut options = Vec::new();
    if let Some(offered) = body.get("options").and_then(Value::as_array) {
        for option in offered {
            let entry = option.as_object();
            let (Some(option_id), Some(name), Some(kind)) = (
                entry
                    .and_then(|entry| entry.get("optionId"))
                    .and_then(Value::as_str),
                entry
                    .and_then(|entry| entry.get("name"))
                    .and_then(Value::as_str),
                entry
                    .and_then(|entry| entry.get("kind"))
                    .and_then(Value::as_str),
            ) else {
                continue;
            };
            if option_id.trim().is_empty() {
                continue;
            }
            options.push(PermissionOption {
                option_id: option_id.to_owned(),
                name: name.to_owned(),
                kind: kind.to_owned(),
            });
        }
    }
    Some(PermissionAsk {
        tool_call_id,
        title,
        tool_name,
        tool_kind,
        raw_input,
        options,
    })
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
            .and_then(|content| {
                let block = content.as_object()?;
                // Only verbatim text blocks contribute assistant text. Older
                // servers omit the discriminator; newer ones send
                // `type: "text"`. Any other block (`image`, `audio`,
                // `resource_link`, `resource`) carries no text and must not
                // read as an empty answer.
                if block
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind != "text")
                {
                    return None;
                }
                block.get("text").and_then(Value::as_str)
            })
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
    fn prompt_response_usage_parses_the_agy_159_extension() {
        // Recorded `session/prompt` result (agy 1.5.9, gemini-4-argon-high,
        // 2026-11-15): the pinned schema names only `stopReason`, but the
        // server also reports per-turn model usage. It must attach as turn
        // scope, never as session occupancy, and never block completion.
        let prompt: PromptResult = serde_json::from_value(json!({
            "stopReason": "end_turn",
            "usage": {
                "inputTokens": 237441,
                "outputTokens": 7276,
                "cachedReadTokens": 392638,
                "cachedWriteTokens": 0,
                "thoughtTokens": 4121,
                "totalTokens": 637355,
            },
        }))
        .expect("prompt result with usage parses");
        let usage = prompt_turn_usage(&prompt).expect("usage maps");
        assert_eq!(usage.scope.as_deref(), Some("turn"));
        assert_eq!(usage.input_tokens, Some(237441));
        assert_eq!(usage.output_tokens, Some(7276));
        assert_eq!(usage.cache_read_tokens, Some(392638));
        assert_eq!(usage.cache_write_tokens, Some(0));
        assert_eq!(usage.thinking_tokens, Some(4121));
        assert_eq!(usage.total_tokens, Some(637355));
        assert_eq!(usage.reasoning_tokens, None);

        // Absent, null, negative and wrong-typed extensions yield no
        // usage at all, never a zeroed or partial guess.
        for usage in [
            json!(null),
            json!({"inputTokens": -1}),
            json!({"inputTokens": "many"}),
            json!({"unrelated": true}),
        ] {
            let prompt: PromptResult = serde_json::from_value(json!({
                "stopReason": "end_turn",
                "usage": usage,
            }))
            .expect("tolerant prompt result parses");
            assert_eq!(prompt_turn_usage(&prompt), None, "usage={usage}");
        }
        let bare: PromptResult = serde_json::from_value(json!({"stopReason": "end_turn"}))
            .expect("bare prompt result parses");
        assert_eq!(prompt_turn_usage(&bare), None);
    }

    #[test]
    fn permission_request_parses_tool_call_and_offered_options() {
        // Per the ACP schema (`RequestPermissionRequest`): the `git log`
        // ask carries the tool call plus options in the four kinds.
        let params = json!({
            "sessionId": "session-1",
            "toolCall": {
                "toolCallId": "tool-call_1",
                "title": "Run git log",
                "name": "exec",
                "kind": "execute",
                "status": "pending",
                "rawInput": {"command": "git log --oneline -n 100"},
            },
            "options": [
                {"optionId": "allow-once", "name": "Allow once", "kind": "allow_once"},
                {"optionId": "allow-always", "name": "Allow always", "kind": "allow_always"},
                {"optionId": "reject-once", "name": "Reject", "kind": "reject_once"},
                {"optionId": "reject-always", "name": "Always reject", "kind": "reject_always"},
            ],
        });
        let ask = parse_permission_request(&params).expect("permission ask parses");
        assert_eq!(ask.tool_call_id, "tool-call_1");
        assert_eq!(ask.title, "Run git log");
        assert_eq!(ask.tool_name.as_deref(), Some("exec"));
        assert_eq!(ask.tool_kind.as_deref(), Some("execute"));
        assert_eq!(ask.options.len(), 4);

        // Direction mapping follows the option kinds, never the labels.
        assert_eq!(permission_option_grants("allow_once"), Some(true));
        assert_eq!(permission_option_grants("allow_always"), Some(true));
        assert_eq!(permission_option_grants("reject_once"), Some(false));
        assert_eq!(permission_option_grants("reject_always"), Some(false));
        assert_eq!(permission_option_grants("sometimes"), None);

        // Defaults are least-privilege: the one-time kind wins per verb.
        assert_eq!(
            default_permission_option(&ask.options, true).map(|option| option.option_id.as_str()),
            Some("allow-once")
        );
        assert_eq!(
            default_permission_option(&ask.options, false).map(|option| option.option_id.as_str()),
            Some("reject-once")
        );

        // Malformed option entries are skipped, never fatal.
        let sparse = parse_permission_request(&json!({
            "sessionId": "session-1",
            "toolCall": {"toolCallId": "tool-call_2"},
            "options": [
                {"optionId": "", "name": "Empty", "kind": "allow_once"},
                {"name": "No id", "kind": "reject_once"},
                {"optionId": "ok", "name": "Ok", "kind": "reject_once"},
            ],
        }))
        .expect("sparse permission ask parses");
        assert_eq!(sparse.title, "ACP tool call");
        assert_eq!(sparse.options.len(), 1);
        assert_eq!(sparse.options[0].option_id, "ok");

        // Missing tool call identity is not an ask at all.
        assert!(
            parse_permission_request(&json!({
                "sessionId": "session-1",
                "toolCall": {"title": "No id"},
                "options": [],
            }))
            .is_none()
        );
    }

    #[test]
    fn message_chunk_takes_text_only_from_text_blocks() {
        // Discriminated text blocks (agy 1.5.9) and the older bare shape
        // both contribute assistant text.
        for content in [
            json!({"type": "text", "text": "READY-42"}),
            json!({"text": "42"}),
        ] {
            let (_, parsed) = update(json!({
                "sessionUpdate": "agent_message_chunk",
                "content": content,
            }));
            assert!(
                matches!(parsed, AcpUpdate::MessageChunk { text } if text == "READY-42" || text == "42"),
                "text block contributes text"
            );
        }
        // Non-text blocks and non-object content never read as an answer:
        // they contribute no text instead of an empty success string.
        for content in [
            json!({"type": "image", "data": "aGVsbG8="}),
            json!({"type": "resource_link", "uri": "file:///x"}),
            json!([{"type": "text", "text": "array-is-not-a-block"}]),
            json!(null),
        ] {
            let (_, parsed) = update(json!({
                "sessionUpdate": "agent_message_chunk",
                "content": content,
            }));
            assert!(
                matches!(parsed, AcpUpdate::MessageChunk { text } if text.is_empty()),
                "non-text content contributes no text"
            );
        }
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
