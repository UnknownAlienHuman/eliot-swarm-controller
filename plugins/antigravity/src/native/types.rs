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
    TurnUsage::from_native(
        "turn",
        TurnUsage {
            input_tokens: eliot_core::usage_counter(usage.get("inputTokens")),
            output_tokens: eliot_core::usage_counter(usage.get("outputTokens")),
            cache_read_tokens: eliot_core::usage_counter(usage.get("cachedReadTokens")),
            cache_write_tokens: eliot_core::usage_counter(usage.get("cachedWriteTokens")),
            thinking_tokens: eliot_core::usage_counter(usage.get("thoughtTokens")),
            total_tokens: eliot_core::usage_counter(usage.get("totalTokens")),
            ..TurnUsage::default()
        },
    )
}

/// One option the native offers for a `session/request_permission` ask.
/// `kind` is one of the ACP `PermissionOptionKind` values (`allow_once`,
/// `allow_always`, `reject_once`, `reject_always`); other kinds never map to
/// a reply decision and their raw tags are returned separately for diagnosis.
#[derive(Clone, Debug)]
pub(crate) struct PermissionOption {
    pub option_id: String,
    pub name: String,
    pub kind: PermissionOptionKind,
}

/// ACP permission kinds stay explicit so unfamiliar values can be diagnosed
/// without ever turning into an approval decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PermissionOptionKind {
    AllowOnce,
    AllowAlways,
    RejectOnce,
    RejectAlways,
    Other,
}

impl PermissionOptionKind {
    fn parse(value: &str) -> (Self, Option<String>) {
        match value {
            "allow_once" => (Self::AllowOnce, None),
            "allow_always" => (Self::AllowAlways, None),
            "reject_once" => (Self::RejectOnce, None),
            "reject_always" => (Self::RejectAlways, None),
            other => (Self::Other, Some(other.to_owned())),
        }
    }
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
    pub unclassified_option_kinds: Vec<String>,
}

/// Map an offered option kind to its reply direction: `true` grants
/// (`allow_once`, `allow_always`), `false` refuses (`reject_once`,
/// `reject_always`). Unknown kinds map to nothing and can never drive a
/// reply on their own.
pub(crate) fn permission_option_grants(kind: &PermissionOptionKind) -> Option<bool> {
    match kind {
        PermissionOptionKind::AllowOnce | PermissionOptionKind::AllowAlways => Some(true),
        PermissionOptionKind::RejectOnce | PermissionOptionKind::RejectAlways => Some(false),
        PermissionOptionKind::Other => None,
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
    let kinds: &[PermissionOptionKind] = if approve {
        &[
            PermissionOptionKind::AllowOnce,
            PermissionOptionKind::AllowAlways,
        ]
    } else {
        &[
            PermissionOptionKind::RejectOnce,
            PermissionOptionKind::RejectAlways,
        ]
    };
    kinds
        .iter()
        .filter_map(|kind| options.iter().find(|option| &option.kind == kind))
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
    let mut unclassified_option_kinds = Vec::new();
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
            let (kind, unclassified_kind) = PermissionOptionKind::parse(kind);
            if let Some(unclassified_kind) = unclassified_kind {
                unclassified_option_kinds.push(unclassified_kind);
            }
            options.push(PermissionOption {
                option_id: option_id.to_owned(),
                name: name.to_owned(),
                kind,
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
        unclassified_option_kinds,
    })
}

#[derive(Debug)]
pub(crate) enum PromptTerminalStatus {
    Completed,
    Cancelled,
    Failed,
    Other,
}

impl PromptResult {
    /// Map known stop reasons onto Eliot turn statuses. Preserve unfamiliar
    /// values explicitly without assigning them a guessed outcome.
    pub fn terminal_status(&self) -> PromptTerminalStatus {
        match self.stop_reason.as_str() {
            "end_turn" | "max_tokens" | "max_turn_requests" => PromptTerminalStatus::Completed,
            "cancelled" => PromptTerminalStatus::Cancelled,
            "refusal" => PromptTerminalStatus::Failed,
            _ => PromptTerminalStatus::Other,
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
        tool_kind: Option<String>,
        tool_name: Option<String>,
        diff_paths: ToolDiffPaths,
        status: Option<String>,
    },
    /// A tool call state change.
    ToolCallUpdate {
        tool_call_id: String,
        title: Option<String>,
        tool_kind: Option<String>,
        tool_name: Option<String>,
        diff_paths: ToolDiffPaths,
        status: Option<String>,
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
    /// A `sessionUpdate` kind outside the known vocabulary.
    Other(String),
}

/// Bounded exact paths from ACP's structured file-diff content. Raw output and
/// prose are intentionally not inspected for path-like strings.
const MAX_TOOL_DIFF_PATHS: usize = 32;
const MAX_TOOL_DIFF_PATH_BYTES: usize = 4096;

#[derive(Debug, Default)]
pub(crate) struct ToolDiffPaths {
    pub paths: Vec<String>,
    pub overflowed: bool,
}

impl ToolDiffPaths {
    pub fn insert(&mut self, path: &str) {
        if self.paths.iter().any(|known| known == path) {
            return;
        }
        if self.paths.len() == MAX_TOOL_DIFF_PATHS {
            self.overflowed = true;
        } else {
            self.paths.push(path.to_owned());
        }
    }
}

fn tool_diff_paths(update: &Value) -> ToolDiffPaths {
    let mut paths = ToolDiffPaths::default();
    let Some(content) = update.get("content").and_then(Value::as_array) else {
        return paths;
    };
    for block in content {
        if block.get("type").and_then(Value::as_str) != Some("diff") {
            continue;
        }
        let Some(path) = block.get("path").and_then(Value::as_str) else {
            continue;
        };
        // AGY's Windows ACP diff location can use the file-URI path spelling.
        #[cfg(windows)]
        let path = match path.as_bytes() {
            [b'/', drive, b':', b'/' | b'\\', ..] if drive.is_ascii_alphabetic() => &path[1..],
            _ => path,
        };
        if path.trim().is_empty() || path.len() > MAX_TOOL_DIFF_PATH_BYTES || path.contains('\0') {
            continue;
        }
        paths.insert(path);
        if paths.overflowed {
            break;
        }
    }
    paths
}

fn tool_name(update: &Value) -> Option<String> {
    update.get("name").and_then(Value::as_str).map(|name| {
        if name.len() <= 256 {
            name.to_owned()
        } else {
            String::new()
        }
    })
}

fn tool_kind(update: &Value) -> Option<String> {
    update.get("kind").and_then(Value::as_str).map(|kind| {
        if kind.len() <= 64 {
            kind.to_owned()
        } else {
            String::new()
        }
    })
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
                tool_kind: tool_kind(update),
                tool_name: tool_name(update),
                diff_paths: tool_diff_paths(update),
                status: update
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            },
            "tool_call_update" => AcpUpdate::ToolCallUpdate {
                tool_call_id: tool_call_id(),
                title: update
                    .get("title")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                tool_kind: tool_kind(update),
                tool_name: tool_name(update),
                diff_paths: tool_diff_paths(update),
                status: update
                    .get("status")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
            },
            "usage_update" => AcpUpdate::SessionUsage {
                used: eliot_core::usage_counter(update.get("used")),
                size: eliot_core::usage_counter(update.get("size")),
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
            other => AcpUpdate::Other(other.to_owned()),
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
    let mut merged = previous.cloned().unwrap_or_default();
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
