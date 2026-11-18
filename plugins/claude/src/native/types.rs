//! Claude stream-json wire shapes for `claude -p`.
//!
//! Field names follow the controller-verified CLI behavior (note 47, installed
//! CLI 2.1.278): stdin/stdout NDJSON with `control_request`/`control_response`
//! envelopes, `user` input records, `command_lifecycle` association,
//! `assistant` text, `system` lines and the terminal `result` record. Records
//! stay open: unknown fields are ignored, required fields are checked by the
//! caller that consumes them.

use eliot_core::{
    MAX_MCP_SERVER_NAME_LEN, SessionCost, SessionTokenTotals, SessionUsage, TurnUsage,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::path::PathBuf;

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
    #[serde(rename = "hookSpecificOutput", skip_serializing_if = "Option::is_none")]
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
    #[serde(rename = "displayName", default)]
    pub display_name: Option<String>,
    #[serde(rename = "supportedEffortLevels", default)]
    pub supported_effort_levels: Option<Vec<String>>,
}

/// `command_lifecycle` association record.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CommandLifecycle {
    #[serde(default)]
    pub command_uuid: String,
    #[serde(default)]
    pub state: String,
}

/// One `assistant` content block; `text` blocks carry reply text,
/// `tool_use` blocks carry a native tool invocation (`id` plus the tool
/// `name`).
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct AssistantContentBlock {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
}

/// One `user` content block; `tool_result` blocks carry a native tool
/// outcome bound to its invocation via `tool_use_id`.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct UserContentBlock {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub tool_use_id: Option<String>,
    #[serde(default)]
    pub is_error: Option<bool>,
}

/// Terminal `result` record. Autonomous task notifications are not terminals
/// for a submitted input, even when another Eliot turn is active.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ResultRecord {
    #[serde(default)]
    pub terminal_reason: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub origin: Option<ResultOrigin>,
    #[serde(default)]
    pub user_message_uuid: Option<String>,
    #[serde(default)]
    pub user_message_uuids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub(crate) struct ResultOrigin {
    pub kind: String,
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

/// Extract cumulative session token counters from a terminal `result`
/// record's `modelUsage`. With persistent streaming input the per-model map
/// is cumulative for the session (main loop, subagents and internal calls,
/// per the Agent SDK `ModelUsage` docs), so its sums map to the
/// session-scoped `cumulative_tokens`, never to turn expenditure and never
/// derived into a total. Only genuinely supplied nonnegative integer
/// counters project (`inputTokens`, `outputTokens`, `cacheReadInputTokens`,
/// `cacheCreationInputTokens`); missing or malformed entries contribute
/// nothing, and an absent or wholly unusable map yields no tokens. Per-model
/// `costUSD` is never mapped here: the session cost authority is the
/// top-level `total_cost_usd` below, and summing per-model costs alongside
/// it would double-count.
pub(crate) fn result_session_tokens(record: &Value) -> Option<SessionTokenTotals> {
    let model_usage = record.get("modelUsage")?.as_object()?;
    let mut sums = [0i128; 4];
    let mut supplied = [0usize; 4];
    for entry in model_usage.values() {
        let Some(entry) = entry.as_object() else {
            continue;
        };
        let counters = [
            entry
                .get("inputTokens")
                .and_then(Value::as_i64)
                .filter(|value| *value >= 0),
            entry
                .get("outputTokens")
                .and_then(Value::as_i64)
                .filter(|value| *value >= 0),
            entry
                .get("cacheReadInputTokens")
                .and_then(Value::as_i64)
                .filter(|value| *value >= 0),
            entry
                .get("cacheCreationInputTokens")
                .and_then(Value::as_i64)
                .filter(|value| *value >= 0),
        ];
        for (index, counter) in counters.into_iter().enumerate() {
            if let Some(value) = counter {
                sums[index] += i128::from(value);
                supplied[index] += 1;
            }
        }
    }
    if supplied.iter().all(|count| *count == 0) {
        return None;
    }
    let get = |index: usize| {
        (supplied[index] > 0).then(|| sums[index].clamp(0, i128::from(i64::MAX)) as i64)
    };
    Some(SessionTokenTotals {
        input_tokens: get(0),
        output_tokens: get(1),
        cache_read_tokens: get(2),
        cache_write_tokens: get(3),
    })
}

/// Context occupancy sample from a terminal `result` record's `usage`: the
/// turn's input-side counters (input plus cache creation/read) approximate
/// the context read for that turn, so the latest sample is the retained
/// occupancy. Missing or malformed counters leave the sample absent; a
/// result without any input-side counter contributes no occupancy (the
/// cumulative `modelUsage` below still maps).
pub(crate) fn result_context_used(record: &Value) -> Option<i64> {
    let usage = record.get("usage")?.as_object()?;
    let mut sum: i128 = 0;
    let mut supplied = 0;
    for key in [
        "input_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
    ] {
        if let Some(value) = usage
            .get(key)
            .and_then(Value::as_i64)
            .filter(|value| *value >= 0)
        {
            sum += i128::from(value);
            supplied += 1;
        }
    }
    if supplied == 0 || sum > i128::from(i64::MAX) {
        return None;
    }
    Some(sum as i64)
}

/// Model context window from an init or result frame: the installed
/// stream-json names the capacity `context_window`/`contextWindow` where the
/// harness reports it. Absent or unusable stays null, never zero or guessed.
pub(crate) fn frame_context_window(record: &Value) -> Option<i64> {
    record
        .get("context_window")
        .or_else(|| record.get("contextWindow"))
        .and_then(Value::as_i64)
        .filter(|window| *window > 0)
}

/// Config roots that may hold the Claude transcript store: an explicit
/// `CLAUDE_CONFIG_DIR` first, then the home-derived `.claude` directories.
/// Only absolute roots are kept; the caller scans them read-only.
pub(crate) fn transcript_config_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Some(dir) = std::env::var_os("CLAUDE_CONFIG_DIR")
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute())
    {
        roots.push(dir);
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        roots.push(home.join(".claude"));
    }
    if let Some(profile) = std::env::var_os("USERPROFILE").map(PathBuf::from) {
        roots.push(profile.join(".claude"));
    }
    roots
}

/// Maximum project entries scanned for one session transcript.
pub(crate) const MAX_TRANSCRIPT_PROJECTS: usize = 512;

/// Native transcript size for one session: `<config>/projects/*/<session>.jsonl`.
/// The project slug is never guessed: the projects directory is scanned
/// (bounded entries) for the exact session filename and the first hit wins.
/// Anything unresolvable fails closed to None, never zero.
pub(crate) fn transcript_bytes_in(roots: &[PathBuf], session_id: &str) -> Option<u64> {
    if session_id.trim().is_empty() || session_id.len() > 128 {
        return None;
    }
    if session_id
        .chars()
        .any(|c| c.is_control() || matches!(c, '/' | '\\'))
    {
        return None;
    }
    let file_name = format!("{session_id}.jsonl");
    for root in roots {
        let entries = std::fs::read_dir(root.join("projects")).ok()?;
        let mut checked = 0;
        for entry in entries.flatten() {
            checked += 1;
            if checked > MAX_TRANSCRIPT_PROJECTS {
                break;
            }
            if let Ok(meta) = std::fs::metadata(entry.path().join(&file_name))
                && meta.is_file()
            {
                return Some(meta.len());
            }
        }
    }
    None
}

/// Extract the session-scoped fact from a terminal `result` record:
/// cumulative token counters from `modelUsage` plus the cumulative cost
/// from `total_cost_usd`, the occupancy sample from `usage`, and the model
/// context window where the frame reports it. With persistent streaming input
/// the cumulative halves are cumulative for the session (resumed cost
/// included) and estimated, so they map to a session-scoped snapshot with
/// source `claude:result`, never to turn expenditure and never converted.
/// Any mapped quarter yields a fact; only when all are absent is there no
/// session fact, so completion proceeds without one. The transcript file
/// size is filled by the caller (it needs the retained session id), never
/// guessed here.
pub(crate) fn result_session_usage(record: &Value) -> Option<SessionUsage> {
    let cumulative_tokens = result_session_tokens(record);
    let amount = record
        .get("total_cost_usd")
        .and_then(Value::as_f64)
        .filter(|amount| amount.is_finite() && *amount >= 0.0);
    let used = result_context_used(record);
    let window = frame_context_window(record);
    if cumulative_tokens.is_none() && amount.is_none() && used.is_none() && window.is_none() {
        return None;
    }
    Some(SessionUsage {
        context_used_tokens: used,
        context_window_tokens: window,
        cumulative_tokens,
        cumulative_cost: amount.map(|amount| SessionCost {
            amount: Some(amount),
            unit: Some("usd_estimate".to_owned()),
        }),
        session_file_bytes: None,
        source: Some("claude:result".to_owned()),
        observed_at: None,
    })
}

/// One proven MCP server entry from a `system`/`init` record: the native
/// name plus its connection status. Only these two bounded fields are
/// retained; every other init field (commands, paths, sources) stays out
/// of persisted data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InitMcpServer {
    pub name: String,
    pub status: String,
}

/// Bound matching the daemon's other per-profile entry lists.
const MAX_INIT_MCP_SERVERS: usize = 64;
/// Native statuses are short words (`connected`, `failed` on installed
/// 2.1.321); anything longer is not a status.
const MAX_INIT_MCP_STATUS_LEN: usize = 64;

/// Extract the proven MCP server surface from a `system`/`init` record.
/// `None` means no provable inventory (absent or malformed list, over
/// bound, duplicated or unusable entries): the caller keeps the effective
/// set unknown instead of echoing a guess. An observed empty list proves
/// an empty surface and returns an empty vec.
pub(crate) fn init_mcp_servers(record: &Value) -> Option<Vec<InitMcpServer>> {
    let servers = record.get("mcp_servers")?.as_array()?;
    if servers.len() > MAX_INIT_MCP_SERVERS {
        return None;
    }
    let mut out = Vec::with_capacity(servers.len());
    for server in servers {
        let entry = server.as_object()?;
        let name = entry.get("name")?.as_str()?.trim();
        let status = entry.get("status")?.as_str()?.trim();
        if name.is_empty()
            || name.len() > MAX_MCP_SERVER_NAME_LEN
            || name.chars().any(char::is_control)
            || status.is_empty()
            || status.len() > MAX_INIT_MCP_STATUS_LEN
            || status.chars().any(char::is_control)
        {
            return None;
        }
        if out.iter().any(|kept: &InitMcpServer| kept.name == name) {
            return None;
        }
        out.push(InitMcpServer {
            name: name.to_owned(),
            status: status.to_owned(),
        });
    }
    out.sort_by(|left, right| left.name.cmp(&right.name));
    Some(out)
}

/// Attach a retained init readback to a terminal's event data. Only a
/// proven (observed) readback is attached, with verified true; no retained
/// readback leaves the data untouched so the effective set stays unknown.
pub(crate) fn attach_mcp_readback(data: &mut Value, servers: Option<&[InitMcpServer]>) {
    let Some(servers) = servers else {
        return;
    };
    let Some(object) = data.as_object_mut() else {
        return;
    };
    object.insert(
        "mcp_servers".to_owned(),
        Value::Array(
            servers
                .iter()
                .map(|server| json!({"name": server.name, "status": server.status}))
                .collect(),
        ),
    );
    object.insert("mcp_servers_verified".to_owned(), Value::Bool(true));
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
    assistant_tool_uses(record)
        .into_iter()
        .map(|tool_use| tool_use.id)
        .collect()
}

/// One native tool invocation: the exact `tool_use.id` plus the native
/// tool name. Both are required: an invocation without an id cannot bind
/// its result, and one without a name cannot label its tool record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ToolUseBlock {
    pub id: String,
    pub name: String,
}

/// Tool invocations in one assistant record, in wire order. Blocks with a
/// missing or blank id or name are dropped here; the caller records the
/// coverage gap so a nameless invocation never reads as a known tool.
pub(crate) fn assistant_tool_uses(record: &Value) -> Vec<ToolUseBlock> {
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
        .filter_map(|block| {
            let id = block.id.filter(|id| !id.trim().is_empty())?;
            let name = block.name.filter(|name| !name.trim().is_empty())?;
            Some(ToolUseBlock { id, name })
        })
        .collect()
}

/// Count of `tool_use` blocks in one assistant record, including blocks
/// with a missing id or name. The event processor compares this against
/// [`assistant_tool_uses`] to spot nameless invocations the tool records
/// cannot label.
pub(crate) fn assistant_tool_use_count(record: &Value) -> usize {
    let Some(blocks) = record
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return 0;
    };
    blocks
        .iter()
        .filter_map(|block| serde_json::from_value::<AssistantContentBlock>(block.clone()).ok())
        .filter(|block| block.kind == "tool_use")
        .count()
}

/// One native tool outcome: the exact `tool_use_id` it answers plus the
/// verdict the native frame carries (`is_error`, absent when unreported).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ToolResultBlock {
    pub tool_use_id: String,
    pub is_error: Option<bool>,
}

/// Tool outcomes in one `user` record, in wire order. Blocks with a missing
/// or blank `tool_use_id` cannot bind an invocation and are dropped here.
pub(crate) fn user_tool_results(record: &Value) -> Vec<ToolResultBlock> {
    let Some(blocks) = record
        .get("message")
        .and_then(|message| message.get("content"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };
    blocks
        .iter()
        .filter_map(|block| serde_json::from_value::<UserContentBlock>(block.clone()).ok())
        .filter(|block| block.kind == "tool_result")
        .filter_map(|block| {
            let tool_use_id = block.tool_use_id.filter(|id| !id.trim().is_empty())?;
            Some(ToolResultBlock {
                tool_use_id,
                is_error: block.is_error,
            })
        })
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
    fn session_cost_maps_total_cost_as_usd_estimate() {
        let usage =
            result_session_usage(&json!({"total_cost_usd": 1.25})).expect("cumulative cost maps");
        assert_eq!(usage.context_used_tokens, None);
        assert_eq!(usage.context_window_tokens, None);
        assert_eq!(usage.cumulative_tokens, None);
        let cost = usage.cumulative_cost.expect("cost");
        assert_eq!(cost.amount, Some(1.25));
        assert_eq!(cost.unit.as_deref(), Some("usd_estimate"));
        assert_eq!(usage.source.as_deref(), Some("claude:result"));
        // Absent, wrong-typed and negative costs yield no session fact, so
        // completion proceeds without one.
        assert_eq!(result_session_usage(&json!({})), None);
        assert_eq!(
            result_session_usage(&json!({"total_cost_usd": "1.25"})),
            None
        );
        assert_eq!(result_session_usage(&json!({"total_cost_usd": -0.5})), None);
    }

    #[test]
    fn result_occupancy_samples_input_side_counters() {
        assert_eq!(
            result_context_used(&json!({"usage": {
                "input_tokens": 100,
                "cache_creation_input_tokens": 50,
                "cache_read_input_tokens": 1000,
                "output_tokens": 20,
            }})),
            Some(1150)
        );
        // Output alone is not occupancy.
        assert_eq!(
            result_context_used(&json!({"usage": {"output_tokens": 20}})),
            None
        );
        assert_eq!(result_context_used(&json!({})), None);
        assert_eq!(
            result_context_used(&json!({"usage": {"input_tokens": -1}})),
            None
        );
        // The window comes from the init or result frames, never guessed.
        assert_eq!(
            frame_context_window(&json!({"context_window": 200_000})),
            Some(200_000)
        );
        assert_eq!(
            frame_context_window(&json!({"contextWindow": 100_000})),
            Some(100_000)
        );
        assert_eq!(frame_context_window(&json!({})), None);
        assert_eq!(frame_context_window(&json!({"context_window": 0})), None);
        // Occupancy alone yields a session fact.
        let usage =
            result_session_usage(&json!({"usage": {"input_tokens": 7}})).expect("occupancy maps");
        assert_eq!(usage.context_used_tokens, Some(7));
        assert_eq!(usage.context_window_tokens, None);
    }

    #[test]
    fn transcript_scan_finds_the_exact_session_file() {
        let root = std::env::temp_dir().join(format!("eliot-claude-tx-{}", ulid::Ulid::new()));
        let _tx = crate::test_temp::TempDir::own(root.clone());
        let project = root.join("projects").join("some-slug");
        std::fs::create_dir_all(&project).expect("fixture projects");
        std::fs::write(project.join("other.jsonl"), b"{}").expect("fixture other");
        std::fs::write(project.join("sess-1.jsonl"), b"{\"a\":1}").expect("fixture session");
        let roots = vec![root.clone()];
        assert_eq!(
            transcript_bytes_in(&roots, "sess-1"),
            Some(7),
            "exact session filename wins without guessing the slug"
        );
        assert_eq!(transcript_bytes_in(&roots, "missing"), None);
        assert_eq!(transcript_bytes_in(&roots, "../escape"), None);
        assert_eq!(transcript_bytes_in(&roots, ""), None);
        assert_eq!(transcript_bytes_in(&[], "sess-1"), None);
    }

    #[test]
    fn model_usage_sums_session_counters_without_a_total() {
        let record = json!({
            "total_cost_usd": 0.00260901,
            "usage": {"input_tokens": 2, "output_tokens": 4},
            "modelUsage": {
                "claude-haiku-4-5": {
                    "inputTokens": 100,
                    "outputTokens": 20,
                    "cacheReadInputTokens": 1000,
                    "cacheCreationInputTokens": 50,
                    "costUSD": 0.001,
                    "webSearchRequests": 0,
                },
                "claude-sonnet-4-6": {
                    "inputTokens": 30,
                    "outputTokens": 5,
                    "cacheReadInputTokens": 200,
                    "cacheCreationInputTokens": 10,
                },
            },
        });
        let tokens = result_session_tokens(&record).expect("session counters map");
        assert_eq!(tokens.input_tokens, Some(130));
        assert_eq!(tokens.output_tokens, Some(25));
        assert_eq!(tokens.cache_read_tokens, Some(1200));
        assert_eq!(tokens.cache_write_tokens, Some(60));
        let usage = result_session_usage(&record).expect("tokens plus cost map");
        let tokens = usage.cumulative_tokens.expect("tokens retained");
        assert_eq!(tokens.input_tokens, Some(130));
        assert_eq!(
            usage.cumulative_cost.as_ref().unwrap().amount,
            Some(0.00260901)
        );
        // Empty, absent and wholly malformed maps yield no tokens, so a
        // cost-free record with one commits no session fact.
        assert_eq!(result_session_tokens(&json!({"modelUsage": {}})), None);
        assert_eq!(result_session_tokens(&json!({})), None);
        assert_eq!(result_session_tokens(&json!({"modelUsage": []})), None);
        assert_eq!(
            result_session_usage(&json!({"modelUsage": {"m": {"inputTokens": "many"}}})),
            None
        );
        // Negative counters never project; usable siblings still sum.
        let partial = result_session_tokens(&json!({"modelUsage": {
            "a": {"inputTokens": -1, "outputTokens": 7},
            "b": "oops",
        }}))
        .expect("usable sibling maps");
        assert_eq!(partial.input_tokens, None);
        assert_eq!(partial.output_tokens, Some(7));
    }

    #[test]
    fn tool_blocks_bind_results_to_exact_invocation_ids() {
        let assistant = json!({
            "type": "assistant",
            "message": {"content": [
                {"type": "text", "text": "reading"},
                {"type": "tool_use", "id": "toolu-1", "name": "Read",
                 "input": {"file_path": "note.txt"}},
                {"type": "tool_use", "id": "toolu-2", "name": "Bash",
                 "input": {"command": "ls"}},
                {"type": "tool_use", "id": "toolu-3"},
                {"type": "tool_use", "id": "  ", "name": "Glob"},
            ]},
        });
        assert_eq!(
            assistant_tool_uses(&assistant),
            vec![
                ToolUseBlock {
                    id: "toolu-1".to_owned(),
                    name: "Read".to_owned()
                },
                ToolUseBlock {
                    id: "toolu-2".to_owned(),
                    name: "Bash".to_owned()
                },
            ]
        );
        assert_eq!(
            assistant_tool_use_ids(&assistant),
            vec!["toolu-1", "toolu-2"]
        );
        // The count keeps the nameless blocks the mapping drops, so the
        // caller can spot the coverage gap.
        assert_eq!(assistant_tool_use_count(&assistant), 4);
        assert!(assistant_tool_uses(&json!({"type": "assistant"})).is_empty());

        let user = json!({
            "type": "user",
            "message": {"content": [
                {"type": "tool_result", "tool_use_id": "toolu-1",
                 "content": "note bytes"},
                {"type": "tool_result", "tool_use_id": "toolu-2",
                 "content": "boom", "is_error": true},
                {"type": "tool_result", "content": "orphan"},
                {"type": "text", "text": "not a result"},
            ]},
        });
        assert_eq!(
            user_tool_results(&user),
            vec![
                ToolResultBlock {
                    tool_use_id: "toolu-1".to_owned(),
                    is_error: None
                },
                ToolResultBlock {
                    tool_use_id: "toolu-2".to_owned(),
                    is_error: Some(true)
                },
            ]
        );
        assert!(user_tool_results(&json!({"type": "user"})).is_empty());
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

    #[test]
    fn init_mcp_servers_keeps_sorted_names_and_statuses() {
        let record = json!({
            "type": "system",
            "subtype": "init",
            "mcp_servers": [
                {"name": "probe_dummy", "status": "failed", "source": "dynamic"},
                {"name": "eliot-search", "status": "connected", "source": "user"},
            ],
        });
        assert_eq!(
            init_mcp_servers(&record).expect("proven inventory"),
            vec![
                InitMcpServer {
                    name: "eliot-search".to_owned(),
                    status: "connected".to_owned(),
                },
                InitMcpServer {
                    name: "probe_dummy".to_owned(),
                    status: "failed".to_owned(),
                },
            ]
        );
        assert_eq!(
            init_mcp_servers(&json!({"type": "system", "subtype": "init", "mcp_servers": []})),
            Some(Vec::new())
        );
    }

    #[test]
    fn init_mcp_servers_stays_unknown_without_proof() {
        assert_eq!(init_mcp_servers(&json!({"type": "system"})), None);
        assert_eq!(init_mcp_servers(&json!({"mcp_servers": null})), None);
        assert_eq!(init_mcp_servers(&json!({"mcp_servers": {"a": {}}})), None);
        // One unusable entry voids the whole inventory, never a partial echo.
        for record in [
            json!({"mcp_servers": [{"name": "", "status": "connected"}]}),
            json!({"mcp_servers": [{"name": "a", "status": ""}]}),
            json!({"mcp_servers": [{"name": "a"}]}),
            json!({"mcp_servers": ["a"]}),
            json!({"mcp_servers": [{"name": "a", "status": "connected"}, {"name": "a", "status": "failed"}]}),
            json!({"mcp_servers": [{"name": "x".repeat(129), "status": "connected"}]}),
            json!({"mcp_servers": [{"name": "a", "status": "x".repeat(65)}]}),
        ] {
            assert_eq!(init_mcp_servers(&record), None);
        }
        let oversized: Vec<Value> = (0..65)
            .map(|index| json!({"name": format!("server-{index}"), "status": "connected"}))
            .collect();
        assert_eq!(init_mcp_servers(&json!({"mcp_servers": oversized})), None);
    }

    #[test]
    fn attach_mcp_readback_verifies_only_proven_sets() {
        let mut data = json!({"command_id": "c1", "turn_id": "t1", "status": "completed"});
        attach_mcp_readback(&mut data, None);
        assert_eq!(data.get("mcp_servers"), None);
        assert_eq!(data.get("mcp_servers_verified"), None);
        attach_mcp_readback(
            &mut data,
            Some(&[InitMcpServer {
                name: "eliot-swarm".to_owned(),
                status: "connected".to_owned(),
            }]),
        );
        assert_eq!(
            data["mcp_servers"],
            json!([{"name": "eliot-swarm", "status": "connected"}])
        );
        assert_eq!(data["mcp_servers_verified"], json!(true));
    }
}
