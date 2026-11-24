//! Claude stream-json wire shapes for `claude -p`.
//!
//! Field names follow the controller-verified CLI behavior (note 47, installed
//! CLI 2.1.278): stdin/stdout NDJSON with `control_request`/`control_response`
//! envelopes, `user` input records, `command_lifecycle` association,
//! `assistant` text, `system` lines and the terminal `result` record. Records
//! stay open: unknown fields are ignored, required fields are checked by the
//! caller that consumes them.
//! The cited CLI version is evidence provenance, not an exact-version gate.

use eliot_core::{
    MAX_MCP_SERVER_NAME_LEN, SessionCost, SessionTokenTotals, SessionUsage, TurnUsage,
    usage_counter,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
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

/// Protocol-level refusal for an inbound control request whose subtype is
/// unsupported. The exact native request id is echoed; no subtype-specific
/// response payload is guessed.
#[derive(Serialize)]
pub(crate) struct OutboundControlError {
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub response: OutboundControlErrorBody,
}

#[derive(Serialize)]
pub(crate) struct OutboundControlErrorBody {
    pub subtype: &'static str,
    pub request_id: String,
    pub error: String,
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
    pub hook_specific_output: Option<HookSpecificOutput>,
}

impl HookCallbackResponse {
    pub fn with_context(hook_event_name: &'static str, additional_context: String) -> Self {
        Self {
            hook_specific_output: Some(HookSpecificOutput {
                hook_event_name,
                additional_context,
            }),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct HookSpecificOutput {
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
    #[serde(default)]
    pub error: Option<Value>,
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
    #[serde(rename = "supportsAutoMode", default)]
    pub supports_auto_mode: Option<bool>,
}

/// `command_lifecycle` association record.
#[derive(Clone, Debug, Deserialize)]
pub(crate) struct CommandLifecycle {
    #[serde(default)]
    pub command_uuid: String,
    #[serde(default)]
    pub state: String,
}

/// The lifecycle states interpreted by the adapter. The original wire value
/// remains on `CommandLifecycle` so `Other` can be diagnosed without
/// assigning it a turn outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CommandLifecycleState {
    Queued,
    Started,
    Completed,
    Cancelled,
    Other,
}

impl CommandLifecycleState {
    pub(crate) fn from_native(value: &str) -> Self {
        match value {
            "queued" => Self::Queued,
            "started" => Self::Started,
            "completed" => Self::Completed,
            "cancelled" => Self::Cancelled,
            _ => Self::Other,
        }
    }
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
    #[serde(default)]
    pub input: Option<Value>,
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
    #[serde(default)]
    pub content: Option<Value>,
}

/// Terminal `result` record. A submitted input is identified by its UUID
/// independently of what triggered the run.
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

impl ResultRecord {
    pub(crate) fn names_input(&self, input: &str) -> bool {
        self.user_message_uuid.as_deref() == Some(input)
            || self.user_message_uuids.iter().any(|uuid| uuid == input)
    }

    pub(crate) fn names_only_other_inputs(&self, input: &str) -> bool {
        (self.user_message_uuid.is_some() || !self.user_message_uuids.is_empty())
            && !self.names_input(input)
    }
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
    TurnUsage::from_native(
        "main_loop_turn",
        TurnUsage {
            input_tokens: usage_counter(usage.get("input_tokens")),
            output_tokens: usage_counter(usage.get("output_tokens")),
            cache_read_tokens: usage_counter(usage.get("cache_read_input_tokens")),
            cache_write_tokens: usage_counter(usage.get("cache_creation_input_tokens")),
            ..TurnUsage::default()
        },
    )
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
            usage_counter(entry.get("inputTokens")),
            usage_counter(entry.get("outputTokens")),
            usage_counter(entry.get("cacheReadInputTokens")),
            usage_counter(entry.get("cacheCreationInputTokens")),
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

/// Context occupancy from one native main-thread `assistant` message.
/// Anthropic `message.usage` separates uncached input from cache-read and
/// cache-creation input. All four nonnegative counters describe one call's
/// context footprint; omitted cache buckets mean zero, while missing or
/// malformed required counters and overflow leave the sample unavailable.
pub(crate) fn assistant_call_input(record: &Value) -> Option<i64> {
    let usage = record.get("message")?.get("usage")?.as_object()?;
    let optional_cache = |key: &str| match usage.get(key) {
        Some(Value::Null) | None => Some(0),
        value => usage_counter(value),
    };
    eliot_plugin::usage::call_input_tokens(
        "claude",
        usage_counter(usage.get("input_tokens")),
        optional_cache("cache_read_input_tokens"),
        optional_cache("cache_creation_input_tokens"),
    )
}

fn assistant_context_used(record: &Value) -> Option<i64> {
    let usage = record.get("message")?.get("usage")?;
    assistant_call_input(record)?.checked_add(usage_counter(usage.get("output_tokens"))?)
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

/// Native transcript for one session: `<config>/projects/*/<session>.jsonl`.
/// The project slug is never guessed: the projects directory is scanned
/// (bounded entries) for the exact session filename and the first hit wins.
fn open_transcript(roots: &[PathBuf], session_id: &str) -> Option<File> {
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
        let Ok(entries) = std::fs::read_dir(root.join("projects")) else {
            continue;
        };
        for entry in entries.flatten().take(MAX_TRANSCRIPT_PROJECTS) {
            let path = entry.path().join(&file_name);
            if path.is_file()
                && let Ok(file) = File::open(path)
            {
                return Some(file);
            }
        }
    }
    None
}

/// The streamed assistant frame can precede the final native output count.
/// Read back only the finalized entry for the message bound by that stream.
pub(crate) fn transcript_usage_in(
    roots: &[PathBuf],
    session_id: &str,
    latest_message: Option<(String, String)>,
) -> Option<(u64, Option<i64>)> {
    let mut file = open_transcript(roots, session_id)?;
    let size = file.metadata().ok()?.len();
    let context = latest_message.and_then(|(model, message_id)| {
        transcript_context(&mut file, size, session_id, &model, &message_id)
    });
    Some((size, context))
}

fn transcript_context(
    file: &mut File,
    size: u64,
    session_id: &str,
    model: &str,
    message_id: &str,
) -> Option<i64> {
    const MAX_CONTEXT_READ_BYTES: u64 = 512 * 1024;
    let offset = size.saturating_sub(MAX_CONTEXT_READ_BYTES);
    file.seek(SeekFrom::Start(offset)).ok()?;
    let mut tail = Vec::new();
    file.take(size - offset).read_to_end(&mut tail).ok()?;
    let tail = if offset > 0 {
        &tail[tail.iter().position(|byte| *byte == b'\n')? + 1..]
    } else {
        &tail
    };
    let record = tail
        .rsplit(|byte| *byte == b'\n')
        .filter_map(|line| serde_json::from_slice::<Value>(line).ok())
        .find(|record| {
            record["type"].as_str() == Some("assistant")
                && record["sessionId"].as_str() == Some(session_id)
                && record["isSidechain"].as_bool() == Some(false)
                && record["message"]["id"].as_str() == Some(message_id)
                && record["message"]["model"].as_str() == Some(model)
                && record["message"]["stop_reason"].as_str().is_some()
        })?;
    assistant_context_used(&record)
}

/// Extract the session-scoped fact from a terminal `result` record:
/// cumulative token counters from `modelUsage`, cumulative cost from
/// `total_cost_usd`, the latest single-call context sample retained from a
/// main-thread assistant message, and the model's window from the result
/// or the matching init frame. Neither `result.usage` nor cumulative
/// `modelUsage` counters supply occupancy. With persistent streaming input
/// the cumulative halves are cumulative for the session (resumed cost
/// included) and estimated, so they map to a session-scoped snapshot with
/// source `claude:result`, never to turn expenditure and never converted.
/// Any mapped value yields a fact; only when all are absent is there no
/// session fact, so completion proceeds without one. The transcript file
/// size is filled by the caller (it needs the retained session id), never
/// guessed here.
pub(crate) fn result_session_usage(
    record: &Value,
    latest_model: Option<&str>,
    used: Option<i64>,
    init_context_window: Option<&(String, i64)>,
) -> Option<SessionUsage> {
    let cumulative_tokens = result_session_tokens(record);
    let amount = record
        .get("total_cost_usd")
        .and_then(Value::as_f64)
        .filter(|amount| amount.is_finite() && *amount >= 0.0);
    let window = latest_model
        .and_then(|model| {
            record["modelUsage"]
                .get(model)
                .and_then(frame_context_window)
        })
        .or_else(|| frame_context_window(record))
        .or_else(|| {
            let (initialized_model, window) = init_context_window?;
            (latest_model? == initialized_model).then_some(*window)
        });
    if cumulative_tokens.is_none() && amount.is_none() && used.is_none() && window.is_none() {
        return None;
    }
    let mut usage = SessionUsage {
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
    };
    usage.retain_valid_context();
    Some(usage)
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
    /// The shell command of a command tool (`input.command`: Bash,
    /// PowerShell); `None` for other tools.
    pub command: Option<String>,
    /// `input.run_in_background`: Claude tracks this call as its own
    /// background task and wakes itself when the task ends.
    pub run_in_background: bool,
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
            let command = block
                .input
                .as_ref()
                .and_then(|input| input.get("command"))
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|command| !command.is_empty())
                .map(str::to_owned);
            let run_in_background = block
                .input
                .as_ref()
                .and_then(|input| input.get("run_in_background"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some(ToolUseBlock {
                id,
                name,
                command,
                run_in_background,
            })
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

/// One native tool outcome bound to its exact invocation and verdict.
/// Complete bounded text is retained only for a native failure.
pub(crate) struct ToolResultBlock {
    pub tool_use_id: String,
    pub is_error: Option<bool>,
    pub failure_output: Option<String>,
}

/// Tool outcomes in one `user` record, in wire order. Blocks with a missing
/// or blank `tool_use_id` cannot bind an invocation and are dropped here.
const MAX_TOOL_RESULT_CONTENT_BLOCKS: usize = 32;
const MAX_TOOL_FAILURE_OUTPUT_BYTES: usize = 4 * 1024;

/// Keep only complete bounded textual results. Non-text or oversized results
/// stay out of repeated-failure matching rather than comparing a prefix.
fn bounded_tool_result_text(content: Option<&Value>) -> Option<String> {
    match content? {
        Value::String(text) if text.len() <= MAX_TOOL_FAILURE_OUTPUT_BYTES => Some(text.clone()),
        Value::Array(blocks) if blocks.len() <= MAX_TOOL_RESULT_CONTENT_BLOCKS => {
            let mut output = String::new();
            for block in blocks {
                if block.get("type").and_then(Value::as_str) != Some("text") {
                    return None;
                }
                let text = block.get("text").and_then(Value::as_str)?;
                if output.len().saturating_add(text.len()) > MAX_TOOL_FAILURE_OUTPUT_BYTES {
                    return None;
                }
                output.push_str(text);
            }
            Some(output)
        }
        _ => None,
    }
}

/// Tool outcomes in one user record, in wire order. Blocks with a missing
/// or blank tool_use_id cannot bind an invocation and are dropped here.
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
            let failure_output = (block.is_error == Some(true))
                .then(|| bounded_tool_result_text(block.content.as_ref()))
                .flatten();
            Some(ToolResultBlock {
                tool_use_id,
                is_error: block.is_error,
                failure_output,
            })
        })
        .collect()
}
