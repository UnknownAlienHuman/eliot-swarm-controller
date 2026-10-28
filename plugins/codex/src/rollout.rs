//! Bounded metadata projection of the exact Codex JSONL path returned by thread/read.
//! This is deliberately not a session executor or a general rollout discovery API.

use crate::discovery::fingerprint;
use eliot_core::{Error, NativeEvent, Result, SecretPolicy};
use eliot_plugin::{
    FileIdentity, MAX_OBSERVATION_CURSOR_BYTES, MAX_OBSERVATION_PAGE_BYTES,
    MAX_OBSERVATION_PAGE_ENTRIES, ObservationQuery, file_final_path, file_identity,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, mpsc},
    time::Instant,
};
use tokio::task::JoinHandle;

const CURSOR_PREFIX: &str = "codex-rollout-v1:";
const MAX_LINE_BYTES: usize = 256 * 1024;
const MAX_HEADER_BYTES: usize = 256 * 1024;
const MAX_SCAN_BYTES: usize = 1024 * 1024;
const ANCHOR_BYTES: usize = 4096;
const MAX_PATH_BYTES: usize = 4096;
const LIVE_CURSOR_PREFIX: &str = "codex-rollout-live-v1:";
const LIVE_CLI_VERSION: &str = "0.161.0";

pub(crate) struct LiveRecord {
    pub events: Vec<NativeEvent>,
    pub cursor: String,
}

pub(crate) struct LiveRolloutPage {
    pub records: Vec<LiveRecord>,
    pub cursor: String,
    pub at_end: bool,
    pub partial_tail: bool,
    pub gap: Option<String>,
}

/// One held file and at most one owned blocking operation. Cancelled reads keep
/// their task here; close joins it before releasing the file. Never clone/reopen.
pub(crate) struct LiveRollout {
    file: Option<LiveFile>,
    pending: Option<PendingLiveRead>,
    baseline: String,
}

struct LiveFile {
    file: File,
    home: PathBuf,
    final_path: PathBuf,
    identity: FileIdentity,
    header_end: u64,
    header_hash: String,
    native_ref: String,
    cli_version: String,
    binding: String,
    policy: Arc<SecretPolicy>,
}

struct PendingLiveRead {
    task: JoinHandle<()>,
    result: Mutex<mpsc::Receiver<(LiveFile, Result<LiveRolloutPage>)>>,
}

impl Drop for PendingLiveRead {
    fn drop(&mut self) {
        // Normal close/read awaits the task first. If the owner itself is
        // dropped during a read, wait for its file to return instead of
        // detaching a blocking operation with access to the private rollout.
        let _ = self
            .result
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .recv();
        while !self.task.is_finished() {
            std::thread::yield_now();
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveCursor {
    version: u8,
    binding: String,
    file_identity: FileIdentity,
    offset: u64,
    anchor_sha256: String,
    current_turn_id: Option<String>,
    calls: BTreeMap<String, LiveCall>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveCall {
    turn_id: String,
    name: String,
}

impl LiveRollout {
    pub(crate) fn open(
        path: PathBuf,
        home: PathBuf,
        native_ref: String,
        cli_version: String,
        binding: String,
        policy: Arc<SecretPolicy>,
        deadline: Instant,
    ) -> Result<Self> {
        if cli_version != LIVE_CLI_VERSION {
            return Err(Error::new(
                "unsupported",
                "Codex live rollout codec requires 0.161.0",
            ));
        }
        for value in [&native_ref, &cli_version, &binding] {
            live_identifier(value, &policy)?;
        }
        if !path.is_absolute() || Instant::now() >= deadline {
            return Err(unavailable());
        }
        let home = fs::canonicalize(home).map_err(map_io_error)?;
        let mut file = OpenOptions::new()
            .read(true)
            .open(path)
            .map_err(map_io_error)?;
        if !file.metadata().map_err(map_io_error)?.is_file() {
            return Err(unavailable());
        }
        let identity = file_identity(&file).map_err(map_io_error)?;
        let final_path = file_final_path(&file).map_err(map_io_error)?;
        verify_final_path(&final_path, &home)?;
        let (header_bytes, header_end) =
            read_complete_line(&mut file, 0, MAX_HEADER_BYTES, deadline)?
                .ok_or_else(unavailable)?;
        let header = parse_line(&header_bytes)?;
        validate_header(&header, &native_ref, &cli_version)?;
        native_timestamp(&header.timestamp)?;
        let binding = fingerprint(json!({
            "domain":"eliot/codex/live-rollout/v1", "caller":binding,
            "native_ref":native_ref, "cli_version":cli_version, "file_identity":identity,
            "home":home.to_str().ok_or_else(unavailable)?,
            "path":final_path.to_str().ok_or_else(unavailable)?,
        }))?;
        let length = file.metadata().map_err(map_io_error)?.len();
        let offset = complete_live_edge(&mut file, header_end, length, deadline)?;
        let cursor = LiveCursor {
            version: 1,
            binding: binding.clone(),
            file_identity: identity,
            offset,
            anchor_sha256: anchor_at(&mut file, offset, deadline)?,
            current_turn_id: None,
            calls: BTreeMap::new(),
        };
        let baseline = encode_live_cursor(&cursor, &policy)?;
        Ok(Self {
            file: Some(LiveFile {
                file,
                home,
                final_path,
                identity,
                header_end,
                header_hash: hex_digest(&header_bytes),
                native_ref,
                cli_version,
                binding,
                policy,
            }),
            pending: None,
            baseline,
        })
    }

    pub(crate) fn baseline_cursor(&self) -> &str {
        &self.baseline
    }

    pub(crate) async fn read_page(
        &mut self,
        cursor: &str,
        limit: usize,
        deadline: Instant,
    ) -> Result<LiveRolloutPage> {
        // A prior caller may have been cancelled at its await. Recover the file
        // first, then deterministically read the supplied acknowledged cursor.
        let _ = self.join_read().await?;
        if !(1..=MAX_OBSERVATION_PAGE_ENTRIES).contains(&limit) {
            return Err(invalid());
        }
        if Instant::now() >= deadline {
            return Err(read_deadline());
        }
        let mut file = self.file.take().ok_or_else(unavailable)?;
        let cursor = cursor.to_owned();
        let (sender, result) = mpsc::sync_channel(1);
        let task = tokio::task::spawn_blocking(move || {
            let page = file.read_page(&cursor, limit, deadline);
            let _ = sender.send((file, page));
        });
        self.pending = Some(PendingLiveRead {
            task,
            result: Mutex::new(result),
        });
        self.join_read().await?.ok_or_else(unavailable)?
    }

    pub(crate) async fn close(&mut self) -> Result<()> {
        let joined = self.join_read().await;
        self.file = None;
        joined.map(|_| ())
    }

    async fn join_read(&mut self) -> Result<Option<Result<LiveRolloutPage>>> {
        let Some(pending) = self.pending.as_mut() else {
            return Ok(None);
        };
        let joined = (&mut pending.task).await;
        let mut pending = self.pending.take().ok_or_else(unavailable)?;
        joined.map_err(|_| Error::new("unknown", "Codex live rollout read task failed"))?;
        let (file, page) = pending
            .result
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .try_recv()
            .map_err(|_| unavailable())?;
        self.file = Some(file);
        Ok(Some(page))
    }
}

impl LiveFile {
    fn verify(&mut self, deadline: Instant) -> Result<u64> {
        let metadata = self.file.metadata().map_err(map_io_error)?;
        if !metadata.is_file() || file_identity(&self.file).map_err(map_io_error)? != self.identity
        {
            return Err(unavailable());
        }
        let final_path = file_final_path(&self.file).map_err(map_io_error)?;
        verify_final_path(&final_path, &self.home)?;
        if final_path != self.final_path {
            return Err(unavailable());
        }
        let (bytes, end) = read_complete_line(&mut self.file, 0, MAX_HEADER_BYTES, deadline)?
            .ok_or_else(unavailable)?;
        let header = parse_line(&bytes)?;
        validate_header(&header, &self.native_ref, &self.cli_version)?;
        if end != self.header_end || hex_digest(&bytes) != self.header_hash {
            return Err(integrity());
        }
        Ok(metadata.len())
    }

    fn read_page(&mut self, raw: &str, limit: usize, deadline: Instant) -> Result<LiveRolloutPage> {
        let mut state = decode_live_cursor(raw, &self.binding, self.identity, &self.policy)?;
        let snapshot = match self.verify(deadline) {
            Ok(length) => length,
            Err(error) => return Ok(live_gap(raw, &error)),
        };
        if state.offset < self.header_end || state.offset > snapshot {
            return Ok(live_gap(raw, &unavailable()));
        }
        if read_range(&mut self.file, state.offset - 1, 1, deadline)?.as_slice() != b"\n"
            || anchor_at(&mut self.file, state.offset, deadline)? != state.anchor_sha256
        {
            return Ok(live_gap(raw, &unavailable()));
        }
        let available = snapshot - state.offset;
        let length = available.min(MAX_SCAN_BYTES as u64) as usize;
        let bytes = read_range(&mut self.file, state.offset, length, deadline)?;
        let scan_limited = available > length as u64;
        let mut position = 0;
        let mut records = Vec::new();
        let mut page_bytes = 0usize;
        let mut partial_tail = false;
        while position < bytes.len() {
            if Instant::now() >= deadline {
                return Err(read_deadline());
            }
            let remaining = &bytes[position..];
            let Some(end) = remaining.iter().position(|byte| *byte == b'\n') else {
                if remaining.len() > MAX_LINE_BYTES {
                    return Ok(live_gap(raw, &too_large()));
                }
                partial_tail = !scan_limited;
                break;
            };
            if end > MAX_LINE_BYTES {
                return Ok(live_gap(raw, &too_large()));
            }
            let start = state.offset;
            let next = start.checked_add(end as u64 + 1).ok_or_else(too_large)?;
            let mut candidate = state.clone();
            let normalized = parse_line(&remaining[..end]).and_then(|line| {
                normalize_live_line(
                    &mut candidate,
                    &line,
                    self.identity,
                    &self.native_ref,
                    start,
                    next,
                    &self.policy,
                )
            });
            candidate.offset = next;
            candidate.anchor_sha256 = anchor_at(&mut self.file, next, deadline)?;
            let events = match normalized {
                Ok(events) => events,
                Err(error) => {
                    candidate.current_turn_id = None;
                    candidate.calls.clear();
                    let cursor = encode_live_cursor(&candidate, &self.policy)?;
                    let mut gap = live_gap(&cursor, &error);
                    gap.partial_tail = false;
                    return Ok(gap);
                }
            };
            let cursor = match encode_live_cursor(&candidate, &self.policy) {
                Ok(cursor) => cursor,
                Err(error) => return Ok(live_gap(raw, &error)),
            };
            if !events.is_empty() {
                let encoded = encoded_len(&json!({"events":events,"cursor":cursor}))?;
                if page_bytes
                    .saturating_add(encoded)
                    .saturating_add(MAX_OBSERVATION_CURSOR_BYTES + 4096)
                    > MAX_OBSERVATION_PAGE_BYTES
                {
                    if records.is_empty() {
                        return Ok(live_gap(raw, &too_large()));
                    }
                    break;
                }
                page_bytes += encoded;
                records.push(LiveRecord { events, cursor });
            }
            state = candidate;
            position += end + 1;
            if records.len() == limit {
                break;
            }
        }
        let after = match self.verify(deadline) {
            Ok(length) => length,
            Err(error) => return Ok(live_gap(raw, &error)),
        };
        if after < snapshot || after < state.offset {
            return Ok(live_gap(raw, &unavailable()));
        }
        let cursor = encode_live_cursor(&state, &self.policy)?;
        if state.offset
            == decode_live_cursor(raw, &self.binding, self.identity, &self.policy)?.offset
            && !partial_tail
            && state.offset < snapshot
        {
            return Ok(live_gap(raw, &unavailable()));
        }
        Ok(LiveRolloutPage {
            // This is the captured file edge, not a native completion assertion.
            // An append racing the final metadata query belongs to the next poll.
            records,
            cursor,
            at_end: state.offset == snapshot && !partial_tail,
            partial_tail,
            gap: None,
        })
    }
}

fn complete_live_edge(
    file: &mut File,
    header_end: u64,
    length: u64,
    deadline: Instant,
) -> Result<u64> {
    if length < header_end {
        return Err(unavailable());
    }
    let start = length.saturating_sub(MAX_SCAN_BYTES as u64).max(header_end);
    let bytes = read_range(file, start, (length - start) as usize, deadline)?;
    let offset = match bytes.iter().rposition(|byte| *byte == b'\n') {
        Some(end) => start + end as u64 + 1,
        None if start == header_end => header_end,
        None => return Err(too_large()),
    };
    if length - offset > MAX_LINE_BYTES as u64 {
        return Err(too_large());
    }
    Ok(offset)
}

fn encode_live_cursor(cursor: &LiveCursor, policy: &SecretPolicy) -> Result<String> {
    let raw = format!(
        "{LIVE_CURSOR_PREFIX}{}",
        serde_json::to_string(cursor).map_err(|_| integrity())?
    );
    if raw.len() > MAX_OBSERVATION_CURSOR_BYTES {
        return Err(too_large());
    }
    if policy.contains_text(&raw) {
        return Err(live_secret());
    }
    Ok(raw)
}

fn decode_live_cursor(
    raw: &str,
    binding: &str,
    identity: FileIdentity,
    policy: &SecretPolicy,
) -> Result<LiveCursor> {
    if raw.len() > MAX_OBSERVATION_CURSOR_BYTES {
        return Err(too_large());
    }
    if policy.contains_text(raw) {
        return Err(live_secret());
    }
    let cursor: LiveCursor = serde_json::from_str(
        raw.strip_prefix(LIVE_CURSOR_PREFIX)
            .ok_or_else(cursor_mismatch)?,
    )
    .map_err(|_| cursor_mismatch())?;
    if cursor.version != 1
        || cursor.binding != binding
        || cursor.file_identity != identity
        || encode_live_cursor(&cursor, policy)? != raw
    {
        return Err(cursor_mismatch());
    }
    if let Some(turn) = &cursor.current_turn_id {
        live_identifier(turn, policy)?;
    }
    for (id, call) in &cursor.calls {
        live_identifier(id, policy)?;
        live_identifier(&call.turn_id, policy)?;
    }
    Ok(cursor)
}

fn live_identifier(value: &str, policy: &SecretPolicy) -> Result<()> {
    validate_text(value, 256)?;
    if policy.contains_text(value) {
        return Err(live_secret());
    }
    Ok(())
}

fn live_secret() -> Error {
    Error::new(
        "secret_in_payload",
        "Codex live rollout immutable binding contains a known secret",
    )
}

fn live_gap(cursor: &str, error: &Error) -> LiveRolloutPage {
    LiveRolloutPage {
        records: Vec::new(),
        cursor: cursor.to_owned(),
        at_end: false,
        partial_tail: false,
        gap: Some(format!("codex_rollout_{}", error.code)),
    }
}

// Pinned format: openai/codex 979011409de0a60b52f179721948e65531d26144,
// protocol/src/{protocol,models}.rs and rollout/src/policy.rs. Lifecycle
// EventMsg records are canonical; legacy message/tool echoes are not republished.
fn normalize_live_line(
    state: &mut LiveCursor,
    line: &RolloutLine,
    identity: FileIdentity,
    native_ref: &str,
    start: u64,
    end: u64,
    policy: &SecretPolicy,
) -> Result<Vec<NativeEvent>> {
    let ts = native_timestamp(&line.timestamp)?;
    if line.kind == "session_meta" {
        return Err(integrity());
    }
    if !line.payload.is_object() {
        return Err(integrity());
    }
    if line.kind == "turn_context" {
        let turn = live_optional_id(&line.payload, "turn_id", policy)?;
        if state.current_turn_id.as_deref() != turn {
            state.calls.clear();
        }
        state.current_turn_id = turn.map(str::to_owned);
        return Ok(Vec::new());
    }
    let normalized = match line.kind.as_str() {
        "event_msg" => normalize_live_event(state, &line.payload, policy)?,
        "response_item" => normalize_live_item(state, &line.payload, policy)?,
        _ => return Ok(Vec::new()),
    };
    let Some((kind, mut data)) = normalized else {
        return Ok(Vec::new());
    };
    // Binding values are checked before any body redaction. Object keys are
    // never rewritten; a known secret in a publication key rejects the record.
    data["native_timestamp"] = json!(line.timestamp);
    sanitize_live_publication(kind, &mut data, policy)?;
    let digest = fingerprint(json!({
        "domain":"eliot/codex/live-rollout-record/v1", "native_ref":native_ref,
        "file_identity":identity, "byte_start":start, "byte_end":end,
        "kind":kind, "data":data, "ts":ts,
    }))?;
    let native_id = format!("codex-rollout:{digest}");
    if policy.contains_text(&native_id) {
        return Err(live_secret());
    }
    let event = NativeEvent {
        kind: kind.to_owned(),
        native_id,
        data,
        ts: Some(ts),
    };
    if serde_json::to_vec(&event).map_err(|_| integrity())?.len() > eliot_plugin::MAX_EVENT_BYTES {
        return Err(too_large());
    }
    Ok(vec![event])
}

fn sanitize_live_publication(kind: &str, data: &mut Value, policy: &SecretPolicy) -> Result<()> {
    // Only body fields may change. IDs, roles, phases, status/reason/type
    // discriminants and JSON keys retain their native meaning or reject.
    for field in [
        "text",
        "final_text",
        "arguments",
        "input",
        "tool",
        "native_timestamp",
        "error",
    ] {
        if let Some(value) = data.get_mut(field) {
            policy.redact_values(value);
        }
    }
    if let Some(output) = data.get_mut("output") {
        if let Some(parts) = output.as_array_mut() {
            for part in parts {
                if let Some(text) = part.get_mut("text") {
                    policy.redact_values(text);
                }
            }
        } else {
            policy.redact_values(output);
        }
    }
    if policy.contains_text(kind) || policy.contains_value(data) {
        return Err(live_secret());
    }
    Ok(())
}

fn normalize_live_event(
    state: &mut LiveCursor,
    payload: &Value,
    policy: &SecretPolicy,
) -> Result<Option<(&'static str, Value)>> {
    let event_type = payload
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(integrity)?;
    match event_type {
        "agent_message" | "user_message" | "exec_command_end" => return Ok(None),
        "task_started" | "turn_started" | "task_complete" | "turn_complete" | "task_aborted"
        | "turn_aborted" => {}
        _ => return Err(live_unsupported()),
    }
    let turn = live_optional_id(payload, "turn_id", policy)?.ok_or_else(live_unbound)?;
    if state
        .current_turn_id
        .as_deref()
        .is_some_and(|current| current != turn)
    {
        return Err(live_unbound());
    }
    let mut data = json!({"turn_id":turn});
    for field in ["started_at", "completed_at"] {
        if let Some(seconds) = optional_i64(payload, field)? {
            data[field] = json!(seconds.checked_mul(1000).ok_or_else(integrity)?);
        }
    }
    if let Some(duration) = optional_i64(payload, "duration_ms")? {
        if duration < 0 {
            return Err(integrity());
        }
        data["duration_ms"] = json!(duration);
    }
    if matches!(event_type, "task_started" | "turn_started") {
        state.current_turn_id = Some(turn.to_owned());
        return Ok(Some(("turn.started", data)));
    }
    let has_error = match payload.get("error") {
        None | Some(Value::Null) => false,
        Some(Value::Object(error)) => {
            data["error"] = json!({"message":error.get("message").and_then(Value::as_str).ok_or_else(integrity)?});
            true
        }
        Some(_) => return Err(integrity()),
    };
    let aborted = matches!(event_type, "task_aborted" | "turn_aborted");
    if aborted {
        let reason = payload
            .get("reason")
            .and_then(Value::as_str)
            .ok_or_else(integrity)?;
        if !matches!(
            reason,
            "interrupted" | "replaced" | "review_ended" | "budget_limited"
        ) {
            return Err(live_unsupported());
        }
        data["abort_reason"] = json!(reason);
    } else {
        match payload.get("last_agent_message") {
            None | Some(Value::Null) => {}
            Some(Value::String(text)) => data["final_text"] = json!(text),
            Some(_) => return Err(integrity()),
        }
    }
    data["status"] = json!(if aborted || has_error {
        "failed"
    } else {
        "completed"
    });
    state.current_turn_id = None;
    state.calls.retain(|_, call| call.turn_id != turn);
    // The terminal's own final_text is retained without emitting a duplicate
    // message completion from last_agent_message.
    Ok(Some(("turn.completed", data)))
}

fn normalize_live_item(
    state: &mut LiveCursor,
    payload: &Value,
    policy: &SecretPolicy,
) -> Result<Option<(&'static str, Value)>> {
    let kind = payload
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(integrity)?;
    if kind == "reasoning" {
        return Ok(None);
    }
    if !matches!(
        kind,
        "message"
            | "function_call"
            | "custom_tool_call"
            | "function_call_output"
            | "custom_tool_call_output"
    ) {
        return Err(live_unsupported());
    }
    let explicit = match payload.get("internal_chat_message_metadata_passthrough") {
        None | Some(Value::Null) => None,
        Some(metadata) if metadata.is_object() => live_optional_id(metadata, "turn_id", policy)?,
        Some(_) => return Err(integrity()),
    };
    let call_id = if kind == "message" {
        None
    } else {
        live_optional_id(payload, "call_id", policy)?
    };
    let saved = call_id.and_then(|id| state.calls.get(id)).cloned();
    let turn = explicit
        .or(state.current_turn_id.as_deref())
        .or_else(|| saved.as_ref().map(|call| call.turn_id.as_str()))
        .ok_or_else(live_unbound)?
        .to_owned();
    live_identifier(&turn, policy)?;
    if explicit.is_some_and(|id| id != turn)
        || state
            .current_turn_id
            .as_deref()
            .is_some_and(|id| id != turn)
        || saved.as_ref().is_some_and(|call| call.turn_id != turn)
    {
        return Err(live_unbound());
    }
    let mut data = json!({"turn_id":turn});
    if let Some(id) = live_optional_id(payload, "id", policy)? {
        data["native_item_id"] = json!(id);
    }
    let event_kind = if kind == "message" {
        let role = payload
            .get("role")
            .and_then(Value::as_str)
            .ok_or_else(integrity)?;
        if !matches!(role, "assistant" | "user" | "system" | "developer") {
            return Err(live_unsupported());
        }
        data["role"] = json!(role);
        data["text"] = json!(live_message_text(payload)?);
        if let Some(id) = live_optional_id(payload, "id", policy)? {
            data["item_id"] = json!(id);
        }
        match payload.get("phase") {
            None | Some(Value::Null) => {}
            Some(Value::String(phase))
                if matches!(phase.as_str(), "commentary" | "final_answer") =>
            {
                if role != "assistant" {
                    return Err(live_unsupported());
                }
                data["phase"] = json!(phase);
            }
            _ => return Err(live_unsupported()),
        }
        "message.completed"
    } else {
        let call_id = call_id.ok_or_else(live_unbound)?;
        data["item_id"] = json!(call_id);
        data["call_id"] = json!(call_id);
        let output = matches!(kind, "function_call_output" | "custom_tool_call_output");
        let name = match payload.get("name") {
            None | Some(Value::Null) if output => saved.as_ref().map(|call| call.name.as_str()),
            Some(Value::String(name)) if !name.trim().is_empty() => Some(name.as_str()),
            _ => return Err(integrity()),
        };
        if let Some(name) = name {
            let name = policy.redact_text(name);
            if saved.as_ref().is_some_and(|call| call.name != name) {
                return Err(live_unbound());
            }
            data["tool"] = json!(name);
        }
        data["native_type"] = json!(kind);
        if output {
            data["output"] = live_tool_output(payload.get("output").ok_or_else(integrity)?)?;
            state.calls.remove(call_id);
            "tool.completed"
        } else {
            if saved.is_some() {
                return Err(live_unbound());
            }
            let field = if kind == "function_call" {
                "arguments"
            } else {
                "input"
            };
            data[field] = json!(
                payload
                    .get(field)
                    .and_then(Value::as_str)
                    .ok_or_else(integrity)?
            );
            state.calls.insert(
                call_id.to_owned(),
                LiveCall {
                    turn_id: turn.clone(),
                    name: data["tool"].as_str().ok_or_else(integrity)?.to_owned(),
                },
            );
            "tool.started"
        }
    };
    state.current_turn_id = Some(turn);
    Ok(Some((event_kind, data)))
}

fn live_message_text(payload: &Value) -> Result<String> {
    let parts = payload
        .get("content")
        .and_then(Value::as_array)
        .ok_or_else(integrity)?;
    let mut text = String::new();
    for (index, part) in parts.iter().enumerate() {
        if !matches!(
            part.get("type").and_then(Value::as_str),
            Some("input_text" | "output_text")
        ) {
            return Err(live_unsupported());
        }
        if index != 0 {
            text.push('\n');
        }
        text.push_str(
            part.get("text")
                .and_then(Value::as_str)
                .ok_or_else(integrity)?,
        );
    }
    Ok(text)
}

fn live_tool_output(body: &Value) -> Result<Value> {
    match body {
        Value::String(_) => Ok(body.clone()),
        Value::Array(parts) => {
            let mut output = Vec::new();
            for part in parts {
                if part.get("type").and_then(Value::as_str) != Some("input_text") {
                    return Err(live_unsupported());
                }
                output.push(json!({"type":"input_text", "text":
                    part.get("text").and_then(Value::as_str).ok_or_else(integrity)?}));
            }
            Ok(Value::Array(output))
        }
        _ => Err(integrity()),
    }
}

fn live_optional_id<'a>(
    value: &'a Value,
    field: &str,
    policy: &SecretPolicy,
) -> Result<Option<&'a str>> {
    match value.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(id)) => {
            live_identifier(id, policy)?;
            Ok(Some(id))
        }
        Some(_) => Err(integrity()),
    }
}

fn native_timestamp(value: &str) -> Result<i64> {
    // Dependency, not copied parser code: chrono 0.4.45, MIT OR Apache-2.0,
    // MSRV 1.62; https://docs.rs/chrono/0.4.45/chrono/struct.DateTime.html#method.parse_from_rfc3339
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.timestamp_millis())
        .map_err(|_| integrity())
}

fn live_unsupported() -> Error {
    Error::new(
        "unsupported",
        "Codex rollout record is outside the 0.161.0 live projection",
    )
}

fn live_unbound() -> Error {
    Error::new(
        "source_integrity",
        "Codex rollout record has no unambiguous native turn or call binding",
    )
}

#[derive(Clone, Debug)]
pub(super) struct RolloutPage {
    pub entries: Vec<Value>,
    pub next_cursor: Option<String>,
    pub coverage: Value,
}

pub(super) struct RolloutReadRequest {
    pub path: PathBuf,
    pub codex_home: PathBuf,
    pub native_ref: String,
    pub cli_version: String,
    pub query: ObservationQuery,
    pub binding: String,
    pub secrets: Vec<String>,
    pub deadline: Instant,
}

struct CursorValidation<'a> {
    binding: &'a str,
    identity: FileIdentity,
    header_end: u64,
    file_len: u64,
    secrets: &'a [String],
    deadline: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct RolloutCursor {
    version: u8,
    binding: String,
    file_identity: FileIdentity,
    /// Exclusive raw-byte position, always immediately after a complete LF.
    offset: u64,
    anchor_sha256: String,
    current_turn_id: Option<String>,
    active_turn: Option<ActiveTurn>,
    any_turn_complete: bool,
    last_completed_turn_id: Option<String>,
    pending_turn_context: bool,
    requested_turn_seen: bool,
    requested_turn_complete: bool,
    turn_anomaly: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ActiveTurn {
    turn_id: String,
    started_at: Option<i64>,
}

struct RolloutLine {
    timestamp: String,
    ordinal: Option<u64>,
    kind: String,
    payload: Value,
    metadata: Option<Value>,
}

pub(super) fn read_page(request: RolloutReadRequest) -> Result<RolloutPage> {
    let RolloutReadRequest {
        path,
        codex_home,
        native_ref,
        cli_version,
        query,
        binding,
        secrets,
        deadline,
    } = request;
    let view = query.view.as_str();
    let turn_id = query.turn_id.as_deref();
    let raw_cursor = query.cursor.as_deref();
    let limit = query.limit;
    validate_text(&native_ref, 256)?;
    validate_text(&cli_version, 256)?;
    validate_text(&binding, 256)?;
    if query.source != "rollout"
        || !matches!(view, "subject" | "turns" | "items")
        || !(1..=MAX_OBSERVATION_PAGE_ENTRIES).contains(&limit)
        || (view == "items") != turn_id.is_some()
    {
        return Err(invalid());
    }
    if let Some(turn_id) = turn_id {
        validate_text(turn_id, 256)?;
    }
    if Instant::now() >= deadline {
        return Err(read_deadline());
    }

    // Open only the path supplied by this exact subject's thread/read metadata.
    let mut file = OpenOptions::new()
        .read(true)
        .open(&path)
        .map_err(map_io_error)?;
    if !file.metadata().map_err(map_io_error)?.is_file() {
        return Err(unavailable());
    }
    // Both facts come from the same handle that will be read below.
    let identity = file_identity(&file).map_err(map_io_error)?;
    let final_path = file_final_path(&file).map_err(map_io_error)?;
    verify_final_path(&final_path, &codex_home)?;

    let Some((header_bytes, header_end)) =
        read_complete_line(&mut file, 0, MAX_HEADER_BYTES, deadline)?
    else {
        return Err(unavailable());
    };
    let header = parse_line(&header_bytes)?;
    if header.kind != "session_meta" {
        return Err(integrity());
    }
    let subject = validate_header(&header, &native_ref, &cli_version)?;

    if view == "subject" {
        if raw_cursor.is_some() {
            return Err(invalid());
        }
        return Ok(RolloutPage {
            entries: vec![subject],
            next_cursor: None,
            coverage: json!({"kind":"rollout_metadata", "complete":true, "live":false}),
        });
    }

    let snapshot_len = file.metadata().map_err(map_io_error)?.len();
    if snapshot_len < header_end {
        return Err(unavailable());
    }
    let mut state = match raw_cursor {
        Some(raw) => decode_cursor(
            raw,
            &mut file,
            CursorValidation {
                binding: &binding,
                identity,
                header_end,
                file_len: snapshot_len,
                secrets: &secrets,
                deadline,
            },
        )?,
        None => {
            let mut state = RolloutCursor {
                version: 1,
                binding: binding.clone(),
                file_identity: identity,
                offset: header_end,
                anchor_sha256: String::new(),
                current_turn_id: None,
                active_turn: None,
                any_turn_complete: false,
                last_completed_turn_id: None,
                pending_turn_context: false,
                requested_turn_seen: false,
                requested_turn_complete: false,
                turn_anomaly: false,
            };
            state.anchor_sha256 = anchor_at(&mut file, state.offset, deadline)?;
            state
        }
    };

    let scan_start = state.offset;
    let available = snapshot_len.saturating_sub(scan_start);
    let read_len = available.min(MAX_SCAN_BYTES as u64) as usize;
    let bytes = read_range(&mut file, scan_start, read_len, deadline)?;
    let scan_limited = available > read_len as u64;

    let mut entries = Vec::new();
    let mut entry_bytes = 0usize;
    let mut position = 0usize;
    let mut partial_tail = false;
    let mut stopped_for_page_bound = false;
    while position < bytes.len() {
        if Instant::now() >= deadline {
            return Err(read_deadline());
        }
        let remaining = &bytes[position..];
        let Some(line_end) = remaining.iter().position(|byte| *byte == b'\n') else {
            if remaining.len() > MAX_LINE_BYTES {
                return Err(too_large());
            }
            if !scan_limited {
                partial_tail = true;
            }
            break;
        };
        if line_end > MAX_LINE_BYTES {
            return Err(too_large());
        }
        let line_start = state.offset;
        let line = parse_line(&remaining[..line_end])?;
        if line.kind == "session_meta" {
            return Err(integrity());
        }
        let after_line = line_start
            .checked_add(line_end as u64)
            .and_then(|offset| offset.checked_add(1))
            .ok_or_else(too_large)?;

        // Commit line context and byte offset together only when its projection fits.
        let mut candidate = state.clone();
        let projected = project_line(&mut candidate, &line, line_start, view, turn_id)?;
        let mut projected_bytes = 0usize;
        for entry in &projected {
            projected_bytes = projected_bytes
                .checked_add(encoded_len(entry)?)
                .ok_or_else(too_large)?;
        }
        if !projected.is_empty()
            && (entries.len().saturating_add(projected.len()) > limit
                || entry_bytes
                    .saturating_add(projected_bytes)
                    .saturating_add(4096)
                    > MAX_OBSERVATION_PAGE_BYTES)
        {
            if entries.is_empty()
                && projected_bytes.saturating_add(4096) > MAX_OBSERVATION_PAGE_BYTES
            {
                return Err(too_large());
            }
            stopped_for_page_bound = true;
            break;
        }
        entry_bytes = entry_bytes.saturating_add(projected_bytes);
        entries.extend(projected);
        candidate.offset = after_line;
        state = candidate;
        position += line_end + 1;
        if entries.len() >= limit {
            stopped_for_page_bound = position < bytes.len() || scan_limited;
            break;
        }
    }

    let file_len_after = file.metadata().map_err(map_io_error)?.len();
    if file_len_after < snapshot_len || file_len_after < state.offset {
        return Err(unavailable());
    }
    let at_current_end = state.offset == file_len_after && !partial_tail;
    let view_complete = match view {
        "turns" => {
            state.any_turn_complete
                && state.active_turn.is_none()
                && !state.pending_turn_context
                && !state.turn_anomaly
        }
        "items" => state.requested_turn_seen && state.requested_turn_complete,
        _ => true,
    };
    let complete = at_current_end && view_complete;

    state.anchor_sha256 = anchor_at(&mut file, state.offset, deadline)?;
    reject_cursor_secrets(&state, &secrets)?;
    let encoded_cursor = format!(
        "{CURSOR_PREFIX}{}",
        serde_json::to_string(&state).map_err(|_| integrity())?
    );
    if encoded_cursor.len() > MAX_OBSERVATION_CURSOR_BYTES {
        return Err(too_large());
    }

    let turn_state = match view {
        "turns" if state.turn_anomaly => "unknown",
        "turns" if state.active_turn.is_some() || state.pending_turn_context => "pending",
        "turns" if !state.any_turn_complete => "not_materialized",
        "items" if !state.requested_turn_seen => "not_found_in_scanned_history",
        "items" if !state.requested_turn_complete => "pending_or_unknown",
        _ => "known",
    };
    let coverage = json!({
        "kind": "rollout_jsonl",
        "complete": complete,
        "live": false,
        "at_current_end": at_current_end,
        "partial_tail": partial_tail,
        "scan_limited": scan_limited,
        "page_limited": stopped_for_page_bound,
        "turn_state": turn_state,
        "rewrite_detection": "file_identity_and_bounded_boundary_anchor",
    });
    let next_cursor = if raw_cursor == Some(encoded_cursor.as_str()) {
        None
    } else {
        Some(encoded_cursor)
    };
    Ok(RolloutPage {
        entries,
        // A progressed checkpoint doubles as a later tail-resume cursor. When no
        // byte/context progress occurred, leave the caller's cursor unchanged.
        next_cursor,
        coverage,
    })
}

fn decode_cursor(
    raw: &str,
    file: &mut File,
    context: CursorValidation<'_>,
) -> Result<RolloutCursor> {
    let CursorValidation {
        binding,
        identity,
        header_end,
        file_len,
        secrets,
        deadline,
    } = context;
    if raw.len() > MAX_OBSERVATION_CURSOR_BYTES {
        return Err(too_large());
    }
    reject_secret_text(raw, secrets)?;
    let cursor: RolloutCursor = serde_json::from_str(
        raw.strip_prefix(CURSOR_PREFIX)
            .ok_or_else(cursor_mismatch)?,
    )
    .map_err(|_| cursor_mismatch())?;
    let canonical = format!(
        "{CURSOR_PREFIX}{}",
        serde_json::to_string(&cursor).map_err(|_| cursor_mismatch())?
    );
    if canonical != raw {
        return Err(cursor_mismatch());
    }
    if cursor.version != 1 || cursor.binding != binding {
        return Err(cursor_mismatch());
    }
    if cursor.file_identity != identity || cursor.offset < header_end || cursor.offset > file_len {
        return Err(unavailable());
    }
    if cursor.offset > 0 {
        let previous = read_range(file, cursor.offset - 1, 1, deadline)?;
        if previous.first() != Some(&b'\n') {
            return Err(unavailable());
        }
    }
    if anchor_at(file, cursor.offset, deadline)? != cursor.anchor_sha256 {
        return Err(unavailable());
    }
    if let Some(turn_id) = &cursor.current_turn_id {
        validate_text(turn_id, 256).map_err(|_| cursor_mismatch())?;
        reject_secret_text(turn_id, secrets)?;
    }
    if let Some(turn_id) = &cursor.last_completed_turn_id {
        validate_text(turn_id, 256).map_err(|_| cursor_mismatch())?;
        reject_secret_text(turn_id, secrets)?;
    }
    if let Some(active) = &cursor.active_turn {
        validate_text(&active.turn_id, 256).map_err(|_| cursor_mismatch())?;
        reject_secret_text(&active.turn_id, secrets)?;
    }
    Ok(cursor)
}

fn project_line(
    state: &mut RolloutCursor,
    line: &RolloutLine,
    byte_offset: u64,
    view: &str,
    requested_turn: Option<&str>,
) -> Result<Vec<Value>> {
    let event_type = if line.kind == "event_msg" {
        line.payload.get("type").and_then(Value::as_str)
    } else {
        None
    };
    let payload_turn = line.payload.get("turn_id").and_then(Value::as_str);
    let metadata_turn = line
        .metadata
        .as_ref()
        .and_then(|metadata| metadata.get("turn_id"))
        .and_then(Value::as_str);
    let explicit_turn = payload_turn.or(metadata_turn);
    if let Some(turn_id) = explicit_turn {
        validate_text(turn_id, 256)?;
    }

    if line.kind == "turn_context"
        && let Some(turn_id) = payload_turn
    {
        state.current_turn_id = Some(turn_id.to_owned());
        state.requested_turn_seen |= requested_turn == Some(turn_id);
        if state.last_completed_turn_id.as_deref() != Some(turn_id) && state.active_turn.is_none() {
            state.pending_turn_context = true;
        }
    }

    if matches!(event_type, Some("task_started" | "turn_started"))
        && let Some(turn_id) = explicit_turn
    {
        if state
            .active_turn
            .as_ref()
            .is_some_and(|active| active.turn_id != turn_id)
        {
            state.turn_anomaly = true;
        }
        state.current_turn_id = Some(turn_id.to_owned());
        state.active_turn = Some(ActiveTurn {
            turn_id: turn_id.to_owned(),
            started_at: optional_i64(&line.payload, "started_at")?,
        });
        state.pending_turn_context = false;
        state.requested_turn_seen |= requested_turn == Some(turn_id);
    }

    let mut entries = Vec::new();
    if matches!(event_type, Some("task_complete" | "turn_complete")) {
        let completed_turn_id = explicit_turn
            .or(state.current_turn_id.as_deref())
            .map(str::to_owned);
        if let Some(turn_id) = completed_turn_id {
            validate_text(&turn_id, 256)?;
            state.current_turn_id = Some(turn_id.clone());
            state.any_turn_complete = true;
            state.last_completed_turn_id = Some(turn_id.clone());
            state.pending_turn_context = false;
            state.requested_turn_seen |= requested_turn == Some(turn_id.as_str());
            state.requested_turn_complete |= requested_turn == Some(turn_id.as_str());
            let active = state
                .active_turn
                .as_ref()
                .filter(|active| active.turn_id.as_str() == turn_id.as_str())
                .cloned();
            let started_at = optional_i64(&line.payload, "started_at")?
                .or_else(|| active.as_ref().and_then(|active| active.started_at));
            let completed_at = optional_i64(&line.payload, "completed_at")?;
            let duration_ms = optional_i64(&line.payload, "duration_ms")?;
            if duration_ms.is_some_and(|duration| duration < 0) {
                return Err(integrity());
            }
            if view == "turns" {
                entries.push(json!({
                    "turn_id": turn_id,
                    "status": if line.payload.get("error").is_some_and(|error| !error.is_null()) {"failed"} else {"completed"},
                    "started_at": started_at.map(super::milliseconds).transpose()?,
                    "completed_at": completed_at.map(super::milliseconds).transpose()?,
                    "duration_ms": duration_ms,
                    "ordinal": line.ordinal,
                }));
            }
            if active.is_some() {
                state.active_turn = None;
            }
        }
    }

    if view == "items" && matches!(line.kind.as_str(), "response_item" | "event_msg") {
        let item_turn = explicit_turn.or(state.current_turn_id.as_deref());
        if item_turn == requested_turn
            && let Some(turn_id) = item_turn
        {
            let kind = if line.kind == "event_msg" {
                event_type.unwrap_or("unknown")
            } else {
                line.payload
                    .get("type")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
            };
            validate_text(kind, 128)?;
            let item_id = if line.kind == "response_item" {
                line.payload
                    .get("id")
                    .and_then(Value::as_str)
                    .map(str::to_owned)
            } else {
                None
            };
            if let Some(item_id) = &item_id {
                validate_text(item_id, 256)?;
            }
            let status = event_type.and_then(|event| match event {
                "task_started" | "turn_started" => Some("in_progress"),
                "task_complete" | "turn_complete"
                    if line
                        .payload
                        .get("error")
                        .is_some_and(|error| !error.is_null()) =>
                {
                    Some("failed")
                }
                "task_complete" | "turn_complete" => Some("completed"),
                _ => None,
            });
            let rollout_id = line
                .ordinal
                .map(|ordinal| format!("ordinal:{ordinal}"))
                .unwrap_or_else(|| format!("offset:{byte_offset}"));
            entries.push(json!({
                "turn_id": turn_id,
                "item_id": item_id,
                "rollout_id": rollout_id,
                "kind": kind,
                "status": status,
                "timestamp": line.timestamp,
                "ordinal": line.ordinal,
            }));
            state.requested_turn_seen = true;
        }
    }
    Ok(entries)
}

fn parse_line(bytes: &[u8]) -> Result<RolloutLine> {
    std::str::from_utf8(bytes).map_err(|_| unavailable())?;
    let value: Value = serde_json::from_slice(bytes).map_err(|_| unavailable())?;
    let object = value.as_object().ok_or_else(integrity)?;
    let timestamp = object
        .get("timestamp")
        .and_then(Value::as_str)
        .ok_or_else(integrity)?;
    validate_text(timestamp, 128)?;
    let ordinal = match object.get("ordinal") {
        None | Some(Value::Null) => None,
        Some(value) => Some(value.as_u64().ok_or_else(integrity)?),
    };
    let kind = object
        .get("type")
        .and_then(Value::as_str)
        .ok_or_else(integrity)?;
    if !known_rollout_kind(kind) {
        return Err(Error::new(
            "unsupported",
            "Codex rollout line type is outside the pinned 0.161 projection",
        ));
    }
    let payload = object.get("payload").cloned().ok_or_else(integrity)?;
    let metadata = object.get("metadata").cloned();
    Ok(RolloutLine {
        timestamp: timestamp.to_owned(),
        ordinal,
        kind: kind.to_owned(),
        payload,
        metadata,
    })
}

fn validate_header(line: &RolloutLine, native_ref: &str, cli_version: &str) -> Result<Value> {
    if line.kind != "session_meta" {
        return Err(integrity());
    }
    let payload = line.payload.as_object().ok_or_else(integrity)?;
    let id = payload
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(integrity)?;
    let version = payload
        .get("cli_version")
        .and_then(Value::as_str)
        .ok_or_else(integrity)?;
    validate_text(id, 256)?;
    validate_text(version, 256)?;
    if id != native_ref || version != cli_version {
        return Err(integrity());
    }
    let cwd = payload
        .get("cwd")
        .and_then(Value::as_str)
        .filter(|cwd| !cwd.is_empty());
    if let Some(cwd) = cwd {
        validate_text(cwd, MAX_PATH_BYTES)?;
    }
    Ok(json!({
        "native_ref": id,
        "cli_version": version,
        "created_at": line.timestamp,
        "workspace": cwd,
    }))
}

fn read_complete_line(
    file: &mut File,
    offset: u64,
    max_bytes: usize,
    deadline: Instant,
) -> Result<Option<(Vec<u8>, u64)>> {
    file.seek(SeekFrom::Start(offset)).map_err(map_io_error)?;
    let mut line = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];
    loop {
        if Instant::now() >= deadline {
            return Err(read_deadline());
        }
        let remaining = max_bytes.saturating_sub(line.len()).saturating_add(1);
        let read_len = remaining.min(chunk.len());
        let count = file.read(&mut chunk[..read_len]).map_err(map_io_error)?;
        if count == 0 {
            return Ok(None);
        }
        if let Some(newline) = chunk[..count].iter().position(|byte| *byte == b'\n') {
            if line.len().saturating_add(newline) > max_bytes {
                return Err(too_large());
            }
            line.extend_from_slice(&chunk[..newline]);
            let next = offset
                .checked_add(line.len() as u64)
                .and_then(|value| value.checked_add(1))
                .ok_or_else(too_large)?;
            return Ok(Some((line, next)));
        }
        line.extend_from_slice(&chunk[..count]);
        if line.len() > max_bytes {
            return Err(too_large());
        }
    }
}

fn read_range(file: &mut File, offset: u64, length: usize, deadline: Instant) -> Result<Vec<u8>> {
    file.seek(SeekFrom::Start(offset)).map_err(map_io_error)?;
    let mut bytes = vec![0u8; length];
    let mut filled = 0usize;
    while filled < length {
        if Instant::now() >= deadline {
            return Err(read_deadline());
        }
        let read = file.read(&mut bytes[filled..]).map_err(map_io_error)?;
        if read == 0 {
            return Err(unavailable());
        }
        filled += read;
    }
    Ok(bytes)
}

fn anchor_at(file: &mut File, offset: u64, deadline: Instant) -> Result<String> {
    // This bounded tail digest catches common rewrite/regrow cases; it does not
    // prove that every same-identity in-place overwrite is detectable.
    let start = offset.saturating_sub(ANCHOR_BYTES as u64);
    let bytes = read_range(file, start, (offset - start) as usize, deadline)?;
    Ok(hex_digest(&bytes))
}

fn verify_final_path(final_path: &Path, codex_home: &Path) -> Result<()> {
    let home = fs::canonicalize(codex_home).map_err(map_io_error)?;
    if !home.is_dir() {
        return Err(unavailable());
    }
    for subdir in ["sessions", "archived_sessions"] {
        let root = match fs::canonicalize(home.join(subdir)) {
            Ok(root) => root,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(map_io_error(error)),
        };
        if !root.is_dir() || !path_within(&root, &home) {
            continue;
        }
        if path_within(final_path, &root) {
            return Ok(());
        }
    }
    Err(unavailable())
}

fn path_within(path: &Path, root: &Path) -> bool {
    #[cfg(windows)]
    {
        fn normalized(path: &Path) -> String {
            let value = path.to_string_lossy().replace('/', "\\");
            let value = if let Some(tail) = value.strip_prefix("\\\\?\\UNC\\") {
                format!("\\\\{tail}")
            } else if let Some(tail) = value.strip_prefix("\\\\?\\") {
                tail.to_owned()
            } else {
                value
            };
            value.to_lowercase()
        }
        let path = normalized(path);
        let root = normalized(root);
        let root = root.trim_end_matches('\\');
        let prefix = format!("{root}\\");
        path.as_str() == root || path.starts_with(prefix.as_str())
    }
    #[cfg(not(windows))]
    {
        path != root && path.starts_with(root)
    }
}

fn known_rollout_kind(kind: &str) -> bool {
    matches!(
        kind,
        "session_meta"
            | "response_item"
            | "inter_agent_communication"
            | "inter_agent_communication_metadata"
            | "compacted"
            | "turn_context"
            | "token_usage_record"
            | "world_state"
            | "security_risk_score"
            | "retained_context"
            | "event_msg"
            | "realtime_item"
    )
}

fn optional_i64(object: &Value, key: &str) -> Result<Option<i64>> {
    match object.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(integrity),
    }
}

fn validate_text(value: &str, max_bytes: usize) -> Result<()> {
    if value.trim().is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(integrity());
    }
    Ok(())
}

fn reject_cursor_secrets(cursor: &RolloutCursor, secrets: &[String]) -> Result<()> {
    for value in [
        Some(cursor.binding.as_str()),
        cursor.current_turn_id.as_deref(),
        cursor.last_completed_turn_id.as_deref(),
        cursor
            .active_turn
            .as_ref()
            .map(|turn| turn.turn_id.as_str()),
    ]
    .into_iter()
    .flatten()
    {
        reject_secret_text(value, secrets)?;
    }
    Ok(())
}

fn reject_secret_text(value: &str, secrets: &[String]) -> Result<()> {
    if SecretPolicy::new(secrets.iter().cloned()).contains_text(value) {
        return Err(Error::new(
            "secret_in_payload",
            "Codex rollout cursor contains a known secret",
        ));
    }
    Ok(())
}

fn encoded_len(value: &Value) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| integrity())
}

fn hex_digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn map_io_error(error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::Unsupported {
        Error::new(
            "unsupported",
            "Codex rollout file identity or final-path checks are unsupported on this platform",
        )
    } else {
        unavailable()
    }
}

fn invalid() -> Error {
    Error::new("invalid", "Codex rollout observation request is invalid")
}
fn integrity() -> Error {
    Error::new(
        "source_integrity",
        "Codex rollout metadata does not match the exact subject or pinned format",
    )
}
fn unavailable() -> Error {
    Error::new(
        "history_gap",
        "Codex rollout file is unavailable, replaced, truncated, or outside the selected home",
    )
}
fn cursor_mismatch() -> Error {
    Error::new(
        "cursor_mismatch",
        "Codex rollout cursor does not match its exact subject, file, or query",
    )
}
fn read_deadline() -> Error {
    Error::new(
        "history_gap",
        "Codex rollout read exceeded its bounded deadline",
    )
}
fn too_large() -> Error {
    Error::new(
        "frame_too_large",
        "Codex rollout line, cursor, or page exceeds its configured bound",
    )
}

#[cfg(test)]
mod live_tests {
    use super::*;
    use std::{
        io::Write,
        sync::atomic::{AtomicU64, Ordering},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        home: PathBuf,
        path: PathBuf,
    }

    impl Fixture {
        fn new(history: &[Value]) -> Self {
            let home = std::env::temp_dir().join(format!(
                "eliot-live-rollout-{}-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir(&home).unwrap();
            fs::create_dir(home.join("sessions")).unwrap();
            let path = home.join("sessions/rollout.jsonl");
            let fixture = Self { home, path };
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&fixture.path)
                .unwrap();
            writeln!(
                file,
                "{}",
                line(
                    "session_meta",
                    json!({
                        "id":"fixture-thread", "cli_version":"0.161.0", "cwd":"fixture-workspace"
                    })
                )
            )
            .unwrap();
            for record in history {
                writeln!(file, "{record}").unwrap();
            }
            file.flush().unwrap();
            fixture
        }

        fn append(&self, record: Value) {
            let mut file = OpenOptions::new().append(true).open(&self.path).unwrap();
            writeln!(file, "{record}").unwrap();
            file.flush().unwrap();
        }

        fn reader(&self, policy: SecretPolicy) -> LiveRollout {
            LiveRollout::open(
                self.path.clone(),
                self.home.clone(),
                "fixture-thread".to_owned(),
                "0.161.0".to_owned(),
                "fixture-binding".to_owned(),
                Arc::new(policy),
                deadline(),
            )
            .unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            // Only the unique directory created by this fixture is owned here.
            let _ = fs::remove_dir_all(&self.home);
        }
    }

    fn deadline() -> Instant {
        Instant::now() + Duration::from_secs(5)
    }

    fn line(kind: &str, payload: Value) -> Value {
        json!({"timestamp":"1970-01-01T01:00:00.123+01:00", "type":kind, "payload":payload})
    }

    fn context(turn: &str) -> Value {
        line("turn_context", json!({"turn_id":turn}))
    }

    fn message(text: &str, phase: Option<&str>) -> Value {
        let mut item = json!({"type":"message", "role":"assistant", "content":[{"type":"output_text","text":text}]});
        if let Some(phase) = phase {
            item["phase"] = json!(phase);
        }
        line("response_item", item)
    }

    #[tokio::test]
    async fn baseline_excludes_history_and_partial_lf_waits_without_a_gap() {
        let fixture = Fixture::new(&[context("old"), message("historical", Some("final_answer"))]);
        let mut reader = fixture.reader(SecretPolicy::default());
        let baseline = reader.baseline_cursor().to_owned();
        let empty = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        assert!(empty.records.is_empty() && empty.at_end && empty.gap.is_none());
        assert_eq!(empty.cursor, baseline);
        fixture.append(context("new"));
        let raw = serde_json::to_vec(&message("live text", None)).unwrap();
        let split = raw.len() / 2;
        let mut writer = OpenOptions::new().append(true).open(&fixture.path).unwrap();
        writer.write_all(&raw[..split]).unwrap();
        writer.flush().unwrap();
        let partial = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        assert!(partial.records.is_empty() && partial.partial_tail && partial.gap.is_none());
        let again = reader
            .read_page(&partial.cursor, 1, deadline())
            .await
            .unwrap();
        assert_eq!(again.cursor, partial.cursor);
        assert!(again.partial_tail && again.gap.is_none());
        writer.write_all(&raw[split..]).unwrap();
        writer.write_all(b"\n").unwrap();
        writer.flush().unwrap();
        let page = reader
            .read_page(&partial.cursor, 1, deadline())
            .await
            .unwrap();
        assert_eq!(page.records.len(), 1);
        let event = &page.records[0].events[0];
        assert_eq!(event.kind, "message.completed");
        assert_eq!(event.ts, Some(123));
        assert_eq!(event.data["text"], "live text");
        assert!(event.data.get("phase").is_none());
        assert_eq!(page.cursor, page.records[0].cursor);
        assert!(page.at_end && !page.partial_tail && page.gap.is_none());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn canonical_lifecycle_phase_tools_and_original_timing_are_preserved() {
        let fixture = Fixture::new(&[]);
        let mut reader = fixture.reader(SecretPolicy::default());
        let baseline = reader.baseline_cursor().to_owned();
        fixture.append(line(
            "event_msg",
            json!({"type":"task_started","turn_id":"turn-1","started_at":7}),
        ));
        fixture.append(message("commentary", Some("commentary")));
        fixture.append(line(
            "event_msg",
            json!({"type":"agent_message","message":"commentary"}),
        ));
        fixture.append(line("response_item", json!({"type":"function_call","name":"exec_command","call_id":"call-1","id":"call-item","arguments":"{}"})));
        fixture.append(line("response_item", json!({"type":"function_call_output","call_id":"call-1","id":"output-item","output":"exit status unknown"})));
        fixture.append(line(
            "event_msg",
            json!({"type":"exec_command_end","call_id":"call-1"}),
        ));
        fixture.append(line("response_item", json!({"type":"custom_tool_call","name":"apply_patch","call_id":"call-2","input":"fixture patch"})));
        fixture.append(line("response_item", json!({"type":"custom_tool_call_output","call_id":"call-2","output":[{"type":"input_text","text":"native result"}]})));
        fixture.append(message("final message", Some("final_answer")));
        let page = reader.read_page(&baseline, 100, deadline()).await.unwrap();
        let events: Vec<_> = page
            .records
            .iter()
            .flat_map(|record| &record.events)
            .collect();
        assert_eq!(events.len(), 7);
        assert!(
            events
                .iter()
                .all(|event| event.ts == Some(123) && event.data.get("command_id").is_none())
        );
        assert!(events.iter().all(|event| event.kind != "turn.completed"));
        assert_eq!(events[0].data["started_at"], 7000);
        assert_eq!(events[1].data["phase"], "commentary");
        assert_eq!(events[2].data["item_id"], events[3].data["item_id"]);
        assert_eq!(events[3].data["tool"], "exec_command");
        assert!(events[3].data.get("status").is_none());
        assert_eq!(events[4].data["item_id"], events[5].data["item_id"]);
        assert!(events[5].data.get("changed_paths").is_none());
        assert!(page.at_end && page.gap.is_none());
        fixture.append(line("event_msg", json!({"type":"turn_complete","turn_id":"turn-1", "error":{"message":"native failure"},
            "started_at":7,"completed_at":9,"duration_ms":2000,"last_agent_message":"terminal answer"})));
        let terminal = reader.read_page(&page.cursor, 1, deadline()).await.unwrap();
        let event = &terminal.records[0].events[0];
        assert_eq!(event.kind, "turn.completed");
        assert_eq!(event.data["status"], "failed");
        assert_eq!(event.data["started_at"], 7000);
        assert_eq!(event.data["completed_at"], 9000);
        assert_eq!(event.data["duration_ms"], 2000);
        assert_eq!(event.data["final_text"], "terminal answer");
        fixture.append(line(
            "event_msg",
            json!({"type":"turn_aborted","turn_id":"turn-2","reason":"interrupted"}),
        ));
        let abort = reader
            .read_page(&terminal.cursor, 1, deadline())
            .await
            .unwrap();
        assert_eq!(abort.records[0].events[0].data["status"], "failed");
        assert_eq!(
            abort.records[0].events[0].data["abort_reason"],
            "interrupted"
        );
        assert!(abort.records[0].events[0].data.get("started_at").is_none());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn unbound_and_malformed_records_have_only_gap_checkpoints() {
        let fixture = Fixture::new(&[context("historical-turn")]);
        let mut reader = fixture.reader(SecretPolicy::default());
        let baseline = reader.baseline_cursor().to_owned();
        fixture.append(message("no proven live turn", None));
        let gap = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        assert!(gap.records.is_empty() && gap.gap.is_some());
        assert_ne!(gap.cursor, baseline);
        fixture.append(line(
            "event_msg",
            json!({"type":"task_complete","turn_id":"historical-turn"}),
        ));
        let terminal = reader.read_page(&gap.cursor, 1, deadline()).await.unwrap();
        assert_eq!(terminal.records[0].events[0].kind, "turn.completed");
        assert!(
            terminal.records[0].events[0]
                .data
                .get("started_at")
                .is_none()
        );
        fixture.append(line(
            "event_msg",
            json!({"type":"turn_aborted","reason":"interrupted"}),
        ));
        let missing_id = reader
            .read_page(&terminal.cursor, 1, deadline())
            .await
            .unwrap();
        assert!(missing_id.gap.is_some() && missing_id.records.is_empty());
        let mut invalid_time = line(
            "event_msg",
            json!({"type":"task_started","turn_id":"bad-time"}),
        );
        invalid_time["timestamp"] = json!("2026-02-30T00:00:00Z");
        fixture.append(invalid_time);
        let invalid = reader
            .read_page(&missing_id.cursor, 1, deadline())
            .await
            .unwrap();
        assert!(invalid.gap.is_some() && invalid.records.is_empty());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn redaction_precedes_event_ids_and_encoded_bounds() {
        let fixture = Fixture::new(&[]);
        let mut reader = fixture.reader(SecretPolicy::new(["fixture-private"]));
        let baseline = reader.baseline_cursor().to_owned();
        fixture.append(context("turn-1"));
        fixture.append(message("body fixture-private", None));
        let page = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        assert_eq!(page.records[0].events[0].data["text"], "body [REDACTED]");
        let repeat = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        assert_eq!(repeat.records[0].events[0], page.records[0].events[0]);
        fixture.append(context("turn-fixture-private"));
        let protected = reader.read_page(&page.cursor, 1, deadline()).await.unwrap();
        assert!(protected.records.is_empty() && protected.gap.is_some());
        assert!(!protected.cursor.contains("fixture-private"));
        reader.close().await.unwrap();

        let fixture = Fixture::new(&[]);
        let mut reader = fixture.reader(SecretPolicy::new(["q"]));
        let baseline = reader.baseline_cursor().to_owned();
        fixture.append(context("turn-1"));
        fixture.append(message(&"q".repeat(10_000), None));
        let oversized = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        assert!(oversized.gap.is_some() && oversized.records.is_empty());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn held_file_rejects_rewrite_truncation_and_path_replacement() {
        let fixture = Fixture::new(&[]);
        let mut reader = fixture.reader(SecretPolicy::default());
        let baseline = reader.baseline_cursor().to_owned();
        fixture.append(context("turn-1"));
        fixture.append(message("live", None));
        let page = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        let cursor: LiveCursor =
            serde_json::from_str(page.cursor.strip_prefix(LIVE_CURSOR_PREFIX).unwrap()).unwrap();
        let mut writer = OpenOptions::new().write(true).open(&fixture.path).unwrap();
        writer.seek(SeekFrom::Start(cursor.offset - 2)).unwrap();
        writer.write_all(b"]").unwrap();
        writer.flush().unwrap();
        let rewritten = reader.read_page(&page.cursor, 1, deadline()).await.unwrap();
        assert!(rewritten.gap.is_some() && rewritten.records.is_empty());
        writer.set_len(0).unwrap();
        let truncated = reader.read_page(&page.cursor, 1, deadline()).await.unwrap();
        assert!(truncated.gap.is_some() && truncated.records.is_empty());
        reader.close().await.unwrap();

        let fixture = Fixture::new(&[]);
        let mut reader = fixture.reader(SecretPolicy::default());
        let baseline = reader.baseline_cursor().to_owned();
        fs::rename(&fixture.path, fixture.home.join("sessions/old.jsonl")).unwrap();
        fs::write(&fixture.path, b"foreign replacement\n").unwrap();
        let replaced = reader.read_page(&baseline, 1, deadline()).await.unwrap();
        assert!(replaced.gap.is_some() && replaced.records.is_empty());
        reader.close().await.unwrap();
    }

    #[tokio::test]
    async fn cursor_subject_and_version_are_exactly_bound() {
        let fixture = Fixture::new(&[]);
        let mut reader = fixture.reader(SecretPolicy::default());
        let baseline = reader.baseline_cursor().to_owned();
        let mut other = LiveRollout::open(
            fixture.path.clone(),
            fixture.home.clone(),
            "fixture-thread".to_owned(),
            "0.161.0".to_owned(),
            "other-binding".to_owned(),
            Arc::new(SecretPolicy::default()),
            deadline(),
        )
        .unwrap();
        assert_eq!(
            other
                .read_page(&baseline, 1, deadline())
                .await
                .err()
                .unwrap()
                .code,
            "cursor_mismatch"
        );
        for (native_ref, version, code) in [
            ("foreign-thread", "0.161.0", "source_integrity"),
            ("fixture-thread", "0.164.0-alpha.11.1", "unsupported"),
        ] {
            assert_eq!(
                LiveRollout::open(
                    fixture.path.clone(),
                    fixture.home.clone(),
                    native_ref.to_owned(),
                    version.to_owned(),
                    "fixture-binding".to_owned(),
                    Arc::new(SecretPolicy::default()),
                    deadline()
                )
                .err()
                .unwrap()
                .code,
                code
            );
        }
        other.close().await.unwrap();
        reader.close().await.unwrap();
    }

    #[test]
    fn cancelled_read_is_retained_and_close_joins_before_dropping_the_file() {
        let fixture = Fixture::new(&[]);
        let mut reader = fixture.reader(SecretPolicy::default());
        let baseline = reader.baseline_cursor().to_owned();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (release, released) = mpsc::sync_channel(1);
            let (started, waiting) = mpsc::sync_channel(1);
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                released.recv().unwrap();
            });
            waiting.recv().unwrap();
            {
                let mut read = Box::pin(reader.read_page(&baseline, 1, deadline()));
                assert!(futures_util::poll!(read.as_mut()).is_pending());
            }
            assert!(reader.pending.is_some());
            release.send(()).unwrap();
            reader.close().await.unwrap();
            blocker.await.unwrap();
            assert!(reader.pending.is_none() && reader.file.is_none());
        });
    }
}
