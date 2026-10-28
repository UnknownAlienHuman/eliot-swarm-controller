//! Bounded metadata projection of the exact Codex JSONL path returned by thread/read.
//! This is deliberately not a session executor or a general rollout discovery API.

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    FileIdentity, MAX_OBSERVATION_CURSOR_BYTES, MAX_OBSERVATION_PAGE_BYTES,
    MAX_OBSERVATION_PAGE_ENTRIES, ObservationQuery, file_final_path, file_identity,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::Instant,
};

const CURSOR_PREFIX: &str = "codex-rollout-v1:";
const MAX_LINE_BYTES: usize = 256 * 1024;
const MAX_HEADER_BYTES: usize = 256 * 1024;
const MAX_SCAN_BYTES: usize = 1024 * 1024;
const ANCHOR_BYTES: usize = 4096;
const MAX_PATH_BYTES: usize = 4096;

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
