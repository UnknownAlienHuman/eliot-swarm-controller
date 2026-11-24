//! Shared Eliot protocol contracts. Vendor, scan and local retention budgets
//! remain with their owners even when their numeric values happen to match.

/// Private host/plugin JSON-RPC frame, including its NDJSON newline.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Space for a terminal reply and its metadata within a private frame.
/// Remaining space is available for the source or snapshot envelope.
pub const MAX_TERMINAL_WIRE_BYTES: usize = MAX_FRAME_BYTES / 2;
/// Maximum inbound frame or response body from a native harness.
pub const MAX_NATIVE_INBOUND_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Inline command input before JSON escaping; the encoded frame is also bounded.
pub const MAX_TEXT_BYTES: usize = 512 * 1024;
/// Persisted event data and final text.
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_UNCLASSIFIED_NATIVE_FACTS_PER_SESSION: i64 = 64;

/// Maximum silence on an associated native turn before its outcome is unknown.
pub const NATIVE_TERMINAL_DEADLINE: std::time::Duration = std::time::Duration::from_secs(30 * 60);
pub const NATIVE_TERMINAL_DEADLINE_REASON: &str = "native_terminal_deadline";
/// An unbound turn left the active projection with no terminal. Its exact
/// native terminal can still settle the retained unknown turn.
pub const NATIVE_TERMINAL_UNCONFIRMED_REASON: &str = "native_terminal_unconfirmed";
pub const DEBUG_NATIVE_INACTIVITY_ENV: &str = "SWARM_QA_NATIVE_INACTIVITY_MS";

/// Keep the end of a terminal reply within the encoded event budget.
/// Callers sanitize the text before applying the bound. Required metadata
/// stays intact, so the caller can still reject an over-budget base event.
pub fn bound_terminal_text(data: &mut serde_json::Value, budget: usize) {
    const MARKER: &str = "[... truncated ...]\n";

    if serde_json::to_vec(data).is_ok_and(|encoded| encoded.len() <= budget) {
        return;
    }
    let Some(serde_json::Value::String(text)) = data.get_mut("final_text") else {
        return;
    };
    let text = std::mem::take(text);
    let encoded_base = serde_json::to_vec(data).map_or(usize::MAX, |encoded| encoded.len());
    let marker_bytes = MARKER.len() + 1; // The newline escapes to two bytes.
    let Some(mut remaining) = budget.checked_sub(encoded_base.saturating_add(marker_bytes)) else {
        data["final_text"] = text.into();
        return;
    };
    let mut start = text.len();
    for (offset, ch) in text.char_indices().rev() {
        let encoded_bytes = match ch {
            '\u{08}' | '\t' | '\n' | '\u{0C}' | '\r' | '"' | '\\' => 2,
            '\0'..='\u{1f}' => 6,
            _ => ch.len_utf8(),
        };
        let Some(rest) = remaining.checked_sub(encoded_bytes) else {
            break;
        };
        remaining = rest;
        start = offset;
    }
    data["final_text"] = format!("{MARKER}{}", &text[start..]).into();
}

pub const MAX_CREDENTIAL_BYTES: usize = 4 * 1024;
pub const MAX_COMMAND_ID_BYTES: usize = 256;
/// A tool's command text on its `tool.started` fact (`command`), after
/// secret redaction: enough for the line a transcript shows.
pub const MAX_TOOL_COMMAND_BYTES: usize = 1024;
/// The tail of a failed tool's native output (`error_tail`), after secret
/// redaction: the last lines that usually carry the reason. Failure-only, so
/// a successful event's size is unchanged apart from its exit code.
pub const MAX_TOOL_ERROR_TAIL_BYTES: usize = 1024;

pub const MAX_DISCOVER_PAGE_ENTRIES: usize = 1_000;
pub const MAX_DISCOVER_PAGE_BYTES: usize = 900 * 1024;
pub const MAX_OBSERVATION_PAGE_ENTRIES: usize = 1_000;
pub const MAX_OBSERVATION_PAGE_BYTES: usize = 900 * 1024;
pub const MAX_OBSERVATION_CURSOR_BYTES: usize = 16 * 1024;
