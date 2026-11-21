//! Shared Eliot protocol contracts. Vendor, scan and local retention budgets
//! remain with their owners even when their numeric values happen to match.

/// Private host/plugin JSON-RPC frame, including its NDJSON newline.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Maximum inbound frame or response body from a native harness.
pub const MAX_NATIVE_INBOUND_FRAME_BYTES: usize = 8 * 1024 * 1024;
/// Inline command input before JSON escaping; the encoded frame is also bounded.
pub const MAX_TEXT_BYTES: usize = 512 * 1024;
/// Persisted event data and final text.
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
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
