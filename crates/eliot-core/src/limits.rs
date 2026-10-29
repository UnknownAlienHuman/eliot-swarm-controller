//! Shared Eliot protocol contracts. Vendor, scan and local retention budgets
//! remain with their owners even when their numeric values happen to match.

/// Private host/plugin JSON-RPC frame, including its NDJSON newline.
pub const MAX_FRAME_BYTES: usize = 1024 * 1024;
/// Inline command input before JSON escaping; the encoded frame is also bounded.
pub const MAX_TEXT_BYTES: usize = 512 * 1024;
/// Persisted event data and final text.
pub const MAX_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_CREDENTIAL_BYTES: usize = 4 * 1024;
pub const MAX_COMMAND_ID_BYTES: usize = 256;

pub const MAX_DISCOVER_PAGE_ENTRIES: usize = 1_000;
pub const MAX_DISCOVER_PAGE_BYTES: usize = 900 * 1024;
pub const MAX_OBSERVATION_PAGE_ENTRIES: usize = 1_000;
pub const MAX_OBSERVATION_PAGE_BYTES: usize = 900 * 1024;
pub const MAX_OBSERVATION_CURSOR_BYTES: usize = 16 * 1024;
