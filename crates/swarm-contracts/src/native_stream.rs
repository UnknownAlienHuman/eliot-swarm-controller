//! Ephemeral native content transport. These facts grant no Task or content rights.
use serde::{Deserialize, Serialize};

pub const PUBLISH_METHOD: &str = "module.stream.publish";
pub const MAX_FRAME_BYTES: usize = 32 * 1024;
pub const MAX_RING_RECORDS: usize = 256;
pub const MAX_RING_BYTES: usize = 256 * 1024;
pub const MAX_RINGS: usize = 32;
pub const MAX_READ_RECORDS: usize = 64;
pub const MAX_READ_BYTES: usize = 64 * 1024;
pub const MAX_VIEWERS: usize = 256;
pub const STALE_AFTER_MS: i64 = 120_000;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(deny_unknown_fields)]
pub struct StreamScope {
    pub binding_id: String,
    pub generation: i64,
    pub boot_id: String,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub operation_id: String,
    pub native_session_id: String,
    pub turn_id: String,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TextClass {
    Assistant,
    ReasoningSummary,
    ReasoningText,
    ToolProgress,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum StreamPayload {
    Text {
        class: TextClass,
        text: String,
        /// Full authoritative native record, replacing all matching deltas.
        final_record: bool,
        /// item/delta has no revision; full native items do.
        revision: Option<u64>,
    },
    /// Session cumulative counters, replaced rather than summed. No quota,
    /// inferred per-turn accounting, provider pricing or cost is represented.
    CumulativeUsage {
        prompt_tokens: u64,
        output_tokens: u64,
        total_tokens: u64,
    },
    Lifecycle { state: StreamLifecycle },
    Unavailable { reason: StreamUnavailable },
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamLifecycle {
    Started,
    InProgress,
    Completed,
    Failed,
    Cancelled,
    TerminalUnknown,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StreamUnavailable {
    HiddenOrEncryptedReasoning,
    LogicalRecordPending,
    NativeTruncation,
    NativeContentUnavailable,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StreamPublish {
    pub scope: StreamScope,
    /// Adapter-local ordered delivery ordinal per StreamScope, never a parsed
    /// native cursor.
    pub source_sequence: u64,
    pub native_cursor: String,
    pub item_id: String,
    pub item_order: u32,
    pub part_index: u32,
    pub payload: StreamPayload,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamOpen {
    pub operation_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamRead {
    pub stream_id: String,
    pub after: u64,
    pub limit: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamClose {
    pub stream_id: String,
}
