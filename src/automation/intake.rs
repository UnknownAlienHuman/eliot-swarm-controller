//! Public-to-the-crate contracts for bounded durable automation intake.
//!
//! Only Store-backed producers named here are admissible. External producers
//! become admissible only through an authenticated, bounded adapter.

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const MAX_INTAKE_PAGE: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalProducer {
    TaskSubmission,
    /// Facts accepted from a setup-issued, repository-scoped HookSource.
    HookCommit,
}

impl LocalProducer {
    pub(crate) const fn source_id(self) -> &'static str {
        match self {
            Self::TaskSubmission => "local:controller-task-submissions-v1",
            Self::HookCommit => "local:controller-hook-commits-v1",
        }
    }

    pub(crate) const fn stream_id(self) -> &'static str {
        match self {
            Self::TaskSubmission => "controller",
            Self::HookCommit => "controller:hooks",
        }
    }

    pub(crate) const fn event_kind(self) -> &'static str {
        match self {
            Self::TaskSubmission => "task.submission",
            Self::HookCommit => "git.post_commit",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SourceRegistration {
    pub(crate) schema_version: u32,
    pub(crate) source_id: String,
    pub(crate) producer: LocalProducer,
    pub(crate) stream_id: String,
    pub(crate) event_kind: String,
    /// The global observation cut selected when this source was first
    /// registered. With `include_existing=false`, the cursor begins here.
    pub(crate) initial_cursor: i64,
    pub(crate) include_existing: bool,
    pub(crate) registered_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IntakeCursor {
    pub(crate) schema_version: u32,
    pub(crate) source_id: String,
    pub(crate) observation_id: i64,
    pub(crate) updated_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventReceipt {
    pub(crate) schema_version: u32,
    pub(crate) source_id: String,
    pub(crate) observation_id: i64,
    pub(crate) source_event_key: String,
    /// Digest of the immutable producer identity and event content. The
    /// local observation ID and recording timestamp are intentionally absent.
    pub(crate) event_digest: String,
    pub(crate) event_kind: String,
    pub(crate) operation_id: Option<String>,
    pub(crate) binding_id: Option<String>,
    pub(crate) binding_generation: Option<i64>,
    pub(crate) payload: Value,
    pub(crate) recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EventGap {
    pub(crate) source_id: String,
    pub(crate) observation_id: i64,
    pub(crate) source_event_key: Option<String>,
    pub(crate) event_digest: Option<String>,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub(crate) enum IntakeItem {
    Receipt(EventReceipt),
    Gap(EventGap),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum IntakeStatus {
    UnknownSource,
    StaleCursor,
    CaughtUp,
    Advanced,
    Gap,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReconcilePage {
    pub(crate) source_id: String,
    pub(crate) status: IntakeStatus,
    pub(crate) cursor: Option<i64>,
    pub(crate) high_water: Option<i64>,
    pub(crate) processed: usize,
    /// The exact durable subjects written by this page. This is a projection
    /// for the caller; readback remains available after a commit/restart.
    pub(crate) items: Vec<IntakeItem>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct PendingPage {
    pub(crate) source_id: String,
    pub(crate) status: IntakeStatus,
    pub(crate) cursor: Option<i64>,
    pub(crate) next_after_observation_id: Option<i64>,
    pub(crate) items: Vec<IntakeItem>,
}
