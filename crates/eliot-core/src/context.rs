//! Pure context-injection values shared by Store and command dispatch.

use serde::{Deserialize, Serialize};

/// The dispatch boundary at which an injected context item may be applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectPoint {
    NextInput,
    TurnSafePoint,
}

/// Semantic arguments for the existing `inject` command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectArgs {
    pub text: String,
    pub point: InjectPoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
}

/// One durable item claimed by a context consumer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextItem {
    pub command_id: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
}

/// Evidence that a claimed context item reached its native application point.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextOutcome {
    pub mechanism: String,
    pub application_point: InjectPoint,
    pub consumer_id: String,
}
