//! Pure context-injection values shared by Store and command dispatch.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// An injection request or its resolved application point. Only concrete
/// points can be used in a native claim or receipt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InjectPoint {
    Soonest,
    NextInput,
    TurnSafePoint,
    ActiveTurn,
}

/// Semantic arguments for the existing `inject` command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InjectArgs {
    pub text: String,
    #[serde(alias = "at")]
    pub point: InjectPoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
}

#[derive(schemars::JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ContextWaitRequest {
    pub id: String,
    #[serde(default)]
    pub wait_ms: u64,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextNoteCounters {
    pub kind: String,
    pub delivered: u64,
    pub moved: u64,
    pub cancelled: u64,
    pub resolved: u64,
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

/// The exact `turn.started` receipt envelope the native adapters persist:
/// command and native turn identity plus the model/effort the native turn
/// request actually carried (omitted when unknown: nothing is invented),
/// plus one outcome per claimed `next_input` item (omitted when empty).
/// The Store budgets claims against this same constructor, so the
/// claim-time bound and every adapter receipt share one serialized shape.
pub fn turn_started_data(
    command_id: &str,
    turn_id: &str,
    model: Option<&str>,
    effort: Option<&str>,
    outcomes: &[ContextOutcome],
) -> Value {
    let mut data = json!({"command_id": command_id, "turn_id": turn_id});
    if let Some(model) = model.filter(|model| !model.trim().is_empty()) {
        data["model"] = json!(model);
    }
    if let Some(effort) = effort.filter(|effort| !effort.trim().is_empty()) {
        data["effort"] = json!(effort);
    }
    if !outcomes.is_empty() {
        data["context_outcomes"] = json!(outcomes);
    }
    data
}
