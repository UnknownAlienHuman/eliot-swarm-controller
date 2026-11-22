//! Pure context-injection values shared by Store and command dispatch.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// `session.ready` effective field naming where a session takes `steer`.
/// Absent means a direct native steer; `turn_safe_point` means Store holds the
/// steer for the next tool hook of its exact turn.
pub const STEER_DELIVERY_FIELD: &str = "steer_delivery";

impl crate::Agent {
    /// Whether this session's plugin declared safe-point steer delivery.
    pub fn steers_at_turn_safe_point(&self) -> bool {
        self.effective
            .as_ref()
            .and_then(|effective| effective.get(STEER_DELIVERY_FIELD))
            .and_then(|value| InjectPoint::deserialize(value).ok())
            == Some(InjectPoint::TurnSafePoint)
    }
}

/// Persisted file edit evidence never includes an absolute path.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ContextEditPath {
    Relative { path: String },
    OutsideOrUnverifiable { sha256: String, basename: String },
}

impl ContextEditPath {
    pub fn outside(path: &str, policy: &crate::SecretPolicy) -> Self {
        let basename = path
            .rsplit(['/', '\\'])
            .find(|part| !part.is_empty())
            .filter(|name| !name.chars().any(char::is_control))
            .unwrap_or("unavailable");
        let basename = policy.redact_text(basename);
        Self::OutsideOrUnverifiable {
            sha256: crate::api::inference::sha256_hex(path.as_bytes()),
            basename: crate::api::truncate_bytes(&basename, 240).0.to_owned(),
        }
    }
}

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

/// Daemon-verified worktree facts attached to an automatic review result.
/// This is retained with the accepted context command and is not part of the
/// `inject` wire arguments.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TrustedReviewLabels {
    pub worktree_fingerprint: String,
    pub worktree_state: ReviewWorktreeState,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewWorktreeState {
    CurrentAtDeliveryCheck,
    ChangedOrUnconfirmedAtDeliveryCheck,
}

/// Retained `inject` arguments may carry daemon-only review labels. Submitted
/// requests still decode through `InjectArgs`, which rejects this field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetainedInjectArgs {
    pub text: String,
    #[serde(alias = "at")]
    pub point: InjectPoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expires_at_ms: Option<i64>,
    #[serde(
        default,
        rename = "_trusted_review_labels",
        skip_serializing_if = "Option::is_none"
    )]
    pub trusted_review_labels: Option<TrustedReviewLabels>,
}

pub fn decode_retained_inject_args(value: &Value) -> serde_json::Result<RetainedInjectArgs> {
    let args: RetainedInjectArgs = serde_json::from_value(value.clone())?;
    if let Some(labels) = &args.trusted_review_labels
        && (labels.worktree_fingerprint.len() != 64
            || !labels
                .worktree_fingerprint
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit()))
    {
        return Err(serde::de::Error::custom(
            "trusted worktree fingerprint must be SHA-256 hex",
        ));
    }
    Ok(args)
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
    #[serde(default)]
    pub expired: u64,
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
