//! External modules share this command boundary, not vendor request schemas.
pub mod opencode_v2;
pub mod owner;
pub(crate) mod prerequisites;
pub mod zed;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeCommand {
    pub operation_id: String,
    pub method: String,
    pub created_at_ms: i64,
    pub binding_id: String,
    pub generation: i64,
    pub native_root_id: Option<String>,
    pub route: Value,
    pub input: Value,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectOutcome {
    /// Native admission is known, but the required application boundary is pending.
    Accepted,
    Applied,
    Rejected,
    Unknown,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOutcome {
    pub operation_id: String,
    pub outcome: EffectOutcome,
    #[serde(default)]
    pub native_scope_key: Option<String>,
    #[serde(default)]
    pub native_root_id: Option<String>,
    #[serde(default)]
    pub turn_id: Option<String>,
    /// Durable native input admission is not a native turn or its completion.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_input_id: Option<String>,
    pub details: Value,
}
