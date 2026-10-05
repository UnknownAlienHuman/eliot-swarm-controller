use crate::module_catalog::{ArtifactIdentity, ModuleId, ProtocolVersion};
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
    /// SHA-256 of the canonical, immutable stored request before host-side
    /// enrichment of `input`. Legacy routes may omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_sha256: Option<String>,
    /// For readback operations, the canonical digest of the exact target
    /// Operation's original request. This is distinct from the current
    /// command's own digest and is supplied by the Store.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_input_sha256: Option<String>,
}

/// Generic immutable receipt identity for an independently built module.
/// The Store validates every field against the authenticated binding,
/// retained descriptor selector, and original Operation request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleReceiptIdentity {
    pub schema_version: u16,
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub protocol: ProtocolVersion,
    pub binding_id: String,
    pub binding_generation: i64,
    pub operation_id: String,
    pub input_sha256: String,
}

impl ModuleReceiptIdentity {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.schema_version != 1
            || self.protocol.major != 1
            || self.protocol.minor != 0
            || self.binding_id.trim().is_empty()
            || self.binding_generation <= 0
            || self.operation_id.trim().is_empty()
            || self.input_sha256.len() != 64
            || !self
                .input_sha256
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err("module receipt identity is outside protocol 1.0");
        }
        Ok(())
    }
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
