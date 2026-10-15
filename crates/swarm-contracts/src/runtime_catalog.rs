//! Read-only projection contract for the documented current runtime catalog.
//!
//! Configured values, trusted module descriptors, and observed native facts
//! remain separate. An unknown value is represented as a gap, never as an
//! empty supported inventory.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStatus {
    Current,
    Stale,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence<T> {
    pub status: EvidenceStatus,
    pub source: Option<String>,
    pub observed_at_ms: Option<i64>,
    pub value: Option<T>,
    pub gaps: Vec<String>,
}

impl<T> Evidence<T> {
    pub fn unknown(gap: impl Into<String>) -> Self {
        Self {
            status: EvidenceStatus::Unknown,
            source: None,
            observed_at_ms: None,
            value: None,
            gaps: vec![gap.into()],
        }
    }

    pub fn current(source: impl Into<String>, observed_at_ms: i64, value: T) -> Self {
        Self {
            status: EvidenceStatus::Current,
            source: Some(source.into()),
            observed_at_ms: Some(observed_at_ms),
            value: Some(value),
            gaps: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredModelTarget {
    pub id: Option<String>,
    pub provider_id: Option<String>,
    pub variant: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfiguredNativeOptions {
    /// Safe, known model/options fields copied from validated host config.
    /// Workspace paths, auth references, environment values, and raw options
    /// are never included here.
    pub values: Value,
    pub gaps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleDescriptorEvidence {
    pub status: String,
    pub module_id: Option<String>,
    pub artifact_id: Option<String>,
    pub registered_revision: Option<u64>,
    pub selected_revision: Option<u64>,
    /// Adapter contract metadata only; these names grant no runtime rights.
    pub contract_capabilities: Vec<String>,
    pub gaps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCatalogRoute {
    pub alias: String,
    pub runtime: String,
    pub artifact_id: String,
    pub enabled: bool,
    pub configured_model_target: Evidence<ConfiguredModelTarget>,
    pub configured_native_options: Evidence<ConfiguredNativeOptions>,
    pub module_descriptor: ModuleDescriptorEvidence,
    pub installation: Evidence<String>,
    pub native_models: Evidence<Vec<Value>>,
    pub native_capabilities: Evidence<Vec<Value>>,
    pub trust: Evidence<String>,
    pub account_capacity_group: Evidence<String>,
    pub freshness: Vec<String>,
    pub gaps: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeMcpProfile {
    pub name: String,
    pub tool_profile: String,
    pub effective_surface: String,
    pub surface_id: String,
    pub configured_surface: Option<String>,
    pub deferred_groups: Vec<String>,
    pub manual_tools: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeCatalog {
    pub schema_version: u16,
    /// Query time, not the observation time of native evidence.
    pub as_of_ms: i64,
    pub module_registry_revision: u64,
    pub routes: Vec<RuntimeCatalogRoute>,
    pub agent_profiles: Evidence<Vec<Value>>,
    pub mcp_profiles: Evidence<Vec<RuntimeMcpProfile>>,
    pub gaps: Vec<String>,
}
