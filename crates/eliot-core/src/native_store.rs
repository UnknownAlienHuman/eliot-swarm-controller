//! Harness-neutral effective native-store selection, verified before control.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeStoreBinding {
    pub home: String,
    pub root: String,
    pub native_scope: String,
}

/// Verified native inputs from a metadata reader, frozen with launch dispatch.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileStoreResult {
    pub binding: NativeStoreBinding,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub startup: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub catalogue_digest: Option<String>,
}
