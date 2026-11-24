use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum ProviderResponseWithheldDetection {
    ExactNativeMessage,
}

/// Provider supplied evidence that a response was withheld.
/// `reset_at` is an absolute Unix timestamp in milliseconds, when an adapter
/// validated and normalized a native protocol value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderResponseWithheldFact {
    pub detection: ProviderResponseWithheldDetection,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_at: Option<i64>,
}

impl ProviderResponseWithheldFact {
    pub fn new(detection: ProviderResponseWithheldDetection, reset_at: Option<i64>) -> Self {
        Self {
            detection,
            reset_at: reset_at.filter(|value| *value >= 0),
        }
    }
}
