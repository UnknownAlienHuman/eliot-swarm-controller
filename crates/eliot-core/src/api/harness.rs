//! Read-only harness installation and native model catalogue DTOs.
//!
//! `harness.list` reports per-harness installation facts without starting a
//! model turn and without inference. `harness.models` returns the harness's
//! own read-only catalogue (Codex/Muse native `model/list`, the OpenCode
//! provider/model list; Claude exposes aliases plus the CLI default, and
//! Antigravity has no catalogue).

use crate::{Error, Result};
use serde::{Deserialize, Serialize};

/// Maximum harness name length.
pub const MAX_HARNESS_NAME_BYTES: usize = 32;
/// Maximum models returned in one catalogue page.
pub const MAX_HARNESS_MODELS: usize = 1024;
/// Maximum model id length.
pub const MAX_HARNESS_MODEL_ID_BYTES: usize = 256;

/// Request for `harness.list`. Empty object: no parameters.
#[derive(schemars::JsonSchema, Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessListRequest {}

impl HarnessListRequest {
    pub fn validate(&self) -> Result<()> {
        Ok(())
    }

    pub fn validate_response(&self, reply: &HarnessListResponse) -> Result<()> {
        if reply.harnesses.is_empty() || reply.harnesses.len() > 16 {
            return Err(Error::new(
                "invalid",
                "harness list response is empty or oversized",
            ));
        }
        for entry in &reply.harnesses {
            if entry.harness.trim().is_empty() || entry.harness.len() > MAX_HARNESS_NAME_BYTES {
                return Err(Error::new(
                    "invalid",
                    "harness list entry names an invalid harness",
                ));
            }
        }
        Ok(())
    }
}

/// One harness installation fact.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessEntry {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    pub harness: String,
    /// True when the installed native executable was found.
    pub installed: bool,
    /// Resolved absolute native path, or null (Codex resolves `codex` by
    /// name at spawn; absent means not found).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cmd: Option<String>,
    /// Bounded `<exe> --version` first line, or null when unavailable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    /// Machine-readable failure code when not installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    /// Human-readable searched-locations detail when not installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response for `harness.list`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessListResponse {
    pub harnesses: Vec<HarnessEntry>,
}

/// Request for `harness.models`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessModelsRequest {
    /// Harness code: codex|claude|muse|opencode|antigravity.
    pub harness: String,
}

impl HarnessModelsRequest {
    pub fn validate(&self) -> Result<()> {
        let trimmed = self.harness.trim();
        if trimmed.is_empty() || trimmed.len() > MAX_HARNESS_NAME_BYTES {
            return Err(Error::new(
                "invalid",
                "harness must be a non-empty string of at most 32 characters",
            ));
        }
        if !matches!(
            trimmed,
            "codex" | "claude" | "muse" | "opencode" | "antigravity"
        ) {
            return Err(Error::new(
                "unsupported",
                format!("unsupported harness {}", self.harness),
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, reply: &HarnessModelsResponse) -> Result<()> {
        if reply.harness.trim() != self.harness.trim() {
            return Err(Error::new(
                "invalid",
                "harness models response names another harness",
            ));
        }
        reply.validate()
    }
}

/// One catalogue model.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModelEntry {
    /// Native model id (`provider/model` for OpenCode).
    pub id: String,
    /// True for the harness native default entry.
    #[serde(default)]
    pub default: bool,
    /// Supported reasoning efforts, when the harness reports them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub efforts: Option<Vec<String>>,
    /// The harness default reasoning effort, when reported.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_effort: Option<String>,
}

/// Response for `harness.models`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessModelsResponse {
    pub harness: String,
    /// `native` (live catalogue), `aliases` (Claude, no catalogue), or
    /// `none` (no catalogue; details in `note`).
    pub catalogue: String,
    pub models: Vec<HarnessModelEntry>,
    /// Native default model id, when the harness names one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Human-readable provenance note (aliases, CLI default, unsupported).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl HarnessModelsResponse {
    pub fn validate(&self) -> Result<()> {
        if self.models.len() > MAX_HARNESS_MODELS {
            return Err(Error::new(
                "invalid",
                "harness model catalogue exceeds 1024 entries",
            ));
        }
        for model in &self.models {
            if model.id.trim().is_empty()
                || model.id.len() > MAX_HARNESS_MODEL_ID_BYTES
                || model.id.chars().any(char::is_control)
            {
                return Err(Error::new(
                    "invalid",
                    "harness model id must be non-empty of at most 256 characters",
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn models_request_validates_harness() {
        HarnessModelsRequest {
            harness: "codex".to_owned(),
        }
        .validate()
        .unwrap();
        let error = HarnessModelsRequest {
            harness: "fake".to_owned(),
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "unsupported");
        let error = HarnessModelsRequest {
            harness: String::new(),
        }
        .validate()
        .unwrap_err();
        assert_eq!(error.code, "invalid");
    }

    #[test]
    fn models_response_rejects_oversize_catalogue() {
        let response = HarnessModelsResponse {
            harness: "codex".to_owned(),
            catalogue: "native".to_owned(),
            models: vec![],
            default: None,
            note: None,
        };
        response.validate().unwrap();
        let big = HarnessModelsResponse {
            harness: "codex".to_owned(),
            catalogue: "native".to_owned(),
            models: (0..(MAX_HARNESS_MODELS + 1))
                .map(|index| HarnessModelEntry {
                    id: format!("m{index}"),
                    default: false,
                    efforts: None,
                    default_effort: None,
                })
                .collect(),
            default: None,
            note: None,
        };
        assert_eq!(big.validate().unwrap_err().code, "invalid");
    }
}
