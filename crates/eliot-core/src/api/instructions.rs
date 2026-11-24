//! Shared instruction discovery declarations and launch-time readback DTOs.
//!
//! Adapter modules own the native paths and evidence rules. These types keep
//! the capability and wire shapes harness-neutral.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Evidence level for whether a selected native harness reads one candidate.
/// Documented and Conditional are not runtime-consumption receipts.
#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionReadStatus {
    Documented,
    Partial,
    Conditional,
    NotNative,
    Unknown,
}

/// Evidence available for native project instruction-file reading.
#[derive(Clone, Copy, Debug, Default)]
pub struct ProjectInstructionRead {
    pub paths: &'static [(&'static str, InstructionReadStatus)],
    pub report_version: bool,
    pub byte_limit: Option<fn(&serde_json::Value) -> u64>,
}

/// Locations of an adapter's documented user instruction files.
#[derive(Clone, Copy, Debug)]
pub struct UserInstructionFiles {
    pub root_override_env: Option<&'static str>,
    pub home_subdir: &'static str,
    pub paths: &'static [&'static str],
}

/// Bounded launch-time view of project instruction files in the actual execution checkout.
/// Native reading is documented behavior, not proof that a specific model consumed a file.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstructionDeliveryView {
    /// Actual checkout scanned after workspace preparation; absent when it could not be established.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_workspace: Option<String>,
    /// The primary instruction candidate used for automatic delivery.
    pub primary_path: String,
    /// Fixed, bounded root instruction candidates and their launch-time facts.
    pub files: Vec<InstructionFileView>,
}

/// Stable union of the project instruction paths supported by the native adapters.
pub const INSTRUCTION_FILE_PATHS: [&str; 7] = [
    "AGENTS.md",
    "AGENTS.override.md",
    "CLAUDE.md",
    "CLAUDE.local.md",
    ".claude/CLAUDE.md",
    ".claude/CLAUDE.local.md",
    "GEMINI.md",
];

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionFileStatus {
    Present,
    Missing,
    Unreadable,
    NotRegular,
    OutsideWorkspace,
    TooLarge,
    Binary,
    WorkspaceUnavailable,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InstructionSourceStatus {
    NotNeeded,
    Complete,
    Truncated,
    Unavailable,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct InstructionFileView {
    /// Relative to source_workspace.
    pub path: String,
    pub status: InstructionFileStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
    pub native_read_status: InstructionReadStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_read_version: Option<String>,
    pub delivery_status: InstructionSourceStatus,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub delivery_redacted: bool,
}

impl InstructionDeliveryView {
    pub fn validate(&self) -> Result<()> {
        if !INSTRUCTION_FILE_PATHS.contains(&self.primary_path.as_str())
            || self.files.len() != INSTRUCTION_FILE_PATHS.len()
            || self
                .source_workspace
                .as_deref()
                .is_some_and(|path| path.trim().is_empty() || path.chars().any(char::is_control))
        {
            return Err(Error::new(
                "invalid",
                "instruction delivery view is malformed",
            ));
        }
        let mut seen = BTreeSet::new();
        for file in &self.files {
            if !INSTRUCTION_FILE_PATHS.contains(&file.path.as_str())
                || !seen.insert(file.path.as_str())
                || file.path.chars().any(char::is_control)
                || file.sha256.as_deref().is_some_and(|digest| {
                    digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
                })
                || (file.status == InstructionFileStatus::Present
                    && (file.size_bytes.is_none()
                        || file.size_bytes.is_some_and(|size| size > 8 * 1024 * 1024)
                        || file.sha256.is_none()))
                || (file.status == InstructionFileStatus::Binary
                    && (file.size_bytes.is_none()
                        || file.size_bytes.is_some_and(|size| size > 8 * 1024 * 1024)
                        || file.sha256.is_none()))
                || (file.status == InstructionFileStatus::Missing
                    && (file.size_bytes.is_some() || file.sha256.is_some()))
            {
                return Err(Error::new("invalid", "instruction file fact is malformed"));
            }
        }
        Ok(())
    }
}
