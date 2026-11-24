//! Shared instruction discovery declarations and launch-time readback DTOs.
//!
//! Adapter modules own the native paths and evidence rules. These types keep
//! the capability and wire shapes harness-neutral.

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// One adapter-owned project instruction path in the composed inventory.
/// Catalog order and primary fallback order are independent stable ranks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstructionInventoryPath {
    pub path: &'static str,
    pub inventory_rank: u8,
    pub primary_rank: u8,
}

impl InstructionInventoryPath {
    pub fn validate(&self) -> Result<()> {
        if !safe_relative_path(self.path) {
            return Err(Error::new(
                "invalid",
                "instruction inventory path is not a safe relative path",
            ));
        }
        Ok(())
    }
}

/// Sort and validate adapter instruction paths without embedding native names.
pub(super) fn normalize_instruction_inventory(
    inventory: &[InstructionInventoryPath],
) -> Result<Vec<InstructionInventoryPath>> {
    if inventory.is_empty() || inventory.len() > u8::MAX as usize {
        return Err(Error::new(
            "invalid",
            "instruction inventory has an invalid number of paths",
        ));
    }
    let mut normalized = inventory.to_vec();
    for path in &normalized {
        path.validate()?;
    }
    normalized.sort_by_key(|path| path.inventory_rank);
    if normalized.iter().enumerate().any(|(index, path)| {
        path.inventory_rank as usize != index
            || normalized[..index]
                .iter()
                .any(|prior| prior.path == path.path)
    }) {
        return Err(Error::new(
            "invalid",
            "instruction inventory paths or catalog ranks are duplicated or incomplete",
        ));
    }
    let mut primary_ranks: Vec<_> = normalized.iter().map(|path| path.primary_rank).collect();
    primary_ranks.sort_unstable();
    if primary_ranks
        .iter()
        .enumerate()
        .any(|(index, rank)| *rank as usize != index)
    {
        return Err(Error::new(
            "invalid",
            "instruction inventory primary ranks are duplicated or incomplete",
        ));
    }
    Ok(normalized)
}

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
    /// Composed, bounded root instruction candidates and their launch-time facts.
    pub files: Vec<InstructionFileView>,
}

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
        super::HarnessCapabilities::require_registered()?;
        let inventory = super::HarnessCapabilities::project_instruction_inventory();
        if self.files.len() != inventory.len()
            || !inventory
                .iter()
                .any(|candidate| candidate.path == self.primary_path)
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
            if !inventory
                .iter()
                .any(|candidate| candidate.path == file.path)
                || !seen.insert(file.path.as_str())
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

fn safe_relative_path(path: &str) -> bool {
    !path.trim().is_empty()
        && !path.starts_with('/')
        && !path.starts_with('\\')
        && !path.contains('\\')
        && !path.contains(':')
        && !path.chars().any(char::is_control)
        && path
            .split('/')
            .all(|component| !component.is_empty() && component != "." && component != "..")
}
