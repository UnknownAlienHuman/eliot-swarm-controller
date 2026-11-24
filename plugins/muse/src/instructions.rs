//! Muse native project instruction discovery declarations.

use eliot_core::api::InstructionInventoryPath;
use eliot_core::api::InstructionReadStatus::NotNative;
use eliot_core::api::harness::ProjectInstructionRead;

pub(super) const INSTRUCTION_INVENTORY: &[InstructionInventoryPath] = &[];

pub(super) const PROJECT_READ: Option<ProjectInstructionRead> = Some(ProjectInstructionRead {
    paths: &[("AGENTS.md", NotNative), ("AGENTS.override.md", NotNative)],
    report_version: false,
    byte_limit: None,
});
