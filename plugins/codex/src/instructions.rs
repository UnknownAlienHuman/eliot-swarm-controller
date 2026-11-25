//! Codex-native project and user instruction discovery declarations.

use eliot_core::api::InstructionInventoryPath;
use eliot_core::api::InstructionReadStatus::Documented;
use eliot_core::api::harness::{ProjectInstructionRead, UserInstructionFiles};
use serde_json::{Map, Value};

pub(super) const INSTRUCTION_INVENTORY: &[InstructionInventoryPath] = &[
    InstructionInventoryPath {
        path: "AGENTS.md",
        inventory_rank: 0,
        primary_rank: 0,
    },
    InstructionInventoryPath {
        path: "AGENTS.override.md",
        inventory_rank: 1,
        primary_rank: 1,
    },
];

pub(super) const PROJECT_READ: Option<ProjectInstructionRead> = Some(ProjectInstructionRead {
    paths: &[
        ("AGENTS.md", Documented),
        ("AGENTS.override.md", Documented),
    ],
    report_version: false,
    byte_limit: Some(project_doc_max_bytes),
});

pub(super) const USER_FILES: Option<UserInstructionFiles> = Some(UserInstructionFiles {
    root_override_env: Some("CODEX_HOME"),
    home_subdir: ".codex",
    paths: &["AGENTS.override.md", "AGENTS.md"],
});

fn project_doc_max_bytes(profile: &Value) -> u64 {
    profile
        .as_object()
        .and_then(project_doc_override)
        .unwrap_or(DEFAULT_PROJECT_DOC_MAX_BYTES)
}

/// Codex CLI's documented default for project instructions.
const DEFAULT_PROJECT_DOC_MAX_BYTES: u64 = 32 * 1024;

/// The native override shared by the launch report and Codex argv.
pub(super) fn project_doc_override(profile: &Map<String, Value>) -> Option<u64> {
    if profile.get(eliot_core::PROVIDER_PROBE_LAUNCH_ARG) == Some(&Value::Bool(true)) {
        return Some(0);
    }
    profile.get("project_doc_max_bytes").and_then(Value::as_u64)
}
