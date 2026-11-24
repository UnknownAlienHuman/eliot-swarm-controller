//! Codex-native project and user instruction discovery declarations.

use eliot_core::api::InstructionReadStatus::Documented;
use eliot_core::api::harness::{ProjectInstructionRead, UserInstructionFiles};
use eliot_core::api::native_profile::{
    CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES, codex_project_doc_override,
};
use serde_json::Value;

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
        .and_then(codex_project_doc_override)
        .unwrap_or(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES)
}
