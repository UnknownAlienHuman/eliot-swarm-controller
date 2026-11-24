//! Antigravity native user instruction discovery declaration.

use eliot_core::api::harness::UserInstructionFiles;

pub(super) const PROJECT_READ: Option<eliot_core::api::harness::ProjectInstructionRead> = None;

pub(super) const USER_FILES: Option<UserInstructionFiles> = Some(UserInstructionFiles {
    root_override_env: None,
    home_subdir: ".gemini",
    paths: &[
        "AGENTS.md",
        "GEMINI.md",
        "config/AGENTS.md",
        "config/GEMINI.md",
    ],
});
