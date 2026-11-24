//! OpenCode native project and user instruction discovery declarations.

use eliot_core::api::InstructionReadStatus::{Conditional, Documented};
use eliot_core::api::harness::{ProjectInstructionRead, UserInstructionFiles};

pub(super) const PROJECT_READ: Option<ProjectInstructionRead> = Some(ProjectInstructionRead {
    paths: &[("AGENTS.md", Documented), ("CLAUDE.md", Conditional)],
    report_version: false,
    byte_limit: None,
});

pub(super) const USER_FILES: Option<UserInstructionFiles> = Some(UserInstructionFiles {
    root_override_env: Some("XDG_CONFIG_HOME"),
    home_subdir: ".config",
    paths: &["opencode/AGENTS.md"],
});
