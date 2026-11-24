//! Claude Code native project and user instruction discovery declarations.

use eliot_core::api::InstructionReadStatus::{Documented, Unknown};
use eliot_core::api::harness::{ProjectInstructionRead, UserInstructionFiles};

pub(super) const PROJECT_READ: Option<ProjectInstructionRead> = Some(ProjectInstructionRead {
    paths: &[
        ("AGENTS.md", Unknown),
        ("CLAUDE.md", Documented),
        ("CLAUDE.local.md", Documented),
        (".claude/CLAUDE.md", Documented),
        (".claude/CLAUDE.local.md", Unknown),
    ],
    report_version: true,
    byte_limit: None,
});

pub(super) const USER_FILES: Option<UserInstructionFiles> = Some(UserInstructionFiles {
    root_override_env: Some("CLAUDE_CONFIG_DIR"),
    home_subdir: ".claude",
    paths: &["CLAUDE.md"],
});
