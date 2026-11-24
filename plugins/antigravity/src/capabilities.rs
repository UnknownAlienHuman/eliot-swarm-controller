//! Antigravity native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::harness::{NativeAccessBounds, UserInstructionFiles};
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        documentation_section: Some("docs/harnesses/antigravity.md#native-identity-and-transport"),
        native_access_bounds: Some(native_access_bounds),
        effort: true,
        prepared_mcp_config: true,
        user_instruction_files: Some(UserInstructionFiles {
            root_override_env: None,
            home_subdir: ".gemini",
            paths: &[
                "AGENTS.md",
                "GEMINI.md",
                "config/AGENTS.md",
                "config/GEMINI.md",
            ],
        }),
        reader_executable_discovery: true,
        resume_config: true,
        guard_refusal_point: Some(InjectPoint::TurnSafePoint),
        configure_session_mode: ConfigureSessionMode::Always,
        configure_reports_effort_verification: true,
        executable_stems: &["agy", "antigravity"],
        version_prefixes: &["antigravity", "agy"],
        ..HarnessCapabilities::default()
    }
}

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match (
        snapshot["dangerously_skip_permissions"].as_bool()?,
        snapshot["mode"].as_str(),
    ) {
        (true, _) => Some((3, 3)),
        (false, Some("plan")) => Some((0, 0)),
        (false, Some("auto" | "accept-edits") | None) => Some((1, 2)),
        (false, Some(_)) => None,
    }
}
