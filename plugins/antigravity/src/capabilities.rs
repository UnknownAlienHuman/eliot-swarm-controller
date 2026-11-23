//! Antigravity native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities};

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        effort: true,
        prepared_mcp_config: true,
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
