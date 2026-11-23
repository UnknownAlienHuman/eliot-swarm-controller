//! Muse native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities};

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        data_root_only_mcp: true,
        resume_config: true,
        launch_default_resolution: true,
        guard_refusal_point: Some(InjectPoint::ActiveTurn),
        allowed_configure_efforts: Some(&eliot_core::api::MUSE_REASONING_EFFORT_TIERS),
        configure_session_mode: ConfigureSessionMode::WhenModelPresent,
        configure_requires_idle_turn: true,
        configure_carries_previous_overlay: true,
        configure_effort_per_turn_overlay: true,
        configure_resolution_readback: true,
        configure_reports_effort_verification: true,
        reads_retained_quota: true,
        view_projection_gap_recovery: true,
        native_child_families: &eliot_core::MUSE_NATIVE_CHILD_FAMILIES,
        executable_stems: &["muse"],
        version_prefixes: &["muse-code", "muse"],
        session_input_includes_cache_reads: true,
        ..HarnessCapabilities::default()
    }
}
