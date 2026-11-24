//! Muse native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::harness::NativeAccessBounds;
use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        native_access_bounds: Some(native_access_bounds),
        data_root_only_mcp: true,
        defer_harness_check: true, // #949
        resume_config: true,
        resume_previous_turn: true,
        launch_default_resolution: true,
        guard_refusal_point: Some(InjectPoint::ActiveTurn),
        allowed_configure_efforts: Some(&eliot_core::api::MUSE_REASONING_EFFORT_TIERS),
        configure_session_mode: ConfigureSessionMode::WhenModelPresent,
        configure_requires_idle_turn: true,
        configure_carries_previous_overlay: true,
        configure_effort_per_turn_overlay: true,
        configure_resolution_readback: true,
        configure_reports_accepted_model: true,
        provider_response_model_verification: true,
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

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match (
        snapshot["permissions"].as_str()?,
        snapshot["approval"].as_str(),
    ) {
        ("danger-full-access", _) => Some((3, 3)),
        ("workspace-write", Some("onRequest" | "denyUnmatched")) => Some((1, 1)),
        ("workspace-write", Some("promptUnmatched" | "allowAll")) => Some((2, 2)),
        ("workspace-write", None) => Some((1, 2)),
        _ => None,
    }
}
