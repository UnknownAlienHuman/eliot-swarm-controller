//! Muse native adapter capability declaration.

use eliot_core::api::harness::{
    AdhocEffort, LaunchDefaultPolicy, ModelReaderOptions, NativeAccessBounds, NativeQuotaReadback,
    NativeRoute, ProfileCapabilities, QuotaProjectionFormat,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{
    ConfigureSessionMode, HarnessCapabilities, InstructionReadStatus::NotNative,
};
use eliot_core::{InjectPoint, McpPolicy};
use eliot_plugin::capabilities::{
    adhoc_launch, instruction_files, quota_readback, sandbox_permissions,
};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        native_route: NativeRoute::FixedCredential("ambient"),
        profile: ProfileCapabilities {
            approval_field: Some("approval"),
            native_subagents_policy: Some(NativeSubagentsPolicy::allow_by_default()),
            ..ProfileCapabilities::default()
        },
        model_reader: ModelReaderOptions {
            budget_seconds: Some(60),
            ..ModelReaderOptions::default()
        },
        quota_readback: Some(NativeQuotaReadback {
            method: "usage --json",
            unsupported_note: Some(
                "Muse usage --json requires the installed Windows vendor launcher",
            ),
            route_refusal: Some(crate::quota::route_refusal),
            ..quota_readback(
                QuotaProjectionFormat::UsagePair,
                |_| "Muse subscription allowance",
            )
        }),
        adhoc_effort: Some(AdhocEffort::Freeform),
        adhoc_launch: Some(adhoc_launch(
            sandbox_permissions(
                true,
                "approval",
                ["onRequest", "promptUnmatched", "allowAll"],
            ),
            McpPolicy::Inherit,
        )),
        documentation_section: Some("docs/harnesses/muse.md#usage-attribution"),
        native_access_bounds: Some(native_access_bounds),
        data_root_only_mcp: true,
        project_instruction_read: Some(instruction_files(&[
            ("AGENTS.md", NotNative),
            ("AGENTS.override.md", NotNative),
        ])),
        defer_harness_check: true, // #949
        resume_config: true,
        resume_previous_turn: true,
        launch_default_policy: LaunchDefaultPolicy::OmitForServerDefault,
        guard_refusal_point: Some(InjectPoint::ActiveTurn),
        guard_refusal_choice_error: Some("Muse guarded request offers multiple refusal options"),
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
        native_child_history_refusal: Some(
            "native history is unsupported for Muse child observations",
        ),
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
