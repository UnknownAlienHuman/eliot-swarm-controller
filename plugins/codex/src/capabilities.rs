//! Codex native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::harness::NativeAccessBounds;
use eliot_core::api::{HarnessCapabilities, InputTokenAccounting};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        native_access_bounds: Some(native_access_bounds),
        workspace_write_commit_roots: true,
        effort: true,
        manager_preference: Some(0),
        provider_probe: true,
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::CodexDeveloperInstructions,
        requires_pass_env: true,
        requires_frozen_store_inputs: true,
        native_discovery: true,
        resume_previous_turn: true,
        reconcile_unfinished_tools: true,
        native_archive: Some(eliot_core::api::harness::NativeArchiveBinding::FrozenStore),
        agent_identity_env: true,
        launch_default_resolution: true,
        guard_refusal_point: Some(InjectPoint::ActiveTurn),
        compact: true,
        observed_families: eliot_core::OBSERVED_ROLLOUT_FAMILIES,
        observed_live_families: eliot_core::OBSERVED_ROLLOUT_FAMILIES,
        native_child_families: &eliot_core::CODEX_NATIVE_CHILD_FAMILIES,
        executable_stems: &["codex"],
        version_prefixes: &["codex-cli", "codex"],
        input_token_accounting: InputTokenAccounting::IncludesCacheReads,
        ..HarnessCapabilities::default()
    }
}

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match (
        snapshot["permissions"].as_str()?,
        snapshot["approval_policy"].as_str()?,
    ) {
        ("read-only", _) => Some((0, 0)),
        ("workspace-write", "on-request") => Some((1, 1)),
        ("workspace-write", "never") => Some((2, 2)),
        ("danger-full-access", _) => Some((3, 3)),
        _ => None,
    }
}
