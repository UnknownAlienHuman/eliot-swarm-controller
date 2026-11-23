//! Codex native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::{HarnessCapabilities, InputTokenAccounting};

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        effort: true,
        manager_preference: Some(0),
        provider_probe: true,
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::CodexDeveloperInstructions,
        requires_pass_env: true,
        requires_frozen_store_inputs: true,
        native_discovery: true,
        resume_previous_turn: true,
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
