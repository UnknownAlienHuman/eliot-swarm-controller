//! Opencode native adapter capability declaration.

use eliot_core::api::{ConfigureSessionMode, HarnessCapabilities, InputTokenAccounting};

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        effort: true,
        manager_preference: Some(2),
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::OpenCodeAgentsFile,
        native_archive: Some(eliot_core::api::harness::NativeArchiveBinding::Workspace),
        resume_config: true,
        manifest_extra_verbs: &["models"],
        compact: true,
        models_workspace_preview: true,
        models_preview_auth_integrations: true,
        initial_model_refusal_cleanup: true,
        configure_session_mode: ConfigureSessionMode::Always,
        turn_completed_reports_effective_model: true,
        backfill_failed_turn_readback: true,
        executable_stems: &["opencode"],
        version_prefixes: &["opencode"],
        input_token_accounting: InputTokenAccounting::ExcludesCacheWrites,
        output_excludes_reasoning: true,
        ..HarnessCapabilities::default()
    }
}
