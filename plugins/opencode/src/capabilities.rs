//! Opencode native adapter capability declaration.

use eliot_core::api::harness::UserInstructionFiles;
use eliot_core::api::{
    ConfigureSessionMode, HarnessCapabilities, InputTokenAccounting, InstructionReadStatus,
};

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        // No frozen permission knob or OS sandbox constrains this range.
        native_access_bounds: Some(|_| Some((1, 3))),
        effort: true,
        manager_preference: Some(2),
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::OpenCodeAgentsFile,
        project_instruction_read: Some(|path, _, _| {
            let status = match path {
                "AGENTS.md" => InstructionReadStatus::Documented,
                "CLAUDE.md" => InstructionReadStatus::Conditional,
                _ => InstructionReadStatus::Unknown,
            };
            (status, None)
        }),
        user_instruction_files: Some(UserInstructionFiles {
            root_override_env: Some("XDG_CONFIG_HOME"),
            home_subdir: ".config",
            paths: &["opencode/AGENTS.md"],
        }),
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
