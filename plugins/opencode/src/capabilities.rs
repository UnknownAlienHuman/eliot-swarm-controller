//! Opencode native adapter capability declaration.

use eliot_core::McpPolicy;
use eliot_core::api::harness::{
    AdhocEffort, AdhocLaunchDefaults, AdhocModel, AdhocPermissions, ModelReaderOptions,
    NativeQuotaReadback, ProfileCapabilities, QuotaProjectionFormat, UserInstructionFiles,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{
    ConfigureSessionMode, HarnessCapabilities, InputTokenAccounting,
    InstructionReadStatus::{Conditional, Documented},
};
use eliot_plugin::capabilities::{adhoc_launch, instruction_files, quota_readback};

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        profile: ProfileCapabilities {
            route_scope_secret_names: &["OPENCODE_CONFIG_CONTENT"],
            local_provider: true,
            native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
            ..ProfileCapabilities::default()
        },
        model_reader: ModelReaderOptions {
            separate_variant_reader: true,
            ..ModelReaderOptions::default()
        },
        quota_readback: Some(NativeQuotaReadback {
            adhoc_integration: Some("opencode-go"),
            ..quota_readback(QuotaProjectionFormat::IntegrationWindows, |details| {
                if details["integration"] == "opencode" {
                    "OpenCode Console workspace budgets and Go allowance; provider read-only values"
                } else {
                    "OpenCode Go account usage; provider read-only values"
                }
            })
        }),
        adhoc_effort: Some(AdhocEffort::Freeform),
        adhoc_launch: Some(AdhocLaunchDefaults {
            model: AdhocModel::ProviderIntegration,
            ..adhoc_launch(
                AdhocPermissions::RefuseFull(
                    "the opencode slice has no full-access permission mapping",
                ),
                McpPolicy::Minimal,
            )
        }),
        documentation_section: Some("docs/harnesses/opencode.md#working-profile"),
        // No frozen permission knob or OS sandbox constrains this range.
        native_access_bounds: Some(|_| Some((1, 3))),
        effort: true,
        manager_preference: Some(2),
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::OpenCodeAgentsFile,
        project_instruction_read: Some(instruction_files(&[
            ("AGENTS.md", Documented),
            ("CLAUDE.md", Conditional),
        ])),
        user_instruction_files: Some(UserInstructionFiles {
            root_override_env: Some("XDG_CONFIG_HOME"),
            home_subdir: ".config",
            paths: &["opencode/AGENTS.md"],
        }),
        native_archive: Some(eliot_core::api::harness::NativeArchiveBinding::Workspace),
        resume_config: true,
        manifest_extra_verbs: &["models"],
        compact: true,
        compaction_gap_recovery: true,
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
