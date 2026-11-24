//! Claude native adapter capability declaration.

use eliot_core::McpPolicy;
use eliot_core::api::harness::{
    AdhocEffort, AdhocLaunchDefaults, DocumentedModelCatalog, ModelReaderOptions,
    NativeAccessBounds, NativeQuotaReadback, ProfileCapabilities, ProjectInstructionRead,
    QuotaProjectionFormat, UserInstructionFiles,
};
use eliot_core::api::native_profile::{CLAUDE_EFFORT_LEVELS, NativeSubagentsPolicy};
use eliot_core::api::{
    ConfigureSessionMode, HarnessCapabilities, InputTokenAccounting,
    InstructionReadStatus::{Documented, Unknown},
};
use eliot_plugin::capabilities::{adhoc_launch, quota_readback, sandbox_permissions};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        profile: ProfileCapabilities {
            model_pin_selectors: true,
            route_selectors_resolved: true,
            route_scope_secret_names: &["CLAUDE_CODE_OAUTH_TOKEN"],
            config_dir_credential: true,
            latest_cmd: true,
            command_refusal: Some(eliot_plugin::capabilities::absolute_native_command_refusal),
            native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
            ..ProfileCapabilities::default()
        },
        model_reader: ModelReaderOptions {
            interruption_code: Some("unknown"),
            ..ModelReaderOptions::default()
        },
        documented_models: Some(DocumentedModelCatalog {
            entries: crate::native::models::documented_models,
            note: "Claude native initialize inventory is unavailable for this scope. Entries are documented aliases and full model ids with unknown availability. The CLI default applies when no model is passed.",
            unverified_route: "Claude model catalogue route could not be verified",
            empty_native_inventory: "Claude model.list returned no usable ids in its projected inventory",
        }),
        quota_readback: Some(NativeQuotaReadback {
            route_refusal: Some(external_quota_route),
            ..quota_readback(
                QuotaProjectionFormat::Windows,
                |_| "Claude CLI OAuth usage; provider read-only values",
            )
        }),
        adhoc_effort: Some(AdhocEffort::Choices {
            values: CLAUDE_EFFORT_LEVELS,
            refusal: "unknown Claude effort level (low, medium, high, xhigh, or max)",
        }),
        adhoc_launch: Some(AdhocLaunchDefaults {
            mcp: McpPolicy::Minimal,
            pass_env: &["CLAUDE_CONFIG_DIR"],
            ..adhoc_launch(
                sandbox_permissions(
                    false,
                    "permission_mode",
                    ["manual", "auto", "bypassPermissions"],
                ),
                McpPolicy::Minimal,
            )
        }),
        documentation_section: Some("docs/harnesses/claude-code.md#sources-and-unresolved-facts"),
        configure_effort_refusal: Some(
            "Claude effort applies only at launch and cannot change mid-session (launch_only)",
        ),
        native_access_bounds: Some(native_access_bounds),
        effort: true,
        manager_preference: Some(1),
        owner_rules_delivery:
            eliot_core::api::OwnerRulesDeliveryMethod::ClaudeAppendSystemPromptFile,
        project_instruction_read: Some(ProjectInstructionRead {
            paths: &[
                ("AGENTS.md", Unknown),
                ("CLAUDE.md", Documented),
                ("CLAUDE.local.md", Documented),
                (".claude/CLAUDE.md", Documented),
                (".claude/CLAUDE.local.md", Unknown),
            ],
            report_version: true,
            ..ProjectInstructionRead::default()
        }),
        user_instruction_files: Some(UserInstructionFiles {
            root_override_env: Some("CLAUDE_CONFIG_DIR"),
            home_subdir: ".claude",
            paths: &["CLAUDE.md"],
        }),
        resume_config: true,
        restart_requires_idle: true,
        accepts_send_during_native_turn: true,
        configure_reports_accepted_model: true,
        configure_session_mode: ConfigureSessionMode::WhenModelPresent,
        configure_requires_idle_turn: true,
        executable_stems: &["claude"],
        version_prefixes: &["claude-code", "claude"],
        legacy_result_status: true,
        input_token_accounting: InputTokenAccounting::ExcludesCacheWrites,
        ..HarnessCapabilities::default()
    }
}

fn external_quota_route(_: &Value, environment: &Value) -> Option<&'static str> {
    crate::quota::uses_external_route(environment)
        .then_some("Claude profile route is external or ambiguous; CLI OAuth quota would describe an unverified route")
}

fn native_access_bounds(snapshot: &Value) -> Option<NativeAccessBounds> {
    match snapshot["permission_mode"].as_str() {
        Some("plan") => Some((0, 0)),
        Some("manual" | "dontAsk") => Some((1, 1)),
        Some("acceptEdits" | "auto") => Some((2, 2)),
        Some("bypassPermissions") => Some((3, 3)),
        // The CLI default can come from settings outside the frozen profile.
        None => Some((1, 3)),
        Some(_) => None,
    }
}
