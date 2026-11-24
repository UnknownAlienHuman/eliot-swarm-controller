//! Codex native adapter capability declaration.

use eliot_core::api::harness::{
    AdhocEffort, LaunchDefaultPolicy, NativeAccessBounds, NativeQuotaReadback, ProfileCapabilities,
    ProjectInstructionRead, QuotaProjectionFormat, UserInstructionFiles,
};
use eliot_core::api::native_profile::{
    CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES, NativeSubagentsPolicy, codex_project_doc_override,
};
use eliot_core::api::{
    HarnessCapabilities, InputTokenAccounting, InstructionReadStatus::Documented,
};
use eliot_core::{InjectPoint, McpPolicy};
use eliot_plugin::capabilities::{adhoc_launch, quota_readback, sandbox_permissions};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        profile: ProfileCapabilities {
            script_wrapper_cmd: true,
            project_doc_max_bytes: true,
            native_subagents_policy: Some(NativeSubagentsPolicy::deny_by_default()),
            ..ProfileCapabilities::default()
        },
        quota_readback: Some(NativeQuotaReadback {
            method: "account/rateLimits/read",
            unsupported_note: Some(
                "Installed Codex app-server does not support account/rateLimits/read",
            ),
            route_refusal: Some(|profile, _| {
                profile.get("route_binding").is_some_and(|value| !value.is_null()).then_some(
                    "Codex profile routes through an adopted proxy; ChatGPT account limits would describe the direct route",
                )
            }),
            ..quota_readback(
                QuotaProjectionFormat::RateLimits,
                |_| "Codex account/rateLimits/read; provider read-only values",
            )
        }),
        adhoc_effort: Some(AdhocEffort::Freeform),
        adhoc_launch: Some(adhoc_launch(
            sandbox_permissions(true, "approval_policy", ["on-request", "never", "never"]),
            McpPolicy::Minimal,
        )),
        documentation_section: Some("docs/harnesses/codex.md#sources"),
        native_access_bounds: Some(native_access_bounds),
        workspace_write_commit_roots: true,
        effort: true,
        manager_preference: Some(0),
        provider_probe: true,
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::CodexDeveloperInstructions,
        project_instruction_read: Some(ProjectInstructionRead {
            paths: &[
                ("AGENTS.md", Documented),
                ("AGENTS.override.md", Documented),
            ],
            byte_limit: Some(|profile| {
                profile
                    .as_object()
                    .and_then(codex_project_doc_override)
                    .unwrap_or(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES)
            }),
            ..ProjectInstructionRead::default()
        }),
        user_instruction_files: Some(UserInstructionFiles {
            root_override_env: Some("CODEX_HOME"),
            home_subdir: ".codex",
            paths: &["AGENTS.override.md", "AGENTS.md"],
        }),
        requires_pass_env: true,
        requires_frozen_store_inputs: true,
        native_discovery: true,
        resume_previous_turn: true,
        reconcile_unfinished_tools: true,
        native_archive: Some(eliot_core::api::harness::NativeArchiveBinding::FrozenStore),
        agent_identity_env: true,
        launch_default_policy: LaunchDefaultPolicy::NativeMarkedDefault,
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
