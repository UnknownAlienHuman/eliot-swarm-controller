//! Codex native adapter capability declaration.

use eliot_core::InjectPoint;
use eliot_core::api::harness::{
    AdhocAccess, AdhocEffort, AdhocLaunchDefaults, LaunchDefaultPolicy, NativeAccessBounds,
    NativeQuotaReadback, QuotaProjectionFormat, UserInstructionFiles,
};
use eliot_core::api::native_profile::{
    CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES, CodexApprovalPolicy, CodexPermissions,
    codex_project_doc_override,
};
use eliot_core::api::{HarnessCapabilities, InputTokenAccounting, InstructionReadStatus};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        quota_readback: Some(NativeQuotaReadback {
            projection: QuotaProjectionFormat::RateLimits,
            method: "account/rateLimits/read",
            description: |_| "Codex account/rateLimits/read; provider read-only values",
            unsupported_note: Some(
                "Installed Codex app-server does not support account/rateLimits/read",
            ),
            route_refusal: Some(|profile, _| {
                profile.get("route_binding").is_some_and(|value| !value.is_null()).then_some(
                    "Codex profile routes through an adopted proxy; ChatGPT account limits would describe the direct route",
                )
            }),
        }),
        adhoc_effort: Some(AdhocEffort::Freeform),
        adhoc_launch: Some(AdhocLaunchDefaults {
            permissions: adhoc_permissions,
            mcp: eliot_core::McpPolicy::Inherit,
            pass_env: &[],
            installed_profile_mcp: eliot_core::McpPolicy::Minimal,
        }),
        documentation_section: Some("docs/harnesses/codex.md#sources"),
        native_access_bounds: Some(native_access_bounds),
        workspace_write_commit_roots: true,
        effort: true,
        manager_preference: Some(0),
        provider_probe: true,
        owner_rules_delivery: eliot_core::api::OwnerRulesDeliveryMethod::CodexDeveloperInstructions,
        project_instruction_read: Some(project_instruction_read),
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

fn adhoc_permissions(
    snapshot: &mut serde_json::Map<String, Value>,
    access: Option<AdhocAccess>,
) -> eliot_core::Result<()> {
    let (permissions, approval) = match access {
        Some(AdhocAccess::Ask) => (
            CodexPermissions::WorkspaceWrite,
            CodexApprovalPolicy::OnRequest,
        ),
        Some(AdhocAccess::Auto) => (CodexPermissions::WorkspaceWrite, CodexApprovalPolicy::Never),
        Some(AdhocAccess::Full) => (
            CodexPermissions::DangerFullAccess,
            CodexApprovalPolicy::Never,
        ),
        None => (CodexPermissions::default(), CodexApprovalPolicy::default()),
    };
    snapshot.insert("permissions".to_owned(), Value::from(permissions.as_str()));
    snapshot.insert("approval_policy".to_owned(), Value::from(approval.as_str()));
    Ok(())
}

fn project_instruction_read(
    path: &str,
    profile: &Value,
    size_bytes: Option<u64>,
) -> (InstructionReadStatus, Option<String>) {
    use InstructionReadStatus::{Documented, NotNative, Partial, Unknown};
    if !matches!(path, "AGENTS.md" | "AGENTS.override.md") {
        return (Unknown, None);
    }
    let limit = profile
        .as_object()
        .and_then(codex_project_doc_override)
        .unwrap_or(CODEX_DEFAULT_PROJECT_DOC_MAX_BYTES);
    let status = match limit {
        0 => NotNative,
        limit if size_bytes.is_some_and(|size| size > limit) => Partial,
        _ => Documented,
    };
    (status, None)
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
