//! Codex native adapter capability declaration.

use eliot_core::api::harness::{
    AdhocEffort, LaunchDefaultPolicy, NativeAccessBounds, NativeAuthCapability, NativeAuthCheck,
    NativeAuthCommand, NativeAuthInvocation, NativeAuthState, NativeQuotaReadback, NativeRoute,
    ProfileCapabilities,
};
use eliot_core::api::native_profile::NativeSubagentsPolicy;
use eliot_core::api::{HarnessCapabilities, InputTokenAccounting};
use eliot_core::{InjectPoint, McpPolicy};
use eliot_plugin::capabilities::{adhoc_launch, quota_readback, sandbox_permissions};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        instruction_inventory: crate::instructions::INSTRUCTION_INVENTORY,
        native_auth: Some(NativeAuthCapability {
            login: Some(NativeAuthCommand {
                resolve: || auth_command(&["login"]),
            }),
            signed_in_check: Some(NativeAuthCheck {
                command: NativeAuthCommand {
                    resolve: || auth_command(&["login", "status"]),
                },
                classify: classify_auth_status,
            }),
        }),
        native_route: NativeRoute::ResponsesProxy {
            reserved_providers: &[
                "openai",
                "amazon-bedrock",
                "amazon-bedrock-runtime",
                "ollama",
                "lmstudio",
                "ollama-chat",
            ],
        },
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
                crate::discovery::quota_evidence,
                crate::discovery::quota_usage_windows,
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
        project_instruction_read: crate::instructions::PROJECT_READ,
        user_instruction_files: crate::instructions::USER_FILES,
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

fn auth_command(args: &[&str]) -> Option<NativeAuthInvocation> {
    use eliot_plugin::install::{find_on_path, native_exe_name};

    let exe = find_on_path(&native_exe_name("codex")).filter(|path| path.is_absolute())?;
    Some((exe, args.iter().copied().map(Into::into).collect()))
}

fn classify_auth_status(code: Option<i32>, stdout: &[u8], stderr: &[u8]) -> NativeAuthState {
    let has_line = |predicate: fn(&str) -> bool| {
        [stdout, stderr]
            .into_iter()
            .filter_map(|bytes| std::str::from_utf8(bytes).ok())
            .any(|text| text.lines().map(str::trim).any(predicate))
    };
    match code {
        Some(0) if has_line(|line| line.starts_with("Logged in")) => NativeAuthState::SignedIn,
        Some(1) if has_line(|line| line.eq_ignore_ascii_case("Not logged in")) => {
            NativeAuthState::SignedOut
        }
        _ => NativeAuthState::Unknown,
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
