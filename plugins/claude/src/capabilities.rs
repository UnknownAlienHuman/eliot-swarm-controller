//! Claude native adapter capability declaration.

use eliot_core::api::harness::{
    AdhocAccess, AdhocEffort, AdhocLaunchDefaults, NativeAccessBounds, NativeQuotaReadback,
    QuotaProjectionFormat, UserInstructionFiles,
};
use eliot_core::api::native_profile::{
    CLAUDE_EFFORT_LEVELS, CodexApprovalPolicy, CodexPermissions,
};
use eliot_core::api::{
    CLAUDE_EXTERNAL_ROUTE_MARKERS, ConfigureSessionMode, HarnessCapabilities, InputTokenAccounting,
    InstructionReadStatus,
};
use serde_json::Value;

pub fn declaration() -> HarnessCapabilities {
    HarnessCapabilities {
        quota_readback: Some(NativeQuotaReadback {
            projection: QuotaProjectionFormat::Windows,
            method: "quota.read",
            description: |_| "Claude CLI OAuth usage; provider read-only values",
            unsupported_note: None,
            route_refusal: Some(external_quota_route),
        }),
        adhoc_effort: Some(AdhocEffort::Choices {
            values: CLAUDE_EFFORT_LEVELS,
            refusal: "unknown Claude effort level (low, medium, high, xhigh, or max)",
        }),
        adhoc_launch: Some(AdhocLaunchDefaults {
            permissions: adhoc_permissions,
            mcp: eliot_core::McpPolicy::Minimal,
            pass_env: &["CLAUDE_CONFIG_DIR"],
            installed_profile_mcp: eliot_core::McpPolicy::Minimal,
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
        project_instruction_read: Some(project_instruction_read),
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
    environment.as_object()?.iter().any(|(name, value)| {
        value.as_str().is_some_and(|text| !text.is_empty())
            && CLAUDE_EXTERNAL_ROUTE_MARKERS.iter().any(|marker| {
                name.as_str() == *marker || (cfg!(windows) && name.eq_ignore_ascii_case(marker))
            })
    }).then_some("Claude profile route is external or ambiguous; CLI OAuth quota would describe an unverified route")
}

fn adhoc_permissions(
    snapshot: &mut serde_json::Map<String, Value>,
    access: Option<AdhocAccess>,
) -> eliot_core::Result<()> {
    snapshot.insert(
        "permissions".to_owned(),
        Value::from(CodexPermissions::default().as_str()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::from(CodexApprovalPolicy::default().as_str()),
    );
    if let Some(access) = access {
        let mode = match access {
            AdhocAccess::Ask => "manual",
            AdhocAccess::Auto => "auto",
            AdhocAccess::Full => "bypassPermissions",
        };
        snapshot.insert("permission_mode".to_owned(), Value::from(mode));
    }
    Ok(())
}

fn project_instruction_read(
    path: &str,
    profile: &Value,
    _: Option<u64>,
) -> (InstructionReadStatus, Option<String>) {
    use InstructionReadStatus::{Conditional, Documented, Unknown};
    let status = match path {
        "AGENTS.md" => Conditional,
        "CLAUDE.md" | "CLAUDE.local.md" | ".claude/CLAUDE.md" => Documented,
        ".claude/CLAUDE.local.md" => Unknown,
        _ => return (Unknown, None),
    };
    let version = profile
        .get("cmd_version")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    (status, version)
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
