//! Named profile settings for this adapter.

use eliot_core::api::native_profile::{
    NativeProfileInput, NativeProfileMapping, NativeProfileResult, ProfileEffort,
    SandboxApprovalPolicy, SandboxPermissions,
};
use eliot_plugin::capabilities::reject_unmapped_native_fields;
use serde_json::{Map as JsonMap, Value};

pub(super) const MAPPING: NativeProfileMapping = NativeProfileMapping {
    name: "Claude",
    model: None,
    effort: ProfileEffort::Choices {
        allowed: CLAUDE_EFFORT_LEVELS,
        refusal: "unknown Claude effort level (low, medium, high, xhigh, or max)",
    },
    fields,
    route: None,
    mcp_minimal: true,
    mcp_allow: false,
    control_before_mcp: true,
};

use eliot_plugin::capabilities::validated_native_choice;

fn fields(
    entry: &NativeProfileInput<'_>,
    snapshot: &mut JsonMap<String, Value>,
) -> NativeProfileResult<()> {
    let permission_mode = validated_native_choice(
        "approval",
        entry.approval.filter(|value| !value.trim().is_empty()),
        &CLAUDE_PERMISSION_MODES,
        "unknown Claude permission mode (acceptEdits, auto, bypassPermissions, manual, dontAsk, or plan)",
        "profile_error",
    )?;
    reject_unmapped_native_fields(&[(
        "collaboration_mode",
        entry.collaboration_mode.is_some(),
        "the Claude slice has no collaboration preset mapping",
    )])?;
    validated_native_choice(
        "permissions",
        entry.permissions,
        &[SandboxPermissions::WorkspaceWrite.as_str()],
        "the Claude slice runs the CLI-default permission posture",
        "unsupported",
    )?;
    validated_native_choice(
        "approval_policy",
        entry.approval_policy,
        &[SandboxApprovalPolicy::Never.as_str()],
        "the Claude slice leaves approvals unconnected",
        "unsupported",
    )?;
    reject_unmapped_native_fields(&[
        (
            "mode",
            entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
            "the Claude slice has no mapping for this preset",
        ),
        (
            "auth_integration",
            entry.auth_integration.is_some(),
            "the Claude slice has no mapping for this preset",
        ),
    ])?;
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(SandboxPermissions::WorkspaceWrite.as_str().to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(SandboxApprovalPolicy::Never.as_str().to_owned()),
    );
    if let Some(mode) = permission_mode {
        snapshot.insert("permission_mode".to_owned(), Value::String(mode.to_owned()));
    }
    Ok(())
}

/// Claude Code `--permission-mode` values (profile key `approval`).
pub const CLAUDE_PERMISSION_MODES: [&str; 6] = [
    "manual",
    "auto",
    "acceptEdits",
    "plan",
    "dontAsk",
    "bypassPermissions",
];

/// Claude Code `--effort` levels: the CLI's closed vocabulary.
pub const CLAUDE_EFFORTS: [&str; 5] = ["low", "medium", "high", "xhigh", "max"];

pub const CLAUDE_EFFORT_LEVELS: &[&str] = &CLAUDE_EFFORTS;
