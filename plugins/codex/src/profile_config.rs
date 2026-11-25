//! Named profile settings for this adapter.

use eliot_core::api::native_profile::{
    CodexApprovalPolicy, CodexCollaborationMode, CodexPermissions, NativeProfileInput,
    NativeProfileMapping, NativeProfileResult, ProfileEffort,
};
use eliot_plugin::capabilities::reject_unmapped_native_fields;
use serde_json::{Map as JsonMap, Value, json};

pub(super) const MAPPING: NativeProfileMapping = NativeProfileMapping {
    name: "Codex",
    model: None,
    effort: ProfileEffort::Freeform,
    fields,
    route: None,
    mcp_minimal: true,
    mcp_allow: true,
    control_before_mcp: false,
};

use eliot_plugin::capabilities::validated_native_choice;

fn fields(
    entry: &NativeProfileInput<'_>,
    snapshot: &mut JsonMap<String, Value>,
) -> NativeProfileResult<()> {
    let collaboration_mode = validated_native_choice(
        "collaboration_mode",
        entry.collaboration_mode,
        &CodexCollaborationMode::ALL.map(CodexCollaborationMode::as_str),
        "must be default or plan",
        "profile_error",
    )?;
    let permissions = validated_native_choice(
        "permissions",
        entry.permissions,
        &CodexPermissions::ALL.map(CodexPermissions::as_str),
        "unsupported permission mode",
        "unsupported",
    )?
    .unwrap_or(CodexPermissions::default().as_str());
    let approval_policy = validated_native_choice(
        "approval_policy",
        entry.approval_policy,
        &CodexApprovalPolicy::ALL.map(CodexApprovalPolicy::as_str),
        "must be never or on-request",
        "profile_error",
    )?
    .unwrap_or(CodexApprovalPolicy::default().as_str());
    reject_unmapped_native_fields(&[
        (
            "approval",
            entry.approval.is_some(),
            "the Codex slice has no approval mapping; use approval_policy",
        ),
        (
            "mode",
            entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
            "mode and dangerously_skip_permissions have no Codex mapping",
        ),
        (
            "auth_integration",
            entry.auth_integration.is_some(),
            "auth_integration has no Codex mapping",
        ),
    ])?;
    if let Some(limit) = entry.project_doc_max_bytes {
        snapshot.insert("project_doc_max_bytes".to_owned(), json!(limit));
    }
    if let Some(collaboration_mode) = collaboration_mode {
        snapshot.insert(
            "collaboration_mode".to_owned(),
            Value::String(collaboration_mode.to_owned()),
        );
    }
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(permissions.to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(approval_policy.to_owned()),
    );
    Ok(())
}
