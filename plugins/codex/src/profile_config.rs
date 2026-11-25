//! Named profile settings for this adapter.

use eliot_core::api::native_profile::{
    CollaborationMode, NativeProfileInput, NativeProfileMapping, NativeProfileResult,
    ProfileEffort, SandboxApprovalPolicy, SandboxPermissions,
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
        &CollaborationMode::ALL.map(CollaborationMode::as_str),
        "must be default or plan",
        "profile_error",
    )?;
    let permissions = validated_native_choice(
        "permissions",
        entry.permissions,
        &SandboxPermissions::ALL.map(SandboxPermissions::as_str),
        "unsupported permission mode",
        "unsupported",
    )?
    .unwrap_or(SandboxPermissions::default().as_str());
    let approval_policy = validated_native_choice(
        "approval_policy",
        entry.approval_policy,
        &SandboxApprovalPolicy::ALL.map(SandboxApprovalPolicy::as_str),
        "must be never or on-request",
        "profile_error",
    )?
    .unwrap_or(SandboxApprovalPolicy::default().as_str());
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

/// The Codex-specific fields of a frozen profile, decoded once at the plugin
/// boundary. Other keys of the snapshot are read by their own owners.
#[derive(Clone, Debug, Default, serde::Deserialize)]
pub(super) struct ProfileFields {
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub daemon_model: Option<String>,
    #[serde(default)]
    pub effort: Option<String>,
    #[serde(default)]
    pub collaboration_mode: Option<CollaborationMode>,
    #[serde(default)]
    pub permissions: SandboxPermissions,
    #[serde(default)]
    pub approval_policy: SandboxApprovalPolicy,
    #[serde(default)]
    pub native_subagents: Option<String>,
}
