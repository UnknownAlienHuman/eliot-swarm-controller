//! Named profile settings for this adapter.

use eliot_core::api::native_profile::{
    NativeProfileInput, NativeProfileMapping, NativeProfileResult, ProfileEffort,
    SandboxApprovalPolicy, SandboxPermissions,
};
use eliot_plugin::capabilities::reject_unmapped_native_fields;
use serde_json::{Map as JsonMap, Value};

pub(super) const MAPPING: NativeProfileMapping = NativeProfileMapping {
    name: "Muse",
    model: None,
    effort: ProfileEffort::Freeform,
    fields,
    route: None,
    mcp_minimal: false,
    mcp_allow: false,
    control_before_mcp: false,
};

use eliot_plugin::capabilities::validated_native_choice;

fn fields(
    entry: &NativeProfileInput<'_>,
    snapshot: &mut JsonMap<String, Value>,
) -> NativeProfileResult<()> {
    let approval = validated_native_choice(
        "approval",
        entry.approval,
        &MUSE_APPROVALS,
        "must be allowAll, promptUnmatched, onRequest or denyUnmatched",
        "profile_error",
    )?;
    reject_unmapped_native_fields(&[(
        "collaboration_mode",
        entry.collaboration_mode.is_some(),
        "the Muse slice has no collaboration preset mapping",
    )])?;
    let permissions = validated_native_choice(
        "permissions",
        entry.permissions,
        &MUSE_PERMISSIONS.map(SandboxPermissions::as_str),
        "the Muse slice supports only workspace-write or danger-full-access",
        "unsupported",
    )?
    .unwrap_or(SandboxPermissions::default().as_str());
    validated_native_choice(
        "approval_policy",
        entry.approval_policy,
        &[SandboxApprovalPolicy::Never.as_str()],
        "the Muse slice leaves approvals unconnected",
        "unsupported",
    )?;
    reject_unmapped_native_fields(&[
        (
            "mode",
            entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
            "the Muse slice has no mapping for this preset",
        ),
        (
            "auth_integration",
            entry.auth_integration.is_some(),
            "the Muse slice has no mapping for this preset",
        ),
    ])?;
    if let Some(approval) = approval {
        snapshot.insert("approval".to_owned(), Value::String(approval.to_owned()));
    }
    snapshot.insert(
        "permissions".to_owned(),
        Value::String(permissions.to_owned()),
    );
    snapshot.insert(
        "approval_policy".to_owned(),
        Value::String(SandboxApprovalPolicy::Never.as_str().to_owned()),
    );
    Ok(())
}

/// Muse `approval` modes.
pub const MUSE_APPROVALS: [&str; 4] = ["onRequest", "promptUnmatched", "denyUnmatched", "allowAll"];

/// Muse `permissions`: the subset of the Codex sandbox modes Muse maps.
pub const MUSE_PERMISSIONS: [SandboxPermissions; 2] = [
    SandboxPermissions::WorkspaceWrite,
    SandboxPermissions::DangerFullAccess,
];
