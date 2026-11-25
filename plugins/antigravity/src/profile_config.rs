//! Named profile settings for this adapter.

use eliot_core::api::native_profile::{
    NativeProfileInput, NativeProfileMapping, NativeProfileResult, ProfileEffort,
};
use eliot_plugin::capabilities::reject_unmapped_native_fields;
use serde_json::{Map as JsonMap, Value};

pub(super) const MAPPING: NativeProfileMapping = NativeProfileMapping {
    name: "Antigravity",
    model: None,
    effort: ProfileEffort::Adapter,
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
    reject_unmapped_native_fields(&[(
        "effort",
        entry.effort.is_some_and(|effort| !effort.trim().is_empty()),
        "the Antigravity slice has no reasoning-effort mapping",
    )])?;
    let mode = validated_native_choice(
        "mode",
        entry.mode,
        &ANTIGRAVITY_MODES,
        "must be plan, auto, or accept-edits",
        "profile_error",
    )?;
    let no_mapping = "the Antigravity slice has no mapping for this preset";
    reject_unmapped_native_fields(&[
        ("approval", entry.approval.is_some(), no_mapping),
        (
            "collaboration_mode",
            entry.collaboration_mode.is_some(),
            no_mapping,
        ),
        ("permissions", entry.permissions.is_some(), no_mapping),
        (
            "approval_policy",
            entry.approval_policy.is_some(),
            no_mapping,
        ),
        (
            "auth_integration",
            entry.auth_integration.is_some(),
            no_mapping,
        ),
    ])?;
    if let Some(mode) = mode {
        snapshot.insert("mode".to_owned(), Value::String(mode.to_owned()));
    }
    snapshot.insert(
        "dangerously_skip_permissions".to_owned(),
        Value::Bool(entry.dangerously_skip_permissions.unwrap_or(false)),
    );
    Ok(())
}

/// Antigravity `mode` values.
pub const ANTIGRAVITY_MODES: [&str; 3] = ["plan", "auto", "accept-edits"];
