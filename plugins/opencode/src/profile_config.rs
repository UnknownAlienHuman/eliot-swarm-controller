//! Named profile settings for this adapter.

use crate::local_provider::LocalProvider;
use eliot_core::api::native_profile::{
    NativeProfileInput, NativeProfileMapping, NativeProfileRefusal, NativeProfileResult,
    ProfileEffort,
};
use eliot_core::{Error, Result};
use eliot_plugin::capabilities::reject_unmapped_native_fields;
use serde_json::{Map as JsonMap, Value};

pub(super) const MAPPING: NativeProfileMapping = NativeProfileMapping {
    name: "OpenCode",
    model: Some(validate_model),
    effort: ProfileEffort::Freeform,
    fields,
    route: Some(route),
    mcp_minimal: true,
    mcp_allow: false,
    control_before_mcp: false,
};

use eliot_plugin::session::split_provider_model;

const MAX_AUTH_INTEGRATION_LEN: usize = 128;

fn fields(
    entry: &NativeProfileInput<'_>,
    _snapshot: &mut JsonMap<String, Value>,
) -> NativeProfileResult<()> {
    let no_mapping = "the OpenCode slice has no mapping for this preset";
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
            "mode",
            entry.mode.is_some() || entry.dangerously_skip_permissions.is_some(),
            no_mapping,
        ),
    ])?;
    if let Some(selector) = entry.auth_integration {
        let trimmed = selector.trim();
        if trimmed.is_empty()
            || trimmed.len() > MAX_AUTH_INTEGRATION_LEN
            || trimmed.starts_with('-')
            || trimmed.chars().any(|value| value.is_control())
        {
            return Err(NativeProfileRefusal::new(
                "auth_integration",
                "must be a non-empty integration ID or name of at most 128 bytes with no control characters and no leading dash",
                "profile_error",
            ));
        }
    }
    Ok(())
}

fn route(
    entry: &NativeProfileInput<'_>,
    pass_env: &[String],
    snapshot: &mut JsonMap<String, Value>,
) -> NativeProfileResult<()> {
    if let Some(payload) = entry.local_provider {
        let field_error =
            |field, message: &str| NativeProfileRefusal::new(field, message, "profile_error");
        let provider = crate::local_provider::decode_and_validate(payload, pass_env)
            .map_err(|message| field_error("local_provider", &message))?;
        if !entry
            .model
            .and_then(split_provider_model)
            .is_some_and(|(id, model)| provider.contains_model(id, model))
        {
            return Err(field_error(
                "model",
                "a local provider profile must select one of its declared models",
            ));
        }
        if entry.auth_integration.is_some() {
            return Err(field_error(
                "auth_integration",
                "a local provider profile cannot name a canonical auth_integration",
            ));
        }
        let frozen = serde_json::to_value(provider)
            .map_err(|_| field_error("local_provider", "could not encode local provider"))?;
        snapshot.insert("local_provider".to_owned(), frozen);
    } else if let Some(selector) = entry.auth_integration {
        snapshot.insert(
            "auth_integration".to_owned(),
            Value::String(selector.trim().to_owned()),
        );
    }
    Ok(())
}

fn validate_model(model: &str) -> NativeProfileResult<()> {
    if split_provider_model(model).is_none() {
        return Err(NativeProfileRefusal::new(
            "model",
            "must be provider/model with non-empty slash segments and no variant suffix",
            "profile_error",
        ));
    }
    Ok(())
}

pub(super) fn launch_model_stays_on_route(
    entry: &NativeProfileInput<'_>,
    selected: &str,
) -> Result<bool> {
    let local = entry
        .local_provider
        .map(LocalProvider::decode)
        .transpose()
        .map_err(|_| Error::new("profile_error", "invalid frozen local provider"))?;
    let Some((provider, model)) = split_provider_model(selected) else {
        return Ok(false);
    };
    if let Some(local) = local {
        return Ok(local.contains_model(provider, model));
    }
    Ok(entry
        .model
        .and_then(split_provider_model)
        .is_some_and(|(configured, _)| configured == provider))
}
