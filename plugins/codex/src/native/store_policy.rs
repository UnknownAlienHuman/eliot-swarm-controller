//! Policy support check for a reader launched with an absolute CLI sqlite pin.
//!
//! Behavior reference: Codex 979011409de0a60b52f179721948e65531d26144,
//! config/src/config_layer_source.rs:33-49 and core/src/config/requirements.rs:13-100.
//! Exact requirements replace CLI values with a warning; legacy file/MDM layers
//! also outrank session flags. No vendor implementation is copied here.

use eliot_core::{Error, Result};
use serde_json::Value;

/// Check raw replies to `config/read` with `includeLayers: true` and the actual
/// absolute reader cwd, and to `configRequirements/read` with empty params.
///
/// Call for both the selector probe and the pinned query reader. The caller must
/// still verify the query's home, canonical root and session-flags origin before
/// listing. These reloadable snapshots do not attest the process's startup store:
/// transient managed overrides between startup and readback remain unsupported.
pub(crate) fn ensure_store_pin_supported(
    config_read: &Value,
    requirements_read: &Value,
) -> Result<()> {
    let config = config_read
        .get("config")
        .and_then(Value::as_object)
        .ok_or_else(unverified)?;
    if !matches!(
        config.get("sqlite_home"),
        Some(Value::Null | Value::String(_))
    ) {
        return Err(unverified());
    }
    let origins = config_read
        .get("origins")
        .and_then(Value::as_object)
        .ok_or_else(unverified)?;
    let layers = config_read
        .get("layers")
        .and_then(Value::as_array)
        .filter(|layers| !layers.is_empty())
        .ok_or_else(unverified)?;

    // Pinned config_processor.rs:569 maps the exact requirement, and v2/config.rs
    // serializes sqliteHome without skip_serializing_if. Missing is not None.
    match requirements_read.get("requirements") {
        Some(Value::Null) => {}
        Some(Value::Object(requirements))
            if requirements.get("sqliteHome") == Some(&Value::Null) => {}
        _ => return Err(unverified()),
    }

    for layer in layers {
        let higher = higher_than_pin(layer)?;
        if !layer.get("config").is_some_and(Value::is_object) {
            return Err(unverified());
        }
        let active = match layer.get("disabledReason") {
            None | Some(Value::Null) => true,
            Some(Value::String(_)) => false,
            _ => return Err(unverified()),
        };
        // Reject even an empty active legacy layer: its source can replace the
        // pin if it acquires sqlite_home before the query process starts.
        if active && higher {
            return Err(unverified());
        }
    }
    if let Some(origin) = origins.get("sqlite_home")
        && higher_than_pin(origin)?
    {
        return Err(unverified());
    }
    Ok(())
}

fn higher_than_pin(metadata: &Value) -> Result<bool> {
    if metadata
        .get("version")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        return Err(unverified());
    }
    match metadata.pointer("/name/type").and_then(Value::as_str) {
        Some("legacyManagedConfigTomlFromFile" | "legacyManagedConfigTomlFromMdm") => Ok(true),
        // MDM/enterprise config layers are below session flags (0/15 < 30).
        // Their separate exact requirements were checked above.
        Some(
            "packagedDefaults" | "mdm" | "system" | "enterpriseManaged" | "user" | "project"
            | "sessionFlags",
        ) => Ok(false),
        _ => Err(unverified()),
    }
}

fn unverified() -> Error {
    Error::new(
        "native_scope_unverified",
        "Codex store pin policy is unverified or unsupported",
    )
}
