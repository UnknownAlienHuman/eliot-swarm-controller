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

#[cfg(test)]
mod tests {
    use super::ensure_store_pin_supported;
    use serde_json::{Value, json};

    fn pinned_config() -> Value {
        json!({
            "config": {"sqlite_home": "/frozen-store"},
            "origins": {
                "sqlite_home": {"name": {"type": "sessionFlags"}, "version": "pin"}
            },
            "layers": [{
                "name": {"type": "sessionFlags"},
                "version": "pin",
                "config": {"sqlite_home": "/frozen-store"}
            }]
        })
    }

    fn rejected(config: &Value, requirements: &Value) {
        let error = ensure_store_pin_supported(config, requirements).expect_err("unsupported pin");
        assert_eq!(error.code, "native_scope_unverified");
        assert!(!error.message.contains("/frozen-store"));
    }

    #[test]
    fn lower_layers_and_explicit_absence_allow_the_cli_pin_scheme() {
        let mut config = pinned_config();
        for kind in ["system", "user", "project", "mdm", "enterpriseManaged"] {
            let name = match kind {
                "system" | "user" => json!({"type": kind, "file": "/config.toml"}),
                "project" => json!({"type": kind, "dotCodexFolder": "/project/.codex"}),
                "mdm" => json!({"type": kind, "domain": "fixture", "key": "config"}),
                _ => json!({"type": kind, "id": "fixture", "name": "fixture"}),
            };
            config["layers"].as_array_mut().unwrap().push(json!({
                "name": name,
                "version": "lower",
                "config": {"sqlite_home": "/lower-store"}
            }));
        }
        ensure_store_pin_supported(&config, &json!({"requirements": null})).unwrap();
        ensure_store_pin_supported(
            &config,
            &json!({"requirements": {"sqliteHome": null, "allowLoginShell": false}}),
        )
        .unwrap();
        let probe = json!({
            "config": {"sqlite_home": null}, "origins": {},
            "layers": [{
                "name": {"type": "system", "file": "/config.toml"},
                "version": "empty-system", "config": {}
            }]
        });
        ensure_store_pin_supported(&probe, &json!({"requirements": null})).unwrap();
    }

    #[test]
    fn an_exact_root_requirement_is_unsupported_even_when_it_matches_the_pin() {
        rejected(
            &pinned_config(),
            &json!({"requirements": {"sqliteHome": "/frozen-store"}}),
        );
    }

    #[test]
    fn active_legacy_layers_cannot_hide_behind_matching_or_empty_config() {
        for kind in [
            "legacyManagedConfigTomlFromFile",
            "legacyManagedConfigTomlFromMdm",
        ] {
            for values in [json!({}), json!({"sqlite_home": "/frozen-store"})] {
                let mut config = pinned_config();
                let mut name = json!({"type": kind});
                if kind == "legacyManagedConfigTomlFromFile" {
                    name["file"] = json!("/managed_config.toml");
                }
                config["layers"].as_array_mut().unwrap().push(json!({
                    "name": name, "version": "managed", "config": values
                }));
                rejected(&config, &json!({"requirements": null}));
            }
        }
    }

    #[test]
    fn absent_or_unrecognized_policy_proof_is_not_absence_of_an_override() {
        let config = pinned_config();
        for requirements in [json!({}), json!({"requirements": {}})] {
            rejected(&config, &requirements);
        }
        for layers in [
            Value::Null,
            json!([]),
            json!([{
                "name": {"type": "futureManaged"}, "version": "unknown", "config": {}
            }]),
        ] {
            let mut config = pinned_config();
            config["layers"] = layers;
            rejected(&config, &json!({"requirements": null}));
        }
    }
}
