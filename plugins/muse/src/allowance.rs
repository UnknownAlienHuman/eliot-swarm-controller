//! Muse subscription allowance identity, diagnostics and retained-source display.

use eliot_core::{Error, FailureOrigin};
use serde_json::Value;
use std::path::Path;

pub(crate) const STREAM_SOURCE: &str = "Muse MSP usage/changed";

/// Scope allowance reads to the effective file-backed sign-in without opening
/// a credential file. The daemon adds the generic harness identity prefix.
pub(crate) fn account_key(environment: &Value) -> Option<String> {
    let env = |name: &str| {
        environment
            .as_object()?
            .iter()
            .find(|(key, _)| {
                key.as_str() == name || (cfg!(windows) && key.eq_ignore_ascii_case(name))
            })?
            .1
            .as_str()
            .filter(|value| !value.trim().is_empty())
    };
    let config_home = match env("XDG_CONFIG_HOME") {
        Some(path) => Path::new(path).to_path_buf(),
        None => Path::new(env(if cfg!(windows) { "USERPROFILE" } else { "HOME" })?).join(".config"),
    };
    if !config_home.is_absolute() {
        return None;
    }
    // Canonicalize the directory when present to collapse aliases without
    // resolving or opening any credential file.
    let path = config_home.canonicalize().unwrap_or(config_home);
    let path = path.to_str()?;
    Some(if cfg!(windows) {
        path.to_lowercase()
    } else {
        path.to_owned()
    })
}

/// Translate only the adapter's explicit unavailable states, preserving the
/// existing failure origin and evidence text.
pub(crate) fn unavailable(code: &str, reason: &str) -> Option<Error> {
    match code {
        "auth_required" => Some(Error::new("auth_required", reason).with_origin(
            FailureOrigin::Environment,
            "Muse usage requires native sign-in",
        )),
        "quota_unavailable" => Some(
            eliot_plugin::quota::provider("Muse reported no subscription allowance")
                .with_message(reason),
        ),
        _ => None,
    }
}

pub(crate) fn source_note(details: &Value) -> &'static str {
    match details.get("source").and_then(Value::as_str) {
        Some(STREAM_SOURCE) => "Muse subscription usage observed during a turn",
        _ => "Muse subscription allowance from usage --json",
    }
}
