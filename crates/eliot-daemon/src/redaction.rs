//! Pure redaction helper for Codex native output callers.
//!
//! The caller supplies a fixed per-source secret snapshot (owner, Agent,
//! plugin credentials) and redacts immediately before the Store commit, so
//! acknowledgement digests stay stable. This module performs no I/O, env
//! reads, logging, or global state.

use serde_json::Value;

/// Marker substituted for each nonempty known secret.
const MASK: &str = "[REDACTED]";

pub(crate) fn redact_text(text: &str, secrets: &[String]) -> String {
    let mut out = text.to_owned();
    for secret in secrets {
        if secret.is_empty() {
            continue;
        }
        if out.contains(secret) {
            out = out.replace(secret, MASK);
        }
    }
    out
}

pub(crate) fn redact_value(value: &mut Value, secrets: &[String]) -> bool {
    match value {
        Value::String(s) => {
            let redacted = redact_text(s, secrets);
            if redacted != *s {
                *s = redacted;
                true
            } else {
                false
            }
        }
        Value::Array(items) => {
            let mut changed = false;
            for item in items.iter_mut() {
                changed |= redact_value(item, secrets);
            }
            changed
        }
        Value::Object(map) => {
            let mut changed = false;
            for v in map.values_mut() {
                changed |= redact_value(v, secrets);
            }
            changed
        }
        _ => false,
    }
}
