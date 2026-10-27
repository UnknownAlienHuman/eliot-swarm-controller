//! Canonical SHA-256 semantic digest for Store admission and batch ack.

//! Owning specs: `docs/spec/core.md` external mutation ids, `docs/spec/api.md` mutation ids,
//! `docs/spec/plugin-protocol.md` commit acknowledgement digests.
//! Donor: mature `serde_json` + `sha2` dependencies per `docs/reference/donors.md` L1-L2 map; no vendored code.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// Compute SHA-256 (32 bytes) over deterministic canonical JSON.
///
/// Recursively sorts object keys; arrays and scalar values keep their semantics.
/// Caller passes only the semantic payload (no transport ids or timing).
pub(crate) fn digest(value: &Value) -> eliot_core::Result<Vec<u8>> {
    let canonical = canonicalize(value);
    let bytes = serde_json::to_vec(&canonical)
        .map_err(|err| eliot_core::Error::new("invalid", err.to_string()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(hasher.finalize().to_vec())
}

fn canonicalize(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                if let Some(inner) = map.get(key) {
                    out.insert(key.clone(), canonicalize(inner));
                }
            }
            Value::Object(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonicalize).collect()),
        _ => value.clone(),
    }
}
