//! Shared byte-preserving extraction of Store canonical.rs and Codex discovery's
//! fingerprint hashing. Callers retain semantic payloads, domains and error mapping.
//! Uses the existing workspace serde_json/sha2 dependencies; no upstream source copied.

use serde_json::Value;
use sha2::{Digest, Sha256};

/// SHA-256 of compact serde_json output after recursively sorting object keys.
/// Arrays and scalar representations are unchanged. This is not RFC 8785.
pub fn canonical_json_sha256(value: &Value) -> Result<[u8; 32], serde_json::Error> {
    let bytes = serde_json::to_vec(&canonicalize(value))?;
    Ok(Sha256::digest(bytes).into())
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
