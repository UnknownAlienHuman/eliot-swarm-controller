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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn hex(value: &Value) -> String {
        canonical_json_sha256(value)
            .unwrap()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect()
    }

    #[test]
    fn golden_digest_retains_nested_unicode_numbers_and_windows_path_bytes() {
        assert_eq!(
            hex(&json!({})),
            "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );
        let value = json!({
            "z": null,
            "text": "e\u{301}\n",
            "path": r"\\?\C:\repo\note.txt",
            "a": [{"z": true, "a": "é"}, 1, 1.0, -0.0],
        });
        // Independently hashed UTF-8 literal, not a second canonicalizer:
        // {"a":[{"a":"é","z":true},1,1.0,-0.0],"path":"\\\\?\\C:\\repo\\note.txt","text":"é\n","z":null}
        assert_eq!(
            hex(&value),
            "f08cfc9410723a53e6911dd9c9763d675cb1fa1828ac4ac620d4ce4046409235"
        );
    }

    #[test]
    fn object_order_is_irrelevant_but_array_order_remains_semantic() {
        let first: Value = serde_json::from_str(r#"{"z":{"b":2,"a":1},"a":[2,1]}"#).unwrap();
        let reordered: Value = serde_json::from_str(r#"{"a":[2,1],"z":{"a":1,"b":2}}"#).unwrap();
        assert_eq!(
            canonical_json_sha256(&first).unwrap(),
            canonical_json_sha256(&reordered).unwrap()
        );
        let changed = json!({"a":[1,2],"z":{"a":1,"b":2}});
        assert_ne!(
            canonical_json_sha256(&first).unwrap(),
            canonical_json_sha256(&changed).unwrap()
        );
    }

    #[test]
    fn golden_scalars_preserve_float_and_unicode_representations() {
        for (value, expected) in [
            (
                json!(1),
                "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
            ),
            (
                json!(1.0),
                "d0ff5974b6aa52cf562bea5921840c032a860a91a3512f7fe8f768f6bbe005f6",
            ),
            (
                json!(-0.0),
                "c26617c7ccbcaa6631b45d851b8cf56e21d2ca624bdb1193afdbd4b560702cec",
            ),
            (
                json!(0.0),
                "8aed642bf5118b9d3c859bd4be35ecac75b6e873cce34e7b6f554b06f75550d7",
            ),
            (
                json!("é"),
                "f2886017e9c7abacf804b54d64787dce2b611c9544ba21f3affdd126a6e50086",
            ),
            (
                json!("e\u{301}"),
                "3d68ce21f2899a475713cdbe7562ba9bdb6b1dfde8af1f221bdff4a0935b53b2",
            ),
        ] {
            assert_eq!(hex(&value), expected);
        }
    }
}
