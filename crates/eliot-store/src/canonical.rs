//! Canonical SHA-256 semantic digest for Store admission and batch ack.

//! Owning specs: `docs/spec/core.md` external mutation ids, `docs/spec/api.md` mutation ids,
//! `docs/spec/plugin-protocol.md` commit acknowledgement digests.
//! Donor: mature `serde_json` + `sha2` dependencies per `docs/reference/donors.md` L1-L2 map; no vendored code.

use serde_json::Value;

/// Compute SHA-256 (32 bytes) over deterministic canonical JSON.
///
/// Recursively sorts object keys; arrays and scalar values keep their semantics.
/// Caller passes only the semantic payload (no transport ids or timing).
pub(crate) fn digest(value: &Value) -> eliot_core::Result<Vec<u8>> {
    eliot_core::canonical_json_sha256(value)
        .map(|digest| digest.to_vec())
        .map_err(|err| eliot_core::Error::new("invalid", err.to_string()))
}

#[cfg(test)]
mod tests {
    use super::digest;
    use serde_json::json;

    #[test]
    fn store_digest_retains_semantic_payload_bytes() {
        let payload = json!({
            "verb": "send", "target": "a1", "expected_turn_id": null,
            "expected_generation": 0, "args": {"text": "literal e\u{301}\n"},
        });
        // Independently hashed compact UTF-8 JSON with sorted keys. Keep the
        // Store's persisted 32-byte representation, rather than a hex string.
        assert_eq!(
            digest(&payload).unwrap(),
            [
                0x6d, 0x5f, 0xaa, 0xa4, 0x75, 0xe9, 0xa3, 0x03, 0xe3, 0x20, 0x94, 0x7c, 0x95, 0x69,
                0x5d, 0xb6, 0x46, 0xa2, 0x31, 0x2d, 0xa6, 0xe8, 0x22, 0x60, 0xa4, 0xec, 0x65, 0x0a,
                0xbf, 0xac, 0x7b, 0xe2,
            ]
        );
    }
}
