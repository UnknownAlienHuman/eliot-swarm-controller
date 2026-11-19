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
