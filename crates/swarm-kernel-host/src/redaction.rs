//! Selected Atlas donor, invoked before retaining native diagnostic payloads.
//! Explicit identity fields stay outside this scrubbed value. This is detection,
//! not a guarantee that an unknown secret format can never pass through.
pub(crate) fn value(input: serde_json::Value) -> serde_json::Value {
    let output = atlas_redact::redact_json(&input.to_string());
    serde_json::from_str(&output.text).unwrap_or_else(
        |_| serde_json::json!({"redacted":true,"reason":"invalid_redaction_output"}),
    )
}

/// Redact the existing producer's bounded human-readable status summary with
/// Atlas before `swarm-telemetry` serializes or queues it.
pub(crate) fn diagnostic_text(input: &str) -> Option<String> {
    Some(atlas_redact::redact_auto(input).text)
}
