use serde_json::Value;

use crate::{Error, Result, api::REDACTION_MARKER};

pub const UNCLASSIFIED_NATIVE_EVENT: &str = "session.native_unclassified";
const MAX_LABEL_BYTES: usize = 128;

pub fn is_native_discriminator(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_LABEL_BYTES
        && text.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':')
        })
}

/// A redaction marker can replace a single byte of a label at intake.
fn is_published_label(text: &str) -> bool {
    !text.is_empty()
        && text.len() <= MAX_LABEL_BYTES * REDACTION_MARKER.len()
        && text
            .split(REDACTION_MARKER)
            .all(|part| part.is_empty() || is_native_discriminator(part))
}

/// Fence diagnostic metadata before either controlled or observed publication.
pub fn validate_unclassified_native_data(data: &Value) -> Result<()> {
    if data.as_object().is_some_and(|object| {
        object
            .keys()
            .all(|field| matches!(field.as_str(), "site" | "native_tag" | "redacted"))
            && object
                .get("site")
                .and_then(Value::as_str)
                .is_some_and(is_published_label)
            && object
                .get("native_tag")
                .is_none_or(|tag| tag.is_null() || tag.as_str().is_some_and(is_published_label))
            && object.get("redacted").is_none_or(Value::is_boolean)
    }) {
        return Ok(());
    }
    Err(Error::new(
        "invalid",
        "unclassified native fact must contain only bounded diagnostic metadata",
    ))
}
