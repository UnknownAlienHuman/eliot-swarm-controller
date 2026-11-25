use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result, api::REDACTION_MARKER};

pub const UNCLASSIFIED_NATIVE_EVENT: &str = "session.native_unclassified";
pub const NATIVE_WARNING_EVENT: &str = "session.native_warning";
pub const NATIVE_WARNING_CONTENT_PREFIX: &str = "native-warning-content:";
pub const MAX_NATIVE_WARNING_BYTES: usize = 1024;
const MAX_LABEL_BYTES: usize = 128;

/// An advisory diagnostic for one exact native thread, never a verdict.
#[derive(schemars::JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NativeWarning {
    pub thread_id: String,
    pub code: String,
    pub count: u64,
    pub message: String,
    pub truncated: bool,
}

impl NativeWarning {
    /// Stable native warning identity uses redacted fields before display aggregation
    /// or truncation. The local display count is deliberately excluded.
    pub fn stable_id_for_fields(
        thread: &str,
        code: &str,
        message: &str,
        truncated: bool,
    ) -> Result<String> {
        let digest = crate::canonical_json_sha256(&serde_json::json!([
            thread, code, message, truncated,
        ]))
        .map_err(|_| Error::new("internal", "native warning identity could not be encoded"))?;
        let digest = digest
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(format!("{NATIVE_WARNING_CONTENT_PREFIX}{digest}"))
    }

    pub fn has_stable_id_shape(id: &str) -> bool {
        id.strip_prefix(NATIVE_WARNING_CONTENT_PREFIX)
            .is_some_and(|digest| crate::is_lowercase_hex(digest, 64))
    }

    /// A restarted producer may have a different display count or overflow bucket.
    pub fn matches_retained_diagnostic(&self, other: &Self) -> bool {
        self.thread_id == other.thread_id
            && (self.code == other.code || self.code == "other" || other.code == "other")
            && self.message == other.message
            && self.truncated == other.truncated
    }

    pub fn validate_data(data: &Value, thread: Option<&str>) -> Result<()> {
        let warning: Self = serde_json::from_value(data.clone())
            .map_err(|_| Error::new("invalid", "native warning schema is invalid"))?;
        warning.validate_for_thread(thread)
    }

    pub fn validate_for_thread(&self, thread: Option<&str>) -> Result<()> {
        if self.thread_id.is_empty() || Some(self.thread_id.as_str()) != thread {
            return Err(Error::new(
                "source_integrity",
                "native warning names another thread",
            ));
        }
        if !is_published_label(&self.code)
            || self.count == 0
            || self.message.len() > MAX_NATIVE_WARNING_BYTES
        {
            return Err(Error::new(
                "invalid",
                "native warning exceeds its diagnostic bounds",
            ));
        }
        Ok(())
    }
}

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
        && has_only_published_label_parts(text)
}

fn has_only_published_label_parts(mut text: &str) -> bool {
    while let Some((head, suffix)) = text.split_once('[') {
        let Some((marker, rest)) = suffix.split_once(']') else {
            return false;
        };
        if (!head.is_empty() && !is_native_discriminator(head)) || !is_marker(marker) {
            return false;
        }
        text = rest;
    }
    text.is_empty() || is_native_discriminator(text)
}

fn is_marker(marker: &str) -> bool {
    if marker == "REDACTED" {
        return true;
    }
    marker
        .strip_prefix("REDACTED:")
        .and_then(|parts| parts.split_once(':'))
        .is_some_and(|(kind, len)| {
            !kind.is_empty()
                && kind
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
                && len.parse::<usize>().is_ok_and(|length| length > 0)
        })
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
