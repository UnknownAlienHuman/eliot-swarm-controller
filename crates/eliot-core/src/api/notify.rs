//! Typed payloads for the local notify/emit actions.
//!
//! These DTOs are pure data. Daemon admission, Store commit, bus/log
//! publication, and settlement belong to callers. Both actions persist one
//! local `x.user.*` event and a standard command readback; neither sends an
//! external message or webhook.

use crate::limits::MAX_EVENT_BYTES;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A bounded local notification: a human message persisted as `x.user.notify`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NotifyDefinitionArgs {
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
}

impl NotifyDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
        validate_text(&self.message, "notify message")?;
        if let Some(title) = &self.title {
            validate_text(title, "notify title")?;
        }
        // Bound the final persisted event payload shape (`{message,title?}`),
        // not just the DTO, so Store never rejects it at commit time.
        let mut payload = serde_json::json!({"message": self.message});
        if let Some(title) = &self.title {
            payload["title"] = Value::String(title.clone());
        }
        bound_payload(&payload, "notify")?;
        Ok(())
    }
}

/// A named local user event: bounded payload persisted as `x.user.<topic>`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmitDefinitionArgs {
    pub topic: String,
    pub data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-maxBytes" = 128), description = "Optional nonblank event key, at most 128 UTF-8 bytes.")]
    pub key: Option<String>,
}

impl EmitDefinitionArgs {
    pub fn validate(&self) -> Result<()> {
        if self.topic.is_empty()
            || self.topic.len() > 128
            || !self
                .topic
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        {
            return Err(invalid("emit topic must be a bounded dotted name"));
        }
        if let Some(key) = &self.key
            && (key.trim().is_empty() || key.len() > 128)
        {
            return Err(invalid(
                "emit key must be nonblank and at most 128 UTF-8 bytes",
            ));
        }
        // Bound the complete persisted event object, including the optional key.
        let mut payload = serde_json::json!({"data": self.data});
        if let Some(key) = &self.key {
            payload["key"] = Value::String(key.clone());
        }
        bound_payload(&payload, "emit")?;
        Ok(())
    }
}

fn bound_payload(payload: &Value, what: &str) -> Result<()> {
    let encoded = serde_json::to_vec(payload)
        .map_err(|_| invalid(format!("{what} payload cannot be encoded")))?;
    if encoded.len() > MAX_EVENT_BYTES {
        return Err(invalid(format!(
            "{what} persisted payload exceeds the 64 KiB event bound"
        )));
    }
    Ok(())
}

fn validate_text(value: &str, what: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(invalid(format!("{what} must be a nonempty string")));
    }
    if value.len() > MAX_EVENT_BYTES {
        return Err(invalid(format!(
            "{what} exceeds the 64 KiB persisted-event bound"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(invalid(format!("{what} cannot contain control characters")));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new("invalid", message)
}
