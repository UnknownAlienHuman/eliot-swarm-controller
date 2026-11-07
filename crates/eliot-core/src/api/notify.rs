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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
        let encoded =
            serde_json::to_vec(self).map_err(|_| invalid("notify arguments cannot be encoded"))?;
        if encoded.len() > MAX_EVENT_BYTES {
            return Err(invalid(
                "notify arguments exceed the 64 KiB persisted-event bound",
            ));
        }
        Ok(())
    }
}

/// A named local user event: opaque bounded data persisted as `x.user.<topic>`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EmitDefinitionArgs {
    pub topic: String,
    pub data: Value,
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
        let encoded =
            serde_json::to_vec(&self.data).map_err(|_| invalid("emit data cannot be encoded"))?;
        if encoded.len() > MAX_EVENT_BYTES {
            return Err(invalid(
                "emit data exceeds the 64 KiB persisted-event bound",
            ));
        }
        Ok(())
    }
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
