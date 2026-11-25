use eliot_core::{
    Error, MAX_NATIVE_WARNING_BYTES, NATIVE_WARNING_EVENT, NativeWarning, Result, SecretPolicy,
    is_native_discriminator,
};
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;

const MAX_WARNING_CODES: usize = 16;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Notification {
    thread_id: Option<String>,
    code: Option<String>,
    message: String,
}

#[derive(Default)]
pub(super) struct Warnings {
    counts: HashMap<String, u64>,
    sequence: u64,
}

impl Warnings {
    pub(super) fn observe(
        &mut self,
        thread: &str,
        params: Value,
        policy: &SecretPolicy,
    ) -> Result<Option<(String, Value)>> {
        let Ok(notification) = serde_json::from_value::<Notification>(params) else {
            return Ok(None);
        };
        if notification.thread_id.as_deref() != Some(thread) || notification.message.is_empty() {
            return Ok(None);
        }
        // The 0.167.1 wire schema supplies no structured warning code.
        let mut code = notification
            .code
            .as_deref()
            .filter(|code| is_native_discriminator(code) && !policy.contains_text(code))
            .unwrap_or("warning");
        if !self.counts.contains_key(code) && self.counts.len() >= MAX_WARNING_CODES - 1 {
            code = "other";
        }
        let count = self.counts.entry(code.to_owned()).or_default();
        *count = count.saturating_add(1);
        let mut message = policy.redact_text(&notification.message);
        let truncated = message.len() > MAX_NATIVE_WARNING_BYTES;
        if truncated {
            message.truncate(message.floor_char_boundary(MAX_NATIVE_WARNING_BYTES));
        }
        let warning = NativeWarning {
            thread_id: thread.to_owned(),
            code: code.to_owned(),
            count: *count,
            message,
            truncated,
        };
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or_else(|| Error::new("source_integrity", "Codex warning sequence exhausted"))?;
        let id = super::event_id(
            thread,
            "session",
            &format!("warning-{}", self.sequence),
            NATIVE_WARNING_EVENT,
        )?;
        let data = serde_json::to_value(warning)
            .map_err(|_| Error::new("internal", "Codex warning could not be encoded"))?;
        Ok(Some((id, data)))
    }
}
