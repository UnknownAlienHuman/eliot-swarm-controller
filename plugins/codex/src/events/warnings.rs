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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StartupStatus {
    thread_id: Option<String>,
    name: String,
    status: String,
    error: Option<String>,
    failure_reason: Option<String>,
}

#[derive(Default)]
pub(super) struct Warnings {
    counts: HashMap<String, u64>,
    sequence: u64,
    unbound_dropped: u64,
}

impl Warnings {
    pub(super) fn observe(
        &mut self,
        bound_thread: Option<&str>,
        params: Value,
        policy: &SecretPolicy,
    ) -> Result<Option<(String, Value)>> {
        let Ok(notification) = serde_json::from_value::<Notification>(params) else {
            return Ok(None);
        };
        let code = notification
            .code
            .as_deref()
            .filter(|code| is_native_discriminator(code) && !policy.contains_text(code))
            .unwrap_or("warning");
        self.observe_fields(
            bound_thread,
            notification.thread_id.as_deref(),
            code,
            &notification.message,
            policy,
        )
    }

    pub(super) fn observe_startup_status(
        &mut self,
        bound_thread: Option<&str>,
        params: Value,
        policy: &SecretPolicy,
    ) -> Result<Option<(String, Value)>> {
        let Ok(notification) = serde_json::from_value::<StartupStatus>(params) else {
            return Ok(None);
        };
        if notification.status != "failed" {
            return Ok(None);
        }
        let code = if notification.failure_reason.as_deref() == Some("reauthenticationRequired") {
            "mcp_reauthentication_required"
        } else {
            "mcp_server_startup_failed"
        };
        let detail = notification
            .error
            .or(notification.failure_reason)
            .unwrap_or_else(|| "MCP server startup failed".to_owned());
        let message = format!("{}: {detail}", notification.name);
        self.observe_fields(
            bound_thread,
            notification.thread_id.as_deref(),
            code,
            &message,
            policy,
        )
    }

    pub(super) fn unbound_dropped(&self) -> u64 {
        self.unbound_dropped
    }

    fn observe_fields(
        &mut self,
        bound_thread: Option<&str>,
        notification_thread: Option<&str>,
        code: &str,
        source_message: &str,
        policy: &SecretPolicy,
    ) -> Result<Option<(String, Value)>> {
        let Some(thread) = bound_thread.filter(|thread| Some(*thread) == notification_thread)
        else {
            self.unbound_dropped = self.unbound_dropped.saturating_add(1);
            return Ok(None);
        };
        if source_message.is_empty() {
            return Ok(None);
        }
        let mut code = code;
        if !self.counts.contains_key(code) && self.counts.len() >= MAX_WARNING_CODES - 1 {
            code = "other";
        }
        let count = self.counts.entry(code.to_owned()).or_default();
        *count = count.saturating_add(1);
        let mut message = policy.redact_text(source_message);
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
