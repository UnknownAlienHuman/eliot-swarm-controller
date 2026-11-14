//! Nonsecret transport provenance, supplied only by the authenticated HTTP bridge.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteIdentity {
    pub issuer: String,
    pub audience: String,
    pub subject: String,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RemoteCallContext {
    pub identity: RemoteIdentity,
    /// Server-generated connection correlation; never a native Agent identity.
    pub connection_id: String,
    pub session_id: Option<String>,
    pub call_id: String,
    pub mcp_request_id: Option<Value>,
    pub expires_at: u64,
}

impl RemoteCallContext {
    pub fn validate(&self) -> Result<()> {
        let bounded =
            |s: &str, max| !s.is_empty() && s.len() <= max && !s.chars().any(char::is_control);
        if !bounded(&self.identity.issuer, 256)
            || !bounded(&self.identity.audience, 256)
            || !bounded(&self.identity.subject, 512)
            || !bounded(&self.connection_id, 128)
            || !bounded(&self.call_id, 128)
            || self.session_id.as_ref().is_some_and(|s| !bounded(s, 256))
            || self.expires_at > i64::MAX as u64 / 1_000
            || self.mcp_request_id.as_ref().is_some_and(|id| match id {
                Value::String(s) => !bounded(s, 256),
                Value::Number(n) => !n.is_i64() && !n.is_u64(),
                _ => true,
            })
        {
            return Err(Error::new("not_allowed", "invalid remote call context"));
        }
        Ok(())
    }
}
