use crate::error::{Error, Result};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub jsonrpc: String,
    pub id: String,
    pub method: String,
    #[serde(default = "empty_object")]
    pub params: Value,
}

fn empty_object() -> Value {
    json!({})
}

impl Request {
    /// Validate the shared request envelope before a server applies policy.
    pub fn validate(&self) -> Result<()> {
        if self.jsonrpc != "2.0"
            || self.id.is_empty()
            || self.method.is_empty()
            || !self.params.is_object()
        {
            return Err(Error::invalid(
                "expected JSON-RPC 2.0 with nonempty string id/method and object params",
            ));
        }
        Ok(())
    }
}
