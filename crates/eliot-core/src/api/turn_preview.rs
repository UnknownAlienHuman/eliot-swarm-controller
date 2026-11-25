use crate::{Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const MAX_TURN_PREVIEW_BYTES: usize = 32 * 1024;

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnPreviewWatchRequest {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    #[serde(default)]
    pub revision: Option<u64>,
}

impl TurnPreviewWatchRequest {
    pub fn validate(&self) -> Result<()> {
        crate::parse_agent_id(&self.agent)?;
        if self.generation <= 0 || !valid_preview_id(&self.turn_id) {
            return Err(Error::new(
                "invalid",
                "preview requires an exact live turn binding",
            ));
        }
        Ok(())
    }
}

pub fn valid_preview_id(id: &str) -> bool {
    !id.trim().is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnPreviewWatchResponse {
    pub revision: Option<u64>,
    pub preview: Option<TurnPreview>,
    pub ended: bool,
}

#[derive(Clone, Debug, Deserialize, JsonSchema, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnPreview {
    pub message_id: String,
    pub text: String,
    pub truncated: bool,
}
