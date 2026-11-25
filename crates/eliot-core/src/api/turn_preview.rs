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

    /// A reply that keeps the contract: an ended turn carries neither a
    /// revision nor a preview; a live one carries its revision, no preview
    /// when that revision is the one echoed (a timeout), and otherwise at
    /// most a bounded preview with a valid message id.
    pub fn validate_response(&self, reply: &TurnPreviewWatchResponse) -> Result<()> {
        let valid = if reply.ended {
            reply.revision.is_none() && reply.preview.is_none()
        } else {
            reply.revision.is_some()
                && match &reply.preview {
                    None => true,
                    Some(preview) => {
                        reply.revision != self.revision
                            && valid_preview_id(&preview.message_id)
                            && preview.text.len() <= MAX_TURN_PREVIEW_BYTES
                    }
                }
        };
        if valid {
            Ok(())
        } else {
            Err(Error::new(
                "invalid",
                "turn preview reply does not match its request",
            ))
        }
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
