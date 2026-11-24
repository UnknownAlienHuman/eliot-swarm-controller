use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{Error, Result, parse_agent_id};

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRestoreRequest {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub checkpoint_ref: String,
    pub workspace: String,
}

impl CheckpointRestoreRequest {
    pub fn validate(&self) -> Result<()> {
        crate::api::inference::inference_id(&self.id)
            .map_err(|_| Error::new("invalid", "checkpoint restore id must be a ULID"))?;
        parse_agent_id(&self.agent)?;
        if self.generation < 0 {
            return Err(Error::new("invalid", "checkpoint generation is invalid"));
        }
        if self.checkpoint_ref.is_empty()
            || self.checkpoint_ref.len() > 512
            || self.checkpoint_ref.contains('\0')
        {
            return Err(Error::new("invalid", "checkpoint reference is invalid"));
        }
        if self.workspace.trim().is_empty()
            || self.workspace.len() > 32_768
            || self.workspace.contains('\0')
        {
            return Err(Error::new("invalid", "restore workspace is invalid"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckpointRestoreResponse {
    pub head: String,
    pub paths: Vec<String>,
}
