//! Versioned, data-only envelope for the exact prompt admitted for a Task.

use serde::{Deserialize, Serialize};

pub const TASK_PROMPT_SCHEMA_ID: &str = "swarm.task_prompt";
pub const TASK_PROMPT_SCHEMA_VERSION: u16 = 1;
pub const TASK_PROMPT_CONTRACT_REVISION: &str = "task-prompt-v1";

/// Store-produced immutable prompt bytes and their frozen Task identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskPromptEnvelopeV1 {
    pub schema_id: String,
    pub schema_version: u16,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub task_snapshot_sha256: String,
    pub prompt_sha256: String,
    pub prompt_bytes: u64,
    pub prompt: String,
}

impl TaskPromptEnvelopeV1 {
    /// Validate the shared data shape; digest contents are checked by the
    /// producer and again by each adapter at the native effect boundary.
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        let prompt_bytes = u64::try_from(self.prompt.len())
            .map_err(|_| "task prompt byte count is outside the supported range")?;
        if self.schema_id != TASK_PROMPT_SCHEMA_ID
            || self.schema_version != TASK_PROMPT_SCHEMA_VERSION
            || !valid_bounded_identity(&self.task_id, 512)
            || self.task_revision <= 0
            || !valid_bounded_identity(&self.attempt_id, 512)
            || !is_lower_sha256(&self.task_snapshot_sha256)
            || !is_lower_sha256(&self.prompt_sha256)
            || self.prompt.trim().is_empty()
            || prompt_bytes > i64::MAX as u64
            || self.prompt_bytes != prompt_bytes
        {
            return Err("task prompt envelope is invalid");
        }
        Ok(())
    }
}

fn valid_bounded_identity(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

fn is_lower_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
