//! Immutable, Store-produced text prompt for a descriptor-selected task.dispatch.
//! Legacy descriptors do not use or implicitly decode this schema.

use serde::{Deserialize, Serialize};

pub const TASK_PROMPT_SCHEMA_ID: &str = "swarm.task_prompt";
pub const TASK_PROMPT_SCHEMA_VERSION: u16 = 1;
pub const TASK_PROMPT_CONTRACT_REVISION: &str = "task-prompt-v1";

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
    /// Data-only shape validation. The effect boundary recomputes SHA-256 over
    /// prompt.as_bytes() before submitting the exact text to a native harness.
    pub fn validate_shape(&self) -> Result<(), &'static str> {
        fn atom(value: &str) -> bool {
            !value.is_empty()
                && value.len() <= 256
                && !value.chars().any(char::is_control)
        }
        fn lower_sha256(value: &str) -> bool {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }

        if self.schema_id != TASK_PROMPT_SCHEMA_ID
            || self.schema_version != TASK_PROMPT_SCHEMA_VERSION
            || !atom(&self.task_id)
            || !atom(&self.attempt_id)
            || self.task_revision <= 0
            || !lower_sha256(&self.task_snapshot_sha256)
            || !lower_sha256(&self.prompt_sha256)
            || self.prompt.trim().is_empty()
            || self.prompt_bytes == 0
            || self.prompt_bytes > i64::MAX as u64
            || self.prompt_bytes != self.prompt.len() as u64
        {
            return Err("TaskPrompt v1 envelope has invalid shape");
        }
        Ok(())
    }
}
