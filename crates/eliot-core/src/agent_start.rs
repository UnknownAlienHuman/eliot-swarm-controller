//! Harness-neutral parameters shared by admission preflight and plugin dispatch.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Parameters for the private `agent.start` request.
///
/// Optional fields retain their nullable wire representation. A Store preflight
/// may omit runtime-only values; the host must check the completed encoded frame
/// after supplying the runtime environment and source readback, before sending.
#[derive(Clone, Serialize, Deserialize)]
pub struct AgentStartParams {
    pub agent_id: String,
    pub generation: i64,
    pub command_id: String,
    #[serde(default)]
    pub profile: Option<Value>,
    #[serde(default)]
    pub workspace: Option<String>,
    #[serde(default)]
    pub target_environment: Option<Value>,
    #[serde(default)]
    pub note: Option<String>,
    #[serde(default)]
    pub source_plan: Option<Value>,
    #[serde(default)]
    pub source_state: Option<Value>,
    #[serde(default)]
    pub resume_from: Option<Value>,
    #[serde(default)]
    pub initial: Option<Value>,
}

impl AgentStartParams {
    pub fn new(
        agent_id: impl Into<String>,
        generation: i64,
        command_id: impl Into<String>,
    ) -> Self {
        Self {
            agent_id: agent_id.into(),
            generation,
            command_id: command_id.into(),
            profile: None,
            workspace: None,
            target_environment: None,
            note: None,
            source_plan: None,
            source_state: None,
            resume_from: None,
            initial: None,
        }
    }
}
