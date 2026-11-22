use crate::{CommandState, Error, Result, limits::MAX_TEXT_BYTES, parse_agent_id};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const PROJECT_BROADCAST_METHOD: &str = "project.broadcast";
pub const MAX_PROJECT_BROADCAST_AGENTS: usize = 256;

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectBroadcastRequest {
    pub id: String,
    pub project: String,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectBroadcastRecipient {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub command_id: String,
    pub state: CommandState,
}

#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectBroadcastResponse {
    pub id: String,
    pub project: String,
    pub recipients: Vec<ProjectBroadcastRecipient>,
}

impl ProjectBroadcastRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.is_empty() || self.id.trim() != self.id || self.id.contains('\0') {
            return Err(Error::invalid_field("params.id", "nonempty mutation id"));
        }
        if self.project.is_empty()
            || self.project.len() > 256
            || self.project.trim() != self.project
            || self.project.contains('\0')
        {
            return Err(Error::invalid_field(
                "params.project",
                "nonempty project id of at most 256 bytes",
            ));
        }
        if self.text.trim().is_empty() || self.text.len() > MAX_TEXT_BYTES {
            return Err(Error::invalid_field(
                "params.text",
                "nonempty text of at most 512 KiB",
            ));
        }
        Ok(())
    }

    pub fn validate_response(&self, response: &ProjectBroadcastResponse) -> Result<()> {
        if response.id != self.id
            || response.project != self.project
            || response.recipients.len() > MAX_PROJECT_BROADCAST_AGENTS
        {
            return Err(Error::new(
                "invalid",
                "project broadcast response does not match",
            ));
        }
        let mut seen_agents = HashSet::new();
        let mut seen_commands = HashSet::new();
        for recipient in &response.recipients {
            parse_agent_id(&recipient.agent)?;
            crate::command::validate_command_id(&recipient.command_id)?;
            if recipient.turn_id.trim().is_empty()
                || recipient.turn_id.len() > 256
                || recipient.turn_id.chars().any(char::is_control)
                || recipient.generation < 1
                || !seen_commands.insert(recipient.command_id.as_str())
                || !seen_agents.insert(recipient.agent.as_str())
                || !matches!(
                    recipient.state,
                    CommandState::Accepted | CommandState::Queued
                )
            {
                return Err(Error::new(
                    "invalid",
                    "project broadcast recipient is invalid",
                ));
            }
        }
        Ok(())
    }
}
