//! Owner task transfer to an existing Agent; ordinary commands own delivery.
use super::{ClaimConflict, ReparentedAgent};
use crate::{CommandState, Error, Result, parse_agent_id};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

pub const MAX_HANDOFF_SENDS: usize = 64;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HandoffRequest {
    pub id: String,
    pub from: String,
    pub from_generation: i64,
    pub to: String,
    pub to_generation: i64,
    /// Retained SEND supplying the original task text, on A's exact generation.
    pub task_command: String,
    /// Ordered, distinct accepted/queued plain SENDs. Empty transfers context only.
    #[schemars(length(max = MAX_HANDOFF_SENDS), extend("uniqueItems" = true))]
    pub commands: Vec<String>,
}

impl HandoffRequest {
    pub fn validate(&self) -> Result<()> {
        validate_selection(
            &self.id,
            &self.from,
            self.from_generation,
            &self.task_command,
            &self.commands,
        )?;
        parse_agent_id(&self.to)?;
        if self.from == self.to || self.to_generation < 1 {
            return Err(Error::new("invalid", "invalid handoff destination"));
        }
        Ok(())
    }

    pub fn validate_response(&self, value: &HandoffResponse) -> Result<()> {
        if value.id != self.id
            || value.from != self.from
            || value.to != self.to
            || value.from_generation != self.from_generation
            || value.to_generation != self.to_generation
            || value.transfers.len() != self.commands.len()
            || value.children.len() != self.commands.len() + 1
            || value.children.first() != Some(&value.brief_command)
            || value
                .transfers
                .iter()
                .zip(&self.commands)
                .enumerate()
                .any(|(i, (entry, id))| {
                    &entry.original != id || value.children.get(i + 1) != Some(&entry.successor)
                })
        {
            return Err(Error::new(
                "source_integrity",
                "handoff response binding differs",
            ));
        }
        Ok(())
    }
}

pub(super) fn validate_selection(
    id: &str,
    from: &str,
    generation: i64,
    task: &str,
    commands: &[String],
) -> Result<()> {
    parse_agent_id(from)?;
    if id.trim().is_empty()
        || generation < 1
        || task.is_empty()
        || task.len() > 256
        || commands.len() > MAX_HANDOFF_SENDS
    {
        return Err(Error::new(
            "invalid",
            "invalid transfer source or selection",
        ));
    }
    let mut seen = HashSet::new();
    if commands
        .iter()
        .any(|id| id.is_empty() || id.len() > 256 || !seen.insert(id))
    {
        return Err(Error::new(
            "invalid",
            "transfer SEND ids must be distinct and bounded",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SendTransfer {
    pub original: String,
    pub successor: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HandoffResponse {
    pub id: String,
    pub from: String,
    pub from_generation: i64,
    pub to: String,
    pub to_generation: i64,
    pub brief_command: String,
    /// Fixed ordinary command children, brief at ordinal zero.
    pub children: Vec<String>,
    pub transfers: Vec<SendTransfer>,
    pub claims: Vec<String>,
    pub claim_conflicts: Vec<ClaimConflict>,
    pub conflicts_truncated: bool,
    pub goal_revision: Option<i64>,
    pub reparented: Vec<ReparentedAgent>,
    pub brief_truncated: bool,
    /// Current readback; immutable transfer identities above never change.
    pub brief_state: CommandState,
    pub successor_gate: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommandCancelRequest {
    pub id: String,
    /// Exact nonblank command ID, at most 256 bytes; includes derived IDs.
    pub command_id: String,
}

impl CommandCancelRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() {
            return Err(Error::new("invalid", "cancel mutation id must be nonblank"));
        }
        crate::command::validate_command_id(&self.command_id)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CommandCancelResponse {
    pub command_id: String,
    pub state: CommandState,
}
