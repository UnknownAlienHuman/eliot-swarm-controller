//! Agent-owned, generation-bound wake conditions (#833 item 3).

use super::waits::WaitTurnRef;
use crate::{Error, ProcessIdentity, Result};
use serde::{Deserialize, Serialize};
use std::path::{Component, Path};

pub const DEFAULT_AGENT_WAKE_TIMEOUT_SECS: u64 = 3_600;
pub const MAX_AGENT_WAKE_TIMEOUT_SECS: u64 = 86_400;
pub const MAX_AGENT_WAKE_ID_BYTES: usize = 64;
pub const MAX_AGENT_WAKE_PATH_BYTES: usize = 4_096;
pub const MAX_AGENT_WAKE_PATH_COMPONENTS: usize = 64;
pub const MAX_AGENT_WAKE_RUN_ID_BYTES: usize = MAX_AGENT_WAKE_ID_BYTES;
pub const MAX_AGENT_WAKE_TURN_ID_BYTES: usize = 256;

/// One exact condition owned by the calling Agent generation.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentWakeCondition {
    /// A regular file exists below the caller's retained managed worktree.
    FileAppears { path: String },
    /// One exact process owned by the caller's Agent or managed-workspace generation exits.
    ProcessExit { identity: ProcessIdentity },
    /// Observe an existing authorized `run` receipt; definite nonzero exits are
    /// delivered with their exit code and bounded redacted stderr tail.
    ArgvExit0 { run_id: String },
    /// One exact turn of a direct child Agent settles.
    ChildCompletion { turn: WaitTurnRef },
    /// The requested number of seconds elapses after admission.
    Seconds { seconds: u64 },
}

/// Arm one wake for the authenticated Agent's current exact turn.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWakeRequest {
    #[schemars(
        description = "Fresh mutation ULID; MCP callers may omit it and receive the minted id for safe retries."
    )]
    pub id: String,
    pub condition: AgentWakeCondition,
    #[serde(default = "default_timeout_secs")]
    #[schemars(
        range(min = 1, max = MAX_AGENT_WAKE_TIMEOUT_SECS),
        description = "Seconds before an unsatisfied wake expires; defaults to 3600 and is capped at 86400."
    )]
    pub timeout_secs: u64,
}

impl AgentWakeRequest {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty()
            || self.id.len() > MAX_AGENT_WAKE_ID_BYTES
            || self.id.contains('\0')
        {
            return Err(Error::invalid_field("params.id", "a fresh mutation ULID"));
        }
        if !(1..=MAX_AGENT_WAKE_TIMEOUT_SECS).contains(&self.timeout_secs) {
            return Err(Error::invalid_field(
                "params.timeout_secs",
                "an integer from 1 to 86400",
            ));
        }
        match &self.condition {
            AgentWakeCondition::FileAppears { path } => validate_relative_path(path, "path"),
            AgentWakeCondition::ProcessExit { identity } => {
                if identity.host_boot_id.trim().is_empty()
                    || identity.host_boot_id.len() > 128
                    || identity.host_boot_id.chars().any(char::is_control)
                    || identity.pid == 0
                {
                    return Err(Error::invalid_field(
                        "params.condition.identity",
                        "an exact nonempty process identity",
                    ));
                }
                Ok(())
            }
            AgentWakeCondition::ArgvExit0 { run_id } => {
                if run_id.trim().is_empty()
                    || run_id.len() > MAX_AGENT_WAKE_RUN_ID_BYTES
                    || run_id.contains('\0')
                {
                    return Err(Error::invalid_field(
                        "params.condition.run_id",
                        "an existing run command ULID",
                    ));
                }
                Ok(())
            }
            AgentWakeCondition::ChildCompletion { turn } => {
                turn.validate()?;
                if turn.turn_id.len() > MAX_AGENT_WAKE_TURN_ID_BYTES
                    || turn.turn_id.chars().any(char::is_control)
                {
                    return Err(Error::invalid_field(
                        "params.condition.turn.turn_id",
                        "an exact turn id of at most 256 bytes without control characters",
                    ));
                }
                Ok(())
            }
            AgentWakeCondition::Seconds { seconds } => {
                if *seconds == 0
                    || *seconds > self.timeout_secs
                    || *seconds > MAX_AGENT_WAKE_TIMEOUT_SECS
                {
                    return Err(Error::invalid_field(
                        "params.condition.seconds",
                        "a positive number no greater than timeout_secs",
                    ));
                }
                Ok(())
            }
        }
    }
}

/// Retained acknowledgement for the one wake admitted by `agent.wake`.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentWakeResponse {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    /// Stable command id reserved for the single eventual fact delivery.
    pub delivery_command_id: String,
    pub state: String,
}

impl AgentWakeResponse {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty()
            || self.agent.trim().is_empty()
            || self.generation <= 0
            || self.turn_id.trim().is_empty()
            || self.delivery_command_id.trim().is_empty()
            || self.delivery_command_id.len() > MAX_AGENT_WAKE_ID_BYTES
            || self.state != "armed"
        {
            return Err(Error::new(
                "invalid",
                "agent wake acknowledgement is invalid",
            ));
        }
        Ok(())
    }
}

fn validate_relative_path(path: &str, field: &str) -> Result<()> {
    if path.trim().is_empty()
        || path.len() > MAX_AGENT_WAKE_PATH_BYTES
        || path.chars().any(char::is_control)
    {
        return Err(Error::invalid_field(
            format!("params.condition.{field}"),
            "a nonempty relative worktree path of at most 4096 bytes",
        ));
    }
    let path_obj = Path::new(path);
    let components: Vec<_> = path_obj.components().collect();
    if path_obj.is_absolute()
        || path_obj.has_root()
        || components.len() > MAX_AGENT_WAKE_PATH_COMPONENTS
        || components
            .iter()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(Error::invalid_field(
            format!("params.condition.{field}"),
            "a relative path without roots, dot, or parent components",
        ));
    }
    Ok(())
}

fn default_timeout_secs() -> u64 {
    DEFAULT_AGENT_WAKE_TIMEOUT_SECS
}
