//! Owner resolution of unknown commands and turns. These requests record an
//! owner assertion about an already-uncertain outcome; they never invent a
//! native receipt, replay an effect, or override capacity.

use crate::{CommandState, Error, Result};
use serde::{Deserialize, Serialize};

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandResolution {
    Applied,
    Failed,
}

impl CommandResolution {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Applied => "applied",
            Self::Failed => "failed",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "applied" => Ok(Self::Applied),
            "failed" => Ok(Self::Failed),
            _ => Err(Error::new(
                "invalid",
                "command.resolve outcome must be applied or failed",
            )),
        }
    }

    pub fn state(self) -> CommandState {
        match self {
            Self::Applied => CommandState::Applied,
            Self::Failed => CommandState::Failed,
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandResolveRequest {
    pub id: String,
    /// Exact nonblank command ID, at most 256 bytes; includes derived IDs.
    pub command_id: String,
    #[serde(rename = "as")]
    pub resolution: CommandResolution,
    pub reason: String,
}

impl CommandResolveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        crate::command::validate_command_id(&self.command_id)?;
        validate_reason(&self.reason)
    }

    pub fn validate_response(&self, response: &CommandResolveResponse) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.command_id != self.command_id
            || response.state != self.resolution.state()
        {
            return Err(unproven());
        }
        if let Some(cleanup) = response.workspace_cleanup.as_ref() {
            let has_text = |field: &str| {
                cleanup[field]
                    .as_str()
                    .is_some_and(|text| !text.trim().is_empty())
            };
            if response.state != CommandState::Failed
                || !has_text("worktree_path")
                || !has_text("branch")
                || match cleanup["state"].as_str() {
                    Some("removed") => false,
                    Some("preserved") => {
                        !has_text("location") || !has_text("code") || !has_text("message")
                    }
                    _ => true,
                }
            {
                return Err(unproven());
            }
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandResolveResponse {
    pub id: String,
    pub command_id: String,
    pub state: CommandState,
    /// Git cleanup result for an initial managed launch resolved as failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_cleanup: Option<serde_json::Value>,
}

/// With no review digest, list one page. With a digest, settle exactly the
/// reviewed page after checking its evidence again.
#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SettleStoppedRequest {
    pub id: String,
    #[serde(default)]
    pub after: Option<String>,
    #[serde(default)]
    pub review: Option<String>,
}

impl SettleStoppedRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if let Some(after) = &self.after {
            crate::validate_command_id(after)?;
        }
        if let Some(review) = &self.review
            && (review.len() != 64 || !review.bytes().all(|b| b.is_ascii_hexdigit()))
        {
            return Err(Error::new(
                "invalid",
                "stop settlement review must be a SHA-256 digest",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct SettleStoppedResponse {
    pub id: String,
    pub review: String,
    pub applied: bool,
    pub stops: Vec<StoppedCommandSettlement>,
    /// Request replies with an exact committed native resolution receipt.
    /// Older stored responses predate this field.
    #[serde(default)]
    pub request_replies: Vec<RequestReplySettlement>,
    /// Reviewed candidates still missing process or reader proof.
    #[serde(default)]
    pub awaiting_proof: Vec<String>,
    pub next: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct StoppedCommandSettlement {
    pub agent: String,
    pub generation: i64,
    pub command_id: String,
    pub mark_stopped: bool,
    pub interrupted_turn: Option<String>,
    /// Exact Owner interrupt completed through this linked force-stop.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interrupt_fallback: Option<InterruptFallbackSettlement>,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct InterruptFallbackSettlement {
    pub command_id: String,
    /// Captured turn binding; null is an exact idle binding.
    pub turn_id: Option<String>,
    pub reason: String,
}

/// An unknown permission or user-input reply whose exact native receipt was
/// committed for the same command, request, and turn.
#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct RequestReplySettlement {
    pub agent: String,
    pub generation: i64,
    pub command_id: String,
    pub verb: String,
    pub request_id: String,
    pub turn_id: String,
    pub receipt_seq: i64,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnResolveStatus {
    Completed,
    Failed,
    Interrupted,
}

impl TurnResolveStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Interrupted => "interrupted",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            _ => Err(Error::new(
                "invalid",
                "turn.resolve status must be completed, failed, or interrupted",
            )),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnResolveRequest {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub status: TurnResolveStatus,
    pub reason: String,
}

impl TurnResolveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_write_id(&self.id)?;
        if self.agent.trim().is_empty() {
            return Err(Error::new("invalid", "turn.resolve agent must be nonempty"));
        }
        if self.generation < 0 {
            return Err(Error::new(
                "invalid",
                "turn.resolve generation must be a nonnegative integer",
            ));
        }
        if self.turn_id.trim().is_empty() {
            return Err(Error::new(
                "invalid",
                "turn.resolve turn_id must be nonempty",
            ));
        }
        validate_reason(&self.reason)
    }

    pub fn validate_response(&self, response: &TurnResolveResponse) -> Result<()> {
        if self.validate().is_err()
            || response.id != self.id
            || response.agent != self.agent
            || response.generation != self.generation
            || response.turn_id != self.turn_id
            || response.status != self.status
        {
            return Err(unproven());
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnResolveResponse {
    pub id: String,
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub status: TurnResolveStatus,
}

fn validate_write_id(id: &str) -> Result<()> {
    if id.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            "resolution write id must be nonempty",
        ));
    }
    Ok(())
}

fn validate_reason(reason: &str) -> Result<()> {
    if reason.trim().is_empty() {
        return Err(Error::new("invalid", "resolution reason must be nonempty"));
    }
    Ok(())
}

fn unproven() -> Error {
    Error::new(
        "unknown",
        "resolution response does not match its requested binding",
    )
}
