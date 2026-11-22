/// Semantic events and per-turn usage for calls through Eliot's Swarm MCP tool.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};

pub const SWARM_MCP_EVENT_ORIGIN: &str = "swarm_mcp";
pub const SWARM_MCP_EVENT_TOOL: &str = "swarm";
pub const MAX_SWARM_MCP_TURNS: usize = 1_000;

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SwarmMcpToolStatus {
    Completed,
    Refused,
}

/// Bounded payload for a semantic tool.started or tool.completed event.
/// It intentionally contains no tool arguments, result text, or error detail.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmMcpToolEventData {
    pub origin: String,
    pub tool: String,
    pub call_id: String,
    pub method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<SwarmMcpToolStatus>,
}

impl SwarmMcpToolEventData {
    pub fn new(call_id: String, method: String, turn_id: Option<String>) -> Self {
        Self {
            origin: SWARM_MCP_EVENT_ORIGIN.to_owned(),
            tool: SWARM_MCP_EVENT_TOOL.to_owned(),
            call_id,
            method,
            turn_id,
            status: None,
        }
    }

    pub fn completed(mut self, status: SwarmMcpToolStatus) -> Self {
        self.status = Some(status);
        self
    }

    pub fn validate_for_kind(&self, kind: &str) -> Result<()> {
        if self.origin != SWARM_MCP_EVENT_ORIGIN
            || self.tool != SWARM_MCP_EVENT_TOOL
            || self.call_id.trim().is_empty()
            || self.call_id.len() > 64
            || !canonical_method_token(&self.method)
            || self.turn_id.as_ref().is_some_and(|turn_id| {
                turn_id.trim().is_empty()
                    || turn_id.len() > 256
                    || turn_id.chars().any(char::is_control)
            })
        {
            return Err(Error::new("invalid", "Swarm MCP tool event is invalid"));
        }
        match (kind, self.status) {
            ("tool.started", None) | ("tool.completed", Some(_)) => Ok(()),
            _ => Err(Error::new(
                "invalid",
                "Swarm MCP tool event status does not match its kind",
            )),
        }
    }
}

fn canonical_method_token(method: &str) -> bool {
    if method.len() > 256 {
        return false;
    }
    method.split('.').all(|segment| {
        let mut bytes = segment.bytes();
        matches!(bytes.next(), Some(b'a'..=b'z'))
            && bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'_'))
    })
}

/// Counts for one retained Agent generation and exact turn. Remote HTTP MCP
/// callers have no local Agent or turn and therefore do not appear here.
#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SwarmMcpTurnUsage {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub calls: usize,
    pub refusals: usize,
}
