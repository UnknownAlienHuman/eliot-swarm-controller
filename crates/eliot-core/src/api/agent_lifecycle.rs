//! Owner-only durable Agent tree presentation and record removal.
use crate::{Error, Result, parse_agent_id};
fn validate_ulid(id: &str) -> Result<()> {
    if id.len() == 26
        && id
            .bytes()
            .all(|b| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&b))
    {
        Ok(())
    } else {
        Err(Error::new("invalid", "id must be a ULID"))
    }
}
use serde::{Deserialize, Serialize};

#[derive(
    schemars::JsonSchema, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ArchivedFilter {
    #[default]
    Exclude,
    Only,
    Include,
}

/// Explicit `agent.list` order: `id` is the stable Agent-ID order every
/// existing caller reads, `recent` is newest activity first (last progress
/// or update, then ID descending) for human surfaces.
#[derive(
    schemars::JsonSchema, Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum AgentListOrder {
    #[default]
    Id,
    Recent,
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentArchiveRequest {
    pub id: String,
    pub agent: String,
    pub archived: bool,
}
impl AgentArchiveRequest {
    pub fn validate(&self) -> Result<()> {
        validate_ulid(&self.id)?;
        parse_agent_id(&self.agent)?;
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentDeleteRequest {
    pub id: String,
    pub agent: String,
}
impl AgentDeleteRequest {
    pub fn validate(&self) -> Result<()> {
        validate_ulid(&self.id)?;
        parse_agent_id(&self.agent)?;
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, Serialize, Deserialize)]
pub struct AgentTreeResponse {
    pub agent: String,
    pub affected: usize,
    pub archived: Option<bool>,
    pub deleted: bool,
}
