//! Grammar for the implemented command/turn/result read calls. These identifiers
//! select existing records; they never admit or retry an external effect.

use super::mutations::nullable;
use crate::{Agent, Command, Error, Result, Turn};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuleGetRequest {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "optional_project"
    )]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub project: Option<String>,
}

impl RuleGetRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            project: None,
        }
    }

    pub fn in_project(mut self, project: impl Into<String>) -> Self {
        self.project = Some(project.into());
        self
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if let Some(project) = &self.project {
            validate_id(project)?;
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuleStateView {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
    pub revision: i64,
    pub current_cursor: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_revision: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retiring_cursor: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retire_through: Option<i64>,
    pub enabled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused_reason: Option<String>,
}

impl RuleStateView {
    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if let Some(project) = &self.project {
            validate_id(project)?;
        }
        if self.revision < 0 || self.current_cursor < 0 {
            return Err(Error::new(
                "invalid",
                "rule revision and cursor must be nonnegative",
            ));
        }
        if self
            .paused_reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
        {
            return Err(Error::new("invalid", "paused_reason must be nonempty"));
        }
        match (
            self.retiring_revision,
            self.retiring_cursor,
            self.retire_through,
        ) {
            (None, None, None) => Ok(()),
            (Some(revision), Some(cursor), Some(through))
                if revision >= 0
                    && revision < self.revision
                    && cursor >= 0
                    && through >= cursor =>
            {
                Ok(())
            }
            _ => Err(Error::new(
                "invalid",
                "rule retirement progress must be a complete, ordered tuple",
            )),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandGetRequest {
    #[serde(deserialize_with = "required_command_id")]
    #[schemars(
        description = "Exact nonblank command ID, at most 256 bytes; includes derived IDs and is preserved without normalization."
    )]
    pub id: String,
}

impl CommandGetRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self { id: id.into() }
    }

    pub fn validate(&self) -> Result<()> {
        crate::command::validate_command_id(&self.id)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnGetRequest {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub agent: String,
    #[serde(deserialize_with = "generation")]
    #[schemars(range(min = 0))]
    pub generation: i64,
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub turn_id: String,
}

impl TurnGetRequest {
    pub fn new(agent: impl Into<String>, generation: i64, turn_id: impl Into<String>) -> Self {
        Self {
            agent: agent.into(),
            generation,
            turn_id: turn_id.into(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.agent)?;
        validate_generation(self.generation)?;
        validate_id(&self.turn_id)
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentResultRequest {
    #[serde(deserialize_with = "required_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_generation"
    )]
    #[schemars(range(min = 0), with = "i64")]
    pub generation: Option<i64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_id"
    )]
    #[schemars(
        description = "Nonblank string; preserved without trimming or normalization.",
        with = "String"
    )]
    pub turn_id: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl AgentResultRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            generation: None,
            turn_id: None,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if let Some(generation) = self.generation {
            validate_generation(generation)?;
        }
        if let Some(turn_id) = &self.turn_id {
            validate_id(turn_id)?;
        }
        if !(1..=super::MAX_LIMIT).contains(&self.limit) {
            return Err(Error::new("invalid", "limit must be between 1 and 1000"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandChildrenRequest {
    #[serde(deserialize_with = "required_command_id")]
    #[schemars(
        description = "Exact nonblank command ID, at most 256 bytes; includes derived IDs and is preserved without normalization."
    )]
    pub id: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_command_id"
    )]
    #[schemars(
        description = "Exact nonblank command ID, at most 256 bytes; preserved without normalization.",
        with = "String"
    )]
    pub after: Option<String>,
    #[serde(
        default = "super::default_limit",
        deserialize_with = "super::event_limit"
    )]
    #[schemars(range(min = 1, max = 1000))]
    pub limit: usize,
}

impl CommandChildrenRequest {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            after: None,
            limit: super::DEFAULT_LIMIT,
        }
    }

    pub fn validate(&self) -> Result<()> {
        crate::command::validate_command_id(&self.id)?;
        if let Some(after) = &self.after {
            crate::command::validate_command_id(after)?;
        }
        if !(1..=super::MAX_LIMIT).contains(&self.limit) {
            return Err(Error::new("invalid", "limit must be between 1 and 1000"));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandChildrenResponse {
    #[serde(deserialize_with = "object_commands")]
    pub children: Vec<Command>,
    #[serde(deserialize_with = "nullable")]
    pub next: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct AgentResultResponse {
    pub agent: String,
    pub generation: i64,
    #[serde(deserialize_with = "object_turns")]
    pub turns: Vec<Turn>,
    #[serde(deserialize_with = "object_agent")]
    pub agent_metadata: Agent,
}

fn required_command_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let id = String::deserialize(deserializer)?;
    crate::command::validate_command_id(&id).map_err(serde::de::Error::custom)?;
    Ok(id)
}

fn present_command_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    required_command_id(deserializer).map(Some)
}

fn validate_id(value: &str) -> Result<()> {
    if value.trim().is_empty() {
        return Err(Error::new(
            "invalid",
            "read identifiers must be non-empty strings",
        ));
    }
    Ok(())
}

fn validate_generation(value: i64) -> Result<()> {
    if value < 0 {
        return Err(Error::new(
            "invalid",
            "generation must be a nonnegative integer",
        ));
    }
    Ok(())
}

fn required_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let id = String::deserialize(deserializer)?;
    validate_id(&id).map_err(serde::de::Error::custom)?;
    Ok(id)
}

fn optional_project<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    let project = Option::<String>::deserialize(deserializer)?;
    if let Some(project) = &project {
        validate_id(project).map_err(serde::de::Error::custom)?;
    }
    Ok(project)
}

fn generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<i64, D::Error> {
    let value = i64::deserialize(deserializer)?;
    validate_generation(value).map_err(serde::de::Error::custom)?;
    Ok(value)
}

fn present_generation<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<i64>, D::Error> {
    generation(deserializer).map(Some)
}

fn present_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    required_id(deserializer).map(Some)
}

fn object_agent<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Agent, D::Error> {
    let object = Map::<String, Value>::deserialize(deserializer)?;
    serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
}

fn object_turns<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Turn>, D::Error> {
    let objects = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    objects
        .into_iter()
        .map(|object| {
            serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
        })
        .collect()
}

fn object_commands<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Vec<Command>, D::Error> {
    let objects = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    objects
        .into_iter()
        .map(|object| {
            serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
        })
        .collect()
}
