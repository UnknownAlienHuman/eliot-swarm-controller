//! Public grammar for the two implemented mutations. Authorization, retained
//! bindings, hydration and effect settlement remain in the Router and Store.

use super::{DrainReport, StatusBlocker};
use crate::{CommandState, Error, Result, is_coordination_verb, is_user_event_verb};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub use crate::SubmitCommand as CommandSubmitRequest;

impl CommandSubmitRequest {
    pub fn new(id: impl Into<String>, verb: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            target: None,
            selector: None,
            verb: verb.into(),
            args: Value::Object(Map::new()),
            expected_generation: None,
            expected_turn_id: None,
            request_id: None,
            probe: false,
        }
    }

    /// Only shared wire shape belongs here. In particular, optional binding
    /// values retain their existing verb/principal-specific admission errors.
    pub fn validate(&self) -> Result<()> {
        if !self.args.is_object() {
            return Err(Error::new("invalid", "command args must be a JSON object"));
        }
        if self.verb == "configure"
            && (self.selector.is_some()
                || self.expected_turn_id.is_some()
                || self.request_id.is_some()
                || self.probe)
        {
            return Err(Error::new(
                "invalid",
                "configure takes only a target and an optional generation binding",
            ));
        }
        if self.probe && self.verb != "send" {
            return Err(Error::new(
                "unsupported",
                "provider probes are supported only for send",
            ));
        }
        if self.verb == "git.fetch"
            && (self.target.is_some()
                || self.expected_generation.is_some()
                || self.expected_turn_id.is_some()
                || self.request_id.is_some()
                || self.probe)
        {
            return Err(Error::new(
                "invalid",
                "git.fetch is targetless and has no Agent bindings",
            ));
        }
        if is_user_event_verb(self.verb.as_str())
            && (self.target.is_some()
                || self.expected_generation.is_some()
                || self.expected_turn_id.is_some()
                || self.request_id.is_some()
                || self.probe)
        {
            return Err(Error::new(
                "invalid",
                "notify/emit are targetless and have no Agent bindings",
            ));
        }
        if is_coordination_verb(self.verb.as_str())
            && (self.target.is_some()
                || self.expected_generation.is_some()
                || self.expected_turn_id.is_some()
                || self.request_id.is_some()
                || self.probe)
        {
            return Err(Error::new(
                "invalid",
                "mail.send/var.set are targetless and have no Agent bindings",
            ));
        }
        Ok(())
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CommandSubmitResponse {
    pub command_id: String,
    #[serde(deserialize_with = "nullable")]
    pub agent: Option<String>,
    #[serde(deserialize_with = "nullable")]
    pub generation: Option<i64>,
    pub state: CommandState,
    #[serde(deserialize_with = "nullable")]
    pub outcome: Option<Value>,
    #[serde(deserialize_with = "nullable")]
    pub linked_command: Option<String>,
}

#[derive(schemars::JsonSchema, Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ShutdownMode {
    Drain,
    Force,
    PrepareRestart,
    CancelRestart,
    Restart,
}

impl ShutdownMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Drain => "drain",
            Self::Force => "force",
            Self::PrepareRestart => "prepare_restart",
            Self::CancelRestart => "cancel_restart",
            Self::Restart => "restart",
        }
    }

    pub fn parse(value: &str) -> Result<Self> {
        match value {
            "drain" => Ok(Self::Drain),
            "force" => Ok(Self::Force),
            "prepare_restart" => Ok(Self::PrepareRestart),
            "cancel_restart" => Ok(Self::CancelRestart),
            "restart" => Ok(Self::Restart),
            _ => Err(Error::new("invalid", "invalid daemon.shutdown mode")),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonShutdownRequest {
    #[serde(deserialize_with = "shutdown_id")]
    #[schemars(description = "Nonblank string; preserved without trimming or normalization.")]
    pub id: String,
    pub mode: ShutdownMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart_id: Option<String>,
}

impl DaemonShutdownRequest {
    pub fn new(id: impl Into<String>, mode: ShutdownMode) -> Self {
        Self {
            id: id.into(),
            mode,
            restart_id: None,
        }
    }

    pub fn validate(&self) -> Result<()> {
        validate_shutdown_id(&self.id)?;
        match (self.mode, &self.restart_id) {
            (ShutdownMode::Restart | ShutdownMode::CancelRestart, Some(id)) => {
                validate_shutdown_id(id)
            }
            (ShutdownMode::Restart | ShutdownMode::CancelRestart, None) | (_, Some(_)) => {
                Err(Error::new(
                    "invalid",
                    "restart_id binds restart and cancel to their prepare receipt",
                ))
            }
            (_, None) => Ok(()),
        }
    }
}

#[derive(schemars::JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonShutdownResponse {
    pub state: CommandState,
    pub mode: ShutdownMode,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_blockers"
    )]
    pub blockers: Option<Vec<StatusBlocker>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub drain: Option<DrainReport>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present_write_faulted"
    )]
    pub store_write_faulted: Option<bool>,
}

pub(super) fn nullable<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    deserializer: D,
) -> std::result::Result<Option<T>, D::Error> {
    Option::<T>::deserialize(deserializer)
}

fn validate_shutdown_id(id: &str) -> Result<()> {
    if id.trim().is_empty() {
        return Err(Error::new("invalid", "id must be a non-empty string"));
    }
    Ok(())
}

fn shutdown_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<String, D::Error> {
    let id = String::deserialize(deserializer)?;
    validate_shutdown_id(&id).map_err(serde::de::Error::custom)?;
    Ok(id)
}

fn present_blockers<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<Vec<StatusBlocker>>, D::Error> {
    // Named structs may accept positional sequences. Keep the existing small
    // object-map boundary for nested public results instead of cloning schemas.
    let objects = Vec::<Map<String, Value>>::deserialize(deserializer)?;
    objects
        .into_iter()
        .map(|object| {
            serde_json::from_value(Value::Object(object)).map_err(serde::de::Error::custom)
        })
        .collect::<std::result::Result<Vec<_>, _>>()
        .map(Some)
}

fn present_write_faulted<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<bool>, D::Error> {
    bool::deserialize(deserializer).map(Some)
}
