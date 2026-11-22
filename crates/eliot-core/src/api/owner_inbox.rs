use crate::{Error, Result};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(
    JsonSchema, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Deserialize, Serialize,
)]
#[serde(rename_all = "snake_case")]
pub enum OwnerInboxKind {
    HarnessRequest,
    UnknownCommand,
    UnknownTurn,
    Hold,
    Agent,
    BlockedGoal,
    LaunchQueue,
    Stuck,
}

impl OwnerInboxKind {
    pub const ALL: [Self; 8] = [
        Self::HarnessRequest,
        Self::UnknownCommand,
        Self::UnknownTurn,
        Self::Hold,
        Self::Agent,
        Self::BlockedGoal,
        Self::LaunchQueue,
        Self::Stuck,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::HarnessRequest => "harness_request",
            Self::UnknownCommand => "unknown_command",
            Self::UnknownTurn => "unknown_turn",
            Self::Hold => "hold",
            Self::Agent => "agent",
            Self::BlockedGoal => "blocked_goal",
            Self::LaunchQueue => "launch_queue",
            Self::Stuck => "stuck",
        }
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerInboxItemRef {
    pub kind: OwnerInboxKind,
    pub subject: String,
    pub revision: String,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct OwnerAction {
    pub choice: String,
    pub method: String,
    pub args: Value,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub struct OwnerInboxItem {
    pub kind: OwnerInboxKind,
    pub subject: String,
    pub revision: String,
    pub age_ms: i64,
    pub reason: String,
    pub recommended: Option<OwnerAction>,
    pub choices: Vec<OwnerAction>,
}

impl OwnerInboxItem {
    pub fn cursor(&self) -> String {
        format!("{}/{}", self.kind.as_str(), self.subject)
    }

    pub fn matches(&self, item: &OwnerInboxItemRef) -> bool {
        self.kind == item.kind && self.subject == item.subject && self.revision == item.revision
    }
}

#[derive(JsonSchema, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerInboxRequest {
    pub after: Option<String>,
    pub limit: Option<usize>,
}

impl OwnerInboxRequest {
    pub const MAX_ROWS: usize = 100;

    pub fn effective_limit(&self) -> Result<usize> {
        let limit = self.limit.unwrap_or(50);
        if !(1..=Self::MAX_ROWS).contains(&limit)
            || self.after.as_ref().is_some_and(|after| !valid_key(after))
        {
            return Err(Error::new("invalid", "invalid owner inbox page"));
        }
        Ok(limit)
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct OwnerInboxResponse {
    pub items: Vec<OwnerInboxItem>,
    pub next: Option<String>,
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OwnerDecideRequest {
    pub id: String,
    pub item: OwnerInboxItemRef,
    pub choice: String,
    pub args: Option<Value>,
}

impl OwnerDecideRequest {
    pub fn validate(&self) -> Result<()> {
        super::launch_queue::validate_id(&self.id)?;
        if !valid_key(&self.item.subject)
            || !valid_key(&self.item.revision)
            || self.choice.is_empty()
            || self.choice.len() > 64
            || self.args.as_ref().is_some_and(|args| !args.is_object())
        {
            return Err(Error::new("invalid", "invalid owner inbox decision"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, Deserialize, Serialize)]
pub struct OwnerDecideResponse {
    pub snoozed: bool,
    pub result: Option<Value>,
}

#[derive(JsonSchema, Clone, Debug, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct SwarmLiveness {
    pub moving: usize,
    pub asked: usize,
    pub stuck: usize,
    pub long: usize,
    pub reasons: BTreeMap<String, usize>,
}

fn valid_key(value: &str) -> bool {
    !value.is_empty() && value.len() <= 2048 && !value.chars().any(char::is_control)
}
