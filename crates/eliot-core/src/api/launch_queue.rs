use crate::{Error, Result, SubmitCommand};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(JsonSchema, Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchQueueRequest {
    pub id: String,
    pub args: serde_json::Value,
}

impl LaunchQueueRequest {
    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if !self.args.is_object()
            || self.args.get("assignment").is_some()
            || self.args.get("owner_override").is_some()
        {
            return Err(Error::new(
                "invalid",
                "queue requires interactive launch arguments",
            ));
        }
        Ok(())
    }

    pub fn command(&self) -> SubmitCommand {
        let mut command = SubmitCommand::new(self.id.clone(), "launch");
        command.args = self.args.clone();
        command
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchIntentGetRequest {
    pub id: String,
}

impl LaunchIntentGetRequest {
    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchIntentState {
    Queued,
    Admitted,
    Rejected,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchWaitReason {
    pub code: String,
    pub scope: String,
    pub changed_by: String,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchIntentView {
    pub id: String,
    pub project: String,
    pub state: LaunchIntentState,
    pub command_id: Option<String>,
    pub reason: Option<LaunchWaitReason>,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssignmentWaitView {
    pub rule: String,
    pub project: String,
    pub due_at: i64,
    pub reason: LaunchWaitReason,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchQueueListRequest {
    pub project: Option<String>,
    pub after: Option<String>,
}

impl LaunchQueueListRequest {
    pub fn validate(&self) -> Result<()> {
        if self
            .after
            .as_ref()
            .is_some_and(|after| after.is_empty() || after.len() > 256)
        {
            return Err(Error::new("invalid", "invalid launch queue cursor"));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchQueueView {
    pub interactive: Vec<LaunchIntentView>,
    pub assignments: Vec<AssignmentWaitView>,
    pub next_after: Option<String>,
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagerBudgetRequest {
    pub id: String,
    pub project: String,
    pub expected_revision: i64,
    pub max_live: Option<u64>,
    pub priority: Option<u8>,
    /// Strict pause of new admissions in the manager's assignment scope,
    /// queued launch intents included; running children keep running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
}

impl ManagerBudgetRequest {
    pub fn validate(&self) -> Result<()> {
        validate_id(&self.id)?;
        if self.project.is_empty()
            || self.expected_revision <= 0
            || (self.max_live.is_none() && self.priority.is_none() && self.paused.is_none())
        {
            return Err(Error::new(
                "invalid",
                "budget change requires project, assignment revision and a value",
            ));
        }
        Ok(())
    }
}

#[derive(JsonSchema, Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagerBudgetView {
    pub project: String,
    /// The effective assignment limit; 0 while paused.
    pub max_live: Option<u64>,
    pub priority: u8,
    #[serde(default)]
    pub paused: bool,
}

fn validate_id(id: &str) -> Result<()> {
    if id.len() != 26
        || !matches!(id.as_bytes().first(), Some(b'0'..=b'7'))
        || !id
            .bytes()
            .all(|byte| b"0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(&byte))
    {
        return Err(Error::invalid_mutation_id());
    }
    Ok(())
}
