use crate::{Error, Result, parse_agent_id};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const PROJECT_ASSIGNMENT_CLAIM_ACTION: &str = "project.assignment.claim";

#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectManagerIdentity {
    Local {
        agent: String,
        generation: i64,
    },
    Remote {
        instance: String,
        grant_revision: i64,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectAssignmentState {
    Vacant,
    Starting,
    Active,
    Blocked,
}

#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ProjectAssignment {
    pub project: String,
    pub manager: Option<ProjectManagerIdentity>,
    pub epoch: i64,
    pub revision: i64,
    pub state: ProjectAssignmentState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_command_id: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectAssignmentOutcome {
    Claimed,
    LaunchAdmitted,
    Released,
    Occupied,
    StaleEpoch,
    Vacant,
    StartupUncertain,
}

#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ProjectAssignmentResult {
    pub assignment: ProjectAssignment,
    pub outcome: ProjectAssignmentOutcome,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub command_id: Option<String>,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectAssignmentGetRequest {
    pub project: String,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectAssignmentClaimRequest {
    pub id: String,
    pub project: String,
    pub manager: Option<ProjectManagerIdentity>,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectAssignmentLaunchRequest {
    pub id: String,
    pub project: String,
    pub profile: String,
    /// Explicitly adopt an existing checkout of the project repository as the
    /// manager workspace instead of preparing a fresh managed worktree.
    /// Absent means a new opaque worktree; the daemon never adopts automatically.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub adopt_worktree: Option<String>,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectAssignmentReleaseRequest {
    pub id: String,
    pub project: String,
    pub expected_epoch: i64,
}

fn validate_name(value: &str, field: &str) -> Result<()> {
    if value.is_empty() || value.len() > 256 || value.trim() != value || value.contains('\0') {
        return Err(Error::invalid_field(
            field,
            "nonempty name of at most 256 bytes",
        ));
    }
    Ok(())
}

impl ProjectAssignmentGetRequest {
    pub fn validate(&self) -> Result<()> {
        validate_name(&self.project, "params.project")
    }
}

impl ProjectAssignmentClaimRequest {
    pub fn validate(&self) -> Result<()> {
        validate_name(&self.project, "params.project")?;
        if let Some(ProjectManagerIdentity::Local { agent, generation }) = &self.manager {
            parse_agent_id(agent)?;
            if *generation < 1 {
                return Err(Error::invalid_field(
                    "params.manager.generation",
                    "positive generation",
                ));
            }
        }
        Ok(())
    }
}

impl ProjectAssignmentLaunchRequest {
    pub fn validate(&self) -> Result<()> {
        validate_name(&self.project, "params.project")?;
        validate_name(&self.profile, "params.profile")?;
        if let Some(adopt) = &self.adopt_worktree
            && (adopt.is_empty() || adopt.contains('\0'))
        {
            return Err(Error::invalid_field(
                "params.adopt_worktree",
                "nonempty path",
            ));
        }
        Ok(())
    }
}

impl ProjectAssignmentReleaseRequest {
    pub fn validate(&self) -> Result<()> {
        validate_name(&self.project, "params.project")?;
        if self.expected_epoch < 0 {
            return Err(Error::invalid_field(
                "params.expected_epoch",
                "nonnegative epoch",
            ));
        }
        Ok(())
    }
}
