use super::{
    HandoffResponse, MAX_HANDOFF_SENDS, ProjectAssignment, ProjectManagerIdentity, ReparentedAgent,
};
use crate::{Error, Result, parse_agent_id};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub const PROJECT_TRANSFER_METHOD: &str = "project.transfer";

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectTransferRequest {
    pub id: String,
    pub project: String,
    pub expected_revision: i64,
    pub action: ProjectTransferAction,
}

#[derive(Clone, Debug, JsonSchema, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProjectTransferAction {
    Begin {
        /// Explicit Owner authorization to provision B after A's custody closes.
        /// Record the saved context and authorized continuation before draining A.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        saved_context: Option<String>,
    },
    Launch {
        intent: String,
        profile: String,
    },
    Commit {
        intent: String,
        destination: ProjectManagerIdentity,
        /// Retained source SEND supplying the original task for the handoff brief.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        task_command: Option<String>,
        /// Ordered, plain source SENDs with a proven no-effect outcome.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        #[schemars(length(max = MAX_HANDOFF_SENDS), extend("uniqueItems" = true))]
        commands: Vec<String>,
    },
    Cancel {
        intent: String,
    },
}

impl ProjectTransferRequest {
    pub fn validate(&self) -> Result<()> {
        super::project_assignment::validate_name(&self.project, "params.project")?;
        if self.expected_revision < 1 {
            return Err(Error::invalid_field(
                "params.expected_revision",
                "positive revision",
            ));
        }
        match &self.action {
            ProjectTransferAction::Begin {
                saved_context: Some(context),
            } if context.trim().is_empty() || context.len() > 8192 || context.contains('\0') => {
                Err(Error::invalid_field(
                    "params.action.saved_context",
                    "nonempty context of at most 8192 bytes",
                ))
            }
            ProjectTransferAction::Launch { profile, .. } => {
                super::project_assignment::validate_name(profile, "params.action.profile")
            }
            ProjectTransferAction::Commit {
                destination,
                task_command,
                commands,
                ..
            } => match destination {
                ProjectManagerIdentity::Local { agent, generation } => {
                    parse_agent_id(agent)?;
                    if *generation < 1 {
                        return Err(Error::invalid_field(
                            "params.action.destination.generation",
                            "positive generation",
                        ));
                    }
                    if let Some(task) = task_command {
                        crate::command::validate_command_id(task)?;
                    }
                    super::handoff::validate_send_selection(commands)
                }
                ProjectManagerIdentity::Remote { .. } => Err(Error::new(
                    "unsupported",
                    "remote transfer requires verified manager grants",
                )),
            },
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectTransferOutcome {
    Draining,
    LaunchAdmitted,
    DestinationReady,
    WaitingCapacity,
    Blocked,
    Transferred,
    Cancelled,
}

#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ProjectTransferBlocker {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, JsonSchema, Serialize, Deserialize)]
pub struct ProjectTransferResult {
    pub id: String,
    pub intent: String,
    pub outcome: ProjectTransferOutcome,
    pub assignment: ProjectAssignment,
    pub source: ProjectManagerIdentity,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destination: Option<ProjectManagerIdentity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub launch_command: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub saved_context: Option<String>,
    pub blockers: Vec<ProjectTransferBlocker>,
    pub moved: Vec<ReparentedAgent>,
    pub claims: Vec<String>,
    pub goal_revision: Option<i64>,
    /// Initial handoff admission. The parent command carries current delivery readback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handoff: Option<HandoffResponse>,
}
