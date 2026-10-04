//! Typed automation steps and their deliberately small capability surface.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Steps retained in the manager-owned configuration format.
/// Selected delivery, acceptance, and publication steps use the existing Store
/// ledgers. GitHub projection remains an explicit capability gap.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationStep {
    WorkDispatch,
    ReviewDispatch,
    ReviewDisposition,
    RepairDispatch,
    Acceptance,
    Publication,
    CheckRun,
    GithubProjection,
}

impl AutomationStep {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::WorkDispatch => "work_dispatch",
            Self::ReviewDispatch => "review_dispatch",
            Self::ReviewDisposition => "review_disposition",
            Self::RepairDispatch => "repair_dispatch",
            Self::Acceptance => "acceptance",
            Self::Publication => "publication",
            Self::CheckRun => "check_run",
            Self::GithubProjection => "github_projection",
        }
    }

    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "work_dispatch" => Ok(Self::WorkDispatch),
            "review_dispatch" => Ok(Self::ReviewDispatch),
            "review_disposition" => Ok(Self::ReviewDisposition),
            "repair_dispatch" => Ok(Self::RepairDispatch),
            "acceptance" => Ok(Self::Acceptance),
            "publication" => Ok(Self::Publication),
            "check_run" => Ok(Self::CheckRun),
            "github_projection" => Ok(Self::GithubProjection),
            _ => Err(Error::invalid(format!("unknown automation step: {value}"))),
        }
    }

    pub(crate) fn has_consumer(self) -> bool {
        matches!(
            self,
            Self::WorkDispatch
                | Self::ReviewDispatch
                | Self::ReviewDisposition
                | Self::RepairDispatch
                | Self::Acceptance
                | Self::Publication
                | Self::CheckRun
        )
    }

    pub(crate) fn capability_gap(self) -> Option<Value> {
        (!self.has_consumer()).then(|| {
            json!({
                "code":"unsupported_step",
                "step":self.as_str(),
                "reason":"this step has no registered Rust action consumer"
            })
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AutomationCause {
    AppliedSubmission {
        observation_id: i64,
        operation_id: String,
        submission_ref: String,
    },
    CronOccurrence {
        occurrence_id: String,
        calendar_generation: String,
        due_at_ms: i64,
        task_id: String,
        attempt_id: String,
        task_revision: i64,
        candidate_ref: String,
        profile_id: String,
        profile_revision: String,
    },
}

impl AutomationCause {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::AppliedSubmission { .. } => "applied_submission",
            Self::CronOccurrence { .. } => "cron_occurrence",
        }
    }

    /// Stable domain identity, independent of event timestamps and config
    /// revisions. The exact observation ID remains separately available as
    /// provenance.
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::AppliedSubmission { submission_ref, .. } => submission_ref,
            Self::CronOccurrence { occurrence_id, .. } => occurrence_id,
        }
    }

    pub(crate) fn as_json(&self) -> Value {
        match self {
            Self::AppliedSubmission {
                observation_id,
                operation_id,
                submission_ref,
            } => json!({
                "kind":self.kind(),
                "observation_id":observation_id,
                "operation_id":operation_id,
                "id":submission_ref
            }),
            Self::CronOccurrence {
                occurrence_id,
                calendar_generation,
                due_at_ms,
                task_id,
                attempt_id,
                task_revision,
                candidate_ref,
                profile_id,
                profile_revision,
            } => json!({
                "kind":self.kind(),
                "id":occurrence_id,
                "calendar_generation":calendar_generation,
                "due_at_ms":due_at_ms,
                "task_id":task_id,
                "attempt_id":attempt_id,
                "task_revision":task_revision,
                "candidate_ref":candidate_ref,
                "profile_id":profile_id,
                "profile_revision":profile_revision,
            }),
        }
    }
}

pub(crate) fn supported_action_for(step: AutomationStep) -> Option<&'static str> {
    match step {
        AutomationStep::WorkDispatch => Some("swarm.launch"),
        AutomationStep::ReviewDispatch => Some("review.assign"),
        AutomationStep::Publication => Some("forge.publish_ref"),
        AutomationStep::CheckRun => Some("check.run"),
        _ => None,
    }
}
