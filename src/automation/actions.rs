//! Typed automation steps and their deliberately small capability surface.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

/// Steps retained in the manager-owned configuration format.
/// `work_dispatch`, `review_dispatch`, and bounded `review_disposition` have
/// Store consumers; the other steps remain visible capability gaps rather
/// than simulated effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum AutomationStep {
    WorkDispatch,
    ReviewDispatch,
    ReviewDisposition,
    RepairDispatch,
    Acceptance,
    Publication,
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
            "github_projection" => Ok(Self::GithubProjection),
            _ => Err(Error::invalid(format!("unknown automation step: {value}"))),
        }
    }

    pub(crate) fn has_consumer(self) -> bool {
        matches!(
            self,
            Self::WorkDispatch | Self::ReviewDispatch | Self::ReviewDisposition
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
}

impl AutomationCause {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::AppliedSubmission { .. } => "applied_submission",
        }
    }

    /// Stable domain identity, independent of event timestamps and config
    /// revisions. The exact observation ID remains separately available as
    /// provenance.
    pub(crate) fn id(&self) -> &str {
        match self {
            Self::AppliedSubmission { submission_ref, .. } => submission_ref,
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
        }
    }
}

pub(crate) fn supported_action_for(step: AutomationStep) -> Option<&'static str> {
    match step {
        AutomationStep::WorkDispatch => Some("swarm.launch"),
        AutomationStep::ReviewDispatch => Some("review.assign"),
        _ => None,
    }
}
