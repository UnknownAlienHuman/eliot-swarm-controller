//! Acceptance is an explicit decision over a sealed submission, never a model stop.
use crate::{
    error::{Error, Result},
    model::{self, TaskSpec},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckProfileRef {
    pub profile_id: String,
    pub profile_revision: String,
}

/// Set by the task author, not by the submitting worker. An explicit empty list
/// selects review-only acceptance; it never asserts that a compiler ran.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptancePolicy {
    pub required_check_profiles: Vec<CheckProfileRef>,
}
impl AcceptancePolicy {
    pub fn validate(&self) -> Result<()> {
        let mut ids = BTreeSet::new();
        for p in &self.required_check_profiles {
            if p.profile_id.trim().is_empty()
                || p.profile_revision.trim().is_empty()
                || !ids.insert(&p.profile_id)
            {
                return Err(Error::invalid(
                    "check profiles need unique IDs and explicit revisions",
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequirementReview {
    pub requirement_id: String,
    pub rationale: String,
    pub evidence: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptRequest {
    pub client_request_id: String,
    pub attempt_id: String,
    pub expected_revision: i64,
    pub submission_ref: String,
    pub candidate_ref: String,
    /// Exact value returned by task.submission. New feedback during verification
    /// invalidates this decision proposal, not the worker's existing code.
    pub expected_feedback_observation_id: i64,
    pub reason: String,
    pub reviews: Vec<RequirementReview>,
    /// Actual CheckRun IDs, not exit codes supplied by the caller.
    #[serde(default)]
    pub check_ids: Vec<String>,
}
impl AcceptRequest {
    pub fn parse(v: &Value) -> Result<Self> {
        let input: Self = serde_json::from_value(v.clone())?;
        for field in [
            "client_request_id",
            "attempt_id",
            "submission_ref",
            "candidate_ref",
            "reason",
        ] {
            model::text(v, field)?;
        }
        if input.expected_revision < 1 || input.expected_feedback_observation_id < 0 {
            return Err(Error::invalid("acceptance revision/cursor is invalid"));
        }
        let mut ids = BTreeSet::new();
        for r in &input.reviews {
            if r.requirement_id.trim().is_empty()
                || r.rationale.trim().is_empty()
                || r.evidence.is_empty()
                || r.evidence.iter().any(|s| s.trim().is_empty())
                || !ids.insert(&r.requirement_id)
            {
                return Err(Error::invalid(
                    "every reviewed requirement needs a unique ID, rationale and evidence",
                ));
            }
        }
        let mut checks = BTreeSet::new();
        if input
            .check_ids
            .iter()
            .any(|id| id.trim().is_empty() || !checks.insert(id))
        {
            return Err(Error::invalid("check IDs must be nonempty and unique"));
        }
        Ok(input)
    }
    pub fn validate_coverage(&self, spec: &TaskSpec) -> Result<()> {
        let expected: BTreeSet<_> = spec.requirements.iter().map(|r| r.id.as_str()).collect();
        let actual: BTreeSet<_> = self
            .reviews
            .iter()
            .map(|r| r.requirement_id.as_str())
            .collect();
        if expected != actual {
            return Err(Error::new(
                "REVIEW_INCOMPLETE",
                "review must address exactly the frozen Task requirements",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InvalidateRequest {
    pub client_request_id: String,
    pub acceptance_operation_id: String,
    pub reason: String,
    pub evidence: Vec<String>,
}
impl InvalidateRequest {
    pub fn parse(v: &Value) -> Result<Self> {
        let input: Self = serde_json::from_value(v.clone())?;
        for field in ["client_request_id", "acceptance_operation_id", "reason"] {
            model::text(v, field)?;
        }
        if input.evidence.is_empty() || input.evidence.iter().any(|s| s.trim().is_empty()) {
            return Err(Error::invalid(
                "invalidation needs concrete evidence references",
            ));
        }
        Ok(input)
    }
}
