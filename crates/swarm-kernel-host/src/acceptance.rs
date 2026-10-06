//! Acceptance is an explicit decision over a sealed submission, never a model stop.
use crate::{
    error::{Error, Result},
    model::TaskSpec,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

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
        let value = serde_json::to_value(self)?;
        swarm_kernel::acceptance::validate_policy(&value).map_err(acceptance_validation_error)
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
        swarm_kernel::acceptance::validate_accept_request(v)
            .map_err(acceptance_validation_error)?;
        Ok(input)
    }
    pub fn validate_coverage(&self, spec: &TaskSpec) -> Result<()> {
        let spec = serde_json::to_value(spec)?;
        let reviews = serde_json::to_value(&self.reviews)?;
        swarm_kernel::acceptance::validate_review_coverage(&spec, &reviews)
            .map_err(acceptance_validation_error)
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
            let _ = v
                .get(field)
                .and_then(Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| Error::invalid(format!("{field} must be a nonempty string")))?;
        }
        swarm_kernel::acceptance::validate_invalidation(v).map_err(acceptance_validation_error)?;
        Ok(input)
    }
}

fn acceptance_validation_error(error: swarm_kernel::acceptance::ValidationError) -> Error {
    Error::new(error.code(), error.message())
}
