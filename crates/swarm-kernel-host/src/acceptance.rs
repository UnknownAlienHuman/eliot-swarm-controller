//! Acceptance is an explicit decision over a sealed submission, never a model stop.
use crate::{
    error::{Error, Result},
    model::TaskSpec,
};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CheckProfileRef {
    pub profile_id: String,
    pub profile_revision: String,
}

/// The retained legacy V1 acceptance policy. It is only a
/// retained reader for frozen historical Tasks and Attempts;
/// current task.create/task.revise admission never accepts it.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LegacyAcceptancePolicyV1 {
    pub required_check_profiles: Vec<CheckProfileRef>,
}

/// The current V2 acceptance policy marker. It carries no
/// global required_check_profiles: the exact profile union is
/// derived once from the frozen per-requirement verification
/// policies by the provider-neutral requirement_evidence_plan.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptancePolicyV2 {
    pub schema_id: String,
    pub schema_version: i64,
}

/// The retained historical shape and the current write shape are
/// distinct types. No untagged fallback is used: a policy is
/// either the exact legacy V1 object or the exact V2 marker.
#[derive(Debug, Clone)]
pub enum RetainedAcceptancePolicy {
    LegacyV1(LegacyAcceptancePolicyV1),
    V2(AcceptancePolicyV2),
}

impl RetainedAcceptancePolicy {
    /// Decode one retained policy value into its exact shape.
    /// Absent schema marker means legacy V1; exact schema_id
    /// and schema_version 2 means V2; mixed, partial or unknown
    /// markers reject.
    pub fn parse(value: &serde_json::Value) -> Result<Self> {
        let object = value.as_object().ok_or_else(|| {
            Error::new(
                "ACCEPTANCE_POLICY_VERSION",
                "acceptance policy must be a closed object",
            )
        })?;
        let schema_id = value.get("schema_id").and_then(serde_json::Value::as_str);
        let schema_version = value
            .get("schema_version")
            .and_then(serde_json::Value::as_i64);
        match (schema_id, schema_version) {
            (None, None) if object.len() == 1 && object.contains_key("required_check_profiles") => {
                let legacy: LegacyAcceptancePolicyV1 = serde_json::from_value(value.clone())?;
                Ok(Self::LegacyV1(legacy))
            }
            (Some("swarm.acceptance_policy"), Some(2))
                if object.len() == 2
                    && object.contains_key("schema_id")
                    && object.contains_key("schema_version") =>
            {
                let v2: AcceptancePolicyV2 = serde_json::from_value(value.clone())?;
                Ok(Self::V2(v2))
            }
            _ => Err(Error::new(
                "ACCEPTANCE_POLICY_VERSION",
                "acceptance policy must be the exact legacy V1 object or the exact V2 marker",
            )),
        }
    }

    pub(crate) fn validate_retained(&self) -> Result<()> {
        match self {
            Self::LegacyV1(legacy) => {
                let value = serde_json::to_value(legacy)?;
                swarm_kernel::acceptance::validate_policy(&value)
                    .map_err(acceptance_validation_error)
            }
            Self::V2(v2) => v2.validate(),
        }
    }

    /// The exact V2 marker for the current write form.
    pub fn v2() -> AcceptancePolicyV2 {
        AcceptancePolicyV2 {
            schema_id: "swarm.acceptance_policy".to_owned(),
            schema_version: 2,
        }
    }
}

impl Serialize for RetainedAcceptancePolicy {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::LegacyV1(policy) => policy.serialize(serializer),
            Self::V2(policy) => policy.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for RetainedAcceptancePolicy {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        Self::parse(&value).map_err(serde::de::Error::custom)
    }
}

/// Set by the task author, not by the submitting worker. The
/// current write form is the exact V2 marker; the exact
/// required CheckRun profiles are derived from the frozen
/// per-requirement verification policies.
pub type AcceptancePolicy = AcceptancePolicyV2;

impl AcceptancePolicyV2 {
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
