//! Submission is an immutable proposal, not a successful check or Task acceptance.
use crate::{
    error::{Error, Result},
    model::{self, TaskSpec},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimStatus {
    Met,
    NotMet,
    Deferred,
    Unreported,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequirementClaim {
    pub requirement_id: String,
    pub status: ClaimStatus,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub note: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubmitRequest {
    pub client_request_id: String,
    pub attempt_id: String,
    pub expected_revision: i64,
    pub expected_submission_ref: Option<String>,
    pub candidate_ref: String,
    pub summary: String,
    #[serde(default)]
    pub claims: Vec<RequirementClaim>,
}
impl SubmitRequest {
    pub fn parse(v: &Value) -> Result<Self> {
        // Missing is not an instruction to replace whichever submission is current.
        if v.get("expected_submission_ref").is_none() {
            return Err(Error::invalid(
                "expected_submission_ref is required; use null for the first submission",
            ));
        }
        let input: Self = serde_json::from_value(v.clone())?;
        for field in [
            "client_request_id",
            "attempt_id",
            "candidate_ref",
            "summary",
        ] {
            model::text(v, field)?;
        }
        if input.expected_revision < 1
            || input
                .expected_submission_ref
                .as_ref()
                .is_some_and(|s| s.trim().is_empty())
        {
            return Err(Error::invalid("expected revision/reference is invalid"));
        }
        let mut ids = BTreeSet::new();
        for c in &input.claims {
            if c.requirement_id.trim().is_empty()
                || !ids.insert(&c.requirement_id)
                || c.evidence.iter().any(|s| s.trim().is_empty())
            {
                return Err(Error::invalid(
                    "claim IDs must be unique and evidence references nonempty",
                ));
            }
            if c.status == ClaimStatus::Met && c.evidence.is_empty() {
                return Err(Error::invalid(
                    "a met claim needs an evidence reference; references remain submitter assertions",
                ));
            }
            if matches!(c.status, ClaimStatus::NotMet | ClaimStatus::Deferred)
                && c.note.trim().is_empty()
            {
                return Err(Error::invalid(
                    "not_met/deferred claims need a concrete note",
                ));
            }
        }
        Ok(input)
    }
    /// Completeness comes from the immutable Task, never from the worker's list.
    pub fn normalized_claims(&self, spec: &TaskSpec) -> Result<Vec<RequirementClaim>> {
        let expected: BTreeSet<_> = spec.requirements.iter().map(|r| r.id.as_str()).collect();
        let supplied: BTreeMap<_, _> = self
            .claims
            .iter()
            .map(|c| (c.requirement_id.as_str(), c))
            .collect();
        if supplied.keys().any(|id| !expected.contains(id)) {
            return Err(Error::invalid(
                "claim names a requirement absent from this Task revision",
            ));
        }
        Ok(spec
            .requirements
            .iter()
            .map(|r| {
                supplied
                    .get(r.id.as_str())
                    .map(|c| (*c).clone())
                    .unwrap_or_else(|| RequirementClaim {
                        requirement_id: r.id.clone(),
                        status: ClaimStatus::Unreported,
                        evidence: Vec::new(),
                        note: "No claim supplied for this requirement.".into(),
                    })
            })
            .collect())
    }
}
pub fn claim_counts(claims: &[RequirementClaim]) -> Value {
    let count = |status| claims.iter().filter(|c| c.status == status).count();
    json!({"total":claims.len(),"met":count(ClaimStatus::Met),"not_met":count(ClaimStatus::NotMet),
        "deferred":count(ClaimStatus::Deferred),"unreported":count(ClaimStatus::Unreported)})
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChangeRequest {
    pub client_request_id: String,
    pub attempt_id: String,
    pub expected_revision: i64,
    pub submission_ref: String,
    pub candidate_ref: String,
    pub finding_id: String,
    pub reason: String,
    #[serde(default)]
    pub requirement_ids: Vec<String>,
    pub evidence: Vec<String>,
}
impl ChangeRequest {
    pub fn parse(v: &Value) -> Result<Self> {
        let input: Self = serde_json::from_value(v.clone())?;
        for field in [
            "client_request_id",
            "attempt_id",
            "submission_ref",
            "candidate_ref",
            "finding_id",
            "reason",
        ] {
            model::text(v, field)?;
        }
        let mut ids = BTreeSet::new();
        if input.expected_revision < 1
            || input.evidence.is_empty()
            || input.evidence.iter().any(|s| s.trim().is_empty())
            || input
                .requirement_ids
                .iter()
                .any(|s| s.trim().is_empty() || !ids.insert(s))
        {
            return Err(Error::invalid(
                "review needs a positive revision, evidence and unique requirement IDs",
            ));
        }
        Ok(input)
    }
    pub fn finding(&self) -> Value {
        // Transport retries and new CLI invocations share the same domain identity.
        json!({"attempt_id":self.attempt_id,"task_revision":self.expected_revision,
            "submission_ref":self.submission_ref,"candidate_ref":self.candidate_ref,
            "finding_id":self.finding_id,"reason":self.reason,
            "requirement_ids":self.requirement_ids,"evidence":self.evidence})
    }
}
