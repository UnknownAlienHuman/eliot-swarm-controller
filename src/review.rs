//! Typed manual review requests and immutable review-slot identities.
//!
//! The Store owns authorization and persistence. These types only define the
//! narrow wire contract; they do not grant reviewer or manager authority.

use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use swarm_kernel::reviews as review_contract;

pub(crate) const PRIMARY_REVIEW_SLOT: &str = "primary";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewAssignRequest {
    pub client_request_id: String,
    pub attempt_id: String,
    pub expected_revision: i64,
    pub submission_ref: String,
    pub candidate_ref: String,
    #[serde(default)]
    pub reviewer_client_id: Option<String>,
    #[serde(default)]
    pub review_profile: Option<String>,
    #[serde(default)]
    pub replaces_review_assignment_id: Option<String>,
    #[serde(default)]
    pub replacement_reason: Option<String>,
    #[serde(default)]
    pub replacement_evidence_refs: Vec<String>,
}

impl ReviewAssignRequest {
    pub(crate) fn for_automation(
        client_request_id: String,
        attempt_id: String,
        expected_revision: i64,
        submission_ref: String,
        candidate_ref: String,
        review_profile: String,
    ) -> Self {
        Self {
            client_request_id,
            attempt_id,
            expected_revision,
            submission_ref,
            candidate_ref,
            reviewer_client_id: None,
            review_profile: Some(review_profile),
            replaces_review_assignment_id: None,
            replacement_reason: None,
            replacement_evidence_refs: Vec::new(),
        }
    }

    pub(crate) fn parse(value: &Value) -> Result<Self> {
        let request: Self = serde_json::from_value(value.clone())?;
        for field in [
            "client_request_id",
            "attempt_id",
            "submission_ref",
            "candidate_ref",
        ] {
            model::text(value, field)?;
        }
        if request.expected_revision < 1 {
            return Err(Error::invalid("expected_revision must be positive"));
        }
        match (&request.reviewer_client_id, &request.review_profile) {
            (Some(client), None) if !client.trim().is_empty() => {}
            (None, Some(profile)) if !profile.trim().is_empty() => {}
            (Some(_), Some(_)) => {
                return Err(Error::invalid(
                    "choose one exact reviewer_client_id or review_profile",
                ));
            }
            _ => {
                return Err(Error::invalid(
                    "review assignment requires an exact reviewer or configured profile",
                ));
            }
        }
        let replacement_fields = [
            request.replaces_review_assignment_id.is_some(),
            request.replacement_reason.is_some(),
            !request.replacement_evidence_refs.is_empty(),
        ];
        if replacement_fields.iter().any(|present| *present)
            && replacement_fields.iter().any(|present| !*present)
        {
            return Err(Error::invalid(
                "replacement requires the prior assignment ID, reason, and evidence refs",
            ));
        }
        if request
            .replacement_reason
            .as_deref()
            .is_some_and(|reason| reason.trim().is_empty())
            || request
                .replacement_evidence_refs
                .iter()
                .any(|reference| reference.trim().is_empty())
        {
            return Err(Error::invalid(
                "replacement reason and evidence refs must be nonempty",
            ));
        }
        Ok(request)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReviewVerdict {
    Pass,
    ChangesRequested,
    Inconclusive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ReviewCoverage {
    Complete,
    Partial,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewFinding {
    pub finding_id: String,
    pub requirement_ids: Vec<String>,
    pub reason: String,
    pub evidence_refs: Vec<String>,
    pub requested_change: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewSubmitRequest {
    pub client_request_id: String,
    pub review_assignment_id: String,
    pub submission_ref: String,
    pub candidate_ref: String,
    pub verdict: ReviewVerdict,
    pub coverage: ReviewCoverage,
    pub findings: Vec<ReviewFinding>,
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub requirement_reviews: Vec<crate::acceptance::RequirementReview>,
}

impl ReviewSubmitRequest {
    pub(crate) fn parse(value: &Value) -> Result<Self> {
        let request: Self = serde_json::from_value(value.clone())?;
        for field in [
            "client_request_id",
            "review_assignment_id",
            "submission_ref",
            "candidate_ref",
        ] {
            model::text(value, field)?;
        }
        review_contract::validate_submit_request(value).map_err(|error| match error {
            review_contract::ReviewValidationError::Evidence => Error::invalid(
                "review result needs nonempty retained evidence refs",
            ),
            review_contract::ReviewValidationError::Findings => Error::invalid(
                "findings need unique IDs, reasons, requested changes, and evidence refs",
            ),
            review_contract::ReviewValidationError::Verdict => match request.verdict {
                ReviewVerdict::Pass => Error::invalid(
                    "pass requires complete coverage and no unresolved findings",
                ),
                ReviewVerdict::ChangesRequested => Error::invalid(
                    "changes_requested requires at least one actionable finding",
                ),
                ReviewVerdict::Inconclusive => Error::invalid("review verdict is invalid"),
            },
            review_contract::ReviewValidationError::RequirementReviews => {
                let mut requirement_ids = BTreeSet::new();
                let malformed = request.requirement_reviews.iter().any(|review| {
                    review.requirement_id.trim().is_empty()
                        || !requirement_ids.insert(review.requirement_id.as_str())
                        || review.rationale.trim().is_empty()
                        || review.evidence.is_empty()
                        || review
                            .evidence
                            .iter()
                            .any(|reference| reference.trim().is_empty())
                });
                if malformed {
                    Error::invalid(
                        "requirement reviews need unique IDs, rationale, and evidence refs",
                    )
                } else {
                    Error::invalid(
                        "requirement reviews require a complete pass with no unresolved findings",
                    )
                }
            }
            review_contract::ReviewValidationError::Identity => {
                Error::invalid("review submission identity fields must be nonempty")
            }
            review_contract::ReviewValidationError::Coverage => {
                Error::invalid("review coverage is invalid")
            }
            review_contract::ReviewValidationError::Shape => {
                Error::invalid("review submission fields are invalid")
            }
            _ => Error::invalid("review submission is invalid"),
        })?;
        Ok(request)
    }

    pub(crate) fn validate_findings(&self, requirement_ids: &BTreeSet<String>) -> Result<()> {
        let findings = serde_json::to_value(&self.findings)?;
        review_contract::validate_finding_requirements(&findings, requirement_ids).map_err(
            |error| match error {
                review_contract::ReviewValidationError::RequirementCoverage
                    if self
                        .findings
                        .iter()
                        .any(|finding| finding.requirement_ids.is_empty()) =>
                {
                    Error::invalid("each actionable finding must name a Task requirement")
                }
                review_contract::ReviewValidationError::RequirementCoverage => Error::invalid(
                    "finding requirement IDs must be unique and belong to this Task revision",
                ),
                _ => Error::new("REVIEW_RESULT_DAMAGED", error.to_string()),
            },
        )
    }

    pub(crate) fn validate_requirement_reviews(
        &self,
        requirement_ids: &BTreeSet<String>,
    ) -> Result<()> {
        if self.requirement_reviews.is_empty() {
            return Ok(());
        }
        let reviews = serde_json::to_value(&self.requirement_reviews)?;
        review_contract::validate_requirement_coverage(&reviews, requirement_ids).map_err(
            |error| match error {
                review_contract::ReviewValidationError::RequirementCoverage => Error::new(
                    "REVIEW_INCOMPLETE",
                    "structured review must address exactly the frozen Task requirements",
                ),
                _ => Error::new("REVIEW_RESULT_DAMAGED", error.to_string()),
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReviewSlotIdentity {
    pub task_id: String,
    pub attempt_id: String,
    pub task_revision: i64,
    pub submission_ref: String,
    pub candidate_ref: String,
    pub review_policy_generation: String,
    pub review_slot: String,
}

impl ReviewSlotIdentity {
    pub(crate) fn digest(&self) -> Result<String> {
        // The reservation key names a semantic action on one exact candidate.
        // Review policy generation is retained on the assignment/result and
        // checked against that fact, but it must not split the candidate slot.
        let key = json!({
            "task_id":&self.task_id,
            "task_revision":self.task_revision,
            "attempt_id":&self.attempt_id,
            "submission_ref":&self.submission_ref,
            "candidate_ref":&self.candidate_ref,
            "action":"review.assign",
            "review_slot":&self.review_slot,
        });
        Ok(model::digest(model::canonical(&key)?.as_bytes()))
    }
}

pub(crate) fn review_policy_generation(spec: &crate::model::TaskSpec) -> Result<String> {
    // The first implementation has one required primary slot. Its generation
    // changes only when requirement/phase/acceptance/source semantics change;
    // reviewer routing preferences are deliberately excluded.
    let policy = json!({
        "phase": spec.phase,
        "requirements": spec.requirements,
        "acceptance": spec.acceptance,
        "scope": spec.scope,
        "source_refs": spec.source_refs,
        "source_index": spec.source_index,
    });
    Ok(model::digest(model::canonical(&policy)?.as_bytes()))
}
