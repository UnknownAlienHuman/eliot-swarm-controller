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
        if request.evidence_refs.is_empty()
            || request
                .evidence_refs
                .iter()
                .any(|reference| reference.trim().is_empty())
        {
            return Err(Error::invalid(
                "review result needs nonempty retained evidence refs",
            ));
        }
        let mut finding_ids = BTreeSet::new();
        if request.findings.iter().any(|finding| {
            finding.finding_id.trim().is_empty()
                || !finding_ids.insert(finding.finding_id.as_str())
                || finding.reason.trim().is_empty()
                || finding.requested_change.trim().is_empty()
                || finding.evidence_refs.is_empty()
                || finding
                    .evidence_refs
                    .iter()
                    .any(|reference| reference.trim().is_empty())
        }) {
            return Err(Error::invalid(
                "findings need unique IDs, reasons, requested changes, and evidence refs",
            ));
        }
        match request.verdict {
            ReviewVerdict::Pass
                if request.coverage != ReviewCoverage::Complete || !request.findings.is_empty() =>
            {
                return Err(Error::invalid(
                    "pass requires complete coverage and no unresolved findings",
                ));
            }
            ReviewVerdict::ChangesRequested if request.findings.is_empty() => {
                return Err(Error::invalid(
                    "changes_requested requires at least one actionable finding",
                ));
            }
            _ => {}
        }
        let mut requirement_ids = BTreeSet::new();
        if request.requirement_reviews.iter().any(|review| {
            review.requirement_id.trim().is_empty()
                || !requirement_ids.insert(review.requirement_id.as_str())
                || review.rationale.trim().is_empty()
                || review.evidence.is_empty()
                || review
                    .evidence
                    .iter()
                    .any(|reference| reference.trim().is_empty())
        }) {
            return Err(Error::invalid(
                "requirement reviews need unique IDs, rationale, and evidence refs",
            ));
        }
        if !request.requirement_reviews.is_empty()
            && (request.verdict != ReviewVerdict::Pass
                || request.coverage != ReviewCoverage::Complete
                || !request.findings.is_empty())
        {
            return Err(Error::invalid(
                "requirement reviews require a complete pass with no unresolved findings",
            ));
        }
        Ok(request)
    }

    pub(crate) fn validate_findings(&self, requirement_ids: &BTreeSet<String>) -> Result<()> {
        for finding in &self.findings {
            if finding.requirement_ids.is_empty() {
                return Err(Error::invalid(
                    "each actionable finding must name a Task requirement",
                ));
            }
            let mut unique = BTreeSet::new();
            if finding
                .requirement_ids
                .iter()
                .any(|id| !requirement_ids.contains(id) || !unique.insert(id))
            {
                return Err(Error::invalid(
                    "finding requirement IDs must be unique and belong to this Task revision",
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_requirement_reviews(
        &self,
        requirement_ids: &BTreeSet<String>,
    ) -> Result<()> {
        if self.requirement_reviews.is_empty() {
            return Ok(());
        }
        if self.verdict != ReviewVerdict::Pass
            || self.coverage != ReviewCoverage::Complete
            || !self.findings.is_empty()
        {
            return Err(Error::invalid(
                "requirement reviews require a complete pass with no unresolved findings",
            ));
        }
        let supplied: BTreeSet<_> = self
            .requirement_reviews
            .iter()
            .map(|review| review.requirement_id.as_str())
            .collect();
        let expected: BTreeSet<_> = requirement_ids.iter().map(String::as_str).collect();
        if supplied.len() != self.requirement_reviews.len() || supplied != expected {
            return Err(Error::new(
                "REVIEW_INCOMPLETE",
                "structured review must address exactly the frozen Task requirements",
            ));
        }
        Ok(())
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
