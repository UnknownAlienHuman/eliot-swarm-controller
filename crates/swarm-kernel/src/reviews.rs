//! Provider-neutral review result and disposition contracts.
//!
//! This module validates the bounded shape and semantic result of an assigned
//! review. The Store remains responsible for assignment lookup, SQL
//! transactions, authorization, Task policy, and operation/observation
//! identity. No provider, adapter, database, or native effect is represented
//! here.

use serde_json::Value;
use std::{collections::BTreeSet, fmt};

const SUBMIT_FIELDS: &[&str] = &[
    "client_request_id",
    "review_assignment_id",
    "submission_ref",
    "candidate_ref",
    "verdict",
    "coverage",
    "findings",
    "evidence_refs",
];
const SUBMIT_OPTIONAL_FIELDS: &[&str] = &["requirement_reviews"];
const FINDING_FIELDS: &[&str] = &[
    "finding_id",
    "requirement_ids",
    "reason",
    "evidence_refs",
    "requested_change",
];
const REQUIREMENT_REVIEW_FIELDS: &[&str] = &["requirement_id", "rationale", "evidence"];
const SLOT_IDENTITY_FIELDS: &[&str] = &[
    "task_id",
    "attempt_id",
    "task_revision",
    "submission_ref",
    "candidate_ref",
    "review_policy_generation",
    "review_slot",
];
const RESULT_INPUT_FIELDS: &[&str] = &[
    "review_assignment_id",
    "submission_ref",
    "candidate_ref",
    "verdict",
    "coverage",
    "findings",
    "evidence_refs",
];
const RESULT_INPUT_OPTIONAL_FIELDS: &[&str] = &["requirement_reviews"];
const RESULT_FIELDS: &[&str] = &[
    "operation_id",
    "review_assignment_id",
    "task_id",
    "attempt_id",
    "task_revision",
    "submission_ref",
    "candidate_ref",
    "candidate_sha256",
    "review_policy_generation",
    "review_slot",
    "reviewer_client_id",
    "sponsor_client_id",
    "verdict",
    "coverage",
    "findings",
    "evidence_refs",
    "evidence_level",
    "applicability",
    "task_transition",
    "task_feedback_applied",
    "acceptance_changed",
    "publication_started",
    "coalesced",
];
const RESULT_OPTIONAL_FIELDS: &[&str] = &["requirement_reviews"];
const RESULT_RECORD_FIELDS: &[&str] = &[
    "schema_version",
    "review_assignment_id",
    "operation_id",
    "identity",
    "input",
    "result",
];
const DISPOSITION_FIELDS: &[&str] = &[
    "schema_version",
    "kind",
    "review_assignment_id",
    "operation_id",
    "disposition",
    "review_result_operation_id",
    "reason",
    "evidence_refs",
    "finding_ids",
    "decided_by",
    "identity",
    "task_feedback_operation_id",
];

/// Structural or semantic failure in a review value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewValidationError {
    Shape,
    Schema,
    Identity,
    Verdict,
    Coverage,
    Findings,
    RequirementReviews,
    RequirementCoverage,
    Evidence,
    Applicability,
    SideEffect,
    Disposition,
    EventIdentity,
    NotActionable,
    FindingNotFound,
}

impl fmt::Display for ReviewValidationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Shape => "review value has an invalid object or field shape",
            Self::Schema => "review value has an unsupported schema version",
            Self::Identity => "review value has an invalid identity",
            Self::Verdict => "review result has an unsupported verdict",
            Self::Coverage => "review result has an unsupported coverage value",
            Self::Findings => "review findings are malformed or duplicated",
            Self::RequirementReviews => "structured requirement reviews are malformed",
            Self::RequirementCoverage => {
                "structured review does not address exactly the required requirements"
            }
            Self::Evidence => "review evidence is missing or malformed",
            Self::Applicability => "review result has an unsupported applicability",
            Self::SideEffect => "review result claims a Task or native side effect",
            Self::Disposition => "review disposition is not allowed or is malformed",
            Self::EventIdentity => "review result event identity is inconsistent",
            Self::NotActionable => "review result is not an actionable current return",
            Self::FindingNotFound => "finding is not present in the review result",
        })
    }
}

impl std::error::Error for ReviewValidationError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewVerdict {
    Pass,
    ChangesRequested,
    Inconclusive,
}

impl ReviewVerdict {
    pub fn parse(value: &Value) -> Result<Self, ReviewValidationError> {
        match value.as_str() {
            Some("pass") => Ok(Self::Pass),
            Some("changes_requested") => Ok(Self::ChangesRequested),
            Some("inconclusive") => Ok(Self::Inconclusive),
            _ => Err(ReviewValidationError::Verdict),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::ChangesRequested => "changes_requested",
            Self::Inconclusive => "inconclusive",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewCoverage {
    Complete,
    Partial,
}

impl ReviewCoverage {
    pub fn parse(value: &Value) -> Result<Self, ReviewValidationError> {
        match value.as_str() {
            Some("complete") => Ok(Self::Complete),
            Some("partial") => Ok(Self::Partial),
            _ => Err(ReviewValidationError::Coverage),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Complete => "complete",
            Self::Partial => "partial",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewApplicability {
    CurrentCandidate,
    HistoricalCandidate,
}

impl ReviewApplicability {
    fn parse(value: &Value) -> Result<Self, ReviewValidationError> {
        match value.as_str() {
            Some("current_candidate") => Ok(Self::CurrentCandidate),
            Some("historical_candidate") => Ok(Self::HistoricalCandidate),
            _ => Err(ReviewValidationError::Applicability),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReviewResult {
    pub verdict: ReviewVerdict,
    pub coverage: ReviewCoverage,
    pub applicability: ReviewApplicability,
    pub finding_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReviewDisposition {
    ReturnForCorrection,
}

impl ReviewDisposition {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReturnForCorrection => "return_for_correction",
        }
    }
}

fn exact_object<'a>(
    value: &'a Value,
    required: &[&str],
    optional: &[&str],
) -> Result<&'a serde_json::Map<String, Value>, ReviewValidationError> {
    let object = value.as_object().ok_or(ReviewValidationError::Shape)?;
    if required.iter().any(|key| !object.contains_key(*key))
        || object.keys().any(|key| {
            !required.contains(&key.as_str()) && !optional.contains(&key.as_str())
        })
    {
        return Err(ReviewValidationError::Shape);
    }
    Ok(object)
}

fn required_text(value: &Value, key: &str) -> Result<(), ReviewValidationError> {
    if value
        .get(key)
        .and_then(Value::as_str)
        .is_some_and(|text| !text.trim().is_empty())
    {
        Ok(())
    } else {
        Err(ReviewValidationError::Identity)
    }
}

fn nonempty_text_array(
    value: &Value,
    error: ReviewValidationError,
) -> Result<(), ReviewValidationError> {
    let values = value.as_array().ok_or(error)?;
    if values.is_empty()
        || values
            .iter()
            .any(|item| item.as_str().is_none_or(|text| text.trim().is_empty()))
    {
        return Err(error);
    }
    Ok(())
}

fn valid_sha256(value: &Value) -> bool {
    value.as_str().is_some_and(|text| {
        text.len() == 64 && text.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn validate_findings_shape(value: &Value) -> Result<(), ReviewValidationError> {
    let findings = value.as_array().ok_or(ReviewValidationError::Findings)?;
    let mut finding_ids = BTreeSet::new();
    for finding in findings {
        exact_object(finding, FINDING_FIELDS, &[])?;
        let finding_id = finding["finding_id"]
            .as_str()
            .filter(|text| !text.trim().is_empty())
            .ok_or(ReviewValidationError::Findings)?;
        if !finding_ids.insert(finding_id) {
            return Err(ReviewValidationError::Findings);
        }
        let requirement_ids = finding["requirement_ids"]
            .as_array()
            .ok_or(ReviewValidationError::Findings)?;
        if requirement_ids
            .iter()
            .any(|item| item.as_str().is_none_or(|text| text.trim().is_empty()))
        {
            return Err(ReviewValidationError::Findings);
        }
        required_text(finding, "reason")
            .map_err(|_| ReviewValidationError::Findings)?;
        required_text(finding, "requested_change")
            .map_err(|_| ReviewValidationError::Findings)?;
        nonempty_text_array(&finding["evidence_refs"], ReviewValidationError::Evidence)?;
    }
    Ok(())
}

fn validate_requirement_reviews_shape(value: &Value) -> Result<(), ReviewValidationError> {
    let reviews = value
        .as_array()
        .ok_or(ReviewValidationError::RequirementReviews)?;
    let mut requirement_ids = BTreeSet::new();
    for review in reviews {
        exact_object(review, REQUIREMENT_REVIEW_FIELDS, &[])?;
        let requirement_id = review["requirement_id"]
            .as_str()
            .filter(|text| !text.trim().is_empty())
            .ok_or(ReviewValidationError::RequirementReviews)?;
        if !requirement_ids.insert(requirement_id) {
            return Err(ReviewValidationError::RequirementReviews);
        }
        required_text(review, "rationale")
            .map_err(|_| ReviewValidationError::RequirementReviews)?;
        nonempty_text_array(&review["evidence"], ReviewValidationError::Evidence)?;
    }
    Ok(())
}

fn validate_slot_identity(value: &Value) -> Result<(), ReviewValidationError> {
    exact_object(value, SLOT_IDENTITY_FIELDS, &[])?;
    for field in [
        "task_id",
        "attempt_id",
        "submission_ref",
        "candidate_ref",
        "review_policy_generation",
        "review_slot",
    ] {
        required_text(value, field)?;
    }
    if value["task_revision"].as_i64().is_none_or(|revision| revision < 1) {
        return Err(ReviewValidationError::Identity);
    }
    Ok(())
}

fn validate_review_semantics(
    verdict: ReviewVerdict,
    coverage: ReviewCoverage,
    finding_count: usize,
    requirement_reviews: Option<&Value>,
) -> Result<(), ReviewValidationError> {
    match verdict {
        ReviewVerdict::Pass if coverage != ReviewCoverage::Complete || finding_count != 0 => {
            return Err(ReviewValidationError::Verdict);
        }
        ReviewVerdict::ChangesRequested if finding_count == 0 => {
            return Err(ReviewValidationError::Verdict);
        }
        _ => {}
    }
    if requirement_reviews.is_some_and(|reviews| {
        !reviews.as_array().is_some_and(|items| items.is_empty())
    }) && (verdict != ReviewVerdict::Pass
        || coverage != ReviewCoverage::Complete
        || finding_count != 0)
    {
        return Err(ReviewValidationError::RequirementReviews);
    }
    Ok(())
}

fn validate_result_input(value: &Value) -> Result<(), ReviewValidationError> {
    exact_object(value, RESULT_INPUT_FIELDS, RESULT_INPUT_OPTIONAL_FIELDS)?;
    for field in ["review_assignment_id", "submission_ref", "candidate_ref"] {
        required_text(value, field)?;
    }
    let verdict = ReviewVerdict::parse(&value["verdict"])?;
    let coverage = ReviewCoverage::parse(&value["coverage"])?;
    validate_findings_shape(&value["findings"])?;
    nonempty_text_array(&value["evidence_refs"], ReviewValidationError::Evidence)?;
    if let Some(requirement_reviews) = value.get("requirement_reviews") {
        validate_requirement_reviews_shape(requirement_reviews)?;
    }
    let finding_count = value["findings"].as_array().map_or(0, Vec::len);
    validate_review_semantics(
        verdict,
        coverage,
        finding_count,
        value.get("requirement_reviews"),
    )
}

/// Validate the provider-neutral shape and result semantics of an incoming
/// assigned-review submission. SQL identity and reviewer authorization remain
/// in the Store.
pub fn validate_submit_request(value: &Value) -> Result<(), ReviewValidationError> {
    exact_object(value, SUBMIT_FIELDS, SUBMIT_OPTIONAL_FIELDS)?;
    for field in [
        "client_request_id",
        "review_assignment_id",
        "submission_ref",
        "candidate_ref",
    ] {
        required_text(value, field)?;
    }
    let verdict = ReviewVerdict::parse(&value["verdict"])?;
    let coverage = ReviewCoverage::parse(&value["coverage"])?;
    validate_findings_shape(&value["findings"])?;
    nonempty_text_array(&value["evidence_refs"], ReviewValidationError::Evidence)?;
    if let Some(requirement_reviews) = value.get("requirement_reviews") {
        validate_requirement_reviews_shape(requirement_reviews)?;
    }
    validate_review_semantics(
        verdict,
        coverage,
        value["findings"].as_array().map_or(0, Vec::len),
        value.get("requirement_reviews"),
    )
}

/// Require every finding requirement ID to belong to the retained Task
/// requirement set. The set itself is supplied by Store policy state.
pub fn validate_finding_requirements(
    findings: &Value,
    requirement_ids: &BTreeSet<String>,
) -> Result<(), ReviewValidationError> {
    validate_findings_shape(findings)?;
    for finding in findings
        .as_array()
        .ok_or(ReviewValidationError::Findings)?
    {
        let ids = finding["requirement_ids"]
            .as_array()
            .ok_or(ReviewValidationError::Findings)?;
        if ids.is_empty() {
            return Err(ReviewValidationError::RequirementCoverage);
        }
        let mut seen = BTreeSet::new();
        for id in ids {
            let id = id
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .ok_or(ReviewValidationError::RequirementCoverage)?;
            if !requirement_ids.contains(id) || !seen.insert(id) {
                return Err(ReviewValidationError::RequirementCoverage);
            }
        }
    }
    Ok(())
}

/// Require structured acceptance evidence to cover the exact frozen
/// requirement set. Whether the Task is eligible for acceptance remains Store
/// policy and authorization.
pub fn validate_requirement_coverage(
    reviews: &Value,
    requirement_ids: &BTreeSet<String>,
) -> Result<(), ReviewValidationError> {
    validate_requirement_reviews_shape(reviews)?;
    let supplied = reviews
        .as_array()
        .ok_or(ReviewValidationError::RequirementReviews)?
        .iter()
        .map(|review| review["requirement_id"].as_str().unwrap_or_default())
        .collect::<BTreeSet<_>>();
    let expected = requirement_ids
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if supplied.len() != reviews.as_array().map_or(0, Vec::len) || supplied != expected {
        return Err(ReviewValidationError::RequirementCoverage);
    }
    Ok(())
}

/// Validate the result payload retained by the Store's review observation.
/// The checks cover result structure, allowed verdict/coverage, evidence, and
/// the promise that review submission itself has no Task/native side effect.
pub fn validate_result(value: &Value) -> Result<ReviewResult, ReviewValidationError> {
    exact_object(value, RESULT_FIELDS, RESULT_OPTIONAL_FIELDS)?;
    for field in [
        "operation_id",
        "review_assignment_id",
        "task_id",
        "attempt_id",
        "submission_ref",
        "candidate_ref",
        "review_policy_generation",
        "review_slot",
        "reviewer_client_id",
        "sponsor_client_id",
    ] {
        required_text(value, field)?;
    }
    if value["task_revision"].as_i64().is_none_or(|revision| revision < 1)
        || !valid_sha256(&value["candidate_sha256"])
    {
        return Err(ReviewValidationError::Identity);
    }
    let verdict = ReviewVerdict::parse(&value["verdict"])?;
    let coverage = ReviewCoverage::parse(&value["coverage"])?;
    validate_findings_shape(&value["findings"])?;
    nonempty_text_array(&value["evidence_refs"], ReviewValidationError::Evidence)?;
    if let Some(requirement_reviews) = value.get("requirement_reviews") {
        validate_requirement_reviews_shape(requirement_reviews)?;
    }
    if value["evidence_level"] != "assigned_auditor_report"
        || value["task_transition"] != "none"
        || value["task_feedback_applied"] != false
        || value["acceptance_changed"] != false
        || value["publication_started"] != false
        || value["coalesced"].as_bool().is_none()
    {
        return Err(ReviewValidationError::SideEffect);
    }
    let applicability = ReviewApplicability::parse(&value["applicability"])?;
    let finding_count = value["findings"].as_array().map_or(0, Vec::len);
    validate_review_semantics(
        verdict,
        coverage,
        finding_count,
        value.get("requirement_reviews"),
    )?;
    Ok(ReviewResult {
        verdict,
        coverage,
        applicability,
        finding_count,
    })
}

/// Validate the complete retained review.result observation envelope.
pub fn validate_result_record(value: &Value) -> Result<ReviewResult, ReviewValidationError> {
    exact_object(value, RESULT_RECORD_FIELDS, &[])?;
    if value["schema_version"] != 1 {
        return Err(ReviewValidationError::Schema);
    }
    required_text(value, "review_assignment_id")?;
    required_text(value, "operation_id")?;
    if value["identity"].as_object().is_none() || value["input"].as_object().is_none() {
        return Err(ReviewValidationError::Identity);
    }
    validate_slot_identity(&value["identity"])?;
    validate_result_input(&value["input"])?;
    let result = validate_result(&value["result"])?;
    if value["result"]["review_assignment_id"] != value["review_assignment_id"]
        || value["result"]["operation_id"] != value["operation_id"]
        || value["input"]["review_assignment_id"] != value["review_assignment_id"]
        || value["input"]["submission_ref"] != value["result"]["submission_ref"]
        || value["input"]["candidate_ref"] != value["result"]["candidate_ref"]
        || value["input"]["verdict"] != value["result"]["verdict"]
        || value["input"]["coverage"] != value["result"]["coverage"]
        || value["input"]["findings"] != value["result"]["findings"]
        || value["input"]["evidence_refs"] != value["result"]["evidence_refs"]
        || value["input"].get("requirement_reviews")
            != value["result"].get("requirement_reviews")
        || [
            "task_id",
            "attempt_id",
            "task_revision",
            "submission_ref",
            "candidate_ref",
            "review_policy_generation",
            "review_slot",
        ]
        .iter()
        .any(|field| value["result"][*field] != value["identity"][*field])
    {
        return Err(ReviewValidationError::EventIdentity);
    }
    Ok(result)
}

/// Validate the event key/operation identity around a retained review.result
/// record. SQL lookup and event paging stay in the Store.
pub fn validate_result_event(
    value: &Value,
    assignment_id: &str,
    operation_id: &str,
) -> Result<ReviewResult, ReviewValidationError> {
    if assignment_id.trim().is_empty() || operation_id.trim().is_empty() {
        return Err(ReviewValidationError::EventIdentity);
    }
    let result = validate_result_record(value)?;
    if value["review_assignment_id"] != assignment_id
        || value["operation_id"] != operation_id
    {
        return Err(ReviewValidationError::EventIdentity);
    }
    Ok(result)
}

/// Return one finding only when the retained result authorizes a current
/// correction. Caller policy still decides whether the manager may act.
pub fn actionable_finding<'a>(
    value: &'a Value,
    finding_id: &str,
) -> Result<&'a Value, ReviewValidationError> {
    let result = validate_result(value)?;
    if result.verdict != ReviewVerdict::ChangesRequested
        || result.applicability != ReviewApplicability::CurrentCandidate
    {
        return Err(ReviewValidationError::NotActionable);
    }
    value["findings"]
        .as_array()
        .and_then(|findings| {
            findings
                .iter()
                .find(|finding| finding["finding_id"] == finding_id)
        })
        .ok_or(ReviewValidationError::FindingNotFound)
}

/// Validate the one manager disposition currently emitted by Store feedback.
/// This is a fact contract only; authorization and exact identity matching
/// remain in Store SQL/transaction code.
pub fn validate_disposition(
    value: &Value,
) -> Result<ReviewDisposition, ReviewValidationError> {
    exact_object(value, DISPOSITION_FIELDS, &[])?;
    if value["schema_version"] != 1 || value["kind"] != "review.disposition" {
        return Err(ReviewValidationError::Schema);
    }
    for field in [
        "review_assignment_id",
        "operation_id",
        "review_result_operation_id",
        "decided_by",
        "task_feedback_operation_id",
    ] {
        required_text(value, field)?;
    }
    if value["disposition"] != ReviewDisposition::ReturnForCorrection.as_str() {
        return Err(ReviewValidationError::Disposition);
    }
    required_text(value, "reason")
        .map_err(|_| ReviewValidationError::Disposition)?;
    nonempty_text_array(&value["evidence_refs"], ReviewValidationError::Evidence)?;
    nonempty_text_array(&value["finding_ids"], ReviewValidationError::Findings)?;
    validate_slot_identity(&value["identity"])?;
    Ok(ReviewDisposition::ReturnForCorrection)
}
