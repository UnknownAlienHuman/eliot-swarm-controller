//! Provider-neutral acceptance policy and acceptance-fact validation.
//!
//! The root Store remains responsible for authorization, GM epoch checks,
//! SQLite queries and transactions, artifact/file verification, check parser
//! coverage and operation/observation ledgers.  This module owns only
//! deterministic predicates over already-read acceptance values.

use serde_json::Value;
use std::collections::BTreeSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidationError {
    code: &'static str,
    message: String,
}

impl ValidationError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    fn invalid(message: impl Into<String>) -> Self {
        Self::new("INVALID_PARAMS", message)
    }

    pub fn code(&self) -> &'static str {
        self.code
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

pub type Result<T> = std::result::Result<T, ValidationError>;

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ValidationError::invalid(format!("{field} must be a nonempty string")))
}

/// Validate the closed acceptance policy after root serde has checked its DTO
/// shape.  The error text is the existing AcceptancePolicy contract.
pub fn validate_policy(policy: &Value) -> Result<()> {
    let profiles = policy
        .get("required_check_profiles")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ValidationError::invalid("check profiles need unique IDs and explicit revisions")
        })?;
    let mut ids = BTreeSet::new();
    for profile in profiles {
        let profile_id = profile
            .get("profile_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let profile_revision = profile
            .get("profile_revision")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if profile_id.trim().is_empty()
            || profile_revision.trim().is_empty()
            || !ids.insert(profile_id)
        {
            return Err(ValidationError::invalid(
                "check profiles need unique IDs and explicit revisions",
            ));
        }
    }
    Ok(())
}

/// Validate the pure request fields after the root handler has performed its
/// strict serde decode.  Defaults remain represented by the typed request.
pub fn validate_accept_request(value: &Value) -> Result<()> {
    for field in [
        "client_request_id",
        "attempt_id",
        "submission_ref",
        "candidate_ref",
        "reason",
    ] {
        text(value, field)?;
    }
    let expected_revision = value
        .get("expected_revision")
        .and_then(Value::as_i64)
        .unwrap_or_default();
    let expected_feedback = value
        .get("expected_feedback_observation_id")
        .and_then(Value::as_i64)
        .unwrap_or(-1);
    if expected_revision < 1 || expected_feedback < 0 {
        return Err(ValidationError::invalid(
            "acceptance revision/cursor is invalid",
        ));
    }

    let reviews = value
        .get("reviews")
        .and_then(Value::as_array)
        .ok_or_else(|| ValidationError::invalid("reviews must be an array"))?;
    let mut ids = BTreeSet::new();
    for review in reviews {
        let requirement_id = review
            .get("requirement_id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let rationale = review
            .get("rationale")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let evidence = review
            .get("evidence")
            .and_then(Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if requirement_id.trim().is_empty()
            || rationale.trim().is_empty()
            || evidence.is_empty()
            || evidence
                .iter()
                .any(|item| item.as_str().is_none_or(|item| item.trim().is_empty()))
            || !ids.insert(requirement_id)
        {
            return Err(ValidationError::invalid(
                "every reviewed requirement needs a unique ID, rationale and evidence",
            ));
        }
    }

    let check_ids = value
        .get("check_ids")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let mut checks = BTreeSet::new();
    if check_ids.iter().any(|item| {
        item.as_str().is_none_or(|item| item.trim().is_empty())
            || !checks.insert(item.as_str().unwrap_or_default())
    }) {
        return Err(ValidationError::invalid(
            "check IDs must be nonempty and unique",
        ));
    }
    Ok(())
}

/// Validate that review evidence covers exactly the frozen Task requirements.
pub fn validate_review_coverage(spec: &Value, reviews: &Value) -> Result<()> {
    let expected: BTreeSet<&str> = spec
        .get("requirements")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("id").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    let actual: BTreeSet<&str> = reviews
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get("requirement_id").and_then(Value::as_str))
                .collect()
        })
        .unwrap_or_default();
    if expected != actual {
        return Err(ValidationError::new(
            "REVIEW_INCOMPLETE",
            "review must address exactly the frozen Task requirements",
        ));
    }
    Ok(())
}

/// Validate the evidence references required to revoke one acceptance.
pub fn validate_invalidation(value: &Value) -> Result<()> {
    let evidence = value
        .get("evidence")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    if evidence.is_empty()
        || evidence
            .iter()
            .any(|item| item.as_str().is_none_or(|item| item.trim().is_empty()))
    {
        return Err(ValidationError::invalid(
            "invalidation needs concrete evidence references",
        ));
    }
    Ok(())
}

/// Validate the exact live submission/Attempt/Task anchors before evidence
/// readback.  All values here have already been read by the Store.
pub fn validate_submission_scope(
    document: &Value,
    attempt: &Value,
    task: &Value,
    attempt_id: &str,
    expected_revision: i64,
    submission_ref: &str,
    candidate_ref: &str,
) -> Result<()> {
    if document["attempt_id"] != attempt_id
        || document["task_revision"] != expected_revision
        || document["candidate_ref"] != candidate_ref
        || attempt["submission_ref"] != submission_ref
        || attempt["candidate_ref"] != candidate_ref
        || attempt["task_revision"] != expected_revision
        || task["revision"] != expected_revision
        || task["current_attempt_id"] != attempt_id
        || task["state"] != "open"
        || !attempt["released_at_ms"].is_null()
        || !matches!(
            attempt["state"].as_str(),
            Some("submitted" | "needs_correction")
        )
    {
        return Err(ValidationError::new(
            "STALE_SUBMISSION",
            "acceptance no longer targets the current unreleased submission",
        ));
    }
    Ok(())
}

pub fn validate_reviewer_independence(document: &Value, reviewer_id: &str) -> Result<()> {
    if document["owner_id"] == reviewer_id || document["submitted_by"] == reviewer_id {
        return Err(ValidationError::new(
            "INDEPENDENT_REVIEW_REQUIRED",
            "the writer/submitter cannot accept its own proposal",
        ));
    }
    Ok(())
}

pub fn validate_acceptance_required(spec: &Value) -> Result<()> {
    if spec.get("acceptance").is_none_or(Value::is_null) {
        return Err(ValidationError::new(
            "ACCEPTANCE_POLICY_REQUIRED",
            "Task has no explicit acceptance policy; assignment and submission remain available",
        ));
    }
    Ok(())
}

pub fn validate_candidate_identity(
    candidate_digest: &str,
    candidate_length: u64,
    submitted_digest: Option<&str>,
    submitted_length: Option<u64>,
) -> Result<()> {
    if submitted_digest != Some(candidate_digest) || submitted_length != Some(candidate_length) {
        return Err(ValidationError::new(
            "CANDIDATE_DAMAGED",
            "candidate identity no longer matches the sealed submission",
        ));
    }
    Ok(())
}

/// Validate one persisted CheckRun against the current acceptance policy and
/// exact Attempt/candidate facts.  The Store retains parser/file proof.
pub fn validate_check(
    policy: &Value,
    check: &Value,
    attempt_id: &str,
    candidate_ref: &str,
    seen_profiles: &BTreeSet<String>,
) -> Result<String> {
    let spec = check.get("spec").unwrap_or(&Value::Null);
    let profile = text(spec, "profile_id")?;
    let revision = text(spec, "profile_revision")?;
    let required = policy
        .get("required_check_profiles")
        .and_then(Value::as_array)
        .unwrap_or(&[]);
    if !required.iter().any(|required| {
        required["profile_id"] == profile && required["profile_revision"] == revision
    }) || seen_profiles.contains(profile)
    {
        return Err(ValidationError::new(
            "CHECK_PROFILE_MISMATCH",
            "check does not match a unique required profile revision",
        ));
    }
    if check["attempt_id"] != attempt_id
        || check["candidate_ref"] != candidate_ref
        || check["state"] != "passed"
        || check["coverage"]["gaps"]
            .as_array()
            .is_none_or(|gaps| !gaps.is_empty())
    {
        return Err(ValidationError::new(
            "CHECK_NOT_READY",
            "required check is not a complete pass for this Attempt/candidate",
        ));
    }
    Ok(profile.to_owned())
}

pub fn validate_check_profile_identity(resolved_inputs: &Value, parser: &Value) -> Result<()> {
    if resolved_inputs["profile_identity"].is_object()
        && resolved_inputs["profile_identity"]["parser"] != *parser
    {
        return Err(ValidationError::new(
            "CHECK_EVIDENCE_MISSING",
            "check profile differs from its resolved identity",
        ));
    }
    Ok(())
}

pub fn validate_check_operation(
    operation: &Value,
    check_id: &str,
    result_ref: &Value,
) -> Result<()> {
    if operation["method"] != "check.run"
        || operation["state"] != "settled"
        || operation["result"]["outcome"] != "applied"
        || operation["result"]["source_checkout_verified"] != true
        || operation["result"]["check_id"] != check_id
        || operation["result"]["result_ref"] != *result_ref
    {
        return Err(ValidationError::new(
            "CHECK_EVIDENCE_MISSING",
            "check has no matching controller execution receipt",
        ));
    }
    Ok(())
}

pub fn validate_cached_source(
    source_spec: &Value,
    source_profile_parser: &Value,
    source_process_valid: bool,
    source_coverage_valid: bool,
) -> Result<()> {
    if !source_spec["cache_source_acceptance"].is_null()
        || !source_spec["cached_from_check_id"].is_null()
        || source_spec["resolved_inputs"]["profile_identity"]["parser"] != *source_profile_parser
        || !source_process_valid
        || !source_coverage_valid
    {
        return Err(ValidationError::new(
            "CHECK_NOT_READY",
            "cached source is not an original parser-complete process result",
        ));
    }
    Ok(())
}

pub fn validate_cached_operation(
    source_candidate_present: bool,
    original_operation: &Value,
    current_operation: &Value,
    source_id: &str,
) -> Result<()> {
    let valid = source_candidate_present
        && original_operation["method"] == "check.run"
        && original_operation["state"] == "settled"
        && original_operation["result"]["cached"].is_null()
        && original_operation["result"]["cached_from_check_id"].is_null()
        && original_operation["result"]["output_refs"]
            == current_operation["result"]["output_refs"]
        && current_operation["result"]["cached_from_check_id"] == source_id;
    if !valid {
        return Err(ValidationError::new(
            "CHECK_NOT_READY",
            "cached process evidence is not valid",
        ));
    }
    Ok(())
}

pub fn validate_checks_complete(seen_count: usize, required_count: usize) -> Result<()> {
    if seen_count != required_count {
        return Err(ValidationError::new(
            "CHECKS_REQUIRED",
            "required machine checks are missing; a review cannot substitute for them",
        ));
    }
    Ok(())
}

/// Return the existing evidence-level serialization label.
pub fn evidence_level(on_behalf: bool, has_checks: bool) -> &'static str {
    match (on_behalf, has_checks) {
        (false, false) => "operator_review",
        (false, true) => "operator_review_with_checks",
        (true, false) => "manager_review",
        (true, true) => "manager_review_with_checks",
    }
}

/// Validate the pure dependency-receipt shape before Store decision/revocation
/// queries. Returned IDs retain the existing dependency iteration order.
pub fn dependency_receipt_ids(
    attempt: &Value,
    dependency_task_ids: &[&str],
) -> Result<Vec<String>> {
    let pins = attempt["task_snapshot"]["dependency_acceptances"]
        .as_array()
        .ok_or_else(|| {
            ValidationError::new(
                "DEPENDENCY_EVIDENCE_MISSING",
                "Attempt has no dependency receipt list",
            )
        })?;
    if pins.len() != dependency_task_ids.len() {
        return Err(ValidationError::new(
            "DEPENDENCY_EVIDENCE_MISSING",
            "dependency receipts do not cover the frozen Task",
        ));
    }
    dependency_task_ids
        .iter()
        .map(|task_id| {
            let pin = pins
                .iter()
                .find(|pin| pin["task_id"] == *task_id)
                .ok_or_else(|| ValidationError::new("DEPENDENCY_EVIDENCE_MISSING", *task_id))?;
            text(pin, "acceptance_operation_id").map(str::to_owned)
        })
        .collect()
}

/// Validate the stale facts used when an on-behalf acceptance coalesces with a
/// prior decision. Authorization and submission readback stay in the Store.
pub fn validate_coalesced_scope(
    task: &Value,
    expected_revision: i64,
    attempt_id: &str,
) -> Result<()> {
    if task["revision"] != expected_revision || task["current_attempt_id"] != attempt_id {
        return Err(ValidationError::new(
            "STALE_SUBMISSION",
            "coalesced acceptance no longer targets the exact current Task and Attempt",
        ));
    }
    Ok(())
}
