//! Provider-neutral acceptance policy and acceptance-fact validation.
//!
//! The root Store remains responsible for authorization, GM epoch checks,
//! SQLite queries and transactions, artifact/file verification, check parser
//! coverage and operation/observation ledgers.  This module owns only
//! deterministic predicates over already-read acceptance values.

use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

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
/// The retained legacy V1 policy validates its global required
/// CheckRun profiles. The current V2 marker carries no profiles:
/// the exact profile union is derived from the frozen per-requirement
/// verification policies by requirement_evidence_plan.
pub fn validate_policy(policy: &Value) -> Result<()> {
    if policy.get("schema_id").is_some() || policy.get("schema_version").is_some() {
        if !has_exact_keys(policy, &["schema_id", "schema_version"])
            || policy.get("schema_id").and_then(Value::as_str) != Some("swarm.acceptance_policy")
        {
            return Err(ValidationError::new(
                "ACCEPTANCE_POLICY_VERSION",
                "acceptance policy must be the exact V2 marker",
            ));
        }
        let schema_version = policy
            .get("schema_version")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                ValidationError::new(
                    "ACCEPTANCE_POLICY_VERSION",
                    "acceptance policy needs an explicit schema_version",
                )
            })?;
        if schema_version != 2 {
            return Err(ValidationError::new(
                "ACCEPTANCE_POLICY_VERSION",
                "current Task admission accepts only the exact V2 acceptance policy marker",
            ));
        }
        return Ok(());
    }
    if !has_exact_keys(policy, &["required_check_profiles"]) {
        return Err(ValidationError::new(
            "ACCEPTANCE_POLICY_VERSION",
            "acceptance policy must be the exact legacy V1 object or the exact V2 marker",
        ));
    }
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

fn has_exact_keys(value: &Value, expected: &[&str]) -> bool {
    value.as_object().is_some_and(|object| {
        object.len() == expected.len() && expected.iter().all(|key| object.contains_key(*key))
    })
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
/// Derive the frozen per-requirement evidence plan from a V2 Task spec.
///
/// One derivation is shared by Task admission, brief projection,
/// acceptance coverage validation and acceptance evidence manifest
/// construction. It does not read SQLite, run a process or decide a
/// verdict; it only maps each frozen requirement to its exact
/// review-required subset and exact CheckRun profile identities.
/// A single exact profile/revision may serve several requirements and
/// is therefore executed once, not per requirement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequirementEvidencePlan {
    /// Requirement IDs that require independent review evidence, in
    /// frozen Task order.
    pub review_requirement_ids: Vec<String>,
    /// Exact CheckRun profile identities required by at least one
    /// requirement, in first-seen order.
    pub required_profiles: Vec<CheckProfileIdentity>,
    /// Every requirement ID that references each exact profile
    /// identity, in frozen Task order.
    pub profile_to_requirements: BTreeMap<CheckProfileIdentity, Vec<String>>,
}

/// The exact CheckRun profile identity a requirement references.
/// A profile ID with a different revision is a different identity,
/// never a substitute.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CheckProfileIdentity {
    pub profile_id: String,
    pub profile_revision: String,
}

impl CheckProfileIdentity {
    fn key(profile_id: &str, profile_revision: &str) -> Self {
        Self {
            profile_id: profile_id.to_owned(),
            profile_revision: profile_revision.to_owned(),
        }
    }
}

fn profile_identity(value: &Value) -> Result<CheckProfileIdentity> {
    let profile_id = value
        .get("profile_id")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| ValidationError::invalid("check profile needs a nonempty profile_id"))?;
    let profile_revision = value
        .get("profile_revision")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ValidationError::invalid("check profile needs a nonempty profile_revision")
        })?;
    Ok(CheckProfileIdentity::key(profile_id, profile_revision))
}

fn requirement_verification(requirement: &Value) -> Result<(bool, Vec<CheckProfileIdentity>)> {
    let verification = requirement.get("verification").ok_or_else(|| {
        ValidationError::new(
            "REQUIREMENT_VERIFICATION_REQUIRED",
            "V2 Task requirement needs an explicit verification policy",
        )
    })?;
    let review_required = verification
        .get("review_required")
        .and_then(Value::as_bool)
        .ok_or_else(|| {
            ValidationError::invalid("requirement verification needs review_required")
        })?;
    let check_profiles = verification
        .get("check_profiles")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ValidationError::invalid("requirement verification needs a check_profiles array")
        })?;
    let mut identities = Vec::with_capacity(check_profiles.len());
    let mut seen = BTreeSet::new();
    for profile in check_profiles {
        let identity = profile_identity(profile)?;
        if !seen.insert(identity.clone()) {
            return Err(ValidationError::invalid(
                "requirement check profiles must be unique within one requirement",
            ));
        }
        identities.push(identity);
    }
    if !review_required && identities.is_empty() {
        return Err(ValidationError::new(
            "REQUIREMENT_EVIDENCE_REQUIRED",
            "every requirement needs review evidence, machine CheckRun evidence, or both",
        ));
    }
    Ok((review_required, identities))
}

/// Derive the frozen evidence plan from a current V2 Task spec.
///
/// The spec must carry the exact V2 acceptance policy marker
/// (schema_id = swarm.acceptance_policy, schema_version = 2).
/// Mixed, partial or unknown markers reject; there is no fallback
/// to the retained legacy V1 shape. Every requirement must carry an
/// explicit verification policy with at least one evidence source.
pub fn requirement_evidence_plan(spec: &Value) -> Result<RequirementEvidencePlan> {
    let acceptance = spec.get("acceptance").ok_or_else(|| {
        ValidationError::new(
            "ACCEPTANCE_POLICY_REQUIRED",
            "V2 Task needs an explicit acceptance policy marker",
        )
    })?;
    let schema_id = acceptance
        .get("schema_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ValidationError::new(
                "ACCEPTANCE_POLICY_VERSION",
                "acceptance policy needs the swarm.acceptance_policy schema marker",
            )
        })?;
    let schema_version = acceptance
        .get("schema_version")
        .and_then(Value::as_i64)
        .ok_or_else(|| {
            ValidationError::new(
                "ACCEPTANCE_POLICY_VERSION",
                "acceptance policy needs an explicit schema_version",
            )
        })?;
    if schema_id != "swarm.acceptance_policy"
        || schema_version != 2
        || !has_exact_keys(acceptance, &["schema_id", "schema_version"])
    {
        return Err(ValidationError::new(
            "ACCEPTANCE_POLICY_VERSION",
            "current Task admission accepts only the exact V2 acceptance policy marker",
        ));
    }

    let requirements = spec
        .get("requirements")
        .and_then(Value::as_array)
        .filter(|items| !items.is_empty())
        .ok_or_else(|| {
            ValidationError::invalid("objective, phase and at least one requirement are required")
        })?;

    let mut review_requirement_ids = Vec::new();
    let mut required_profiles: Vec<CheckProfileIdentity> = Vec::new();
    let mut profile_to_requirements: BTreeMap<CheckProfileIdentity, Vec<String>> = BTreeMap::new();
    let mut requirement_ids = BTreeSet::new();
    let mut profile_revisions: BTreeMap<String, String> = BTreeMap::new();

    for requirement in requirements {
        let requirement_id = requirement
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| {
                ValidationError::invalid("requirement IDs must be nonempty and unique")
            })?;
        if !requirement_ids.insert(requirement_id.to_owned()) {
            return Err(ValidationError::invalid(
                "requirement IDs must be nonempty and unique",
            ));
        }
        let (review_required, identities) = requirement_verification(requirement)?;
        if review_required {
            review_requirement_ids.push(requirement_id.to_owned());
        }
        for identity in identities {
            if let Some(existing) = profile_revisions.get(&identity.profile_id) {
                if *existing != identity.profile_revision {
                    return Err(ValidationError::new(
                        "CHECK_PROFILE_REVISION_CONFLICT",
                        "one profile ID cannot carry different revisions in one Task",
                    ));
                }
            } else {
                profile_revisions.insert(
                    identity.profile_id.clone(),
                    identity.profile_revision.clone(),
                );
                required_profiles.push(identity.clone());
            }
            profile_to_requirements
                .entry(identity)
                .or_default()
                .push(requirement_id.to_owned());
        }
    }

    Ok(RequirementEvidencePlan {
        review_requirement_ids,
        required_profiles,
        profile_to_requirements,
    })
}

/// Serialize the provider-neutral plan into the exact deterministic form
/// retained with a claimed V2 Attempt. Ordered arrays preserve Task and
/// profile-reference order; profile relations use the plan's stable key order.
pub fn requirement_evidence_plan_value(spec: &Value) -> Result<Value> {
    let plan = requirement_evidence_plan(spec)?;
    let profile_to_requirements: Vec<_> = plan
        .profile_to_requirements
        .iter()
        .map(|(profile, requirement_ids)| {
            serde_json::json!({
                "profile_id": profile.profile_id,
                "profile_revision": profile.profile_revision,
                "requirement_ids": requirement_ids,
            })
        })
        .collect();
    Ok(serde_json::json!({
        "review_requirement_ids": plan.review_requirement_ids,
        "required_profiles": plan.required_profiles.iter().map(|profile| serde_json::json!({
            "profile_id": profile.profile_id,
            "profile_revision": profile.profile_revision,
        })).collect::<Vec<_>>(),
        "profile_to_requirements": profile_to_requirements,
    }))
}

/// Verify a retained Attempt plan against the exact frozen V2 Task spec.
/// The retained value is only an integrity check; authority is always derived
/// again from the Task spec through requirement_evidence_plan.
pub fn validate_frozen_requirement_evidence_plan(
    spec: &Value,
    frozen_plan: &Value,
) -> Result<RequirementEvidencePlan> {
    let expected = requirement_evidence_plan_value(spec)?;
    if frozen_plan != &expected {
        return Err(ValidationError::new(
            "REQUIREMENT_EVIDENCE_PLAN_STALE",
            "frozen Attempt evidence plan differs from its exact Task specification",
        ));
    }
    requirement_evidence_plan(spec)
}

pub fn validate_review_coverage(spec: &Value, reviews: &Value) -> Result<()> {
    let acceptance = spec.get("acceptance").ok_or_else(|| {
        ValidationError::new(
            "ACCEPTANCE_POLICY_REQUIRED",
            "Task has no explicit acceptance policy",
        )
    })?;
    let expected_ids =
        if acceptance.get("schema_id").is_some() || acceptance.get("schema_version").is_some() {
            requirement_evidence_plan(spec)?.review_requirement_ids
        } else {
            validate_policy(acceptance)?;
            spec.get("requirements")
                .and_then(Value::as_array)
                .ok_or_else(|| ValidationError::invalid("Task requirements are missing"))?
                .iter()
                .map(|requirement| text(requirement, "id").map(str::to_owned))
                .collect::<Result<Vec<_>>>()?
        };
    let expected: BTreeSet<&str> = expected_ids.iter().map(String::as_str).collect();
    let items = reviews
        .as_array()
        .ok_or_else(|| ValidationError::invalid("reviews must be an array"))?;
    let mut actual = BTreeSet::new();
    let duplicate = items.iter().any(|item| {
        item.get("requirement_id")
            .and_then(Value::as_str)
            .is_none_or(|id| !actual.insert(id))
    });
    if duplicate || expected != actual {
        return Err(ValidationError::new(
            "REVIEW_INCOMPLETE",
            "review must address exactly the Task's required evidence subset",
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
    required_profiles: &[CheckProfileIdentity],
    check: &Value,
    attempt_id: &str,
    candidate_ref: &str,
    seen_profiles: &BTreeSet<CheckProfileIdentity>,
) -> Result<CheckProfileIdentity> {
    let spec = check.get("spec").unwrap_or(&Value::Null);
    let identity = profile_identity(spec)?;
    if !required_profiles.iter().any(|required| {
        required.profile_id == identity.profile_id
            && required.profile_revision == identity.profile_revision
    }) || seen_profiles.contains(&identity)
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
    Ok(identity)
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

pub fn validate_checks_complete(
    seen_profiles: &BTreeSet<CheckProfileIdentity>,
    required_profiles: &BTreeSet<CheckProfileIdentity>,
) -> Result<()> {
    if seen_profiles != required_profiles {
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
