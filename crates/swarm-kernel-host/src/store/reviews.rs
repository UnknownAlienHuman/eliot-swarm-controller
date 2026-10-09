//! Manual assigned-review reservations and immutable reviewer results.
//!
//! This module intentionally writes only to the existing Operations,
//! Observations, and meta records. Review results never change Task/Attempt
//! state; a manager applies actionable findings through task.request_changes.

use super::{
    Error, Principal, Result, Role, coordination, meta, operations, results, submissions, tasks,
};
use crate::{
    automation::authorization::ManagerExecutionContext,
    model::{self, TaskSpec},
    review::{
        PRIMARY_REVIEW_SLOT, ReviewAssignRequest, ReviewSlotIdentity, ReviewSubmitRequest,
        review_policy_generation,
    },
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use swarm_kernel::reviews as review_contract;

const REVIEW_STREAM: &str = "controller:review";
const REVIEW_LIST_SCAN_MAX: i64 = 512;
const REVIEW_LIST_RESPONSE_MAX_BYTES: usize = 512 * 1024;

#[derive(Clone, Copy)]
enum ReviewReplacementClass {
    Unanswered,
    Inconclusive,
    ReturnedForCorrection,
}

impl ReviewReplacementClass {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unanswered => "unanswered",
            Self::Inconclusive => "inconclusive",
            Self::ReturnedForCorrection => "returned_for_correction",
        }
    }
}

/// A public Principal is accepted only for direct manager/operator requests.
/// Automatic actions use a retained context constructed by the Store.
pub(crate) enum ReviewActor<'a> {
    Direct(&'a Principal),
    OnBehalf(&'a ManagerExecutionContext),
}

struct SubmissionContext {
    task: Value,
    attempt: Value,
    document: Value,
    spec: TaskSpec,
    candidate_sha256: String,
    identity: ReviewSlotIdentity,
}

fn identity_scope(identity: &ReviewSlotIdentity, review_assignment_id: Value) -> Value {
    json!({
        "review_assignment_id": review_assignment_id,
        "task_id": identity.task_id,
        "attempt_id": identity.attempt_id,
        "task_revision": identity.task_revision,
        "submission_ref": identity.submission_ref,
        "candidate_ref": identity.candidate_ref,
    })
}

fn load_submission_context(
    db: &Connection,
    attempt_id: &str,
    expected_revision: i64,
    submission_ref: &str,
    candidate_ref: &str,
    require_current: bool,
) -> Result<SubmissionContext> {
    let document = submissions::document(db, submission_ref)?;
    if document["attempt_id"] != attempt_id
        || document["task_revision"] != expected_revision
        || document["candidate_ref"] != candidate_ref
    {
        return Err(Error::new(
            "REVIEW_ANCHOR_MISMATCH",
            "review anchors do not identify this retained applied submission",
        ));
    }
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let task_id = model::text(&attempt, "task_id")?;
    if attempt["task_revision"] != expected_revision {
        return Err(Error::new(
            "REVIEW_ANCHOR_MISMATCH",
            "submission Attempt revision differs from review request",
        ));
    }
    if document["task_id"] != task_id {
        return Err(Error::new(
            "REVIEW_ANCHOR_MISMATCH",
            "submission Task differs from its Attempt",
        ));
    }
    let task = tasks::get_task(db, task_id)?;
    let spec: TaskSpec = serde_json::from_value(attempt["task_snapshot"]["spec"].clone())?;
    let candidate = results::get(db, candidate_ref)?;
    let digest = candidate.content_digest.as_str();
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Error::new(
            "REVIEW_CANDIDATE_DAMAGED",
            "candidate has no valid content digest",
        ));
    }
    if candidate.artifact_id != candidate_ref
        || document["candidate_sha256"].as_str() != Some(digest)
        || document["candidate_kind"].as_str() != Some(candidate.kind.as_str())
        || document["candidate_byte_length"].as_u64() != Some(candidate.byte_length)
    {
        return Err(Error::new(
            "REVIEW_CANDIDATE_DAMAGED",
            "candidate bytes differ from the immutable applied submission",
        ));
    }
    if require_current
        && (task["state"] != "open"
            || task["revision"] != expected_revision
            || task["current_attempt_id"] != attempt_id
            || attempt["released_at_ms"] != Value::Null
            || !matches!(
                attempt["state"].as_str(),
                Some("submitted" | "needs_correction")
            )
            || attempt["submission_ref"] != submission_ref
            || attempt["candidate_ref"] != candidate_ref)
    {
        return Err(Error::new(
            "STALE_REVIEW_SUBJECT",
            "review assignment requires the current applied submission of the active Attempt",
        ));
    }
    let generation = review_policy_generation(&spec)?;
    let identity = ReviewSlotIdentity {
        task_id: task_id.to_owned(),
        attempt_id: attempt_id.to_owned(),
        task_revision: expected_revision,
        submission_ref: submission_ref.to_owned(),
        candidate_ref: candidate_ref.to_owned(),
        review_policy_generation: generation,
        review_slot: PRIMARY_REVIEW_SLOT.to_owned(),
    };
    Ok(SubmissionContext {
        task,
        attempt,
        document,
        spec,
        candidate_sha256: digest.to_owned(),
        identity,
    })
}

fn pending_scope(identity: &ReviewSlotIdentity) -> Value {
    identity_scope(identity, Value::Null)
}

fn exact_scope(identity: &ReviewSlotIdentity, assignment_id: &str) -> Value {
    identity_scope(identity, json!(assignment_id))
}

fn slot_meta_key(identity: &ReviewSlotIdentity) -> Result<String> {
    Ok(format!("review:slot:{}", identity.digest()?))
}

fn current_slot_pointer(
    db: &Connection,
    identity: &ReviewSlotIdentity,
) -> Result<Option<(String, String)>> {
    let key = slot_meta_key(identity)?;
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
            row.get(0)
        })
        .optional()?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let pointer: Value = serde_json::from_str(&raw).map_err(|_| {
        Error::new(
            "REVIEW_SLOT_DAMAGED",
            "current review slot pointer is not valid JSON",
        )
    })?;
    let slot_key = identity.digest()?;
    let assignment_id = pointer
        .get("review_assignment_id")
        .and_then(Value::as_str)
        .filter(|id| !id.trim().is_empty())
        .ok_or_else(|| {
            Error::new(
                "REVIEW_SLOT_DAMAGED",
                "current review slot pointer has no assignment ID",
            )
        })?;
    if pointer["slot_key"] != slot_key || pointer["identity"] != json!(identity) {
        return Err(Error::new(
            "REVIEW_SLOT_DAMAGED",
            "current review slot pointer differs from its exact slot identity",
        ));
    }
    Ok(Some((raw, assignment_id.to_owned())))
}

fn require_retained_reviewer_scope(
    db: &Connection,
    principal: &Principal,
    identity: &ReviewSlotIdentity,
    assignment_id: &str,
) -> Result<()> {
    let Some((_, current_assignment_id)) = current_slot_pointer(db, identity)? else {
        return Err(Error::new(
            "REVIEW_SLOT_DAMAGED",
            "retained review assignment has no current slot pointer",
        ));
    };
    let scope = exact_scope(identity, assignment_id);
    if current_assignment_id == assignment_id {
        coordination::require_review_scope(db, principal, assignment_id, &scope)
    } else {
        coordination::require_historical_review_result_scope(db, principal, assignment_id, &scope)
    }
}

fn authorize_direct_successor_read(
    db: &Connection,
    principal: &Principal,
    predecessor_assignment_id: &str,
    identity: &ReviewSlotIdentity,
) -> Result<bool> {
    let Some(registration) = meta(db, &format!("client:{}", principal.client_id))? else {
        return Ok(false);
    };
    let Some(scope) = registration
        .get("participation_basis")
        .and_then(|basis| basis.get("review_scope"))
    else {
        return Ok(false);
    };
    let Some(successor_assignment_id) = scope.get("review_assignment_id").and_then(Value::as_str)
    else {
        return Ok(false);
    };
    if successor_assignment_id == predecessor_assignment_id {
        return Ok(false);
    }
    let successor = assignment_observation(db, successor_assignment_id)?;
    if successor["reviewer_client_id"] != principal.client_id
        || successor["supersedes_review_assignment_id"] != predecessor_assignment_id
        || successor["identity"] != json!(identity)
    {
        return Ok(false);
    }
    let successor_identity: ReviewSlotIdentity =
        serde_json::from_value(successor["identity"].clone())?;
    require_retained_reviewer_scope(db, principal, &successor_identity, successor_assignment_id)?;
    Ok(true)
}

fn replacement_class(
    tx: &Transaction<'_>,
    assignment_id: &str,
    previous: &Value,
    identity: &ReviewSlotIdentity,
) -> Result<(ReviewReplacementClass, Option<Value>, Option<Value>)> {
    let prior_result = result_observation(tx, assignment_id)?;
    let prior_disposition = latest_disposition(tx, assignment_id)?;
    let Some(result) = prior_result.as_ref() else {
        if !prior_disposition.is_null() {
            return Err(Error::new(
                "REVIEW_REPLACEMENT_DISPOSITION_MISMATCH",
                "an unanswered assignment cannot have a retained result disposition",
            ));
        }
        return Ok((ReviewReplacementClass::Unanswered, None, None));
    };
    if result["identity"] != previous["identity"] || result["identity"] != json!(identity) {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "prior result does not match the exact current review assignment",
        ));
    }
    let verdict = result["result"]["verdict"].as_str().map(str::to_owned);
    match verdict.as_deref() {
        Some("inconclusive") => {
            if !prior_disposition.is_null() {
                return Err(Error::new(
                    "REVIEW_REPLACEMENT_DISPOSITION_MISMATCH",
                    "an inconclusive result cannot be replaced through a correction disposition",
                ));
            }
            Ok((ReviewReplacementClass::Inconclusive, prior_result, None))
        }
        Some("changes_requested") => {
            if prior_disposition.is_null() {
                return Err(Error::new(
                    "REVIEW_REPLACEMENT_UNDISPOSED",
                    "replacement of a changes_requested result requires its retained manager disposition",
                ));
            }
            if prior_disposition["review_assignment_id"] != assignment_id
                || prior_disposition["identity"] != json!(identity)
                || prior_disposition["review_result_operation_id"] != result["operation_id"]
                || prior_disposition["disposition"]
                    != review_contract::ReviewDisposition::ReturnForCorrection.as_str()
            {
                return Err(Error::new(
                    "REVIEW_REPLACEMENT_DISPOSITION_MISMATCH",
                    "prior disposition does not authorize replacement of this exact returned result",
                ));
            }
            Ok((
                ReviewReplacementClass::ReturnedForCorrection,
                prior_result,
                Some(prior_disposition),
            ))
        }
        Some("pass") => Err(Error::new(
            "REVIEW_REPLACEMENT_NOT_ALLOWED",
            "a passing review assignment cannot be replaced through this path",
        )),
        _ => Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "prior review result has an unsupported verdict",
        )),
    }
}

fn compare_and_set_slot_pointer(
    tx: &Transaction<'_>,
    slot_record_key: &str,
    slot_key: &str,
    identity: &ReviewSlotIdentity,
    assignment_id: &str,
    expected_pointer: Option<&str>,
) -> Result<()> {
    let value = model::canonical(&json!({
        "review_assignment_id":assignment_id,
        "slot_key":slot_key,
        "identity":identity,
    }))?;
    let changed = if let Some(expected_pointer) = expected_pointer {
        tx.execute(
            "UPDATE meta SET value_json=?2 WHERE key=?1 AND value_json=?3",
            params![slot_record_key, value, expected_pointer],
        )?
    } else {
        tx.execute(
            "INSERT INTO meta(key,value_json) VALUES(?1,?2) ON CONFLICT(key) DO NOTHING",
            params![slot_record_key, value],
        )?
    };
    if changed != 1 {
        return Err(Error::new(
            if expected_pointer.is_some() {
                "REVIEW_REPLACEMENT_STALE"
            } else {
                "REVIEW_SLOT_CONFLICT"
            },
            "review slot pointer changed before the assignment could be committed",
        ));
    }
    Ok(())
}

fn assignment_observation(db: &Connection, assignment_id: &str) -> Result<Value> {
    let key = format!("assignment:{assignment_id}");
    let raw: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.assignment'",
            params![REVIEW_STREAM, key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (raw, operation_id) = raw.ok_or_else(|| {
        Error::new(
            "REVIEW_ASSIGNMENT_NOT_FOUND",
            format!("review assignment {assignment_id} was not retained"),
        )
    })?;
    let record: Value = serde_json::from_str(&raw)?;
    if record["review_assignment_id"] != assignment_id || record["operation_id"] != operation_id {
        return Err(Error::new(
            "REVIEW_RECORD_DAMAGED",
            "assignment observation identity differs from its event key",
        ));
    }
    let operation = operations::get_operation(db, &operation_id)?;
    if operation["method"] != "review.assign"
        || operation["state"] != "settled"
        || operation["result"]["review_assignment_id"] != assignment_id
        || operation["result"]["identity"] != record["identity"]
    {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "assignment has no matching settled review.assign Operation",
        ));
    }
    Ok(record)
}

fn result_observation(db: &Connection, assignment_id: &str) -> Result<Option<Value>> {
    let key = format!("result:{assignment_id}");
    let raw: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.result'",
            params![REVIEW_STREAM, key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((raw, operation_id)) = raw else {
        return Ok(None);
    };
    let record: Value = serde_json::from_str(&raw)?;
    review_contract::validate_result_record(&record).map_err(|error| {
        Error::new(
            "REVIEW_RESULT_DAMAGED",
            format!("retained review result is invalid: {error}"),
        )
    })?;
    let operation = operations::get_operation(db, &operation_id)?;
    let assignment = assignment_observation(db, assignment_id)?;
    if record["operation_id"] != operation_id
        || record["review_assignment_id"] != assignment_id
        || operation["method"] != "review.submit"
        || operation["state"] != "settled"
        || operation["caller_id"] != assignment["reviewer_client_id"]
        || operation["result"]["review_assignment_id"] != assignment_id
    {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "result observation has no matching settled review.submit Operation",
        ));
    }
    Ok(Some(record))
}

fn manager_may_assign(
    tx: &Transaction<'_>,
    actor: &ReviewActor<'_>,
    submission: &SubmissionContext,
) -> Result<(String, String, Value)> {
    let task_id = model::text(&submission.task, "task_id")?;
    let attempt_id = model::text(&submission.attempt, "attempt_id")?;
    let project_id = model::text(&submission.task, "project_id")?;
    let owner_id = model::text(&submission.attempt, "owner_id")?;
    match actor {
        ReviewActor::Direct(principal) => {
            if principal.role == Role::Operator {
                principal.require_operator()?;
            } else if principal.role == Role::Manager {
                super::gm::require_attempt_control(tx, principal, &submission.attempt)?;
            } else {
                return Err(Error::new(
                    "FORBIDDEN",
                    "review.assign requires the current Attempt owner manager or operator",
                ));
            }
            Ok((
                principal.client_id.clone(),
                principal.client_id.clone(),
                Value::Null,
            ))
        }
        ReviewActor::OnBehalf(manager_context) => {
            let inherited_authority = manager_context.require_action_object_with_transfer(
                tx,
                "review.assign",
                task_id,
                submission.identity.task_revision,
                attempt_id,
                project_id,
                &submission.identity.submission_ref,
            )?;
            let sponsor = manager_context.effective_manager_id();
            let inherited_owner = inherited_authority.as_ref().is_some_and(|authority| {
                authority.source_attempt_owner_id() == owner_id
                    && authority.successor_manager_id() == sponsor
            });
            if sponsor != owner_id && !inherited_owner {
                return Err(Error::new(
                    "FORBIDDEN",
                    "automation sponsor does not own or inherit authority for the current Attempt",
                ));
            }
            Ok((
                sponsor.to_owned(),
                manager_context.technical_requester_id().to_owned(),
                manager_context.linkage_value(),
            ))
        }
    }
}

/// Resolve a configured reviewer profile only when one exact eligible
/// sponsored-reviewer registration matches the sponsor and pending slot.
/// `None` means no eligible target; ambiguity is an explicit error.
pub(crate) fn resolve_reviewer_profile(
    tx: &Transaction<'_>,
    sponsor_client_id: &str,
    profile: &str,
    identity: &ReviewSlotIdentity,
) -> Result<Option<String>> {
    if profile.trim().is_empty() {
        return Err(Error::invalid("review_profile cannot be empty"));
    }
    let expected = pending_scope(identity);
    let mut eligible =
        coordination::find_review_profile_participants(tx, sponsor_client_id, profile, &expected)?;
    match eligible.len() {
        0 => Ok(None),
        1 => Ok(eligible.pop()),
        _ => Err(Error::new(
            "REVIEW_AUDITOR_AMBIGUOUS",
            "more than one eligible sponsored auditor matches this profile and exact slot",
        )),
    }
}

/// Shared semantic reservation for direct and manager-on-behalf assignment.
/// The central Store dispatcher owns the Operation receipt and calls this in
/// its Immediate transaction.
pub(crate) fn reserve_assign(
    tx: &Transaction<'_>,
    actor: ReviewActor<'_>,
    request: &ReviewAssignRequest,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let context = load_submission_context(
        tx,
        &request.attempt_id,
        request.expected_revision,
        &request.submission_ref,
        &request.candidate_ref,
        true,
    )?;
    let (sponsor_id, technical_requester_id, on_behalf) = manager_may_assign(tx, &actor, &context)?;
    if let ReviewActor::OnBehalf(manager_context) = &actor
        && (request.reviewer_client_id.is_some()
            || request.review_profile.as_deref() != manager_context.review_profile()
            || request.replaces_review_assignment_id.is_some())
    {
        return Err(Error::new(
            "FORBIDDEN",
            "on-behalf review assignment must use its retained auditor profile and cannot replace an existing review",
        ));
    }
    let slot_key = context.identity.digest()?;
    let slot_record_key = slot_meta_key(&context.identity)?;
    let current_pointer = current_slot_pointer(tx, &context.identity)?;
    let existing_id = current_pointer
        .as_ref()
        .map(|(_, assignment_id)| assignment_id.as_str());

    if let Some(existing_id) = existing_id {
        let previous = assignment_observation(tx, existing_id)?;
        let previous_identity = &previous["identity"];
        if previous["slot_key"].as_str() != Some(slot_key.as_str())
            || previous_identity["task_id"] != context.identity.task_id
            || previous_identity["task_revision"] != context.identity.task_revision
            || previous_identity["attempt_id"] != context.identity.attempt_id
            || previous_identity["submission_ref"] != context.identity.submission_ref
            || previous_identity["candidate_ref"] != context.identity.candidate_ref
            || previous_identity["review_slot"] != context.identity.review_slot
        {
            return Err(Error::new(
                "REVIEW_SLOT_DAMAGED",
                "current slot pointer names an assignment with a different identity",
            ));
        }
        if previous_identity["review_policy_generation"]
            != context.identity.review_policy_generation
        {
            return Err(Error::new(
                "REVIEW_POLICY_GENERATION_MISMATCH",
                "the semantic slot is unchanged but its retained policy generation differs",
            ));
        }
        let resolved_target = match (&request.reviewer_client_id, &request.review_profile) {
            (Some(client), None) => Some(client.clone()),
            (None, Some(profile)) if previous["review_profile"] == *profile => {
                if previous["sponsor_client_id"] == sponsor_id {
                    previous["reviewer_client_id"].as_str().map(str::to_owned)
                } else {
                    resolve_reviewer_profile(tx, &sponsor_id, profile, &context.identity)?
                }
            }
            (None, Some(_)) => None,
            _ => None,
        };
        let same_target = resolved_target.as_deref() == previous["reviewer_client_id"].as_str();
        let replacing = request.replaces_review_assignment_id.as_deref() == Some(existing_id);
        if !replacing {
            if request.replaces_review_assignment_id.is_some() {
                return Err(Error::new(
                    "REVIEW_REPLACEMENT_STALE",
                    "replacement must name the currently retained review assignment for this slot",
                ));
            }
            if same_target {
                let result = assignment_receipt(&previous, operation_id, true)?;
                tx.execute(
                    "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
                    params![operation_id, context.identity.task_id, context.identity.attempt_id, model::canonical(&json!({"review_assignment":result,"on_behalf":on_behalf}))?],
                )?;
                return Ok(result);
            }
            return Err(Error::new(
                "REVIEW_SLOT_CONFLICT",
                "this exact candidate slot already has a different reviewer; an explicit prior disposition is required",
            ));
        }
        let (replacement_class, prior_result, prior_disposition) =
            replacement_class(tx, existing_id, &previous, &context.identity)?;
        if matches!(
            replacement_class,
            ReviewReplacementClass::ReturnedForCorrection
        ) && context.attempt["state"] != "needs_correction"
        {
            return Err(Error::new(
                "REVIEW_REPLACEMENT_DISPOSITION_MISMATCH",
                "returned-for-correction replacement requires the same Attempt in needs_correction",
            ));
        }
        let mut replacement_context = json!({
            "class":replacement_class.as_str(),
            "reason":request.replacement_reason,
            "evidence_refs":request.replacement_evidence_refs,
        });
        if let Some(result) = prior_result {
            replacement_context["prior_review_result_operation_id"] =
                result["operation_id"].clone();
        }
        if let Some(disposition) = prior_disposition {
            replacement_context["prior_disposition_operation_id"] =
                disposition["operation_id"].clone();
        }
        let reviewer_id = resolve_request_reviewer(
            tx,
            &request.reviewer_client_id,
            &request.review_profile,
            &sponsor_id,
            &context.identity,
        )?;
        if reviewer_id == previous["reviewer_client_id"] {
            return Err(Error::new(
                "REVIEW_REPLACEMENT_SAME_REVIEWER",
                "replacement requires a distinct assigned auditor",
            ));
        }
        return create_assignment(
            tx,
            operation_id,
            now,
            &context,
            &sponsor_id,
            &technical_requester_id,
            &on_behalf,
            &reviewer_id,
            request.review_profile.as_deref(),
            Some(existing_id),
            Some(replacement_context),
            &slot_key,
            &slot_record_key,
            current_pointer.as_ref().map(|(raw, _)| raw.as_str()),
        );
    }

    if request.replaces_review_assignment_id.is_some() {
        return Err(Error::new(
            "REVIEW_REPLACEMENT_NOT_FOUND",
            "the requested prior assignment is not the retained current slot",
        ));
    }
    let reviewer_id = resolve_request_reviewer(
        tx,
        &request.reviewer_client_id,
        &request.review_profile,
        &sponsor_id,
        &context.identity,
    )?;
    create_assignment(
        tx,
        operation_id,
        now,
        &context,
        &sponsor_id,
        &technical_requester_id,
        &on_behalf,
        &reviewer_id,
        request.review_profile.as_deref(),
        None,
        None,
        &slot_key,
        &slot_record_key,
        None,
    )
}

fn resolve_request_reviewer(
    tx: &Transaction<'_>,
    reviewer_client_id: &Option<String>,
    review_profile: &Option<String>,
    sponsor_client_id: &str,
    identity: &ReviewSlotIdentity,
) -> Result<String> {
    match (reviewer_client_id, review_profile) {
        (Some(client_id), None) => Ok(client_id.clone()),
        (None, Some(profile)) => {
            resolve_reviewer_profile(tx, sponsor_client_id, profile, identity)?.ok_or_else(|| {
                Error::new(
                    "REVIEW_AUDITOR_UNAVAILABLE",
                    "no eligible sponsored auditor matches this profile and exact slot",
                )
            })
        }
        _ => Err(Error::invalid(
            "choose one exact reviewer_client_id or review_profile",
        )),
    }
}

fn assignment_receipt(assignment: &Value, operation_id: &str, coalesced: bool) -> Result<Value> {
    if assignment["assignment_state"] != "assigned"
        || model::text(assignment, "review_assignment_id").is_err()
        || assignment["identity"].as_object().is_none()
        || model::text(assignment, "sponsor_client_id").is_err()
        || model::text(assignment, "technical_requester_id").is_err()
        || model::text(assignment, "reviewer_client_id").is_err()
    {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "retained assignment cannot produce a public assignment receipt",
        ));
    }
    Ok(json!({
        "operation_id":operation_id,
        "review_assignment_id":assignment["review_assignment_id"],
        "identity":assignment["identity"],
        "sponsor_client_id":assignment["sponsor_client_id"],
        "technical_requester_id":assignment["technical_requester_id"],
        "reviewer_client_id":assignment["reviewer_client_id"],
        "review_profile":assignment["review_profile"],
        "state":"assigned",
        "coalesced":coalesced,
        "supersedes_review_assignment_id":assignment["supersedes_review_assignment_id"],
        "replacement_context":assignment["replacement_context"],
        "task_transition":"none",
        "on_behalf":assignment["on_behalf"],
    }))
}

#[allow(clippy::too_many_arguments)]
fn create_assignment(
    tx: &Transaction<'_>,
    operation_id: &str,
    now: i64,
    context: &SubmissionContext,
    sponsor_id: &str,
    technical_requester_id: &str,
    on_behalf: &Value,
    reviewer_id: &str,
    review_profile: Option<&str>,
    supersedes: Option<&str>,
    replacement_context: Option<Value>,
    slot_key: &str,
    slot_record_key: &str,
    expected_pointer: Option<&str>,
) -> Result<Value> {
    let assignment_id = model::new_id();
    let scope = pending_scope(&context.identity);
    let mut assignment = json!({
        "schema_version":1,
        "review_assignment_id":assignment_id,
        "operation_id":operation_id,
        "identity":context.identity,
        "slot_key":slot_key,
        "sponsor_client_id":sponsor_id,
        "technical_requester_id":technical_requester_id,
        "reviewer_client_id":reviewer_id,
        "review_profile":review_profile,
        "required_coverage":{
            "requirement_ids":context.spec.requirements.iter().map(|r|r.id.clone()).collect::<Vec<_>>(),
            "source_refs":context.spec.source_refs,
            "source_index":context.spec.source_index,
            "phase":context.spec.phase,
        },
        "candidate_sha256":context.candidate_sha256,
        "submission_sha256":context.document["candidate_sha256"],
        "candidate_kind":context.document["candidate_kind"],
        "candidate_byte_length":context.document["candidate_byte_length"],
        "supersedes_review_assignment_id":supersedes,
        "replacement_context":replacement_context,
        "assignment_state":"assigned",
        "on_behalf":on_behalf,
    });
    coordination::bind_review_assignment(tx, reviewer_id, &assignment_id, sponsor_id, &scope)?;
    let result = assignment_receipt(&assignment, operation_id, false)?;
    assignment["result"] = result.clone();
    let event_key = format!("assignment:{assignment_id}");
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,'review.assignment',?4,?5)",
        params![REVIEW_STREAM, event_key, operation_id, model::canonical(&assignment)?, now],
    )?;
    compare_and_set_slot_pointer(
        tx,
        slot_record_key,
        slot_key,
        &context.identity,
        &assignment_id,
        expected_pointer,
    )?;
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
        params![operation_id, context.identity.task_id, context.identity.attempt_id, model::canonical(&json!({"review_assignment":result,"on_behalf":on_behalf}))?],
    )?;
    Ok(result)
}

pub(crate) fn reserve_submit(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: &ReviewSubmitRequest,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let assignment_id = &request.review_assignment_id;
    let assignment = assignment_observation(tx, assignment_id)?;
    let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())?;
    if principal.role != Role::Participant
        || principal.client_id != assignment["reviewer_client_id"]
    {
        return Err(Error::new(
            "FORBIDDEN",
            "only the authenticated auditor assigned to this exact slot may submit its result",
        ));
    }
    if request.submission_ref != identity.submission_ref
        || request.candidate_ref != identity.candidate_ref
    {
        return Err(Error::new(
            "REVIEW_ANCHOR_MISMATCH",
            "result submission/candidate differs from its retained review assignment",
        ));
    }
    let scope = exact_scope(&identity, assignment_id);
    // This is the narrow late-result exception: a registered, non-revoked
    // assigned reviewer may finish only its already-retained exact slot.
    coordination::require_historical_review_result_scope(tx, principal, assignment_id, &scope)?;
    let retained = load_submission_context(
        tx,
        &identity.attempt_id,
        identity.task_revision,
        &identity.submission_ref,
        &identity.candidate_ref,
        false,
    )?;
    if retained.identity.review_policy_generation != identity.review_policy_generation {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "retained Task policy differs from the assigned review generation",
        ));
    }
    let requirement_ids = retained
        .spec
        .requirements
        .iter()
        .map(|requirement| requirement.id.clone())
        .collect::<BTreeSet<_>>();
    request.validate_findings(&requirement_ids)?;
    request.validate_requirement_reviews(&requirement_ids)?;
    validate_requirement_review_evidence(request, &retained.identity)?;
    let semantic_input = semantic_result_input(request);
    if let Some(existing) = result_observation(tx, assignment_id)? {
        if existing["input"] == semantic_input {
            let mut coalesced = existing["result"].clone();
            coalesced["operation_id"] = json!(operation_id);
            coalesced["coalesced"] = json!(true);
            tx.execute(
                "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
                params![operation_id, identity.task_id, identity.attempt_id, model::canonical(&json!({"review_result":coalesced,"assignment_operation_id":assignment["operation_id"]}))?],
            )?;
            return Ok(coalesced);
        }
        return Err(Error::new(
            "REVIEW_RESULT_CONFLICT",
            "this assigned review attempt already retained a different result",
        ));
    }
    let current = current_applicability(tx, &identity, assignment_id)?;
    let mut result = json!({
        "operation_id":operation_id,
        "review_assignment_id":assignment_id,
        "task_id":identity.task_id,
        "attempt_id":identity.attempt_id,
        "task_revision":identity.task_revision,
        "submission_ref":identity.submission_ref,
        "candidate_ref":identity.candidate_ref,
        "candidate_sha256":retained.candidate_sha256,
        "review_policy_generation":identity.review_policy_generation,
        "review_slot":identity.review_slot,
        "reviewer_client_id":principal.client_id,
        "sponsor_client_id":assignment["sponsor_client_id"],
        "verdict":request.verdict,
        "coverage":request.coverage,
        "findings":request.findings,
        "evidence_refs":request.evidence_refs,
        "evidence_level":"assigned_auditor_report",
        "applicability":if current {"current_candidate"} else {"historical_candidate"},
        "task_transition":"none",
        "task_feedback_applied":false,
        "acceptance_changed":false,
        "publication_started":false,
        "coalesced":false,
    });
    if !request.requirement_reviews.is_empty() {
        result["requirement_reviews"] = json!(&request.requirement_reviews);
    }
    let record = json!({
        "schema_version":1,
        "review_assignment_id":assignment_id,
        "operation_id":operation_id,
        "identity":identity,
        "input":semantic_input,
        "result":result,
    });
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,'review.result',?4,?5)",
        params![REVIEW_STREAM, format!("result:{assignment_id}"), operation_id, model::canonical(&record)?, now],
    )?;
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,effective_request_json=?4 WHERE operation_id=?1",
        params![operation_id, identity.task_id, identity.attempt_id, model::canonical(&json!({"review_result":result,"assignment_operation_id":assignment["operation_id"]}))?],
    )?;
    Ok(result)
}

/// Return the exact current actionable review result with its original ordered
/// findings array. Feedback/disposition callers still apply their own manager
/// authority and select/package findings without reordering this source list.
pub(crate) fn actionable_review_result(
    db: &Connection,
    task_id: &str,
    attempt_id: &str,
    task_revision: i64,
    submission_ref: &str,
    candidate_ref: &str,
) -> Result<Value> {
    let retained = load_submission_context(
        db,
        attempt_id,
        task_revision,
        submission_ref,
        candidate_ref,
        true,
    )?;
    if retained.identity.task_id != task_id {
        return Err(Error::new(
            "REVIEW_ANCHOR_MISMATCH",
            "feedback Task differs from the reviewed submission",
        ));
    }
    let assignment_id = current_slot_pointer(db, &retained.identity)?
        .map(|(_, assignment_id)| assignment_id)
        .ok_or_else(|| {
            Error::new(
                "REVIEW_FINDING_NOT_FOUND",
                "current candidate has no retained assigned review",
            )
        })?;
    let assignment = assignment_observation(db, &assignment_id)?;
    let result = result_observation(db, &assignment_id)?.ok_or_else(|| {
        Error::new(
            "REVIEW_FINDING_NOT_FOUND",
            "current review assignment has no submitted result",
        )
    })?;
    if assignment["identity"] != json!(retained.identity) {
        return Err(Error::new(
            "REVIEW_FINDING_NOT_ACTIONABLE",
            "current assigned result is not an applicable changes_requested verdict",
        ));
    }
    let validated = review_contract::validate_result(&result["result"]).map_err(|error| {
        Error::new(
            "REVIEW_RESULT_DAMAGED",
            format!("retained review result is invalid: {error}"),
        )
    })?;
    if validated.verdict != review_contract::ReviewVerdict::ChangesRequested
        || validated.applicability != review_contract::ReviewApplicability::CurrentCandidate
    {
        return Err(Error::new(
            "REVIEW_FINDING_NOT_ACTIONABLE",
            "current assigned result is not an applicable changes_requested verdict",
        ));
    }
    Ok(json!({
        "review_assignment_id":assignment_id,
        "review_operation_id":result["operation_id"],
        "reviewer_client_id":assignment["reviewer_client_id"],
        "identity":retained.identity,
        "review_result":result["result"],
    }))
}

/// Find one exact actionable finding for legacy single-finding feedback.
/// New multi-finding callers should consume `actionable_review_result` and
/// preserve its findings order. Callers still apply manager authorization.
pub(crate) fn actionable_finding(
    db: &Connection,
    task_id: &str,
    attempt_id: &str,
    task_revision: i64,
    submission_ref: &str,
    candidate_ref: &str,
    finding_id: &str,
) -> Result<Value> {
    let provenance = actionable_review_result(
        db,
        task_id,
        attempt_id,
        task_revision,
        submission_ref,
        candidate_ref,
    )?;
    let finding = review_contract::actionable_finding(&provenance["review_result"], finding_id)
        .map_err(|error| match error {
            review_contract::ReviewValidationError::NotActionable => Error::new(
                "REVIEW_FINDING_NOT_ACTIONABLE",
                "current assigned result is not an applicable changes_requested verdict",
            ),
            review_contract::ReviewValidationError::FindingNotFound => Error::new(
                "REVIEW_FINDING_NOT_FOUND",
                "finding is not present in the current assigned review result",
            ),
            _ => Error::new(
                "REVIEW_RESULT_DAMAGED",
                format!("retained review result is invalid: {error}"),
            ),
        })?;
    Ok(json!({
        "review_assignment_id":provenance["review_assignment_id"],
        "review_operation_id":provenance["review_operation_id"],
        "reviewer_client_id":provenance["reviewer_client_id"],
        "finding":finding,
        "identity":provenance["identity"],
    }))
}

fn semantic_result_input(request: &ReviewSubmitRequest) -> Value {
    let mut input = json!({
        "review_assignment_id":request.review_assignment_id,
        "submission_ref":request.submission_ref,
        "candidate_ref":request.candidate_ref,
        "verdict":request.verdict,
        "coverage":request.coverage,
        "findings":request.findings,
        "evidence_refs":request.evidence_refs,
    });
    if !request.requirement_reviews.is_empty() {
        input["requirement_reviews"] = json!(&request.requirement_reviews);
    }
    input
}

/// Structured acceptance evidence may cite only artifacts already exposed by
/// the exact assigned-review surface. `load_submission_context` has verified
/// the retained submission Operation/digest and candidate artifact/digest
/// against this slot before this check runs; no evidence-read scope is added.
fn validate_requirement_review_evidence(
    request: &ReviewSubmitRequest,
    identity: &ReviewSlotIdentity,
) -> Result<()> {
    for review in &request.requirement_reviews {
        if review.evidence.iter().any(|reference| {
            reference != &identity.submission_ref && reference != &identity.candidate_ref
        }) {
            return Err(Error::new(
                "REVIEW_EVIDENCE_SCOPE",
                "structured evidence must reference this assigned submission or candidate artifact",
            ));
        }
    }
    Ok(())
}

fn current_applicability(
    db: &Connection,
    identity: &ReviewSlotIdentity,
    review_assignment_id: &str,
) -> Result<bool> {
    let current_slot = current_slot_pointer(db, identity)?
        .is_some_and(|(_, current_assignment_id)| current_assignment_id == review_assignment_id);
    if !current_slot {
        return Ok(false);
    }
    let task = tasks::get_task(db, &identity.task_id)?;
    let attempt = tasks::get_attempt(db, &identity.attempt_id)?;
    Ok(task["state"] == "open"
        && task["revision"] == identity.task_revision
        && task["current_attempt_id"] == identity.attempt_id
        && attempt["released_at_ms"] == Value::Null
        && attempt["task_revision"] == identity.task_revision
        && attempt["submission_ref"] == identity.submission_ref
        && attempt["candidate_ref"] == identity.candidate_ref
        && matches!(
            attempt["state"].as_str(),
            Some("submitted" | "needs_correction")
        ))
}

fn authorize_assignment_read(
    db: &Connection,
    principal: &Principal,
    assignment: &Value,
    allow_historical_reviewer: bool,
) -> Result<()> {
    let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())?;
    let assignment_id = model::text(assignment, "review_assignment_id")?;
    let scope = exact_scope(&identity, assignment_id);
    if principal.role == Role::Operator {
        return principal.require_operator();
    }
    if principal.role == Role::Manager {
        if principal
            .owns(model::text(assignment, "sponsor_client_id")?)
            .is_ok()
        {
            return Ok(());
        }
        let attempt = tasks::get_attempt(db, &identity.attempt_id)?;
        if attempt["task_id"] != identity.task_id
            || attempt["task_revision"] != identity.task_revision
        {
            return Err(Error::new(
                "FORBIDDEN",
                "review assignment is outside the current Task and Attempt scope",
            ));
        }
        return super::gm::require_attempt_control(db, principal, &attempt);
    }
    if principal.role == Role::Participant
        && principal.client_id == assignment["reviewer_client_id"]
    {
        return if allow_historical_reviewer {
            require_retained_reviewer_scope(db, principal, &identity, assignment_id)
        } else {
            coordination::require_review_scope(db, principal, assignment_id, &scope)
        };
    }
    if principal.role == Role::Participant
        && authorize_direct_successor_read(db, principal, assignment_id, &identity)?
    {
        return Ok(());
    }
    Err(Error::new(
        "FORBIDDEN",
        "review assignment is outside this caller's manager or assigned-auditor scope",
    ))
}

fn review_view(db: &Connection, assignment: &Value, include_context: bool) -> Result<Value> {
    let assignment_id = model::text(assignment, "review_assignment_id")?;
    let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())?;
    let result = result_observation(db, assignment_id)?;
    let current_slot = current_slot_pointer(db, &identity)?
        .is_some_and(|(_, current_assignment_id)| current_assignment_id == assignment_id);
    let mut view = json!({
        "assignment":assignment,
        "result":result.as_ref().map(|record| record["result"].clone()),
        "current_slot":current_slot,
        "current_candidate":current_applicability(db, &identity, assignment_id)?,
        "latest_disposition":latest_disposition(db, assignment_id)?,
    });
    if include_context {
        let retained = load_submission_context(
            db,
            &identity.attempt_id,
            identity.task_revision,
            &identity.submission_ref,
            &identity.candidate_ref,
            false,
        )?;
        let candidate = results::get(db, &identity.candidate_ref)?;
        view["context"] = json!({
            "task":{
                "task_id":identity.task_id,
                "task_revision":identity.task_revision,
                "phase":retained.spec.phase,
                "requirements":retained.spec.requirements,
                "scope":retained.spec.scope,
                "source_refs":retained.spec.source_refs,
                "source_index":retained.spec.source_index,
            },
            "submission":retained.document,
            "candidate":{
                "artifact_id":candidate.artifact_id,
                "kind":candidate.kind,
                "byte_length":candidate.byte_length,
                "content_digest":candidate.content_digest,
                "metadata":candidate.metadata,
                "content_access":"use artifact.read with this exact artifact_id",
            },
            "review_policy_generation":identity.review_policy_generation,
            "review_slot":identity.review_slot,
        });
    }
    Ok(view)
}

fn latest_disposition(db: &Connection, assignment_id: &str) -> Result<Value> {
    let key = format!("disposition:{assignment_id}");
    let raw: Option<String> = db
        .query_row(
            "SELECT payload_json FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.disposition'",
            params![REVIEW_STREAM, key],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(Value::Null);
    };
    let value: Value = serde_json::from_str(&raw)?;
    review_contract::validate_disposition(&value).map_err(|error| {
        Error::new(
            "REVIEW_DISPOSITION_DAMAGED",
            format!("retained review disposition is invalid: {error}"),
        )
    })?;
    Ok(value)
}

pub(crate) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<Value> {
    match method {
        "review.get" => {
            model::fields(value, &["review_assignment_id"])?;
            let assignment =
                assignment_observation(db, model::text(value, "review_assignment_id")?)?;
            authorize_assignment_read(db, principal, &assignment, true)?;
            review_view(db, &assignment, false)
        }
        "swarm.review.context" => {
            model::fields(value, &["review_assignment_id"])?;
            let assignment =
                assignment_observation(db, model::text(value, "review_assignment_id")?)?;
            authorize_assignment_read(db, principal, &assignment, false)?;
            review_view(db, &assignment, true)
        }
        "review.list" => list(db, principal, value),
        _ => Err(Error::new("METHOD_NOT_FOUND", method)),
    }
}

/// Narrow the generic artifact reader for Participant credentials. Assigned
/// auditors can read only the exact currently assigned immutable candidate.
pub(crate) fn authorize_artifact_read(
    db: &Connection,
    principal: &Principal,
    artifact_id: &str,
) -> Result<()> {
    if principal.role != Role::Participant {
        return Ok(());
    }
    principal.require_participant()?;
    let registration = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "participant is not registered"))?;
    if matches!(
        registration["participation_basis"]["kind"].as_str(),
        Some("attempt_owner" | "producer_ref")
    ) {
        return submissions::authorize_participant_artifact_read(db, principal, artifact_id);
    }
    let scope = registration["participation_basis"]["review_scope"].clone();
    let assignment_id = model::text(&scope, "review_assignment_id")?;
    coordination::require_review_scope(db, principal, assignment_id, &scope)?;
    if scope["candidate_ref"].as_str() == Some(artifact_id) {
        Ok(())
    } else {
        Err(Error::new(
            "FORBIDDEN",
            "artifact is outside the assigned review candidate",
        ))
    }
}

/// Allow only the two retained CheckRunner/submission readers in the
/// assigned-review palette. Every other Participant read stays unavailable.
pub(crate) fn authorize_evidence_read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<()> {
    if principal.role != Role::Participant {
        return Ok(());
    }
    principal.require_participant()?;
    let registration = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "participant is not registered"))?;
    let scope = registration["participation_basis"]["review_scope"].clone();
    let assignment_id = model::text(&scope, "review_assignment_id")?;
    coordination::require_review_scope(db, principal, assignment_id, &scope)?;
    match method {
        "task.submission" => {
            model::fields(value, &["submission_ref", "after", "limit"])?;
            let submission_ref = model::text(value, "submission_ref")?;
            if scope["submission_ref"] != submission_ref {
                return Err(Error::new(
                    "FORBIDDEN",
                    "submission is outside the assigned review slot",
                ));
            }
            let document = submissions::document(db, submission_ref)?;
            if document["task_id"] != scope["task_id"]
                || document["attempt_id"] != scope["attempt_id"]
                || document["task_revision"] != scope["task_revision"]
                || document["candidate_ref"] != scope["candidate_ref"]
            {
                return Err(Error::new(
                    "REVIEW_ASSIGNMENT_DAMAGED",
                    "retained submission no longer matches the assigned review tuple",
                ));
            }
            Ok(())
        }
        "check.get" => {
            model::fields(value, &["check_id"])?;
            let check = super::checks::describe(db, value)?;
            if check["attempt_id"] != scope["attempt_id"]
                || check["candidate_ref"] != scope["candidate_ref"]
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "CheckRun is outside the assigned Attempt and candidate",
                ));
            }
            let operation_id = model::text(&check, "operation_id")?;
            let (method, original_raw, effective_raw): (String, String, String) = db.query_row(
                "SELECT method,original_request_json,effective_request_json FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            let original: Value = serde_json::from_str(&original_raw)?;
            let effective: Value = serde_json::from_str(&effective_raw)?;
            if method != "check.run"
                || original["attempt_id"] != scope["attempt_id"]
                || original["candidate_ref"] != scope["candidate_ref"]
                || effective["check_id"] != check["check_id"]
            {
                return Err(Error::new(
                    "CHECK_EVIDENCE_DAMAGED",
                    "CheckRun is missing its exact retained admission request",
                ));
            }
            let attempt = tasks::get_attempt(db, model::text(&scope, "attempt_id")?)?;
            if attempt["task_id"] != scope["task_id"]
                || attempt["task_revision"] != scope["task_revision"]
            {
                return Err(Error::new(
                    "CHECK_EVIDENCE_DAMAGED",
                    "CheckRun Attempt no longer matches the assigned review tuple",
                ));
            }
            Ok(())
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "Participant evidence reads are limited to the assigned submission and CheckRun",
        )),
    }
}

fn list(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    model::fields(
        value,
        &[
            "task_id",
            "attempt_id",
            "submission_ref",
            "after",
            "after_observation_id",
            "limit",
        ],
    )?;
    let optional_text = |name: &str| -> Result<Option<&str>> {
        value
            .get(name)
            .map(|item| {
                item.as_str()
                    .filter(|text| !text.trim().is_empty())
                    .ok_or_else(|| Error::invalid(format!("{name} must be nonempty text")))
            })
            .transpose()
    };
    let task_filter = optional_text("task_id")?;
    let attempt_filter = optional_text("attempt_id")?;
    let submission_filter = optional_text("submission_ref")?;
    let integer = |name: &str, default: i64| -> Result<i64> {
        value.get(name).map_or(Ok(default), |item| {
            item.as_i64()
                .ok_or_else(|| Error::invalid(format!("{name} must be an integer")))
        })
    };
    let after = integer("after", 0)?;
    let limit = integer("limit", 50)?;
    if after < 0 || !(1..=200).contains(&limit) {
        return Err(Error::invalid(
            "after must be nonnegative and limit must be 1..200",
        ));
    }
    let after_observation_id = value
        .get("after_observation_id")
        .map(|item| {
            item.as_i64()
                .filter(|id| *id > 0)
                .ok_or_else(|| Error::invalid("after_observation_id must be a positive integer"))
        })
        .transpose()?;
    if after_observation_id.is_some() && after != 0 {
        return Err(Error::invalid(
            "after remains a visible-result offset; use after_observation_id with after=0 for keyset continuation",
        ));
    }
    let scan_limit = usize::try_from(REVIEW_LIST_SCAN_MAX)
        .map_err(|_| Error::new("REVIEW_LIST_DAMAGED", "review scan bound is invalid"))?;
    let mut statement = db.prepare(
        "SELECT observation_id,payload_json FROM observations \
         WHERE source_stream_id=?1 AND kind='review.assignment' \
           AND source_event_key GLOB 'assignment:*' AND observation_id>?2 \
         ORDER BY observation_id LIMIT ?3",
    )?;
    let rows = statement
        .query_map(
            params![
                REVIEW_STREAM,
                after_observation_id.unwrap_or(0),
                REVIEW_LIST_SCAN_MAX + 1
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let scan_window_has_more = rows.len() > scan_limit;
    let mut visible_offset_remaining = after;
    let mut visible_skipped = 0_i64;
    let mut items = Vec::new();
    let mut last_examined = after_observation_id;
    let mut consumed_rows = 0_usize;
    let mut stopped_before_visible = false;

    for (observation_id, raw) in rows.iter().take(scan_limit) {
        let assignment: Value = serde_json::from_str(raw)?;
        let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())?;
        if task_filter.is_some_and(|filter| identity.task_id != filter)
            || attempt_filter.is_some_and(|filter| identity.attempt_id != filter)
            || submission_filter.is_some_and(|filter| identity.submission_ref != filter)
        {
            last_examined = Some(*observation_id);
            consumed_rows += 1;
            continue;
        }
        match authorize_assignment_read(db, principal, &assignment, false) {
            Ok(()) => {}
            Err(error) if error.code == "FORBIDDEN" => {
                last_examined = Some(*observation_id);
                consumed_rows += 1;
                continue;
            }
            Err(error) => return Err(error),
        }
        if visible_offset_remaining > 0 {
            visible_offset_remaining -= 1;
            visible_skipped += 1;
            last_examined = Some(*observation_id);
            consumed_rows += 1;
            continue;
        }
        if items.len() >= limit as usize {
            // The next visible row is deliberately left beyond the cursor so
            // the caller can receive it on the next bounded page.
            stopped_before_visible = true;
            break;
        }
        let mut item = review_view(db, &assignment, false)?;
        item["observation_id"] = json!(observation_id);
        let mut candidate_items = items.clone();
        candidate_items.push(item.clone());
        if review_list_response_size(&candidate_items)? > REVIEW_LIST_RESPONSE_MAX_BYTES {
            if items.is_empty() {
                return Err(Error::new(
                    "REVIEW_LIST_ITEM_TOO_LARGE",
                    "one review list item exceeds the bounded response size",
                ));
            }
            // Do not advance over a valid item excluded by the byte budget.
            stopped_before_visible = true;
            break;
        }
        items.push(item);
        last_examined = Some(*observation_id);
        consumed_rows += 1;
    }
    if visible_offset_remaining > 0 && scan_window_has_more {
        return Err(Error::new(
            "REVIEW_LIST_OFFSET_TOO_DEEP",
            "the visible-result offset exceeds this bounded scan; restart from the first page and continue with after_observation_id",
        ));
    }
    let has_more = stopped_before_visible
        || scan_window_has_more
        || consumed_rows < rows.len().min(scan_limit);
    let scan_complete = !has_more;
    let visible_count = visible_skipped + items.len() as i64;
    let next_after = has_more.then(|| after.saturating_add(items.len() as i64));
    Ok(json!({
        "items":items,
        "count":if scan_complete {Some(visible_count)} else {None},
        "count_exact":scan_complete,
        "next_after":next_after,
        "next_after_observation_id":if has_more {last_examined} else {None},
        "has_older":after > 0 || after_observation_id.is_some(),
        "has_newer":has_more,
        "coverage":if scan_complete { "complete" } else { "partial" },
        "scope_filtered_before_paging":true,
    }))
}

fn review_list_response_size(items: &[Value]) -> Result<usize> {
    let response = json!({
        "items":items,
        "count":i64::MAX,
        "count_exact":true,
        "next_after":i64::MAX,
        "next_after_observation_id":i64::MAX,
        "has_older":true,
        "has_newer":true,
        "coverage":"partial",
        "scope_filtered_before_paging":true,
    });
    Ok(model::canonical(&response)?.len())
}
