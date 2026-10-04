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

const REVIEW_STREAM: &str = "controller:review";

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
    let existing_id = meta(tx, &slot_record_key)?.and_then(|record| {
        record
            .get("review_assignment_id")
            .and_then(Value::as_str)
            .map(str::to_owned)
    });

    if let Some(existing_id) = existing_id.as_deref() {
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
                let mut result = previous.clone();
                result["operation_id"] = json!(operation_id);
                result["coalesced"] = json!(true);
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
        let previous_result = result_observation(tx, existing_id)?.ok_or_else(|| {
            Error::new(
                "REVIEW_REPLACEMENT_UNDISPOSED",
                "an unresolved assignment cannot be replaced without observed prior disposition",
            )
        })?;
        let disposition_key = format!("disposition:{existing_id}");
        let prior_disposition: Option<String> = tx
            .query_row(
                "SELECT payload_json FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.disposition'",
                params![REVIEW_STREAM, disposition_key],
                |row| row.get(0),
            )
            .optional()?;
        let Some(prior_disposition) = prior_disposition else {
            return Err(Error::new(
                "REVIEW_REPLACEMENT_UNDISPOSED",
                "replacement requires an already retained manager disposition for the prior result",
            ));
        };
        let prior_disposition: Value = serde_json::from_str(&prior_disposition)?;
        if prior_disposition["review_assignment_id"] != existing_id
            || prior_disposition["identity"] != json!(context.identity)
            || prior_disposition["review_result_operation_id"] != previous_result["operation_id"]
            || prior_disposition["disposition"] != "return_for_correction"
        {
            return Err(Error::new(
                "REVIEW_REPLACEMENT_DISPOSITION_MISMATCH",
                "prior disposition does not authorize replacement of this exact returned result",
            ));
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
            Some(json!({
                "reason":request.replacement_reason,
                "evidence_refs":request.replacement_evidence_refs,
                "prior_disposition_operation_id":prior_disposition["operation_id"],
            })),
            &slot_key,
            &slot_record_key,
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
) -> Result<Value> {
    let reviewer_registration = meta(tx, &format!("client:{reviewer_id}"))?
        .ok_or_else(|| Error::new("REVIEW_AUDITOR_UNAVAILABLE", "reviewer is not registered"))?;
    if reviewer_registration["disabled"] == true {
        return Err(Error::new(
            "REVIEW_AUDITOR_UNAVAILABLE",
            "reviewer credential is disabled",
        ));
    }
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
    let result = json!({
        "operation_id":operation_id,
        "review_assignment_id":assignment_id,
        "identity":context.identity,
        "sponsor_client_id":sponsor_id,
        "technical_requester_id":technical_requester_id,
        "reviewer_client_id":reviewer_id,
        "review_profile":review_profile,
        "state":"assigned",
        "coalesced":false,
        "supersedes_review_assignment_id":supersedes,
        "replacement_context":replacement_context,
        "task_transition":"none",
        "on_behalf":on_behalf,
    });
    assignment["result"] = result.clone();
    let event_key = format!("assignment:{assignment_id}");
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES(?1,?2,?3,'review.assignment',?4,?5)",
        params![REVIEW_STREAM, event_key, operation_id, model::canonical(&assignment)?, now],
    )?;
    // Registration binds only after this transaction has retained the exact
    // slot evidence; it remains ineffective until Operation commit/settlement.
    coordination::bind_review_assignment(tx, reviewer_id, &assignment_id, sponsor_id, &scope)?;
    tx.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
        params![slot_record_key, model::canonical(&json!({"review_assignment_id":assignment_id,"slot_key":slot_key,"identity":context.identity}))?],
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
    let current = current_applicability(tx, &identity)?;
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

/// Find one exact currently applicable actionable finding for the existing
/// manager feedback handler. This validates review provenance only; callers
/// must still run their normal current manager/Task authorization.
pub(crate) fn actionable_finding(
    db: &Connection,
    task_id: &str,
    attempt_id: &str,
    task_revision: i64,
    submission_ref: &str,
    candidate_ref: &str,
    finding_id: &str,
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
    let slot_key = slot_meta_key(&retained.identity)?;
    let assignment_id = meta(db, &slot_key)?
        .and_then(|record| {
            record
                .get("review_assignment_id")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
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
    if assignment["identity"] != json!(retained.identity)
        || result["result"]["verdict"] != "changes_requested"
        || result["result"]["applicability"] != "current_candidate"
    {
        return Err(Error::new(
            "REVIEW_FINDING_NOT_ACTIONABLE",
            "current assigned result is not an applicable changes_requested verdict",
        ));
    }
    let finding = result["result"]["findings"]
        .as_array()
        .and_then(|findings| {
            findings
                .iter()
                .find(|finding| finding["finding_id"] == finding_id)
        })
        .ok_or_else(|| {
            Error::new(
                "REVIEW_FINDING_NOT_FOUND",
                "finding is not present in the current assigned review result",
            )
        })?;
    Ok(json!({
        "review_assignment_id":assignment_id,
        "review_operation_id":result["operation_id"],
        "reviewer_client_id":assignment["reviewer_client_id"],
        "finding":finding,
        "identity":retained.identity,
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

fn current_applicability(db: &Connection, identity: &ReviewSlotIdentity) -> Result<bool> {
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
            coordination::require_historical_review_result_scope(
                db,
                principal,
                assignment_id,
                &scope,
            )
        } else {
            coordination::require_review_scope(db, principal, assignment_id, &scope)
        };
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
    let mut view = json!({
        "assignment":assignment,
        "result":result.as_ref().map(|record| record["result"].clone()),
        "current_candidate":current_applicability(db, &identity)?,
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
    raw.map(|raw| serde_json::from_str(&raw).map_err(Into::into))
        .transpose()
        .map(|value: Option<Value>| value.unwrap_or(Value::Null))
}

fn list_assignments(db: &Connection) -> Result<Vec<(i64, Value)>> {
    let rows = {
        let mut statement = db.prepare(
            "SELECT observation_id,payload_json FROM observations WHERE source_stream_id=?1 AND kind='review.assignment' AND source_event_key GLOB 'assignment:*' ORDER BY observation_id",
        )?;
        let rows = statement.query_map([REVIEW_STREAM], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()?
    };
    rows.into_iter()
        .map(|(id, raw)| Ok((id, serde_json::from_str(&raw)?)))
        .collect()
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

/// Keep `operation.get` useful to a reviewer while restricting it to the exact
/// assignment, its own submitted result, and the applied submission receipt.
pub(crate) fn authorize_operation_read(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<()> {
    if principal.role != Role::Participant {
        return Ok(());
    }
    principal.require_participant()?;
    let registration = meta(db, &format!("client:{}", principal.client_id))?
        .ok_or_else(|| Error::new("UNAUTHORIZED", "participant is not registered"))?;
    let registered_scope = registration["participation_basis"]["review_scope"].clone();
    let assignment_id = model::text(&registered_scope, "review_assignment_id")?;
    let assignment = assignment_observation(db, assignment_id)?;
    if assignment["reviewer_client_id"] != principal.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "operation is outside the assigned review evidence path",
        ));
    }
    let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())?;
    let exact_scope = exact_scope(&identity, assignment_id);
    let operation = operations::get_operation(db, operation_id)?;
    let assignment_operation = operation["method"] == "review.assign"
        && operation_id == assignment["operation_id"].as_str().unwrap_or_default()
        && operation["result"]["review_assignment_id"] == assignment_id;
    let result_operation = if operation["method"] == "review.submit"
        && operation["caller_id"] == principal.client_id
        && operation["result"]["review_assignment_id"] == assignment_id
    {
        result_observation(db, assignment_id)?
            .is_some_and(|record| record["operation_id"].as_str() == Some(operation_id))
    } else {
        false
    };
    if assignment_operation || result_operation {
        return coordination::require_historical_review_result_scope(
            db,
            principal,
            assignment_id,
            &exact_scope,
        );
    }
    let applied_submission_operation = if operation["method"] == "task.submit"
        && operation["result"]["submission_ref"] == identity.submission_ref
    {
        let submission_document = submissions::document(db, &identity.submission_ref)?;
        model::text(&submission_document, "operation_id")? == operation_id
    } else {
        false
    };
    if applied_submission_operation {
        return coordination::require_review_scope(db, principal, assignment_id, &exact_scope);
    }
    Err(Error::new(
        "FORBIDDEN",
        "operation is outside the assigned review evidence path",
    ))
}

fn list(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    model::fields(
        value,
        &["task_id", "attempt_id", "submission_ref", "after", "limit"],
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
    let mut visible = Vec::new();
    for (observation_id, assignment) in list_assignments(db)? {
        let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())?;
        if task_filter.is_some_and(|filter| identity.task_id != filter)
            || attempt_filter.is_some_and(|filter| identity.attempt_id != filter)
            || submission_filter.is_some_and(|filter| identity.submission_ref != filter)
        {
            continue;
        }
        if authorize_assignment_read(db, principal, &assignment, false).is_err() {
            continue;
        }
        visible.push((observation_id, review_view(db, &assignment, false)?));
    }
    let start = usize::try_from(after)
        .unwrap_or(usize::MAX)
        .min(visible.len());
    let end = visible.len().min(start.saturating_add(limit as usize));
    let items = visible[start..end]
        .iter()
        .map(|(observation_id, value)| {
            let mut item = value.clone();
            item["observation_id"] = json!(observation_id);
            item
        })
        .collect::<Vec<_>>();
    Ok(json!({
        "items":items,
        "count":visible.len(),
        "next_after":if end < visible.len() {Some(end)} else {None},
        "has_older":start > 0,
        "has_newer":end < visible.len(),
        "scope_filtered_before_paging":true,
    }))
}
