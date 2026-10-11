//! Exact acceptance consumer for a selected manager automation.
//!
//! It reuses the Store's guarded task.accept reserve path after proving the
//! exact assigned review, current candidate, current GM authority and required
//! CheckRuns. The asynchronous acceptance worker remains responsible for
//! artifact byte verification and the final Task transition.

use super::operation_lifecycle::{
    ExistingAutomaticOperation, ExistingAutomaticOperationLookup, OperationLifecycleState,
};
use super::{capacity, operations, results, submissions, tasks};
use crate::{
    acceptance::{AcceptRequest, AcceptancePolicy, RequirementReview},
    automation::{
        acceptance::{AcceptanceContext, AcceptanceReviewEvidence, assignment_sponsor_authorized},
        actions::AutomationStep,
        authorization,
        config::{self, AutomationEntry},
    },
    checks::model::CheckRequest,
    error::{Error, Result},
    model::{self, TaskSpec},
    review::{PRIMARY_REVIEW_SLOT, ReviewSlotIdentity, review_policy_generation},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const REVIEW_STREAM: &str = "controller:review";

struct CommittedReviewResult {
    assignment: Value,
    record: Value,
    identity: ReviewSlotIdentity,
    observation_id: i64,
}

struct CheckRunRow {
    check_id: String,
    operation_id: String,
    state: String,
    exit_code: Option<i64>,
    released_at_ms: Option<i64>,
    cached_from_check_id: Option<String>,
    result_ref: Option<String>,
    coverage: Value,
}

/// Consume one exact retained review result in the Store transaction that
/// settled it. When structured requirement evidence is present, this reserves
/// only the guarded `task.accept` operation. It never publishes, merges, or
/// releases ownership; missing structured evidence remains an explicit gap.
pub(super) fn consume_review_result_for_entry(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    review_assignment_id: &str,
    review_result_operation_id: &str,
    now_ms: i64,
) -> Result<Value> {
    let Some(review) =
        committed_review_result(tx, review_assignment_id, review_result_operation_id)?
    else {
        return Ok(json!({
            "status":"not_canonical_result_event",
            "review_assignment_id":review_assignment_id,
            "review_result_operation_id":review_result_operation_id,
            "acceptance_started":false,
            "task_accepted":false,
            "publication_started":false
        }));
    };

    if !entry.enabled || !entry.steps.contains(&AutomationStep::Acceptance) {
        return Ok(skipped(
            &review,
            "acceptance_not_selected",
            "the current manager entry does not select acceptance",
        ));
    }
    if entry.scope.work_pool_id.is_some() {
        return Ok(capability_gap(
            &review,
            "work_pool_scope_unavailable",
            "the current Task source has no committed work-pool membership reader",
        ));
    }
    authorization::require_registered_manager(tx, &entry.owner_manager_id)?;
    let current_entry = config::load_entry(
        tx,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let Some(current_entry) = current_entry else {
        return Ok(skipped(
            &review,
            "automation_action_changed",
            "the retained acceptance entry no longer exists",
        ));
    };
    if !current_entry.enabled
        || current_entry.revision != entry.revision
        || !current_entry.steps.contains(&AutomationStep::Acceptance)
        || current_entry.scope != entry.scope
        || current_entry.value()? != entry.value()?
    {
        return Ok(skipped(
            &review,
            "automation_action_changed",
            "the retained entry revision no longer selects this exact acceptance action",
        ));
    }

    let result = &review.record["result"];
    if result["applicability"] != "current_candidate" {
        return Ok(skipped(
            &review,
            "historical_review_result",
            "the assigned pass is not applicable to the current candidate",
        ));
    }
    if result["verdict"] != "pass" || result["coverage"] != "complete" {
        return Ok(skipped(
            &review,
            "assigned_review_pass_required",
            "only a complete assigned pass can proceed to acceptance preflight",
        ));
    }
    if result["findings"]
        .as_array()
        .is_none_or(|findings| !findings.is_empty())
    {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "passing assigned review does not have an empty findings list",
        ));
    }
    let evidence_refs = nonempty_text_array(&result["evidence_refs"]).ok_or_else(|| {
        Error::new(
            "REVIEW_RESULT_DAMAGED",
            "passing assigned review has no retained evidence references",
        )
    })?;

    let assignment_sponsor_id = model::text(&review.assignment, "sponsor_client_id")?;
    let reviewer_id = model::text(result, "reviewer_client_id")?;
    if result["sponsor_client_id"] != assignment_sponsor_id
        || result["reviewer_client_id"] != review.assignment["reviewer_client_id"]
        || reviewer_id == assignment_sponsor_id
        || reviewer_id == entry.owner_manager_id
    {
        return Ok(skipped(
            &review,
            "acceptance_manager_or_reviewer_mismatch",
            "the retained assignment sponsor differs from its result or the reviewer is not independent of the sponsor and current GM",
        ));
    }
    if entry.steps.contains(&AutomationStep::ReviewDispatch)
        && entry.review.profile.as_deref() != review.assignment["review_profile"].as_str()
    {
        return Ok(skipped(
            &review,
            "review_profile_mismatch",
            "the current assigned pass was not produced under the entry's selected reviewer profile",
        ));
    }
    let Some(current_gm) = super::gm::current(tx)? else {
        return Ok(capability_gap(
            &review,
            "current_gm_required",
            "automatic acceptance requires the selected manager to hold the current GM designation",
        ));
    };
    let gm_epoch = current_gm.epoch;
    if current_gm.client_id != entry.owner_manager_id {
        return Ok(skipped(
            &review,
            "manager_is_not_current_gm",
            "the selected automation owner no longer has current GM acceptance authority",
        ));
    }
    let expected_feedback_observation_id = feedback_cursor(tx, &review.identity.submission_ref)?;
    if review.observation_id < expected_feedback_observation_id {
        let mut value = skipped(
            &review,
            "feedback_after_assigned_pass",
            "manager feedback for this submission was recorded after the assigned pass; a fresh assigned review is required",
        );
        value["review_result_observation_id"] = json!(review.observation_id);
        value["feedback_observation_id"] = json!(expected_feedback_observation_id);
        return Ok(value);
    }

    if review.identity.review_slot != PRIMARY_REVIEW_SLOT {
        return Ok(capability_gap(
            &review,
            "review_slot_policy_unavailable",
            "the retained acceptance consumer supports only the exact current primary review slot",
        ));
    }
    if entry.review.required_reviewers != 1 {
        return Ok(capability_gap(
            &review,
            "required_review_slots_unavailable",
            "the current review store records one required primary slot and cannot prove the configured reviewer count",
        ));
    }

    let (task, attempt) = match current_subject(tx, &review.identity)? {
        Some(subject) => subject,
        None => {
            return Ok(skipped(
                &review,
                "stale_review_subject",
                "Task revision, Attempt, submission or candidate is no longer current",
            ));
        }
    };
    let project_id = model::text(&task, "project_id")?;
    if project_id != entry.project_id {
        return Ok(skipped(
            &review,
            "automation_project_mismatch",
            "the exact Task is outside this acceptance entry project",
        ));
    }
    let document = submissions::document(tx, &review.identity.submission_ref)?;
    let reviewer_id = model::text(result, "reviewer_client_id")?;
    let submitted_by = model::text(&document, "submitted_by")?;
    let owner_id = model::text(&document, "owner_id")?;
    let attempt_owner_id = model::text(&attempt, "owner_id")?;
    if !assignment_sponsor_authorized(
        tx,
        entry,
        &review.identity,
        assignment_sponsor_id,
        attempt_owner_id,
        gm_epoch,
    )? {
        return Ok(skipped(
            &review,
            "acceptance_owner_sponsor_or_transfer_required",
            "the pass must be sponsored by the Attempt owner or a manager in the exact transfer lineage ending at the current GM",
        ));
    }
    if document["attempt_id"] != review.identity.attempt_id
        || document["task_revision"] != review.identity.task_revision
        || document["candidate_ref"] != review.identity.candidate_ref
        || owner_id != attempt_owner_id
        || submitted_by == entry.owner_manager_id.as_str()
        || owner_id == entry.owner_manager_id.as_str()
        || reviewer_id == submitted_by
        || reviewer_id == owner_id
    {
        return Ok(skipped(
            &review,
            "acceptance_subject_or_owner_sponsor_mismatch",
            "automatic acceptance requires the exact current Attempt, an owner-consistent submission, a valid review sponsor, and an independent reviewer",
        ));
    }
    let candidate = results::get(tx, &review.identity.candidate_ref)?;
    if Some(candidate.byte_length) != document["candidate_byte_length"].as_u64()
        || Some(candidate.content_digest.as_str()) != document["candidate_sha256"].as_str()
    {
        return Err(Error::new(
            "CANDIDATE_DAMAGED",
            "sealed candidate metadata differs from its submission record",
        ));
    }

    let spec: TaskSpec = serde_json::from_value(attempt["task_snapshot"]["spec"].clone())?;
    spec.validate()?;
    if review.identity.review_policy_generation != review_policy_generation(&spec)? {
        return Ok(skipped(
            &review,
            "review_policy_generation_changed",
            "assigned review was not produced for the current frozen Task policy",
        ));
    }
    let Some(policy) = spec.acceptance.as_ref() else {
        return Ok(capability_gap(
            &review,
            "acceptance_policy_required",
            "the exact Task revision has no explicit acceptance policy",
        ));
    };
    policy.validate()?;

    let check_ids = match exact_required_check_ids(
        tx,
        &review.identity.task_id,
        &review.identity.attempt_id,
        &review.identity.candidate_ref,
        policy,
    )? {
        CheckSelection::Ready(check_ids) => check_ids,
        CheckSelection::Incomplete(missing) => {
            let mut value = capability_gap(
                &review,
                "required_checks_incomplete",
                "the exact candidate does not yet have one retained passing CheckRun for each required profile revision",
            );
            value["missing_profiles"] = json!(missing);
            return Ok(value);
        }
    };

    let context = match AcceptanceContext::from_committed_entry(
        tx,
        entry,
        AcceptanceReviewEvidence {
            identity: review.identity.clone(),
            assignment_id: review_assignment_id.to_owned(),
            result_operation_id: review_result_operation_id.to_owned(),
            assignment_sponsor_id: assignment_sponsor_id.to_owned(),
        },
        expected_feedback_observation_id,
        check_ids.clone(),
    ) {
        Ok(context) => context,
        Err(error)
            if matches!(
                error.code.as_str(),
                "FORBIDDEN"
                    | "AUTOMATION_ACTION_CHANGED"
                    | "AUTOMATION_ACTION_UNAVAILABLE"
                    | "AUTOMATION_NOT_FOUND"
            ) =>
        {
            return Ok(capability_gap(
                &review,
                "acceptance_authority_unavailable",
                &error.message,
            ));
        }
        Err(error) => return Err(error),
    };

    let requirement_reviews = match result.get("requirement_reviews") {
        Some(Value::Array(reviews)) if !reviews.is_empty() => {
            serde_json::from_value::<Vec<RequirementReview>>(json!(reviews)).map_err(|_| {
                Error::new(
                    "REVIEW_RESULT_DAMAGED",
                    "retained per-requirement review evidence is malformed",
                )
            })?
        }
        _ => {
            return Ok(capability_gap(
                &review,
                "requirement_level_acceptance_evidence_unavailable",
                "the assigned pass has no structured rationale and evidence for each frozen Task requirement",
            ));
        }
    };
    let reason = format!(
        "Assigned review {} recorded a complete pass for candidate {} in review.submit Operation {}.",
        review_assignment_id, review.identity.candidate_ref, review_result_operation_id
    );
    let request = AcceptRequest {
        client_request_id: context.semantic_request_id()?,
        attempt_id: context.identity().attempt_id.clone(),
        expected_revision: context.identity().task_revision,
        submission_ref: context.identity().submission_ref.clone(),
        candidate_ref: context.identity().candidate_ref.clone(),
        expected_feedback_observation_id,
        reason,
        reviews: requirement_reviews,
        check_ids: context.check_ids().to_vec(),
    };
    request.validate_coverage(&spec)?;
    let request_value = serde_json::to_value(&request)?;
    // Parse through the same strict request boundary as manual acceptance.
    AcceptRequest::parse(&request_value)?;
    context.require_action_object(
        tx,
        "task.accept",
        &context.identity().task_id,
        context.identity().task_revision,
        &context.identity().attempt_id,
        &context.identity().submission_ref,
        &context.identity().candidate_ref,
        context.expected_feedback_observation_id(),
        context.check_ids(),
    )?;

    // This is the same guarded Store acceptance authority/evidence gate used
    // for manual decisions, with typed on-behalf attribution. It reserves only
    // task.accept; its existing async worker performs byte verification and
    // rechecks the exact context before Task state changes.
    let receipt = reserve_automatic_acceptance(tx, &context, &request_value, now_ms)?;
    let coalesced = receipt["coalesced"].as_bool().unwrap_or(false);
    let status = receipt["status"].as_str().unwrap_or(if coalesced {
        "coalesced"
    } else {
        "acceptance_reserved"
    });
    let mut value = json!({
        "status":status,
        "action":"task.accept",
        "operation_id":receipt["operation_id"],
        "acceptance_operation_id":receipt["acceptance_operation_id"],
        "coalesced":coalesced,
        "review_assignment_id":context.review_assignment_id(),
        "review_result_operation_id":context.review_result_operation_id(),
        "review_assignment_sponsor_id":context.review_assignment_sponsor_id(),
        "task_id":context.identity().task_id,
        "task_revision":context.identity().task_revision,
        "attempt_id":context.identity().attempt_id,
        "submission_ref":context.identity().submission_ref,
        "candidate_ref":context.identity().candidate_ref,
        "review_slot":context.identity().review_slot,
        "review_policy_generation":context.identity().review_policy_generation,
        "check_ids":context.check_ids(),
        "gm_epoch":context.gm_epoch(),
        "acceptance_started":receipt["acceptance_started"] == true || receipt["task_accepted"] == true,
        "task_accepted":receipt["task_accepted"] == true,
        "publication_started":false,
        "ownership_released":false
    });
    value["receipt"] = receipt;
    value["automation_id"] = json!(context.automation_id());
    value["automation_revision"] = json!(context.automation_revision());
    value["project_id"] = json!(context.project_id());
    value["effective_manager_id"] = json!(context.effective_manager_id());
    value["technical_requester_id"] = json!(context.technical_requester_id());
    value["semantic_request_id"] = json!(request.client_request_id);
    value["review_evidence_refs"] = json!(evidence_refs);
    value["required_check_profiles"] = json!(policy.required_check_profiles);
    Ok(value)
}
/// Classify one retained `task.accept` Operation truthfully.
///
/// `cancelled` and `rejected` are terminal states in their own right. They are
/// reported as themselves and never as corruption and never as applied or
/// coalesced acceptance success. A `settled` state proves nothing on its own:
/// only an exact `task_accepted` result tied to this same Operation establishes
/// that the acceptance applied. `outcome_unknown` requires readback and never
/// authorizes a replacement or replay under the same semantic request ID.
fn classify_retained_acceptance(retained: ExistingAutomaticOperation, decision: &Value) -> Value {
    let operation_id = retained.operation_id.clone();
    let result = retained.result.clone().unwrap_or(Value::Null);
    let outcome = result
        .get("outcome")
        .and_then(Value::as_str)
        .unwrap_or_default();
    // The admission receipt describes reservation, not completion. Only the
    // current result and the domain-validated decision can prove acceptance.
    let receipt = result.clone();
    // The decision receipt must name this exact Operation. A receipt retained
    // for a different semantic Operation, or one carrying no identity at all,
    // cannot establish that this acceptance applied.
    let receipt_names_operation = receipt
        .get("operation_id")
        .is_none_or(|named| named == &json!(operation_id));
    // The acceptance applied only with the exact retained decision evidence.
    let accepted = retained.state == OperationLifecycleState::Settled
        && receipt["operation_id"] == operation_id
        && receipt["task_accepted"] == true
        && decision["current"] == true
        && decision["invalidated"] == false;
    let mut value = json!({
        "operation_id":operation_id,
        "acceptance_operation_id":receipt.get("acceptance_operation_id").cloned().unwrap_or_else(|| json!(operation_id)),
        "operation_state":retained.state.as_str(),
        "acceptance_started":retained.sent_at_ms.is_some() || accepted,
        "task_accepted":accepted,
    });
    let object = value.as_object_mut().expect("base receipt is an object");
    if !receipt_names_operation && retained.state != OperationLifecycleState::Cancelled {
        // The retained receipt cannot be attributed to this Operation, so the
        // identity mismatch is explicit rather than silently treated as applied.
        object.insert("receipt_identity_mismatch".to_owned(), json!(true));
    }
    match retained.state {
        OperationLifecycleState::Queued => {
            object.insert("status".to_owned(), json!("acceptance_in_flight"));
            object.insert("coalesced".to_owned(), json!(false));
        }
        OperationLifecycleState::Sending | OperationLifecycleState::NativeAccepted => {
            object.insert("status".to_owned(), json!("acceptance_in_flight"));
            object.insert("coalesced".to_owned(), json!(false));
        }
        OperationLifecycleState::OutcomeUnknown => {
            object.insert("status".to_owned(), json!("acceptance_readback_required"));
            object.insert("coalesced".to_owned(), json!(false));
            object.insert("readback_required".to_owned(), json!(true));
        }
        OperationLifecycleState::Cancelled => {
            object.insert("status".to_owned(), json!("acceptance_cancelled"));
            object.insert("coalesced".to_owned(), json!(false));
            object.insert("terminal_not_applied".to_owned(), json!(true));
            object.insert(
                "code".to_owned(),
                json!("acceptance_cancelled_before_effect"),
            );
        }
        OperationLifecycleState::Rejected => {
            object.insert("status".to_owned(), json!("acceptance_rejected"));
            object.insert("coalesced".to_owned(), json!(false));
            object.insert("terminal_not_applied".to_owned(), json!(true));
            object.insert(
                "code".to_owned(),
                receipt
                    .get("code")
                    .cloned()
                    .unwrap_or_else(|| receipt["error"]["code"].clone()),
            );
        }
        OperationLifecycleState::Settled => {
            object.insert(
                "status".to_owned(),
                json!(if accepted {
                    "acceptance_applied"
                } else {
                    "acceptance_not_applied"
                }),
            );
            object.insert("coalesced".to_owned(), json!(receipt["coalesced"] == true));
            object.insert("terminal_not_applied".to_owned(), json!(!accepted));
            if !accepted {
                object.insert(
                    "code".to_owned(),
                    if outcome == "failed" {
                        receipt["error"]["code"].clone()
                    } else if decision["invalidated"] == true {
                        json!("acceptance_invalidated")
                    } else {
                        json!("acceptance_not_current")
                    },
                );
            }
        }
    }
    object.insert("receipt".to_owned(), receipt);
    value
}

/// Validate the retained result against the authoritative acceptance producer,
/// including a coalesced wrapper's exact original decision. The admission
/// receipt is never used as a substitute for the terminal result.
fn retained_acceptance_decision(
    db: &Connection,
    retained: &ExistingAutomaticOperation,
    task_id: &str,
    request_value: &Value,
) -> Result<Value> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_OPERATION_CORRUPT",
            "retained acceptance result does not match its exact lifecycle and decision",
        )
    };
    if !retained.has_unbound_execution_scope() {
        return Err(corrupt());
    }
    let result = retained.result.as_ref();
    if result.is_some_and(|value| !value.is_object()) {
        return Err(corrupt());
    }
    match retained.state {
        OperationLifecycleState::Queued
        | OperationLifecycleState::Sending
        | OperationLifecycleState::NativeAccepted
        | OperationLifecycleState::OutcomeUnknown => {
            if result.is_some_and(|value| {
                value["operation_id"] != retained.operation_id
                    || value["task_accepted"] == true
                    || value["outcome"] == "applied"
            }) {
                return Err(corrupt());
            }
            Ok(Value::Null)
        }
        OperationLifecycleState::Cancelled => {
            let result = result.ok_or_else(corrupt)?;
            let cancellation_id = result["cancelled_by"]
                .as_str()
                .filter(|id| !id.is_empty())
                .ok_or_else(corrupt)?;
            if retained.sent_at_ms.is_some()
                || result["reason"]
                    .as_str()
                    .is_none_or(|reason| reason.trim().is_empty())
            {
                return Err(corrupt());
            }
            let cancellation = ExistingAutomaticOperation::load(
                db,
                ExistingAutomaticOperationLookup::OperationId(cancellation_id),
            )?
            .ok_or_else(corrupt)?;
            if cancellation.method != "operation.cancel"
                || cancellation.state != OperationLifecycleState::Settled
                || cancellation.result.as_ref().is_none_or(|result| {
                    result["operation_id"] != cancellation_id
                        || result["cancelled_operation_id"] != retained.operation_id
                        || result["native_cancel_sent"] != false
                })
                || cancellation.original_request["operation_id"] != retained.operation_id
                || cancellation.original_request["reason"] != result["reason"]
                || cancellation.original_request["client_request_id"]
                    != cancellation.client_request_id
            {
                return Err(corrupt());
            }
            Ok(Value::Null)
        }
        OperationLifecycleState::Rejected => {
            let result = result.ok_or_else(corrupt)?;
            let error = if result["error"].is_object() {
                &result["error"]
            } else {
                result
            };
            if error["code"]
                .as_str()
                .is_none_or(|code| code.trim().is_empty())
                || error["message"]
                    .as_str()
                    .is_none_or(|message| message.trim().is_empty())
                || retained.sent_at_ms.is_some()
                || result
                    .get("operation_id")
                    .is_some_and(|id| id != &json!(retained.operation_id))
                || result["task_accepted"] == true
                || result["outcome"] == "applied"
            {
                return Err(corrupt());
            }
            Ok(Value::Null)
        }
        OperationLifecycleState::Settled => {
            let result = result.ok_or_else(corrupt)?;
            if result["operation_id"] != retained.operation_id {
                return Err(corrupt());
            }
            if result["outcome"] == "failed" {
                if result["task_accepted"] != false
                    || result["error"]["code"]
                        .as_str()
                        .is_none_or(|code| code.trim().is_empty())
                    || result["error"]["message"]
                        .as_str()
                        .is_none_or(|message| message.trim().is_empty())
                {
                    return Err(corrupt());
                }
                return Ok(Value::Null);
            }
            let decision_id = result["acceptance_operation_id"]
                .as_str()
                .ok_or_else(corrupt)?;
            let coalesced = result["coalesced"] == true;
            if result["task_accepted"] != true
                || (!coalesced
                    && (result["outcome"] != "applied" || decision_id != retained.operation_id))
                || (coalesced && decision_id == retained.operation_id)
            {
                return Err(corrupt());
            }
            let decision =
                super::acceptance::describe(db, &json!({"acceptance_operation_id":decision_id}))?;
            let request = AcceptRequest::parse(request_value)?;
            if decision["task_id"] != task_id
                || decision["attempt_id"] != request.attempt_id
                || decision["task_revision"] != request.expected_revision
                || decision["submission_ref"] != request.submission_ref
                || decision["candidate_ref"] != request.candidate_ref
                || decision["task_accepted"] != true
            {
                return Err(corrupt());
            }
            if !coalesced
                && (decision["reason"] != request.reason
                    || decision["reviews"] != serde_json::to_value(&request.reviews)?
                    || decision["check_ids"] != serde_json::to_value(&request.check_ids)?
                    || decision["feedback_observation_id"]
                        != request.expected_feedback_observation_id)
            {
                return Err(corrupt());
            }
            let task = tasks::get_task(db, task_id)?;
            if decision["current"] == true
                && (task["state"] != "accepted"
                    || task["accepted_attempt_id"] != request.attempt_id
                    || task["accepted_revision"] != request.expected_revision
                    || task["accepted_candidate_ref"] != request.candidate_ref
                    || task["accepted_phase"] != decision["phase"])
            {
                return Err(corrupt());
            }
            Ok(decision)
        }
    }
}

fn reserve_automatic_acceptance(
    tx: &Transaction<'_>,
    context: &AcceptanceContext,
    request_value: &Value,
    now_ms: i64,
) -> Result<Value> {
    let caller_id = context.technical_requester_id();
    let request_id = model::text(request_value, "client_request_id")?;
    let original_json = model::canonical(request_value)?;
    let old = ExistingAutomaticOperation::load(
        tx,
        ExistingAutomaticOperationLookup::ClientRequest {
            caller_id,
            client_request_id: request_id,
        },
    )?;

    if let Some(retained) = old {
        if retained.caller_id != caller_id
            || retained.client_request_id != request_id
            || retained.method != "task.accept"
            || retained.original_request_json != original_json
        {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "this semantic acceptance request already retains different inputs",
            ));
        }
        if retained.effective_request["automation_on_behalf"] != context.linkage_value()
            || retained.effective_request["request"] != *request_value
            || retained.task_id.as_deref() != Some(context.identity().task_id.as_str())
            || retained.attempt_id.as_deref() != Some(context.identity().attempt_id.as_str())
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "existing semantic acceptance request is linked to a different exact scope",
            ));
        }
        let decision = retained_acceptance_decision(
            tx,
            &retained,
            &context.identity().task_id,
            request_value,
        )?;
        return Ok(classify_retained_acceptance(retained, &decision));
    }

    let operation_id = model::new_id();
    let effective = json!({
        "request":request_value,
        "automation_on_behalf":context.linkage_value()
    });
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'task.accept',?4,?5,'queued',?6,?6,?6)",
        params![
            operation_id,
            caller_id,
            request_id,
            original_json,
            model::canonical(&effective)?,
            now_ms
        ],
    )?;
    save_acceptance_operation_link(tx, &operation_id, context, now_ms)?;

    let mut receipt =
        super::acceptance::reserve_on_behalf(tx, context, request_value, &operation_id)?;
    let prior_decision = receipt["acceptance_operation_id"]
        .as_str()
        .filter(|prior| *prior != operation_id)
        .map(str::to_owned);
    let decision_was_coalesced = receipt["coalesced"] == true;
    receipt["operation_id"] = json!(operation_id);
    if receipt["acceptance_operation_id"].as_str().is_none() {
        receipt["acceptance_operation_id"] =
            json!(prior_decision.as_deref().unwrap_or(&operation_id));
    }
    receipt["coalesced"] = json!(decision_was_coalesced);
    if decision_was_coalesced {
        let prior = prior_decision.as_deref().ok_or_else(|| {
            Error::new(
                "ACCEPTANCE_OPERATION_CORRUPT",
                "coalesced acceptance has no retained prior decision",
            )
        })?;
        let prior_operation = operations::get_operation(tx, prior)?;
        receipt["task_accepted"] = json!(prior_operation["result"]["task_accepted"] == true);
        receipt["state"] = json!("settled");
    } else {
        receipt["state"] = json!("queued");
        receipt["task_accepted"] = json!(false);
    }

    let retained = ExistingAutomaticOperation::load(
        tx,
        ExistingAutomaticOperationLookup::OperationId(&operation_id),
    )?
    .ok_or_else(|| {
        Error::new(
            "ACCEPTANCE_OPERATION_CORRUPT",
            "reserved Operation disappeared",
        )
    })?;
    if retained.caller_id != caller_id
        || retained.client_request_id != request_id
        || retained.method != "task.accept"
        || retained.original_request_json != original_json
        || retained.original_request != *request_value
        || retained.task_id.as_deref() != Some(context.identity().task_id.as_str())
        || retained.attempt_id.as_deref() != Some(context.identity().attempt_id.as_str())
    {
        return Err(Error::new(
            "ACCEPTANCE_OPERATION_CORRUPT",
            "reserved Operation differs from its immutable acceptance context",
        ));
    }
    let mut effective = retained.effective_request;
    effective["automation_on_behalf"] = context.linkage_value();
    effective["receipt"] = json!({"ok":true,"value":receipt});
    let state = if decision_was_coalesced {
        "settled"
    } else {
        "queued"
    };
    tx.execute(
        "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,updated_at_ms=?5,effective_request_json=?6 \
         WHERE operation_id=?1",
        params![
            operation_id,
            state,
            model::canonical(&receipt)?,
            decision_was_coalesced.then_some(now_ms),
            now_ms,
            model::canonical(&effective)?
        ],
    )?;
    capacity::sync_operation(tx, &operation_id, now_ms)?;
    Ok(receipt)
}

fn save_acceptance_operation_link(
    tx: &Transaction<'_>,
    operation_id: &str,
    context: &AcceptanceContext,
    now_ms: i64,
) -> Result<()> {
    let mut cause = context.cause_value();
    cause["id"] = json!(context.review_assignment_id());
    let record = json!({
        "schema_version":1,
        "operation_id":operation_id,
        "technical_requester_id":context.technical_requester_id(),
        "effective_manager_id":context.effective_manager_id(),
        "automation_id":context.automation_id(),
        "automation_revision":context.automation_revision(),
        "project_id":context.project_id(),
        "action":"task.accept",
        "cause":cause,
        "linked_at_ms":now_ms
    });
    let operation_key = config::operation_link_key(operation_id)?;
    config::write_record(tx, &operation_key, &record)?;
    let entry_key = config::entry_operation_key(
        context.effective_manager_id(),
        context.project_id(),
        context.automation_id(),
        operation_id,
    )?;
    config::write_record(tx, &entry_key, &record)
}

enum CheckSelection {
    Ready(Vec<String>),
    Incomplete(Vec<Value>),
}

fn exact_required_check_ids(
    db: &Connection,
    task_id: &str,
    attempt_id: &str,
    candidate_ref: &str,
    policy: &AcceptancePolicy,
) -> Result<CheckSelection> {
    let mut selected = Vec::with_capacity(policy.required_check_profiles.len());
    let mut missing = Vec::new();
    for required in &policy.required_check_profiles {
        let mut statement = db.prepare(
            "SELECT check_id,operation_id,state,exit_code,resource_released_at_ms, \
                    cached_from_check_id,result_ref,coverage_json \
             FROM check_runs WHERE attempt_id=?1 AND candidate_ref=?2 \
               AND json_extract(spec_json,'$.profile_id')=?3 \
               AND json_extract(spec_json,'$.profile_revision')=?4 \
             ORDER BY finished_at_ms DESC,created_at_ms DESC,check_id DESC",
        )?;
        let rows = statement.query_map(
            params![
                attempt_id,
                candidate_ref,
                required.profile_id,
                required.profile_revision
            ],
            |row| {
                let coverage_raw: Option<String> = row.get(7)?;
                let coverage = coverage_raw
                    .map(|raw| serde_json::from_str(&raw))
                    .transpose()
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            7,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?
                    .unwrap_or(Value::Null);
                Ok(CheckRunRow {
                    check_id: row.get(0)?,
                    operation_id: row.get(1)?,
                    state: row.get(2)?,
                    exit_code: row.get(3)?,
                    released_at_ms: row.get(4)?,
                    cached_from_check_id: row.get(5)?,
                    result_ref: row.get(6)?,
                    coverage,
                })
            },
        )?;
        let mut passing = Vec::new();
        for row in rows {
            let row = row?;
            if check_run_is_passing(
                db,
                &row,
                task_id,
                attempt_id,
                candidate_ref,
                &required.profile_id,
                &required.profile_revision,
            )? {
                passing.push(row.check_id);
            }
        }
        if let Some(check_id) = passing.into_iter().next() {
            selected.push(check_id);
        } else {
            missing.push(json!({
                "profile_id":required.profile_id,
                "profile_revision":required.profile_revision
            }));
        }
    }
    if missing.is_empty() {
        Ok(CheckSelection::Ready(selected))
    } else {
        Ok(CheckSelection::Incomplete(missing))
    }
}

fn check_run_is_passing(
    db: &Connection,
    row: &CheckRunRow,
    task_id: &str,
    attempt_id: &str,
    candidate_ref: &str,
    profile_id: &str,
    profile_revision: &str,
) -> Result<bool> {
    let cached = row.cached_from_check_id.is_some();
    if row.state != "passed"
        || (!cached && (row.exit_code != Some(0) || row.released_at_ms.is_none()))
        || row.result_ref.as_deref().is_none_or(str::is_empty)
        || row.coverage["gaps"]
            .as_array()
            .is_none_or(|gaps| !gaps.is_empty())
    {
        return Ok(false);
    }
    let operation = operations::get_operation(db, &row.operation_id)?;
    let request_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [&row.operation_id],
        |result| result.get(0),
    )?;
    let request: CheckRequest = CheckRequest::parse(&serde_json::from_str::<Value>(&request_raw)?)?;
    Ok(operation["method"] == "check.run"
        && operation["state"] == "settled"
        && operation["task_id"] == task_id
        && operation["attempt_id"] == attempt_id
        && operation["result"]["outcome"] == "applied"
        && operation["result"]["check_id"] == row.check_id
        && operation["result"]["state"] == "passed"
        && operation["result"]["source_checkout_verified"] == true
        && (if cached {
            operation["result"]["cached"] == true
                && operation["result"]["cached_from_check_id"]
                    == row.cached_from_check_id.as_deref().unwrap_or_default()
        } else {
            operation["result"]["exit_code"] == 0
        })
        && operation["result"]["result_ref"] == row.result_ref.as_deref().unwrap_or_default()
        && request.attempt_id == attempt_id
        && request.candidate_ref == candidate_ref
        && request.profile_id == profile_id
        && request.profile_revision == profile_revision)
}

fn committed_review_result(
    db: &Connection,
    assignment_id: &str,
    result_operation_id: &str,
) -> Result<Option<CommittedReviewResult>> {
    let assignment_key = format!("assignment:{assignment_id}");
    let assignment_raw: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.assignment'",
            params![REVIEW_STREAM, assignment_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((assignment_json, assignment_operation_id)) = assignment_raw else {
        return Ok(None);
    };
    let assignment: Value = serde_json::from_str(&assignment_json)?;
    if assignment["review_assignment_id"] != assignment_id
        || assignment["operation_id"] != assignment_operation_id
    {
        return Err(Error::new(
            "REVIEW_RECORD_DAMAGED",
            "assignment observation identity differs from its event key",
        ));
    }
    let assignment_operation = operations::get_operation(db, &assignment_operation_id)?;
    if assignment_operation["method"] != "review.assign"
        || assignment_operation["state"] != "settled"
        || assignment_operation["result"]["review_assignment_id"] != assignment_id
        || assignment_operation["result"]["identity"] != assignment["identity"]
    {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "assignment has no matching settled review.assign Operation",
        ));
    }

    let result_key = format!("result:{assignment_id}");
    let result_raw: Option<(i64, String, String)> = db
        .query_row(
            "SELECT observation_id,payload_json,operation_id FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.result'",
            params![REVIEW_STREAM, result_key],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((observation_id, result_json, event_operation_id)) = result_raw else {
        return Ok(None);
    };
    // Only the original Operation that created the result event is a cause;
    // a coalesced submit retry cannot retrigger the acceptance consumer.
    if event_operation_id != result_operation_id {
        return Ok(None);
    }
    let record: Value = serde_json::from_str(&result_json)?;
    if record["schema_version"] != 1
        || record["review_assignment_id"] != assignment_id
        || record["operation_id"] != result_operation_id
        || record["identity"] != assignment["identity"]
    {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "result observation does not match its retained review assignment",
        ));
    }
    let result_operation = operations::get_operation(db, result_operation_id)?;
    if result_operation["method"] != "review.submit"
        || result_operation["state"] != "settled"
        || result_operation["caller_id"] != assignment["reviewer_client_id"]
        || result_operation["result"] != record["result"]
        || record["result"]["review_assignment_id"] != assignment_id
        || record["result"]["reviewer_client_id"] != assignment["reviewer_client_id"]
        || record["result"]["sponsor_client_id"] != assignment["sponsor_client_id"]
    {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "result has no matching settled review.submit Operation and assigned reviewer",
        ));
    }
    let original_request_raw: String = db.query_row(
        "SELECT original_request_json FROM operations WHERE operation_id=?1",
        [result_operation_id],
        |row| row.get(0),
    )?;
    let original_request: Value = serde_json::from_str(&original_request_raw)?;
    if original_request["review_assignment_id"] != assignment_id
        || original_request["submission_ref"] != record["result"]["submission_ref"]
        || original_request["candidate_ref"] != record["result"]["candidate_ref"]
        || original_request["verdict"] != record["result"]["verdict"]
        || original_request["coverage"] != record["result"]["coverage"]
        || original_request["evidence_refs"] != record["result"]["evidence_refs"]
        || normalized_requirement_reviews(&original_request)
            != normalized_requirement_reviews(&record["result"])
    {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "retained result differs from the submitted review and requirement evidence",
        ));
    }
    let identity: ReviewSlotIdentity = serde_json::from_value(assignment["identity"].clone())?;
    if record["result"]["task_id"] != identity.task_id
        || record["result"]["task_revision"] != identity.task_revision
        || record["result"]["attempt_id"] != identity.attempt_id
        || record["result"]["submission_ref"] != identity.submission_ref
        || record["result"]["candidate_ref"] != identity.candidate_ref
    {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "result tuple differs from the exact assigned review slot",
        ));
    }
    let slot_key = format!("review:slot:{}", identity.digest()?);
    if super::meta(db, &slot_key)?.is_none_or(|slot| slot["review_assignment_id"] != assignment_id)
    {
        return Ok(None);
    }
    Ok(Some(CommittedReviewResult {
        assignment,
        record,
        identity,
        observation_id,
    }))
}

fn current_subject(
    db: &Connection,
    identity: &ReviewSlotIdentity,
) -> Result<Option<(Value, Value)>> {
    let task = tasks::get_task(db, &identity.task_id)?;
    let attempt = tasks::get_attempt(db, &identity.attempt_id)?;
    // Reconciliation must also read back the exact accepted subject. Requiring
    // an open Task hides the terminal decision immediately after the worker
    // applies it, leaving the source disposition unable to observe completion.
    let current_state = (task["state"] == "open"
        && task["current_attempt_id"] == identity.attempt_id
        && matches!(
            attempt["state"].as_str(),
            Some("submitted" | "needs_correction")
        ))
        || (task["state"] == "accepted"
            && task["accepted_attempt_id"] == identity.attempt_id
            && task["accepted_revision"] == identity.task_revision
            && task["accepted_candidate_ref"] == identity.candidate_ref
            && attempt["state"] == "accepted");
    let current = current_state
        && task["revision"] == identity.task_revision
        && attempt["task_id"] == identity.task_id
        && attempt["task_revision"] == identity.task_revision
        && attempt["released_at_ms"].is_null()
        && attempt["submission_ref"] == identity.submission_ref
        && attempt["candidate_ref"] == identity.candidate_ref;
    Ok(current.then_some((task, attempt)))
}

fn nonempty_text_array(value: &Value) -> Option<Vec<String>> {
    let values = value.as_array()?;
    if values.is_empty() {
        return None;
    }
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|text| !text.trim().is_empty())
                .map(str::to_owned)
        })
        .collect()
}

fn normalized_requirement_reviews(value: &Value) -> Value {
    match value.get("requirement_reviews") {
        None | Some(Value::Null) => Value::Null,
        Some(Value::Array(reviews)) if reviews.is_empty() => Value::Null,
        Some(reviews) => reviews.clone(),
    }
}

fn feedback_cursor(db: &Connection, submission_ref: &str) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations WHERE kind='task.feedback' \
         AND (json_extract(payload_json,'$.finding.submission_ref')=?1 \
              OR json_extract(payload_json,'$.submission_ref')=?1)",
        [submission_ref],
        |row| row.get(0),
    )?)
}

fn skipped(review: &CommittedReviewResult, code: &str, reason: &str) -> Value {
    json!({
        "status":"skipped",
        "code":code,
        "reason":reason,
        "review_assignment_id":review.assignment["review_assignment_id"],
        "review_result_operation_id":review.record["operation_id"],
        "task_id":review.identity.task_id,
        "task_revision":review.identity.task_revision,
        "attempt_id":review.identity.attempt_id,
        "submission_ref":review.identity.submission_ref,
        "candidate_ref":review.identity.candidate_ref,
        "acceptance_started":false,
        "task_accepted":false,
        "publication_started":false
    })
}

fn capability_gap(review: &CommittedReviewResult, code: &str, reason: &str) -> Value {
    let mut value = skipped(review, code, reason);
    value["status"] = json!("capability_gap");
    value
}

#[cfg(test)]
mod tests {
    use super::{
        AutomationEntry, AutomationStep, ExistingAutomaticOperation, classify_retained_acceptance,
    };
    use crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
    use rusqlite::{Connection, params};
    use serde_json::{Value, json};

    /// Classify one retained `task.accept` Operation through the real decoder and
    /// classifier. The third argument represents the separately validated
    /// current decision, never the admission receipt.
    fn classify(state: &str, result: Option<Value>, receipt: Option<Value>) -> Value {
        let retained = ExistingAutomaticOperation::decode(
            "op-accept".to_owned(),
            state.to_owned(),
            result.map(|value| value.to_string()),
            matches!(state, "sending" | "native_accepted" | "outcome_unknown").then_some(1),
            matches!(state, "settled" | "rejected" | "cancelled").then_some(2),
        )
        .unwrap();
        classify_retained_acceptance(retained, &receipt.unwrap_or(Value::Null))
    }

    /// The exact applied decision receipt the acceptance domain retains.
    fn applied_receipt() -> Value {
        json!({
            "operation_id":"op-accept",
            "acceptance_operation_id":"op-accept",
            "outcome":"applied",
            "task_accepted":true
        })
    }

    #[test]
    fn cancelled_automatic_acceptance_is_truthful_not_corrupt() {
        // A retained `cancelled` Operation is a real terminal state, not
        // corruption, and never an applied or coalesced acceptance.
        let classified = classify(
            "cancelled",
            Some(json!({"reason":"cancel queued acceptance","cancelled_by":"cancel-op-accept"})),
            None,
        );
        assert_eq!(classified["status"], "acceptance_cancelled");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["coalesced"], false);
    }

    #[test]
    fn rejected_acceptance_is_not_task_accepted() {
        let classified = classify(
            "rejected",
            Some(json!({"operation_id":"op-accept","outcome":"rejected","code":"scope_denied"})),
            None,
        );
        assert_eq!(classified["status"], "acceptance_rejected");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["coalesced"], false);
    }

    #[test]
    fn settled_nonapplied_acceptance_does_not_satisfy_slot() {
        // A settled state carrying no accepted decision is a terminal result
        // that did not apply, so it must not be reported as task accepted.
        let classified = classify(
            "settled",
            Some(json!({"operation_id":"op-accept","outcome":"not_applied"})),
            None,
        );
        assert_eq!(classified["status"], "acceptance_not_applied");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["task_accepted"], false);
    }

    #[test]
    fn settled_exact_accepted_decision_is_acceptance_applied() {
        let classified = classify(
            "settled",
            Some(applied_receipt()),
            Some(json!({"current":true,"invalidated":false})),
        );
        assert_eq!(classified["status"], "acceptance_applied");
        assert_eq!(classified["terminal_not_applied"], false);
        assert_eq!(classified["task_accepted"], true);
    }

    #[test]
    fn acceptance_unknown_stays_pending_without_duplicate_operation() {
        for state in ["queued", "sending", "native_accepted"] {
            let classified = classify(state, None, None);
            assert_eq!(classified["status"], "acceptance_in_flight");
            assert_eq!(classified["coalesced"], false);
        }
        let unknown = classify("outcome_unknown", None, None);
        assert_eq!(unknown["status"], "acceptance_readback_required");
        assert_eq!(unknown["readback_required"], true);
    }

    #[test]
    fn queued_acceptance_does_not_claim_an_effect_started() {
        // The generic lifecycle must not invent effect-start proof: a queued
        // Operation has not been dispatched by the Store runtime.
        let classified = classify("queued", None, None);
        assert_eq!(classified["acceptance_started"], false);
    }

    #[test]
    fn blank_retained_result_is_corruption_not_absence() {
        // A blank result column value is retained but malformed JSON, so it must
        // never be mistaken for an absent result and classified as in flight.
        let error = ExistingAutomaticOperation::decode(
            "op-accept".to_owned(),
            "settled".to_owned(),
            Some("   ".to_owned()),
            None,
            Some(2),
        )
        .expect_err("a blank retained result is malformed JSON");
        assert_eq!(error.code, "AUTOMATION_OPERATION_CORRUPT");
    }

    #[test]
    fn absent_retained_result_is_distinct_from_blank() {
        let retained = ExistingAutomaticOperation::decode(
            "op-accept".to_owned(),
            "queued".to_owned(),
            None,
            None,
            None,
        )
        .expect("an absent result column is not corruption");
        assert!(retained.result.is_none());
    }

    /// One isolated Store with the acceptance step selected, mirroring the existing
    /// sibling fixture convention.
    fn acceptance_fixture() -> (Connection, AutomationEntry) {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(super::super::SCHEMA).unwrap();
        let mut entry = AutomationEntry::new("manager", "fixture", "acceptance", 1);
        entry.enabled = true;
        entry.steps = vec![AutomationStep::Acceptance];
        (db, entry)
    }

    /// Insert one retained `task.accept` Operation row exactly as the Store
    /// persists it, and return its id.
    ///
    /// This models retained history only; the lifecycle state is never rewritten to
    /// make a classification pass.
    fn insert_acceptance_operation(
        db: &Connection,
        caller: &str,
        request_id: &str,
        state: &str,
        result: Option<Value>,
    ) -> String {
        let operation_id = format!("op-{request_id}");
        db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'task.accept','{}','{}',?4,?5,?6,1,1,1)",
        params![
            operation_id,
            caller,
            request_id,
            state,
            result.map(|value| value.to_string()),
            matches!(state, "settled" | "rejected" | "cancelled").then_some(1_i64),
        ],
    )
    .unwrap();
        operation_id
    }

    fn count_acceptance_operations(db: &Connection, request_id: &str) -> i64 {
        db.query_row(
            "SELECT count(*) FROM operations WHERE client_request_id=?1 AND method='task.accept'",
            [request_id],
            |row| row.get(0),
        )
        .unwrap()
    }

    /// A retained cancelled acceptance is classified as its own terminal state, is
    /// never reported as applied or coalesced, and never causes a replacement
    /// Operation under the same semantic request ID.
    #[test]
    fn retained_cancelled_acceptance_is_terminal_not_applied_without_replay() {
        let (db, _entry) = acceptance_fixture();
        insert_acceptance_operation(
            &db,
            AUTOMATION_TECHNICAL_REQUESTER_ID,
            "accept-cancelled",
            "cancelled",
            Some(json!({
                "operation_id":"op-accept-cancelled",
                "outcome":"cancelled_before_effect",
                "task_accepted":false
            })),
        );

        let retained_state: String = db
            .query_row(
                "SELECT state FROM operations WHERE client_request_id='accept-cancelled'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(retained_state, "cancelled");

        // Classify the retained row through the shared decoder and the real
        // acceptance classifier rather than asserting on the raw state string.
        let raw: String = db
            .query_row(
                "SELECT result_json FROM operations WHERE client_request_id='accept-cancelled'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let retained = ExistingAutomaticOperation::decode(
            "op-accept-cancelled".to_owned(),
            retained_state,
            Some(raw),
            None,
            Some(2),
        )
        .expect("a cancelled acceptance row decodes");
        let classified = json!(classify_retained_acceptance(retained, &json!({})));
        assert_eq!(classified["status"], "acceptance_cancelled");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["coalesced"], false);

        // No replacement row is created for the same semantic request.
        assert_eq!(count_acceptance_operations(&db, "accept-cancelled"), 1);
    }

    /// A retained rejected acceptance keeps its own terminal disposition and is
    /// never reported as task accepted.
    #[test]
    fn retained_rejected_acceptance_is_not_task_accepted() {
        let (db, _entry) = acceptance_fixture();
        insert_acceptance_operation(
            &db,
            AUTOMATION_TECHNICAL_REQUESTER_ID,
            "accept-rejected",
            "rejected",
            Some(json!({
                "operation_id":"op-accept-rejected",
                "outcome":"rejected",
                "code":"scope_denied",
                "task_accepted":false
            })),
        );

        let raw: String = db
            .query_row(
                "SELECT result_json FROM operations WHERE client_request_id='accept-rejected'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let retained = ExistingAutomaticOperation::decode(
            "op-accept-rejected".to_owned(),
            "rejected".to_owned(),
            Some(raw),
            None,
            Some(2),
        )
        .expect("a rejected acceptance row decodes");
        let classified = json!(classify_retained_acceptance(retained, &json!({})));
        assert_eq!(classified["status"], "acceptance_rejected");
        assert_eq!(classified["terminal_not_applied"], true);
        assert_eq!(classified["task_accepted"], false);
        assert_eq!(count_acceptance_operations(&db, "accept-rejected"), 1);
    }

    /// A retained unknown acceptance stays unresolved, requires readback, and never
    /// authorizes a replay under the same semantic request ID.
    #[test]
    fn retained_unknown_acceptance_requires_readback_without_replay() {
        let (db, _entry) = acceptance_fixture();
        insert_acceptance_operation(
            &db,
            AUTOMATION_TECHNICAL_REQUESTER_ID,
            "accept-unknown",
            "outcome_unknown",
            Some(json!({
                "operation_id":"op-accept-unknown",
                "outcome":"unknown",
                "task_accepted":false
            })),
        );

        let raw: String = db
            .query_row(
                "SELECT result_json FROM operations WHERE client_request_id='accept-unknown'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let retained = ExistingAutomaticOperation::decode(
            "op-accept-unknown".to_owned(),
            "outcome_unknown".to_owned(),
            Some(raw),
            Some(1),
            None,
        )
        .expect("an unknown acceptance row decodes");
        let classified = json!(classify_retained_acceptance(retained, &json!({})));
        assert_eq!(classified["status"], "acceptance_readback_required");
        assert_eq!(classified["readback_required"], true);
        assert_eq!(count_acceptance_operations(&db, "accept-unknown"), 1);
    }

    /// An admission-shaped receipt without a validated current decision cannot
    /// establish applied acceptance, even when the result names this Operation.
    #[test]
    fn unvalidated_admission_receipt_does_not_establish_acceptance() {
        let (db, _entry) = acceptance_fixture();
        insert_acceptance_operation(
            &db,
            AUTOMATION_TECHNICAL_REQUESTER_ID,
            "accept-mismatch",
            "settled",
            Some(json!({
                "operation_id":"op-accept-mismatch",
                "outcome":"applied",
                "task_accepted":true
            })),
        );

        let raw: String = db
            .query_row(
                "SELECT result_json FROM operations WHERE client_request_id='accept-mismatch'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let retained = ExistingAutomaticOperation::decode(
            "op-accept-mismatch".to_owned(),
            "settled".to_owned(),
            Some(raw),
            None,
            Some(2),
        )
        .expect("a settled acceptance row decodes");
        // The effective-request receipt belongs to a different semantic Operation.
        let effective = json!({"receipt":{"ok":true,"value":{
            "operation_id":"op-some-other-operation",
            "outcome":"applied",
            "task_accepted":true
        }}});
        let classified = json!(classify_retained_acceptance(retained, &effective));
        assert_eq!(classified["status"], "acceptance_not_applied");
        assert_eq!(classified["task_accepted"], false);
        assert_eq!(classified["receipt_identity_mismatch"], Value::Null);
    }

    /// A settled decision receipt naming this exact Operation is applied.
    #[test]
    fn settled_receipt_naming_this_operation_is_applied() {
        let (db, _entry) = acceptance_fixture();
        insert_acceptance_operation(
            &db,
            AUTOMATION_TECHNICAL_REQUESTER_ID,
            "accept-applied",
            "settled",
            Some(json!({
                "operation_id":"op-accept-applied",
                "outcome":"applied",
                "task_accepted":true
            })),
        );

        let raw: String = db
            .query_row(
                "SELECT result_json FROM operations WHERE client_request_id='accept-applied'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let retained = ExistingAutomaticOperation::decode(
            "op-accept-applied".to_owned(),
            "settled".to_owned(),
            Some(raw),
            None,
            Some(2),
        )
        .expect("a settled acceptance row decodes");
        let classified = json!(classify_retained_acceptance(
            retained,
            &json!({"current":true,"invalidated":false})
        ));
        assert_eq!(classified["status"], "acceptance_applied");
        assert_eq!(classified["task_accepted"], true);
    }
}
