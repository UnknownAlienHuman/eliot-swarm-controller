//! Bounded review-result consumer for selected manager-owned disposition.

use super::{capacity, operations, reviews, submissions, tasks};
use crate::{
    automation::{
        actions::AutomationStep, authorization, config, disposition::ReviewDispositionContext,
    },
    config::Config,
    error::{Error, Result},
    model,
    review::ReviewSlotIdentity,
    submission::ChangeRequest,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const REVIEW_STREAM: &str = "controller:review";
struct CommittedReviewResult {
    assignment: Value,
    record: Value,
    identity: ReviewSlotIdentity,
}

/// Consume one canonical retained review result inside its Store transaction.
/// The root Store calls this after the review.submit Operation has been
/// settled. A duplicate/coalesced submit Operation is not treated as a second
/// result cause.
pub(super) fn consume_review_result_for_entry(
    tx: &Transaction<'_>,
    config_value: &Config,
    entry: &config::AutomationEntry,
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
            "disposition_applied":false
        }));
    };

    let result = &review.record["result"];
    if !entry.enabled
        || (!entry.steps.contains(&AutomationStep::ReviewDisposition)
            && !entry.steps.contains(&AutomationStep::RepairDispatch)
            && !entry.steps.contains(&AutomationStep::Acceptance))
    {
        return Ok(skipped(
            &review,
            "review_disposition_not_selected",
            "this current automation entry does not select a result disposition action",
        ));
    }
    authorization::require_registered_manager(tx, &entry.owner_manager_id)?;
    let current_entry = config::load_entry(
        tx,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    if !current_entry.is_some_and(|current| {
        current.enabled
            && current.revision == entry.revision
            && current.steps == entry.steps
            && current.scope == entry.scope
    }) {
        return Ok(skipped(
            &review,
            "automation_action_changed",
            "the retained entry revision no longer selects this result action",
        ));
    }
    if result["applicability"] != "current_candidate" {
        return Ok(skipped(
            &review,
            "historical_review_result",
            "the retained review result is not applicable to the current candidate",
        ));
    }

    let manager_id = model::text(&review.assignment, "sponsor_client_id")?;
    let independent_acceptance =
        result["verdict"] == "pass" && entry.steps.contains(&AutomationStep::Acceptance);
    let (task, attempt) = match current_subject(tx, &review.identity)? {
        Some(subject) => subject,
        None => {
            return Ok(skipped(
                &review,
                "stale_review_subject",
                "Task revision, Attempt owner, submission, or candidate is no longer current",
            ));
        }
    };
    let project_id = model::text(&task, "project_id")?;
    if project_id != entry.project_id {
        return Ok(skipped(
            &review,
            "automation_project_mismatch",
            "the exact Task is outside this automation entry project",
        ));
    }
    let acceptance_selected = entry.steps.contains(&AutomationStep::Acceptance);
    let disposition_selected = entry.steps.contains(&AutomationStep::ReviewDisposition);
    let repair_selected = entry.steps.contains(&AutomationStep::RepairDispatch);
    let attempt_owner_id = model::text(&attempt, "owner_id")?;
    let ordinary_owner_scope =
        attempt_owner_id == entry.owner_manager_id && manager_id == entry.owner_manager_id;
    let transferred_disposition = result["verdict"] == "changes_requested"
        && disposition_selected
        && attempt_owner_id != entry.owner_manager_id;
    let transferred_sponsor_scope = if transferred_disposition {
        match authorization::current_transferred_attempt_authority(
            tx,
            entry,
            AutomationStep::ReviewDisposition,
            &review.identity.task_id,
            review.identity.task_revision,
            &review.identity.attempt_id,
            &review.identity.submission_ref,
            &review.identity.candidate_ref,
        ) {
            Ok(Some(proof)) => {
                proof.source_attempt_owner_id() == attempt_owner_id
                    && proof.successor_manager_id() == entry.owner_manager_id
                    && proof.contains_manager_id(manager_id)
            }
            Ok(None) => false,
            Err(error) if error.code == "FORBIDDEN" => false,
            Err(error) => return Err(error),
        }
    } else {
        false
    };
    // A passing acceptance remains an independent current-GM path. Successor
    // authority here is only for this exact non-pass disposition and does not
    // widen repair, acceptance, or other Task actions.
    if !independent_acceptance && !ordinary_owner_scope && !transferred_sponsor_scope {
        return Ok(skipped(
            &review,
            "automation_owner_mismatch",
            "the retained sponsor is not the current owner or a manager in the exact transferred Attempt lineage",
        ));
    }

    match result["verdict"].as_str() {
        Some("pass") => {
            let disposition = disposition_selected.then(|| {
                skipped(
                    &review,
                    "review_pass_is_advisory",
                    "a passing review does not request a return for correction",
                )
            });
            let acceptance = if acceptance_selected {
                Some(
                    super::automation_acceptance::consume_review_result_for_entry(
                        tx,
                        entry,
                        review_assignment_id,
                        review_result_operation_id,
                        now_ms,
                    )?,
                )
            } else {
                None
            };
            let repair = if repair_selected {
                Some(super::automation_repair::consume_review_result_for_entry(
                    tx,
                    config_value,
                    entry,
                    review_assignment_id,
                    review_result_operation_id,
                    now_ms,
                )?)
            } else {
                None
            };
            return Ok(compose_action_results(
                &review,
                disposition,
                repair,
                acceptance,
            ));
        }
        Some("inconclusive") => {
            let disposition = disposition_selected.then(|| {
                skipped(
                    &review,
                    "inconclusive_review_result",
                    "inconclusive evidence does not authorize a return for correction",
                )
            });
            let acceptance = if acceptance_selected {
                Some(
                    super::automation_acceptance::consume_review_result_for_entry(
                        tx,
                        entry,
                        review_assignment_id,
                        review_result_operation_id,
                        now_ms,
                    )?,
                )
            } else {
                None
            };
            let repair = if repair_selected {
                Some(super::automation_repair::consume_review_result_for_entry(
                    tx,
                    config_value,
                    entry,
                    review_assignment_id,
                    review_result_operation_id,
                    now_ms,
                )?)
            } else {
                None
            };
            return Ok(compose_action_results(
                &review,
                disposition,
                repair,
                acceptance,
            ));
        }
        Some("changes_requested") => {}
        _ => {
            return Err(Error::new(
                "REVIEW_RESULT_DAMAGED",
                "retained review result has an unsupported verdict",
            ));
        }
    }

    let disposition = if disposition_selected {
        consume_selected_disposition(
            tx,
            entry,
            &review,
            &attempt,
            review_assignment_id,
            review_result_operation_id,
            manager_id,
            now_ms,
        )?
    } else {
        skipped(
            &review,
            "review_disposition_not_selected",
            "the current manager automation does not select review_disposition",
        )
    };
    if !repair_selected {
        if acceptance_selected {
            let acceptance = super::automation_acceptance::consume_review_result_for_entry(
                tx,
                entry,
                review_assignment_id,
                review_result_operation_id,
                now_ms,
            )?;
            return Ok(compose_action_results(
                &review,
                Some(disposition),
                None,
                Some(acceptance),
            ));
        }
        return Ok(disposition);
    }

    let repair = super::automation_repair::consume_review_result_for_entry(
        tx,
        config_value,
        entry,
        review_assignment_id,
        review_result_operation_id,
        now_ms,
    )?;
    let acceptance = if acceptance_selected {
        Some(
            super::automation_acceptance::consume_review_result_for_entry(
                tx,
                entry,
                review_assignment_id,
                review_result_operation_id,
                now_ms,
            )?,
        )
    } else {
        None
    };
    Ok(compose_action_results(
        &review,
        Some(disposition),
        Some(repair),
        acceptance,
    ))
}

fn compose_action_results(
    review: &CommittedReviewResult,
    disposition: Option<Value>,
    repair: Option<Value>,
    acceptance: Option<Value>,
) -> Value {
    if repair.is_none() && acceptance.is_none() {
        return disposition.unwrap_or_else(|| {
            json!({
                "status":"skipped",
                "review_assignment_id":review.assignment["review_assignment_id"],
                "review_result_operation_id":review.record["operation_id"],
                "disposition_applied":false
            })
        });
    }
    // Report the selected action's own outcome. In particular, mailbox
    // feedback is retained as provenance and never promoted to a repair
    // delivery receipt.
    let status_source = acceptance
        .as_ref()
        .filter(|value| value["status"] != "skipped")
        .or(repair.as_ref())
        .or(disposition.as_ref());
    let status = status_source
        .map(|value| value["status"].clone())
        .unwrap_or_else(|| json!("skipped"));
    let feedback = disposition
        .as_ref()
        .and_then(|value| value.get("feedback"))
        .cloned()
        .unwrap_or(Value::Null);
    let disposition_applied = disposition
        .as_ref()
        .is_some_and(|value| value["disposition_applied"] == true);
    json!({
        "status":status,
        "code":status_source.and_then(|value| value.get("code")).cloned().unwrap_or(Value::Null),
        "reason":status_source.and_then(|value| value.get("reason")).cloned().unwrap_or(Value::Null),
        "operation_id":status_source.and_then(|value| value.get("operation_id")).cloned().unwrap_or(Value::Null),
        "coalesced":status_source.and_then(|value| value.get("coalesced")).cloned().unwrap_or(json!(false)),
        "review_assignment_id":review.assignment["review_assignment_id"],
        "review_result_operation_id":review.record["operation_id"],
        "disposition":disposition,
        "feedback":feedback,
        "repair_dispatch":repair,
        "acceptance":acceptance,
        "disposition_applied":disposition_applied
    })
}

#[allow(clippy::too_many_arguments)]
fn consume_selected_disposition(
    tx: &Transaction<'_>,
    entry: &config::AutomationEntry,
    review: &CommittedReviewResult,
    attempt: &Value,
    review_assignment_id: &str,
    review_result_operation_id: &str,
    review_assignment_sponsor_id: &str,
    now_ms: i64,
) -> Result<Value> {
    let result = &review.record["result"];
    if !crate::policy::allows_scoped_manager_feedback(&attempt["task_snapshot"]) {
        return Ok(capability_gap(
            review,
            "owner_policy_v2_required",
            "manager-owned review disposition is available only for a new Attempt explicitly bound to owner-policy-v2",
        ));
    }

    let findings = result["findings"].as_array().ok_or_else(|| {
        Error::new(
            "REVIEW_RESULT_DAMAGED",
            "changes_requested result has no findings array",
        )
    })?;
    match findings.len() {
        0 => {
            return Err(Error::new(
                "REVIEW_RESULT_DAMAGED",
                "changes_requested result has no actionable finding",
            ));
        }
        1 => {}
        count => {
            return Ok(json!({
                "status":"capability_gap",
                "code":"manual_finding_selection_required",
                "reason":"the current Task feedback handler accepts one finding per decision; the automation will not choose among multiple reviewer findings",
                "review_assignment_id":review_assignment_id,
                "review_result_operation_id":review_result_operation_id,
                "actionable_finding_count":count,
                "disposition_applied":false
            }));
        }
    }

    let finding_id = model::text(&findings[0], "finding_id")?;
    let provenance = reviews::actionable_finding(
        tx,
        &review.identity.task_id,
        &review.identity.attempt_id,
        review.identity.task_revision,
        &review.identity.submission_ref,
        &review.identity.candidate_ref,
        finding_id,
    )?;
    if provenance["review_assignment_id"] != review_assignment_id
        || provenance["review_operation_id"] != review_result_operation_id
        || provenance["identity"] != json!(review.identity)
        || provenance["finding"] != findings[0]
    {
        return Err(Error::new(
            "REVIEW_RESULT_DAMAGED",
            "actionable finding differs from the committed review result",
        ));
    }

    if entry.scope.work_pool_id.is_some() {
        return Ok(capability_gap(
            review,
            "work_pool_scope_unavailable",
            "the current Task source has no committed work-pool membership reader",
        ));
    }
    let context = match ReviewDispositionContext::from_committed_entry(
        tx,
        entry,
        review.identity.clone(),
        review_assignment_id,
        review_result_operation_id,
        review_assignment_sponsor_id,
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
                review,
                "review_disposition_unavailable",
                &error.message,
            ));
        }
        Err(error) => return Err(error),
    };

    let request = change_request(&context, &review.identity, &provenance["finding"])?;
    let request_value = serde_json::to_value(&request)?;
    let (operation_id, value, coalesced) =
        reserve_feedback_operation(tx, entry, &context, &request, &request_value, now_ms)?;
    Ok(json!({
        "status":if value["code"] == "semantic_duplicate_requires_current_disposition" {"semantic_duplicate_requires_current_disposition"} else if coalesced {"coalesced"} else if value["applied"] == true {"applied"} else {"settled_without_application"},
        "code":value.get("code").cloned().unwrap_or(Value::Null),
        "action":"task.request_changes",
        "operation_id":operation_id,
        "coalesced":coalesced,
        "applied":value["applied"] == true,
        "review_assignment_id":review_assignment_id,
        "review_result_operation_id":review_result_operation_id,
        "feedback":value,
        "disposition_applied":value["applied"] == true,
        "current_disposition_recorded":value.get("current_disposition_recorded").and_then(Value::as_bool).unwrap_or(value["applied"] == true)
    }))
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
    let (assignment_json, assignment_observation_operation_id) =
        assignment_raw.ok_or_else(|| {
            Error::new(
                "REVIEW_ASSIGNMENT_NOT_FOUND",
                "review result has no retained exact assignment",
            )
        })?;
    let assignment: Value = serde_json::from_str(&assignment_json)?;
    if assignment["review_assignment_id"] != assignment_id
        || assignment["operation_id"] != assignment_observation_operation_id
    {
        return Err(Error::new(
            "REVIEW_RECORD_DAMAGED",
            "assignment observation identity differs from its event key",
        ));
    }
    let assignment_operation = operations::get_operation(db, &assignment_observation_operation_id)?;
    if assignment_operation["method"] != "review.assign"
        || assignment_operation["state"] != "settled"
        || assignment_operation["result"]["review_assignment_id"] != assignment_id
        || assignment_operation["result"]["identity"] != assignment["identity"]
        || assignment_operation["result"]["sponsor_client_id"] != assignment["sponsor_client_id"]
    {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "assignment has no matching settled review.assign Operation",
        ));
    }

    let result_key = format!("result:{assignment_id}");
    let result_raw: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.result'",
            params![REVIEW_STREAM, result_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((result_json, result_observation_operation_id)) = result_raw else {
        return Ok(None);
    };
    // A transport retry may create a second settled review.submit Operation
    // which coalesces to the original result. Only the original result event
    // is a new automation cause.
    if result_observation_operation_id != result_operation_id {
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
    Ok(Some(CommittedReviewResult {
        assignment,
        record,
        identity,
    }))
}

fn current_subject(
    db: &Connection,
    identity: &ReviewSlotIdentity,
) -> Result<Option<(Value, Value)>> {
    let task = tasks::get_task(db, &identity.task_id)?;
    let attempt = tasks::get_attempt(db, &identity.attempt_id)?;
    let current = task["state"] == "open"
        && task["revision"] == identity.task_revision
        && task["current_attempt_id"] == identity.attempt_id
        && attempt["task_id"] == identity.task_id
        && attempt["task_revision"] == identity.task_revision
        && attempt["released_at_ms"].is_null()
        && attempt["submission_ref"] == identity.submission_ref
        && attempt["candidate_ref"] == identity.candidate_ref
        && matches!(
            attempt["state"].as_str(),
            Some("submitted" | "needs_correction")
        );
    Ok(current.then_some((task, attempt)))
}

fn change_request(
    context: &ReviewDispositionContext,
    identity: &ReviewSlotIdentity,
    finding: &Value,
) -> Result<ChangeRequest> {
    let finding_id = model::text(finding, "finding_id")?.to_owned();
    let requirements = finding["requirement_ids"]
        .as_array()
        .ok_or_else(|| Error::new("REVIEW_RESULT_DAMAGED", "finding requirements are missing"))?
        .iter()
        .map(|value| {
            value.as_str().map(str::to_owned).ok_or_else(|| {
                Error::new("REVIEW_RESULT_DAMAGED", "finding requirement is invalid")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let evidence = finding["evidence_refs"]
        .as_array()
        .ok_or_else(|| Error::new("REVIEW_RESULT_DAMAGED", "finding evidence is missing"))?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or_else(|| Error::new("REVIEW_RESULT_DAMAGED", "finding evidence is invalid"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(ChangeRequest {
        client_request_id: context.semantic_request_id(&identity.submission_ref, &finding_id)?,
        attempt_id: identity.attempt_id.clone(),
        expected_revision: identity.task_revision,
        submission_ref: identity.submission_ref.clone(),
        candidate_ref: identity.candidate_ref.clone(),
        finding_id,
        reason: model::text(finding, "reason")?.to_owned(),
        requirement_ids: requirements,
        evidence,
    })
}

fn reserve_feedback_operation(
    tx: &Transaction<'_>,
    entry: &config::AutomationEntry,
    context: &ReviewDispositionContext,
    request: &ChangeRequest,
    request_value: &Value,
    now_ms: i64,
) -> Result<(String, Value, bool)> {
    let caller_id = context.technical_requester_id();
    let original_json = model::canonical(request_value)?;
    let existing: Option<(String, String, String, String, Option<String>)> = tx
        .query_row(
            "SELECT operation_id,method,original_request_json,state,result_json FROM operations \
             WHERE caller_id=?1 AND client_request_id=?2",
            params![caller_id, request.client_request_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    if let Some((operation_id, method, original, state, result)) = existing {
        if method != "task.request_changes" || original != original_json {
            return Err(Error::new(
                "REVIEW_DISPOSITION_CONFLICT",
                "the manager/submission/finding request ID is already used for another action",
            ));
        }
        if state != "settled" {
            return Ok((
                operation_id.clone(),
                json!({
                    "status":"pending",
                    "operation_id":operation_id,
                    "applied":false,
                    "reason":"an identical retained disposition Operation is unresolved"
                }),
                true,
            ));
        }
        let link = authorization::operation_link(tx, &operation_id)?.ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "settled automatic feedback Operation has no manager linkage",
            )
        })?;
        if link.action != "task.request_changes"
            || link.effective_manager_id != context.effective_manager_id()
            || link.project_id != context.project_id()
            || link.cause["identity"]["task_id"] != context.identity().task_id
            || link.cause["identity"]["task_revision"] != context.identity().task_revision
            || link.cause["identity"]["attempt_id"] != context.identity().attempt_id
            || link.cause["identity"]["submission_ref"] != context.identity().submission_ref
            || link.cause["identity"]["candidate_ref"] != context.identity().candidate_ref
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "settled feedback Operation belongs to another manager or semantic subject",
            ));
        }
        let same_review_cause = link.cause["kind"] == "review_result"
            && link.cause["id"] == context.review_assignment_id()
            && link.cause["review_assignment_id"] == context.review_assignment_id()
            && link.cause["operation_id"] == context.review_result_operation_id()
            && link.cause["identity"] == json!(context.identity())
            && link
                .cause
                .get("review_assignment_sponsor_id")
                .is_none_or(|sponsor| sponsor == context.review_assignment_sponsor_id());
        if !same_review_cause {
            return Ok((
                operation_id.clone(),
                json!({
                    "status":"semantic_duplicate_requires_current_disposition",
                    "code":"semantic_duplicate_requires_current_disposition",
                    "operation_id":operation_id,
                    "semantic_duplicate_operation_id":operation_id,
                    "coalesced":true,
                    "applied":false,
                    "current_disposition_recorded":false,
                    "prior_review_cause":{
                        "review_assignment_id":link.cause["review_assignment_id"],
                        "review_result_operation_id":link.cause["operation_id"]
                    },
                    "reason":"the manager/submission/finding semantic decision already has a settled Operation for another assigned review result; that Operation does not record a disposition for this exact assignment"
                }),
                true,
            ));
        }
        let mut result: Value = serde_json::from_str(&result.ok_or_else(|| {
            Error::new(
                "AUTOMATION_OPERATION_CORRUPT",
                "settled feedback Operation has no result",
            )
        })?)?;
        result["coalesced"] = json!(true);
        return Ok((operation_id, result, true));
    }

    let operation_id = model::new_id();
    let effective = json!({
        "request":request_value,
        "automation_on_behalf":context.linkage_value()
    });
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'task.request_changes',?4,?5,'queued',?6,?6,?6)",
        params![
            operation_id,
            caller_id,
            request.client_request_id,
            original_json,
            model::canonical(&effective)?,
            now_ms
        ],
    )?;
    save_operation_link(tx, context, entry, &operation_id, now_ms)?;

    tx.execute_batch("SAVEPOINT automation_review_disposition")?;
    let feedback =
        submissions::request_changes_on_behalf(tx, context, request, &operation_id, now_ms);
    let value = match feedback {
        Ok(value) => {
            tx.execute_batch("RELEASE SAVEPOINT automation_review_disposition")?;
            value
        }
        Err(error) if is_expected_disposition_conflict(&error) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT automation_review_disposition; \
                 RELEASE SAVEPOINT automation_review_disposition",
            )?;
            json!({
                "operation_id":operation_id,
                "applied":false,
                "status":"conflict",
                "error":error
            })
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO SAVEPOINT automation_review_disposition; \
                 RELEASE SAVEPOINT automation_review_disposition",
            )?;
            return Err(error);
        }
    };
    let mut effective = effective;
    effective["receipt"] = json!({"ok":true,"value":value});
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3,state='settled',result_json=?4,settled_at_ms=?5,updated_at_ms=?5,effective_request_json=?6 WHERE operation_id=?1",
        params![
            operation_id,
            context.identity().task_id,
            context.identity().attempt_id,
            model::canonical(&value)?,
            now_ms,
            model::canonical(&effective)?
        ],
    )?;
    capacity::sync_operation(tx, &operation_id, now_ms)?;
    tx.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES('controller',?1,?1,'task.request_changes',?2,?3)",
        params![operation_id, model::canonical(&value)?, now_ms],
    )?;
    Ok((operation_id, value, false))
}

fn save_operation_link(
    db: &Connection,
    context: &ReviewDispositionContext,
    entry: &config::AutomationEntry,
    operation_id: &str,
    now_ms: i64,
) -> Result<()> {
    let link = json!({
        "schema_version":1,
        "operation_id":operation_id,
        "technical_requester_id":context.technical_requester_id(),
        "effective_manager_id":context.effective_manager_id(),
        "automation_id":context.automation_id(),
        "automation_revision":context.automation_revision(),
        "project_id":context.project_id(),
        "action":"task.request_changes",
        "cause":context.cause_value(),
        "linked_at_ms":now_ms
    });
    let operation_key = config::operation_link_key(operation_id)?;
    config::write_record(db, &operation_key, &link)?;
    let index_key = config::entry_operation_key(
        context.effective_manager_id(),
        context.project_id(),
        &entry.automation_id,
        operation_id,
    )?;
    config::write_record(db, &index_key, &link)?;
    Ok(())
}

fn is_expected_disposition_conflict(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "FORBIDDEN"
            | "STALE_REVIEW_SUBJECT"
            | "REVIEW_FINDING_NOT_FOUND"
            | "REVIEW_FINDING_NOT_ACTIONABLE"
            | "REVIEW_ANCHOR_MISMATCH"
            | "REVIEW_FINDING_MISMATCH"
            | "REVIEW_EVIDENCE_MISMATCH"
            | "REVIEW_DISPOSITION_CONFLICT"
    )
}

fn skipped(review: &CommittedReviewResult, reason: &str, explanation: &str) -> Value {
    json!({
        "status":"skipped",
        "reason":reason,
        "explanation":explanation,
        "review_assignment_id":review.assignment["review_assignment_id"],
        "review_result_operation_id":review.record["operation_id"],
        "disposition_applied":false
    })
}

fn capability_gap(review: &CommittedReviewResult, code: &str, explanation: &str) -> Value {
    json!({
        "status":"capability_gap",
        "code":code,
        "reason":explanation,
        "review_assignment_id":review.assignment["review_assignment_id"],
        "review_result_operation_id":review.record["operation_id"],
        "disposition_applied":false
    })
}
