//! Exact acceptance consumer for a selected manager automation.
//!
//! It reuses the Store's guarded task.accept reserve path after proving the
//! exact assigned review, current candidate, current GM authority and required
//! CheckRuns. The asynchronous acceptance worker remains responsible for
//! artifact byte verification and the final Task transition.

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

struct ExistingAcceptanceOperation {
    operation_id: String,
    method: String,
    original_request_json: String,
    state: String,
    result_json: Option<String>,
    effective_request_json: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
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
    let gm: Option<(String, i64)> = tx
        .query_row(
            "SELECT json_extract(value_json,'$.client_id'),json_extract(value_json,'$.epoch') \
             FROM meta WHERE key='gm'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((current_gm_id, gm_epoch)) = gm else {
        return Ok(capability_gap(
            &review,
            "current_gm_required",
            "automatic acceptance requires the selected manager to hold the current GM designation",
        ));
    };
    if current_gm_id != entry.owner_manager_id || gm_epoch <= 0 {
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
    let mut value = json!({
        "status":if coalesced {"coalesced"} else {"acceptance_reserved"},
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
        "acceptance_started":matches!(receipt["state"].as_str(), Some("queued" | "sending" | "outcome_unknown")),
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

fn reserve_automatic_acceptance(
    tx: &Transaction<'_>,
    context: &AcceptanceContext,
    request_value: &Value,
    now_ms: i64,
) -> Result<Value> {
    let caller_id = context.technical_requester_id();
    let request_id = model::text(request_value, "client_request_id")?;
    let original_json = model::canonical(request_value)?;
    let old: Option<ExistingAcceptanceOperation> = tx
        .query_row(
            "SELECT operation_id,method,original_request_json,state,result_json, \
                    effective_request_json,task_id,attempt_id \
             FROM operations WHERE caller_id=?1 AND client_request_id=?2",
            params![caller_id, request_id],
            |row| {
                Ok(ExistingAcceptanceOperation {
                    operation_id: row.get(0)?,
                    method: row.get(1)?,
                    original_request_json: row.get(2)?,
                    state: row.get(3)?,
                    result_json: row.get(4)?,
                    effective_request_json: row.get(5)?,
                    task_id: row.get(6)?,
                    attempt_id: row.get(7)?,
                })
            },
        )
        .optional()?;

    if let Some(ExistingAcceptanceOperation {
        operation_id,
        method,
        original_request_json,
        state,
        result_json,
        effective_request_json,
        task_id,
        attempt_id,
    }) = old
    {
        if method != "task.accept" || original_request_json != original_json {
            return Err(Error::new(
                "REQUEST_ID_CONFLICT",
                "this semantic acceptance request already retains different inputs",
            ));
        }
        let effective: Value = serde_json::from_str(&effective_request_json)?;
        if effective["automation_on_behalf"] != context.linkage_value()
            || task_id.as_deref() != Some(context.identity().task_id.as_str())
            || attempt_id.as_deref() != Some(context.identity().attempt_id.as_str())
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "existing semantic acceptance request is linked to a different exact scope",
            ));
        }
        if !matches!(
            state.as_str(),
            "queued" | "sending" | "outcome_unknown" | "settled" | "rejected"
        ) {
            return Err(Error::new(
                "AUTOMATION_OPERATION_CORRUPT",
                "existing semantic acceptance request has an unsupported state",
            ));
        }
        let current_result = result_json
            .map(|raw| serde_json::from_str::<Value>(&raw))
            .transpose()?
            .unwrap_or(Value::Null);
        let retained_receipt = effective["receipt"]["value"].clone();
        let mut receipt = if retained_receipt.is_object() {
            retained_receipt
        } else if current_result.is_object() {
            current_result.clone()
        } else {
            json!({})
        };
        receipt["operation_id"] = json!(operation_id);
        if receipt["acceptance_operation_id"].as_str().is_none() {
            receipt["acceptance_operation_id"] = json!(operation_id);
        }
        receipt["coalesced"] = json!(true);
        receipt["state"] = json!(state);
        receipt["task_accepted"] = json!(current_result["task_accepted"] == true);
        return Ok(receipt);
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

    let raw_effective: String = tx.query_row(
        "SELECT effective_request_json FROM operations WHERE operation_id=?1",
        [&operation_id],
        |row| row.get(0),
    )?;
    let mut effective: Value = serde_json::from_str(&raw_effective)?;
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
