//! Typed manager-on-behalf authority for an exact Task acceptance decision.

use super::{actions::AutomationStep, authorization, config};
use crate::store::gm::current as current_gm;
use crate::{
    acceptance::AcceptRequest,
    error::{Error, Result},
    model,
    review::{PRIMARY_REVIEW_SLOT, ReviewSlotIdentity},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;

/// Authority reconstructed only from a selected, enabled manager entry and a
/// committed pass for one exact current review slot. It never impersonates a
/// Manager Principal.
#[derive(Debug, Clone)]
pub(crate) struct AcceptanceContext {
    technical_requester_id: String,
    effective_manager_id: String,
    review_assignment_sponsor_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    gm_epoch: i64,
    expected_feedback_observation_id: i64,
    review_assignment_id: String,
    review_result_operation_id: String,
    identity: ReviewSlotIdentity,
    check_ids: Vec<String>,
}

pub(crate) struct AcceptanceReviewEvidence {
    pub(crate) identity: ReviewSlotIdentity,
    pub(crate) assignment_id: String,
    pub(crate) result_operation_id: String,
    pub(crate) assignment_sponsor_id: String,
}

struct StoredAcceptanceOperation {
    caller_id: String,
    method: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
}

struct RetainedAssignmentOperation {
    caller_id: String,
    method: String,
    state: String,
    result_json: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
}

struct RetainedAssignmentSourceObservation {
    source_stream_id: String,
    source_event_key: String,
    operation_id: String,
    kind: String,
    payload_json: String,
}

struct RetainedAssignmentSourceOperation {
    caller_id: String,
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
    result_json: String,
}

struct AssignmentAuthorityEvidence<'a> {
    operation_id: &'a str,
    sponsor_id: &'a str,
    identity: &'a ReviewSlotIdentity,
    assignment: &'a Value,
    result: &'a Value,
    original_request: &'a Value,
    effective_request: &'a Value,
    operation: &'a RetainedAssignmentOperation,
}

impl AcceptanceContext {
    pub(crate) fn from_committed_entry(
        db: &Connection,
        entry: &config::AutomationEntry,
        review: AcceptanceReviewEvidence,
        expected_feedback_observation_id: i64,
        check_ids: Vec<String>,
    ) -> Result<Self> {
        let AcceptanceReviewEvidence {
            identity,
            assignment_id: review_assignment_id,
            result_operation_id: review_result_operation_id,
            assignment_sponsor_id: review_assignment_sponsor_id,
        } = review;
        config::validate_entry(entry)?;
        validate_identity_text(&review_assignment_id, "review_assignment_id")?;
        validate_identity_text(&review_result_operation_id, "review_result_operation_id")?;
        validate_identity_text(
            &review_assignment_sponsor_id,
            "review_assignment_sponsor_id",
        )?;
        validate_check_ids(&check_ids)?;
        if !entry.enabled
            || !entry.steps.contains(&AutomationStep::Acceptance)
            || entry.scope.work_pool_id.is_some()
        {
            return Err(Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "entry does not currently admit exact acceptance",
            ));
        }
        if identity.task_revision <= 0
            || identity.review_slot != PRIMARY_REVIEW_SLOT
            || expected_feedback_observation_id < 0
        {
            return Err(Error::new(
                "REVIEW_RESULT_DAMAGED",
                "acceptance requires a valid current primary review slot",
            ));
        }

        let retained_sponsor =
            retained_review_assignment_sponsor(db, &review_assignment_id, &identity)?;
        if retained_sponsor != review_assignment_sponsor_id {
            return Err(Error::new(
                "REVIEW_ASSIGNMENT_DAMAGED",
                "retained assignment sponsor differs from the review evidence",
            ));
        }

        authorization::require_registered_manager(db, &entry.owner_manager_id)?;
        let gm = current_gm(db)?.ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "a current GM designation is required for acceptance",
            )
        })?;
        if gm.client_id != entry.owner_manager_id {
            return Err(Error::new(
                "FORBIDDEN",
                "the enabled acceptance entry owner is not the current GM",
            ));
        }

        let current = config::load_entry(
            db,
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )?
        .ok_or_else(|| Error::new("AUTOMATION_NOT_FOUND", "acceptance entry disappeared"))?;
        if !current.enabled
            || current.revision != entry.revision
            || !current.steps.contains(&AutomationStep::Acceptance)
            || current.scope.work_pool_id.is_some()
        {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current entry no longer selects exact acceptance",
            ));
        }
        let attempt_owner_id = retained_attempt_owner(&identity, db)?;
        if !assignment_sponsor_authorized(
            db,
            entry,
            &identity,
            &review_assignment_sponsor_id,
            &attempt_owner_id,
            gm.epoch,
        )? {
            return Err(Error::new(
                "FORBIDDEN",
                "assigned review sponsor is neither the Attempt owner nor a manager in the current transfer lineage",
            ));
        }

        Ok(Self {
            technical_requester_id: authorization::AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: entry.owner_manager_id.clone(),
            review_assignment_sponsor_id,
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            project_id: entry.project_id.clone(),
            gm_epoch: gm.epoch,
            expected_feedback_observation_id,
            review_assignment_id,
            review_result_operation_id,
            identity,
            check_ids: sorted_check_ids(check_ids),
        })
    }

    /// Rehydrate an on-behalf context only from a retained task.accept
    /// Operation. The saved linkage must match the live entry, current GM
    /// epoch, exact review slot and original request after restart.
    pub(crate) fn from_committed_operation(db: &Connection, operation_id: &str) -> Result<Self> {
        validate_identity_text(operation_id, "operation_id")?;
        let row: Option<StoredAcceptanceOperation> = db
            .query_row(
                "SELECT caller_id,method,task_id,attempt_id,original_request_json,effective_request_json \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| {
                    Ok(StoredAcceptanceOperation {
                        caller_id: row.get(0)?,
                        method: row.get(1)?,
                        task_id: row.get(2)?,
                        attempt_id: row.get(3)?,
                        original_request_json: row.get(4)?,
                        effective_request_json: row.get(5)?,
                    })
                },
            )
            .optional()?;
        let row =
            row.ok_or_else(|| Error::new("NOT_FOUND", "acceptance Operation was not retained"))?;
        let StoredAcceptanceOperation {
            caller_id,
            method,
            task_id,
            attempt_id,
            original_request_json,
            effective_request_json,
        } = row;
        if method != "task.accept" || caller_id != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        {
            return Err(Error::new(
                "FORBIDDEN",
                "Operation is not an on-behalf acceptance decision",
            ));
        }
        let original: Value = serde_json::from_str(&original_request_json)?;
        let effective: Value = serde_json::from_str(&effective_request_json)?;
        let saved_link = effective
            .get("automation_on_behalf")
            .filter(|value| value.is_object())
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "acceptance Operation has no retained manager linkage",
                )
            })?;
        let manager_id = model::text(saved_link, "effective_manager_id")?;
        let project_id = model::text(saved_link, "project_id")?;
        let automation_id = model::text(saved_link, "automation_id")?;
        let assignment_id = model::text(&saved_link["cause"], "review_assignment_id")?;
        let result_operation_id = model::text(&saved_link["cause"], "review_result_operation_id")?;
        let review_assignment_sponsor_id =
            model::text(&saved_link["cause"], "review_assignment_sponsor_id")?.to_owned();
        let identity: ReviewSlotIdentity =
            serde_json::from_value(saved_link["cause"]["identity"].clone()).map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "acceptance linkage has an invalid review-slot identity",
                )
            })?;
        let check_ids: Vec<String> =
            serde_json::from_value(saved_link.get("check_ids").cloned().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "acceptance linkage has no exact CheckRun set",
                )
            })?)
            .map_err(|_| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "acceptance linkage CheckRun set is malformed",
                )
            })?;
        validate_check_ids(&check_ids)?;
        let request = AcceptRequest::parse(&original)?;
        let entry = config::load_entry(db, manager_id, project_id, automation_id)?
            .ok_or_else(|| Error::new("FORBIDDEN", "acceptance automation was removed"))?;
        if model::text(saved_link, "technical_requester_id")?
            != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
            || model::text(saved_link, "action")? != "task.accept"
            || model::text(saved_link, "semantic_cause_kind")? != "review_result"
            || saved_link["schema_version"] != 1
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "acceptance linkage has an unsupported identity or action",
            ));
        }
        let saved_epoch = model::positive(saved_link, "gm_epoch")?;
        let context = Self::from_committed_entry(
            db,
            &entry,
            AcceptanceReviewEvidence {
                identity,
                assignment_id: assignment_id.to_owned(),
                result_operation_id: result_operation_id.to_owned(),
                assignment_sponsor_id: review_assignment_sponsor_id,
            },
            request.expected_feedback_observation_id,
            check_ids,
        )?;
        if context.gm_epoch != saved_epoch
            || &context.linkage_value() != saved_link
            || task_id.as_deref() != Some(context.identity.task_id.as_str())
            || attempt_id.as_deref() != Some(context.identity.attempt_id.as_str())
        {
            return Err(Error::new(
                "FORBIDDEN",
                "retained acceptance authority or exact Task scope has changed",
            ));
        }
        if request.expected_revision != context.identity.task_revision
            || request.attempt_id != context.identity.attempt_id
            || request.submission_ref != context.identity.submission_ref
            || request.candidate_ref != context.identity.candidate_ref
            || request.expected_feedback_observation_id != context.expected_feedback_observation_id
            || sorted_check_ids(request.check_ids) != context.check_ids
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "acceptance request differs from its retained exact review/check scope",
            ));
        }
        Ok(context)
    }

    pub(crate) fn require_current_action(&self, db: &Connection) -> Result<()> {
        authorization::require_registered_manager(db, &self.effective_manager_id)?;
        let gm = current_gm(db)?.ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "a current GM designation is required for acceptance",
            )
        })?;
        if gm.client_id != self.effective_manager_id || gm.epoch != self.gm_epoch {
            return Err(Error::new(
                "FORBIDDEN",
                "current GM authority or epoch differs from acceptance admission",
            ));
        }
        let current = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning acceptance entry was removed"))?;
        if !current.enabled
            || current.revision != self.automation_revision
            || !current.steps.contains(&AutomationStep::Acceptance)
            || current.scope.work_pool_id.is_some()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "current automation settings no longer permit acceptance",
            ));
        }
        let attempt_owner_id = retained_attempt_owner(&self.identity, db)?;
        if !assignment_sponsor_authorized(
            db,
            &current,
            &self.identity,
            &self.review_assignment_sponsor_id,
            &attempt_owner_id,
            gm.epoch,
        )? {
            return Err(Error::new(
                "FORBIDDEN",
                "assigned review sponsor is neither the Attempt owner nor a manager in the current transfer lineage",
            ));
        }
        Ok(())
    }

    // The separate arguments are the immutable anchors from the canonical
    // task.accept request. Keeping them explicit avoids accepting a partially
    // normalized request as the exact candidate/review/check scope.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_action_object(
        &self,
        db: &Connection,
        action: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
        submission_ref: &str,
        candidate_ref: &str,
        expected_feedback_observation_id: i64,
        check_ids: &[String],
    ) -> Result<()> {
        self.require_current_action(db)?;
        if action != "task.accept"
            || task_id != self.identity.task_id
            || task_revision != self.identity.task_revision
            || attempt_id != self.identity.attempt_id
            || submission_ref != self.identity.submission_ref
            || candidate_ref != self.identity.candidate_ref
            || expected_feedback_observation_id != self.expected_feedback_observation_id
            || sorted_check_ids(check_ids.to_vec()) != self.check_ids
        {
            return Err(Error::new(
                "FORBIDDEN",
                "acceptance action differs from its retained candidate, review slot or checks",
            ));
        }
        Ok(())
    }

    pub(crate) fn technical_requester_id(&self) -> &str {
        &self.technical_requester_id
    }

    pub(crate) fn effective_manager_id(&self) -> &str {
        &self.effective_manager_id
    }

    pub(crate) fn automation_id(&self) -> &str {
        &self.automation_id
    }

    pub(crate) fn automation_revision(&self) -> i64 {
        self.automation_revision
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn gm_epoch(&self) -> i64 {
        self.gm_epoch
    }

    pub(crate) fn expected_feedback_observation_id(&self) -> i64 {
        self.expected_feedback_observation_id
    }

    pub(crate) fn review_assignment_id(&self) -> &str {
        &self.review_assignment_id
    }

    pub(crate) fn review_result_operation_id(&self) -> &str {
        &self.review_result_operation_id
    }

    pub(crate) fn review_assignment_sponsor_id(&self) -> &str {
        &self.review_assignment_sponsor_id
    }

    pub(crate) fn identity(&self) -> &ReviewSlotIdentity {
        &self.identity
    }

    pub(crate) fn check_ids(&self) -> &[String] {
        &self.check_ids
    }

    pub(crate) fn cause_value(&self) -> Value {
        json!({
            "kind":"review_result",
            "review_assignment_id":self.review_assignment_id,
            "review_result_operation_id":self.review_result_operation_id,
            "review_assignment_sponsor_id":self.review_assignment_sponsor_id,
            "identity":self.identity,
            "check_ids":self.check_ids,
        })
    }

    /// Retained attribution is evidence of the real technical requester and
    /// effective GM; it is not a credential or a Principal replacement.
    pub(crate) fn linkage_value(&self) -> Value {
        json!({
            "schema_version":1,
            "technical_requester_id":self.technical_requester_id,
            "effective_manager_id":self.effective_manager_id,
            "automation_id":self.automation_id,
            "automation_revision":self.automation_revision,
            "project_id":self.project_id,
            "action":"task.accept",
            "semantic_cause_kind":"review_result",
            "semantic_cause_id":self.review_assignment_id,
            "gm_epoch":self.gm_epoch,
            "expected_feedback_observation_id":self.expected_feedback_observation_id,
            "cause":self.cause_value(),
            "check_ids":self.check_ids,
        })
    }

    /// Same manager and exact candidate/review/check evidence coalesce even
    /// when the selected automation entry or trigger delivery is retried.
    pub(crate) fn semantic_request_id(&self) -> Result<String> {
        let identity = model::canonical(&json!([
            self.effective_manager_id,
            self.review_assignment_sponsor_id,
            self.identity.task_id,
            self.identity.task_revision,
            self.identity.attempt_id,
            self.identity.submission_ref,
            self.identity.candidate_ref,
            self.identity.digest()?,
            self.expected_feedback_observation_id,
            self.check_ids,
        ]))?;
        Ok(model::digest(identity.as_bytes()))
    }
}

const REVIEW_STREAM: &str = "controller:review";

fn retained_attempt_owner(identity: &ReviewSlotIdentity, db: &Connection) -> Result<String> {
    let owner_id: Option<String> = db
        .query_row(
            "SELECT owner_id FROM attempts WHERE attempt_id=?1 AND task_id=?2 \
             AND task_revision=?3 AND submission_ref=?4 AND candidate_ref=?5 \
             AND released_at_ms IS NULL",
            params![
                identity.attempt_id,
                identity.task_id,
                identity.task_revision,
                identity.submission_ref,
                identity.candidate_ref,
            ],
            |row| row.get(0),
        )
        .optional()?;
    owner_id.ok_or_else(|| {
        Error::new(
            "AUTOMATION_ATTEMPT_STALE",
            "acceptance review no longer identifies the exact unreleased Attempt",
        )
    })
}

/// Keep legacy owner-sponsored acceptance unchanged. A review sponsored by a
/// different manager is accepted only when that exact retained sponsor is in
/// the validated transfer lineage ending at the current successor GM.
pub(crate) fn assignment_sponsor_authorized(
    db: &Connection,
    entry: &config::AutomationEntry,
    identity: &ReviewSlotIdentity,
    assignment_sponsor_id: &str,
    attempt_owner_id: &str,
    current_gm_epoch: i64,
) -> Result<bool> {
    if assignment_sponsor_id == attempt_owner_id {
        return Ok(true);
    }
    let authority = match authorization::current_transferred_attempt_authority(
        db,
        entry,
        AutomationStep::Acceptance,
        &identity.task_id,
        identity.task_revision,
        &identity.attempt_id,
        &identity.submission_ref,
        &identity.candidate_ref,
    ) {
        Ok(Some(authority)) => authority,
        Ok(None) => return Ok(false),
        Err(error)
            if matches!(
                error.code.as_str(),
                "FORBIDDEN"
                    | "AUTOMATION_ACTION_CHANGED"
                    | "AUTOMATION_ATTEMPT_STALE"
                    | "AUTOMATION_TRANSFER_CORRUPT"
            ) =>
        {
            return Ok(false);
        }
        Err(error) => return Err(error),
    };
    Ok(authority.source_attempt_owner_id() == attempt_owner_id
        && authority.successor_manager_id() == entry.owner_manager_id.as_str()
        && authority.current_gm_epoch() == current_gm_epoch
        && authority.contains_manager_id(attempt_owner_id)
        && authority.contains_manager_id(assignment_sponsor_id)
        && authority.contains_manager_id(entry.owner_manager_id.as_str())
        && !authority.transfer_operation_ids().is_empty())
}

fn retained_review_assignment_sponsor(
    db: &Connection,
    review_assignment_id: &str,
    identity: &ReviewSlotIdentity,
) -> Result<String> {
    let assignment_key = format!("assignment:{review_assignment_id}");
    let row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.assignment'",
            rusqlite::params![REVIEW_STREAM, assignment_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let (payload, operation_id) = row.ok_or_else(|| {
        Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "acceptance linkage has no retained review assignment",
        )
    })?;
    let assignment: Value = serde_json::from_str(&payload).map_err(|_| {
        Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "retained review assignment is invalid",
        )
    })?;
    let expected_identity = serde_json::to_value(identity)?;
    let sponsor_id = model::text(&assignment, "sponsor_client_id")?.to_owned();
    if assignment["review_assignment_id"] != review_assignment_id
        || assignment["operation_id"] != operation_id
        || assignment["identity"] != expected_identity
    {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "retained review assignment differs from its exact acceptance scope",
        ));
    }

    let operation: Option<RetainedAssignmentOperation> = db
        .query_row(
            "SELECT caller_id,method,state,result_json,task_id,attempt_id,\
                    original_request_json,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [&operation_id],
            |row| {
                Ok(RetainedAssignmentOperation {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    state: row.get(2)?,
                    result_json: row.get(3)?,
                    task_id: row.get(4)?,
                    attempt_id: row.get(5)?,
                    original_request_json: row.get(6)?,
                    effective_request_json: row.get(7)?,
                })
            },
        )
        .optional()?;
    let Some(operation) = operation else {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "retained review assignment has no settled Operation",
        ));
    };
    let result: Value = serde_json::from_str(&operation.result_json).map_err(|_| {
        Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "retained review assignment Operation result is invalid",
        )
    })?;
    let original_request: Value =
        serde_json::from_str(&operation.original_request_json).map_err(|_| {
            Error::new(
                "REVIEW_ASSIGNMENT_DAMAGED",
                "retained review assignment request is invalid",
            )
        })?;
    let effective_request: Value = serde_json::from_str(&operation.effective_request_json)
        .map_err(|_| {
            Error::new(
                "REVIEW_ASSIGNMENT_DAMAGED",
                "retained review assignment attribution is invalid",
            )
        })?;
    if operation.method != "review.assign"
        || operation.state != "settled"
        || operation.task_id.as_deref() != Some(identity.task_id.as_str())
        || operation.attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || original_request["attempt_id"] != identity.attempt_id
        || original_request["expected_revision"] != identity.task_revision
        || original_request["submission_ref"] != identity.submission_ref
        || original_request["candidate_ref"] != identity.candidate_ref
        || result["review_assignment_id"] != review_assignment_id
        || result["operation_id"] != operation_id
        || result["identity"] != expected_identity
        || result["sponsor_client_id"] != sponsor_id
        || result["reviewer_client_id"] != assignment["reviewer_client_id"]
        || assignment["technical_requester_id"] != operation.caller_id
        || assignment["result"] != result
        || result["technical_requester_id"] != operation.caller_id
        || effective_request["review_assignment"] != result
    {
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "review assignment and settled Operation differ from the exact Task and Attempt scope",
        ));
    }
    validate_assignment_operation_authority(
        db,
        AssignmentAuthorityEvidence {
            operation_id: &operation_id,
            sponsor_id: &sponsor_id,
            identity,
            assignment: &assignment,
            result: &result,
            original_request: &original_request,
            effective_request: &effective_request,
            operation: &operation,
        },
    )?;
    Ok(sponsor_id)
}

fn validate_assignment_operation_authority(
    db: &Connection,
    evidence: AssignmentAuthorityEvidence<'_>,
) -> Result<()> {
    let AssignmentAuthorityEvidence {
        operation_id,
        sponsor_id,
        identity,
        assignment,
        result,
        original_request,
        effective_request,
        operation,
    } = evidence;
    let damaged = || {
        Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "retained review assignment sponsor has no exact direct or manager-on-behalf authority",
        )
    };
    if operation.caller_id == sponsor_id {
        if !effective_request["on_behalf"].is_null()
            || authorization::operation_link(db, operation_id)?.is_some()
            || assignment["on_behalf"] != Value::Null
            || result["on_behalf"] != Value::Null
        {
            return Err(damaged());
        }
        return Ok(());
    }
    if operation.caller_id != authorization::AUTOMATION_TECHNICAL_REQUESTER_ID {
        return Err(damaged());
    }
    let link = authorization::operation_link(db, operation_id)?.ok_or_else(damaged)?;
    let saved = effective_request
        .get("on_behalf")
        .filter(|value| value.is_object())
        .ok_or_else(damaged)?;
    let task_project_id: Option<String> = db
        .query_row(
            "SELECT project_id FROM tasks WHERE task_id=?1",
            [&identity.task_id],
            |row| row.get(0),
        )
        .optional()?;
    let Some(task_project_id) = task_project_id else {
        return Err(damaged());
    };
    if link.operation_id != operation_id
        || link.action != "review.assign"
        || link.technical_requester_id != operation.caller_id
        || link.effective_manager_id != sponsor_id
        || link.project_id != task_project_id
        || link.cause["kind"] != "applied_submission"
        || link.cause["id"] != identity.submission_ref
        || saved["technical_requester_id"] != link.technical_requester_id
        || saved["effective_manager_id"] != link.effective_manager_id
        || saved["automation_id"] != link.automation_id
        || saved["automation_revision"] != link.automation_revision
        || saved["project_id"] != link.project_id
        || saved["action"] != link.action
        || saved["semantic_cause_kind"] != link.cause["kind"]
        || saved["semantic_cause_id"] != link.cause["id"]
        || saved["cause"] != link.cause
        || saved["review_profile"] != assignment["review_profile"]
        || !original_request["reviewer_client_id"].is_null()
        || original_request["review_profile"] != assignment["review_profile"]
        || !original_request["replaces_review_assignment_id"].is_null()
        || assignment["on_behalf"] != *saved
        || result["on_behalf"] != *saved
    {
        return Err(damaged());
    }
    validate_assignment_submission_cause(db, &link, identity)
}

fn validate_assignment_submission_cause(
    db: &Connection,
    link: &authorization::OnBehalfOperationLink,
    identity: &ReviewSlotIdentity,
) -> Result<()> {
    let damaged = || {
        Error::new(
            "REVIEW_ASSIGNMENT_DAMAGED",
            "on-behalf review assignment is detached from its applied submission source",
        )
    };
    let observation_id = link.cause["observation_id"]
        .as_i64()
        .filter(|id| *id > 0)
        .ok_or_else(damaged)?;
    let source_operation_id = link.cause["operation_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(damaged)?;
    let source_observation: Option<RetainedAssignmentSourceObservation> = db
        .query_row(
            "SELECT source_stream_id,source_event_key,operation_id,kind,payload_json \
             FROM observations WHERE observation_id=?1",
            [observation_id],
            |row| {
                Ok(RetainedAssignmentSourceObservation {
                    source_stream_id: row.get(0)?,
                    source_event_key: row.get(1)?,
                    operation_id: row.get(2)?,
                    kind: row.get(3)?,
                    payload_json: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(source_observation) = source_observation else {
        return Err(damaged());
    };
    let expected_event_key = format!("submission:{source_operation_id}");
    let source_payload: Value =
        serde_json::from_str(&source_observation.payload_json).map_err(|_| damaged())?;
    if source_observation.source_stream_id != "controller"
        || source_observation.source_event_key != expected_event_key
        || source_observation.operation_id != source_operation_id
        || source_observation.kind != "task.submission"
        || source_payload["outcome"] != "applied"
        || source_payload["operation_id"] != source_operation_id
        || source_payload["submission_ref"] != identity.submission_ref
        || source_payload["attempt_id"] != identity.attempt_id
        || source_payload["candidate_ref"] != identity.candidate_ref
    {
        return Err(damaged());
    }
    let source_operation: Option<RetainedAssignmentSourceOperation> = db
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,original_request_json,\
                    effective_request_json,result_json FROM operations WHERE operation_id=?1",
            [source_operation_id],
            |row| {
                Ok(RetainedAssignmentSourceOperation {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    state: row.get(2)?,
                    task_id: row.get(3)?,
                    attempt_id: row.get(4)?,
                    original_request_json: row.get(5)?,
                    effective_request_json: row.get(6)?,
                    result_json: row.get(7)?,
                })
            },
        )
        .optional()?;
    let Some(source_operation) = source_operation else {
        return Err(damaged());
    };
    let source_request: Value =
        serde_json::from_str(&source_operation.original_request_json).map_err(|_| damaged())?;
    let source_effective: Value =
        serde_json::from_str(&source_operation.effective_request_json).map_err(|_| damaged())?;
    let source_result: Value =
        serde_json::from_str(&source_operation.result_json).map_err(|_| damaged())?;
    let document = &source_effective["submission_document"];
    if source_operation.method != "task.submit"
        || source_operation.state != "settled"
        || source_operation.task_id.as_deref() != Some(identity.task_id.as_str())
        || source_operation.attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || source_request["attempt_id"] != identity.attempt_id
        || source_request["expected_revision"] != identity.task_revision
        || source_request["candidate_ref"] != identity.candidate_ref
        || source_result != source_payload
        || source_result["outcome"] != "applied"
        || source_result["submission_ref"] != identity.submission_ref
        || document["operation_id"] != source_operation_id
        || document["task_id"] != identity.task_id
        || document["attempt_id"] != identity.attempt_id
        || document["task_revision"] != identity.task_revision
        || document["candidate_ref"] != identity.candidate_ref
        || document["submitted_by"] != source_operation.caller_id
    {
        return Err(damaged());
    }
    Ok(())
}

fn sorted_check_ids(mut check_ids: Vec<String>) -> Vec<String> {
    check_ids.sort();
    check_ids
}

fn validate_check_ids(check_ids: &[String]) -> Result<()> {
    let mut unique = BTreeSet::new();
    if check_ids
        .iter()
        .any(|id| id.trim().is_empty() || id.len() > 128 || !unique.insert(id.as_str()))
    {
        return Err(Error::invalid(
            "acceptance CheckRun IDs must be unique bounded nonempty text",
        ));
    }
    Ok(())
}

fn validate_identity_text(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
    {
        return Err(Error::invalid(format!("{field} is invalid")));
    }
    Ok(())
}
