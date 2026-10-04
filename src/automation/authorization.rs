//! Internal manager-on-behalf identity. This is constructed from verified
//! Store state and intentionally has no public deserializer or constructor.

use super::{
    actions::{AutomationCause, AutomationStep},
    config,
};
use crate::{
    acceptance::AcceptRequest,
    error::{Error, Result},
    model::{self, Principal, Role},
    review::{
        PRIMARY_REVIEW_SLOT, ReviewCoverage, ReviewSlotIdentity, ReviewSubmitRequest, ReviewVerdict,
    },
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

pub(crate) const AUTOMATION_TECHNICAL_REQUESTER_ID: &str = "eliot-internal-automation-v1";

#[derive(Debug, Clone)]
pub(crate) struct ManagerExecutionContext {
    technical_requester_id: String,
    effective_manager_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    review_profile: Option<String>,
    cause: AutomationCause,
}

struct CurrentReviewSubject {
    owner_id: String,
    task_id: String,
    task_revision: i64,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    released_at_ms: Option<i64>,
    attempt_state: String,
    project_id: String,
    task_state: String,
    current_task_revision: i64,
    current_attempt_id: Option<String>,
}

struct RetainedAcceptanceAttempt {
    owner_id: String,
    task_id: String,
    task_revision: i64,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    project_id: String,
}

type AcceptanceOperationRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
);
type AcceptanceAssignmentOperationRow = (String, String, String, Option<String>, String);
type AcceptanceResultOperationRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    String,
);

impl ManagerExecutionContext {
    /// Created only from a currently stored, digest-verified enabled entry.
    /// There is no API or serde representation that can construct this from a
    /// request body.
    pub(crate) fn from_committed_entry(
        db: &Connection,
        entry: &config::AutomationEntry,
        cause: AutomationCause,
    ) -> Result<Self> {
        config::validate_entry(entry)?;
        if !entry.review_dispatch_ready() {
            return Err(Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "entry does not currently admit review_dispatch",
            ));
        }
        require_registered_manager(db, &entry.owner_manager_id)?;
        let current = config::load_entry(
            db,
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )?
        .ok_or_else(|| Error::new("AUTOMATION_NOT_FOUND", "automation entry disappeared"))?;
        if current.revision != entry.revision
            || !current.enabled
            || !current
                .steps
                .contains(&super::actions::AutomationStep::ReviewDispatch)
            || current.review.profile != entry.review.profile
            || cause.kind() != "applied_submission"
        {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current entry no longer admits this review action",
            ));
        }
        Ok(Self {
            technical_requester_id: AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: entry.owner_manager_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            project_id: entry.project_id.clone(),
            review_profile: entry.review.profile.clone(),
            cause,
        })
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

    pub(crate) fn semantic_cause_kind(&self) -> &str {
        self.cause.kind()
    }

    pub(crate) fn semantic_cause_id(&self) -> &str {
        self.cause.id()
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn review_profile(&self) -> Option<&str> {
        self.review_profile.as_deref()
    }

    pub(crate) fn cause_value(&self) -> Value {
        self.cause.as_json()
    }

    /// The exact on-behalf attribution retained with an Operation. This is
    /// evidence, not a credential and not a Principal substitute.
    pub(crate) fn linkage_value(&self) -> Value {
        json!({
            "technical_requester_id":self.technical_requester_id,
            "effective_manager_id":self.effective_manager_id,
            "automation_id":self.automation_id,
            "automation_revision":self.automation_revision(),
            "project_id":self.project_id,
            "action":"review.assign",
            "semantic_cause_kind":self.semantic_cause_kind(),
            "semantic_cause_id":self.semantic_cause_id(),
            "cause":self.cause_value(),
            "review_profile":self.review_profile
        })
    }

    /// Rechecks current config, manager registration and the exact live
    /// attempt/submission scope immediately before a new review assignment.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn require_action_object_with_transfer(
        &self,
        db: &Connection,
        action: &str,
        task_id: &str,
        task_revision: i64,
        attempt_id: &str,
        project_id: &str,
        submission_ref: &str,
    ) -> Result<Option<TransferredAttemptAuthority>> {
        if action != "review.assign"
            || project_id != self.project_id
            || submission_ref != self.semantic_cause_id()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "on-behalf action does not match its retained project and submission cause",
            ));
        }
        require_registered_manager(db, &self.effective_manager_id)?;
        let current = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        if !current.enabled
            || current.revision != self.automation_revision
            || !current
                .steps
                .contains(&super::actions::AutomationStep::ReviewDispatch)
            || !current.review_dispatch_ready()
            || current.review.profile != self.review_profile
        {
            return Err(Error::new(
                "FORBIDDEN",
                "current automation settings no longer permit this review assignment",
            ));
        }
        let row: Option<CurrentReviewSubject> = db
            .query_row(
                "SELECT a.owner_id,a.task_id,a.task_revision,a.submission_ref,a.candidate_ref,\
                        a.released_at_ms,a.state,t.project_id,t.state,t.revision, \
                        (SELECT active.attempt_id FROM attempts AS active \
                         WHERE active.task_id=t.task_id AND active.released_at_ms IS NULL) \
                 FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id WHERE a.attempt_id=?1",
                [attempt_id],
                |r| {
                    Ok(CurrentReviewSubject {
                        owner_id: r.get(0)?,
                        task_id: r.get(1)?,
                        task_revision: r.get(2)?,
                        submission_ref: r.get(3)?,
                        candidate_ref: r.get(4)?,
                        released_at_ms: r.get(5)?,
                        attempt_state: r.get(6)?,
                        project_id: r.get(7)?,
                        task_state: r.get(8)?,
                        current_task_revision: r.get(9)?,
                        current_attempt_id: r.get(10)?,
                    })
                },
            )
            .optional()?;
        let Some(subject) = row else {
            return Err(Error::new("NOT_FOUND", "review attempt was not found"));
        };
        if subject.task_id != task_id
            || subject.task_revision != task_revision
            || subject.project_id != project_id
            || subject.released_at_ms.is_some()
            || subject.attempt_state != "submitted"
            || subject.task_state != "open"
            || subject.current_task_revision != subject.task_revision
            || subject.current_attempt_id.as_deref() != Some(attempt_id)
            || subject.submission_ref.as_deref() != Some(submission_ref)
            || subject.candidate_ref.as_deref().is_none_or(str::is_empty)
        {
            return Err(Error::new(
                "FORBIDDEN",
                "manager action no longer targets this exact current review subject",
            ));
        }
        if subject.owner_id == self.effective_manager_id {
            return Ok(None);
        }
        let candidate_ref = subject.candidate_ref.as_deref().ok_or_else(|| {
            Error::new(
                "AUTOMATION_ATTEMPT_STALE",
                "current review Attempt has no retained candidate",
            )
        })?;
        let authority = current_transferred_attempt_authority(
            db,
            &current,
            AutomationStep::ReviewDispatch,
            task_id,
            subject.task_revision,
            attempt_id,
            submission_ref,
            candidate_ref,
        )?
        .ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "automation sponsor does not own or inherit authority for this review Attempt",
            )
        })?;
        if authority.source_attempt_owner_id() != subject.owner_id
            || authority.successor_manager_id() != self.effective_manager_id
        {
            return Err(Error::new(
                "FORBIDDEN",
                "transferred review authority differs from the exact Attempt owner and sponsor",
            ));
        }
        Ok(Some(authority))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct OnBehalfOperationLink {
    pub(crate) schema_version: u32,
    pub(crate) operation_id: String,
    pub(crate) technical_requester_id: String,
    pub(crate) effective_manager_id: String,
    pub(crate) automation_id: String,
    pub(crate) automation_revision: i64,
    pub(crate) project_id: String,
    pub(crate) action: String,
    pub(crate) cause: Value,
    pub(crate) linked_at_ms: i64,
}

/// A validated retained attribution for an Operation admitted by automation.
/// Review, Acceptance, Publication, WorkDispatch and Repair links keep
/// separate provenance contracts; this enum is only their shared visibility
/// boundary.
pub(crate) enum AnyOnBehalfOperationLink {
    Review(OnBehalfOperationLink),
    Acceptance(OnBehalfOperationLink),
    Publication(OnBehalfOperationLink),
    WorkDispatch(crate::store::automation_work_dispatch::WorkDispatchOperationLink),
    Repair(Box<crate::store::automation_repair::RepairDispatchOperationLink>),
}

/// Current execution authority derived from an immutable old operation and a
/// committed automation ownership-transfer chain. The historical attribution
/// stays on the operation; this value is only a separately checked grant for
/// a still-queued effect that has not started.
#[derive(Debug, Clone)]
pub(crate) struct TransferContinuation {
    historical_owner_id: String,
    historical_revision: i64,
    current_owner_id: String,
    current_gm_epoch: i64,
    project_id: String,
    automation_id: String,
    action: String,
    task_id: String,
    transfer_operation_ids: Vec<String>,
    current_entry: config::AutomationEntry,
}

/// Read-only authority to reconcile one exact unknown WorkDispatch workspace
/// effect after its automation has transferred. This is intentionally a
/// different capability from `TransferContinuation`, which only admits a
/// queued, unsent effect under the successor's current settings.
#[derive(Debug, Clone)]
pub(crate) struct TransferReadbackAuthority {
    operation_id: String,
    project_id: String,
    task_id: String,
}

/// Current-GM authority for one exact live Attempt whose owner is in the
/// committed transfer lineage of the current automation entry.
#[derive(Debug, Clone)]
pub(crate) struct TransferredAttemptAuthority {
    source_attempt_owner_id: String,
    successor_manager_id: String,
    current_gm_epoch: i64,
    owner_lineage: Vec<String>,
    transfer_operation_ids: Vec<String>,
}

impl TransferReadbackAuthority {
    pub(crate) fn matches_work_dispatch(
        &self,
        operation_id: &str,
        project_id: &str,
        task_id: &str,
    ) -> bool {
        self.operation_id == operation_id
            && self.project_id == project_id
            && self.task_id == task_id
    }
}

impl TransferredAttemptAuthority {
    pub(crate) fn source_attempt_owner_id(&self) -> &str {
        &self.source_attempt_owner_id
    }

    pub(crate) fn successor_manager_id(&self) -> &str {
        &self.successor_manager_id
    }

    pub(crate) fn current_gm_epoch(&self) -> i64 {
        self.current_gm_epoch
    }

    pub(crate) fn transfer_operation_ids(&self) -> &[String] {
        &self.transfer_operation_ids
    }

    pub(crate) fn contains_manager_id(&self, manager_id: &str) -> bool {
        self.owner_lineage.iter().any(|owner| owner == manager_id)
    }
}

impl TransferContinuation {
    pub(crate) fn historical_owner_id(&self) -> &str {
        &self.historical_owner_id
    }

    pub(crate) fn historical_revision(&self) -> i64 {
        self.historical_revision
    }

    pub(crate) fn current_owner_id(&self) -> &str {
        &self.current_owner_id
    }

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn automation_id(&self) -> &str {
        &self.automation_id
    }

    pub(crate) fn action(&self) -> &str {
        &self.action
    }

    pub(crate) fn task_id(&self) -> &str {
        &self.task_id
    }

    pub(crate) fn current_entry(&self) -> &config::AutomationEntry {
        &self.current_entry
    }

    /// Revalidate this already-created queued grant at Forge's exact
    /// pre-write boundary. This cannot construct a grant from a sending,
    /// unknown, or otherwise unadmitted Operation.
    pub(crate) fn revalidate_publication_prewrite(
        &self,
        db: &Connection,
        operation_id: &str,
    ) -> Result<Self> {
        let current = current_transfer_continuation_at_phase(
            db,
            operation_id,
            "forge.publish_ref",
            AutomationStep::Publication,
            &self.task_id,
            TransferContinuationPhase::PublicationPreWrite,
        )?
        .ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "the queued transfer grant no longer has an active transfer lineage",
            )
        })?;
        if current.historical_owner_id != self.historical_owner_id
            || current.historical_revision != self.historical_revision
            || current.current_owner_id != self.current_owner_id
            || current.current_gm_epoch != self.current_gm_epoch
            || current.project_id != self.project_id
            || current.automation_id != self.automation_id
            || current.action != self.action
            || current.task_id != self.task_id
            || current.transfer_operation_ids != self.transfer_operation_ids
        {
            return Err(Error::new(
                "FORBIDDEN",
                "GM epoch, transfer lineage, or exact Operation scope changed before publication",
            ));
        }
        Ok(current)
    }
}

impl AnyOnBehalfOperationLink {
    pub(crate) fn belongs_to(&self, principal: &Principal) -> bool {
        match self {
            Self::Review(link) => link.belongs_to(principal),
            Self::Acceptance(link) => link.belongs_to(principal),
            Self::Publication(link) => link.belongs_to(principal),
            Self::WorkDispatch(link) => link.belongs_to(principal),
            Self::Repair(link) => {
                principal.role == Role::Manager && principal.client_id == link.effective_manager_id
            }
        }
    }
}

impl OnBehalfOperationLink {
    pub(crate) fn belongs_to(&self, principal: &Principal) -> bool {
        principal.role == Role::Manager && principal.client_id == self.effective_manager_id
    }

    pub(crate) fn value(&self) -> Result<Value> {
        serde_json::to_value(self).map_err(Into::into)
    }
}

pub(crate) fn operation_link(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<OnBehalfOperationLink>> {
    let key = config::operation_link_key(operation_id)?;
    let Some(value) = config::read_record(db, &key, "on-behalf operation link")? else {
        return Ok(None);
    };
    let link: OnBehalfOperationLink = serde_json::from_value(value).map_err(|_| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "on-behalf operation link fields are invalid",
        )
    })?;
    let expected_method = match (link.action.as_str(), link.cause["kind"].as_str()) {
        ("review.assign", Some("applied_submission")) => "review.assign",
        ("task.request_changes", Some("review_result")) => "task.request_changes",
        ("task.accept", Some("review_result")) => "task.accept",
        ("forge.publish_ref", Some("task.acceptance")) => "forge.publish_ref",
        _ => "",
    };
    if link.schema_version != 1
        || link.operation_id != operation_id
        || link.technical_requester_id != AUTOMATION_TECHNICAL_REQUESTER_ID
        || expected_method.is_empty()
        || link.automation_revision <= 0
        || link.effective_manager_id.is_empty()
        || link.project_id.is_empty()
        || link.automation_id.is_empty()
        || link.cause["id"].as_str().is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "on-behalf operation link identity is invalid",
        ));
    }
    let operation: Option<(String, String)> = db
        .query_row(
            "SELECT caller_id,method FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if !operation.is_some_and(|(caller, method)| {
        caller == link.technical_requester_id && method == expected_method
    }) {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "linked Operation was not admitted by the recorded technical requester",
        ));
    }
    if link.action == "task.request_changes" {
        validate_review_disposition_link(db, &link)?;
    } else if link.action == "task.accept" {
        validate_acceptance_link(db, &link)?;
    } else if link.action == "forge.publish_ref" {
        validate_publication_link(db, &link)?;
    }
    Ok(Some(link))
}

fn validate_acceptance_link(db: &Connection, link: &OnBehalfOperationLink) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "acceptance link does not match its retained request and assigned review pass",
        )
    };
    let assignment_id = link.cause["review_assignment_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let result_operation_id = link.cause["review_result_operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let review_assignment_sponsor_id = link.cause["review_assignment_sponsor_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(corrupt)?;
    let identity: ReviewSlotIdentity =
        serde_json::from_value(link.cause["identity"].clone()).map_err(|_| corrupt())?;
    if identity.task_revision <= 0
        || identity.review_slot != PRIMARY_REVIEW_SLOT
        || identity.task_id.trim().is_empty()
        || identity.attempt_id.trim().is_empty()
        || identity.submission_ref.trim().is_empty()
        || identity.candidate_ref.trim().is_empty()
        || identity.review_policy_generation.trim().is_empty()
        || link.linked_at_ms < 0
        || link.cause["kind"] != "review_result"
        || link.cause["id"] != assignment_id
    {
        return Err(corrupt());
    }
    let identity_value = serde_json::to_value(&identity).map_err(|_| corrupt())?;

    let operation: Option<AcceptanceOperationRow> = db
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,original_request_json,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [&link.operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((caller, method, task_id, attempt_id, original_raw, effective_raw)) = operation else {
        return Err(corrupt());
    };
    let original: Value = serde_json::from_str(&original_raw).map_err(|_| corrupt())?;
    let request = AcceptRequest::parse(&original).map_err(|_| corrupt())?;
    let effective: Value = serde_json::from_str(&effective_raw).map_err(|_| corrupt())?;
    let saved_link = effective
        .get("automation_on_behalf")
        .filter(|value| value.is_object())
        .ok_or_else(corrupt)?;
    let gm_epoch = saved_link["gm_epoch"]
        .as_i64()
        .filter(|epoch| *epoch > 0)
        .ok_or_else(corrupt)?;
    let assignment_cause = json!({
        "kind":"review_result",
        "review_assignment_id":assignment_id,
        "review_result_operation_id":result_operation_id,
        "review_assignment_sponsor_id":review_assignment_sponsor_id,
        "identity":identity_value,
        "check_ids":request.check_ids
    });
    let mut linked_cause = assignment_cause.clone();
    linked_cause["id"] = json!(assignment_id);
    let mut sorted_check_ids = request.check_ids.clone();
    sorted_check_ids.sort();
    let semantic_input = model::canonical(&json!([
        link.effective_manager_id,
        review_assignment_sponsor_id,
        identity.task_id,
        identity.task_revision,
        identity.attempt_id,
        identity.submission_ref,
        identity.candidate_ref,
        identity.digest().map_err(|_| corrupt())?,
        request.expected_feedback_observation_id,
        request.check_ids,
    ]))
    .map_err(|_| corrupt())?;
    let semantic_request_id = model::digest(semantic_input.as_bytes());
    let expected_reason = format!(
        "Assigned review {assignment_id} recorded a complete pass for candidate {} in review.submit Operation {result_operation_id}.",
        identity.candidate_ref
    );
    if request.check_ids != sorted_check_ids
        || link.cause != linked_cause
        || caller != AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "task.accept"
        || task_id.as_deref() != Some(identity.task_id.as_str())
        || attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || request.expected_revision != identity.task_revision
        || request.attempt_id != identity.attempt_id
        || request.submission_ref != identity.submission_ref
        || request.candidate_ref != identity.candidate_ref
        || request.client_request_id != semantic_request_id
        || request.reason != expected_reason
        || request.expected_feedback_observation_id
            != saved_link["expected_feedback_observation_id"]
        || effective["request"] != original
    {
        return Err(corrupt());
    }

    let expected_saved_link = json!({
        "schema_version":1,
        "technical_requester_id":link.technical_requester_id,
        "effective_manager_id":link.effective_manager_id,
        "automation_id":link.automation_id,
        "automation_revision":link.automation_revision,
        "project_id":link.project_id,
        "action":"task.accept",
        "semantic_cause_kind":"review_result",
        "semantic_cause_id":assignment_id,
        "gm_epoch":gm_epoch,
        "expected_feedback_observation_id":request.expected_feedback_observation_id,
        "cause":assignment_cause,
        "check_ids":request.check_ids
    });
    if saved_link != &expected_saved_link {
        return Err(corrupt());
    }

    validate_acceptance_review_pass(
        db,
        link,
        &identity,
        assignment_id,
        result_operation_id,
        &request,
    )
}

fn validate_acceptance_review_pass(
    db: &Connection,
    link: &OnBehalfOperationLink,
    identity: &ReviewSlotIdentity,
    assignment_id: &str,
    result_operation_id: &str,
    acceptance_request: &AcceptRequest,
) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "acceptance link does not match its retained request and assigned review pass",
        )
    };
    let sponsor_id = link.cause["review_assignment_sponsor_id"]
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(corrupt)?;
    let assignment_key = format!("assignment:{assignment_id}");
    let assignment_row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 \
               AND kind='review.assignment'",
            [&assignment_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((assignment_raw, assignment_operation_id)) = assignment_row else {
        return Err(corrupt());
    };
    let assignment: Value = serde_json::from_str(&assignment_raw).map_err(|_| corrupt())?;
    if assignment["schema_version"] != 1
        || assignment["review_assignment_id"] != assignment_id
        || assignment["operation_id"] != assignment_operation_id
        || assignment["identity"] != serde_json::to_value(identity).map_err(|_| corrupt())?
        || assignment["sponsor_client_id"] != sponsor_id
        || assignment["reviewer_client_id"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return Err(corrupt());
    }
    let reviewer_id = assignment["reviewer_client_id"]
        .as_str()
        .ok_or_else(corrupt)?;
    if reviewer_id == sponsor_id || reviewer_id == link.effective_manager_id {
        return Err(corrupt());
    }

    let retained_attempt: Option<RetainedAcceptanceAttempt> = db
        .query_row(
            "SELECT a.owner_id,a.task_id,a.task_revision,a.submission_ref,a.candidate_ref,t.project_id \
             FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id WHERE a.attempt_id=?1",
            [&identity.attempt_id],
            |row| {
                Ok(RetainedAcceptanceAttempt {
                    owner_id: row.get(0)?,
                    task_id: row.get(1)?,
                    task_revision: row.get(2)?,
                    submission_ref: row.get(3)?,
                    candidate_ref: row.get(4)?,
                    project_id: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(retained_attempt) = retained_attempt else {
        return Err(corrupt());
    };
    if retained_attempt.task_id != identity.task_id
        || retained_attempt.task_revision != identity.task_revision
        || retained_attempt.submission_ref.as_deref() != Some(identity.submission_ref.as_str())
        || retained_attempt.candidate_ref.as_deref() != Some(identity.candidate_ref.as_str())
        || retained_attempt.project_id != link.project_id
    {
        return Err(corrupt());
    }

    let assignment_operation: Option<AcceptanceAssignmentOperationRow> = db
        .query_row(
            "SELECT caller_id,method,state,result_json,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [&assignment_operation_id],
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
    let Some((assignment_caller, method, state, result_raw, assignment_effective_raw)) =
        assignment_operation
    else {
        return Err(corrupt());
    };
    let assignment_result: Value =
        serde_json::from_str(&result_raw.ok_or_else(corrupt)?).map_err(|_| corrupt())?;
    let assignment_effective: Value =
        serde_json::from_str(&assignment_effective_raw).map_err(|_| corrupt())?;
    let (operation_sponsor_id, assignment_manager_id) = match assignment_effective.get("on_behalf")
    {
        None | Some(Value::Null) => {
            if operation_link(db, &assignment_operation_id)?.is_some() {
                return Err(corrupt());
            }
            (assignment_caller.clone(), None)
        }
        Some(saved) if saved.is_object() => {
            let Some(operation_link) = operation_link(db, &assignment_operation_id)? else {
                return Err(corrupt());
            };
            if operation_link.action != "review.assign"
                || saved["technical_requester_id"] != operation_link.technical_requester_id
                || saved["effective_manager_id"] != operation_link.effective_manager_id
                || saved["automation_id"] != operation_link.automation_id
                || saved["automation_revision"] != operation_link.automation_revision
                || saved["project_id"] != operation_link.project_id
                || saved["action"] != operation_link.action
                || operation_link.project_id != link.project_id
                || operation_link.automation_id != link.automation_id
                || saved["semantic_cause_kind"] != operation_link.cause["kind"]
                || saved["semantic_cause_id"] != operation_link.cause["id"]
                || saved["cause"] != operation_link.cause
                || operation_link.cause["id"] != identity.submission_ref
            {
                return Err(corrupt());
            }
            (
                operation_link.effective_manager_id.clone(),
                Some(operation_link.effective_manager_id),
            )
        }
        Some(_) => return Err(corrupt()),
    };
    if method != "review.assign"
        || state != "settled"
        || assignment["technical_requester_id"] != assignment_caller
        || assignment_result != assignment["result"]
        || assignment_result["review_assignment_id"] != assignment_id
        || assignment_result["identity"] != assignment["identity"]
        || assignment_result["technical_requester_id"] != assignment_caller
        || assignment_result["sponsor_client_id"] != sponsor_id
        || assignment_result["reviewer_client_id"] != assignment["reviewer_client_id"]
        || operation_sponsor_id != sponsor_id
    {
        return Err(corrupt());
    }

    // A retained acceptance can outlive another transfer (B -> C). Validate
    // the immutable owner chain and historical sponsor/decision manager
    // against its sealed edges without requiring either actor to remain
    // today's GM or the accepted Task to stay open.
    if sponsor_id != retained_attempt.owner_id {
        let lineage = validated_transfer_owner_lineage(
            db,
            &retained_attempt.owner_id,
            &retained_attempt.project_id,
            &link.automation_id,
        )?;
        if !lineage.iter().any(|owner| owner == sponsor_id)
            || !lineage
                .iter()
                .any(|owner| owner == &link.effective_manager_id)
            || (sponsor_id != retained_attempt.owner_id
                && assignment_manager_id.as_deref() != Some(sponsor_id))
        {
            return Err(corrupt());
        }
    }

    let result_key = format!("result:{assignment_id}");
    let result_row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 \
               AND kind='review.result'",
            [&result_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((result_raw, observed_operation_id)) = result_row else {
        return Err(corrupt());
    };
    let record: Value = serde_json::from_str(&result_raw).map_err(|_| corrupt())?;
    if observed_operation_id != result_operation_id
        || record["schema_version"] != 1
        || record["operation_id"] != result_operation_id
        || record["review_assignment_id"] != assignment_id
        || record["identity"] != assignment["identity"]
    {
        return Err(corrupt());
    }

    let result_operation: Option<AcceptanceResultOperationRow> = db
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,result_json \
             FROM operations WHERE operation_id=?1",
            [result_operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((reviewer_id, method, state, task_id, attempt_id, result_json)) = result_operation
    else {
        return Err(corrupt());
    };
    let result: Value = serde_json::from_str(&result_json).map_err(|_| corrupt())?;
    let submit_raw: String = db
        .query_row(
            "SELECT original_request_json FROM operations WHERE operation_id=?1",
            [result_operation_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(corrupt)?;
    let submit_value: Value = serde_json::from_str(&submit_raw).map_err(|_| corrupt())?;
    let submit = ReviewSubmitRequest::parse(&submit_value).map_err(|_| corrupt())?;
    let required_ids: Vec<String> =
        serde_json::from_value(assignment["required_coverage"]["requirement_ids"].clone())
            .map_err(|_| corrupt())?;
    let required_ids = required_ids.into_iter().collect::<BTreeSet<_>>();
    let submitted_ids = submit
        .requirement_reviews
        .iter()
        .map(|review| review.requirement_id.clone())
        .collect::<BTreeSet<_>>();
    let accepted_reviews =
        serde_json::to_value(&acceptance_request.reviews).map_err(|_| corrupt())?;
    let submitted_reviews =
        serde_json::to_value(&submit.requirement_reviews).map_err(|_| corrupt())?;
    if reviewer_id != assignment["reviewer_client_id"]
        || method != "review.submit"
        || state != "settled"
        || task_id.as_deref() != Some(identity.task_id.as_str())
        || attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || result != record["result"]
        || result["review_assignment_id"] != assignment_id
        || result["reviewer_client_id"] != assignment["reviewer_client_id"]
        || result["sponsor_client_id"] != sponsor_id
        || result["task_id"] != identity.task_id
        || result["attempt_id"] != identity.attempt_id
        || result["task_revision"] != identity.task_revision
        || result["submission_ref"] != identity.submission_ref
        || result["candidate_ref"] != identity.candidate_ref
        || result["verdict"] != "pass"
        || result["coverage"] != "complete"
        || result["findings"] != json!([])
        || result["applicability"] != "current_candidate"
        || result["task_transition"] != "none"
        || submit.review_assignment_id != assignment_id
        || submit.submission_ref != identity.submission_ref
        || submit.candidate_ref != identity.candidate_ref
        || submit.verdict != ReviewVerdict::Pass
        || submit.coverage != ReviewCoverage::Complete
        || !submit.findings.is_empty()
        || submit.requirement_reviews.is_empty()
        || required_ids.is_empty()
        || required_ids.len()
            != assignment["required_coverage"]["requirement_ids"]
                .as_array()
                .map_or(0, Vec::len)
        || submitted_ids != required_ids
        || submitted_reviews != result["requirement_reviews"]
        || accepted_reviews != submitted_reviews
    {
        return Err(corrupt());
    }
    Ok(())
}

struct PublicationOperationRow {
    caller_id: String,
    method: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
}

struct AcceptedSourceOperationRow {
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
    result_json: Option<String>,
}

struct AcceptanceObservationRow {
    observation_id: i64,
    operation_id: String,
    payload_json: String,
}

struct PublicationAttemptSubjectRow {
    task_id: String,
    task_revision: i64,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
}

fn validate_publication_link(db: &Connection, link: &OnBehalfOperationLink) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "publication link does not match its exact accepted candidate and Forge intent",
        )
    };
    let cause = &link.cause;
    model::fields(
        cause,
        &[
            "kind",
            "observation_id",
            "operation_id",
            "id",
            "task_id",
            "task_revision",
            "attempt_id",
            "submission_ref",
            "candidate_ref",
            "canonical_repository",
            "gm_epoch",
            "policy_revision",
            "target_ref",
            "expected_old_ref",
            "expected_create",
            "activation_cut",
            "historical_replay_authorized",
        ],
    )
    .map_err(|_| corrupt())?;
    let accepted_operation_id = cause["operation_id"].as_str().ok_or_else(corrupt)?;
    let observation_id = cause["observation_id"].as_i64().ok_or_else(corrupt)?;
    let activation_cut = cause["activation_cut"].as_i64().ok_or_else(corrupt)?;
    let historical_replay_authorized = cause["historical_replay_authorized"]
        .as_bool()
        .ok_or_else(corrupt)?;
    if link.action != "forge.publish_ref"
        || cause["kind"] != "task.acceptance"
        || accepted_operation_id.is_empty()
        || cause["id"] != accepted_operation_id
        || observation_id <= 0
        || activation_cut < 0
        || historical_replay_authorized != (observation_id <= activation_cut)
        || cause["gm_epoch"].as_i64().is_none_or(|epoch| epoch <= 0)
    {
        return Err(corrupt());
    }

    let accepted_key = format!("accept:{accepted_operation_id}");
    let observation: Option<AcceptanceObservationRow> = db
        .query_row(
            "SELECT observation_id,operation_id,payload_json FROM observations \
             WHERE source_stream_id='controller:acceptance' AND source_event_key=?1 \
               AND kind='task.acceptance'",
            [&accepted_key],
            |row| {
                Ok(AcceptanceObservationRow {
                    observation_id: row.get(0)?,
                    operation_id: row.get(1)?,
                    payload_json: row.get(2)?,
                })
            },
        )
        .optional()?;
    let Some(observation) = observation else {
        return Err(corrupt());
    };
    let observed_payload: Value =
        serde_json::from_str(&observation.payload_json).map_err(|_| corrupt())?;

    let accepted_operation: Option<AcceptedSourceOperationRow> = db
        .query_row(
            "SELECT method,state,task_id,attempt_id,original_request_json, \
                    effective_request_json,result_json \
             FROM operations WHERE operation_id=?1",
            [accepted_operation_id],
            |row| {
                Ok(AcceptedSourceOperationRow {
                    method: row.get(0)?,
                    state: row.get(1)?,
                    task_id: row.get(2)?,
                    attempt_id: row.get(3)?,
                    original_request_json: row.get(4)?,
                    effective_request_json: row.get(5)?,
                    result_json: row.get(6)?,
                })
            },
        )
        .optional()?;
    let Some(accepted_operation) = accepted_operation else {
        return Err(corrupt());
    };
    let accepted_result: Value =
        serde_json::from_str(&accepted_operation.result_json.ok_or_else(corrupt)?)
            .map_err(|_| corrupt())?;
    let accepted_request_value: Value =
        serde_json::from_str(&accepted_operation.original_request_json).map_err(|_| corrupt())?;
    let accepted_request = AcceptRequest::parse(&accepted_request_value).map_err(|_| corrupt())?;
    let accepted_task_id = cause["task_id"].as_str().ok_or_else(corrupt)?;
    let attempt_id = cause["attempt_id"].as_str().ok_or_else(corrupt)?;
    let task_revision = cause["task_revision"].as_i64().ok_or_else(corrupt)?;
    let submission_ref = cause["submission_ref"].as_str().ok_or_else(corrupt)?;
    let candidate_ref = cause["candidate_ref"].as_str().ok_or_else(corrupt)?;
    let canonical_repository = cause["canonical_repository"].as_str().ok_or_else(corrupt)?;
    if crate::forge::canonical_repository(canonical_repository).map_err(|_| corrupt())?
        != canonical_repository
    {
        return Err(corrupt());
    }
    let target_ref = cause["target_ref"].as_str().ok_or_else(corrupt)?;
    let expected_old_ref = match &cause["expected_old_ref"] {
        Value::Null => None,
        Value::String(value) => Some(value.clone()),
        _ => return Err(corrupt()),
    };
    let expected_create = cause["expected_create"].as_bool().ok_or_else(corrupt)?;
    let source_task: Option<String> = db
        .query_row(
            "SELECT project_id FROM tasks WHERE task_id=?1",
            [accepted_task_id],
            |row| row.get(0),
        )
        .optional()?;
    let source_attempt: Option<PublicationAttemptSubjectRow> = db
        .query_row(
            "SELECT task_id,task_revision,submission_ref,candidate_ref \
             FROM attempts WHERE attempt_id=?1",
            [attempt_id],
            |row| {
                Ok(PublicationAttemptSubjectRow {
                    task_id: row.get(0)?,
                    task_revision: row.get(1)?,
                    submission_ref: row.get(2)?,
                    candidate_ref: row.get(3)?,
                })
            },
        )
        .optional()?;
    if observation.observation_id != observation_id
        || observation.operation_id != accepted_operation_id
        || observed_payload != accepted_result
        || accepted_operation.method != "task.accept"
        || accepted_operation.state != "settled"
        || accepted_operation.task_id.as_deref() != Some(accepted_task_id)
        || accepted_operation.attempt_id.as_deref() != Some(attempt_id)
        || accepted_result["outcome"] != "applied"
        || accepted_result["task_accepted"] != true
        || accepted_result["acceptance_operation_id"] != accepted_operation_id
        || accepted_result["task_id"] != accepted_task_id
        || accepted_result["attempt_id"] != attempt_id
        || accepted_result["task_revision"] != task_revision
        || accepted_result["submission_ref"] != submission_ref
        || accepted_result["candidate_ref"] != candidate_ref
        || accepted_request.attempt_id != attempt_id
        || accepted_request.expected_revision != task_revision
        || accepted_request.submission_ref != submission_ref
        || accepted_request.candidate_ref != candidate_ref
        || source_task.as_deref() != Some(link.project_id.as_str())
        || !source_attempt.is_some_and(|attempt| {
            attempt.task_id == accepted_task_id
                && attempt.task_revision == task_revision
                && attempt.submission_ref.as_deref() == Some(submission_ref)
                && attempt.candidate_ref.as_deref() == Some(candidate_ref)
        })
    {
        return Err(corrupt());
    }

    let accepted_effective: Value =
        serde_json::from_str(&accepted_operation.effective_request_json).map_err(|_| corrupt())?;
    match accepted_effective.get("automation_on_behalf") {
        Some(Value::Object(_)) => {
            if !operation_link(db, accepted_operation_id)?
                .is_some_and(|source_link| source_link.action == "task.accept")
            {
                return Err(corrupt());
            }
        }
        None | Some(Value::Null) => {
            if operation_link(db, accepted_operation_id)?.is_some() {
                return Err(corrupt());
            }
        }
        Some(_) => return Err(corrupt()),
    }

    let publication_operation: Option<PublicationOperationRow> = db
        .query_row(
            "SELECT caller_id,method,task_id,attempt_id,original_request_json, \
                    effective_request_json \
             FROM operations WHERE operation_id=?1",
            [&link.operation_id],
            |row| {
                Ok(PublicationOperationRow {
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
    let Some(publication_operation) = publication_operation else {
        return Err(corrupt());
    };
    let request_value: Value = serde_json::from_str(&publication_operation.original_request_json)
        .map_err(|_| corrupt())?;
    let request = crate::forge::PublishRefRequest::parse(&request_value).map_err(|_| corrupt())?;
    let effective: Value = serde_json::from_str(&publication_operation.effective_request_json)
        .map_err(|_| corrupt())?;
    model::fields(
        &effective,
        &["publication_intent", "automation_on_behalf", "receipt"],
    )
    .map_err(|_| corrupt())?;
    let saved_linkage = effective
        .get("automation_on_behalf")
        .filter(|value| value.is_object())
        .ok_or_else(corrupt)?;
    let expected_linkage = json!({
        "schema_version":link.schema_version,
        "technical_requester_id":link.technical_requester_id,
        "effective_manager_id":link.effective_manager_id,
        "automation_id":link.automation_id,
        "automation_revision":link.automation_revision,
        "project_id":link.project_id,
        "action":link.action,
        "cause":link.cause
    });
    let receipt = effective
        .get("receipt")
        .filter(|value| value.is_object())
        .ok_or_else(corrupt)?;
    model::fields(receipt, &["ok", "value"]).map_err(|_| corrupt())?;
    if receipt["ok"] != true || receipt["value"]["operation_id"] != link.operation_id {
        return Err(corrupt());
    }
    let intent: crate::forge::PublicationIntent = serde_json::from_value(
        effective
            .get("publication_intent")
            .cloned()
            .ok_or_else(corrupt)?,
    )
    .map_err(|_| corrupt())?;
    intent.validate().map_err(|_| corrupt())?;
    let gm_epoch = cause["gm_epoch"].as_i64().ok_or_else(corrupt)?;
    let policy_revision = cause["policy_revision"].as_str().ok_or_else(corrupt)?;
    if publication_operation.caller_id != link.technical_requester_id
        || publication_operation.method != "forge.publish_ref"
        || publication_operation.task_id.as_deref() != Some(accepted_task_id)
        || publication_operation.attempt_id.as_deref() != Some(attempt_id)
        || saved_linkage != &expected_linkage
        || request.accepted_operation_id != accepted_operation_id
        || request.attempt_id != attempt_id
        || request.expected_revision != task_revision
        || request.submission_ref != submission_ref
        || request.candidate_ref != candidate_ref
        || request.expected_policy_revision != policy_revision
        || request.target_ref != target_ref
        || request.expected_old_ref != expected_old_ref
        || request.expected_create != expected_create
        || intent.operation_id != link.operation_id
        || intent.project_id != link.project_id
        || intent.attempt_id != attempt_id
        || intent.task_revision != task_revision
        || intent.canonical_repository != canonical_repository
        || intent.admitted_gm_epoch != gm_epoch
        || intent.submission_ref != submission_ref
        || intent.accepted_operation_id != accepted_operation_id
        || intent.candidate_ref != candidate_ref
        || intent.policy_revision != policy_revision
        || intent.target_ref != target_ref
        || intent.expected_old_ref != expected_old_ref
        || intent.expected_create != expected_create
        || intent.force
    {
        return Err(corrupt());
    }
    Ok(())
}

fn validate_review_disposition_link(db: &Connection, link: &OnBehalfOperationLink) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "review disposition link does not match its retained assignment and result",
        )
    };
    let assignment_id = link.cause["review_assignment_id"]
        .as_str()
        .ok_or_else(corrupt)?;
    let result_operation_id = link.cause["operation_id"].as_str().ok_or_else(corrupt)?;
    let identity = &link.cause["identity"];
    if assignment_id.is_empty()
        || result_operation_id.is_empty()
        || link.cause["id"] != assignment_id
        || !identity.is_object()
    {
        return Err(corrupt());
    }

    let assignment_key = format!("assignment:{assignment_id}");
    let assignment_row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 \
               AND kind='review.assignment'",
            [&assignment_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((assignment_json, assignment_operation_id)) = assignment_row else {
        return Err(corrupt());
    };
    let assignment: Value = serde_json::from_str(&assignment_json).map_err(|_| corrupt())?;
    if assignment["review_assignment_id"] != assignment_id
        || assignment["operation_id"] != assignment_operation_id
        || assignment["identity"] != *identity
        || assignment["sponsor_client_id"] != link.effective_manager_id
    {
        return Err(corrupt());
    }
    let assignment_operation: Option<(String, String, Option<String>)> = db
        .query_row(
            "SELECT method,state,result_json FROM operations WHERE operation_id=?1",
            [&assignment_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((method, state, result_json)) = assignment_operation else {
        return Err(corrupt());
    };
    let assignment_result: Value =
        serde_json::from_str(&result_json.ok_or_else(corrupt)?).map_err(|_| corrupt())?;
    if method != "review.assign"
        || state != "settled"
        || assignment_result["review_assignment_id"] != assignment_id
        || assignment_result["identity"] != *identity
        || assignment_result["sponsor_client_id"] != link.effective_manager_id
        || assignment["reviewer_client_id"]
            .as_str()
            .is_none_or(str::is_empty)
    {
        return Err(corrupt());
    }

    let result_key = format!("result:{assignment_id}");
    let result_row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 \
               AND kind='review.result'",
            [&result_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((result_json, observed_result_operation_id)) = result_row else {
        return Err(corrupt());
    };
    let record: Value = serde_json::from_str(&result_json).map_err(|_| corrupt())?;
    if observed_result_operation_id != result_operation_id
        || record["operation_id"] != result_operation_id
        || record["review_assignment_id"] != assignment_id
        || record["identity"] != *identity
    {
        return Err(corrupt());
    }
    let result_operation: Option<(String, String, String, Option<String>)> = db
        .query_row(
            "SELECT caller_id,method,state,result_json FROM operations WHERE operation_id=?1",
            [result_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((reviewer_id, method, state, result_json)) = result_operation else {
        return Err(corrupt());
    };
    let result: Value =
        serde_json::from_str(&result_json.ok_or_else(corrupt)?).map_err(|_| corrupt())?;
    if reviewer_id != assignment["reviewer_client_id"]
        || method != "review.submit"
        || state != "settled"
        || result != record["result"]
        || result["review_assignment_id"] != assignment_id
        || result["reviewer_client_id"] != assignment["reviewer_client_id"]
        || result["sponsor_client_id"] != link.effective_manager_id
        || result["task_id"] != identity["task_id"]
        || result["attempt_id"] != identity["attempt_id"]
        || result["task_revision"] != identity["task_revision"]
        || result["submission_ref"] != identity["submission_ref"]
        || result["candidate_ref"] != identity["candidate_ref"]
        || result["verdict"] != "changes_requested"
        || result["applicability"] != "current_candidate"
    {
        return Err(corrupt());
    }

    let feedback_operation: Option<(String, String, String, String)> = db
        .query_row(
            "SELECT caller_id,method,original_request_json,state FROM operations WHERE operation_id=?1",
            [&link.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((caller_id, method, original_json, state)) = feedback_operation else {
        return Err(corrupt());
    };
    let request: Value = serde_json::from_str(&original_json).map_err(|_| corrupt())?;
    let finding_id = request["finding_id"].as_str().ok_or_else(corrupt)?;
    let finding = result["findings"]
        .as_array()
        .and_then(|findings| {
            findings
                .iter()
                .find(|finding| finding["finding_id"] == finding_id)
        })
        .ok_or_else(corrupt)?;
    if caller_id != AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "task.request_changes"
        || state != "settled"
        || request["attempt_id"] != identity["attempt_id"]
        || request["expected_revision"] != identity["task_revision"]
        || request["submission_ref"] != identity["submission_ref"]
        || request["candidate_ref"] != identity["candidate_ref"]
        || request["reason"] != finding["reason"]
        || request["requirement_ids"] != finding["requirement_ids"]
        || request["evidence"] != finding["evidence_refs"]
    {
        return Err(corrupt());
    }
    Ok(())
}

pub(crate) fn save_operation_link(
    db: &Connection,
    operation_id: &str,
    context: &ManagerExecutionContext,
    now_ms: i64,
) -> Result<OnBehalfOperationLink> {
    let link = OnBehalfOperationLink {
        schema_version: 1,
        operation_id: operation_id.to_owned(),
        technical_requester_id: context.technical_requester_id.clone(),
        effective_manager_id: context.effective_manager_id.clone(),
        automation_id: context.automation_id.clone(),
        automation_revision: context.automation_revision(),
        project_id: context.project_id.clone(),
        action: "review.assign".to_owned(),
        cause: context.cause_value(),
        linked_at_ms: now_ms,
    };
    let key = config::operation_link_key(operation_id)?;
    let value = link.value()?;
    config::write_record(db, &key, &value)?;
    let index_key = config::entry_operation_key(
        context.effective_manager_id(),
        context.project_id(),
        context.automation_id(),
        operation_id,
    )?;
    config::write_record(db, &index_key, &value)?;
    Ok(link)
}

pub(crate) fn on_behalf_visible_to(
    db: &Connection,
    principal: &Principal,
    operation_id: &str,
) -> Result<bool> {
    let Some(link) = any_on_behalf_operation_link(db, operation_id)? else {
        return Ok(false);
    };
    if !link.belongs_to(principal) {
        return current_gm_on_behalf_scope_visible_to(db, principal, &link);
    }
    match link {
        AnyOnBehalfOperationLink::Review(_) => Ok(true),
        AnyOnBehalfOperationLink::Acceptance(link) => {
            let task_id = link.cause["identity"]["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "acceptance link has no exact Task identity",
                )
            })?;
            current_manager_has_task_scope(db, principal, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::Publication(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication link has no exact Task identity",
                )
            })?;
            current_manager_has_task_scope(db, principal, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::WorkDispatch(link) => {
            current_work_dispatch_scope_visible_to(db, principal, &link)
        }
        AnyOnBehalfOperationLink::Repair(link) => current_manager_id_has_task_scope(
            db,
            &principal.client_id,
            &link.task_id,
            &link.project_id,
        ),
    }
}

/// A registered current GM may read validated project/task history whose
/// immutable attribution names a former GM. This grants read continuity only;
/// it does not change the recorded requester or the admission authority.
fn current_gm_on_behalf_scope_visible_to(
    db: &Connection,
    principal: &Principal,
    link: &AnyOnBehalfOperationLink,
) -> Result<bool> {
    if !is_current_registered_gm(db, principal)? {
        return Ok(false);
    }
    match link {
        AnyOnBehalfOperationLink::Review(link) => {
            let task_id: Option<String> = if link.action == "review.assign" {
                db.query_row(
                    "SELECT assignment_op.task_id
                     FROM operations AS assignment_op
                     JOIN observations AS assignment
                       ON assignment.operation_id=assignment_op.operation_id
                     JOIN tasks AS target ON target.task_id=assignment_op.task_id
                     WHERE assignment_op.operation_id=?1
                       AND assignment_op.caller_id=?2
                       AND assignment_op.method='review.assign'
                       AND assignment.source_stream_id='controller:review'
                       AND assignment.kind='review.assignment'
                       AND assignment_op.task_id=json_extract(assignment.payload_json,'$.identity.task_id')
                       AND assignment_op.attempt_id=json_extract(assignment.payload_json,'$.identity.attempt_id')
                       AND target.project_id=?3
                       AND json_extract(assignment.payload_json,'$.technical_requester_id')=?2
                       AND json_extract(assignment.payload_json,'$.sponsor_client_id')=?4
                       AND json_extract(assignment.payload_json,'$.on_behalf.effective_manager_id')=?4
                       AND json_extract(assignment.payload_json,'$.on_behalf.project_id')=?3
                       AND json_extract(assignment.payload_json,'$.on_behalf.cause.kind')=?5
                       AND json_extract(assignment.payload_json,'$.on_behalf.cause.observation_id')=?6
                       AND json_extract(assignment.payload_json,'$.on_behalf.cause.operation_id')=?7
                       AND json_extract(assignment.payload_json,'$.on_behalf.cause.id')=?8
                       AND json_extract(assignment_op.effective_request_json,'$.on_behalf.effective_manager_id')=?4
                       AND json_extract(assignment_op.effective_request_json,'$.on_behalf.project_id')=?3
                       AND json_extract(assignment_op.effective_request_json,'$.on_behalf.cause.observation_id')=?6
                       AND json_extract(assignment_op.effective_request_json,'$.on_behalf.cause.operation_id')=?7
                       AND json_extract(assignment_op.effective_request_json,'$.on_behalf.cause.id')=?8",
                    params![
                        link.operation_id,
                        AUTOMATION_TECHNICAL_REQUESTER_ID,
                        link.project_id,
                        link.effective_manager_id,
                        link.cause["kind"].as_str().unwrap_or_default(),
                        link.cause["observation_id"].as_i64().unwrap_or_default(),
                        link.cause["operation_id"].as_str().unwrap_or_default(),
                        link.cause["id"].as_str().unwrap_or_default(),
                    ],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten()
            } else {
                db.query_row(
                    "SELECT task_id FROM operations WHERE operation_id=?1 AND caller_id=?2 AND method=?3",
                    params![
                        link.operation_id,
                        AUTOMATION_TECHNICAL_REQUESTER_ID,
                        link.action,
                    ],
                    |row| row.get::<_, Option<String>>(0),
                )
                .optional()?
                .flatten()
            };
            let Some(task_id) = task_id else {
                return Ok(false);
            };
            current_gm_has_task_project(db, &task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::Acceptance(link) => {
            let task_id = link.cause["identity"]["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "acceptance link has no exact Task identity",
                )
            })?;
            current_gm_has_task_project(db, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::Publication(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "publication link has no exact Task identity",
                )
            })?;
            current_gm_has_task_project(db, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::WorkDispatch(link) => {
            current_gm_has_task_project(db, &link.task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::Repair(link) => {
            current_gm_has_task_project(db, &link.task_id, &link.project_id)
        }
    }
}

fn is_current_registered_gm(db: &Connection, principal: &Principal) -> Result<bool> {
    if principal.role != Role::Manager {
        return Ok(false);
    }
    if let Err(error) = require_registered_manager(db, &principal.client_id) {
        if error.code == "FORBIDDEN" {
            return Ok(false);
        }
        return Err(error);
    }
    let current_gm: Option<String> = db
        .query_row(
            "SELECT json_extract(value_json,'$.client_id') FROM meta WHERE key='gm'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(current_gm.as_deref() == Some(principal.client_id.as_str()))
}

fn current_gm_has_task_project(db: &Connection, task_id: &str, project_id: &str) -> Result<bool> {
    let current_project: Option<String> = db
        .query_row(
            "SELECT project_id FROM tasks WHERE task_id=?1",
            [task_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(current_project.as_deref() == Some(project_id))
}

/// Load a retained on-behalf link and validate its own durable provenance.
/// This is intended for the local Operator's global diagnostic branch; manager
/// authorization must additionally use `on_behalf_visible_to` so current
/// Task/project rights are rechecked.
pub(crate) fn any_on_behalf_operation_link(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<AnyOnBehalfOperationLink>> {
    let review = operation_link(db, operation_id)?;
    let work_dispatch = crate::store::automation_work_dispatch::operation_link(db, operation_id)?;
    let repair = crate::store::automation_repair::operation_link(db, operation_id)?;
    let mut count = 0;
    if review.is_some() {
        count += 1;
    }
    if work_dispatch.is_some() {
        count += 1;
    }
    if repair.is_some() {
        count += 1;
    }
    if count > 1 {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "Operation has multiple on-behalf attribution records",
        ));
    }
    match (review, work_dispatch, repair) {
        (Some(link), None, None) if link.action == "task.accept" => {
            Ok(Some(AnyOnBehalfOperationLink::Acceptance(link)))
        }
        (Some(link), None, None) if link.action == "forge.publish_ref" => {
            Ok(Some(AnyOnBehalfOperationLink::Publication(link)))
        }
        (Some(link), None, None) => Ok(Some(AnyOnBehalfOperationLink::Review(link))),
        (None, Some(link), None) => Ok(Some(AnyOnBehalfOperationLink::WorkDispatch(link))),
        (None, None, Some(link)) => Ok(Some(AnyOnBehalfOperationLink::Repair(Box::new(link)))),
        (None, None, None) => Ok(None),
        _ => Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "Operation has multiple on-behalf attribution records",
        )),
    }
}

type TransferOperationRow = (Option<String>, String, String, Option<i64>, Option<String>);
type TransferMutationRow = (String, String, String, Option<String>);
type CurrentTransferredAttemptRow = (
    String,
    String,
    i64,
    String,
    Option<i64>,
    Option<String>,
    Option<String>,
    String,
    String,
    i64,
    Option<String>,
);

/// Validate the narrow successor-GM path for one exact current Attempt.
/// The ordinary same-owner path returns `None`; a different owner is admitted
/// only through the complete retained automation transfer chain.
// Keeping this exact subject tuple explicit prevents callers from substituting
// a Task, revision, Attempt, submission, or candidate independently.
#[allow(clippy::too_many_arguments)]
pub(crate) fn current_transferred_attempt_authority(
    db: &Connection,
    entry: &config::AutomationEntry,
    step: AutomationStep,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    submission_ref: &str,
    candidate_ref: &str,
) -> Result<Option<TransferredAttemptAuthority>> {
    if !matches!(
        step,
        AutomationStep::ReviewDispatch
            | AutomationStep::ReviewDisposition
            | AutomationStep::Acceptance
    ) {
        return Err(Error::invalid(
            "transferred Attempt authority is limited to review and acceptance steps",
        ));
    }
    config::validate_entry(entry)?;
    let current_entry = config::load_entry(
        db,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current transferred automation entry is missing",
        )
    })?;
    let step_ready = match step {
        AutomationStep::ReviewDispatch => current_entry.review_dispatch_ready(),
        AutomationStep::ReviewDisposition | AutomationStep::Acceptance => {
            current_entry.enabled
                && current_entry.steps.contains(&step)
                && current_entry.scope.work_pool_id.is_none()
        }
        _ => false,
    };
    if current_entry != *entry || !step_ready {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current entry no longer enables the exact transferred action",
        ));
    }

    let subject: Option<CurrentTransferredAttemptRow> = db
        .query_row(
            "SELECT a.owner_id,a.task_id,a.task_revision,a.state,a.released_at_ms,\
                    a.submission_ref,a.candidate_ref,t.project_id,t.state,t.revision,\
                    (SELECT active.attempt_id FROM attempts AS active \
                     WHERE active.task_id=t.task_id AND active.released_at_ms IS NULL) \
             FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id \
             WHERE a.attempt_id=?1",
            [attempt_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .optional()?;
    let Some((
        attempt_owner_id,
        attempt_task_id,
        attempt_revision,
        attempt_state,
        released_at_ms,
        attempt_submission_ref,
        attempt_candidate_ref,
        task_project_id,
        task_state,
        current_task_revision,
        current_attempt_id,
    )) = subject
    else {
        return Err(Error::new(
            "AUTOMATION_ATTEMPT_STALE",
            "transferred review subject Attempt was not found",
        ));
    };
    let allowed_attempt_state = match step {
        AutomationStep::ReviewDisposition => {
            matches!(attempt_state.as_str(), "submitted" | "needs_correction")
        }
        AutomationStep::ReviewDispatch => attempt_state == "submitted",
        AutomationStep::Acceptance => {
            matches!(attempt_state.as_str(), "submitted" | "needs_correction")
        }
        _ => false,
    };
    if attempt_task_id != task_id
        || attempt_revision != task_revision
        || current_task_revision != task_revision
        || current_attempt_id.as_deref() != Some(attempt_id)
        || task_project_id != entry.project_id
        || task_state != "open"
        || !allowed_attempt_state
        || released_at_ms.is_some()
        || attempt_submission_ref.as_deref() != Some(submission_ref)
        || attempt_candidate_ref.as_deref() != Some(candidate_ref)
    {
        return Err(Error::new(
            "AUTOMATION_ATTEMPT_STALE",
            "Task, Attempt, submission, or candidate is no longer the exact live subject",
        ));
    }
    if attempt_owner_id == entry.owner_manager_id {
        return Ok(None);
    }

    require_registered_manager(db, &entry.owner_manager_id)?;
    let gm: Option<(String, i64)> = db
        .query_row(
            "SELECT json_extract(value_json,'$.client_id'),json_extract(value_json,'$.epoch') \
             FROM meta WHERE key='gm'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((gm_client_id, current_gm_epoch)) = gm else {
        return Err(Error::new(
            "FORBIDDEN",
            "transferred Attempt action requires a current GM designation",
        ));
    };
    if gm_client_id != entry.owner_manager_id || current_gm_epoch <= 0 {
        return Err(Error::new(
            "FORBIDDEN",
            "the current registered GM must own the transferred automation entry",
        ));
    }

    let lineage = config::transfer_successors(
        db,
        &attempt_owner_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    if lineage.is_empty() {
        return Err(Error::new(
            "FORBIDDEN",
            "Attempt owner is not in the current automation transfer lineage",
        ));
    }
    let mut expected_former_owner = attempt_owner_id.clone();
    let mut prior_new_revision = None;
    let mut owner_lineage = vec![attempt_owner_id.clone()];
    let mut transfer_operation_ids = Vec::with_capacity(lineage.len());
    for transfer in &lineage {
        if transfer.former_owner_manager_id != expected_former_owner
            || transfer.project_id != entry.project_id
            || transfer.automation_id != entry.automation_id
            || prior_new_revision.is_some_and(|revision| transfer.former_owner_revision < revision)
            || transfer.former_owner_revision.checked_add(1) != Some(transfer.new_owner_revision)
        {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "Attempt transfer chain has an inconsistent owner or revision edge",
            ));
        }
        let former_entry = config::load_entry(
            db,
            &transfer.former_owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "Attempt transfer chain has no retained former-owner snapshot",
            )
        })?;
        if former_entry.enabled || former_entry.revision != transfer.former_owner_revision {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "Attempt transfer former-owner snapshot differs from its committed edge",
            ));
        }
        validate_transfer_operation(db, transfer)?;
        expected_former_owner.clone_from(&transfer.new_owner_manager_id);
        prior_new_revision = Some(transfer.new_owner_revision);
        owner_lineage.push(transfer.new_owner_manager_id.clone());
        transfer_operation_ids.push(transfer.transfer_operation_id.clone());
    }
    if expected_former_owner != entry.owner_manager_id
        || prior_new_revision.is_none_or(|revision| current_entry.revision < revision)
    {
        return Err(Error::new(
            "FORBIDDEN",
            "current automation entry is not the terminal destination of the Attempt owner transfer",
        ));
    }
    if !current_manager_id_has_task_scope(db, &entry.owner_manager_id, task_id, &entry.project_id)?
    {
        return Err(Error::new(
            "FORBIDDEN",
            "current GM no longer has scope for the exact Task and project",
        ));
    }
    Ok(Some(TransferredAttemptAuthority {
        source_attempt_owner_id: attempt_owner_id,
        successor_manager_id: entry.owner_manager_id.clone(),
        current_gm_epoch,
        owner_lineage,
        transfer_operation_ids,
    }))
}

/// Rehydrate a current-GM continuation grant for one exact, still-queued
/// linked Operation. The stored caller/effective manager and action request
/// are never rewritten. Callers must additionally compare their immutable
/// action-specific request/intent with `current_entry()` before beginning an
/// effect. Sent, unknown, or settled effects are intentionally excluded.
pub(crate) fn current_transfer_continuation(
    db: &Connection,
    operation_id: &str,
    expected_action: &str,
    expected_step: AutomationStep,
    expected_task_id: &str,
) -> Result<Option<TransferContinuation>> {
    current_transfer_continuation_at_phase(
        db,
        operation_id,
        expected_action,
        expected_step,
        expected_task_id,
        TransferContinuationPhase::QueuedUnsent,
    )
}

/// Resolve a read-only continuation authority for the exact unknown
/// workspace-preparation effect of a transferred WorkDispatch launch. Unlike
/// `current_transfer_continuation`, this does not authorize new work and does
/// not require the successor entry to remain enabled or keep old settings.
pub(crate) fn current_transfer_workspace_readback_authority(
    db: &Connection,
    operation_id: &str,
    expected_task_id: &str,
) -> Result<Option<TransferReadbackAuthority>> {
    let Some(AnyOnBehalfOperationLink::WorkDispatch(link)) =
        any_on_behalf_operation_link(db, operation_id)?
    else {
        return Ok(None);
    };
    if link.operation_id != operation_id
        || link.action != "swarm.launch"
        || link.technical_requester_id != AUTOMATION_TECHNICAL_REQUESTER_ID
        || link.task_id != expected_task_id
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained WorkDispatch link does not match the requested launch readback",
        ));
    }

    type ReadbackOperationRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
        String,
    );
    let operation: Option<ReadbackOperationRow> = db
        .query_row(
            "SELECT caller_id,method,state,task_id,attempt_id,result_json,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            },
        )
        .optional()?;
    let Some((caller, method, state, task_id, attempt_id, result_json, effective_json)) = operation
    else {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "retained WorkDispatch launch Operation is missing",
        ));
    };
    if state != "outcome_unknown" {
        return Ok(None);
    }
    let effective: Value = serde_json::from_str(&effective_json)?;
    let manifest = &effective["launch_manifest"];
    let result = result_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()?;
    if manifest["failure"]["code"] != "workspace_effect_unknown"
        && result
            .as_ref()
            .is_none_or(|value: &Value| value["failure"]["code"] != "workspace_effect_unknown")
    {
        return Ok(None);
    }
    let result = result.ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "unknown workspace launch has no retained result",
        )
    })?;
    let expected_attempt = link
        .attempt_id
        .clone()
        .map(Value::String)
        .unwrap_or(Value::Null);
    if caller != AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "swarm.launch"
        || task_id.as_deref() != Some(link.task_id.as_str())
        || attempt_id != link.attempt_id
        || manifest["state"] != "outcome_unknown"
        || manifest["failure"]["code"] != "workspace_effect_unknown"
        || manifest["actor"]["kind"] != "work_dispatch"
        || manifest["actor"]["client_id"] != AUTOMATION_TECHNICAL_REQUESTER_ID
        || manifest["actor"]["effective_manager_id"] != link.effective_manager_id
        || manifest["actor"]["automation_id"] != link.automation_id
        || manifest["actor"]["automation_revision"] != link.automation_revision
        || manifest["actor"]["semantic_slot_id"] != link.semantic_slot_id
        || manifest["task"]["task_id"] != link.task_id
        || manifest["task"]["project_id"] != link.project_id
        || manifest["task"]["observed_revision"] != link.task_revision
        || manifest["task"]["attempt_id"] != expected_attempt
        || result["operation_id"] != operation_id
        || result["launch_state"] != "outcome_unknown"
        || result["state"] != "outcome_unknown"
        || result["failure"]["code"] != "workspace_effect_unknown"
        || result["plan_digest"] != manifest["plan_digest"]
        || result["task_id"] != link.task_id
        || result["task_revision"] != link.task_revision
        || result["attempt_id"] != expected_attempt
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "unknown launch does not retain the exact workspace readback scope",
        ));
    }

    let lineage = config::transfer_successors(
        db,
        &link.effective_manager_id,
        &link.project_id,
        &link.automation_id,
    )?;
    if lineage.is_empty() {
        return Ok(None);
    }
    let mut expected_former_owner = link.effective_manager_id.clone();
    let mut previous_new_revision = link.automation_revision;
    for (index, transfer) in lineage.iter().enumerate() {
        if transfer.former_owner_manager_id != expected_former_owner
            || transfer.project_id != link.project_id
            || transfer.automation_id != link.automation_id
            || transfer.former_owner_revision < previous_new_revision
            || (index == 0 && link.automation_revision > transfer.former_owner_revision)
        {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "workspace readback transfer chain does not match the retained launch owner",
            ));
        }
        let former = config::load_entry(
            db,
            &transfer.former_owner_manager_id,
            &link.project_id,
            &link.automation_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "workspace readback transfer chain has no former-owner snapshot",
            )
        })?;
        if former.enabled || former.revision != transfer.former_owner_revision {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "workspace readback former-owner snapshot differs from its transfer edge",
            ));
        }
        validate_transfer_operation(db, transfer)?;
        expected_former_owner.clone_from(&transfer.new_owner_manager_id);
        previous_new_revision = transfer.new_owner_revision;
    }

    let current_owner_id = lineage
        .last()
        .map(|transfer| transfer.new_owner_manager_id.clone())
        .ok_or_else(|| Error::new("AUTOMATION_TRANSFER_CORRUPT", "transfer chain is empty"))?;
    require_registered_manager(db, &current_owner_id)?;
    let designated_gm: Option<(String, i64)> = db
        .query_row(
            "SELECT json_extract(value_json,'$.client_id'),json_extract(value_json,'$.epoch') \
             FROM meta WHERE key='gm'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((designated_manager, epoch)) = designated_gm else {
        return Err(Error::new(
            "FORBIDDEN",
            "workspace readback requires a current GM designation",
        ));
    };
    if designated_manager != current_owner_id || epoch <= 0 {
        return Err(Error::new(
            "FORBIDDEN",
            "workspace readback requires the current GM to own the transferred entry",
        ));
    }
    let current_entry =
        config::load_entry(db, &current_owner_id, &link.project_id, &link.automation_id)?
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_TRANSFER_CORRUPT",
                    "workspace readback successor entry is missing",
                )
            })?;
    config::validate_entry(&current_entry)?;
    if current_entry.owner_manager_id != current_owner_id
        || current_entry.project_id != link.project_id
        || current_entry.automation_id != link.automation_id
        || current_entry.revision < previous_new_revision
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "workspace readback successor identity is inconsistent with transfer history",
        ));
    }
    if !current_manager_id_has_task_scope(
        db,
        &current_owner_id,
        expected_task_id,
        &link.project_id,
    )? {
        return Err(Error::new(
            "FORBIDDEN",
            "current GM no longer has scope for the exact Task and project",
        ));
    }
    Ok(Some(TransferReadbackAuthority {
        operation_id: operation_id.to_owned(),
        project_id: link.project_id,
        task_id: link.task_id,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TransferContinuationPhase {
    QueuedUnsent,
    PublicationPreWrite,
}

fn current_transfer_continuation_at_phase(
    db: &Connection,
    operation_id: &str,
    expected_action: &str,
    expected_step: AutomationStep,
    expected_task_id: &str,
    phase: TransferContinuationPhase,
) -> Result<Option<TransferContinuation>> {
    if !transfer_action_matches_step(expected_action, expected_step) {
        return Err(Error::invalid(
            "transfer continuation action does not match its selected step",
        ));
    }
    let Some(link) = any_on_behalf_operation_link(db, operation_id)? else {
        return Ok(None);
    };
    let (
        linked_operation_id,
        historical_owner_id,
        historical_revision,
        project_id,
        automation_id,
        action,
    ) = match &link {
        AnyOnBehalfOperationLink::Review(link)
        | AnyOnBehalfOperationLink::Acceptance(link)
        | AnyOnBehalfOperationLink::Publication(link) => (
            link.operation_id.as_str(),
            link.effective_manager_id.as_str(),
            link.automation_revision,
            link.project_id.as_str(),
            link.automation_id.as_str(),
            link.action.as_str(),
        ),
        AnyOnBehalfOperationLink::WorkDispatch(link) => (
            link.operation_id.as_str(),
            link.effective_manager_id.as_str(),
            link.automation_revision,
            link.project_id.as_str(),
            link.automation_id.as_str(),
            link.action.as_str(),
        ),
        AnyOnBehalfOperationLink::Repair(link) => (
            link.operation_id.as_str(),
            link.effective_manager_id.as_str(),
            link.automation_revision,
            link.project_id.as_str(),
            link.automation_id.as_str(),
            link.action.as_str(),
        ),
    };
    if linked_operation_id != operation_id || action != expected_action {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_SCOPE",
            "retained Operation does not match the requested transfer action",
        ));
    }
    let operation: Option<TransferOperationRow> = db
        .query_row(
            "SELECT task_id,method,state,sent_at_ms,result_json FROM operations WHERE operation_id=?1",
            [operation_id],
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
    let Some((task_id, method, state, sent_at_ms, result_json)) = operation else {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "linked Operation disappeared during transfer continuation",
        ));
    };
    if method != expected_action || task_id.as_deref() != Some(expected_task_id) {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "linked Operation Task or method does not match its retained attribution",
        ));
    }
    let phase_is_current = match phase {
        TransferContinuationPhase::QueuedUnsent => state == "queued" && sent_at_ms.is_none(),
        TransferContinuationPhase::PublicationPreWrite => {
            if expected_step != AutomationStep::Publication {
                false
            } else {
                let result: Value = result_json
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()?
                    .unwrap_or(Value::Null);
                state == "sending"
                    && sent_at_ms.is_some()
                    && result["publication_may_have_started"] == false
            }
        }
    };
    if !phase_is_current {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_EFFECT_NOT_QUEUED",
            "Operation is not in the exact no-effect transfer-continuation phase",
        ));
    }
    let lineage = config::transfer_successors(db, historical_owner_id, project_id, automation_id)?;
    if lineage.is_empty() {
        return Ok(None);
    }

    let mut expected_former_owner = historical_owner_id.to_owned();
    let mut previous_new_revision = historical_revision;
    for (index, transfer) in lineage.iter().enumerate() {
        if transfer.former_owner_manager_id != expected_former_owner
            || transfer.project_id != project_id
            || transfer.automation_id != automation_id
            || transfer.former_owner_revision < previous_new_revision
            || (index == 0 && historical_revision > transfer.former_owner_revision)
        {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "transfer chain does not continue from the retained Operation owner and revision",
            ));
        }
        let former_entry = config::load_entry(
            db,
            &transfer.former_owner_manager_id,
            project_id,
            automation_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "transfer chain has no retained former-owner entry snapshot",
            )
        })?;
        if former_entry.enabled || former_entry.revision != transfer.former_owner_revision {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "former-owner snapshot does not match its committed transfer edge",
            ));
        }
        validate_transfer_operation(db, transfer)?;
        expected_former_owner.clone_from(&transfer.new_owner_manager_id);
        previous_new_revision = transfer.new_owner_revision;
    }

    let current_owner_id = lineage
        .last()
        .map(|transfer| transfer.new_owner_manager_id.clone())
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "validated transfer chain unexpectedly has no destination",
            )
        })?;
    require_registered_manager(db, &current_owner_id)?;
    let designated_gm: Option<(String, i64)> = db
        .query_row(
            "SELECT json_extract(value_json,'$.client_id'),json_extract(value_json,'$.epoch') FROM meta WHERE key='gm'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((designated_manager, current_gm_epoch)) = designated_gm else {
        return Err(Error::new(
            "FORBIDDEN",
            "transfer continuation requires a current GM designation",
        ));
    };
    if designated_manager != current_owner_id || current_gm_epoch <= 0 {
        return Err(Error::new(
            "FORBIDDEN",
            "transfer continuation requires the current registered GM to own the destination entry",
        ));
    }
    let current_entry = config::load_entry(db, &current_owner_id, project_id, automation_id)?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "transfer destination entry is missing",
            )
        })?;
    if current_entry.revision < previous_new_revision
        || !current_entry.enabled
        || !current_entry.steps.contains(&expected_step)
    {
        return Err(Error::new(
            "FORBIDDEN",
            "current transferred automation no longer enables this action",
        ));
    }
    let ready = match expected_step {
        AutomationStep::WorkDispatch => current_entry.work_dispatch_ready(),
        AutomationStep::ReviewDispatch => current_entry.review_dispatch_ready(),
        AutomationStep::Publication => current_entry.publication_ready(),
        AutomationStep::ReviewDisposition
        | AutomationStep::RepairDispatch
        | AutomationStep::Acceptance => {
            current_entry.enabled && current_entry.scope.work_pool_id.is_none()
        }
        AutomationStep::GithubProjection => false,
    };
    if !ready {
        return Err(Error::new(
            "FORBIDDEN",
            "current transferred automation settings do not permit this action",
        ));
    }
    if !current_manager_id_has_task_scope(db, &current_owner_id, expected_task_id, project_id)? {
        return Err(Error::new(
            "FORBIDDEN",
            "current GM no longer has scope for the exact Task and project",
        ));
    }
    Ok(Some(TransferContinuation {
        historical_owner_id: historical_owner_id.to_owned(),
        historical_revision,
        current_owner_id,
        current_gm_epoch,
        project_id: project_id.to_owned(),
        automation_id: automation_id.to_owned(),
        action: action.to_owned(),
        task_id: expected_task_id.to_owned(),
        transfer_operation_ids: lineage
            .into_iter()
            .map(|transfer| transfer.transfer_operation_id)
            .collect(),
        current_entry,
    }))
}

fn transfer_action_matches_step(action: &str, step: AutomationStep) -> bool {
    matches!(
        (action, step),
        ("swarm.launch", AutomationStep::WorkDispatch)
            | ("review.assign", AutomationStep::ReviewDispatch)
            | ("task.request_changes", AutomationStep::ReviewDisposition)
            | ("task.request_changes", AutomationStep::RepairDispatch)
            | ("agent.send", AutomationStep::RepairDispatch)
            | ("task.accept", AutomationStep::Acceptance)
            | ("forge.publish_ref", AutomationStep::Publication)
    )
}

fn validated_transfer_owner_lineage(
    db: &Connection,
    source_owner_id: &str,
    project_id: &str,
    automation_id: &str,
) -> Result<Vec<String>> {
    let transfers = config::transfer_successors(db, source_owner_id, project_id, automation_id)?;
    let mut expected_former_owner = source_owner_id.to_owned();
    let mut prior_new_revision = None;
    let mut owners = vec![source_owner_id.to_owned()];
    for transfer in &transfers {
        if transfer.former_owner_manager_id != expected_former_owner
            || transfer.project_id != project_id
            || transfer.automation_id != automation_id
            || prior_new_revision.is_some_and(|revision| transfer.former_owner_revision < revision)
            || transfer.former_owner_revision.checked_add(1) != Some(transfer.new_owner_revision)
        {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "retained acceptance transfer lineage has an inconsistent owner or revision edge",
            ));
        }
        let former_entry = config::load_entry(
            db,
            &transfer.former_owner_manager_id,
            project_id,
            automation_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "retained acceptance transfer has no former-owner snapshot",
            )
        })?;
        if former_entry.enabled || former_entry.revision != transfer.former_owner_revision {
            return Err(Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "retained acceptance transfer differs from its former-owner snapshot",
            ));
        }
        validate_transfer_operation(db, transfer)?;
        expected_former_owner.clone_from(&transfer.new_owner_manager_id);
        prior_new_revision = Some(transfer.new_owner_revision);
        owners.push(transfer.new_owner_manager_id.clone());
    }
    let terminal_entry = config::load_entry(db, &expected_former_owner, project_id, automation_id)?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_TRANSFER_CORRUPT",
                "retained acceptance transfer has no terminal-owner entry",
            )
        })?;
    if prior_new_revision.is_some_and(|revision| terminal_entry.revision < revision) {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "retained acceptance terminal entry predates its transfer edge",
        ));
    }
    Ok(owners)
}

fn validate_transfer_operation(
    db: &Connection,
    transfer: &config::TransferProvenance,
) -> Result<()> {
    let operation: Option<TransferMutationRow> = db
        .query_row(
            "SELECT method,state,original_request_json,result_json FROM operations WHERE operation_id=?1",
            [&transfer.transfer_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((method, state, original_json, result_json)) = operation else {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "transfer edge has no retained Operation",
        ));
    };
    let request: Value = serde_json::from_str(&original_json).map_err(|_| {
        Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "transfer Operation request is invalid",
        )
    })?;
    let result_json = result_json.as_deref().ok_or_else(|| {
        Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "transfer Operation has no retained result",
        )
    })?;
    let result: Value = serde_json::from_str(result_json).map_err(|_| {
        Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "transfer Operation result is invalid",
        )
    })?;
    if method != "automation.config.transfer"
        || state != "settled"
        || request["project_id"] != transfer.project_id
        || request["former_owner_manager_id"] != transfer.former_owner_manager_id
        || request["automation_id"] != transfer.automation_id
        || request["expected_revision"] != transfer.former_owner_revision
        || result["transfer_operation_id"] != transfer.transfer_operation_id
        || result["project_id"] != transfer.project_id
        || result["former_owner_manager_id"] != transfer.former_owner_manager_id
        || result["new_owner_manager_id"] != transfer.new_owner_manager_id
        || result["automation_id"] != transfer.automation_id
        || result["former_owner_revision"] != transfer.former_owner_revision
        || result["new_owner_revision"] != transfer.new_owner_revision
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CORRUPT",
            "transfer record does not match its settled Operation request and result",
        ));
    }
    Ok(())
}

fn current_work_dispatch_scope_visible_to(
    db: &Connection,
    principal: &Principal,
    link: &crate::store::automation_work_dispatch::WorkDispatchOperationLink,
) -> Result<bool> {
    current_manager_has_task_scope(db, principal, &link.task_id, &link.project_id)
}

/// Current manager project/object policy shared by WorkDispatch history and
/// launcher admission. An active Attempt grants only its current owner access;
/// an unassigned or differently owned Task remains current-GM scoped. This
/// intentionally does not consult the Automation entry's enabled flag/revision.
pub(crate) fn current_manager_has_task_scope(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    project_id: &str,
) -> Result<bool> {
    if principal.role != Role::Manager {
        return Ok(false);
    }
    current_manager_id_has_task_scope(db, &principal.client_id, task_id, project_id)
}

pub(crate) fn current_manager_id_has_task_scope(
    db: &Connection,
    manager_id: &str,
    task_id: &str,
    project_id: &str,
) -> Result<bool> {
    if let Err(error) = require_registered_manager(db, manager_id) {
        if error.code == "FORBIDDEN" {
            return Ok(false);
        }
        return Err(error);
    }

    // Match the ordinary launcher read policy: a manager can read their
    // current Task scope when they own its current unreleased Attempt; an
    // unassigned or differently owned Task is visible only to the current GM.
    // The historical automation configuration revision is deliberately not
    // part of this check, so disabling or revising the entry does not erase
    // access to its retained Operation history.
    let scope: Option<(String, Option<String>)> = db
        .query_row(
            "SELECT t.project_id,(SELECT a.owner_id FROM attempts AS a \
             WHERE a.task_id=t.task_id AND a.released_at_ms IS NULL \
             ORDER BY a.created_at_ms DESC,a.attempt_id DESC LIMIT 1) \
             FROM tasks AS t WHERE t.task_id=?1",
            [task_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((current_project_id, current_attempt_owner)) = scope else {
        return Ok(false);
    };
    if current_project_id != project_id {
        return Ok(false);
    }
    if current_attempt_owner.as_deref() == Some(manager_id) {
        return Ok(true);
    }
    let current_gm: Option<String> = db
        .query_row(
            "SELECT json_extract(value_json,'$.client_id') FROM meta WHERE key='gm'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(current_gm.as_deref() == Some(manager_id))
}

pub(crate) fn entry_operation_links(
    db: &Connection,
    owner: &str,
    project: &str,
    automation_id: &str,
    after: &str,
    limit: usize,
) -> Result<Vec<OnBehalfOperationLink>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let limit = limit.min(100);
    let prefix = config::entry_operation_prefix(owner, project, automation_id)?;
    let after_key = if after.is_empty() {
        prefix.clone()
    } else {
        config::entry_operation_key(owner, project, automation_id, after)?
    };
    let pattern = format!("{prefix}%");
    let mut statement =
        db.prepare("SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 ORDER BY key LIMIT ?3")?;
    let keys = statement
        .query_map(rusqlite::params![pattern, after_key, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let mut links = Vec::with_capacity(limit);
    let mut seen_operation_ids = BTreeSet::new();
    for key in keys {
        let operation_id = key.strip_prefix(&prefix).ok_or_else(|| {
            Error::new("AUTOMATION_LINK_CORRUPT", "operation index key is invalid")
        })?;
        let link = operation_link(db, operation_id)?.ok_or_else(|| {
            Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "indexed operation link is missing",
            )
        })?;
        if link.effective_manager_id != owner
            || link.project_id != project
            || link.automation_id != automation_id
        {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "indexed operation link has a different owner or entry",
            ));
        }
        if !seen_operation_ids.insert(link.operation_id.clone()) {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "Operation appears more than once in the review entry index",
            ));
        }
        if matches!(link.action.as_str(), "task.accept" | "forge.publish_ref") {
            let task_id = if link.action == "task.accept" {
                link.cause["identity"]["task_id"].as_str()
            } else {
                link.cause["task_id"].as_str()
            }
            .ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "on-behalf link has no exact Task identity",
                )
            })?;
            if !current_manager_id_has_task_scope(db, owner, task_id, project)? {
                continue;
            }
        }
        links.push(link);
    }

    let mut dispatch_after = after.to_owned();
    let mut dispatch_scanned = 0usize;
    let mut dispatch_visible = 0usize;
    const MAX_WORK_DISPATCH_HISTORY_SCAN: usize = 100;
    while dispatch_visible < limit && dispatch_scanned < MAX_WORK_DISPATCH_HISTORY_SCAN {
        let page_limit = limit.min(MAX_WORK_DISPATCH_HISTORY_SCAN - dispatch_scanned);
        let work_dispatch = crate::store::automation_work_dispatch::entry_operation_links(
            db,
            owner,
            project,
            automation_id,
            &dispatch_after,
            page_limit,
        )?;
        if work_dispatch.is_empty() {
            break;
        }
        let page_len = work_dispatch.len();
        if let Some(last) = work_dispatch.last() {
            dispatch_after.clone_from(&last.operation_id);
        }
        dispatch_scanned += page_len;
        for link in work_dispatch {
            if !seen_operation_ids.insert(link.operation_id.clone()) {
                return Err(Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "Operation appears in both review and WorkDispatch entry indexes",
                ));
            }
            // `store::automation::explain` has already authenticated a Manager
            // and loaded this exact owner/project Automation entry. Filter each
            // WorkDispatch history item through the current Task scope before it
            // participates in the bounded response page.
            if !current_manager_id_has_task_scope(db, owner, &link.task_id, &link.project_id)? {
                continue;
            }
            dispatch_visible += 1;
            links.push(OnBehalfOperationLink {
                schema_version: 1,
                operation_id: link.operation_id,
                technical_requester_id: link.technical_requester_id,
                effective_manager_id: link.effective_manager_id,
                automation_id: link.automation_id,
                automation_revision: link.automation_revision,
                project_id: link.project_id,
                action: link.action,
                cause: json!({
                    "kind":link.semantic_cause_kind,
                    "id":link.semantic_cause_id,
                    "semantic_slot_id":link.semantic_slot_id,
                    "task_id":link.task_id,
                    "task_revision":link.task_revision,
                    "attempt_id":link.attempt_id,
                    "source":link.source
                }),
                linked_at_ms: link.linked_at_ms,
            });
        }
        if page_len < page_limit {
            break;
        }
    }

    let mut repair_after = after.to_owned();
    let mut repair_scanned = 0usize;
    let mut repair_visible = 0usize;
    const MAX_REPAIR_HISTORY_SCAN: usize = 100;
    while repair_visible < limit && repair_scanned < MAX_REPAIR_HISTORY_SCAN {
        let page_limit = (limit - repair_visible).min(MAX_REPAIR_HISTORY_SCAN - repair_scanned);
        let repairs = crate::store::automation_repair::entry_operation_links(
            db,
            owner,
            project,
            automation_id,
            &repair_after,
            page_limit,
        )?;
        if repairs.is_empty() {
            break;
        }
        let page_len = repairs.len();
        if let Some(last) = repairs.last() {
            repair_after.clone_from(&last.operation_id);
        }
        repair_scanned += page_len;
        for link in repairs {
            if !seen_operation_ids.insert(link.operation_id.clone()) {
                return Err(Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "Operation appears in more than one on-behalf entry index",
                ));
            }
            if !current_manager_id_has_task_scope(db, owner, &link.task_id, &link.project_id)? {
                continue;
            }
            repair_visible += 1;
            links.push(OnBehalfOperationLink {
                schema_version: 1,
                operation_id: link.operation_id,
                technical_requester_id: link.technical_requester_id,
                effective_manager_id: link.effective_manager_id,
                automation_id: link.automation_id,
                automation_revision: link.automation_revision,
                project_id: link.project_id,
                action: link.action,
                cause: link.cause,
                linked_at_ms: link.linked_at_ms,
            });
        }
        if page_len < page_limit {
            break;
        }
    }
    links.sort_by(|left, right| left.operation_id.cmp(&right.operation_id));
    links.truncate(limit);
    Ok(links)
}

pub(crate) fn require_registered_manager(db: &Connection, manager_id: &str) -> Result<()> {
    let key = format!("client:{manager_id}");
    let raw: Option<String> = db
        .query_row(
            "SELECT value_json FROM meta WHERE key=?1",
            params![key],
            |row| row.get(0),
        )
        .optional()?;
    let Some(raw) = raw else {
        return Err(Error::new(
            "FORBIDDEN",
            "automation manager is not registered",
        ));
    };
    let record: Value = serde_json::from_str(&raw)?;
    if record["role"] != "manager" || record["disabled"] == true {
        return Err(Error::new(
            "FORBIDDEN",
            "automation requires the current enabled manager identity",
        ));
    }
    Ok(())
}
