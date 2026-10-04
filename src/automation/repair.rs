//! Typed authority and immutable request identity for correction delivery.
//!
//! A retained manager disposition is only mailbox feedback. This context is
//! constructed from that committed disposition plus the exact assigned review
//! finding, and is revalidated before a correction Operation is admitted or
//! sent. It is never deserialized from caller input and never impersonates a
//! Manager Principal.

use super::{actions::AutomationStep, authorization, config};
use crate::{
    error::{Error, Result},
    model,
    review::{ReviewFinding, ReviewSlotIdentity},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};

const REVIEW_STREAM: &str = "controller:review";
const MAX_REPAIR_TEXT_BYTES: usize = 32 * 1024;

type RepairDeliveryOperationRow = (
    String,
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
);
type RepairCurrentSubjectRow = (
    String,
    String,
    i64,
    String,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
    Option<i64>,
    String,
    String,
    String,
    i64,
    Option<String>,
);
type RepairFeedbackOperationRow = (
    String,
    String,
    String,
    Option<String>,
    Option<String>,
    Option<String>,
);

#[derive(Debug, Clone)]
pub(crate) struct RepairDispatchContext {
    technical_requester_id: String,
    effective_manager_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    review_assignment_id: String,
    review_result_operation_id: String,
    disposition_operation_id: String,
    feedback_operation_id: String,
    feedback_observation_id: i64,
    identity: ReviewSlotIdentity,
    finding: ReviewFinding,
    binding_id: String,
    binding_generation: i64,
    semantic_slot_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RepairDeliveryRequest {
    pub(crate) client_request_id: String,
    pub(crate) binding_id: String,
    pub(crate) generation: i64,
    pub(crate) text: String,
}

impl RepairDeliveryRequest {
    pub(crate) fn value(&self) -> Value {
        json!({
            "client_request_id":self.client_request_id,
            "binding_id":self.binding_id,
            "generation":self.generation,
            "delivery":"next_turn",
            "text":self.text
        })
    }

    pub(crate) fn parameters_digest(&self) -> Result<String> {
        Ok(model::digest(
            model::canonical(&json!({
                "action":"agent.send",
                "delivery":"next_turn",
                "binding_id":self.binding_id,
                "generation":self.generation,
                "text":self.text
            }))?
            .as_bytes(),
        ))
    }
}

impl RepairDispatchContext {
    /// Construct from Store-verified evidence for one actual return-for-
    /// correction disposition. The exact finding and feedback lineage are
    /// checked here again so no caller can mint this authority from a result
    /// summary or a request body.
    #[allow(clippy::too_many_arguments)] // Each argument is one immutable evidence anchor.
    pub(crate) fn from_committed_disposition(
        db: &Connection,
        entry: &config::AutomationEntry,
        identity: ReviewSlotIdentity,
        review_assignment_id: &str,
        review_result_operation_id: &str,
        disposition_operation_id: &str,
        feedback_operation_id: &str,
        feedback_observation_id: i64,
        finding: ReviewFinding,
    ) -> Result<Self> {
        config::validate_entry(entry)?;
        validate_identity_text(review_assignment_id, "review_assignment_id")?;
        validate_identity_text(review_result_operation_id, "review_result_operation_id")?;
        validate_identity_text(disposition_operation_id, "disposition_operation_id")?;
        validate_identity_text(feedback_operation_id, "feedback_operation_id")?;
        if feedback_observation_id <= 0 || identity.task_revision <= 0 {
            return Err(source_damaged());
        }
        if !entry.enabled || !entry.steps.contains(&AutomationStep::RepairDispatch) {
            return Err(Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "current automation entry does not select repair_dispatch",
            ));
        }
        if entry.scope.work_pool_id.is_some() {
            return Err(Error::new(
                "AUTOMATION_SCOPE_UNAVAILABLE",
                "repair_dispatch cannot resolve configured work-pool membership",
            ));
        }
        authorization::require_registered_manager(db, &entry.owner_manager_id)?;
        require_current_entry_action(db, entry)?;
        let (binding_id, binding_generation) =
            current_subject_binding(db, &identity, &entry.owner_manager_id, &entry.project_id)?;
        let context = Self {
            technical_requester_id: authorization::AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: entry.owner_manager_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            project_id: entry.project_id.clone(),
            review_assignment_id: review_assignment_id.to_owned(),
            review_result_operation_id: review_result_operation_id.to_owned(),
            disposition_operation_id: disposition_operation_id.to_owned(),
            feedback_operation_id: feedback_operation_id.to_owned(),
            feedback_observation_id,
            identity,
            finding,
            binding_id,
            binding_generation,
            semantic_slot_id: String::new(),
        };
        let mut context = context;
        context.semantic_slot_id = context.compute_semantic_slot_id()?;
        context.require_committed_lineage(db)?;
        context.require_current_subject(db)?;
        Ok(context)
    }

    /// Rebuild the context from the source identities retained in an
    /// automation Operation link. The Store layer supplies the sealed link;
    /// this method still re-reads the retained review, disposition, and
    /// feedback facts before constructing readback provenance. A separate
    /// stage guard is required before any new native effect.
    #[allow(clippy::too_many_arguments)] // The link is intentionally expanded into named anchors.
    pub(crate) fn from_retained_link(
        db: &Connection,
        owner_manager_id: &str,
        automation_id: &str,
        automation_revision: i64,
        project_id: &str,
        review_assignment_id: &str,
        review_result_operation_id: &str,
        disposition_operation_id: &str,
        feedback_operation_id: &str,
        feedback_observation_id: i64,
        identity: ReviewSlotIdentity,
        finding: ReviewFinding,
        binding_id: &str,
        binding_generation: i64,
        semantic_slot_id: &str,
    ) -> Result<Self> {
        validate_identity_text(owner_manager_id, "owner_manager_id")?;
        validate_identity_text(automation_id, "automation_id")?;
        validate_identity_text(project_id, "project_id")?;
        validate_identity_text(binding_id, "binding_id")?;
        validate_identity_text(semantic_slot_id, "semantic_slot_id")?;
        if automation_revision <= 0 || binding_generation <= 0 {
            return Err(source_damaged());
        }
        let context = Self::from_retained_source(
            db,
            owner_manager_id,
            automation_id,
            automation_revision,
            project_id,
            identity,
            review_assignment_id,
            review_result_operation_id,
            disposition_operation_id,
            feedback_operation_id,
            feedback_observation_id,
            finding,
            binding_id,
            binding_generation,
        )?;
        if context.semantic_slot_id != semantic_slot_id {
            return Err(source_damaged());
        }
        Ok(context)
    }

    #[allow(clippy::too_many_arguments)] // Shared validated construction for new and retained Operations.
    fn from_retained_source(
        db: &Connection,
        owner_manager_id: &str,
        automation_id: &str,
        automation_revision: i64,
        project_id: &str,
        identity: ReviewSlotIdentity,
        review_assignment_id: &str,
        review_result_operation_id: &str,
        disposition_operation_id: &str,
        feedback_operation_id: &str,
        feedback_observation_id: i64,
        finding: ReviewFinding,
        expected_binding_id: &str,
        expected_binding_generation: i64,
    ) -> Result<Self> {
        validate_identity_text(review_assignment_id, "review_assignment_id")?;
        validate_identity_text(review_result_operation_id, "review_result_operation_id")?;
        validate_identity_text(disposition_operation_id, "disposition_operation_id")?;
        validate_identity_text(feedback_operation_id, "feedback_operation_id")?;
        if feedback_observation_id <= 0 || identity.task_revision <= 0 {
            return Err(source_damaged());
        }
        if expected_binding_generation <= 0 {
            return Err(source_damaged());
        }
        let mut context = Self {
            technical_requester_id: authorization::AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: owner_manager_id.to_owned(),
            automation_id: automation_id.to_owned(),
            automation_revision,
            project_id: project_id.to_owned(),
            review_assignment_id: review_assignment_id.to_owned(),
            review_result_operation_id: review_result_operation_id.to_owned(),
            disposition_operation_id: disposition_operation_id.to_owned(),
            feedback_operation_id: feedback_operation_id.to_owned(),
            feedback_observation_id,
            identity,
            finding,
            binding_id: expected_binding_id.to_owned(),
            binding_generation: expected_binding_generation,
            semantic_slot_id: String::new(),
        };
        context.semantic_slot_id = context.compute_semantic_slot_id()?;
        context.require_committed_lineage(db)?;
        Ok(context)
    }

    pub(crate) fn require_current_for_admission(&self, db: &Connection) -> Result<()> {
        self.require_current_action(db)?;
        self.require_current_subject(db)?;
        self.require_committed_lineage(db)?;
        self.require_no_unresolved_effect(db, None)
    }

    /// Called at the native pre-effect boundary for one already admitted
    /// delivery. The sole queued Operation excluded from the prior-effect
    /// fence must be the exact linked Operation being dispatched.
    pub(crate) fn require_current_for_effect(
        &self,
        db: &Connection,
        delivery_operation_id: &str,
    ) -> Result<()> {
        validate_identity_text(delivery_operation_id, "delivery_operation_id")?;
        self.require_current_action(db)?;
        self.require_current_subject(db)?;
        self.require_committed_lineage(db)?;
        self.require_delivery_operation(db, delivery_operation_id)?;
        self.require_no_unresolved_effect(db, Some(delivery_operation_id))
    }

    fn require_current_action(&self, db: &Connection) -> Result<()> {
        authorization::require_registered_manager(db, &self.effective_manager_id)?;
        let current = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "repair automation entry was removed"))?;
        if !current.enabled
            || current.revision != self.automation_revision
            || !current.steps.contains(&AutomationStep::RepairDispatch)
            || current.scope.work_pool_id.is_some()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "current automation no longer selects repair_dispatch for this scope",
            ));
        }
        Ok(())
    }

    fn require_current_subject(&self, db: &Connection) -> Result<()> {
        authorization::require_registered_manager(db, &self.effective_manager_id)?;
        let subject = read_current_subject(db, &self.identity)?;
        if subject.project_id != self.project_id
            || subject.owner_id != self.effective_manager_id
            || subject.task_state != "open"
            || subject.current_task_revision != self.identity.task_revision
            || subject.current_attempt_id.as_deref() != Some(self.identity.attempt_id.as_str())
            || subject.task_revision != self.identity.task_revision
            || subject.released_at_ms.is_some()
            || subject.attempt_state != "needs_correction"
            || subject.submission_ref.as_deref() != Some(&self.identity.submission_ref)
            || subject.candidate_ref.as_deref() != Some(&self.identity.candidate_ref)
            || subject.binding_id.as_deref() != Some(self.binding_id.as_str())
            || subject.binding_generation != Some(self.binding_generation)
            || !crate::policy::allows_scoped_manager_feedback(&subject.task_snapshot)
        {
            return Err(Error::new(
                "STALE_REPAIR_SUBJECT",
                "the exact owner-policy-v2 Task, Attempt, candidate, or correction phase is no longer current",
            ));
        }
        let binding_state: Option<(String, Option<i64>)> = db
            .query_row(
                "SELECT state,released_at_ms FROM bindings WHERE binding_id=?1 AND generation=?2",
                params![self.binding_id, self.binding_generation],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        if !binding_state.is_some_and(|(state, released)| state == "ready" && released.is_none()) {
            return Err(Error::new(
                "REPAIR_OWNER_UNAVAILABLE",
                "the current owner's exact native binding is not ready",
            ));
        }
        Ok(())
    }

    fn require_committed_lineage(&self, db: &Connection) -> Result<()> {
        validate_committed_review_and_feedback(
            db,
            &self.effective_manager_id,
            &self.identity,
            &self.review_assignment_id,
            &self.review_result_operation_id,
            &self.disposition_operation_id,
            &self.feedback_operation_id,
            self.feedback_observation_id,
            &self.finding,
        )
    }

    fn require_delivery_operation(&self, db: &Connection, operation_id: &str) -> Result<()> {
        let row: Option<RepairDeliveryOperationRow> = db
            .query_row(
                "SELECT caller_id,method,state,original_request_json,task_id,attempt_id,binding_id,binding_generation \
                 FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?,row.get(6)?,row.get(7)?)),
            )
            .optional()?;
        let Some((
            caller_id,
            method,
            state,
            original_json,
            task_id,
            attempt_id,
            binding_id,
            generation,
        )) = row
        else {
            return Err(source_damaged());
        };
        let request: Value = serde_json::from_str(&original_json).map_err(|_| source_damaged())?;
        let expected = self.delivery_request()?.value();
        if caller_id != self.technical_requester_id
            || method != "agent.send"
            || state != "queued"
            || task_id.as_deref() != Some(self.identity.task_id.as_str())
            || attempt_id.as_deref() != Some(self.identity.attempt_id.as_str())
            || binding_id.as_deref() != Some(self.binding_id.as_str())
            || generation != Some(self.binding_generation)
            || model::canonical(&request)? != model::canonical(&expected)?
        {
            return Err(Error::new(
                "REPAIR_OPERATION_MISMATCH",
                "queued correction Operation differs from its immutable retained repair context",
            ));
        }
        Ok(())
    }

    fn require_no_unresolved_effect(
        &self,
        db: &Connection,
        own_operation_id: Option<&str>,
    ) -> Result<()> {
        let unresolved: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM operations \
             WHERE binding_id=?1 AND binding_generation=?2 \
               AND state IN ('sending','native_accepted','outcome_unknown') \
               AND method IN ('agent.open','task.dispatch','agent.send','agent.configure','agent.goal','agent.recover') \
               AND (?3 IS NULL OR operation_id<>?3))",
            params![self.binding_id, self.binding_generation, own_operation_id],
            |row| row.get(0),
        )?;
        if unresolved {
            return Err(Error::new(
                "REPAIR_NATIVE_EFFECT_UNRESOLVED",
                "an earlier native effect on the exact owner binding must be reconciled before correction",
            ));
        }
        Ok(())
    }

    fn compute_semantic_slot_id(&self) -> Result<String> {
        semantic_slot_id(
            &self.effective_manager_id,
            &self.identity,
            &self.finding.finding_id,
        )
    }

    pub(crate) fn delivery_request(&self) -> Result<RepairDeliveryRequest> {
        let text = render_correction_text(&self.identity, &self.finding);
        if text.len() > MAX_REPAIR_TEXT_BYTES {
            return Err(Error::new(
                "REPAIR_REQUEST_TOO_LARGE",
                "exact correction feedback exceeds the bounded native input size",
            ));
        }
        Ok(RepairDeliveryRequest {
            client_request_id: self.semantic_slot_id.clone(),
            binding_id: self.binding_id.clone(),
            generation: self.binding_generation,
            text,
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

    pub(crate) fn project_id(&self) -> &str {
        &self.project_id
    }

    pub(crate) fn review_assignment_id(&self) -> &str {
        &self.review_assignment_id
    }

    pub(crate) fn review_result_operation_id(&self) -> &str {
        &self.review_result_operation_id
    }

    pub(crate) fn disposition_operation_id(&self) -> &str {
        &self.disposition_operation_id
    }

    pub(crate) fn feedback_operation_id(&self) -> &str {
        &self.feedback_operation_id
    }

    pub(crate) fn feedback_observation_id(&self) -> i64 {
        self.feedback_observation_id
    }

    pub(crate) fn identity(&self) -> &ReviewSlotIdentity {
        &self.identity
    }

    pub(crate) fn finding(&self) -> &ReviewFinding {
        &self.finding
    }

    pub(crate) fn binding_id(&self) -> &str {
        &self.binding_id
    }

    pub(crate) fn binding_generation(&self) -> i64 {
        self.binding_generation
    }

    pub(crate) fn semantic_slot_id(&self) -> &str {
        &self.semantic_slot_id
    }

    pub(crate) fn cause_value(&self) -> Value {
        json!({
            "kind":"review_disposition",
            "id":self.review_assignment_id,
            "review_assignment_id":self.review_assignment_id,
            "review_result_operation_id":self.review_result_operation_id,
            "disposition_operation_id":self.disposition_operation_id,
            "feedback_operation_id":self.feedback_operation_id,
            "feedback_observation_id":self.feedback_observation_id,
            "identity":self.identity,
            "finding_id":self.finding.finding_id,
            "semantic_slot_id":self.semantic_slot_id
        })
    }

    pub(crate) fn linkage_value(&self) -> Value {
        json!({
            "technical_requester_id":self.technical_requester_id,
            "effective_manager_id":self.effective_manager_id,
            "automation_id":self.automation_id,
            "automation_revision":self.automation_revision,
            "project_id":self.project_id,
            "action":"agent.send",
            "semantic_cause_kind":"review_disposition",
            "semantic_cause_id":self.review_assignment_id,
            "semantic_slot_id":self.semantic_slot_id,
            "cause":self.cause_value()
        })
    }
}

/// Validate the live owner and binding for a direct Manager send that exactly
/// matches one retained correction request. This grants no automation rights;
/// the caller still passes the normal authenticated `agent.send` checks.
pub(crate) fn require_current_manual_owner(
    db: &Connection,
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    binding_id: &str,
    binding_generation: i64,
) -> Result<()> {
    authorization::require_registered_manager(db, manager_id)?;
    let subject = read_current_subject(db, identity)?;
    if subject.owner_id != manager_id
        || subject.task_id != identity.task_id
        || subject.task_revision != identity.task_revision
        || subject.task_state != "open"
        || subject.current_task_revision != identity.task_revision
        || subject.current_attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || subject.released_at_ms.is_some()
        || subject.attempt_state != "needs_correction"
        || subject.submission_ref.as_deref() != Some(identity.submission_ref.as_str())
        || subject.candidate_ref.as_deref() != Some(identity.candidate_ref.as_str())
        || subject.binding_id.as_deref() != Some(binding_id)
        || subject.binding_generation != Some(binding_generation)
        || !crate::policy::allows_scoped_manager_feedback(&subject.task_snapshot)
    {
        return Err(Error::new(
            "STALE_REPAIR_SUBJECT",
            "direct correction does not match the current owner-policy-v2 Task Attempt",
        ));
    }
    let binding_state: Option<(String, Option<i64>)> = db
        .query_row(
            "SELECT state,released_at_ms FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id, binding_generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if !binding_state.is_some_and(|(state, released)| state == "ready" && released.is_none()) {
        return Err(Error::new(
            "REPAIR_OWNER_UNAVAILABLE",
            "the exact correction owner's binding is not ready",
        ));
    }
    Ok(())
}

struct CurrentSubject {
    owner_id: String,
    task_id: String,
    task_revision: i64,
    attempt_state: String,
    released_at_ms: Option<i64>,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    task_snapshot: Value,
    project_id: String,
    task_state: String,
    current_task_revision: i64,
    current_attempt_id: Option<String>,
}

fn read_current_subject(db: &Connection, identity: &ReviewSlotIdentity) -> Result<CurrentSubject> {
    let raw: Option<RepairCurrentSubjectRow> = db
        .query_row(
            "SELECT a.owner_id,a.task_id,a.task_revision,a.state,a.released_at_ms,a.submission_ref,a.candidate_ref, \
                    a.binding_id,a.binding_generation,a.task_snapshot_json,t.project_id,t.state,t.revision,t.current_attempt_id \
             FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id WHERE a.attempt_id=?1",
            [&identity.attempt_id],
            |row| {
                Ok((
                    row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?,
                    row.get(5)?, row.get(6)?, row.get(7)?, row.get(8)?, row.get(9)?,
                    row.get(10)?, row.get(11)?, row.get(12)?, row.get(13)?,
                ))
            },
        )
        .optional()?;
    let Some((
        owner_id,
        task_id,
        task_revision,
        attempt_state,
        released_at_ms,
        submission_ref,
        candidate_ref,
        binding_id,
        binding_generation,
        task_snapshot,
        project_id,
        task_state,
        current_task_revision,
        current_attempt_id,
    )) = raw
    else {
        return Err(Error::new(
            "STALE_REPAIR_SUBJECT",
            "the correction Attempt no longer exists",
        ));
    };
    let task_snapshot: Value = serde_json::from_str(&task_snapshot).map_err(|_| {
        Error::new(
            "TASK_DAMAGED",
            "the retained Attempt Task snapshot is invalid",
        )
    })?;
    Ok(CurrentSubject {
        owner_id,
        task_id,
        task_revision,
        attempt_state,
        released_at_ms,
        submission_ref,
        candidate_ref,
        binding_id,
        binding_generation,
        task_snapshot,
        project_id,
        task_state,
        current_task_revision,
        current_attempt_id,
    })
}

fn current_subject_binding(
    db: &Connection,
    identity: &ReviewSlotIdentity,
    manager_id: &str,
    project_id: &str,
) -> Result<(String, i64)> {
    let subject = read_current_subject(db, identity)?;
    if subject.owner_id != manager_id
        || subject.task_id != identity.task_id
        || subject.task_revision != identity.task_revision
        || subject.project_id != project_id
        || subject.task_state != "open"
        || subject.current_task_revision != identity.task_revision
        || subject.current_attempt_id.as_deref() != Some(identity.attempt_id.as_str())
        || subject.released_at_ms.is_some()
        || subject.attempt_state != "needs_correction"
        || subject.submission_ref.as_deref() != Some(identity.submission_ref.as_str())
        || subject.candidate_ref.as_deref() != Some(identity.candidate_ref.as_str())
        || !crate::policy::allows_scoped_manager_feedback(&subject.task_snapshot)
    {
        return Err(Error::new(
            "STALE_REPAIR_SUBJECT",
            "repair requires the exact current owner-policy-v2 Attempt after feedback was applied",
        ));
    }
    let binding_id = subject.binding_id.ok_or_else(|| {
        Error::new(
            "REPAIR_OWNER_UNAVAILABLE",
            "the exact Attempt has no retained native owner binding",
        )
    })?;
    let generation = subject.binding_generation.ok_or_else(|| {
        Error::new(
            "REPAIR_OWNER_UNAVAILABLE",
            "the exact Attempt has no retained native binding generation",
        )
    })?;
    if generation <= 0 {
        return Err(source_damaged());
    }
    Ok((binding_id, generation))
}

fn require_current_entry_action(db: &Connection, entry: &config::AutomationEntry) -> Result<()> {
    let current = config::load_entry(
        db,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_NOT_FOUND",
            "repair automation entry disappeared",
        )
    })?;
    if current.revision != entry.revision
        || !current.enabled
        || !current.steps.contains(&AutomationStep::RepairDispatch)
        || current.scope.work_pool_id.is_some()
    {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current automation entry no longer admits this repair action",
        ));
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)] // Parameters are the distinct immutable review/feedback evidence anchors.
pub(crate) fn validate_committed_review_and_feedback(
    db: &Connection,
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    assignment_id: &str,
    result_operation_id: &str,
    disposition_operation_id: &str,
    feedback_operation_id: &str,
    feedback_observation_id: i64,
    finding: &ReviewFinding,
) -> Result<()> {
    let damaged = source_damaged;
    let assignment_key = format!("assignment:{assignment_id}");
    let assignment_row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.assignment'",
            params![REVIEW_STREAM, assignment_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((assignment_json, assignment_operation_id)) = assignment_row else {
        return Err(damaged());
    };
    let assignment: Value = serde_json::from_str(&assignment_json).map_err(|_| damaged())?;
    if assignment["review_assignment_id"] != assignment_id
        || assignment["operation_id"] != assignment_operation_id
        || assignment["identity"] != json!(identity)
        || assignment["sponsor_client_id"] != manager_id
    {
        return Err(damaged());
    }
    let assignment_operation: Option<(String, String, Option<String>)> = db
        .query_row(
            "SELECT method,state,result_json FROM operations WHERE operation_id=?1",
            [&assignment_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((method, state, result_json)) = assignment_operation else {
        return Err(damaged());
    };
    let assignment_result: Value =
        serde_json::from_str(&result_json.ok_or_else(damaged)?).map_err(|_| damaged())?;
    if method != "review.assign"
        || state != "settled"
        || assignment_result["review_assignment_id"] != assignment_id
        || assignment_result["identity"] != json!(identity)
        || assignment_result["sponsor_client_id"] != manager_id
    {
        return Err(damaged());
    }

    let result_key = format!("result:{assignment_id}");
    let result_row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.result'",
            params![REVIEW_STREAM, result_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((result_json, observed_result_operation_id)) = result_row else {
        return Err(damaged());
    };
    let review_result: Value = serde_json::from_str(&result_json).map_err(|_| damaged())?;
    if observed_result_operation_id != result_operation_id
        || review_result["operation_id"] != result_operation_id
        || review_result["review_assignment_id"] != assignment_id
        || review_result["identity"] != json!(identity)
    {
        return Err(damaged());
    }
    let result_operation: Option<(String, String, String, Option<String>)> = db
        .query_row(
            "SELECT caller_id,method,state,result_json FROM operations WHERE operation_id=?1",
            [result_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((reviewer_id, method, state, result_json)) = result_operation else {
        return Err(damaged());
    };
    let result_value: Value =
        serde_json::from_str(&result_json.ok_or_else(damaged)?).map_err(|_| damaged())?;
    let finding_value = serde_json::to_value(finding).map_err(|_| damaged())?;
    if method != "review.submit"
        || state != "settled"
        || reviewer_id
            != assignment["reviewer_client_id"]
                .as_str()
                .unwrap_or_default()
        || result_value != review_result["result"]
        || result_value["review_assignment_id"] != assignment_id
        || result_value["sponsor_client_id"] != manager_id
        || result_value["task_id"] != identity.task_id
        || result_value["task_revision"] != identity.task_revision
        || result_value["attempt_id"] != identity.attempt_id
        || result_value["submission_ref"] != identity.submission_ref
        || result_value["candidate_ref"] != identity.candidate_ref
        || result_value["verdict"] != "changes_requested"
        || result_value["applicability"] != "current_candidate"
        || !result_value["findings"].as_array().is_some_and(|items| {
            items
                .iter()
                .filter(|item| item["finding_id"] == finding.finding_id)
                .count()
                == 1
                && items.iter().any(|item| item == &finding_value)
        })
    {
        return Err(damaged());
    }

    let disposition_key = format!("disposition:{assignment_id}");
    let disposition_row: Option<(String, String)> = db
        .query_row(
            "SELECT payload_json,operation_id FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 AND kind='review.disposition'",
            params![REVIEW_STREAM, disposition_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((disposition_json, disposition_observation_operation_id)) = disposition_row else {
        return Err(damaged());
    };
    let disposition: Value = serde_json::from_str(&disposition_json).map_err(|_| damaged())?;
    if disposition["schema_version"] != 1
        || disposition["kind"] != "review.disposition"
        || disposition["review_assignment_id"] != assignment_id
        || disposition["operation_id"] != disposition_operation_id
        || disposition_observation_operation_id != disposition_operation_id
        || disposition["review_result_operation_id"] != result_operation_id
        || disposition["identity"] != json!(identity)
        || disposition["disposition"] != "return_for_correction"
        || disposition["decided_by"] != manager_id
        || disposition["finding_ids"] != json!([finding.finding_id])
        || disposition["task_feedback_operation_id"] != feedback_operation_id
    {
        return Err(damaged());
    }
    validate_manager_disposition_operation(db, manager_id, disposition_operation_id, identity)?;

    let feedback_operation: Option<RepairFeedbackOperationRow> = db
        .query_row(
            "SELECT caller_id,method,state,result_json,task_id,attempt_id FROM operations WHERE operation_id=?1",
            [feedback_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .optional()?;
    let Some((
        feedback_caller,
        feedback_method,
        feedback_state,
        feedback_result_json,
        feedback_task,
        feedback_attempt,
    )) = feedback_operation
    else {
        return Err(damaged());
    };
    let feedback_result: Value =
        serde_json::from_str(&feedback_result_json.ok_or_else(damaged)?).map_err(|_| damaged())?;
    let expected_finding = json!({
        "attempt_id":identity.attempt_id,
        "task_revision":identity.task_revision,
        "submission_ref":identity.submission_ref,
        "candidate_ref":identity.candidate_ref,
        "finding_id":finding.finding_id,
        "reason":finding.reason,
        "requirement_ids":finding.requirement_ids,
        "evidence":finding.evidence_refs
    });
    if feedback_method != "task.request_changes"
        || feedback_state != "settled"
        || feedback_task.as_deref() != Some(identity.task_id.as_str())
        || feedback_attempt.as_deref() != Some(identity.attempt_id.as_str())
        || feedback_result["operation_id"] != feedback_operation_id
        || feedback_result["applied"] != true
        || feedback_result["status"] != "needs_correction"
        || feedback_result["delivery"] != "durable_mailbox_only"
        || feedback_result["recipient"] != manager_id
        || feedback_result["task_id"] != identity.task_id
        || feedback_result["finding"] != expected_finding
        || feedback_result["review_provenance"]["review_assignment_id"] != assignment_id
        || feedback_result["review_provenance"]["review_operation_id"] != result_operation_id
        || feedback_result["review_provenance"]["identity"] != json!(identity)
        || feedback_result["review_provenance"]["finding"] != finding_value
        || feedback_result["native_input_sent"] != false
        || feedback_result["repair_started"] != false
    {
        return Err(damaged());
    }
    if feedback_caller != manager_id {
        let link = authorization::operation_link(db, feedback_operation_id)?.ok_or_else(damaged)?;
        if link.action != "task.request_changes"
            || link.effective_manager_id != manager_id
            || link.cause["identity"] != json!(identity)
            || link.cause["review_assignment_id"] != assignment_id
        {
            return Err(damaged());
        }
    }

    let feedback_observation: Option<(String, String, String)> = db
        .query_row(
            "SELECT source_stream_id,kind,payload_json FROM observations WHERE observation_id=?1 AND operation_id=?2",
            params![feedback_observation_id, feedback_operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?;
    let Some((stream, kind, payload_json)) = feedback_observation else {
        return Err(damaged());
    };
    let feedback_payload: Value = serde_json::from_str(&payload_json).map_err(|_| damaged())?;
    if stream != REVIEW_STREAM || kind != "task.feedback" || feedback_payload != feedback_result {
        return Err(damaged());
    }
    Ok(())
}

fn validate_manager_disposition_operation(
    db: &Connection,
    manager_id: &str,
    operation_id: &str,
    identity: &ReviewSlotIdentity,
) -> Result<()> {
    let row: Option<(String, String, String, Option<String>)> = db
        .query_row(
            "SELECT caller_id,method,state,result_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((caller_id, method, state, result_json)) = row else {
        return Err(source_damaged());
    };
    let result: Value = serde_json::from_str(&result_json.ok_or_else(source_damaged)?)
        .map_err(|_| source_damaged())?;
    if method != "task.request_changes"
        || state != "settled"
        || result["applied"] != true
        || result["status"] != "needs_correction"
        || result["current_disposition_recorded"] == false
    {
        return Err(source_damaged());
    }
    if caller_id == manager_id {
        return Ok(());
    }
    let link = authorization::operation_link(db, operation_id)?.ok_or_else(source_damaged)?;
    if link.action != "task.request_changes"
        || link.effective_manager_id != manager_id
        || link.cause["identity"] != json!(identity)
    {
        return Err(source_damaged());
    }
    Ok(())
}

pub(crate) fn semantic_slot_id(
    manager_id: &str,
    identity: &ReviewSlotIdentity,
    finding_id: &str,
) -> Result<String> {
    validate_identity_text(manager_id, "manager_id")?;
    validate_identity_text(finding_id, "finding_id")?;
    let slot_identity = json!({
        "manager_id":manager_id,
        "task_id":identity.task_id,
        "task_revision":identity.task_revision,
        "attempt_id":identity.attempt_id,
        "submission_ref":identity.submission_ref,
        "candidate_ref":identity.candidate_ref,
        "action":"repair_dispatch",
        "finding_id":finding_id
    });
    Ok(model::digest(model::canonical(&slot_identity)?.as_bytes()))
}

pub(crate) fn render_correction_text(
    identity: &ReviewSlotIdentity,
    finding: &ReviewFinding,
) -> String {
    let requirements = finding
        .requirement_ids
        .iter()
        .map(|value| format!("- {value}"))
        .collect::<Vec<_>>()
        .join("\n");
    let evidence = finding
        .evidence_refs
        .iter()
        .map(|value| format!("- {value}"))
        .collect::<Vec<_>>()
        .join("\n");
    let text = format!(
        "A manager applied this correction request to your current Task Attempt. Keep the same unreleased Attempt, address the exact finding and requirements below, and submit a new candidate linked to the prior submission.\n\nTask: {}\nTask revision: {}\nAttempt: {}\nPrior submission: {}\nPrior candidate: {}\nFinding: {}\n\nReason:\n{}\n\nRequested change:\n{}\n\nRequirements:\n{}\n\nEvidence references:\n{}",
        identity.task_id,
        identity.task_revision,
        identity.attempt_id,
        identity.submission_ref,
        identity.candidate_ref,
        finding.finding_id,
        finding.reason,
        finding.requested_change,
        requirements,
        evidence
    );
    text
}

fn source_damaged() -> Error {
    Error::new(
        "REPAIR_SOURCE_DAMAGED",
        "retained review disposition or feedback does not match the exact repair subject",
    )
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
