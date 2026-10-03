//! Typed manager-on-behalf authority for exact assigned-review dispositions.

use super::{actions::AutomationStep, authorization, config};
use crate::{
    error::{Error, Result},
    model,
    review::ReviewSlotIdentity,
};
use rusqlite::Connection;
use serde_json::{Value, json};

/// A review disposition is authorized by a retained manager-owned automation
/// entry and one exact committed review result. It is never deserialized from
/// caller input and does not impersonate a Manager Principal.
#[derive(Debug, Clone)]
pub(crate) struct ReviewDispositionContext {
    technical_requester_id: String,
    effective_manager_id: String,
    automation_id: String,
    automation_revision: i64,
    project_id: String,
    review_assignment_id: String,
    review_result_operation_id: String,
    identity: ReviewSlotIdentity,
}

impl ReviewDispositionContext {
    pub(crate) fn from_committed_entry(
        db: &Connection,
        entry: &config::AutomationEntry,
        identity: ReviewSlotIdentity,
        review_assignment_id: &str,
        review_result_operation_id: &str,
    ) -> Result<Self> {
        config::validate_entry(entry)?;
        validate_identity_text(review_assignment_id, "review_assignment_id")?;
        validate_identity_text(review_result_operation_id, "review_result_operation_id")?;
        if !entry.enabled || !entry.steps.contains(&AutomationStep::ReviewDisposition) {
            return Err(Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "entry does not currently admit review_disposition",
            ));
        }
        if entry.scope.work_pool_id.is_some() {
            return Err(Error::new(
                "AUTOMATION_SCOPE_UNAVAILABLE",
                "review disposition cannot resolve a configured work-pool scope",
            ));
        }
        if identity.task_revision <= 0 {
            return Err(Error::new(
                "REVIEW_RESULT_DAMAGED",
                "review result has an invalid Task revision",
            ));
        }

        authorization::require_registered_manager(db, &entry.owner_manager_id)?;
        let current = config::load_entry(
            db,
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "AUTOMATION_NOT_FOUND",
                "review disposition automation entry disappeared",
            )
        })?;
        if current.revision != entry.revision
            || !current.enabled
            || !current.steps.contains(&AutomationStep::ReviewDisposition)
            || current.scope.work_pool_id.is_some()
        {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current entry no longer admits this review disposition",
            ));
        }

        Ok(Self {
            technical_requester_id: authorization::AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: entry.owner_manager_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            project_id: entry.project_id.clone(),
            review_assignment_id: review_assignment_id.to_owned(),
            review_result_operation_id: review_result_operation_id.to_owned(),
            identity,
        })
    }

    /// Recheck the live registration and action before reserving the feedback
    /// effect. This context is transaction-scoped and cannot outlive the exact
    /// automation revision that authorized it.
    pub(crate) fn require_current_action(&self, db: &Connection) -> Result<()> {
        authorization::require_registered_manager(db, &self.effective_manager_id)?;
        let current = config::load_entry(
            db,
            &self.effective_manager_id,
            &self.project_id,
            &self.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        if !current.enabled
            || current.revision != self.automation_revision
            || !current.steps.contains(&AutomationStep::ReviewDisposition)
            || current.scope.work_pool_id.is_some()
        {
            return Err(Error::new(
                "FORBIDDEN",
                "current automation settings no longer permit review disposition",
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

    pub(crate) fn review_assignment_id(&self) -> &str {
        &self.review_assignment_id
    }

    pub(crate) fn review_result_operation_id(&self) -> &str {
        &self.review_result_operation_id
    }

    pub(crate) fn identity(&self) -> &ReviewSlotIdentity {
        &self.identity
    }

    pub(crate) fn cause_value(&self) -> Value {
        json!({
            "kind":"review_result",
            "id":self.review_assignment_id,
            "review_assignment_id":self.review_assignment_id,
            "operation_id":self.review_result_operation_id,
            "identity":self.identity,
        })
    }

    /// Operation attribution records the selected automation but keys the
    /// semantic request separately by manager/submission/finding.
    pub(crate) fn linkage_value(&self) -> Value {
        json!({
            "technical_requester_id":self.technical_requester_id,
            "effective_manager_id":self.effective_manager_id,
            "automation_id":self.automation_id,
            "automation_revision":self.automation_revision,
            "project_id":self.project_id,
            "action":"task.request_changes",
            "semantic_cause_kind":"review_result",
            "semantic_cause_id":self.review_assignment_id,
            "cause":self.cause_value(),
        })
    }

    /// Matches the direct manager feedback identity so manual and automatic
    /// retries share one durable decision for the same submission finding.
    pub(crate) fn semantic_request_id(
        &self,
        submission_ref: &str,
        finding_id: &str,
    ) -> Result<String> {
        let identity = model::canonical(&json!([
            self.effective_manager_id,
            submission_ref,
            finding_id
        ]))?;
        Ok(model::digest(identity.as_bytes()))
    }
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
