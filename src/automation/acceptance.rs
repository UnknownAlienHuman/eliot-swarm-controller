//! Typed manager-on-behalf authority for an exact Task acceptance decision.

use super::{actions::AutomationStep, authorization, config};
use crate::{
    acceptance::AcceptRequest,
    error::{Error, Result},
    model,
    review::{PRIMARY_REVIEW_SLOT, ReviewSlotIdentity},
};
use rusqlite::{Connection, OptionalExtension};
use serde_json::{Value, json};
use std::collections::BTreeSet;

/// Authority reconstructed only from a selected, enabled manager entry and a
/// committed pass for one exact current review slot. It never impersonates a
/// Manager Principal.
#[derive(Debug, Clone)]
pub(crate) struct AcceptanceContext {
    technical_requester_id: String,
    effective_manager_id: String,
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

struct StoredAcceptanceOperation {
    caller_id: String,
    method: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
}

impl AcceptanceContext {
    pub(crate) fn from_committed_entry(
        db: &Connection,
        entry: &config::AutomationEntry,
        identity: ReviewSlotIdentity,
        review_assignment_id: &str,
        review_result_operation_id: &str,
        expected_feedback_observation_id: i64,
        check_ids: Vec<String>,
    ) -> Result<Self> {
        config::validate_entry(entry)?;
        validate_identity_text(review_assignment_id, "review_assignment_id")?;
        validate_identity_text(review_result_operation_id, "review_result_operation_id")?;
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

        Ok(Self {
            technical_requester_id: authorization::AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
            effective_manager_id: entry.owner_manager_id.clone(),
            automation_id: entry.automation_id.clone(),
            automation_revision: entry.revision,
            project_id: entry.project_id.clone(),
            gm_epoch: gm.epoch,
            expected_feedback_observation_id,
            review_assignment_id: review_assignment_id.to_owned(),
            review_result_operation_id: review_result_operation_id.to_owned(),
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
            identity,
            assignment_id,
            result_operation_id,
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

struct CurrentGm {
    client_id: String,
    epoch: i64,
}

fn current_gm(db: &Connection) -> Result<Option<CurrentGm>> {
    let raw: Option<String> = db
        .query_row("SELECT value_json FROM meta WHERE key='gm'", [], |row| {
            row.get(0)
        })
        .optional()?;
    raw.map(|raw| {
        let value: Value = serde_json::from_str(&raw)?;
        let client_id = model::text(&value, "client_id")?.to_owned();
        let epoch = model::positive(&value, "epoch")?;
        Ok(CurrentGm { client_id, epoch })
    })
    .transpose()
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
