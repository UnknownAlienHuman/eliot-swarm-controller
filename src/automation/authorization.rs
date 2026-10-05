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

type ScriptSubmissionRunLinkRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
);

type ScriptEventRunLinkRow = (
    String,
    String,
    Option<String>,
    Option<String>,
    String,
    String,
    String,
    String,
    i64,
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
    String,
);

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

/// A closed, DB-derived authority for one manager-owned cron CheckRun.
/// Callers cannot deserialize or construct this context from request JSON;
/// the exact current AutomationEntry and logical occurrence are revalidated
/// again in the admission transaction.
#[derive(Debug, Clone)]
pub(crate) struct CronExecutionContext {
    entry: config::AutomationEntry,
    occurrence_id: String,
    cause: AutomationCause,
}

/// A direct Manager invocation of an entry's saved CheckRun action. Unlike a
/// CronExecutionContext this carries no calendar generation or due slot and
/// deliberately does not require `entry.enabled`.
#[derive(Debug, Clone)]
pub(crate) struct ManualCheckRunContext {
    entry: config::AutomationEntry,
    manager_id: String,
    check_request_id: String,
}

type ManualCheckRunSubjectRow = (
    String,
    i64,
    Option<i64>,
    String,
    String,
    i64,
    Option<String>,
);

pub(crate) fn manual_run_now_check_request_id(
    manager_id: &str,
    client_request_id: &str,
    project_id: &str,
    automation_id: &str,
) -> Result<String> {
    let identity = json!({
        "schema_version":1,
        "method":"schedule.run_now",
        "manager_id":manager_id,
        "client_request_id":client_request_id,
        "project_id":project_id,
        "automation_id":automation_id,
    });
    Ok(format!(
        "manual-{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

impl ManualCheckRunContext {
    pub(crate) fn from_committed_entry(
        db: &Connection,
        principal: &Principal,
        project_id: &str,
        automation_id: &str,
        client_request_id: &str,
    ) -> Result<Self> {
        if principal.role != Role::Manager {
            return Err(Error::new(
                "FORBIDDEN",
                "schedule.run_now requires the owning Manager",
            ));
        }
        require_registered_manager(db, &principal.client_id)?;
        let entry = config::load_entry(db, &principal.client_id, project_id, automation_id)?
            .ok_or_else(|| Error::new("AUTOMATION_NOT_FOUND", "automation entry was not found"))?;
        config::validate_entry(&entry)?;
        if !manual_check_run_selected(&entry) {
            return Err(Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "entry has no supported saved CheckRun action",
            ));
        }
        let context = Self {
            check_request_id: manual_run_now_check_request_id(
                &principal.client_id,
                client_request_id,
                project_id,
                automation_id,
            )?,
            manager_id: principal.client_id.clone(),
            entry,
        };
        context.require_current_check_target(db, principal)?;
        Ok(context)
    }

    pub(crate) fn request_params(&self) -> Result<Value> {
        let settings = self.entry.cron.as_ref().ok_or_else(|| {
            Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "saved CheckRun settings are missing",
            )
        })?;
        match &settings.action {
            crate::scheduler::ScheduleAction::CheckRun {
                attempt_id,
                candidate_ref,
                profile_id,
                profile_revision,
                ..
            } => Ok(json!({
                "client_request_id":self.check_request_id,
                "attempt_id":attempt_id,
                "candidate_ref":candidate_ref,
                "profile_id":profile_id,
                "profile_revision":profile_revision,
            })),
        }
    }

    /// Recheck the exact saved action and pinned source target before the
    /// normal direct `check.run` grant and reservation checks execute.
    pub(crate) fn require_current_check_target(
        &self,
        db: &Connection,
        principal: &Principal,
    ) -> Result<()> {
        if principal.role != Role::Manager || principal.client_id != self.manager_id {
            return Err(Error::new(
                "FORBIDDEN",
                "manual CheckRun actor is not the authenticated automation owner",
            ));
        }
        let current = config::load_entry(
            db,
            &self.manager_id,
            &self.entry.project_id,
            &self.entry.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        if current != self.entry || !manual_check_run_selected(&current) {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "current automation no longer has this exact manual CheckRun action",
            ));
        }
        require_registered_manager(db, &self.manager_id)?;
        let crate::scheduler::ScheduleAction::CheckRun {
            attempt_id,
            expected_task_revision,
            candidate_ref,
            ..
        } = &current
            .cron
            .as_ref()
            .expect("selected CheckRun has settings")
            .action;
        let subject: Option<ManualCheckRunSubjectRow> = db
            .query_row(
                "SELECT a.task_id,a.task_revision,a.released_at_ms,t.project_id,t.state,t.revision,\
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
                    ))
                },
            )
            .optional()?;
        let Some((
            task_id,
            attempt_revision,
            released_at_ms,
            project_id,
            task_state,
            task_revision,
            current_attempt,
        )) = subject
        else {
            return Err(Error::new(
                "AUTOMATION_ATTEMPT_STALE",
                "manual CheckRun Attempt was not found",
            ));
        };
        if attempt_revision != *expected_task_revision
            || task_revision != *expected_task_revision
            || current_attempt.as_deref() != Some(attempt_id.as_str())
            || released_at_ms.is_some()
            || task_state != "open"
            || project_id != current.project_id
        {
            return Err(Error::new(
                "AUTOMATION_ATTEMPT_STALE",
                "manual CheckRun no longer targets the exact current open Attempt revision",
            ));
        }
        let artifact: Option<(String, String)> = db
            .query_row(
                "SELECT kind,metadata_json FROM artifacts WHERE artifact_id=?1",
                [candidate_ref],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((kind, metadata_json)) = artifact else {
            return Err(Error::new(
                "CHECK_SOURCE_REQUIRED",
                "manual CheckRun source snapshot is not registered",
            ));
        };
        let metadata: Value = serde_json::from_str(&metadata_json)?;
        if kind != "source_snapshot"
            || metadata["task_id"] != task_id
            || metadata["attempt_id"] != *attempt_id
            || metadata["task_revision"] != *expected_task_revision
        {
            return Err(Error::new(
                "CHECK_SOURCE_REQUIRED",
                "manual CheckRun source does not match the exact current Attempt",
            ));
        }
        Ok(())
    }
}

fn manual_check_run_selected(entry: &config::AutomationEntry) -> bool {
    entry.steps.contains(&AutomationStep::CheckRun)
        && entry.cron.is_some()
        && entry.scope.work_pool_id.is_none()
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

impl CronExecutionContext {
    pub(crate) fn from_committed_entry(
        db: &Connection,
        entry: &config::AutomationEntry,
        calendar_generation: &str,
        occurrence_id: &str,
        due_at_ms: i64,
    ) -> Result<Self> {
        config::validate_entry(entry)?;
        if !entry.check_run_ready() {
            return Err(Error::new(
                "AUTOMATION_ACTION_UNAVAILABLE",
                "entry does not currently admit check_run",
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
        if current != *entry {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "cron execution requires the exact current committed entry",
            ));
        }
        let settings = entry.cron.as_ref().ok_or_else(|| {
            Error::new("AUTOMATION_ACTION_UNAVAILABLE", "cron settings are missing")
        })?;
        let generation = crate::scheduler::calendar::generation_digest(&settings.calendar)?;
        if generation != calendar_generation {
            return Err(Error::new(
                "AUTOMATION_ACTION_CHANGED",
                "calendar generation differs from the committed entry",
            ));
        }
        let lineage = config::transfer_lineage(
            db,
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
        )?;
        let origin_manager_id = lineage
            .last()
            .map(|transfer| transfer.former_owner_manager_id.clone())
            .unwrap_or_else(|| entry.owner_manager_id.clone());
        let expected_occurrence = crate::scheduler::calendar::occurrence_id(
            &origin_manager_id,
            &entry.project_id,
            &entry.automation_id,
            &generation,
            due_at_ms,
        )?;
        if expected_occurrence != occurrence_id {
            return Err(Error::new(
                "AUTOMATION_OCCURRENCE_INVALID",
                "cron occurrence identity does not match its committed entry and due time",
            ));
        }
        let action = settings.action.clone();
        let cause = match action {
            crate::scheduler::ScheduleAction::CheckRun {
                attempt_id,
                expected_task_revision,
                candidate_ref,
                profile_id,
                profile_revision,
            } => AutomationCause::CronOccurrence {
                occurrence_id: occurrence_id.to_owned(),
                calendar_generation: generation.clone(),
                due_at_ms,
                task_id: cron_task_id(db, &attempt_id)?,
                attempt_id,
                task_revision: expected_task_revision,
                candidate_ref,
                profile_id,
                profile_revision,
            },
        };
        Ok(Self {
            entry: entry.clone(),
            occurrence_id: occurrence_id.to_owned(),
            cause,
        })
    }

    pub(crate) fn request_params(&self) -> Result<Value> {
        let AutomationCause::CronOccurrence {
            attempt_id,
            candidate_ref,
            profile_id,
            profile_revision,
            ..
        } = &self.cause
        else {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "cron execution context has an unsupported cause",
            ));
        };
        Ok(json!({
            "client_request_id":format!("cron-{}", self.occurrence_id),
            "attempt_id":attempt_id,
            "candidate_ref":candidate_ref,
            "profile_id":profile_id,
            "profile_revision":profile_revision,
        }))
    }

    pub(crate) fn cause_value(&self) -> Value {
        self.cause.as_json()
    }

    /// Revalidate both the manager-owned action and the exact current
    /// CheckRun subject at the same SQLite boundary as Operation admission.
    pub(crate) fn require_current_check_target(&self, db: &Connection) -> Result<()> {
        let current = config::load_entry(
            db,
            &self.entry.owner_manager_id,
            &self.entry.project_id,
            &self.entry.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?;
        if current != self.entry || !current.check_run_ready() {
            return Err(Error::new(
                "FORBIDDEN",
                "current automation settings no longer permit this exact CheckRun",
            ));
        }
        require_registered_manager(db, &current.owner_manager_id)?;
        let AutomationCause::CronOccurrence {
            task_id,
            attempt_id,
            task_revision,
            candidate_ref,
            ..
        } = &self.cause
        else {
            return Err(Error::new(
                "AUTOMATION_LINK_CORRUPT",
                "cron execution context has an unsupported cause",
            ));
        };
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
            _attempt_state,
            released_at_ms,
            _submission_ref,
            _attempt_candidate_ref,
            project_id,
            task_state,
            current_task_revision,
            current_attempt_id,
        )) = subject
        else {
            return Err(Error::new(
                "AUTOMATION_ATTEMPT_STALE",
                "cron CheckRun Attempt was not found",
            ));
        };
        if attempt_task_id != *task_id
            || attempt_revision != *task_revision
            || current_task_revision != *task_revision
            || current_attempt_id.as_deref() != Some(attempt_id.as_str())
            || project_id != current.project_id
            || task_state != "open"
            || released_at_ms.is_some()
        {
            return Err(Error::new(
                "AUTOMATION_ATTEMPT_STALE",
                "cron CheckRun no longer targets the exact current open Attempt",
            ));
        }
        let artifact: Option<(String, String)> = db
            .query_row(
                "SELECT kind,metadata_json FROM artifacts WHERE artifact_id=?1",
                [candidate_ref],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((kind, metadata_json)) = artifact else {
            return Err(Error::new(
                "CHECK_SOURCE_REQUIRED",
                "cron CheckRun source snapshot is not registered",
            ));
        };
        let metadata: Value = serde_json::from_str(&metadata_json)?;
        if kind != "source_snapshot"
            || metadata["task_id"] != *task_id
            || metadata["attempt_id"] != *attempt_id
            || metadata["task_revision"] != *task_revision
        {
            return Err(Error::new(
                "CHECK_SOURCE_REQUIRED",
                "cron CheckRun source snapshot does not match the exact current Attempt",
            ));
        }

        // A transferred entry acting for an Attempt from its sealed lineage
        // needs the exact transfer grant. This path deliberately does not
        // rewrite the Attempt owner or fall back to broad GM ownership.
        let lineage = config::transfer_lineage(
            db,
            &current.owner_manager_id,
            &current.project_id,
            &current.automation_id,
        )?;
        let attempt_is_transferred_origin = lineage
            .iter()
            .any(|transfer| transfer.former_owner_manager_id == attempt_owner_id);
        if attempt_owner_id != current.owner_manager_id && attempt_is_transferred_origin {
            current_transferred_attempt_authority(
                db,
                &current,
                AutomationStep::CheckRun,
                task_id,
                *task_revision,
                attempt_id,
                "",
                candidate_ref,
            )?
            .ok_or_else(|| {
                Error::new(
                    "FORBIDDEN",
                    "transferred cron CheckRun lacks exact successor authority",
                )
            })?;
        } else if !current_manager_id_has_task_scope(
            db,
            &current.owner_manager_id,
            task_id,
            &current.project_id,
        )? {
            return Err(Error::new(
                "FORBIDDEN",
                "automation owner lacks current scope for the exact CheckRun Task",
            ));
        }
        Ok(())
    }
}

fn cron_task_id(db: &Connection, attempt_id: &str) -> Result<String> {
    db.query_row(
        "SELECT task_id FROM attempts WHERE attempt_id=?1",
        [attempt_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_ATTEMPT_STALE",
            "cron CheckRun Attempt was not found",
        )
    })
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
    CronCheckRun(OnBehalfOperationLink),
    GoalProgression(OnBehalfOperationLink),
    ScriptRun(OnBehalfOperationLink),
    ScriptEffect(OnBehalfOperationLink),
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

    pub(crate) fn owner_lineage(&self) -> &[String] {
        &self.owner_lineage
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
            Self::CronCheckRun(link) => link.belongs_to(principal),
            Self::GoalProgression(link) => link.belongs_to(principal),
            Self::ScriptRun(link) => link.belongs_to(principal),
            Self::ScriptEffect(link) => link.belongs_to(principal),
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
        ("check.run", Some("cron_occurrence")) => "check.run",
        ("agent.goal", Some("goal_progression")) => "agent.goal",
        ("script.run", Some("applied_submission")) => "script.run",
        ("script.run", Some("system_event")) => "script.run",
        ("message.send", Some("script_controller_effect")) => "message.send",
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
    } else if link.action == "check.run" {
        validate_cron_check_run_link(db, &link)?;
    } else if link.action == "agent.goal" {
        crate::store::automation_goal_progression::validate_operation_link(db, &link)?;
    } else if link.action == "script.run" {
        validate_script_run_operation_link(db, &link)?;
    } else if link.action == "message.send" {
        validate_script_controller_effect_operation_link(db, &link)?;
    }
    Ok(Some(link))
}

struct ScriptControllerEffectOperationRow {
    caller_id: String,
    method: String,
    client_request_id: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    original_request_json: String,
    effective_request_json: String,
    state: String,
    settled_at_ms: Option<i64>,
    result_json: Option<String>,
}

struct ScriptControllerEffectParentRow {
    caller_id: String,
    method: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    state: String,
    result_json: Option<String>,
    run_state: String,
    script_id: String,
    script_revision: i64,
    bundle_ref: String,
    run_task_id: Option<String>,
    run_task_revision: Option<i64>,
    run_attempt_id: Option<String>,
    spec_json: String,
}

/// Validate the retained child effect against its completed ScriptRun. This
/// confirms provenance only; current Manager and exact subject rights remain a
/// separate read/admission check.
fn validate_script_controller_effect_operation_link(
    db: &Connection,
    link: &OnBehalfOperationLink,
) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "ScriptRun effect link does not match its exact retained child Operation",
        )
    };
    const CAUSE_FIELDS: &[&str] = &[
        "kind",
        "id",
        "script_run_operation_id",
        "script_run_id",
        "script_id",
        "script_revision",
        "effect",
        "task_id",
        "task_revision",
        "attempt_id",
        "recipient",
        "request_sha256",
        "effective_manager_id",
    ];
    let cause = &link.cause;
    let Some(cause_object) = cause.as_object() else {
        return Err(corrupt());
    };
    if cause_object.len() != CAUSE_FIELDS.len()
        || CAUSE_FIELDS
            .iter()
            .any(|field| !cause_object.contains_key(*field))
        || link.action != "message.send"
        || cause["kind"] != "script_controller_effect"
        || cause["effect"] != "task_owner_message"
        || cause["effective_manager_id"] != link.effective_manager_id
    {
        return Err(corrupt());
    }
    let parent_operation_id = cause["script_run_operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let run_id = cause["script_run_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let script_id = cause["script_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let script_revision = cause["script_revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let task_id = cause["task_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let task_revision = cause["task_revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let attempt_id = cause["attempt_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let recipient = cause["recipient"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let request_id = cause["id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let expected_request_id = format!(
        "script-effect-{}",
        model::digest(
            model::canonical(&json!([
                "script-effect-v1",
                parent_operation_id,
                run_id,
                "task_owner_message"
            ]))?
            .as_bytes()
        )
    );
    if request_id != expected_request_id {
        return Err(corrupt());
    }

    let child: Option<ScriptControllerEffectOperationRow> = db
        .query_row(
            "SELECT caller_id,method,client_request_id,task_id,attempt_id,original_request_json,effective_request_json,
                    state,settled_at_ms,result_json
             FROM operations WHERE operation_id=?1",
            [&link.operation_id],
            |row| {
                Ok(ScriptControllerEffectOperationRow {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    client_request_id: row.get(2)?,
                    task_id: row.get(3)?,
                    attempt_id: row.get(4)?,
                    original_request_json: row.get(5)?,
                    effective_request_json: row.get(6)?,
                    state: row.get(7)?,
                    settled_at_ms: row.get(8)?,
                    result_json: row.get(9)?,
                })
            },
        )
        .optional()?;
    let Some(child) = child else {
        return Err(corrupt());
    };
    let original: Value =
        serde_json::from_str(&child.original_request_json).map_err(|_| corrupt())?;
    if child.caller_id != AUTOMATION_TECHNICAL_REQUESTER_ID
        || child.method != "message.send"
        || child.client_request_id != request_id
        || child.task_id.is_some()
        || child.attempt_id.is_some()
        || original.as_object().is_none_or(|object| object.len() != 3)
        || original["client_request_id"] != request_id
        || original["recipient"] != recipient
        || original["text"].as_str().is_none_or(str::is_empty)
        || model::digest(model::canonical(&original)?.as_bytes())
            != cause["request_sha256"].as_str().ok_or_else(corrupt)?
    {
        return Err(corrupt());
    }

    let parent_link = operation_link(db, parent_operation_id)?.ok_or_else(corrupt)?;
    if parent_link.action != "script.run"
        || parent_link.technical_requester_id != link.technical_requester_id
        || parent_link.effective_manager_id != link.effective_manager_id
        || parent_link.automation_id != link.automation_id
        || parent_link.automation_revision != link.automation_revision
        || parent_link.project_id != link.project_id
    {
        return Err(corrupt());
    }
    let parent: Option<ScriptControllerEffectParentRow> = db
        .query_row(
            "SELECT o.caller_id,o.method,o.task_id,o.attempt_id,o.state,o.result_json,
                    r.state,r.script_id,r.revision,r.bundle_ref,r.task_id,r.task_revision,r.attempt_id,r.spec_json
             FROM operations AS o JOIN script_runs AS r ON r.operation_id=o.operation_id
             WHERE o.operation_id=?1 AND r.run_id=?2",
            params![parent_operation_id, run_id],
            |row| {
                Ok(ScriptControllerEffectParentRow {
                    caller_id: row.get(0)?,
                    method: row.get(1)?,
                    task_id: row.get(2)?,
                    attempt_id: row.get(3)?,
                    state: row.get(4)?,
                    result_json: row.get(5)?,
                    run_state: row.get(6)?,
                    script_id: row.get(7)?,
                    script_revision: row.get(8)?,
                    bundle_ref: row.get(9)?,
                    run_task_id: row.get(10)?,
                    run_task_revision: row.get(11)?,
                    run_attempt_id: row.get(12)?,
                    spec_json: row.get(13)?,
                })
            },
        )
        .optional()?;
    let Some(parent) = parent else {
        return Err(corrupt());
    };
    let parent_result: Value = parent
        .result_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| corrupt())?
        .ok_or_else(corrupt)?;
    let spec: Value = serde_json::from_str(&parent.spec_json).map_err(|_| corrupt())?;
    let declared_effects: Vec<crate::scripts::manifest::ScriptControllerEffect> =
        serde_json::from_value(spec["capabilities"].clone()).map_err(|_| corrupt())?;
    let bundle = crate::scripts::registry::bundle_record(db, script_id, script_revision)
        .map_err(|_| corrupt())?;
    let bundle_effects: Vec<crate::scripts::manifest::ScriptControllerEffect> =
        serde_json::from_value(
            bundle
                .metadata
                .get("controller_effects")
                .cloned()
                .unwrap_or_else(|| json!([])),
        )
        .map_err(|_| corrupt())?;
    if parent.caller_id != AUTOMATION_TECHNICAL_REQUESTER_ID
        || parent.method != "script.run"
        || parent.state != "settled"
        || parent.run_state != "completed"
        || parent.task_id.as_deref() != Some(task_id)
        || parent.attempt_id.as_deref() != Some(attempt_id)
        || parent.script_id != script_id
        || parent.script_revision != script_revision
        || bundle.kind != crate::scripts::registry::BUNDLE_KIND
        || bundle.artifact_id != parent.bundle_ref
        || declared_effects != bundle_effects
        || declared_effects
            != vec![crate::scripts::manifest::ScriptControllerEffect::TaskOwnerMessage]
        || parent.run_task_id.as_deref() != Some(task_id)
        || parent.run_task_revision != Some(task_revision)
        || parent.run_attempt_id.as_deref() != Some(attempt_id)
        || spec["invocation"]["operation_id"] != parent_operation_id
        || spec["invocation"]["run_id"] != run_id
        || spec["invocation"]["script_id"] != script_id
        || spec["invocation"]["script_revision"] != script_revision
        || spec["invocation"]["task_id"] != task_id
        || spec["invocation"]["task_revision"] != task_revision
        || spec["invocation"]["attempt_id"] != attempt_id
        || spec["invocation"]["effective_manager_id"] != link.effective_manager_id
        || spec["automation_on_behalf"] != parent_link.value().map_err(|_| corrupt())?
    {
        return Err(corrupt());
    }

    let expected_invocation_cause = json!({
        "kind":"script_invocation",
        "id":parent_operation_id,
        "script_run_operation_id":parent_operation_id,
        "script_run_id":run_id,
        "identity":{
            "script_id":script_id,
            "script_revision":script_revision,
            "task_id":task_id,
            "task_revision":task_revision,
            "attempt_id":attempt_id,
        },
        "effective_manager_id":link.effective_manager_id,
    });
    let effective: Value =
        serde_json::from_str(&child.effective_request_json).map_err(|_| corrupt())?;
    let expected_invocation_link = json!({
        "schema_version":1,
        "operation_id":link.operation_id,
        "technical_requester_id":link.technical_requester_id,
        "effective_manager_id":link.effective_manager_id,
        "action":"message.send",
        "grant":"task_owner_message",
        "cause":expected_invocation_cause,
    });
    let expected_link_value = link.value().map_err(|_| corrupt())?;
    if effective["automation_on_behalf"] != expected_link_value
        || effective["script_invocation"] != expected_invocation_link
    {
        return Err(corrupt());
    }
    let index_key = config::entry_operation_key(
        &link.effective_manager_id,
        &link.project_id,
        &link.automation_id,
        &link.operation_id,
    )?;
    if config::read_record(db, &index_key, "ScriptRun effect entry index")?.as_ref()
        != Some(&expected_link_value)
    {
        return Err(corrupt());
    }

    let effect = parent_result["controller_effects"]
        .as_array()
        .filter(|effects| effects.len() == 1)
        .and_then(|effects| effects.first())
        .ok_or_else(corrupt)?;
    if parent_result["operation_id"] != parent_operation_id
        || parent_result["run_id"] != run_id
        || parent_result["state"] != "completed"
        || effect["effect"] != "task_owner_message"
        || effect["operation_id"] != link.operation_id
        || effect["recipient"] != recipient
        || effect["cause"] != expected_invocation_cause
    {
        return Err(corrupt());
    }
    let child_result: Value = child
        .result_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| corrupt())?
        .ok_or_else(corrupt)?;
    let receipt = &effective["receipt"];
    match effect["status"].as_str() {
        Some("applied")
            if child.state == "settled"
                && child.settled_at_ms.is_some()
                && receipt["ok"] == true
                && child_result == receipt["value"]
                && child_result["operation_id"] == link.operation_id
                && child_result["message_id"] == link.operation_id
                && child_result["sender"] == link.effective_manager_id
                && child_result["recipient"] == recipient
                && child_result["text"] == original["text"]
                && effect["message_id"] == link.operation_id
                && effect.get("error").is_none() =>
        {
            Ok(())
        }
        Some("rejected")
            if child.state == "rejected"
                && child.settled_at_ms.is_some()
                && receipt["ok"] == false
                && child_result == receipt["error"]
                && effect["error"]["code"] == child_result["code"]
                && effect["error"]["message"] == child_result["message"]
                && effect.get("message_id").is_none() =>
        {
            Ok(())
        }
        _ => Err(corrupt()),
    }
}

/// Retain exact automation, submission, Task, Attempt, script and immutable
/// revision attribution for the ordinary queued `script.run` Operation.
pub(crate) fn save_script_run_operation_link(
    db: &Connection,
    operation_id: &str,
    entry: &config::AutomationEntry,
    cause: &Value,
    now_ms: i64,
) -> Result<Value> {
    let selected_script = entry
        .script_run
        .as_ref()
        .map(|settings| settings.script_id.as_str());
    let cause_kind = cause["kind"].as_str().unwrap_or_default();
    let cause_is_selected = match cause_kind {
        "applied_submission" => cause["id"].as_str().is_some_and(|id| !id.is_empty()),
        "system_event" => {
            let status = super::event_rules::EventStatus::parse_optional(cause["status"].as_str())?;
            cause["source_id"]
                .as_str()
                .zip(cause["event_kind"].as_str())
                .is_some_and(|(source_id, event_kind)| {
                    entry.accepts_script_run_event(source_id, event_kind, status)
                })
                && cause["observation_id"].as_i64().is_some_and(|id| id > 0)
                && cause["recorded_at_ms"]
                    .as_i64()
                    .is_some_and(|time| time >= 0)
                && cause["id"].as_str().is_some_and(|id| !id.is_empty())
        }
        _ => false,
    };
    if !entry.script_run_ready()
        || !cause_is_selected
        || cause["script_id"].as_str() != selected_script
    {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "ScriptRun trigger no longer admits this exact invocation",
        ));
    }
    require_registered_manager(db, &entry.owner_manager_id)?;
    let current = config::load_entry(
        db,
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?
    .ok_or_else(|| {
        Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "ScriptRun automation disappeared",
        )
    })?;
    if current.revision != entry.revision
        || !current.script_run_ready()
        || current.script_run != entry.script_run
        || current.event_rules != entry.event_rules
    {
        return Err(Error::new(
            "AUTOMATION_ACTION_CHANGED",
            "current automation no longer selects this ScriptRun target",
        ));
    }
    let link = OnBehalfOperationLink {
        schema_version: 1,
        operation_id: operation_id.to_owned(),
        technical_requester_id: AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
        effective_manager_id: entry.owner_manager_id.clone(),
        automation_id: entry.automation_id.clone(),
        automation_revision: entry.revision,
        project_id: entry.project_id.clone(),
        action: "script.run".to_owned(),
        cause: cause.clone(),
        linked_at_ms: now_ms,
    };
    let value = link.value()?;
    config::write_record(db, &config::operation_link_key(operation_id)?, &value)?;
    config::write_record(
        db,
        &config::entry_operation_key(
            &entry.owner_manager_id,
            &entry.project_id,
            &entry.automation_id,
            operation_id,
        )?,
        &value,
    )?;
    Ok(json!({
        "technical_requester_id":AUTOMATION_TECHNICAL_REQUESTER_ID,
        "effective_manager_id":entry.owner_manager_id,
        "automation_id":entry.automation_id,
        "automation_revision":entry.revision,
        "project_id":entry.project_id,
        "action":"script.run",
        "semantic_cause_kind":cause_kind,
        "semantic_cause_id":cause["id"],
        "cause":cause
    }))
}

/// Stable semantic request identity shared by staging and normal `script.run`
/// admission. Observation IDs are intentionally excluded so repeated intake
/// receipts for the same applied submission cannot request a second run.
pub(crate) fn script_run_request_id(
    automation_id: &str,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    submission_ref: &str,
    script_id: &str,
) -> Result<String> {
    let identity = json!({
        "automation_id":automation_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "submission_ref":submission_ref,
        "script_id":script_id,
        "action":"script.run"
    });
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

/// Stable semantic request identity for one exact normalized occurrence (or
/// metadata-only observation when no typed occurrence adapter exists).
pub(crate) fn script_run_event_request_id(
    automation_id: &str,
    semantic_event_id: &str,
    script_id: &str,
) -> Result<String> {
    let identity = json!({
        "automation_id":automation_id,
        "semantic_event_id":semantic_event_id,
        "script_id":script_id,
        "action":"script.run"
    });
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

pub(crate) fn script_run_causes_semantically_match(left: &Value, right: &Value) -> bool {
    if left["kind"] == "system_event" || right["kind"] == "system_event" {
        if left["kind"] != "system_event" || right["kind"] != "system_event" {
            return false;
        }
        let same_occurrence = if left["occurrence_phase"].as_str().is_some()
            && left["occurrence_id"].as_str().is_some()
            && right["occurrence_phase"].as_str().is_some()
            && right["occurrence_id"].as_str().is_some()
        {
            left["occurrence_phase"] == right["occurrence_phase"]
                && left["occurrence_id"] == right["occurrence_id"]
        } else {
            left["observation_id"] == right["observation_id"]
                && left["source_id"] == right["source_id"]
                && left["event_kind"] == right["event_kind"]
        };
        let same_scope = ["task_id", "task_revision", "attempt_id"]
            .iter()
            .all(|key| left[*key] == right[*key]);
        let same_script_run = match (
            left["script_run_id"].as_str(),
            right["script_run_id"].as_str(),
        ) {
            (Some(left), Some(right)) => left == right,
            // Older retained causes predate the optional exact Run reference.
            _ => true,
        };
        return left["id"].as_str().is_some_and(|id| !id.is_empty())
            && right["id"].as_str().is_some_and(|id| !id.is_empty())
            && left["id"] == right["id"]
            && left["script_id"] == right["script_id"]
            && left["status"] == right["status"]
            && left["error_code"] == right["error_code"]
            && left["failure_category"] == right["failure_category"]
            && left["failed_supervisor"] == right["failed_supervisor"]
            && left["operation_id"] == right["operation_id"]
            && same_occurrence
            && same_scope
            && same_script_run;
    }
    const KEYS: &[&str] = &[
        "kind",
        "operation_id",
        "id",
        "script_id",
        "task_id",
        "task_revision",
        "attempt_id",
        "candidate_ref",
    ];
    left["observation_id"].as_i64().is_some_and(|id| id > 0)
        && right["observation_id"].as_i64().is_some_and(|id| id > 0)
        && KEYS.iter().all(|key| left[*key] == right[*key])
}

fn validate_script_run_operation_link(db: &Connection, link: &OnBehalfOperationLink) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "ScriptRun link does not match its retained request and script run",
        )
    };
    let cause = &link.cause;
    if cause["kind"] == "system_event" {
        return validate_script_event_run_operation_link(db, link);
    }
    let observation_id = cause["observation_id"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let operation_id = cause["operation_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let submission_ref = cause["id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let script_id = cause["script_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let script_revision = cause["script_revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let task_id = cause["task_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let task_revision = cause["task_revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let attempt_id = cause["attempt_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let candidate_ref = cause["candidate_ref"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    if cause["kind"] != "applied_submission" || link.action != "script.run" {
        return Err(corrupt());
    }
    let row: Option<ScriptSubmissionRunLinkRow> = db
        .query_row(
            "SELECT o.caller_id,o.method,o.task_id,o.attempt_id,o.client_request_id,\
                o.original_request_json,o.effective_request_json,r.script_id,r.revision,\
                r.bundle_ref,r.task_id,r.task_revision,r.attempt_id \
         FROM operations o JOIN script_runs r ON r.operation_id=o.operation_id \
         WHERE o.operation_id=?1",
            [&link.operation_id],
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
                    row.get(11)?,
                    row.get(12)?,
                ))
            },
        )
        .optional()?;
    let Some((
        caller,
        method,
        operation_task,
        operation_attempt,
        request_id,
        original_json,
        effective_json,
        stored_script,
        stored_revision,
        bundle_ref,
        stored_task,
        stored_task_revision,
        stored_attempt,
    )) = row
    else {
        return Err(corrupt());
    };
    let original: Value = serde_json::from_str(&original_json).map_err(|_| corrupt())?;
    let effective: Value = serde_json::from_str(&effective_json).map_err(|_| corrupt())?;
    let input = json!({
        "kind":"task.submission.applied",
        "submission_ref":submission_ref,
        "operation_id":operation_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "candidate_ref":candidate_ref
    });
    let expected_request_id = model::digest(
        model::canonical(&json!({
            "automation_id":link.automation_id,
            "task_id":task_id,
            "task_revision":task_revision,
            "attempt_id":attempt_id,
            "submission_ref":submission_ref,
            "script_id":script_id,
            "action":"script.run"
        }))?
        .as_bytes(),
    );
    let expected_linkage = json!({
        "technical_requester_id":AUTOMATION_TECHNICAL_REQUESTER_ID,
        "effective_manager_id":link.effective_manager_id,
        "automation_id":link.automation_id,
        "automation_revision":link.automation_revision,
        "project_id":link.project_id,
        "action":"script.run",
        "semantic_cause_kind":"applied_submission",
        "semantic_cause_id":submission_ref,
        "cause":cause
    });
    if caller != AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "script.run"
        || operation_task.as_deref() != Some(task_id)
        || operation_attempt.as_deref() != Some(attempt_id)
        || request_id != expected_request_id
        || original["client_request_id"] != expected_request_id
        || original["script_id"] != script_id
        || original["expected_script_revision"] != script_revision
        || original["attempt_id"] != attempt_id
        || original["expected_task_revision"] != task_revision
        || original["input"] != input
        || effective["automation_on_behalf"] != expected_linkage
        || effective["script_run"]["bundle_ref"] != bundle_ref
        || stored_script != script_id
        || stored_revision != script_revision
        || stored_task.as_deref() != Some(task_id)
        || stored_task_revision != Some(task_revision)
        || stored_attempt.as_deref() != Some(attempt_id)
        || cause["operation_id"] != operation_id
        || observation_id <= 0
    {
        return Err(corrupt());
    }
    let task_project: Option<String> = db
        .query_row(
            "SELECT project_id FROM tasks WHERE task_id=?1",
            [task_id],
            |row| row.get(0),
        )
        .optional()?;
    if task_project.as_deref() != Some(link.project_id.as_str()) {
        return Err(corrupt());
    }
    Ok(())
}

fn validate_script_event_run_operation_link(
    db: &Connection,
    link: &OnBehalfOperationLink,
) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "ScriptRun event link does not match its retained request and invocation",
        )
    };
    let cause = &link.cause;
    let semantic_event_id = cause["id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(corrupt)?;
    let script_id = cause["script_id"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(corrupt)?;
    let script_revision = cause["script_revision"]
        .as_i64()
        .filter(|revision| *revision > 0)
        .ok_or_else(corrupt)?;
    if link.action != "script.run" {
        return Err(corrupt());
    }
    let expected_request_id =
        script_run_event_request_id(&link.automation_id, semantic_event_id, script_id)?;
    let input = crate::store::automation_dispatch::validate_retained_script_event_cause(
        db,
        &link.project_id,
        cause,
    )
    .map_err(|_| corrupt())?;
    let row: Option<ScriptEventRunLinkRow> = db
        .query_row(
            "SELECT o.caller_id,o.method,o.task_id,o.attempt_id,o.client_request_id,\
                o.original_request_json,o.effective_request_json,r.script_id,r.revision,\
                r.bundle_ref,r.task_id,r.task_revision,r.attempt_id,r.spec_json \
         FROM operations o JOIN script_runs r ON r.operation_id=o.operation_id \
         WHERE o.operation_id=?1",
            [&link.operation_id],
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
                    row.get(11)?,
                    row.get(12)?,
                    row.get(13)?,
                ))
            },
        )
        .optional()?;
    let Some((
        caller,
        method,
        operation_task,
        operation_attempt,
        request_id,
        original_json,
        effective_json,
        stored_script,
        stored_revision,
        bundle_ref,
        stored_task,
        stored_task_revision,
        stored_attempt,
        spec_json,
    )) = row
    else {
        return Err(corrupt());
    };
    let original: Value = serde_json::from_str(&original_json).map_err(|_| corrupt())?;
    let effective: Value = serde_json::from_str(&effective_json).map_err(|_| corrupt())?;
    let spec: Value = serde_json::from_str(&spec_json).map_err(|_| corrupt())?;
    let expected_linkage = json!({
        "technical_requester_id":AUTOMATION_TECHNICAL_REQUESTER_ID,
        "effective_manager_id":link.effective_manager_id,
        "automation_id":link.automation_id,
        "automation_revision":link.automation_revision,
        "project_id":link.project_id,
        "action":"script.run",
        "semantic_cause_kind":"system_event",
        "semantic_cause_id":semantic_event_id,
        "cause":cause
    });
    let task_id = cause["task_id"].as_str();
    let task_revision = cause["task_revision"].as_i64();
    let attempt_id = cause["attempt_id"].as_str();
    let task_scope_valid = match (task_id, task_revision, attempt_id) {
        (None, None, None) => true,
        (Some(task_id), Some(task_revision), Some(attempt_id)) => {
            !task_id.is_empty() && task_revision > 0 && !attempt_id.is_empty()
        }
        _ => false,
    };
    let expected_capabilities = if task_id.is_some() {
        spec["capabilities"].clone()
    } else {
        json!([])
    };
    if caller != AUTOMATION_TECHNICAL_REQUESTER_ID
        || method != "script.run"
        || operation_task.as_deref() != task_id
        || operation_attempt.as_deref() != attempt_id
        || request_id != expected_request_id
        || original["client_request_id"] != expected_request_id
        || original["script_id"] != script_id
        || original["expected_script_revision"] != script_revision
        || original["attempt_id"].as_str() != attempt_id
        || original["expected_task_revision"].as_i64() != task_revision
        || original["input"] != input
        || effective["automation_on_behalf"] != expected_linkage
        || effective["script_run"]["bundle_ref"] != bundle_ref
        || stored_script != script_id
        || stored_revision != script_revision
        || stored_task.as_deref() != task_id
        || stored_task_revision != task_revision
        || stored_attempt.as_deref() != attempt_id
        || spec["invocation"]["operation_id"] != link.operation_id
        || spec["invocation"]["script_id"] != script_id
        || spec["invocation"]["script_revision"] != script_revision
        || spec["invocation"]["task_id"].as_str() != task_id
        || spec["invocation"]["task_revision"].as_i64() != task_revision
        || spec["invocation"]["attempt_id"].as_str() != attempt_id
        || spec["invocation"]["effective_manager_id"] != link.effective_manager_id
        || spec["automation_on_behalf"] != expected_linkage
        || spec["capabilities"] != expected_capabilities
        || !task_scope_valid
        || task_id.is_none() && spec["capabilities"] != json!([])
    {
        return Err(corrupt());
    }
    Ok(())
}

fn validate_cron_check_run_link(db: &Connection, link: &OnBehalfOperationLink) -> Result<()> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "cron CheckRun link does not match its retained request and check record",
        )
    };
    let cause = &link.cause;
    let occurrence_id = cause["id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let generation = cause["calendar_generation"]
        .as_str()
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(corrupt)?;
    let due_at_ms = cause["due_at_ms"]
        .as_i64()
        .filter(|value| *value >= 0)
        .ok_or_else(corrupt)?;
    let task_id = cause["task_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let attempt_id = cause["attempt_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let task_revision = cause["task_revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let candidate_ref = cause["candidate_ref"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let profile_id = cause["profile_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let profile_revision = cause["profile_revision"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    if cause["kind"] != "cron_occurrence"
        || occurrence_id.len() != 64
        || !occurrence_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        || generation.is_empty()
    {
        return Err(corrupt());
    }
    type CronCheckOperationRow = (
        String,
        String,
        String,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    let row: Option<CronCheckOperationRow> = db
        .query_row(
            "SELECT caller_id,method,original_request_json,task_id,attempt_id,effective_request_json \
             FROM operations WHERE operation_id=?1",
            [&link.operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
        )
        .optional()?;
    let Some((caller, method, original_json, operation_task, operation_attempt, effective_json)) =
        row
    else {
        return Err(corrupt());
    };
    let request: Value = serde_json::from_str(&original_json).map_err(|_| corrupt())?;
    let effective: Value = effective_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|_| corrupt())?
        .ok_or_else(corrupt)?;
    if link.action != "check.run"
        || link.technical_requester_id != AUTOMATION_TECHNICAL_REQUESTER_ID
        || caller != link.technical_requester_id
        || method != "check.run"
        || link.automation_revision <= 0
        || link.effective_manager_id.is_empty()
        || link.project_id.is_empty()
        || link.automation_id.is_empty()
        || request["client_request_id"] != format!("cron-{occurrence_id}")
        || request["attempt_id"] != attempt_id
        || request["candidate_ref"] != candidate_ref
        || request["profile_id"] != profile_id
        || request["profile_revision"] != profile_revision
    {
        return Err(corrupt());
    }
    let target_task: Option<String> = db
        .query_row(
            "SELECT task_id FROM attempts WHERE attempt_id=?1 AND task_revision=?2",
            params![attempt_id, task_revision],
            |row| row.get(0),
        )
        .optional()?;
    if target_task.as_deref() != Some(task_id) {
        return Err(corrupt());
    }
    if effective["receipt"]["ok"] == false {
        if effective["receipt"]["error"].is_null()
            || effective.get("check_id").is_some()
            || effective.get("coalesced_check_id").is_some()
            || operation_task.is_some()
            || operation_attempt.is_some()
        {
            return Err(corrupt());
        }
        return Ok(());
    }
    if effective["receipt"]["ok"] != true
        || operation_task.as_deref() != Some(task_id)
        || operation_attempt.as_deref() != Some(attempt_id)
    {
        return Err(corrupt());
    }
    let check_id = effective["check_id"]
        .as_str()
        .or_else(|| effective["coalesced_check_id"].as_str())
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let check: Option<(String, String, String, String)> = db
        .query_row(
            "SELECT c.operation_id,c.attempt_id,c.candidate_ref,c.spec_json FROM check_runs c WHERE c.check_id=?1",
            [check_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((check_operation_id, check_attempt, check_candidate, spec_json)) = check else {
        return Err(corrupt());
    };
    let spec: Value = serde_json::from_str(&spec_json).map_err(|_| corrupt())?;
    if (effective.get("coalesced_check_id").is_some()
        && effective["coalesced_check_id"] != check_id)
        || (effective.get("coalesced_check_id").is_none()
            && check_operation_id != link.operation_id)
        || check_attempt != attempt_id
        || check_candidate != candidate_ref
        || spec["profile_id"] != profile_id
        || spec["profile_revision"] != profile_revision
        || spec["task_revision"] != task_revision
    {
        return Err(corrupt());
    }
    // Touch both timestamps to reject malformed/noncanonical fields while
    // keeping these historical facts independent from current activation.
    if due_at_ms < 0 || generation.len() != 64 {
        return Err(corrupt());
    }
    Ok(())
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

pub(crate) fn save_cron_operation_link(
    db: &Connection,
    operation_id: &str,
    context: &CronExecutionContext,
    now_ms: i64,
) -> Result<OnBehalfOperationLink> {
    let link = OnBehalfOperationLink {
        schema_version: 1,
        operation_id: operation_id.to_owned(),
        technical_requester_id: AUTOMATION_TECHNICAL_REQUESTER_ID.to_owned(),
        effective_manager_id: context.entry.owner_manager_id.clone(),
        automation_id: context.entry.automation_id.clone(),
        automation_revision: context.entry.revision,
        project_id: context.entry.project_id.clone(),
        action: "check.run".to_owned(),
        cause: context.cause_value(),
        linked_at_ms: now_ms,
    };
    let key = config::operation_link_key(operation_id)?;
    let value = link.value()?;
    config::write_record(db, &key, &value)?;
    let index_key = config::entry_operation_key(
        &context.entry.owner_manager_id,
        &context.entry.project_id,
        &context.entry.automation_id,
        operation_id,
    )?;
    config::write_record(db, &index_key, &value)?;
    Ok(link)
}

/// Retain the exact terminal-event/Goal attribution for one normally admitted
/// `agent.goal continue` Operation.
pub(crate) fn save_goal_progression_operation_link(
    db: &Connection,
    operation_id: &str,
    linkage: &Value,
    now_ms: i64,
) -> Result<OnBehalfOperationLink> {
    if linkage["action"] != "agent.goal"
        || linkage["semantic_cause_kind"] != "goal_progression"
        || linkage["cause"]["kind"] != "goal_progression"
        || linkage["cause"]["id"].as_str().is_none_or(str::is_empty)
    {
        return Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "Goal progression attribution is incomplete",
        ));
    }
    let link = OnBehalfOperationLink {
        schema_version: 1,
        operation_id: operation_id.to_owned(),
        technical_requester_id: model::text(linkage, "technical_requester_id")?.to_owned(),
        effective_manager_id: model::text(linkage, "effective_manager_id")?.to_owned(),
        automation_id: model::text(linkage, "automation_id")?.to_owned(),
        automation_revision: model::positive(linkage, "automation_revision")?,
        project_id: model::text(linkage, "project_id")?.to_owned(),
        action: "agent.goal".to_owned(),
        cause: linkage["cause"].clone(),
        linked_at_ms: now_ms,
    };
    let key = config::operation_link_key(operation_id)?;
    config::write_record(db, &key, &link.value()?)?;
    let index_key = config::entry_operation_key(
        &link.effective_manager_id,
        &link.project_id,
        &link.automation_id,
        operation_id,
    )?;
    config::write_record(db, &index_key, &link.value()?)?;
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
        if matches!(
            &link,
            AnyOnBehalfOperationLink::ScriptRun(script_link)
                if script_link.cause["kind"] == "system_event"
                    && script_link.cause["task_id"].as_str().is_none()
        ) {
            // Event-only invocations carry no Task from which a successor
            // Manager could derive a delegated scope.
            return Ok(false);
        }
        return current_gm_on_behalf_scope_visible_to(db, principal, &link);
    }
    match link {
        AnyOnBehalfOperationLink::Review(_) => Ok(true),
        AnyOnBehalfOperationLink::GoalProgression(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "Goal progression link has no exact Task identity",
                )
            })?;
            current_manager_has_task_scope(db, principal, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::ScriptRun(_) => {
            // The immutable Operation link is fully validated before this
            // scope check. Its effective Manager owns this history even after
            // Task completion, automation disable, or GM handover; current
            // admission rights are checked separately before a new ScriptRun.
            require_registered_manager(db, &principal.client_id)?;
            Ok(true)
        }
        AnyOnBehalfOperationLink::ScriptEffect(link) => script_effect_historical_task_scope(
            db,
            &principal.client_id,
            &link.project_id,
            &link.cause,
        ),
        AnyOnBehalfOperationLink::CronCheckRun(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "cron CheckRun link has no exact Task identity",
                )
            })?;
            current_manager_has_task_scope(db, principal, task_id, &link.project_id)
        }
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
        AnyOnBehalfOperationLink::CronCheckRun(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "cron CheckRun link has no exact Task identity",
                )
            })?;
            current_gm_has_task_project(db, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::GoalProgression(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "Goal progression link has no exact Task identity",
                )
            })?;
            current_gm_has_task_project(db, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::ScriptRun(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "ScriptRun link has no exact Task identity",
                )
            })?;
            current_gm_has_task_project(db, task_id, &link.project_id)
        }
        AnyOnBehalfOperationLink::ScriptEffect(link) => {
            let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "ScriptRun effect link has no exact Task identity",
                )
            })?;
            current_gm_has_task_project(db, task_id, &link.project_id)
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
    let is_cron = review
        .as_ref()
        .is_some_and(|link| link.action == "check.run");
    let is_script_run = review
        .as_ref()
        .is_some_and(|link| link.action == "script.run");
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
        (Some(link), None, None) if link.action == "check.run" && is_cron => {
            Ok(Some(AnyOnBehalfOperationLink::CronCheckRun(link)))
        }
        (Some(link), None, None) if link.action == "task.accept" => {
            Ok(Some(AnyOnBehalfOperationLink::Acceptance(link)))
        }
        (Some(link), None, None) if link.action == "forge.publish_ref" => {
            Ok(Some(AnyOnBehalfOperationLink::Publication(link)))
        }
        (Some(link), None, None) if link.action == "agent.goal" => {
            Ok(Some(AnyOnBehalfOperationLink::GoalProgression(link)))
        }
        (Some(link), None, None) if link.action == "script.run" && is_script_run => {
            Ok(Some(AnyOnBehalfOperationLink::ScriptRun(link)))
        }
        (Some(link), None, None)
            if link.action == "message.send"
                && link.cause["kind"] == "script_controller_effect" =>
        {
            Ok(Some(AnyOnBehalfOperationLink::ScriptEffect(link)))
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
            | AutomationStep::RepairDispatch
            | AutomationStep::CheckRun
    ) {
        return Err(Error::invalid(
            "transferred Attempt authority is limited to review, repair, acceptance, and check steps",
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
        AutomationStep::CheckRun => current_entry.check_run_ready(),
        AutomationStep::ReviewDisposition
        | AutomationStep::Acceptance
        | AutomationStep::RepairDispatch => {
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
        AutomationStep::RepairDispatch => attempt_state == "needs_correction",
        AutomationStep::Acceptance => {
            matches!(attempt_state.as_str(), "submitted" | "needs_correction")
        }
        AutomationStep::CheckRun => true,
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
        || (step != AutomationStep::CheckRun
            && (attempt_submission_ref.as_deref() != Some(submission_ref)
                || attempt_candidate_ref.as_deref() != Some(candidate_ref)))
    {
        return Err(Error::new(
            "AUTOMATION_ATTEMPT_STALE",
            "Task, Attempt, submission, or candidate is no longer the exact live subject",
        ));
    }
    if step == AutomationStep::CheckRun {
        let source: Option<(String, String)> = db
            .query_row(
                "SELECT kind,metadata_json FROM artifacts WHERE artifact_id=?1",
                [candidate_ref],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((kind, metadata_json)) = source else {
            return Err(Error::new(
                "CHECK_SOURCE_REQUIRED",
                "transferred CheckRun source snapshot is not registered",
            ));
        };
        let metadata: Value = serde_json::from_str(&metadata_json)?;
        if kind != "source_snapshot"
            || metadata["task_id"] != task_id
            || metadata["attempt_id"] != attempt_id
            || metadata["task_revision"] != task_revision
        {
            return Err(Error::new(
                "CHECK_SOURCE_REQUIRED",
                "transferred CheckRun source snapshot does not match the exact Attempt",
            ));
        }
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

/// Revalidate a queued, unsent cron CheckRun against the currently committed
/// Automation entry immediately before CheckRun transitions to sending. A
/// transferred operation keeps its original caller and attribution; only a
/// sealed transfer continuation can authorize its successor.
pub(crate) fn authorize_cron_check_run_start(db: &Connection, operation_id: &str) -> Result<()> {
    let Some(AnyOnBehalfOperationLink::CronCheckRun(link)) =
        any_on_behalf_operation_link(db, operation_id)?
    else {
        return Err(Error::new(
            "FORBIDDEN",
            "queued technical CheckRun has no validated cron attribution",
        ));
    };
    let state: Option<(String, Option<i64>)> = db
        .query_row(
            "SELECT state,sent_at_ms FROM operations WHERE operation_id=?1 AND method='check.run'",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    if !state.is_some_and(|(state, sent_at_ms)| state == "queued" && sent_at_ms.is_none()) {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_EFFECT_NOT_QUEUED",
            "cron CheckRun is no longer queued and unsent",
        ));
    }
    let task_id = link.cause["task_id"].as_str().ok_or_else(|| {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "cron CheckRun link has no exact Task identity",
        )
    })?;
    let entry = if config::transfer_from_source(
        db,
        &link.effective_manager_id,
        &link.project_id,
        &link.automation_id,
    )?
    .is_some()
    {
        current_transfer_continuation(
            db,
            operation_id,
            "check.run",
            AutomationStep::CheckRun,
            task_id,
        )?
        .ok_or_else(|| {
            Error::new(
                "FORBIDDEN",
                "queued cron CheckRun has no current sealed transfer continuation",
            )
        })?
        .current_entry
    } else {
        config::load_entry(
            db,
            &link.effective_manager_id,
            &link.project_id,
            &link.automation_id,
        )?
        .ok_or_else(|| Error::new("FORBIDDEN", "owning automation was removed"))?
    };
    let generation = link.cause["calendar_generation"]
        .as_str()
        .ok_or_else(|| Error::new("AUTOMATION_LINK_CORRUPT", "cron generation is missing"))?;
    let occurrence_id = link.cause["id"]
        .as_str()
        .ok_or_else(|| Error::new("AUTOMATION_LINK_CORRUPT", "cron occurrence ID is missing"))?;
    let due_at_ms = link.cause["due_at_ms"]
        .as_i64()
        .ok_or_else(|| Error::new("AUTOMATION_LINK_CORRUPT", "cron due time is missing"))?;
    let context = CronExecutionContext::from_committed_entry(
        db,
        &entry,
        generation,
        occurrence_id,
        due_at_ms,
    )?;
    if context.cause_value() != link.cause {
        return Err(Error::new(
            "FORBIDDEN",
            "current cron settings no longer match the retained CheckRun target",
        ));
    }
    context.require_current_check_target(db)
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
        AnyOnBehalfOperationLink::CronCheckRun(link) => (
            link.operation_id.as_str(),
            link.effective_manager_id.as_str(),
            link.automation_revision,
            link.project_id.as_str(),
            link.automation_id.as_str(),
            link.action.as_str(),
        ),
        AnyOnBehalfOperationLink::GoalProgression(link) => (
            link.operation_id.as_str(),
            link.effective_manager_id.as_str(),
            link.automation_revision,
            link.project_id.as_str(),
            link.automation_id.as_str(),
            link.action.as_str(),
        ),
        AnyOnBehalfOperationLink::ScriptRun(link) => (
            link.operation_id.as_str(),
            link.effective_manager_id.as_str(),
            link.automation_revision,
            link.project_id.as_str(),
            link.automation_id.as_str(),
            link.action.as_str(),
        ),
        AnyOnBehalfOperationLink::ScriptEffect(link) => (
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
        AutomationStep::CheckRun => current_entry.check_run_ready(),
        AutomationStep::GithubProjection => false,
        AutomationStep::GoalProgression => false,
        AutomationStep::ScriptRun => false,
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
            | ("check.run", AutomationStep::CheckRun)
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

/// ScriptEffect history is visible to its retained effective Manager while the
/// exact historical Task and Attempt still match the immutable link. Admission
/// rechecks live authority separately; completion, release, later revisions,
/// and automation disable do not erase the Manager's own receipt history.
fn script_effect_historical_task_scope(
    db: &Connection,
    manager_id: &str,
    project_id: &str,
    cause: &Value,
) -> Result<bool> {
    let corrupt = || {
        Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "ScriptRun effect link has no exact historical Task and Attempt identity",
        )
    };
    let task_id = cause["task_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    let task_revision = cause["task_revision"]
        .as_i64()
        .filter(|value| *value > 0)
        .ok_or_else(corrupt)?;
    let attempt_id = cause["attempt_id"]
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(corrupt)?;
    require_registered_manager(db, manager_id)?;
    db.query_row(
        "SELECT EXISTS(
            SELECT 1 FROM tasks AS t JOIN attempts AS a ON a.task_id=t.task_id
            WHERE t.task_id=?1 AND t.project_id=?2
              AND a.attempt_id=?4 AND a.task_revision=?3
        )",
        params![task_id, project_id, task_revision, attempt_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
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
        if matches!(
            link.action.as_str(),
            "task.accept" | "forge.publish_ref" | "check.run" | "message.send"
        ) {
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
            let in_scope = if link.action == "message.send"
                && link.cause["kind"] == "script_controller_effect"
            {
                script_effect_historical_task_scope(db, owner, project, &link.cause)?
            } else {
                current_manager_id_has_task_scope(db, owner, task_id, project)?
            };
            if !in_scope {
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
