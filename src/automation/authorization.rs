//! Internal manager-on-behalf identity. This is constructed from verified
//! Store state and intentionally has no public deserializer or constructor.

use super::{actions::AutomationCause, config};
use crate::{
    error::{Error, Result},
    model::{Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

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
    released_at_ms: Option<i64>,
    attempt_state: String,
    project_id: String,
    task_state: String,
    current_task_revision: i64,
}

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
    pub(crate) fn require_action_object(
        &self,
        db: &Connection,
        action: &str,
        task_id: &str,
        attempt_id: &str,
        project_id: &str,
        submission_ref: &str,
    ) -> Result<()> {
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
                "SELECT a.owner_id,a.task_id,a.task_revision,a.submission_ref,a.released_at_ms,a.state,t.project_id,t.state,t.revision \
                 FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id WHERE a.attempt_id=?1",
                [attempt_id],
                |r| Ok(CurrentReviewSubject {
                    owner_id: r.get(0)?,
                    task_id: r.get(1)?,
                    task_revision: r.get(2)?,
                    submission_ref: r.get(3)?,
                    released_at_ms: r.get(4)?,
                    attempt_state: r.get(5)?,
                    project_id: r.get(6)?,
                    task_state: r.get(7)?,
                    current_task_revision: r.get(8)?,
                }),
            )
            .optional()?;
        let Some(subject) = row else {
            return Err(Error::new("NOT_FOUND", "review attempt was not found"));
        };
        if subject.owner_id != self.effective_manager_id
            || subject.task_id != task_id
            || subject.project_id != project_id
            || subject.released_at_ms.is_some()
            || subject.attempt_state != "submitted"
            || subject.task_state != "open"
            || subject.current_task_revision != subject.task_revision
            || subject.submission_ref.as_deref() != Some(submission_ref)
        {
            return Err(Error::new(
                "FORBIDDEN",
                "manager no longer owns this current review subject",
            ));
        }
        Ok(())
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
/// Review links and WorkDispatch links have separate contracts; this enum is
/// only their shared visibility boundary and does not widen either contract.
pub(crate) enum AnyOnBehalfOperationLink {
    Review(OnBehalfOperationLink),
    WorkDispatch(crate::store::automation_work_dispatch::WorkDispatchOperationLink),
}

impl AnyOnBehalfOperationLink {
    pub(crate) fn belongs_to(&self, principal: &Principal) -> bool {
        match self {
            Self::Review(link) => link.belongs_to(principal),
            Self::WorkDispatch(link) => link.belongs_to(principal),
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
    }
    Ok(Some(link))
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
        return Ok(false);
    }
    match link {
        AnyOnBehalfOperationLink::Review(_) => Ok(true),
        AnyOnBehalfOperationLink::WorkDispatch(link) => {
            current_work_dispatch_scope_visible_to(db, principal, &link)
        }
    }
}

/// Load a retained review or WorkDispatch link and validate its own durable
/// provenance. This is intended for the local Operator's global diagnostic
/// branch; manager authorization must additionally use
/// `on_behalf_visible_to` so current Task/project rights are rechecked.
pub(crate) fn any_on_behalf_operation_link(
    db: &Connection,
    operation_id: &str,
) -> Result<Option<AnyOnBehalfOperationLink>> {
    let review = operation_link(db, operation_id)?;
    let work_dispatch = crate::store::automation_work_dispatch::operation_link(db, operation_id)?;
    match (review, work_dispatch) {
        (Some(_), Some(_)) => Err(Error::new(
            "AUTOMATION_LINK_CORRUPT",
            "Operation has both review and WorkDispatch attribution records",
        )),
        (Some(link), None) => Ok(Some(AnyOnBehalfOperationLink::Review(link))),
        (None, Some(link)) => Ok(Some(AnyOnBehalfOperationLink::WorkDispatch(link))),
        (None, None) => Ok(None),
    }
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

fn current_manager_id_has_task_scope(
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
            if links
                .iter()
                .any(|existing| existing.operation_id == link.operation_id)
            {
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
