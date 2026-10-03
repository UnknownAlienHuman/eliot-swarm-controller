//! Bounded durable routing from committed submission facts into the existing
//! review-assignment Operation and semantic slot.

use super::{capacity, reviews::ReviewActor, submissions, tasks};
use crate::{
    automation::{
        actions::{AutomationCause, AutomationStep},
        authorization::{self, ManagerExecutionContext},
        config::{self, AutomationEntry},
    },
    error::{Error, Result},
    model,
    review::ReviewAssignRequest,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const DISPATCH_SCHEMA_VERSION: u32 = 1;
const MAX_PENDING_SUBJECTS: usize = 128;
const MAX_RECENT_DISPOSITIONS: usize = 20;
const MAX_RECONCILE_FACTS: usize = 16;
const MAX_PENDING_RECHECKS: usize = 8;
const MAX_SUBMISSION_PAGE: usize = 32;
const GLOBAL_CURSOR_KEY: &str = "automation:v1:dispatch_global_cursor";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GlobalDispatchCursor {
    schema_version: u32,
    last_entry_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DispatchState {
    schema_version: u32,
    owner_manager_id: String,
    project_id: String,
    automation_id: String,
    step: String,
    cursor: i64,
    activation_cut: i64,
    catch_up_until: Option<i64>,
    pending_after_observation_id: i64,
    pending: Vec<PendingSubject>,
    recent: Vec<Value>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingSubject {
    cause: Value,
    reason: String,
    wake_when: Vec<String>,
    first_seen_at_ms: i64,
    last_checked_at_ms: i64,
    held: bool,
}

#[derive(Debug)]
enum SubjectResult {
    Assigned {
        operation_id: String,
        value: Value,
    },
    Pending {
        reason: String,
        wake_when: Vec<String>,
    },
    Skipped {
        reason: String,
    },
}

pub(super) fn configure_activation(
    tx: &Transaction<'_>,
    before: Option<&AutomationEntry>,
    after: &AutomationEntry,
    include_existing: bool,
    cut: i64,
    now_ms: i64,
) -> Result<()> {
    let had_review_coverage = before.is_some_and(|entry| {
        entry.enabled && entry.steps.contains(&AutomationStep::ReviewDispatch)
    });
    let has_review_coverage =
        after.enabled && after.steps.contains(&AutomationStep::ReviewDispatch);
    let key = config::dispatch_state_key(
        &after.owner_manager_id,
        &after.project_id,
        &after.automation_id,
    )?;
    let mut state = match load_state(tx, after)? {
        Some(state) => state,
        None if before.is_none() => empty_state(after, cut, now_ms),
        None => {
            return Err(Error::new(
                "AUTOMATION_CURSOR_MISSING",
                "existing automation entry has no durable dispatch cursor",
            ));
        }
    };

    if !has_review_coverage {
        if before.is_some_and(|entry| entry.enabled) {
            for pending in &mut state.pending {
                pending.held = true;
                pending.reason = "automation_disabled_or_step_removed".to_owned();
                pending.last_checked_at_ms = now_ms;
            }
        }
    } else if !had_review_coverage {
        state.activation_cut = cut;
        state.cursor = if include_existing { 0 } else { cut };
        state.catch_up_until = include_existing.then_some(cut);
        for pending in &mut state.pending {
            if pending_observation_id(pending) <= cut {
                pending.held = !include_existing;
                if include_existing {
                    pending.reason = "awaiting_review_assignment".to_owned();
                }
            }
        }
    } else if before.is_some_and(|entry| entry.enabled) && !after.enabled {
        for pending in &mut state.pending {
            pending.held = true;
            pending.reason = "automation_disabled".to_owned();
            pending.last_checked_at_ms = now_ms;
        }
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)
}

/// Process one enabled entry. The caller owns the surrounding Store
/// transaction; fact cursor movement, pending reasons, Operation admission and
/// review-slot reservation therefore commit together.
pub(super) fn reconcile_entry(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    budget: usize,
    now_ms: i64,
) -> Result<Value> {
    if !entry.enabled || !entry.steps.contains(&AutomationStep::ReviewDispatch) {
        return Ok(
            json!({"automation_id":entry.automation_id,"processed":0,"waiting_for":"disabled_or_unselected"}),
        );
    }
    let key = config::dispatch_state_key(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let high_water = observation_cut(tx)?;
    let mut state = load_state(tx, entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURSOR_MISSING",
            "enabled automation entry has no durable dispatch cursor",
        )
    })?;
    let budget = budget.min(MAX_RECONCILE_FACTS);
    if budget == 0 {
        return Ok(state_projection(&state));
    }

    let mut processed = 0usize;
    let pending_budget = budget.min(MAX_PENDING_RECHECKS);
    processed += recheck_pending(tx, entry, &mut state, pending_budget, now_ms)?;
    let remaining_budget = budget.saturating_sub(processed);
    if remaining_budget > 0 && state.pending.len() < MAX_PENDING_SUBJECTS {
        processed +=
            consume_submission_page(tx, entry, &mut state, remaining_budget, high_water, now_ms)?;
    }
    state.updated_at_ms = now_ms;
    save_state(tx, &key, &state)?;
    let mut projection = state_projection(&state);
    projection["processed"] = json!(processed);
    projection["high_water"] = json!(high_water);
    Ok(projection)
}

pub(super) fn dispatch_state(db: &rusqlite::Connection, entry: &AutomationEntry) -> Result<Value> {
    let state = load_state(db, entry)?.ok_or_else(|| {
        Error::new(
            "AUTOMATION_CURSOR_MISSING",
            "automation entry has no durable dispatch cursor",
        )
    })?;
    Ok(state_projection(&state))
}

/// Shared-reconciler entry point. A bounded global keyset cursor prevents one
/// owner/entry from monopolizing each wake. The Store must call this in its
/// writer transaction after committed changes and at startup; watch messages
/// remain lossy hints only.
pub(crate) fn reconcile(
    tx: &Transaction<'_>,
    entry_budget: usize,
    fact_budget: usize,
    now_ms: i64,
) -> Result<Value> {
    let entry_budget = entry_budget.clamp(1, 32);
    let fact_budget = fact_budget.clamp(1, MAX_RECONCILE_FACTS);
    let (entries, last_entry_key) = enabled_entry_page(tx, entry_budget)?;
    let mut results = Vec::with_capacity(entries.len());
    for entry in entries {
        results.push(reconcile_entry(tx, &entry, fact_budget, now_ms)?);
    }
    if let Some(last_entry_key) = last_entry_key {
        config::write_record(
            tx,
            GLOBAL_CURSOR_KEY,
            &json!({"schema_version":1,"last_entry_key":last_entry_key}),
        )?;
    }
    Ok(json!({"entries":results,"entry_budget":entry_budget,"fact_budget_per_entry":fact_budget}))
}

fn enabled_entry_page(
    db: &rusqlite::Connection,
    limit: usize,
) -> Result<(Vec<AutomationEntry>, Option<String>)> {
    let prefix = "automation:v1:entry:";
    let pattern = format!("{prefix}%");
    let cursor = config::read_record(db, GLOBAL_CURSOR_KEY, "automation dispatcher cursor")?
        .map(|value| {
            serde_json::from_value::<GlobalDispatchCursor>(value).map_err(|_| {
                Error::new(
                    "AUTOMATION_CURSOR_CORRUPT",
                    "global automation cursor fields are invalid",
                )
            })
        })
        .transpose()?;
    if cursor.as_ref().is_some_and(|cursor| {
        cursor.schema_version != 1 || !cursor.last_entry_key.starts_with(prefix)
    }) {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "global automation cursor version or key is invalid",
        ));
    }
    let after = cursor.map_or_else(|| prefix.to_owned(), |cursor| cursor.last_entry_key);
    let mut keys = select_enabled_entry_keys(db, &pattern, &after, limit)?;
    if keys.len() < limit {
        let wrapped =
            select_enabled_entry_keys_before(db, &pattern, prefix, &after, limit - keys.len())?;
        keys.extend(wrapped);
    }
    if keys.is_empty() {
        keys = select_enabled_entry_keys(db, &pattern, prefix, limit)?;
    }
    let last_key = keys.last().cloned();
    let mut entries = Vec::with_capacity(keys.len());
    for key in keys {
        let raw: String =
            db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                row.get(0)
            })?;
        let sealed: Value = serde_json::from_str(&raw).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "automation entry JSON is invalid",
            )
        })?;
        let value = config::open_record(sealed, "automation entry")?;
        let entry: AutomationEntry = serde_json::from_value(value).map_err(|_| {
            Error::new(
                "AUTOMATION_RECORD_CORRUPT",
                "automation entry fields are invalid",
            )
        })?;
        config::validate_entry(&entry)?;
        if entry.enabled && entry.steps.contains(&AutomationStep::ReviewDispatch) {
            entries.push(entry);
        }
    }
    Ok((entries, last_key))
}

fn select_enabled_entry_keys(
    db: &rusqlite::Connection,
    pattern: &str,
    after: &str,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = db.prepare(
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 \
         AND json_extract(value_json,'$.record.enabled')=1 \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                    WHERE step.value='review_dispatch') \
         ORDER BY key LIMIT ?3",
    )?;
    Ok(statement
        .query_map(params![pattern, after, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn select_enabled_entry_keys_before(
    db: &rusqlite::Connection,
    pattern: &str,
    prefix: &str,
    before: &str,
    limit: usize,
) -> Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let mut statement = db.prepare(
        "SELECT key FROM meta WHERE key LIKE ?1 AND key>?2 AND key<?3 \
         AND json_extract(value_json,'$.record.enabled')=1 \
         AND EXISTS(SELECT 1 FROM json_each(value_json,'$.record.steps') AS step \
                    WHERE step.value='review_dispatch') \
         ORDER BY key LIMIT ?4",
    )?;
    Ok(statement
        .query_map(params![pattern, prefix, before, limit as i64], |row| {
            row.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?)
}

fn consume_submission_page(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    state: &mut DispatchState,
    budget: usize,
    high_water: i64,
    now_ms: i64,
) -> Result<usize> {
    let target = state
        .catch_up_until
        .map_or(high_water, |cut| high_water.min(cut));
    if state.cursor >= target {
        if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
            state.catch_up_until = None;
        }
        return Ok(0);
    }
    let page_limit = budget.min(MAX_SUBMISSION_PAGE);
    let mut statement = tx.prepare(
        "SELECT observation_id,operation_id,payload_json FROM observations \
         WHERE source_stream_id='controller' AND kind='task.submission' \
         AND observation_id>?1 AND observation_id<=?2 \
         ORDER BY observation_id LIMIT ?3",
    )?;
    let rows = statement
        .query_map(params![state.cursor, target, page_limit as i64], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    if rows.is_empty() {
        state.cursor = target;
        if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
            state.catch_up_until = None;
        }
        return Ok(0);
    }
    let mut processed = 0usize;
    let mut last_seen = state.cursor;
    for (observation_id, fact_operation_id, payload_json) in &rows {
        if state.pending.len() >= MAX_PENDING_SUBJECTS {
            break;
        }
        let payload: Value = serde_json::from_str(payload_json).map_err(|_| {
            Error::new(
                "AUTOMATION_FACT_CORRUPT",
                "submission fact payload is invalid",
            )
        })?;
        last_seen = *observation_id;
        processed += 1;
        let fact_operation_id = fact_operation_id.as_deref().ok_or_else(|| {
            Error::new(
                "AUTOMATION_FACT_CORRUPT",
                "submission observation has no Operation identity",
            )
        })?;
        if payload.get("operation_id").and_then(Value::as_str) != Some(fact_operation_id) {
            return Err(Error::new(
                "AUTOMATION_FACT_CORRUPT",
                "submission observation Operation does not match its payload",
            ));
        }
        match cause_from_fact(*observation_id, &payload)? {
            None => {
                remember_recent(
                    state,
                    json!({
                        "observation_id":observation_id,
                        "disposition":"skipped",
                        "reason":"submission_not_applied"
                    }),
                );
            }
            Some(cause) => match attempt_review_assignment(tx, entry, &cause, now_ms)? {
                SubjectResult::Assigned {
                    operation_id,
                    value,
                } => remember_recent(
                    state,
                    json!({
                        "observation_id":observation_id,
                        "submission_ref":cause.id(),
                        "disposition":"assigned",
                        "operation_id":operation_id,
                        "review_assignment_id":value["review_assignment_id"]
                    }),
                ),
                SubjectResult::Pending { reason, wake_when } => {
                    state.pending.push(PendingSubject {
                        cause: cause.as_json(),
                        reason,
                        wake_when,
                        first_seen_at_ms: now_ms,
                        last_checked_at_ms: now_ms,
                        held: false,
                    });
                }
                SubjectResult::Skipped { reason } => remember_recent(
                    state,
                    json!({
                        "observation_id":observation_id,
                        "submission_ref":cause.id(),
                        "disposition":"skipped",
                        "reason":reason
                    }),
                ),
            },
        }
        state.cursor = *observation_id;
    }
    if processed == rows.len() && rows.len() < page_limit {
        state.cursor = target;
    } else if processed == rows.len() {
        state.cursor = last_seen;
    }
    if state.catch_up_until.is_some_and(|cut| state.cursor >= cut) {
        state.catch_up_until = None;
    }
    Ok(processed)
}

fn recheck_pending(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    state: &mut DispatchState,
    budget: usize,
    now_ms: i64,
) -> Result<usize> {
    if budget == 0 || state.pending.is_empty() || !entry.enabled {
        return Ok(0);
    }
    state.pending.sort_by_key(pending_observation_id);
    let start = state
        .pending
        .iter()
        .position(|pending| pending_observation_id(pending) > state.pending_after_observation_id)
        .unwrap_or(0);
    let candidates = state.pending.len().min(budget);
    let indices = (0..candidates)
        .map(|offset| (start + offset) % state.pending.len())
        .collect::<Vec<_>>();
    let mut remove = BTreeSet::new();
    for index in indices {
        state.pending_after_observation_id = pending_observation_id(&state.pending[index]);
        if state.pending[index].held {
            continue;
        }
        let cause = match cause_from_json(&state.pending[index].cause) {
            Ok(cause) => cause,
            Err(error) => {
                state.pending[index].reason = "retained_cause_corrupt".to_owned();
                state.pending[index].wake_when = vec!["operator_inspection".to_owned()];
                state.pending[index].last_checked_at_ms = now_ms;
                remember_recent(state, json!({"disposition":"blocked","error":error}));
                continue;
            }
        };
        match attempt_review_assignment(tx, entry, &cause, now_ms)? {
            SubjectResult::Assigned {
                operation_id,
                value,
            } => {
                remove.insert(cause.id().to_owned());
                remember_recent(
                    state,
                    json!({
                        "submission_ref":cause.id(),
                        "disposition":"assigned",
                        "operation_id":operation_id,
                        "review_assignment_id":value["review_assignment_id"]
                    }),
                );
            }
            SubjectResult::Pending { reason, wake_when } => {
                state.pending[index].reason = reason;
                state.pending[index].wake_when = wake_when;
                state.pending[index].last_checked_at_ms = now_ms;
            }
            SubjectResult::Skipped { reason } => {
                remove.insert(cause.id().to_owned());
                remember_recent(
                    state,
                    json!({"submission_ref":cause.id(),"disposition":"skipped","reason":reason}),
                );
            }
        }
    }
    state
        .pending
        .retain(|pending| !remove.contains(pending_cause_id(pending)));
    Ok(candidates)
}

fn attempt_review_assignment(
    tx: &Transaction<'_>,
    entry: &AutomationEntry,
    cause: &AutomationCause,
    now_ms: i64,
) -> Result<SubjectResult> {
    if !entry.enabled || !entry.steps.contains(&AutomationStep::ReviewDispatch) {
        return Ok(SubjectResult::Pending {
            reason: "automation_disabled_or_step_removed".to_owned(),
            wake_when: vec!["automation_config_changed".to_owned()],
        });
    }
    if entry.scope.work_pool_id.is_some() {
        return Ok(SubjectResult::Pending {
            reason: "work_pool_scope_unavailable".to_owned(),
            wake_when: vec!["work_pool_membership_reader_available".to_owned()],
        });
    }
    if entry.review.required_reviewers != 1 {
        return Ok(SubjectResult::Pending {
            reason: "reviewer_count_unsupported".to_owned(),
            wake_when: vec!["supported_review_slot_count_selected".to_owned()],
        });
    }
    let Some(profile) = entry.review.profile.as_deref() else {
        return Ok(SubjectResult::Pending {
            reason: "review_profile_required".to_owned(),
            wake_when: vec!["review_profile_configured".to_owned()],
        });
    };
    let submission_ref = cause.id();
    let document = match submissions::document(tx, submission_ref) {
        Ok(document) => document,
        Err(error) => {
            return Ok(SubjectResult::Pending {
                reason: format!("submission_record_unavailable:{}", error.code),
                wake_when: vec!["submission_record_readable".to_owned()],
            });
        }
    };
    if document["operation_id"]
        != json!(match cause {
            AutomationCause::AppliedSubmission { operation_id, .. } => operation_id,
        })
        || document["task_revision"].as_i64().is_none()
    {
        return Ok(SubjectResult::Pending {
            reason: "submission_identity_mismatch".to_owned(),
            wake_when: vec!["operator_inspection".to_owned()],
        });
    }
    let attempt_id = match document["attempt_id"].as_str() {
        Some(value) => value,
        None => {
            return Ok(SubjectResult::Pending {
                reason: "submission_missing_attempt".to_owned(),
                wake_when: vec!["operator_inspection".to_owned()],
            });
        }
    };
    let task_id = match document["task_id"].as_str() {
        Some(value) => value,
        None => {
            return Ok(SubjectResult::Pending {
                reason: "submission_missing_task".to_owned(),
                wake_when: vec!["operator_inspection".to_owned()],
            });
        }
    };
    let task_revision = document["task_revision"].as_i64().unwrap_or_default();
    let candidate_ref = match document["candidate_ref"].as_str() {
        Some(value) => value,
        None => {
            return Ok(SubjectResult::Pending {
                reason: "submission_missing_candidate".to_owned(),
                wake_when: vec!["operator_inspection".to_owned()],
            });
        }
    };
    let attempt = match tasks::get_attempt(tx, attempt_id) {
        Ok(value) => value,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(SubjectResult::Skipped {
                reason: "attempt_no_longer_available".to_owned(),
            });
        }
        Err(error) => return Err(error),
    };
    let task = match tasks::get_task(tx, task_id) {
        Ok(value) => value,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(SubjectResult::Skipped {
                reason: "task_no_longer_available".to_owned(),
            });
        }
        Err(error) => return Err(error),
    };
    if task["project_id"] != entry.project_id {
        return Ok(SubjectResult::Skipped {
            reason: "outside_project_scope".to_owned(),
        });
    }
    if task["state"] != "open"
        || task["revision"] != json!(task_revision)
        || attempt["state"] != "submitted"
        || attempt["task_revision"] != json!(task_revision)
        || attempt["task_id"] != json!(task_id)
        || attempt["owner_id"] != json!(entry.owner_manager_id)
        || !attempt["released_at_ms"].is_null()
    {
        return Ok(SubjectResult::Skipped {
            reason: "submission_is_not_current_manager_work".to_owned(),
        });
    }
    if attempt["submission_ref"] != json!(submission_ref)
        || attempt["candidate_ref"] != json!(candidate_ref)
    {
        return Ok(SubjectResult::Skipped {
            reason: "submission_is_not_current_attempt_candidate".to_owned(),
        });
    }
    let context = match ManagerExecutionContext::from_committed_entry(tx, entry, cause.clone()) {
        Ok(context) => context,
        Err(error) if error.code == "FORBIDDEN" || error.code == "UNAUTHORIZED" => {
            return Ok(SubjectResult::Pending {
                reason: "manager_authority_unavailable".to_owned(),
                wake_when: vec!["manager_registration_or_rights_changed".to_owned()],
            });
        }
        Err(error) if error.code.starts_with("AUTOMATION_ACTION") => {
            return Ok(SubjectResult::Pending {
                reason: error.code.to_ascii_lowercase(),
                wake_when: vec!["automation_configuration_changed".to_owned()],
            });
        }
        Err(error) => return Err(error),
    };
    let client_request_id = automatic_request_id(
        &context,
        task_id,
        attempt_id,
        task_revision,
        submission_ref,
        candidate_ref,
    )?;
    let request = ReviewAssignRequest::for_automation(
        client_request_id.clone(),
        attempt_id.to_owned(),
        task_revision,
        submission_ref.to_owned(),
        candidate_ref.to_owned(),
        profile.to_owned(),
    );
    let request_value = json!({
        "client_request_id":client_request_id,
        "attempt_id":attempt_id,
        "expected_revision":task_revision,
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref,
        "review_profile":profile
    });
    match reserve_automatic_operation(tx, &context, &request, &request_value, now_ms) {
        Ok((operation_id, value)) => Ok(SubjectResult::Assigned {
            operation_id,
            value,
        }),
        Err(error) if is_pending_review_error(&error) => Ok(SubjectResult::Pending {
            reason: error.code.to_ascii_lowercase(),
            wake_when: vec!["eligible_reviewer_or_review_slot_change".to_owned()],
        }),
        Err(error) if is_stale_review_error(&error) => Ok(SubjectResult::Skipped {
            reason: error.code.to_ascii_lowercase(),
        }),
        Err(error) => Err(error),
    }
}

fn reserve_automatic_operation(
    tx: &Transaction<'_>,
    context: &ManagerExecutionContext,
    request: &ReviewAssignRequest,
    request_value: &Value,
    now_ms: i64,
) -> Result<(String, Value)> {
    let caller = context.technical_requester_id();
    let request_id = model::text(request_value, "client_request_id")?;
    let original_json = model::canonical(request_value)?;
    let old: Option<(String, String, String, Option<String>)> = tx
        .query_row(
            "SELECT method,original_request_json,state,result_json FROM operations \
             WHERE caller_id=?1 AND client_request_id=?2",
            params![caller, request_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    if let Some((method, original, state, result)) = old {
        if method != "review.assign" || original != original_json {
            return Err(Error::new(
                "REVIEW_SLOT_CONFLICT",
                "this semantic review request already retained different effective inputs",
            ));
        }
        if state == "settled" {
            let operation_id: String = tx.query_row(
                "SELECT operation_id FROM operations WHERE caller_id=?1 AND client_request_id=?2",
                params![caller, request_id],
                |row| row.get(0),
            )?;
            let link = authorization::operation_link(tx, &operation_id)?.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "settled automatic review request has no retained manager link",
                )
            })?;
            if link.effective_manager_id != context.effective_manager_id()
                || link.project_id != context.project_id()
                || link.automation_id != context.automation_id()
                || link.cause["id"] != context.semantic_cause_id()
            {
                return Err(Error::new(
                    "AUTOMATION_LINK_CORRUPT",
                    "settled automatic review request is linked to another manager, entry, or subject",
                ));
            }
            let result: Value = serde_json::from_str(&result.ok_or_else(|| {
                Error::new(
                    "AUTOMATION_OPERATION_CORRUPT",
                    "settled review request has no result",
                )
            })?)?;
            return Ok((operation_id, result));
        }
        return Err(Error::new(
            "REVIEW_ASSIGNMENT_RECONCILING",
            "automatic review assignment has an unresolved retained Operation",
        ));
    }

    let operation_id = model::new_id();
    let effective = json!({
        "request":request_value,
        "automation_on_behalf":context.linkage_value()
    });
    tx.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,?2,?3,'review.assign',?4,?5,'queued',?6,?6,?6)",
        params![operation_id, caller, request_id, original_json, model::canonical(&effective)?, now_ms],
    )?;
    tx.execute_batch("SAVEPOINT automation_review_assignment")?;
    let outcome = super::reviews::reserve_assign(
        tx,
        ReviewActor::OnBehalf(context),
        request,
        &operation_id,
        now_ms,
    );
    match outcome {
        Ok(value) => {
            let receipt = json!({"ok":true,"value":value});
            let effective: Value = {
                let raw: String = tx.query_row(
                    "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                    [&operation_id],
                    |row| row.get(0),
                )?;
                serde_json::from_str(&raw)?
            };
            let mut effective = effective;
            if !effective.is_object() {
                effective = json!({"request":request_value});
            }
            effective["automation_on_behalf"] = context.linkage_value();
            effective["receipt"] = receipt;
            tx.execute(
                "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3,effective_request_json=?4 WHERE operation_id=?1",
                params![operation_id, model::canonical(&value)?, now_ms, model::canonical(&effective)?],
            )?;
            capacity::sync_operation(tx, &operation_id, now_ms)?;
            tx.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES('controller',?1,?1,'review.assign',?2,?3)",
                params![operation_id, model::canonical(&value)?, now_ms],
            )?;
            authorization::save_operation_link(tx, &operation_id, context, now_ms)?;
            tx.execute_batch("RELEASE automation_review_assignment")?;
            Ok((operation_id, value))
        }
        Err(error) => {
            tx.execute_batch(
                "ROLLBACK TO automation_review_assignment; RELEASE automation_review_assignment",
            )?;
            tx.execute(
                "DELETE FROM operations WHERE operation_id=?1",
                [&operation_id],
            )?;
            Err(error)
        }
    }
}

fn automatic_request_id(
    context: &ManagerExecutionContext,
    task_id: &str,
    attempt_id: &str,
    task_revision: i64,
    submission_ref: &str,
    candidate_ref: &str,
) -> Result<String> {
    let identity = json!({
        "automation_id":context.automation_id(),
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "submission_ref":submission_ref,
        "candidate_ref":candidate_ref,
        "action":"review.assign",
        "slot":"primary"
    });
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

fn cause_from_fact(observation_id: i64, payload: &Value) -> Result<Option<AutomationCause>> {
    if payload["outcome"] != "applied" {
        return Ok(None);
    }
    let operation_id = model::text(payload, "operation_id").map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "applied submission fact has no Operation identity",
        )
    })?;
    let submission_ref = model::text(payload, "submission_ref").map_err(|_| {
        Error::new(
            "AUTOMATION_FACT_CORRUPT",
            "applied submission fact has no submission identity",
        )
    })?;
    Ok(Some(AutomationCause::AppliedSubmission {
        observation_id,
        operation_id: operation_id.to_owned(),
        submission_ref: submission_ref.to_owned(),
    }))
}

fn cause_from_json(value: &Value) -> Result<AutomationCause> {
    if value["kind"] != "applied_submission" {
        return Err(Error::new(
            "AUTOMATION_CAUSE_INVALID",
            "pending cause kind is unsupported",
        ));
    }
    Ok(AutomationCause::AppliedSubmission {
        observation_id: value["observation_id"].as_i64().ok_or_else(|| {
            Error::new(
                "AUTOMATION_CAUSE_INVALID",
                "pending cause has no observation",
            )
        })?,
        operation_id: model::text(value, "operation_id")?.to_owned(),
        submission_ref: model::text(value, "id")?.to_owned(),
    })
}

fn is_pending_review_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "REVIEW_AUDITOR_UNAVAILABLE"
            | "REVIEW_AUDITOR_AMBIGUOUS"
            | "REVIEWER_PROFILE_UNAVAILABLE"
            | "REVIEWER_UNAVAILABLE"
            | "REVIEW_SLOT_CONFLICT"
            | "REVIEW_ASSIGNMENT_RECONCILING"
            | "REVIEW_CAPACITY"
            | "CAPACITY_UNAVAILABLE"
            | "FORBIDDEN"
            | "UNAUTHORIZED"
    )
}

fn is_stale_review_error(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "STALE_REVISION" | "STALE_SUBMISSION" | "NOT_FOUND" | "SUBMISSION_DAMAGED"
    )
}

fn load_state(db: &rusqlite::Connection, entry: &AutomationEntry) -> Result<Option<DispatchState>> {
    let key = config::dispatch_state_key(
        &entry.owner_manager_id,
        &entry.project_id,
        &entry.automation_id,
    )?;
    let Some(record) = config::read_record(db, &key, "automation dispatch cursor")? else {
        return Ok(None);
    };
    let state: DispatchState = serde_json::from_value(record).map_err(|_| {
        Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "automation dispatch state fields are invalid",
        )
    })?;
    if state.schema_version != DISPATCH_SCHEMA_VERSION
        || state.owner_manager_id != entry.owner_manager_id
        || state.project_id != entry.project_id
        || state.automation_id != entry.automation_id
        || state.step != AutomationStep::ReviewDispatch.as_str()
        || state.cursor < 0
        || state.pending.len() > MAX_PENDING_SUBJECTS
        || state.recent.len() > MAX_RECENT_DISPOSITIONS
    {
        return Err(Error::new(
            "AUTOMATION_CURSOR_CORRUPT",
            "automation dispatch cursor identity or bounds are invalid",
        ));
    }
    Ok(Some(state))
}

fn save_state(db: &rusqlite::Connection, key: &str, state: &DispatchState) -> Result<()> {
    let value = serde_json::to_value(state)?;
    config::write_record(db, key, &value)
}

fn empty_state(entry: &AutomationEntry, cut: i64, now_ms: i64) -> DispatchState {
    DispatchState {
        schema_version: DISPATCH_SCHEMA_VERSION,
        owner_manager_id: entry.owner_manager_id.clone(),
        project_id: entry.project_id.clone(),
        automation_id: entry.automation_id.clone(),
        step: AutomationStep::ReviewDispatch.as_str().to_owned(),
        cursor: cut,
        activation_cut: cut,
        catch_up_until: None,
        pending_after_observation_id: 0,
        pending: Vec::new(),
        recent: Vec::new(),
        updated_at_ms: now_ms,
    }
}

fn observation_cut(db: &rusqlite::Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(observation_id),0) FROM observations",
        [],
        |row| row.get(0),
    )?)
}

fn state_projection(state: &DispatchState) -> Value {
    json!({
        "schema_version":state.schema_version,
        "step":state.step,
        "cursor":state.cursor,
        "activation_cut":state.activation_cut,
        "catch_up_until":state.catch_up_until,
        "pending":state.pending,
        "recent":state.recent,
        "updated_at_ms":state.updated_at_ms
    })
}

fn pending_observation_id(pending: &PendingSubject) -> i64 {
    pending.cause["observation_id"].as_i64().unwrap_or(i64::MAX)
}

fn pending_cause_id(pending: &PendingSubject) -> &str {
    pending.cause["id"].as_str().unwrap_or_default()
}

fn remember_recent(state: &mut DispatchState, event: Value) {
    state.recent.push(event);
    if state.recent.len() > MAX_RECENT_DISPOSITIONS {
        let excess = state.recent.len() - MAX_RECENT_DISPOSITIONS;
        state.recent.drain(0..excess);
    }
}
