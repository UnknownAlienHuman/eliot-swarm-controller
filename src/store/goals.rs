//! Durable task-scoped Goals, exact acceptance readback, and one-shot reminders.
//!
//! Goal records and their list/due/reminder indexes live in the existing
//! `meta` store. The host's shared automation scheduler calls `reconcile`; this
//! module owns no timer, native input, or model continuation.

use super::{current_principal, gm, meta, set_meta, tasks};
use crate::{
    automation::authorization,
    error::{Error, Result},
    goals::{self as api, CompletionEvidence, Mutation, ReadRequest, ReminderConfig, Scope},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

const RECORD_PREFIX: &str = "goals:v1:record:";
const LIST_PREFIX: &str = "goals:v1:list:";
const DUE_PREFIX: &str = "goals:v1:due:";
const REMINDER_RECEIPT_PREFIX: &str = "goals:v1:reminder-receipt:";
const NOTICE_PREFIX: &str = "goals:v1:notice:";
const QUARANTINE_FACT_PREFIX: &str = "goals:v1:quarantine-fact:";
const QUARANTINE_DIAGNOSTIC_PREFIX: &str = "goals:v1:quarantine-diagnostic:";
const QUARANTINE_TIME_PREFIX: &str = "goals:v1:quarantine-time:";
const RECORD_SCHEMA: u32 = 1;
const MAX_SCOPE_HISTORY: usize = 32;
const MAX_RECONCILE_LIMIT: usize = 128;
const MAX_NOTIFICATION_LIMIT: usize = 50;
const MAX_DIAGNOSTIC_PAGE: usize = 8;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoalRecord {
    schema_version: u32,
    goal_id: String,
    scope: Scope,
    scope_history: Vec<Scope>,
    revision: i64,
    objective: String,
    completion_evidence: CompletionEvidence,
    reminder: Option<ReminderConfig>,
    reminder_status: String,
    enabled: bool,
    created_by_manager_id: String,
    created_at_ms: i64,
    updated_by_manager_id: String,
    updated_at_ms: i64,
    last_readback: Option<Value>,
    last_reminder: Option<ReminderReceipt>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReminderReceipt {
    notification_id: String,
    due_at_ms: i64,
    cooldown_ms: i64,
    recorded_at_ms: i64,
    outcome: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReminderSlotReceipt {
    schema_version: u32,
    notification_id: String,
    goal_id: String,
    project_id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    due_at_ms: i64,
    cooldown_ms: i64,
    recorded_at_ms: i64,
    outcome: String,
    goal_revision: i64,
    completion_status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct QuarantineDiagnostic {
    schema_version: u32,
    source_kind: String,
    source_key_digest: String,
    raw_digest: String,
    fact_key: String,
    reason: String,
    error_code: String,
    recorded_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DueIndex {
    schema_version: u32,
    goal_id: String,
    record_key: String,
    due_at_ms: i64,
    slot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ReminderNotice {
    schema_version: u32,
    notification_id: String,
    goal_id: String,
    project_id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
    due_at_ms: i64,
    recorded_at_ms: i64,
    goal_revision: i64,
    created_by_manager_id: String,
    objective_summary: String,
    completion_status: String,
}

struct CreateCommand {
    scope: Scope,
    goal_id: String,
    objective: String,
    completion_evidence: CompletionEvidence,
    reminder: Option<ReminderConfig>,
    enabled: bool,
}

struct ReviseCommand {
    scope: Scope,
    goal_id: String,
    expected_revision: i64,
    objective: Option<String>,
    completion_evidence: Option<CompletionEvidence>,
    reminder: Option<Option<ReminderConfig>>,
    enabled: Option<bool>,
}

struct ReminderToggle {
    scope: Scope,
    goal_id: String,
    expected_revision: i64,
    enabled: bool,
}

fn record_key(project_id: &str, task_id: &str, goal_id: &str) -> Result<String> {
    let identity = json!([project_id, task_id, goal_id]);
    Ok(format!(
        "{RECORD_PREFIX}{}",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn scoped_list_prefix(scope: &Scope) -> Result<String> {
    let identity = json!([
        scope.project_id,
        scope.task_id,
        scope.task_revision,
        scope.attempt_id
    ]);
    Ok(format!(
        "{LIST_PREFIX}{}:",
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn scope_list_key(scope: &Scope, goal_id: &str) -> Result<String> {
    Ok(format!(
        "{}{}",
        scoped_list_prefix(scope)?,
        model::digest(goal_id.as_bytes())
    ))
}

fn reminder_slot_id(record: &GoalRecord, due_at_ms: i64) -> Result<String> {
    reminder_slot_id_for(
        &record.scope.project_id,
        &record.scope.task_id,
        &record.goal_id,
        due_at_ms,
    )
}

fn reminder_slot_id_for(
    project_id: &str,
    task_id: &str,
    goal_id: &str,
    due_at_ms: i64,
) -> Result<String> {
    let identity = json!([project_id, task_id, goal_id, due_at_ms]);
    Ok(model::digest(model::canonical(&identity)?.as_bytes()))
}

fn due_index_key(record_key: &str, due_at_ms: i64) -> Result<String> {
    let record_id = record_key
        .strip_prefix(RECORD_PREFIX)
        .ok_or_else(|| damaged("Goal record key is outside its namespace"))?;
    Ok(format!("{DUE_PREFIX}{due_at_ms:019}:{record_id}"))
}

fn reminder_receipt_key(slot_id: &str) -> String {
    format!("{REMINDER_RECEIPT_PREFIX}{slot_id}")
}

fn quarantine_identity(source_key: &str, raw: &str) -> (String, String, String, String) {
    let source_digest = model::digest(source_key.as_bytes());
    let raw_digest = model::digest(raw.as_bytes());
    let identity = format!("{source_digest}:{raw_digest}");
    (
        source_digest,
        raw_digest,
        format!("{QUARANTINE_FACT_PREFIX}{identity}"),
        format!("{QUARANTINE_DIAGNOSTIC_PREFIX}{identity}"),
    )
}

fn raw_meta(db: &Connection, key: &str) -> Result<Option<String>> {
    Ok(db
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?)
}

/// Preserve the exact malformed optional fact before its derived due index is
/// removed. Stable keys make repeat detection idempotent; the timestamp index
/// keeps the current-GM diagnostic projection bounded and recent-first.
fn quarantine_optional_fact(
    db: &Connection,
    source_kind: &str,
    source_key: &str,
    raw: &str,
    reason: &str,
    now: i64,
) -> Result<QuarantineDiagnostic> {
    let (source_digest, raw_digest, fact_key, diagnostic_key) =
        quarantine_identity(source_key, raw);
    let diagnostic = QuarantineDiagnostic {
        schema_version: RECORD_SCHEMA,
        source_kind: source_kind.to_owned(),
        source_key_digest: source_digest.clone(),
        raw_digest: raw_digest.clone(),
        fact_key: fact_key.clone(),
        reason: reason.to_owned(),
        error_code: "GOAL_OPTIONAL_DATA_HELD".to_owned(),
        recorded_at_ms: now,
    };
    let time_key = format!("{QUARANTINE_TIME_PREFIX}{now:019}:{source_digest}:{raw_digest}");
    db.execute_batch("SAVEPOINT goals_optional_quarantine")?;
    let write_result = (|| {
        db.execute(
            "INSERT OR IGNORE INTO meta(key,value_json) VALUES(?1,?2)",
            params![fact_key, raw],
        )?;
        if raw_meta(db, &diagnostic_key)?.is_none() {
            set_meta(db, &diagnostic_key, &json!(diagnostic))?;
            set_meta(db, &time_key, &json!({"diagnostic_key":diagnostic_key}))?;
        }
        Ok::<(), Error>(())
    })();
    match write_result {
        Ok(()) => db.execute_batch("RELEASE goals_optional_quarantine")?,
        Err(error) => {
            db.execute_batch(
                "ROLLBACK TO goals_optional_quarantine; RELEASE goals_optional_quarantine",
            )?;
            return Err(error);
        }
    }
    Ok(diagnostic)
}

fn parse_record(raw: &str, project_id: &str, task_id: &str, goal_id: &str) -> Result<GoalRecord> {
    let record: GoalRecord =
        serde_json::from_str(raw).map_err(|_| damaged("Goal record is malformed"))?;
    verify_record(&record, project_id, task_id, goal_id)?;
    Ok(record)
}

fn notice_key(notice: &ReminderNotice) -> Result<String> {
    let identity = json!([
        notice.project_id,
        notice.task_id,
        notice.goal_id,
        notice.due_at_ms
    ]);
    Ok(format!(
        "{NOTICE_PREFIX}{:019}:{}",
        notice.due_at_ms,
        model::digest(model::canonical(&identity)?.as_bytes())
    ))
}

fn load_record(
    db: &Connection,
    project_id: &str,
    task_id: &str,
    goal_id: &str,
) -> Result<GoalRecord> {
    let key = record_key(project_id, task_id, goal_id)?;
    let raw = raw_meta(db, &key)?.ok_or_else(|| Error::new("NOT_FOUND", "Goal not found"))?;
    parse_record(&raw, project_id, task_id, goal_id)
}

fn verify_record(
    record: &GoalRecord,
    project_id: &str,
    task_id: &str,
    goal_id: &str,
) -> Result<()> {
    if record.schema_version != RECORD_SCHEMA
        || record.goal_id != goal_id
        || record.scope.project_id != project_id
        || record.scope.task_id != task_id
        || record.revision <= 0
        || record.scope.task_revision <= 0
        || record.scope_history.len() > MAX_SCOPE_HISTORY
        || !matches!(
            record.reminder_status.as_str(),
            "none"
                | "scheduled"
                | "disabled"
                | "delivered"
                | "suppressed_completed"
                | "suppressed_stale_scope"
        )
        || record.completion_evidence.kind != "task_acceptance"
        || (record.enabled && record.reminder.is_some() && record.reminder_status != "scheduled")
        || record
            .scope_history
            .iter()
            .any(|scope| scope.project_id != project_id || scope.task_id != task_id)
        || record.reminder.as_ref().is_some_and(|reminder| {
            reminder.due_at_ms <= 0 || !(0..=api::MAX_COOLDOWN_MS).contains(&reminder.cooldown_ms)
        })
    {
        return Err(damaged("Goal record identity or invariant is invalid"));
    }
    if let Some(last_reminder) = &record.last_reminder
        && (last_reminder.due_at_ms <= 0
            || last_reminder.recorded_at_ms < last_reminder.due_at_ms
            || !(0..=api::MAX_COOLDOWN_MS).contains(&last_reminder.cooldown_ms)
            || !matches!(
                last_reminder.outcome.as_str(),
                "delivered" | "suppressed_completed" | "suppressed_stale_scope"
            )
            || last_reminder.notification_id != reminder_slot_id(record, last_reminder.due_at_ms)?)
    {
        return Err(damaged("Goal reminder receipt invariant is invalid"));
    }
    Ok(())
}

fn save_record(tx: &Transaction<'_>, record: &GoalRecord) -> Result<()> {
    verify_record(
        record,
        &record.scope.project_id,
        &record.scope.task_id,
        &record.goal_id,
    )?;
    let key = record_key(
        &record.scope.project_id,
        &record.scope.task_id,
        &record.goal_id,
    )?;
    set_meta(tx, &key, &json!(record))?;
    Ok(())
}

fn create_list_index(tx: &Transaction<'_>, scope: &Scope, goal_id: &str) -> Result<()> {
    let key = scope_list_key(scope, goal_id)?;
    let value = json!({
        "schema_version":RECORD_SCHEMA,
        "project_id":scope.project_id,
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
        "goal_id":goal_id,
    });
    if meta(tx, &key)?.is_some() {
        return Ok(());
    }
    set_meta(tx, &key, &value)
}

fn upsert_due_index(tx: &Transaction<'_>, record: &GoalRecord) -> Result<()> {
    let key = record_key(
        &record.scope.project_id,
        &record.scope.task_id,
        &record.goal_id,
    )?;
    if !record.enabled {
        if let Some(reminder) = &record.reminder {
            tx.execute(
                "DELETE FROM meta WHERE key=?1",
                [due_index_key(&key, reminder.due_at_ms)?],
            )?;
        }
        return Ok(());
    }
    let Some(reminder) = &record.reminder else {
        return Ok(());
    };
    let slot_id = reminder_slot_id(record, reminder.due_at_ms)?;
    let index_key = due_index_key(&key, reminder.due_at_ms)?;
    if meta(tx, &reminder_receipt_key(&slot_id))?.is_some() {
        tx.execute("DELETE FROM meta WHERE key=?1", [index_key])?;
        return Ok(());
    }
    set_meta(
        tx,
        &index_key,
        &json!(DueIndex {
            schema_version: RECORD_SCHEMA,
            goal_id: record.goal_id.clone(),
            record_key: key,
            due_at_ms: reminder.due_at_ms,
            slot_id,
        }),
    )
}

fn remove_due_index(tx: &Transaction<'_>, record: &GoalRecord) -> Result<()> {
    let Some(reminder) = &record.reminder else {
        return Ok(());
    };
    let key = record_key(
        &record.scope.project_id,
        &record.scope.task_id,
        &record.goal_id,
    )?;
    tx.execute(
        "DELETE FROM meta WHERE key=?1",
        [due_index_key(&key, reminder.due_at_ms)?],
    )?;
    Ok(())
}

fn readback_value(db: &Connection, record: &GoalRecord, now: i64, actor: &str) -> Result<Value> {
    let task = match tasks::get_task(db, &record.scope.task_id) {
        Ok(task) => task,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(json!({
                "status":"unknown",
                "completed":false,
                "reason":"task_not_found",
                "criterion":record.completion_evidence,
                "task_id":record.scope.task_id,
                "task_revision":record.scope.task_revision,
                "attempt_id":record.scope.attempt_id,
                "evaluated_at_ms":now,
                "evaluated_by":actor,
                "evidence":null,
            }));
        }
        Err(error) => return Err(error),
    };
    if task["project_id"] != record.scope.project_id {
        return Err(damaged("Goal Task changed project identity"));
    }
    let attempt = match tasks::get_attempt(db, &record.scope.attempt_id) {
        Ok(attempt) => attempt,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(json!({
                "status":"unknown",
                "completed":false,
                "reason":"assigned_attempt_not_found",
                "criterion":record.completion_evidence,
                "task_id":record.scope.task_id,
                "task_revision":record.scope.task_revision,
                "attempt_id":record.scope.attempt_id,
                "evaluated_at_ms":now,
                "evaluated_by":actor,
                "evidence":null,
            }));
        }
        Err(error) => return Err(error),
    };
    if attempt["task_id"] != record.scope.task_id {
        return Err(damaged("Goal Attempt belongs to a different Task"));
    }

    let same_task_revision = task["revision"] == record.scope.task_revision;
    let accepted_scope = same_task_revision
        && task["state"] == "accepted"
        && task["accepted_attempt_id"] == record.scope.attempt_id
        && task["accepted_revision"] == record.scope.task_revision
        && attempt["task_revision"] == record.scope.task_revision
        && attempt["state"] == "accepted"
        && task["accepted_candidate_ref"] == attempt["candidate_ref"]
        && super::acceptance::accepted_attempt(db, &attempt)?;
    if accepted_scope {
        return Ok(json!({
            "status":"completed",
            "completed":true,
            "reason":"exact_assigned_task_acceptance_verified",
            "criterion":record.completion_evidence,
            "task_id":record.scope.task_id,
            "task_revision":record.scope.task_revision,
            "attempt_id":record.scope.attempt_id,
            "evaluated_at_ms":now,
            "evaluated_by":actor,
            "evidence":{
                "acceptance_operation_id":task["accepted_operation_id"],
                "candidate_ref":task["accepted_candidate_ref"],
                "submission_ref":attempt["submission_ref"],
                "attempt_id":record.scope.attempt_id,
                "task_revision":record.scope.task_revision,
            },
        }));
    }

    let is_current_assignment = same_task_revision
        && task["current_attempt_id"] == record.scope.attempt_id
        && attempt["released_at_ms"].is_null()
        && attempt["task_revision"] == record.scope.task_revision;
    let (status, reason) = if is_current_assignment {
        if task["state"] == "accepted" {
            ("unknown", "acceptance_not_verified_or_invalidated")
        } else {
            ("pending", "assigned_task_not_accepted")
        }
    } else {
        ("unknown", "assigned_task_or_attempt_scope_is_stale")
    };
    Ok(json!({
        "status":status,
        "completed":false,
        "reason":reason,
        "criterion":record.completion_evidence,
        "task_id":record.scope.task_id,
        "task_revision":record.scope.task_revision,
        "attempt_id":record.scope.attempt_id,
        "evaluated_at_ms":now,
        "evaluated_by":actor,
        "evidence":null,
    }))
}

/// Exact task/attempt snapshot consumed by explicit manager-enabled
/// progression. Reminder `enabled` is intentionally absent from eligibility:
/// it controls only the existing one-shot reminder workflow.
pub(crate) fn progression_target(
    db: &Connection,
    project_id: &str,
    task_id: &str,
    goal_id: &str,
    now: i64,
) -> Result<Option<Value>> {
    let record = match load_record(db, project_id, task_id, goal_id) {
        Ok(record) => record,
        Err(error) if error.code == "NOT_FOUND" => return Ok(None),
        Err(error) => return Err(error),
    };
    let task = tasks::get_task(db, task_id)?;
    let attempt = tasks::get_attempt(db, &record.scope.attempt_id)?;
    if task["project_id"] != project_id || attempt["task_id"] != task_id {
        return Err(damaged("Goal progression target crossed its Task scope"));
    }
    let completion = readback_value(db, &record, now, "automation_goal_progression")?;
    let binding_id = attempt["binding_id"].as_str().map(str::to_owned);
    let binding_generation = attempt["binding_generation"].as_i64();
    let current = task["state"] == "open"
        && task["revision"] == record.scope.task_revision
        && task["current_attempt_id"] == record.scope.attempt_id
        && attempt["attempt_id"] == record.scope.attempt_id
        && attempt["task_revision"] == record.scope.task_revision
        && attempt["released_at_ms"].is_null()
        && binding_id.is_some()
        && binding_generation.is_some_and(|generation| generation > 0);
    Ok(Some(json!({
        "goal_id":record.goal_id,
        "project_id":record.scope.project_id,
        "scope":record.scope,
        "revision":record.revision,
        "objective":record.objective,
        "created_by_manager_id":record.created_by_manager_id,
        "updated_by_manager_id":record.updated_by_manager_id,
        "completion":completion,
        "active":current && completion["status"] == "pending",
        "binding_id":binding_id,
        "binding_generation":binding_generation,
    })))
}

fn scope_is_current_assignment(db: &Connection, scope: &Scope) -> Result<bool> {
    let task = tasks::get_task(db, &scope.task_id)?;
    if task["project_id"] != scope.project_id || task["revision"] != scope.task_revision {
        return Ok(false);
    }
    let attempt = tasks::get_attempt(db, &scope.attempt_id)?;
    if attempt["task_id"] != scope.task_id || attempt["task_revision"] != scope.task_revision {
        return Ok(false);
    }
    Ok(
        (task["current_attempt_id"] == scope.attempt_id && attempt["released_at_ms"].is_null())
            || (task["state"] == "accepted"
                && task["accepted_attempt_id"] == scope.attempt_id
                && task["accepted_revision"] == scope.task_revision
                && attempt["state"] == "accepted"
                && super::acceptance::accepted_attempt(db, &attempt)?),
    )
}

fn authorize_scope(db: &Connection, principal: &Principal, scope: &Scope) -> Result<()> {
    let principal = current_principal(db, principal.clone())?;
    let task = tasks::get_task(db, &scope.task_id)?;
    if task["project_id"] != scope.project_id {
        return Err(Error::new(
            "FORBIDDEN",
            "Goal project scope does not match its Task",
        ));
    }
    let attempt = tasks::get_attempt(db, &scope.attempt_id)?;
    if attempt["task_id"] != scope.task_id {
        return Err(Error::new(
            "FORBIDDEN",
            "Goal Attempt is outside the requested Task",
        ));
    }
    if principal.role == Role::Operator {
        gm::require_authority(db, &principal)?;
        return Ok(());
    }
    if principal.role != Role::Manager {
        return Err(Error::new(
            "FORBIDDEN",
            "Goal access requires an assigned Manager or current GM",
        ));
    }
    let attempt_owner = model::text(&attempt, "owner_id")?;
    if principal.owns(attempt_owner).is_ok() {
        return Ok(());
    }
    gm::require_authority(db, &principal)?;
    if authorization::current_manager_has_task_scope(
        db,
        &principal,
        &scope.task_id,
        &scope.project_id,
    )? {
        Ok(())
    } else {
        Err(Error::new(
            "FORBIDDEN",
            "current GM lacks authority for this Task and project",
        ))
    }
}

fn require_current_scope(db: &Connection, principal: &Principal, scope: &Scope) -> Result<()> {
    authorize_scope(db, principal, scope)?;
    if scope_is_current_assignment(db, scope)? {
        Ok(())
    } else {
        Err(Error::new(
            "GOAL_SCOPE_STALE",
            "mutation requires the exact current Task revision and assigned Attempt",
        ))
    }
}

fn scope_matches_record(record: &GoalRecord, scope: &Scope) -> bool {
    record.scope == *scope || record.scope_history.iter().any(|prior| prior == scope)
}

fn projection(record: &GoalRecord) -> Value {
    json!({
        "goal_id":record.goal_id,
        "project_id":record.scope.project_id,
        "task_id":record.scope.task_id,
        "task_revision":record.scope.task_revision,
        "attempt_id":record.scope.attempt_id,
        "scope_history":record.scope_history,
        "revision":record.revision,
        "objective":record.objective,
        "completion_evidence":record.completion_evidence,
        "enabled":record.enabled,
        "reminder":record.reminder,
        "reminder_status":record.reminder_status,
        "last_reminder":record.last_reminder,
        "created_by_manager_id":record.created_by_manager_id,
        "created_at_ms":record.created_at_ms,
        "updated_by_manager_id":record.updated_by_manager_id,
        "updated_at_ms":record.updated_at_ms,
        "last_readback":record.last_readback,
    })
}

fn check_revision(record: &GoalRecord, expected: i64) -> Result<()> {
    if record.revision != expected {
        return Err(Error::new(
            "GOAL_REVISION_CONFLICT",
            format!(
                "expected Goal revision {expected}, found {}",
                record.revision
            ),
        ));
    }
    Ok(())
}

fn check_new_reminder(record: &GoalRecord, reminder: &ReminderConfig, now: i64) -> Result<()> {
    if reminder.due_at_ms <= now {
        return Err(Error::new(
            "GOAL_REMINDER_DUE_IN_PAST",
            "a new one-shot reminder must have a future due_at_ms",
        ));
    }
    if let Some(previous) = &record.last_reminder
        && previous.outcome == "delivered"
    {
        let earliest = previous.recorded_at_ms.saturating_add(previous.cooldown_ms);
        if reminder.due_at_ms < earliest {
            return Err(Error::new(
                "GOAL_REMINDER_COOLDOWN",
                format!("the next reminder cannot be due before {earliest}"),
            ));
        }
    }
    Ok(())
}

fn apply_create(
    tx: &Transaction<'_>,
    principal: &Principal,
    command: CreateCommand,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let CreateCommand {
        scope,
        goal_id,
        objective,
        completion_evidence,
        reminder,
        enabled,
    } = command;
    require_current_scope(tx, principal, &scope)?;
    let key = record_key(&scope.project_id, &scope.task_id, &goal_id)?;
    if meta(tx, &key)?.is_some() {
        return Err(Error::new(
            "GOAL_ALREADY_EXISTS",
            "Goal ID already exists for this Task",
        ));
    }
    let mut record = GoalRecord {
        schema_version: RECORD_SCHEMA,
        goal_id: goal_id.clone(),
        scope: scope.clone(),
        scope_history: Vec::new(),
        revision: 1,
        objective,
        completion_evidence,
        reminder,
        reminder_status: "none".to_owned(),
        enabled,
        created_by_manager_id: principal.client_id.clone(),
        created_at_ms: now,
        updated_by_manager_id: principal.client_id.clone(),
        updated_at_ms: now,
        last_readback: None,
        last_reminder: None,
    };
    if let Some(reminder) = &record.reminder {
        check_new_reminder(&record, reminder, now)?;
        record.reminder_status = if enabled { "scheduled" } else { "disabled" }.to_owned();
    }
    save_record(tx, &record)?;
    create_list_index(tx, &scope, &goal_id)?;
    upsert_due_index(tx, &record)?;
    link_operation(tx, operation_id, &scope)?;
    Ok((json!({"goal":projection(&record),"created":true}), false))
}

fn apply_revise(
    tx: &Transaction<'_>,
    principal: &Principal,
    command: ReviseCommand,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let ReviseCommand {
        scope,
        goal_id,
        expected_revision,
        objective,
        completion_evidence,
        reminder: reminder_patch,
        enabled: enabled_patch,
    } = command;
    require_current_scope(tx, principal, &scope)?;
    let mut record = load_record(tx, &scope.project_id, &scope.task_id, &goal_id)?;
    check_revision(&record, expected_revision)?;
    let scope_changed = record.scope != scope;
    if objective.is_none()
        && completion_evidence.is_none()
        && reminder_patch.is_none()
        && enabled_patch.is_none()
        && !scope_changed
    {
        return Err(Error::invalid(
            "goal.revise must change at least one field or assignment scope",
        ));
    }
    let objective_changed = objective
        .as_ref()
        .is_some_and(|value| value != &record.objective);
    let completion_evidence_changed = completion_evidence
        .as_ref()
        .is_some_and(|value| value != &record.completion_evidence);
    if scope_changed {
        if record.scope_history.len() >= MAX_SCOPE_HISTORY {
            return Err(Error::new(
                "GOAL_SCOPE_HISTORY_LIMIT",
                "Goal assignment history has reached its retained limit",
            ));
        }
        if !record.scope_history.contains(&record.scope) {
            record.scope_history.push(record.scope.clone());
        }
        create_list_index(tx, &scope, &goal_id)?;
        record.scope = scope.clone();
    }
    if let Some(objective) = objective {
        record.objective = objective;
    }
    if let Some(completion_evidence) = completion_evidence {
        record.completion_evidence = completion_evidence;
        // An evidence contract change invalidates only the cached evaluator
        // projection; its source receipts and Operation history remain.
        record.last_readback = None;
    }
    let was_enabled = record.enabled;
    let old_reminder = record.reminder.clone();
    let mut reminder_changed = false;
    if let Some(reminder) = reminder_patch {
        if let Some(reminder) = &reminder
            && record.reminder.as_ref() != Some(reminder)
        {
            check_new_reminder(&record, reminder, now)?;
        }
        reminder_changed = record.reminder != reminder;
        if reminder_changed {
            remove_due_index(tx, &record)?;
        }
        record.reminder = reminder;
    }
    let final_enabled = enabled_patch.unwrap_or(record.enabled);
    if final_enabled
        && !was_enabled
        && let Some(reminder) = &record.reminder
        && !reminder_changed
    {
        check_new_reminder(&record, reminder, now)?;
    }
    if scope_changed
        && final_enabled
        && !reminder_changed
        && record
            .reminder
            .as_ref()
            .is_some_and(|reminder| reminder.due_at_ms <= now)
    {
        return Err(Error::new(
            "GOAL_REMINDER_DUE_IN_PAST",
            "reassign the Goal with a new future reminder or clear its expired reminder",
        ));
    }
    if was_enabled && !final_enabled && !reminder_changed {
        remove_due_index(tx, &record)?;
    }
    if final_enabled != record.enabled || reminder_changed || scope_changed {
        record.enabled = final_enabled;
        record.reminder_status = match (&record.reminder, record.enabled) {
            (Some(_), true) => "scheduled",
            (Some(_), false) => "disabled",
            (None, _) => "none",
        }
        .to_owned();
    } else if record.reminder != old_reminder {
        // Kept as a defensive invariant if a future patch adds another
        // reminder representation without changing the due-slot identity.
        return Err(damaged("Goal reminder patch did not update its state"));
    }
    let changed = scope_changed
        || objective_changed
        || completion_evidence_changed
        || reminder_changed
        || enabled_patch.is_some_and(|value| value != was_enabled);
    if !changed {
        link_operation(tx, operation_id, &scope)?;
        return Ok((json!({"goal":projection(&record),"changed":false}), false));
    }
    record.revision = record
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::new("GOAL_REVISION_OVERFLOW", "Goal revision overflow"))?;
    record.updated_by_manager_id = principal.client_id.clone();
    record.updated_at_ms = now;
    save_record(tx, &record)?;
    upsert_due_index(tx, &record)?;
    link_operation(tx, operation_id, &scope)?;
    Ok((json!({"goal":projection(&record),"changed":true}), false))
}

fn apply_enable_disable(
    tx: &Transaction<'_>,
    principal: &Principal,
    command: ReminderToggle,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let ReminderToggle {
        scope,
        goal_id,
        expected_revision,
        enabled,
    } = command;
    require_current_scope(tx, principal, &scope)?;
    let mut record = load_record(tx, &scope.project_id, &scope.task_id, &goal_id)?;
    if record.scope != scope {
        return Err(Error::new(
            "GOAL_SCOPE_STALE",
            "enable/disable applies only to the Goal's current assigned Attempt; revise its scope first",
        ));
    }
    check_revision(&record, expected_revision)?;
    if record.enabled == enabled {
        link_operation(tx, operation_id, &scope)?;
        return Ok((json!({"goal":projection(&record),"changed":false}), false));
    }
    if enabled && let Some(reminder) = &record.reminder {
        check_new_reminder(&record, reminder, now)?;
    }
    if !enabled {
        remove_due_index(tx, &record)?;
    }
    record.enabled = enabled;
    record.reminder_status = match (&record.reminder, enabled) {
        (Some(_), true) => "scheduled",
        (Some(_), false) => "disabled",
        (None, _) => "none",
    }
    .to_owned();
    record.revision = record
        .revision
        .checked_add(1)
        .ok_or_else(|| Error::new("GOAL_REVISION_OVERFLOW", "Goal revision overflow"))?;
    record.updated_by_manager_id = principal.client_id.clone();
    record.updated_at_ms = now;
    save_record(tx, &record)?;
    upsert_due_index(tx, &record)?;
    link_operation(tx, operation_id, &scope)?;
    Ok((json!({"goal":projection(&record),"changed":true}), false))
}

fn apply_readback(
    tx: &Transaction<'_>,
    principal: &Principal,
    scope: Scope,
    goal_id: String,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    authorize_scope(tx, principal, &scope)?;
    let mut record = load_record(tx, &scope.project_id, &scope.task_id, &goal_id)?;
    if record.scope != scope {
        return Err(Error::new(
            "GOAL_SCOPE_STALE",
            "goal.readback requires the Goal's current assigned Task and Attempt scope",
        ));
    }
    let current = current_principal(tx, principal.clone())?;
    let readback = readback_value(tx, &record, now, &current.client_id)?;
    record.last_readback = Some(readback.clone());
    save_record(tx, &record)?;
    link_operation(tx, operation_id, &scope)?;
    Ok((
        json!({"goal_id":goal_id,"revision":record.revision,"readback":readback}),
        false,
    ))
}

fn link_operation(tx: &Transaction<'_>, operation_id: &str, scope: &Scope) -> Result<()> {
    tx.execute(
        "UPDATE operations SET task_id=?2,attempt_id=?3 WHERE operation_id=?1",
        params![operation_id, scope.task_id, scope.attempt_id],
    )?;
    Ok(())
}

/// Apply one receipt-backed Goal mutation. Every action is synchronous metadata
/// work; the returned queue flag is always false because Goals never dispatch
/// model/native work.
pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let current = current_principal(tx, principal.clone())?;
    match api::parse_mutation(method, value)? {
        Mutation::Create {
            scope,
            goal_id,
            objective,
            completion_evidence,
            reminder,
            enabled,
        } => apply_create(
            tx,
            &current,
            CreateCommand {
                scope,
                goal_id,
                objective,
                completion_evidence,
                reminder,
                enabled,
            },
            operation_id,
            now,
        ),
        Mutation::Revise {
            scope,
            goal_id,
            expected_revision,
            objective,
            completion_evidence,
            reminder,
            enabled,
        } => apply_revise(
            tx,
            &current,
            ReviseCommand {
                scope,
                goal_id,
                expected_revision,
                objective,
                completion_evidence,
                reminder,
                enabled,
            },
            operation_id,
            now,
        ),
        Mutation::Enable {
            scope,
            goal_id,
            expected_revision,
        } => apply_enable_disable(
            tx,
            &current,
            ReminderToggle {
                scope,
                goal_id,
                expected_revision,
                enabled: true,
            },
            operation_id,
            now,
        ),
        Mutation::Disable {
            scope,
            goal_id,
            expected_revision,
        } => apply_enable_disable(
            tx,
            &current,
            ReminderToggle {
                scope,
                goal_id,
                expected_revision,
                enabled: false,
            },
            operation_id,
            now,
        ),
        Mutation::Readback { scope, goal_id } => {
            apply_readback(tx, &current, scope, goal_id, operation_id, now)
        }
    }
}

fn read_one(db: &Connection, principal: &Principal, scope: &Scope, goal_id: &str) -> Result<Value> {
    authorize_scope(db, principal, scope)?;
    let record = load_record(db, &scope.project_id, &scope.task_id, goal_id)?;
    if !scope_matches_record(&record, scope) {
        return Err(Error::new(
            "NOT_FOUND",
            "Goal not found in the requested assignment scope",
        ));
    }
    Ok(projection(&record))
}

pub(super) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<Value> {
    match api::parse_read(method, value)? {
        ReadRequest::Get { scope, goal_id } => Ok(json!({
            "goal":read_one(db, principal, &scope, &goal_id)?,
            "coverage":"complete",
            "gaps":[],
        })),
        ReadRequest::List {
            scope,
            limit,
            after_goal_id,
        } => list(db, principal, &scope, limit, after_goal_id.as_deref()),
    }
}

fn list(
    db: &Connection,
    principal: &Principal,
    scope: &Scope,
    limit: i64,
    after_goal_id: Option<&str>,
) -> Result<Value> {
    authorize_scope(db, principal, scope)?;
    let prefix = scoped_list_prefix(scope)?;
    let upper = format!("{prefix}g");
    let after_key = after_goal_id
        .map(|goal_id| scope_list_key(scope, goal_id))
        .transpose()?
        .unwrap_or_else(|| prefix.clone());
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 AND key>?3 ORDER BY key LIMIT ?4",
    )?;
    let rows: Vec<(String, String)> = statement
        .query_map(
            params![prefix, upper, after_key, limit.saturating_add(1)],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?
        .collect::<std::result::Result<_, _>>()?;
    drop(statement);
    let has_more = rows.len() as i64 > limit;
    let mut items = Vec::new();
    for (index_key, raw) in rows.iter().take(limit as usize) {
        let index: Value = serde_json::from_str(raw)?;
        if index["schema_version"] != RECORD_SCHEMA
            || index["project_id"] != scope.project_id
            || index["task_id"] != scope.task_id
            || index["task_revision"] != scope.task_revision
            || index["attempt_id"] != scope.attempt_id
        {
            return Err(damaged("Goal list index identity is invalid"));
        }
        let goal_id = model::text(&index, "goal_id")?;
        if *index_key != scope_list_key(scope, goal_id)? {
            return Err(damaged("Goal list index key is invalid"));
        }
        let record = load_record(db, &scope.project_id, &scope.task_id, goal_id)?;
        if !scope_matches_record(&record, scope) {
            return Err(damaged(
                "Goal list index points outside its assignment history",
            ));
        }
        items.push(projection(&record));
    }
    let next_after_goal_id = items
        .last()
        .and_then(|item| item.get("goal_id"))
        .cloned()
        .unwrap_or(Value::Null);
    Ok(json!({
        "items":items,
        "project_id":scope.project_id,
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
        "next_after_goal_id":next_after_goal_id,
        "coverage":if has_more {"partial"} else {"complete"},
        "gaps":[],
    }))
}

fn due_upper_bound(now: i64) -> String {
    format!("{DUE_PREFIX}{now:019}~")
}

fn parse_due_key(key: &str) -> Result<i64> {
    let due = key
        .strip_prefix(DUE_PREFIX)
        .and_then(|tail| tail.get(..19))
        .ok_or_else(|| damaged("Goal due index key has no timestamp"))?;
    due.parse::<i64>()
        .map_err(|_| damaged("Goal due index timestamp is invalid"))
}

fn hold_due_index(
    tx: &Transaction<'_>,
    index_key: &str,
    raw_index: &str,
    related_fact: Option<(&str, &str, &str)>,
    reason: &str,
    now: i64,
) -> Result<()> {
    if let Some((source_kind, source_key, raw)) = related_fact {
        quarantine_optional_fact(tx, source_kind, source_key, raw, reason, now)?;
    }
    quarantine_optional_fact(tx, "due_index", index_key, raw_index, reason, now)?;
    tx.execute("DELETE FROM meta WHERE key=?1", [index_key])?;
    Ok(())
}

fn slot_receipt_from_raw(raw: &str) -> Result<ReminderSlotReceipt> {
    serde_json::from_str(raw).map_err(|_| damaged("Goal reminder receipt is malformed"))
}

fn validate_slot_receipt(
    receipt: &ReminderSlotReceipt,
    record: &GoalRecord,
    due_at_ms: i64,
    slot_id: &str,
) -> Result<()> {
    let receipt_scope = Scope {
        project_id: receipt.project_id.clone(),
        task_id: receipt.task_id.clone(),
        task_revision: receipt.task_revision,
        attempt_id: receipt.attempt_id.clone(),
    };
    let outcome_matches = match receipt.outcome.as_str() {
        "delivered" => matches!(receipt.completion_status.as_str(), "pending" | "unknown"),
        "suppressed_completed" => receipt.completion_status == "completed",
        "suppressed_stale_scope" => receipt.completion_status == "unknown",
        _ => false,
    };
    if receipt.schema_version != RECORD_SCHEMA
        || receipt.notification_id != slot_id
        || receipt.goal_id != record.goal_id
        || receipt.project_id != record.scope.project_id
        || receipt.task_id != record.scope.task_id
        || receipt.due_at_ms != due_at_ms
        || receipt.task_revision <= 0
        || receipt.attempt_id.is_empty()
        || receipt.goal_revision <= 0
        || receipt.goal_revision > record.revision
        || receipt.recorded_at_ms < receipt.due_at_ms
        || !(0..=api::MAX_COOLDOWN_MS).contains(&receipt.cooldown_ms)
        || !outcome_matches
        || !scope_matches_record(record, &receipt_scope)
    {
        return Err(damaged(
            "Goal reminder receipt identity or outcome is invalid",
        ));
    }
    Ok(())
}

fn load_due_rows(db: &Connection, now: i64, limit: usize) -> Result<Vec<(String, String)>> {
    let upper = due_upper_bound(now);
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT ?3",
    )?;
    let rows = statement
        .query_map(params![DUE_PREFIX, upper, limit as i64], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

/// Read the actual earliest indexed reminder due time for the shared scheduler.
pub(super) fn next_due_at_ms(db: &Connection) -> Result<Option<i64>> {
    let upper = format!("{DUE_PREFIX}~");
    let now = model::now_ms()?;
    loop {
        let row: Option<(String, String)> = db
            .query_row(
                "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT 1",
                params![DUE_PREFIX, upper],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((key, raw)) = row else {
            return Ok(None);
        };
        match parse_due_key(&key) {
            Ok(due) => return Ok(Some(due)),
            Err(error) if error.code == "GOAL_RECORD_CORRUPT" => {
                quarantine_optional_fact(
                    db,
                    "due_index",
                    &key,
                    &raw,
                    "due_index_key_invalid",
                    now,
                )?;
                db.execute("DELETE FROM meta WHERE key=?1", [key])?;
            }
            Err(error) => return Err(error),
        }
    }
}

fn clear_due_record(tx: &Transaction<'_>, index_key: &str, record: &mut GoalRecord) -> Result<()> {
    tx.execute("DELETE FROM meta WHERE key=?1", [index_key])?;
    record.reminder = None;
    Ok(())
}

fn notice_summary(objective: &str) -> String {
    objective.chars().take(180).collect()
}

fn validate_notice_shell(notice: &ReminderNotice, stored_key: &str) -> Result<()> {
    let slot_id = reminder_slot_id_for(
        &notice.project_id,
        &notice.task_id,
        &notice.goal_id,
        notice.due_at_ms,
    )?;
    if notice.schema_version != RECORD_SCHEMA
        || notice.project_id.is_empty()
        || notice.task_id.is_empty()
        || notice.task_revision <= 0
        || notice.attempt_id.is_empty()
        || notice.goal_id.is_empty()
        || notice.due_at_ms <= 0
        || notice.recorded_at_ms < notice.due_at_ms
        || notice.goal_revision <= 0
        || notice.created_by_manager_id.is_empty()
        || notice.objective_summary.chars().count() > 180
        || !matches!(notice.completion_status.as_str(), "pending" | "unknown")
        || notice.notification_id != slot_id
        || stored_key != notice_key(notice)?
    {
        return Err(damaged("Goal reminder header identity is invalid"));
    }
    Ok(())
}

fn validate_notice(
    notice: &ReminderNotice,
    notice_key_value: &str,
    record: &GoalRecord,
    receipt: &ReminderSlotReceipt,
) -> Result<()> {
    let notice_scope = Scope {
        project_id: notice.project_id.clone(),
        task_id: notice.task_id.clone(),
        task_revision: notice.task_revision,
        attempt_id: notice.attempt_id.clone(),
    };
    let slot_id = reminder_slot_id_for(
        &notice.project_id,
        &notice.task_id,
        &notice.goal_id,
        notice.due_at_ms,
    )?;
    if notice.schema_version != RECORD_SCHEMA
        || notice_scope.task_revision <= 0
        || notice_scope.attempt_id.is_empty()
        || notice.due_at_ms <= 0
        || notice.recorded_at_ms < notice.due_at_ms
        || notice.goal_revision <= 0
        || notice.goal_revision > record.revision
        || notice.created_by_manager_id != record.created_by_manager_id
        || notice.objective_summary.chars().count() > 180
        || !matches!(notice.completion_status.as_str(), "pending" | "unknown")
        || notice.notification_id != slot_id
        || notice_key_value != notice_key(notice)?
        || notice.goal_id != record.goal_id
        || !scope_matches_record(record, &notice_scope)
        || receipt.outcome != "delivered"
        || receipt.notification_id != notice.notification_id
        || receipt.goal_id != notice.goal_id
        || receipt.project_id != notice.project_id
        || receipt.task_id != notice.task_id
        || receipt.task_revision != notice.task_revision
        || receipt.attempt_id != notice.attempt_id
        || receipt.due_at_ms != notice.due_at_ms
        || receipt.recorded_at_ms != notice.recorded_at_ms
        || receipt.goal_revision != notice.goal_revision
        || receipt.completion_status != notice.completion_status
    {
        return Err(damaged("Goal reminder header identity is invalid"));
    }
    validate_slot_receipt(receipt, record, notice.due_at_ms, &slot_id)
}

fn public_quarantine_error(diagnostic: &QuarantineDiagnostic) -> Value {
    json!({
        "kind":"goal_optional_data",
        "code":diagnostic.error_code,
        "source_kind":diagnostic.source_kind,
        "source_id":diagnostic.source_key_digest,
        "reason":diagnostic.reason,
        "recorded_at_ms":diagnostic.recorded_at_ms,
    })
}

fn recent_quarantine_errors(db: &Connection, bound: usize) -> Result<(Vec<Value>, bool)> {
    let upper = format!("{QUARANTINE_TIME_PREFIX}~");
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key DESC LIMIT ?3",
    )?;
    let rows = statement
        .query_map(
            params![
                QUARANTINE_TIME_PREFIX,
                upper,
                bound.saturating_add(1) as i64
            ],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let has_more = rows.len() > bound;
    let mut errors = Vec::new();
    for (time_key, raw_index) in rows.iter().take(bound) {
        let Some((timestamp, identity)) = time_key
            .strip_prefix(QUARANTINE_TIME_PREFIX)
            .and_then(|suffix| suffix.split_once(':'))
        else {
            errors.push(json!({
                "kind":"goal_optional_data",
                "code":"GOAL_QUARANTINE_INDEX_CORRUPT",
                "source_id":model::digest(time_key.as_bytes()),
            }));
            continue;
        };
        let timestamp_ms = match timestamp.parse::<i64>() {
            Ok(timestamp) => timestamp,
            Err(_) => {
                errors.push(json!({
                    "kind":"goal_optional_data",
                    "code":"GOAL_QUARANTINE_INDEX_CORRUPT",
                    "source_id":model::digest(time_key.as_bytes()),
                }));
                continue;
            }
        };
        let parsed_index: Value = match serde_json::from_str(raw_index) {
            Ok(index) => index,
            Err(_) => {
                errors.push(json!({
                    "kind":"goal_optional_data",
                    "code":"GOAL_QUARANTINE_INDEX_CORRUPT",
                    "source_id":model::digest(time_key.as_bytes()),
                }));
                continue;
            }
        };
        let Some(diagnostic_key) = parsed_index
            .get("diagnostic_key")
            .and_then(Value::as_str)
            .filter(|key| key.starts_with(QUARANTINE_DIAGNOSTIC_PREFIX))
        else {
            errors.push(json!({
                "kind":"goal_optional_data",
                "code":"GOAL_QUARANTINE_INDEX_CORRUPT",
                "source_id":model::digest(time_key.as_bytes()),
            }));
            continue;
        };
        let Some(raw_diagnostic) = raw_meta(db, diagnostic_key)? else {
            errors.push(json!({
                "kind":"goal_optional_data",
                "code":"GOAL_QUARANTINE_DIAGNOSTIC_MISSING",
                "source_id":model::digest(diagnostic_key.as_bytes()),
            }));
            continue;
        };
        let diagnostic: QuarantineDiagnostic = match serde_json::from_str(&raw_diagnostic) {
            Ok(diagnostic) => diagnostic,
            Err(_) => {
                errors.push(json!({
                    "kind":"goal_optional_data",
                    "code":"GOAL_QUARANTINE_DIAGNOSTIC_CORRUPT",
                    "source_id":model::digest(diagnostic_key.as_bytes()),
                }));
                continue;
            }
        };
        let (source_digest, raw_digest, fact_key, expected_diag_key) =
            quarantine_identity_from_digests(&diagnostic.source_key_digest, &diagnostic.raw_digest);
        let expected_time_key = format!(
            "{QUARANTINE_TIME_PREFIX}{:019}:{source_digest}:{raw_digest}",
            diagnostic.recorded_at_ms
        );
        let raw_fact = raw_meta(db, &diagnostic.fact_key)?;
        let reason_is_known = matches!(
            diagnostic.reason.as_str(),
            "due_index_key_invalid"
                | "due_index_malformed"
                | "due_index_identity_invalid"
                | "due_index_target_missing"
                | "goal_record_malformed"
                | "goal_record_identity_invalid"
                | "due_index_has_no_goal_reminder"
                | "due_index_does_not_match_goal"
                | "due_index_slot_invalid"
                | "reminder_receipt_malformed"
                | "reminder_receipt_identity_invalid"
                | "reminder_notice_malformed"
                | "reminder_notice_identity_invalid"
                | "reminder_notice_goal_missing"
                | "reminder_notice_goal_record_invalid"
                | "reminder_notice_receipt_missing"
                | "reminder_notice_receipt_malformed"
                | "reminder_notice_link_invalid"
        );
        if diagnostic.schema_version != RECORD_SCHEMA
            || diagnostic.error_code != "GOAL_OPTIONAL_DATA_HELD"
            || !matches!(
                diagnostic.source_kind.as_str(),
                "due_index" | "goal_record" | "reminder_receipt" | "reminder_notice"
            )
            || diagnostic.source_key_digest != source_digest
            || diagnostic.raw_digest != raw_digest
            || !is_digest(&source_digest)
            || !is_digest(&raw_digest)
            || diagnostic.fact_key != fact_key
            || !reason_is_known
            || diagnostic.recorded_at_ms != timestamp_ms
            || identity != format!("{source_digest}:{raw_digest}")
            || *time_key != expected_time_key
            || expected_diag_key != diagnostic_key
            || raw_fact
                .as_ref()
                .is_none_or(|raw| model::digest(raw.as_bytes()) != raw_digest)
        {
            errors.push(json!({
                "kind":"goal_optional_data",
                "code":"GOAL_QUARANTINE_DIAGNOSTIC_CORRUPT",
                "source_id":model::digest(diagnostic_key.as_bytes()),
            }));
            continue;
        }
        errors.push(public_quarantine_error(&diagnostic));
    }
    Ok((errors, has_more))
}

fn quarantine_identity_from_digests(
    source_digest: &str,
    raw_digest: &str,
) -> (String, String, String, String) {
    let identity = format!("{source_digest}:{raw_digest}");
    (
        source_digest.to_owned(),
        raw_digest.to_owned(),
        format!("{QUARANTINE_FACT_PREFIX}{identity}"),
        format!("{QUARANTINE_DIAGNOSTIC_PREFIX}{identity}"),
    )
}

fn is_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Consume a bounded due page through the host's existing shared automation
/// pump. The due slot receipt, Goal projection and visible reminder header are
/// written in one transaction; no volatile notification is required for
/// recovery and no Task/model action is queued.
pub(super) fn reconcile(tx: &Transaction<'_>, now: i64, bound: usize) -> Result<Value> {
    let bound = bound.clamp(1, MAX_RECONCILE_LIMIT);
    let rows = load_due_rows(tx, now, bound.saturating_add(1))?;
    let has_more = rows.len() > bound;
    let mut delivered = 0usize;
    let mut suppressed_completed = 0usize;
    let mut suppressed_stale = 0usize;
    let mut held_optional_data = 0usize;
    for (index_key, raw_index) in rows.iter().take(bound) {
        let due = match parse_due_key(index_key) {
            Ok(due) => due,
            Err(error) if error.code == "GOAL_RECORD_CORRUPT" => {
                hold_due_index(tx, index_key, raw_index, None, "due_index_key_invalid", now)?;
                held_optional_data += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        let index: DueIndex = match serde_json::from_str(raw_index) {
            Ok(index) => index,
            Err(_) => {
                hold_due_index(tx, index_key, raw_index, None, "due_index_malformed", now)?;
                held_optional_data += 1;
                continue;
            }
        };
        if index.schema_version != RECORD_SCHEMA
            || index.due_at_ms != due
            || index.record_key.strip_prefix(RECORD_PREFIX).is_none()
            || *index_key != due_index_key(&index.record_key, due)?
        {
            hold_due_index(
                tx,
                index_key,
                raw_index,
                None,
                "due_index_identity_invalid",
                now,
            )?;
            held_optional_data += 1;
            continue;
        }
        let Some(raw_record) = raw_meta(tx, &index.record_key)? else {
            hold_due_index(
                tx,
                index_key,
                raw_index,
                None,
                "due_index_target_missing",
                now,
            )?;
            held_optional_data += 1;
            continue;
        };
        let mut record: GoalRecord = match serde_json::from_str(&raw_record) {
            Ok(record) => record,
            Err(_) => {
                hold_due_index(
                    tx,
                    index_key,
                    raw_index,
                    Some(("goal_record", &index.record_key, &raw_record)),
                    "goal_record_malformed",
                    now,
                )?;
                held_optional_data += 1;
                continue;
            }
        };
        let record_identity = record_key(
            &record.scope.project_id,
            &record.scope.task_id,
            &index.goal_id,
        )?;
        let record_validation = verify_record(
            &record,
            &record.scope.project_id,
            &record.scope.task_id,
            &index.goal_id,
        );
        if record_identity != index.record_key
            || matches!(&record_validation, Err(error) if error.code == "GOAL_RECORD_CORRUPT")
        {
            hold_due_index(
                tx,
                index_key,
                raw_index,
                Some(("goal_record", &index.record_key, &raw_record)),
                "goal_record_identity_invalid",
                now,
            )?;
            held_optional_data += 1;
            continue;
        }
        record_validation?;
        let Some(reminder) = record.reminder.clone() else {
            hold_due_index(
                tx,
                index_key,
                raw_index,
                None,
                "due_index_has_no_goal_reminder",
                now,
            )?;
            held_optional_data += 1;
            continue;
        };
        if !record.enabled || reminder.due_at_ms != due || record.goal_id != index.goal_id {
            hold_due_index(
                tx,
                index_key,
                raw_index,
                None,
                "due_index_does_not_match_goal",
                now,
            )?;
            held_optional_data += 1;
            continue;
        }
        let slot_id = reminder_slot_id(&record, due)?;
        if index.slot_id != slot_id {
            hold_due_index(
                tx,
                index_key,
                raw_index,
                None,
                "due_index_slot_invalid",
                now,
            )?;
            held_optional_data += 1;
            continue;
        }
        let receipt_key = reminder_receipt_key(&slot_id);
        if let Some(raw_receipt) = raw_meta(tx, &receipt_key)? {
            let receipt = match slot_receipt_from_raw(&raw_receipt) {
                Ok(receipt) => receipt,
                Err(_) => {
                    hold_due_index(
                        tx,
                        index_key,
                        raw_index,
                        Some(("reminder_receipt", &receipt_key, &raw_receipt)),
                        "reminder_receipt_malformed",
                        now,
                    )?;
                    held_optional_data += 1;
                    continue;
                }
            };
            match validate_slot_receipt(&receipt, &record, due, &slot_id) {
                Ok(()) => {}
                Err(error) if error.code == "GOAL_RECORD_CORRUPT" => {
                    hold_due_index(
                        tx,
                        index_key,
                        raw_index,
                        Some(("reminder_receipt", &receipt_key, &raw_receipt)),
                        "reminder_receipt_identity_invalid",
                        now,
                    )?;
                    held_optional_data += 1;
                    continue;
                }
                Err(error) => return Err(error),
            }
            clear_due_record(tx, index_key, &mut record)?;
            record.reminder_status = receipt.outcome.clone();
            record.last_reminder = Some(ReminderReceipt {
                notification_id: receipt.notification_id,
                due_at_ms: receipt.due_at_ms,
                cooldown_ms: receipt.cooldown_ms,
                recorded_at_ms: receipt.recorded_at_ms,
                outcome: receipt.outcome,
            });
            save_record(tx, &record)?;
            continue;
        }
        let completion = readback_value(tx, &record, now, "scheduler")?;
        let (outcome, completion_status) = if completion["status"] == "completed" {
            suppressed_completed += 1;
            ("suppressed_completed", "completed")
        } else if completion["status"] == "unknown"
            && completion["reason"] == "assigned_task_or_attempt_scope_is_stale"
        {
            suppressed_stale += 1;
            ("suppressed_stale_scope", "unknown")
        } else {
            delivered += 1;
            (
                "delivered",
                completion["status"].as_str().unwrap_or("unknown"),
            )
        };
        clear_due_record(tx, index_key, &mut record)?;
        record.reminder_status = outcome.to_owned();
        record.last_reminder = Some(ReminderReceipt {
            notification_id: slot_id.clone(),
            due_at_ms: due,
            cooldown_ms: reminder.cooldown_ms,
            recorded_at_ms: now,
            outcome: outcome.to_owned(),
        });
        record.last_readback = Some(completion.clone());
        let receipt = json!({
            "schema_version":RECORD_SCHEMA,
            "notification_id":slot_id,
            "goal_id":record.goal_id,
            "project_id":record.scope.project_id,
            "task_id":record.scope.task_id,
            "task_revision":record.scope.task_revision,
            "attempt_id":record.scope.attempt_id,
            "due_at_ms":due,
            "cooldown_ms":reminder.cooldown_ms,
            "recorded_at_ms":now,
            "outcome":outcome,
            "goal_revision":record.revision,
            "completion_status":completion_status,
        });
        set_meta(tx, &receipt_key, &receipt)?;
        if outcome == "delivered" {
            let notice = ReminderNotice {
                schema_version: RECORD_SCHEMA,
                notification_id: slot_id,
                goal_id: record.goal_id.clone(),
                project_id: record.scope.project_id.clone(),
                task_id: record.scope.task_id.clone(),
                task_revision: record.scope.task_revision,
                attempt_id: record.scope.attempt_id.clone(),
                due_at_ms: due,
                recorded_at_ms: now,
                goal_revision: record.revision,
                created_by_manager_id: record.created_by_manager_id.clone(),
                objective_summary: notice_summary(&record.objective),
                completion_status: completion_status.to_owned(),
            };
            set_meta(tx, &notice_key(&notice)?, &json!(notice))?;
        }
        save_record(tx, &record)?;
    }
    Ok(json!({
        "processed":rows.len().min(bound),
        "delivered":delivered,
        "suppressed_completed":suppressed_completed,
        "suppressed_stale_scope":suppressed_stale,
        "held_optional_data":held_optional_data,
        "more_due":has_more,
        "next_due_at_ms":next_due_at_ms(tx)?,
        "model_wake":false,
        "native_work_queued":false,
    }))
}

/// Current-GM/operator view used beside mailbox headers. Other profiles receive
/// a bounded empty envelope so this optional projection cannot break inboxes.
pub(super) fn notifications(db: &Connection, principal: &Principal, limit: i64) -> Result<Value> {
    let principal = current_principal(db, principal.clone())?;
    let authorized = if principal.role == Role::Operator {
        true
    } else if principal.role == Role::Manager {
        match gm::require_authority(db, &principal) {
            Ok(()) => true,
            Err(error) if error.code == "FORBIDDEN" => false,
            Err(error) => return Err(error),
        }
    } else {
        false
    };
    if !authorized {
        return Ok(json!({
            "items":[],
            "coverage":"complete",
            "gaps":[],
            "errors":[],
            "model_wake":false,
            "native_work_queued":false,
        }));
    }
    let limit = if limit <= 0 {
        api::DEFAULT_PAGE_SIZE
    } else {
        limit.min(MAX_NOTIFICATION_LIMIT as i64)
    };
    let upper = format!("{NOTICE_PREFIX}~");
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key DESC LIMIT ?3",
    )?;
    let rows = statement
        .query_map(
            params![NOTICE_PREFIX, upper, limit.saturating_add(1)],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(statement);
    let has_more = rows.len() as i64 > limit;
    let (mut errors, diagnostic_rows_more) = recent_quarantine_errors(db, MAX_DIAGNOSTIC_PAGE)?;
    let mut items = Vec::new();
    for (key, raw) in rows.iter().take(limit as usize) {
        let notice: ReminderNotice = match serde_json::from_str(raw) {
            Ok(notice) => notice,
            Err(_) => {
                let diagnostic = quarantine_optional_fact(
                    db,
                    "reminder_notice",
                    key,
                    raw,
                    "reminder_notice_malformed",
                    model::now_ms()?,
                )?;
                push_quarantine_error(&mut errors, public_quarantine_error(&diagnostic));
                continue;
            }
        };
        match validate_notice_shell(&notice, key) {
            Ok(()) => {}
            Err(error) if error.code == "GOAL_RECORD_CORRUPT" => {
                let diagnostic = quarantine_optional_fact(
                    db,
                    "reminder_notice",
                    key,
                    raw,
                    "reminder_notice_identity_invalid",
                    model::now_ms()?,
                )?;
                push_quarantine_error(&mut errors, public_quarantine_error(&diagnostic));
                continue;
            }
            Err(error) => return Err(error),
        }
        let goal_record_key = record_key(&notice.project_id, &notice.task_id, &notice.goal_id)?;
        let Some(raw_record) = raw_meta(db, &goal_record_key)? else {
            let diagnostic = quarantine_optional_fact(
                db,
                "reminder_notice",
                key,
                raw,
                "reminder_notice_goal_missing",
                model::now_ms()?,
            )?;
            push_quarantine_error(&mut errors, public_quarantine_error(&diagnostic));
            continue;
        };
        let record = match parse_record(
            &raw_record,
            &notice.project_id,
            &notice.task_id,
            &notice.goal_id,
        ) {
            Ok(record) => record,
            Err(error) if error.code == "GOAL_RECORD_CORRUPT" => {
                let diagnostic = quarantine_optional_fact(
                    db,
                    "goal_record",
                    &goal_record_key,
                    &raw_record,
                    "reminder_notice_goal_record_invalid",
                    model::now_ms()?,
                )?;
                quarantine_optional_fact(
                    db,
                    "reminder_notice",
                    key,
                    raw,
                    "reminder_notice_goal_record_invalid",
                    model::now_ms()?,
                )?;
                push_quarantine_error(&mut errors, public_quarantine_error(&diagnostic));
                continue;
            }
            Err(error) => return Err(error),
        };
        let receipt_key = reminder_receipt_key(&notice.notification_id);
        let Some(raw_receipt) = raw_meta(db, &receipt_key)? else {
            let diagnostic = quarantine_optional_fact(
                db,
                "reminder_notice",
                key,
                raw,
                "reminder_notice_receipt_missing",
                model::now_ms()?,
            )?;
            push_quarantine_error(&mut errors, public_quarantine_error(&diagnostic));
            continue;
        };
        let receipt = match slot_receipt_from_raw(&raw_receipt) {
            Ok(receipt) => receipt,
            Err(_) => {
                let diagnostic = quarantine_optional_fact(
                    db,
                    "reminder_receipt",
                    &receipt_key,
                    &raw_receipt,
                    "reminder_notice_receipt_malformed",
                    model::now_ms()?,
                )?;
                quarantine_optional_fact(
                    db,
                    "reminder_notice",
                    key,
                    raw,
                    "reminder_notice_receipt_malformed",
                    model::now_ms()?,
                )?;
                push_quarantine_error(&mut errors, public_quarantine_error(&diagnostic));
                continue;
            }
        };
        match validate_notice(&notice, key, &record, &receipt) {
            Ok(()) => {}
            Err(error) if error.code == "GOAL_RECORD_CORRUPT" => {
                let diagnostic = quarantine_optional_fact(
                    db,
                    "reminder_notice",
                    key,
                    raw,
                    "reminder_notice_link_invalid",
                    model::now_ms()?,
                )?;
                quarantine_optional_fact(
                    db,
                    "reminder_receipt",
                    &receipt_key,
                    &raw_receipt,
                    "reminder_notice_link_invalid",
                    model::now_ms()?,
                )?;
                push_quarantine_error(&mut errors, public_quarantine_error(&diagnostic));
                continue;
            }
            Err(error) => return Err(error),
        }
        if principal.role == Role::Manager
            && !authorization::current_manager_has_task_scope(
                db,
                &principal,
                &notice.task_id,
                &notice.project_id,
            )?
        {
            continue;
        }
        items.push(json!({
            "kind":"goal_reminder",
            "notification_id":notice.notification_id,
            "goal_id":notice.goal_id,
            "project_id":notice.project_id,
            "task_id":notice.task_id,
            "task_revision":notice.task_revision,
            "attempt_id":notice.attempt_id,
            "due_at_ms":notice.due_at_ms,
            "recorded_at_ms":notice.recorded_at_ms,
            "goal_revision":notice.goal_revision,
            "created_by_manager_id":notice.created_by_manager_id,
            "objective_summary":notice.objective_summary,
            "completion_status":notice.completion_status,
            "model_wake":false,
            "native_work_queued":false,
        }));
    }
    let next_after = items
        .last()
        .and_then(|item| item.get("notification_id"))
        .cloned()
        .unwrap_or(Value::Null);
    let gaps: Vec<Value> = errors
        .iter()
        .map(|error| {
            json!({
                "kind":"goal_optional_data",
                "source_id":error["source_id"],
                "code":error["code"],
            })
        })
        .collect();
    let more_diagnostics = diagnostic_rows_more || errors.len() == MAX_DIAGNOSTIC_PAGE;
    Ok(json!({
        "items":items,
        "next_after_notification_id":next_after,
        "coverage":if has_more || more_diagnostics || !errors.is_empty() {"partial"} else {"complete"},
        "gaps":gaps,
        "errors":errors,
        "more_diagnostics":more_diagnostics,
        "model_wake":false,
        "native_work_queued":false,
    }))
}

fn push_quarantine_error(errors: &mut Vec<Value>, error: Value) {
    let source_id = error.get("source_id").cloned().unwrap_or(Value::Null);
    let code = error.get("code").cloned().unwrap_or(Value::Null);
    if errors.iter().any(|existing| {
        existing.get("source_id") == Some(&source_id) && existing.get("code") == Some(&code)
    }) {
        return;
    }
    if errors.len() >= MAX_DIAGNOSTIC_PAGE {
        errors.remove(0);
    }
    errors.push(error);
}

fn damaged(message: &str) -> Error {
    Error::new("GOAL_RECORD_CORRUPT", message)
}
