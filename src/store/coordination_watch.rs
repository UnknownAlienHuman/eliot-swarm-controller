//! Durable, exact-scope coordination watches over retained Store facts.
//!
//! Watch notifications are metadata headers. They never become peer messages,
//! Operations that start work, or runtime/model wake requests.

use super::{coordination, meta, operations, set_meta, tasks};
use crate::{
    coordination::{self as keys, watch},
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde_json::{Value, json};

const WATCH_SCHEMA: &str = "eliot.coordination.watch.v1";
const NOTICE_SCHEMA: &str = "eliot.coordination.watch.notice.v1";
const RECORD_PREFIX: &str = "coordination:watch:v1:record:";
const ACTIVE_PREFIX: &str = "coordination:watch:v1:active:";
const LIST_PREFIX: &str = "coordination:watch:v1:list:";
const SUBJECT_PREFIX: &str = "coordination:watch:v1:subject:";
const NOTICE_PREFIX: &str = "coordination:watch:v1:notice:";
const RECONCILE_CURSOR_KEY: &str = "coordination:watch:v1:reconcile_cursor";
const MAX_RECONCILE_LIMIT: i64 = 256;
const DEFAULT_NOTICE_LIMIT: i64 = 20;

#[derive(Debug, Clone)]
struct ExactScope {
    id: String,
    task_id: String,
    task_revision: i64,
    attempt_id: String,
}

struct TerminalOperationRow {
    task_id: Option<String>,
    attempt_id: Option<String>,
    state: String,
    updated_at_ms: i64,
    settled_at_ms: Option<i64>,
}

/// Store mutation seam. The boolean is the existing queued-work signal; watch
/// admission always returns false because it is durable, passive metadata.
pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let principal = super::current_principal(tx, principal.clone())?;
    let receipt = match watch::parse_mutation(method, value)? {
        watch::Mutation::Create(request) => create(tx, &principal, request, operation_id, now)?,
        watch::Mutation::Cancel(request) => cancel(tx, &principal, request, operation_id, now)?,
    };
    // The receipt Operation uses the same exact assignment scope as the
    // retained watch, including on semantic coalescing or cancellation.
    // Attach it inside the mutation savepoint before the durable ACK.
    let watch_id = model::text(&receipt, "watch_id")?;
    let record = owned_watch(tx, watch_id, &principal.client_id)?;
    let scope = scope_from_record(&record)?;
    let attempt = tasks::get_attempt(tx, &scope.attempt_id)?;
    coordination::attach_operation_scope(
        tx,
        operation_id,
        &scope.task_id,
        &scope.attempt_id,
        &attempt,
    )?;
    Ok((receipt, false))
}

/// Read the caller's current scoped watch page. Manager calls must provide an
/// exact current Task/Attempt triple; Participants derive it from registration.
pub(super) fn read(db: &Connection, principal: &Principal, value: &Value) -> Result<Value> {
    let principal = super::current_principal(db, principal.clone())?;
    let request = watch::parse_list(value)?;
    let scope = request_scope(
        db,
        &principal,
        request.task_id.as_deref(),
        request.task_revision,
        request.attempt_id.as_deref(),
    )?;
    let prefix = list_prefix(&scope.id, &principal.client_id);
    let upper = format!("{prefix}g");
    let after_key = if let Some(after_watch_id) = request.after_watch_id.as_deref() {
        let record = owned_watch(db, after_watch_id, &principal.client_id)?;
        require_record_scope(&record, &scope)?;
        Some(
            record["list_index_key"]
                .as_str()
                .ok_or_else(|| damaged("watch has no list index key"))?
                .to_owned(),
        )
    } else {
        None
    };
    let lower = after_key.as_deref().unwrap_or(&prefix);
    let compare = if after_key.is_some() { ">" } else { ">=" };
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {compare} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(
            params![lower, upper, request.limit.saturating_add(1)],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = rows.len() as i64 > request.limit;
    let mut items = Vec::new();
    for (index_key, raw_index) in rows.iter().take(request.limit as usize) {
        let index: Value = serde_json::from_str(raw_index)?;
        let watch_id = model::text(&index, "watch_id")?;
        let record = owned_watch(db, watch_id, &principal.client_id)?;
        require_record_scope(&record, &scope)?;
        if record["list_index_key"] != index_key.as_str() {
            return Err(damaged("watch list index differs from its record"));
        }
        items.push(watch_projection(&record));
    }
    let next_after_watch_id = if has_more {
        items
            .last()
            .and_then(|item| item.get("watch_id"))
            .cloned()
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    Ok(json!({
        "items":items,
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
        "next_after_watch_id":next_after_watch_id,
        "coverage":if has_more { "partial" } else { "complete" },
        "gaps":[],
    }))
}

/// Process one bounded page of current watches from the shared Store tick.
/// The durable cursor, one-shot receipt, and mailbox header update commit with
/// the caller's transaction.
pub(super) fn reconcile(tx: &Transaction<'_>, limit: i64, now: i64) -> Result<Value> {
    let limit = limit.clamp(1, MAX_RECONCILE_LIMIT);
    let cursor = meta(tx, RECONCILE_CURSOR_KEY)?
        .and_then(|value| value.as_str().map(str::to_owned))
        .filter(|value| value.starts_with(ACTIVE_PREFIX));
    let rows = scan_active(tx, cursor.as_deref(), limit)?;
    let mut matched = 0usize;
    let mut expired = 0usize;
    let mut stale = 0usize;
    let mut last_key = None;
    for (active_key, raw_index) in &rows {
        last_key = Some(active_key.clone());
        let index: Value = serde_json::from_str(raw_index)?;
        let watch_id = model::text(&index, "watch_id")?;
        let Some(mut record) = meta(tx, &record_key(watch_id))? else {
            tx.execute("DELETE FROM meta WHERE key=?1", [active_key])?;
            stale += 1;
            continue;
        };
        verify_record(&record, watch_id)?;
        if record["state"] != "active" {
            tx.execute("DELETE FROM meta WHERE key=?1", [active_key])?;
            continue;
        }
        if record["expires_at_ms"].as_i64().unwrap_or_default() <= now {
            settle_record(tx, &mut record, "expired", now)?;
            expired += 1;
            continue;
        }
        if !stored_creator_scope_is_current(tx, &record)? {
            settle_record(tx, &mut record, "stale_scope", now)?;
            stale += 1;
            continue;
        }
        let Some(cursor) = terminal_operation_cursor(tx, &record)? else {
            // The exact Operation disappeared or no longer names this retained
            // Task/Attempt. Preserve the watch as a visible stale receipt.
            settle_record(tx, &mut record, "stale_subject", now)?;
            stale += 1;
            continue;
        };
        if cursor["settled_at_ms"].is_null()
            || !matches!(
                cursor["state"].as_str(),
                Some("settled" | "rejected" | "cancelled")
            )
        {
            continue;
        }
        let scope = scope_from_record(&record)?;
        let creator_id = model::text(&record["creator"], "client_id")?;
        let operation_id = model::text(&record["address"], "operation_id")?;
        let notification = json!({
            "schema":NOTICE_SCHEMA,
            "notification_id":watch_id,
            "watch_id":watch_id,
            "watch_kind":"operation_terminal",
            "operation_id":operation_id,
            "state":cursor["state"],
            "matched_at_ms":now,
        });
        let notice_key = notice_key(&scope.id, creator_id, now, watch_id);
        set_meta(
            tx,
            &notice_key,
            &json!({"watch_id":watch_id,"notification_id":watch_id}),
        )?;
        record["cursor"] = cursor;
        record["notification"] = notification;
        record["state"] = json!("matched");
        record["updated_at_ms"] = json!(now);
        record["settled_at_ms"] = json!(now);
        record["notice_index_key"] = json!(notice_key);
        set_meta(tx, &record_key(watch_id), &record)?;
        tx.execute("DELETE FROM meta WHERE key=?1", [active_key])?;
        matched += 1;
    }
    let cursor_receipt = last_key.clone();
    if let Some(last_key) = last_key {
        set_meta(tx, RECONCILE_CURSOR_KEY, &json!(last_key))?;
    } else {
        tx.execute("DELETE FROM meta WHERE key=?1", [RECONCILE_CURSOR_KEY])?;
    }
    Ok(json!({
        "processed":rows.len(),
        "matched":matched,
        "expired":expired,
        "stale":stale,
        "cursor":cursor_receipt,
        "model_wake":false,
        "native_work_queued":false,
    }))
}

/// Participant inbox projection for silent watch headers. The existing mail
/// message page and its cursor remain a separate projection.
pub(super) fn notifications(db: &Connection, principal: &Principal, limit: i64) -> Result<Value> {
    let principal = super::current_principal(db, principal.clone())?;
    principal.require_participant()?;
    let scope = request_scope(db, &principal, None, None, None)?;
    let limit = if limit <= 0 {
        DEFAULT_NOTICE_LIMIT
    } else {
        limit.clamp(1, watch::MAX_PAGE_SIZE)
    };
    let prefix = notice_prefix(&scope.id, &principal.client_id);
    let upper = format!("{prefix}g");
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key DESC LIMIT ?3",
    )?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![prefix, upper, limit.saturating_add(1)], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = rows.len() as i64 > limit;
    let mut items = Vec::new();
    for (index_key, raw_index) in rows.iter().take(limit as usize).rev() {
        let index: Value = serde_json::from_str(raw_index)?;
        let watch_id = model::text(&index, "watch_id")?;
        let record = owned_watch(db, watch_id, &principal.client_id)?;
        require_record_scope(&record, &scope)?;
        if record["state"] != "matched"
            || record["notice_index_key"] != index_key.as_str()
            || record["notification"]["notification_id"] != watch_id
        {
            return Err(damaged(
                "mailbox notice index differs from its watch receipt",
            ));
        }
        items.push(record["notification"].clone());
    }
    Ok(json!({
        "items":items,
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
        "coverage":if has_more { "partial" } else { "complete" },
        "gaps":if has_more { json!([{"kind":"watch_notice_window_truncated","retained_inventory":"coordination.watch.list"}]) } else { json!([]) },
    }))
}

fn create(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: watch::CreateRequest,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let scope = request_scope(
        tx,
        principal,
        request.task_id.as_deref(),
        request.task_revision,
        request.attempt_id.as_deref(),
    )?;
    let max_expiry = now
        .checked_add(watch::MAX_WATCH_TTL_MS)
        .ok_or_else(|| Error::invalid("current time exceeds the supported watch clock range"))?;
    if request.expires_at_ms <= now || request.expires_at_ms > max_expiry {
        return Err(Error::invalid(format!(
            "expires_at_ms must be after now and no more than {} days ahead",
            watch::MAX_WATCH_TTL_MS / (24 * 60 * 60 * 1000)
        )));
    }
    let target = visible_operation(tx, principal, &scope, &request.operation_id)?;
    let subject_key = subject_key(
        &scope,
        &principal.client_id,
        &request.watch_kind,
        &request.operation_id,
    )?;
    if let Some(subject) = meta(tx, &subject_key)? {
        let existing_id = model::text(&subject, "watch_id")?;
        let Some(mut existing) = meta(tx, &record_key(existing_id))? else {
            return Err(damaged("watch subject index has no retained record"));
        };
        verify_record(&existing, existing_id)?;
        require_record_scope(&existing, &scope)?;
        if existing["creator"]["client_id"] != principal.client_id {
            return Err(damaged("watch subject index belongs to another creator"));
        }
        if existing["state"] == "matched" {
            return Ok(create_receipt(&existing, operation_id, true));
        }
        if existing["state"] == "active"
            && existing["expires_at_ms"].as_i64().unwrap_or_default() > now
        {
            return Ok(create_receipt(&existing, operation_id, true));
        }
        if existing["state"] == "active" {
            settle_record(tx, &mut existing, "expired", now)?;
        }
    }
    let watch_id = model::new_id();
    let creator = json!({
        "client_id":principal.client_id,
        "role":role_tag(&principal.role)?,
    });
    let list_index_key = list_key(&scope, &principal.client_id, now, &watch_id);
    let record = json!({
        "schema":WATCH_SCHEMA,
        "watch_id":watch_id,
        "creator":creator,
        "scope":scope_json(&scope),
        "watch_kind":request.watch_kind,
        "address":{"operation_id":request.operation_id},
        "delivery":"mailbox_header",
        "one_shot":true,
        "state":"active",
        "cursor":null,
        "notification":null,
        "operation_id":operation_id,
        "created_at_ms":now,
        "updated_at_ms":now,
        "expires_at_ms":request.expires_at_ms,
        "settled_at_ms":null,
        "subject_key":subject_key,
        "list_index_key":list_index_key,
        "notice_index_key":null,
        "target_state_at_create":target["state"],
    });
    set_meta(tx, &record_key(&watch_id), &record)?;
    set_meta(tx, &list_index_key, &json!({"watch_id":watch_id}))?;
    set_meta(tx, &active_key(&watch_id), &json!({"watch_id":watch_id}))?;
    set_meta(tx, &subject_key, &json!({"watch_id":watch_id}))?;
    Ok(create_receipt(&record, operation_id, false))
}

fn cancel(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: watch::CancelRequest,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let mut record = owned_watch(tx, &request.watch_id, &principal.client_id)?;
    let stored_scope = scope_from_record(&record)?;
    let live_scope = request_scope(
        tx,
        principal,
        if principal.role == Role::Participant {
            None
        } else {
            Some(stored_scope.task_id.as_str())
        },
        if principal.role == Role::Participant {
            None
        } else {
            Some(stored_scope.task_revision)
        },
        if principal.role == Role::Participant {
            None
        } else {
            Some(stored_scope.attempt_id.as_str())
        },
    )?;
    require_record_scope(&record, &live_scope)?;
    if record["state"] == "active" {
        record["state"] = json!("cancelled");
        record["updated_at_ms"] = json!(now);
        record["settled_at_ms"] = json!(now);
        set_meta(tx, &record_key(&request.watch_id), &record)?;
        tx.execute(
            "DELETE FROM meta WHERE key=?1",
            [active_key(&request.watch_id)],
        )?;
    }
    Ok(json!({
        "operation_id":operation_id,
        "watch_id":request.watch_id,
        "state":record["state"],
        "cancelled":record["state"] == "cancelled",
        "notification":record["notification"],
    }))
}

fn visible_operation(
    tx: &Transaction<'_>,
    principal: &Principal,
    scope: &ExactScope,
    operation_id: &str,
) -> Result<Value> {
    if !super::operation_visible_to(tx, principal, operation_id)? {
        return Err(Error::new(
            "NOT_FOUND",
            "Operation was not found or is not visible in this identity",
        ));
    }
    let operation = operations::get_operation(tx, operation_id)?;
    if operation["task_id"] != scope.task_id || operation["attempt_id"] != scope.attempt_id {
        return Err(Error::new(
            "WATCH_SCOPE_MISMATCH",
            "operation is outside the exact current Task/Attempt scope",
        ));
    }
    let attempt = tasks::get_attempt(tx, &scope.attempt_id)?;
    if attempt["task_id"] != scope.task_id || attempt["task_revision"] != scope.task_revision {
        return Err(Error::new(
            "WATCH_SCOPE_MISMATCH",
            "operation Attempt does not match the exact current Task revision",
        ));
    }
    Ok(json!({"state":operation["state"]}))
}

fn terminal_operation_cursor(tx: &Transaction<'_>, record: &Value) -> Result<Option<Value>> {
    let watch_scope = scope_from_record(record)?;
    let operation_id = model::text(&record["address"], "operation_id")?;
    let row: Option<TerminalOperationRow> = tx
        .query_row(
            "SELECT task_id,attempt_id,state,updated_at_ms,settled_at_ms FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok(TerminalOperationRow {
                    task_id: row.get(0)?,
                    attempt_id: row.get(1)?,
                    state: row.get(2)?,
                    updated_at_ms: row.get(3)?,
                    settled_at_ms: row.get(4)?,
                })
            },
        )
        .optional()?;
    let Some(row) = row else {
        return Ok(None);
    };
    if row.task_id.as_deref() != Some(watch_scope.task_id.as_str())
        || row.attempt_id.as_deref() != Some(watch_scope.attempt_id.as_str())
    {
        return Ok(None);
    }
    Ok(Some(json!({
        "operation_id":operation_id,
        "state":row.state,
        "updated_at_ms":row.updated_at_ms,
        "settled_at_ms":row.settled_at_ms,
    })))
}

fn stored_creator_scope_is_current(tx: &Transaction<'_>, record: &Value) -> Result<bool> {
    let scope = scope_from_record(record)?;
    let creator = &record["creator"];
    let role = model::text(creator, "role")?;
    let client_id = model::text(creator, "client_id")?;
    Ok(coordination::watch_scope_for_creator(
        tx,
        role,
        client_id,
        &scope.task_id,
        scope.task_revision,
        &scope.attempt_id,
    )?
    .is_some())
}

fn request_scope(
    db: &Connection,
    principal: &Principal,
    task_id: Option<&str>,
    task_revision: Option<i64>,
    attempt_id: Option<&str>,
) -> Result<ExactScope> {
    let projection = coordination::watch_scope(db, principal, task_id, task_revision, attempt_id)?;
    Ok(ExactScope {
        id: model::text(&projection, "scope_id")?.to_owned(),
        task_id: model::text(&projection["task"], "task_id")?.to_owned(),
        task_revision: model::positive(&projection["task"], "revision")?,
        attempt_id: model::text(&projection["attempt"], "attempt_id")?.to_owned(),
    })
}

fn scope_from_record(record: &Value) -> Result<ExactScope> {
    let scope = record
        .get("scope")
        .ok_or_else(|| damaged("watch has no retained scope"))?;
    Ok(ExactScope {
        id: model::text(scope, "scope_id")?.to_owned(),
        task_id: model::text(scope, "task_id")?.to_owned(),
        task_revision: model::positive(scope, "task_revision")?,
        attempt_id: model::text(scope, "attempt_id")?.to_owned(),
    })
}

fn scope_json(scope: &ExactScope) -> Value {
    json!({
        "scope_id":scope.id,
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
    })
}

fn require_record_scope(record: &Value, scope: &ExactScope) -> Result<()> {
    let stored = scope_from_record(record)?;
    if stored.id != scope.id
        || stored.task_id != scope.task_id
        || stored.task_revision != scope.task_revision
        || stored.attempt_id != scope.attempt_id
    {
        return Err(Error::new(
            "WATCH_SCOPE_MISMATCH",
            "watch is outside the authenticated current Task/Attempt scope",
        ));
    }
    Ok(())
}

fn owned_watch(db: &Connection, watch_id: &str, client_id: &str) -> Result<Value> {
    let Some(record) = meta(db, &record_key(watch_id))? else {
        return Err(Error::new("WATCH_NOT_FOUND", "watch is not retained"));
    };
    verify_record(&record, watch_id)?;
    if record["creator"]["client_id"] != client_id {
        return Err(Error::new("WATCH_NOT_FOUND", "watch is not retained"));
    }
    Ok(record)
}

fn verify_record(record: &Value, expected_id: &str) -> Result<()> {
    if record["schema"] != WATCH_SCHEMA
        || record["watch_id"] != expected_id
        || record["watch_kind"] != "operation_terminal"
        || record["delivery"] != "mailbox_header"
        || record["one_shot"] != true
        || !matches!(
            record["state"].as_str(),
            Some("active" | "matched" | "cancelled" | "expired" | "stale_scope" | "stale_subject")
        )
    {
        return Err(damaged("watch record does not match its supported schema"));
    }
    let _ = scope_from_record(record)?;
    let _ = model::text(&record["creator"], "client_id")?;
    let _ = model::text(&record["creator"], "role")?;
    let _ = model::text(&record["address"], "operation_id")?;
    Ok(())
}

fn settle_record(tx: &Transaction<'_>, record: &mut Value, state: &str, now: i64) -> Result<()> {
    let watch_id = model::text(record, "watch_id")?.to_owned();
    record["state"] = json!(state);
    record["updated_at_ms"] = json!(now);
    record["settled_at_ms"] = json!(now);
    set_meta(tx, &record_key(&watch_id), record)?;
    tx.execute("DELETE FROM meta WHERE key=?1", [active_key(&watch_id)])?;
    Ok(())
}

fn create_receipt(record: &Value, operation_id: &str, coalesced: bool) -> Value {
    json!({
        "operation_id":operation_id,
        "watch_id":record["watch_id"],
        "watch_kind":record["watch_kind"],
        "state":record["state"],
        "expires_at_ms":record["expires_at_ms"],
        "target_state_at_create":record["target_state_at_create"],
        "coalesced":coalesced,
        "notification":record["notification"],
    })
}

fn watch_projection(record: &Value) -> Value {
    json!({
        "watch_id":record["watch_id"],
        "watch_kind":record["watch_kind"],
        "address":record["address"],
        "state":record["state"],
        "delivery":record["delivery"],
        "one_shot":record["one_shot"],
        "created_at_ms":record["created_at_ms"],
        "updated_at_ms":record["updated_at_ms"],
        "expires_at_ms":record["expires_at_ms"],
        "settled_at_ms":record["settled_at_ms"],
        "cursor":record["cursor"],
        "notification":record["notification"],
    })
}

fn scan_active(db: &Connection, cursor: Option<&str>, limit: i64) -> Result<Vec<(String, String)>> {
    let upper = format!("{ACTIVE_PREFIX}g");
    let Some(cursor) = cursor else {
        return scan_active_segment(db, ACTIVE_PREFIX, ">=", &upper, "<", limit);
    };
    let mut rows = scan_active_segment(db, cursor, ">", &upper, "<", limit)?;
    if (rows.len() as i64) < limit {
        let remaining = limit - rows.len() as i64;
        rows.extend(scan_active_segment(
            db,
            ACTIVE_PREFIX,
            ">=",
            cursor,
            "<=",
            remaining,
        )?);
    }
    Ok(rows)
}

fn scan_active_segment(
    db: &Connection,
    lower: &str,
    lower_op: &str,
    upper: &str,
    upper_op: &str,
    limit: i64,
) -> Result<Vec<(String, String)>> {
    // Operators are selected only from the constant call sites above.
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {lower_op} ?1 AND key {upper_op} ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let rows = statement
        .query_map(params![lower, upper, limit], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn role_tag(role: &Role) -> Result<&'static str> {
    match role {
        Role::Participant => Ok("participant"),
        Role::Manager => Ok("manager"),
        Role::Operator => Ok("operator"),
        _ => Err(Error::new(
            "FORBIDDEN",
            "watch requires a Participant or authorized manager identity",
        )),
    }
}

fn record_key(watch_id: &str) -> String {
    format!("{RECORD_PREFIX}{}", keys::key_component(watch_id))
}

fn active_key(watch_id: &str) -> String {
    format!("{ACTIVE_PREFIX}{}", keys::key_component(watch_id))
}

fn list_prefix(scope_id: &str, creator_id: &str) -> String {
    format!(
        "{LIST_PREFIX}{scope_id}:{}:",
        keys::key_component(creator_id)
    )
}

fn list_key(scope: &ExactScope, creator_id: &str, created_at_ms: i64, watch_id: &str) -> String {
    format!(
        "{}{created_at_ms:020}:{}",
        list_prefix(&scope.id, creator_id),
        keys::key_component(watch_id)
    )
}

fn subject_key(
    scope: &ExactScope,
    creator_id: &str,
    watch_kind: &str,
    operation_id: &str,
) -> Result<String> {
    let subject = json!({
        "scope_id":scope.id,
        "watcher_id":creator_id,
        "watch_kind":watch_kind,
        "operation_id":operation_id,
    });
    Ok(format!(
        "{SUBJECT_PREFIX}{}",
        model::digest(model::canonical(&subject)?.as_bytes())
    ))
}

fn notice_prefix(scope_id: &str, creator_id: &str) -> String {
    format!(
        "{NOTICE_PREFIX}{scope_id}:{}:",
        keys::key_component(creator_id)
    )
}

fn notice_key(scope_id: &str, creator_id: &str, matched_at_ms: i64, watch_id: &str) -> String {
    format!(
        "{}{matched_at_ms:020}:{}",
        notice_prefix(scope_id, creator_id),
        keys::key_component(watch_id)
    )
}

fn damaged(message: &str) -> Error {
    Error::new("WATCH_RECORD_DAMAGED", message)
}
