//! Durable, exact-scope coordination watches over retained Store facts.
//!
//! Watch notifications are metadata headers. They never become peer messages,
//! Operations that start work, or runtime/model wake requests.

use super::{automation_reconcile, coordination, meta, operations, set_meta, submissions, tasks};
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
const OWNER_NOTICE_PREFIX: &str = "coordination:watch:v1:owner-notice:";
const RECONCILE_CURSOR_KEY: &str = "coordination:watch:v1:reconcile_cursor";
const REVISIT_PREFIX: &str = "coordination:watch:v1:revisit:";
const REVISIT_SEQUENCE_KEY: &str = "coordination:watch:v1:revisit_sequence";
const REVISIT_TURN_KEY: &str = "coordination:watch:v1:revisit_turn";
const QUARANTINE_PREFIX: &str = "coordination:watch:v1:quarantine:";
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
    let scope = match request_scope(
        db,
        &principal,
        request.task_id.as_deref(),
        request.task_revision,
        request.attempt_id.as_deref(),
    ) {
        Ok(scope) => scope,
        Err(current_error) => retained_transition_scope(
            db,
            &principal,
            request.task_id.as_deref(),
            request.task_revision,
            request.attempt_id.as_deref(),
        )?
        .ok_or(current_error)?,
    };
    let prefix = list_prefix(&scope.id, &principal.client_id);
    let upper = format!("{prefix}g");
    let after_key = if let Some(after_watch_id) = request.after_watch_id.as_deref() {
        Some(resolve_after_watch_id(
            db,
            &scope,
            &principal.client_id,
            &prefix,
            &upper,
            after_watch_id,
        )?)
    } else if let Some(cursor) = request.after_scan_cursor.as_deref() {
        Some(keys::decode_list_scan_cursor(
            &prefix,
            &principal.client_id,
            &upper,
            cursor,
        )?)
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
    let mut damaged_entries = 0usize;
    let mut last_examined_watch_id = None;
    let mut last_examined_key = None;
    for (index_key, raw_index) in rows.iter().take(request.limit as usize) {
        // The exact-scope list key retains the server-issued watch id even if
        // its JSON index payload or retained record is damaged. This id is only
        // a continuation token; projection still requires every authority and
        // record-binding check below.
        last_examined_key = Some(index_key.as_str());
        last_examined_watch_id = None;
        let key_watch_id = match watch_id_from_list_key(index_key, &prefix) {
            Ok(id) => id,
            Err(error) if watch_list_entry_is_damaged(&error) => {
                damaged_entries += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        last_examined_watch_id = Some(key_watch_id.clone());
        let index: Value = match serde_json::from_str(raw_index) {
            Ok(index) => index,
            Err(_) => {
                damaged_entries += 1;
                continue;
            }
        };
        if index.get("watch_id").and_then(Value::as_str) != Some(key_watch_id.as_str()) {
            damaged_entries += 1;
            continue;
        }
        let record = match (|| {
            let record = owned_watch(db, &key_watch_id, &principal.client_id)?;
            require_record_scope(&record, &scope)?;
            if record["list_index_key"] != index_key.as_str() {
                return Err(damaged("watch list index differs from its record"));
            }
            Ok(record)
        })() {
            Ok(record) => record,
            Err(error) if watch_list_entry_is_damaged(&error) => {
                damaged_entries += 1;
                continue;
            }
            Err(error) => return Err(error),
        };
        items.push(watch_projection(&record));
    }
    let next_after_watch_id = if has_more {
        last_examined_watch_id
            .map(Value::String)
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    let mut gaps = Vec::new();
    let next_scan_cursor = if has_more {
        last_examined_key
            .map(|key| keys::encode_list_scan_cursor(&prefix, &principal.client_id, key))
            .transpose()?
    } else {
        None
    };
    if damaged_entries > 0 {
        gaps.push(json!({
            "kind":"watch_list_entries_damaged",
            "count":damaged_entries,
        }));
    }
    if has_more {
        gaps.push(json!({
            "kind":"watch_list_window_truncated",
        }));
    }
    Ok(json!({
        "items":items,
        "task_id":scope.task_id,
        "task_revision":scope.task_revision,
        "attempt_id":scope.attempt_id,
        "next_after_watch_id":next_after_watch_id,
        "next_scan_cursor":next_scan_cursor,
        "coverage":if has_more || damaged_entries > 0 { "partial" } else { "complete" },
        "gaps":gaps,
    }))
}

fn resolve_after_watch_id(
    db: &Connection,
    scope: &ExactScope,
    creator_id: &str,
    prefix: &str,
    upper: &str,
    watch_id: &str,
) -> Result<String> {
    let record_failure = match owned_watch(db, watch_id, creator_id) {
        Ok(record) => match require_record_scope(&record, scope) {
            Ok(()) => match record["list_index_key"].as_str() {
                Some(index_key)
                    if watch_id_from_list_key(index_key, prefix)
                        .is_ok_and(|indexed_watch_id| indexed_watch_id == watch_id) =>
                {
                    return Ok(index_key.to_owned());
                }
                _ => damaged("watch cursor has no valid retained list index key"),
            },
            Err(error) => error,
        },
        Err(error) => error,
    };
    if !watch_list_entry_is_damaged(&record_failure) {
        return Err(record_failure);
    }
    if let Some(index_key) = scoped_list_index_key(db, prefix, upper, watch_id)? {
        return Ok(index_key);
    }
    Err(record_failure)
}

fn watch_id_from_list_key(index_key: &str, prefix: &str) -> Result<String> {
    let suffix = index_key
        .strip_prefix(prefix)
        .ok_or_else(|| damaged("watch list key is outside its exact owner scope"))?;
    let (created_at_ms, encoded_watch_id) = suffix
        .split_once(':')
        .ok_or_else(|| damaged("watch list key has no encoded watch id"))?;
    if created_at_ms.len() != 20
        || !created_at_ms.bytes().all(|byte| byte.is_ascii_digit())
        || encoded_watch_id.is_empty()
        || encoded_watch_id.len() % 2 != 0
    {
        return Err(damaged("watch list key has an invalid cursor shape"));
    }

    let mut watch_id_bytes = Vec::with_capacity(encoded_watch_id.len() / 2);
    for pair in encoded_watch_id.as_bytes().as_chunks::<2>().0 {
        let pair = std::str::from_utf8(pair)
            .map_err(|_| damaged("watch list key has a non-hex watch id"))?;
        watch_id_bytes.push(
            u8::from_str_radix(pair, 16)
                .map_err(|_| damaged("watch list key has a non-hex watch id"))?,
        );
    }
    let watch_id = String::from_utf8(watch_id_bytes)
        .map_err(|_| damaged("watch list key has a non-text watch id"))?;
    if watch_id.is_empty()
        || watch_id.len() > 128
        || watch_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || keys::key_component(&watch_id) != encoded_watch_id
    {
        return Err(damaged("watch list key has an invalid watch id"));
    }
    Ok(watch_id)
}

fn scoped_list_index_key(
    db: &Connection,
    prefix: &str,
    upper: &str,
    watch_id: &str,
) -> Result<Option<String>> {
    let watch_id_component = keys::key_component(watch_id);
    let mut statement = db.prepare(
        "SELECT key FROM meta \
         WHERE key>=?1 AND key<?2 \
           AND substr(key, -length(?3))=?3 \
           AND substr(key, -length(?3)-1, 1)=':' \
         ORDER BY key LIMIT 2",
    )?;
    let keys: Vec<String> = statement
        .query_map(params![prefix, upper, watch_id_component], |row| row.get(0))?
        .collect::<std::result::Result<_, _>>()?;
    match keys.as_slice() {
        [] => Ok(None),
        [key] => Ok(Some(key.clone())),
        _ => Err(damaged(
            "watch cursor has multiple exact-scope list indexes",
        )),
    }
}

fn watch_list_entry_is_damaged(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "INVALID_PARAMS" | "WATCH_NOT_FOUND" | "WATCH_SCOPE_MISMATCH" | "WATCH_RECORD_DAMAGED"
    )
}

#[cfg(test)]
#[path = "coordination_watch_quarantine_tests.rs"]
mod coordination_watch_quarantine_tests;
/// Process one bounded page of current watches from the shared Store tick.
/// The durable cursor, one-shot receipt, and mailbox header update commit with
/// the caller's transaction.
/// Store-private subject vocabulary for one watch row. The domain maps
/// only its own known semantic errors; anything unrecognized stays hard.
enum WatchSubjectDisposition {
    Applied(WatchSubjectKind),
    Skipped,
    Pending,
    Quarantined {
        code: String,
        evidence: automation_reconcile::QuarantineEvidence,
        source: WatchDamagedSource,
    },
}

enum WatchSubjectKind {
    Matched,
    Expired,
    Stale,
}

enum WatchDamagedSource {
    ActiveIndex { key: String, raw: String },
    RetainedRecord { key: String, raw: String },
}

impl WatchDamagedSource {
    fn key_and_raw(&self) -> (&str, &str) {
        match self {
            Self::ActiveIndex { key, raw } | Self::RetainedRecord { key, raw } => (key, raw),
        }
    }

    fn code(&self) -> &'static str {
        match self {
            Self::ActiveIndex { .. } => "WATCH_ACTIVE_INDEX_DAMAGED",
            Self::RetainedRecord { .. } => "WATCH_RECORD_DAMAGED",
        }
    }
}

/// Identity addresses the active subject; pointer and digest address the
/// exact damaged source. Neither raw source nor plaintext IDs are persisted.
fn watch_subject_evidence(
    active_key: &str,
    source: &WatchDamagedSource,
) -> automation_reconcile::QuarantineEvidence {
    let (source_key, raw_source) = source.key_and_raw();
    automation_reconcile::QuarantineEvidence {
        subject_identity: format!(
            "watch-active-key-sha256:{}",
            model::digest(active_key.as_bytes())
        ),
        source_pointer: Some(format!(
            "meta/key-sha256:{}",
            model::digest(source_key.as_bytes())
        )),
        source_digest: Some(model::digest(raw_source.as_bytes())),
    }
}

fn watch_id_from_active_index(index_key: &str, raw_index: &str) -> Result<String> {
    let index: Value = serde_json::from_str(raw_index)?;
    model::fields(&index, &["watch_id", "revisit_key"])?;
    let watch_id = model::text(&index, "watch_id")?;
    if watch_id.len() > 128 || active_key(watch_id) != index_key {
        return Err(Error::invalid("watch active key differs from its index"));
    }
    if index.get("revisit_key").is_some_and(|key| {
        !key.as_str()
            .is_some_and(|key| valid_revisit_key(key, index_key))
    }) {
        return Err(Error::invalid("watch active revisit key is invalid"));
    }
    Ok(watch_id.to_owned())
}

fn require_watch_source(tx: &Transaction<'_>, key: &str, raw: &str) -> Result<()> {
    let current: Option<String> = tx
        .query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
            row.get(0)
        })
        .optional()?;
    if current.as_deref() != Some(raw) {
        return Err(Error::new(
            "WATCH_SOURCE_CONFLICT",
            "watch source changed after damage classification",
        ));
    }
    Ok(())
}

/// Remove an already-quarantined subject from the durable active index and
/// advance the reconcile cursor in the same transaction as its evidence, so
/// the cursor only ever moves past a disposed subject.
fn retire_quarantined_watch(
    tx: &Transaction<'_>,
    active_key: &str,
    raw_index: &str,
    source: &WatchDamagedSource,
    evidence: &automation_reconcile::QuarantineEvidence,
    code: &str,
    now: i64,
) -> Result<()> {
    let record_key = automation_reconcile::quarantine_record_key(QUARANTINE_PREFIX, evidence)?;
    match automation_reconcile::with_subject_savepoint(
        tx,
        || {
            let (source_key, raw_source) = source.key_and_raw();
            require_watch_source(tx, source_key, raw_source)?;
            automation_reconcile::persist_quarantine(tx, &record_key, code, evidence.clone(), now)?;
            let removed = tx.execute(
                "DELETE FROM meta WHERE key=?1 AND value_json=?2",
                params![active_key, raw_index],
            )?;
            if removed != 1 {
                return Err(Error::new(
                    "WATCH_SOURCE_CONFLICT",
                    "watch active index changed",
                ));
            }
            // Also fence changes caused by evidence/index write triggers.
            if matches!(source, WatchDamagedSource::RetainedRecord { .. }) {
                require_watch_source(tx, source_key, raw_source)?;
            }
            Ok(automation_reconcile::SubjectDisposition::Applied(()))
        },
        |_| None,
    )? {
        automation_reconcile::SubjectDisposition::Applied(()) => Ok(()),
        _ => Err(Error::new(
            "WATCH_QUARANTINE_INVALID",
            "quarantined watch subject did not produce durable evidence",
        )),
    }
}

/// One bounded subject behind the shared subject savepoint. Global row scans,
/// the reconcile cursor and their writes stay outside this boundary. Only
/// decoding this subject's two source rows may classify source damage.
fn reconcile_watch_subject(
    tx: &Transaction<'_>,
    active_key: &str,
    raw_index: &str,
    now: i64,
) -> Result<WatchSubjectDisposition> {
    // The subject runs inside the shared savepoint helper, whose own
    // vocabulary is Applied / Pending / Skipped / Quarantined. The watch
    // domain maps its outcome into that vocabulary here and records the
    // matched / expired / stale ledger in the Applied payload.
    let mut damaged_source = None;
    let mut quarantine = |source: WatchDamagedSource| {
        let code = source.code().to_owned();
        let evidence = watch_subject_evidence(active_key, &source);
        damaged_source = Some(source);
        automation_reconcile::SubjectDisposition::Quarantined { code, evidence }
    };
    let disposition = automation_reconcile::with_subject_savepoint(
        tx,
        || {
            let watch_id = match watch_id_from_active_index(active_key, raw_index) {
                Ok(watch_id) => watch_id,
                Err(_) => {
                    return Ok(quarantine(WatchDamagedSource::ActiveIndex {
                        key: active_key.to_owned(),
                        raw: raw_index.to_owned(),
                    }));
                }
            };
            let watch_id = watch_id.as_str();
            let retained_key = record_key(watch_id);
            let raw_record: Option<String> = tx
                .query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [&retained_key],
                    |row| row.get(0),
                )
                .optional()?;
            let Some(raw_record) = raw_record else {
                tx.execute("DELETE FROM meta WHERE key=?1", [active_key])?;
                return Ok(automation_reconcile::SubjectDisposition::Applied(
                    WatchSubjectKind::Stale,
                ));
            };
            let parsed_record = serde_json::from_str::<Value>(&raw_record)
                .map_err(|_| damaged("retained watch record JSON is invalid"))
                .and_then(|record| {
                    verify_reconcile_record(&record, watch_id)?;
                    Ok(record)
                });
            let mut record = match parsed_record {
                Ok(record) => record,
                Err(error) if error.code == "WATCH_RECORD_DAMAGED" => {
                    return Ok(quarantine(WatchDamagedSource::RetainedRecord {
                        key: retained_key,
                        raw: raw_record,
                    }));
                }
                Err(error) => return Err(error),
            };
            if record["state"] != "active" {
                tx.execute("DELETE FROM meta WHERE key=?1", [active_key])?;
                return Ok(automation_reconcile::SubjectDisposition::Applied(
                    WatchSubjectKind::Stale,
                ));
            }
            let expires_at_ms = record["expires_at_ms"].as_i64().unwrap_or_default();
            let deadline_boundary = record["watch_kind"] == "exact_deadline_reached"
                && record["address"]["expected_deadline_ms"]
                    .as_i64()
                    .is_some_and(|deadline| expires_at_ms >= deadline);
            if !deadline_boundary && expires_at_ms <= now {
                settle_record(tx, &mut record, "expired", now)?;
                return Ok(automation_reconcile::SubjectDisposition::Applied(
                    WatchSubjectKind::Expired,
                ));
            }
            let creator_is_current = if record["watch_kind"] == "operation_terminal" {
                stored_creator_scope_is_current(tx, &record)?
            } else {
                event_creator_is_current(tx, &record)?
            };
            if !creator_is_current {
                settle_record(tx, &mut record, "stale_scope", now)?;
                return Ok(automation_reconcile::SubjectDisposition::Applied(
                    WatchSubjectKind::Stale,
                ));
            }
            match event_cursor(tx, &record, now)? {
                EventCursor::Pending => {
                    if expires_at_ms <= now {
                        settle_record(tx, &mut record, "expired", now)?;
                        return Ok(automation_reconcile::SubjectDisposition::Applied(
                            WatchSubjectKind::Expired,
                        ));
                    }
                    Ok(automation_reconcile::SubjectDisposition::Pending {
                        code: "WATCH_EVENT_PENDING".to_owned(),
                        reason: "watch event cursor is still pending".to_owned(),
                    })
                }
                EventCursor::StaleSubject => {
                    // Exact historical subjects are readable only as bounded
                    // transition facts. Missing or mismatched identities settle
                    // visibly without projecting their retained context.
                    settle_record(tx, &mut record, "stale_subject", now)?;
                    Ok(automation_reconcile::SubjectDisposition::Applied(
                        WatchSubjectKind::Stale,
                    ))
                }
                EventCursor::Matched(cursor) => {
                    let scope = scope_from_record(&record)?;
                    let creator_id = model::text(&record["creator"], "client_id")?;
                    let creator_role = model::text(&record["creator"], "role")?;
                    let watch_kind = model::text(&record, "watch_kind")?;
                    let notification = if watch_kind == "operation_terminal" {
                        // Preserve the original O1 notification envelope for
                        // existing inbox consumers while the event-kind
                        // envelopes stay fact-only.
                        json!({
                            "schema":NOTICE_SCHEMA,
                            "notification_id":watch_id,
                            "watch_id":watch_id,
                            "watch_kind":watch_kind,
                            "operation_id":record["address"]["operation_id"],
                            "state":cursor["state"],
                            "matched_at_ms":now,
                        })
                    } else {
                        json!({
                            "schema":NOTICE_SCHEMA,
                            "notification_id":watch_id,
                            "watch_id":watch_id,
                            "watch_kind":watch_kind,
                            "address":record["address"],
                            "facts":notification_facts(watch_kind, &record["address"], &cursor),
                            "matched_at_ms":now,
                        })
                    };
                    let notice_index_key = match creator_role {
                        "participant" => {
                            let notice_key = notice_key(&scope.id, creator_id, now, watch_id);
                            set_meta(
                                tx,
                                &notice_key,
                                &json!({"watch_id":watch_id,"notification_id":watch_id}),
                            )?;
                            set_meta(
                                tx,
                                &owner_notice_key(creator_id, now, watch_id),
                                &json!({
                                    "watch_id":watch_id,
                                    "scope_id":scope.id,
                                    "notice_index_key":notice_key,
                                }),
                            )?;
                            json!(notice_key)
                        }
                        "manager" | "operator" => Value::Null,
                        _ => return Err(damaged("watch creator role is unsupported")),
                    };
                    record["cursor"] = cursor;
                    record["notification"] = notification;
                    record["state"] = json!("matched");
                    record["updated_at_ms"] = json!(now);
                    record["settled_at_ms"] = json!(now);
                    record["notice_index_key"] = notice_index_key;
                    set_meta(tx, &record_key(watch_id), &record)?;
                    tx.execute("DELETE FROM meta WHERE key=?1", [active_key])?;
                    Ok(automation_reconcile::SubjectDisposition::Applied(
                        WatchSubjectKind::Matched,
                    ))
                }
            }
        },
        |_| None,
    )?;
    // Translate the shared vocabulary into this domain’s ledger value.
    Ok(match disposition {
        automation_reconcile::SubjectDisposition::Applied(kind) => {
            WatchSubjectDisposition::Applied(kind)
        }
        automation_reconcile::SubjectDisposition::Skipped { .. } => {
            WatchSubjectDisposition::Skipped
        }
        automation_reconcile::SubjectDisposition::Quarantined { code, evidence } => {
            WatchSubjectDisposition::Quarantined {
                code,
                evidence,
                source: damaged_source.ok_or_else(|| {
                    Error::new(
                        "WATCH_QUARANTINE_INVALID",
                        "watch quarantine has no exact damaged source",
                    )
                })?,
            }
        }
        automation_reconcile::SubjectDisposition::Pending { .. } => {
            WatchSubjectDisposition::Pending
        }
    })
}

/// Process one bounded page of current watches from the shared Store tick.
/// The durable cursor, one-shot receipt, and mailbox header update commit with
/// the caller's transaction. A semantically damaged retained subject is
/// quarantined with its evidence and removed from the active index in the same
/// transaction that advances the cursor, so one poison row cannot starve the
/// subjects behind it. A broken global reconcile cursor fails
/// the whole domain transaction without partial subject writes.
pub(super) fn reconcile(tx: &Transaction<'_>, limit: i64, now: i64) -> Result<Value> {
    let limit = limit.clamp(1, MAX_RECONCILE_LIMIT);
    let cursor = match meta(tx, RECONCILE_CURSOR_KEY)? {
        None => None,
        Some(Value::String(key))
            if key.strip_prefix(ACTIVE_PREFIX).is_some_and(|suffix| {
                !suffix.is_empty()
                    && suffix.len() % 2 == 0
                    && suffix
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            }) =>
        {
            Some(key)
        }
        Some(_) => {
            return Err(Error::new(
                "WATCH_CURSOR_DAMAGED",
                "watch reconcile cursor is invalid",
            ));
        }
    };
    let (rows, next_cursor) = scan_active(tx, cursor.as_deref(), limit)?;
    let mut matched = 0usize;
    let mut expired = 0usize;
    let mut stale = 0usize;
    let mut quarantined = 0usize;
    for (active_key, raw_index) in &rows {
        match reconcile_watch_subject(tx, active_key, raw_index, now)? {
            WatchSubjectDisposition::Applied(kind) => match kind {
                WatchSubjectKind::Matched => matched += 1,
                WatchSubjectKind::Expired => expired += 1,
                WatchSubjectKind::Stale => stale += 1,
            },
            WatchSubjectDisposition::Skipped => stale += 1,
            WatchSubjectDisposition::Pending => {
                queue_pending_watch(tx, active_key, raw_index)?;
                continue;
            }
            WatchSubjectDisposition::Quarantined {
                code,
                evidence,
                source,
            } => {
                // The active-index row is removed with its durable evidence, so
                // the cursor only advances past a disposed subject.
                retire_quarantined_watch(
                    tx, active_key, raw_index, &source, &evidence, &code, now,
                )?;
                quarantined += 1;
            }
        }
        remove_watch_revisit(tx, active_key, raw_index)?;
    }
    let cursor_receipt = if rows.is_empty() {
        None
    } else {
        next_cursor.or(cursor)
    };
    if let Some(key) = &cursor_receipt {
        set_meta(tx, RECONCILE_CURSOR_KEY, &json!(key))?;
    } else {
        tx.execute("DELETE FROM meta WHERE key=?1", [RECONCILE_CURSOR_KEY])?;
    }
    Ok(json!({
        "processed":rows.len(),
        "matched":matched,
        "expired":expired,
        "stale":stale,
        "quarantined":quarantined,
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
    let limit = if limit <= 0 {
        DEFAULT_NOTICE_LIMIT
    } else {
        limit.clamp(1, watch::MAX_PAGE_SIZE)
    };
    let current_scope = match request_scope(db, &principal, None, None, None) {
        Ok(scope) => Some(scope),
        Err(error)
            if matches!(
                error.code.as_str(),
                "FORBIDDEN"
                    | "NOT_FOUND"
                    | "STALE_REVISION"
                    | "STALE_PARTICIPANT"
                    | "PARTICIPANT_NOT_ASSIGNED"
                    | "STALE_REVIEW_ASSIGNMENT"
            ) =>
        {
            None
        }
        Err(error) => return Err(error),
    };
    let mut candidates = std::collections::BTreeMap::<String, (String, String)>::new();
    let mut has_more = false;
    if let Some(scope) = current_scope.as_ref() {
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
        has_more |= rows.len() as i64 > limit;
        for (index_key, raw_index) in rows.into_iter().take(limit as usize) {
            let index: Value = serde_json::from_str(&raw_index)?;
            let watch_id = model::text(&index, "watch_id")?.to_owned();
            candidates.insert(watch_id, (scope.id.clone(), index_key));
        }
    }
    let prefix = owner_notice_prefix(&principal.client_id);
    let upper = format!("{prefix}g");
    let mut statement = db.prepare(
        "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key DESC LIMIT ?3",
    )?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![prefix, upper, limit.saturating_add(1)], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    has_more |= rows.len() as i64 > limit;
    for (owner_key, raw_index) in rows.into_iter().take(limit as usize) {
        let index: Value = serde_json::from_str(&raw_index)?;
        let watch_id = model::text(&index, "watch_id")?.to_owned();
        let notice_key = model::text(&index, "notice_index_key")?.to_owned();
        if !owner_key.ends_with(&keys::key_component(&watch_id)) {
            return Err(damaged("watch owner notice key differs from its index"));
        }
        candidates.insert(
            watch_id,
            (model::text(&index, "scope_id")?.to_owned(), notice_key),
        );
    }
    let mut notices = Vec::<(i64, String, Value)>::new();
    let mut stale_authority = 0usize;
    for (watch_id, (scope_id, notice_index_key)) in &candidates {
        let record = owned_watch(db, watch_id, &principal.client_id)?;
        let scope = scope_from_record(&record)?;
        if record["state"] != "matched"
            || scope.id != *scope_id
            || record["notice_index_key"] != *notice_index_key
            || record["notification"]["notification_id"].as_str() != Some(watch_id.as_str())
        {
            return Err(damaged(
                "mailbox notice index differs from its watch receipt",
            ));
        }
        let authority_current = if record["watch_kind"] == "operation_terminal" {
            stored_creator_scope_is_current(db, &record)?
        } else {
            event_creator_is_current(db, &record)?
        };
        if !authority_current {
            stale_authority += 1;
            continue;
        }
        let matched_at_ms = record["notification"]["matched_at_ms"]
            .as_i64()
            .ok_or_else(|| damaged("watch notification has no numeric matched_at_ms"))?;
        notices.push((
            matched_at_ms,
            watch_id.clone(),
            record["notification"].clone(),
        ));
    }
    notices.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    if notices.len() > limit as usize {
        notices.drain(..notices.len() - limit as usize);
        has_more = true;
    }
    let items: Vec<Value> = notices
        .into_iter()
        .map(|(_, _, notification)| notification)
        .collect();
    let mut gaps = Vec::new();
    if stale_authority > 0 {
        gaps.push(json!({
            "kind":"watch_notice_current_authority_filtered",
            "count":stale_authority,
        }));
    }
    if has_more {
        gaps.push(json!({
            "kind":"watch_notice_window_truncated",
            "retained_inventory":"coordination.watch.list",
        }));
    }
    if current_scope.is_none() {
        gaps.push(json!({
            "kind":"current_scope_unavailable",
            "projection":"watch_headers_only",
        }));
    }
    Ok(json!({
        "items":items,
        "task_id":current_scope.as_ref().map(|scope|json!(scope.task_id)).unwrap_or(Value::Null),
        "task_revision":current_scope.as_ref().map(|scope|json!(scope.task_revision)).unwrap_or(Value::Null),
        "attempt_id":current_scope.as_ref().map(|scope|json!(scope.attempt_id)).unwrap_or(Value::Null),
        "coverage":if has_more { "partial" } else { "complete" },
        "gaps":gaps,
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
    let exact_deadline_ms = if request.watch_kind == "exact_deadline_reached" {
        let deadline = request.address["expected_deadline_ms"]
            .as_i64()
            .ok_or_else(|| Error::invalid("exact deadline watch has no numeric deadline"))?;
        if request.expires_at_ms < deadline {
            return Err(Error::invalid(
                "exact deadline watch cannot expire before its retained deadline",
            ));
        }
        Some(deadline)
    } else {
        None
    };
    let subject_key = subject_key(
        &scope,
        &principal.client_id,
        &request.watch_kind,
        &request.address,
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
        let existing_expires_at_ms = existing["expires_at_ms"].as_i64().unwrap_or_default();
        let invalid_deadline_boundary =
            exact_deadline_ms.is_some_and(|deadline| existing_expires_at_ms < deadline);
        if existing["state"] == "active"
            && existing_expires_at_ms > now
            && !invalid_deadline_boundary
        {
            return Ok(create_receipt(&existing, operation_id, true));
        }
        if existing["state"] == "active" {
            settle_record(tx, &mut existing, "expired", now)?;
        }
    }
    let target = validate_target(tx, principal, &scope, &request.watch_kind, &request.address)?;
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
        "address":request.address,
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
        "target_state_at_create":target,
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
    let live_scope = match request_scope(
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
    ) {
        Ok(scope) => scope,
        Err(current_error) if record["watch_kind"] != "operation_terminal" => {
            if event_creator_is_current(tx, &record)? {
                stored_scope
            } else {
                return Err(current_error);
            }
        }
        Err(current_error) => return Err(current_error),
    };
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
    db: &Connection,
    principal: &Principal,
    scope: &ExactScope,
    operation_id: &str,
) -> Result<Value> {
    if !super::operation_visible_to(db, principal, operation_id)? {
        return Err(Error::new(
            "NOT_FOUND",
            "Operation was not found or is not visible in this identity",
        ));
    }
    let operation = operations::get_operation(db, operation_id)?;
    if operation["task_id"] != scope.task_id || operation["attempt_id"] != scope.attempt_id {
        return Err(Error::new(
            "WATCH_SCOPE_MISMATCH",
            "operation is outside the exact current Task/Attempt scope",
        ));
    }
    let attempt = tasks::get_attempt(db, &scope.attempt_id)?;
    if attempt["task_id"] != scope.task_id || attempt["task_revision"] != scope.task_revision {
        return Err(Error::new(
            "WATCH_SCOPE_MISMATCH",
            "operation Attempt does not match the exact current Task revision",
        ));
    }
    Ok(json!({"state":operation["state"]}))
}

fn validate_target(
    db: &Connection,
    principal: &Principal,
    scope: &ExactScope,
    watch_kind: &str,
    address: &Value,
) -> Result<Value> {
    validate_address_scope(watch_kind, address, scope)?;
    match watch_kind {
        "operation_terminal" => {
            let operation_id = model::text(address, "operation_id")?;
            let target = visible_operation(db, principal, scope, operation_id)?;
            Ok(target["state"].clone())
        }
        "contract_revision_changed" => {
            let client_id = model::text(address, "client_id")?;
            if principal.role == Role::Participant && client_id != principal.client_id {
                return Err(Error::new(
                    "FORBIDDEN",
                    "participants may watch only their own exact contract card",
                ));
            }
            if coordination::watch_scope_for_creator(
                db,
                "participant",
                client_id,
                &scope.task_id,
                scope.task_revision,
                &scope.attempt_id,
            )?
            .is_none()
            {
                return Err(Error::new(
                    "NOT_FOUND",
                    "contract card owner is not an active Participant in this exact scope",
                ));
            }
            let contract_key = model::text(address, "contract_key")?;
            let expected_revision = address["expected_revision"]
                .as_i64()
                .ok_or_else(|| damaged("contract watch has no expected revision"))?;
            let card = meta(
                db,
                &keys::card_key(&scope.id, "contract", contract_key, client_id),
            )?;
            let revision = card
                .as_ref()
                .and_then(|value| value["card_revision"].as_i64())
                .unwrap_or(0);
            if revision != expected_revision {
                return Err(Error::new(
                    "STALE_REVISION",
                    "contract card revision differs from expected_revision",
                ));
            }
            Ok(json!({
                "client_id":client_id,
                "contract_key":contract_key,
                "card_revision":revision,
                "state":card.as_ref().and_then(|value| value.get("state")).cloned().unwrap_or(json!("absent")),
            }))
        }
        "task_revision_changed" => {
            let task = tasks::get_task(db, &scope.task_id)?;
            if task["revision"] != scope.task_revision {
                return Err(Error::new(
                    "STALE_REVISION",
                    "Task revision differs from watch scope",
                ));
            }
            Ok(json!({"task_id":scope.task_id,"revision":task["revision"]}))
        }
        "attempt_disposition_changed" => {
            let attempt = tasks::get_attempt(db, &scope.attempt_id)?;
            if !attempt["released_at_ms"].is_null()
                || attempt["task_id"] != scope.task_id
                || attempt["task_revision"] != scope.task_revision
                || attempt["state"] != address["expected_state"]
            {
                return Err(Error::new(
                    "STALE_REVISION",
                    "Attempt is not live at expected_state in the exact watch scope",
                ));
            }
            Ok(json!({
                "attempt_id":scope.attempt_id,
                "state":attempt["state"],
                "released_at_ms":null,
            }))
        }
        "submission_reviewed" => {
            if !submission_subject_is_current(db, scope, address)? {
                return Err(Error::new(
                    "WATCH_SUBJECT_MISMATCH",
                    "watch must name the exact applied submission and candidate in this current Attempt",
                ));
            }
            let cursor = submission_review_cursor(db, scope, address)?;
            Ok(json!({
                "submission_ref":address["submission_ref"],
                "candidate_ref":address["candidate_ref"],
                "state":if cursor.is_some() { "reviewed" } else { "awaiting_review" },
            }))
        }
        "exact_deadline_reached" => {
            let operation_id = model::text(address, "operation_id")?;
            let _ = visible_operation(db, principal, scope, operation_id)?;
            let operation = operations::get_operation(db, operation_id)?;
            let expected_deadline_ms = address["expected_deadline_ms"].as_i64();
            if operation["method"] != "coordination.consult"
                || operation["state"] != "settled"
                || operation["result"]["ask"]["reply_deadline_ms"].as_i64() != expected_deadline_ms
            {
                return Err(Error::new(
                    "WATCH_SUBJECT_MISMATCH",
                    "deadline watch must name the exact retained coordination.consult reply deadline",
                ));
            }
            Ok(json!({
                "operation_id":operation_id,
                "deadline_field":"reply_deadline_ms",
                "deadline_at_ms":expected_deadline_ms,
            }))
        }
        _ => Err(Error::new(
            "WATCH_KIND_UNSUPPORTED",
            format!("watch kind {watch_kind:?} has no authoritative Store fact source"),
        )),
    }
}

fn validate_address_scope(watch_kind: &str, address: &Value, scope: &ExactScope) -> Result<()> {
    let matches = match watch_kind {
        "contract_revision_changed" => {
            address["task_id"] == scope.task_id && address["attempt_id"] == scope.attempt_id
        }
        "task_revision_changed" => {
            address["task_id"] == scope.task_id
                && address["expected_revision"] == scope.task_revision
        }
        "attempt_disposition_changed" => address["attempt_id"] == scope.attempt_id,
        "submission_reviewed" => true,
        "operation_terminal" | "exact_deadline_reached" => true,
        _ => false,
    };
    if !matches {
        return Err(Error::new(
            "WATCH_SCOPE_MISMATCH",
            "watch address differs from the authenticated exact Task/Attempt scope",
        ));
    }
    Ok(())
}

enum EventCursor {
    Pending,
    StaleSubject,
    Matched(Value),
}

fn event_cursor(tx: &Transaction<'_>, record: &Value, now: i64) -> Result<EventCursor> {
    let watch_kind = model::text(record, "watch_kind")?;
    let address = &record["address"];
    let scope = scope_from_record(record)?;
    match watch_kind {
        "operation_terminal" => {
            let Some(cursor) = terminal_operation_cursor(tx, record)? else {
                return Ok(EventCursor::StaleSubject);
            };
            if cursor["settled_at_ms"].is_null()
                || !matches!(
                    cursor["state"].as_str(),
                    Some("settled" | "rejected" | "cancelled")
                )
            {
                return Ok(EventCursor::Pending);
            }
            Ok(EventCursor::Matched(cursor))
        }
        "contract_revision_changed" => {
            let expected = address["expected_revision"]
                .as_i64()
                .ok_or_else(|| damaged("contract watch expected_revision is invalid"))?;
            match contract_revision_cursor(tx, &scope, address, expected)? {
                Some(cursor) => Ok(EventCursor::Matched(cursor)),
                None => Ok(EventCursor::Pending),
            }
        }
        "task_revision_changed" => {
            let task = match tasks::get_task(tx, &scope.task_id) {
                Ok(task) => task,
                Err(error) if error.code == "NOT_FOUND" => {
                    return Ok(EventCursor::StaleSubject);
                }
                Err(error) => return Err(error),
            };
            let expected = address["expected_revision"].as_i64().unwrap_or_default();
            let current = task["revision"].as_i64().unwrap_or_default();
            if current < expected {
                return Ok(EventCursor::StaleSubject);
            }
            if current == expected {
                return Ok(EventCursor::Pending);
            }
            Ok(EventCursor::Matched(json!({
                "task_id":scope.task_id,
                "expected_revision":expected,
                "current_revision":current,
            })))
        }
        "attempt_disposition_changed" => {
            let attempt = match tasks::get_attempt(tx, &scope.attempt_id) {
                Ok(attempt) => attempt,
                Err(error) if error.code == "NOT_FOUND" => {
                    return Ok(EventCursor::StaleSubject);
                }
                Err(error) => return Err(error),
            };
            if attempt["task_id"] != scope.task_id
                || attempt["task_revision"] != scope.task_revision
            {
                return Ok(EventCursor::StaleSubject);
            }
            let Some(released_at_ms) = attempt["released_at_ms"].as_i64() else {
                return Ok(EventCursor::Pending);
            };
            let disposition = attempt["state"].as_str().unwrap_or_default();
            if !matches!(
                disposition,
                "accepted" | "failed" | "cancelled" | "superseded"
            ) {
                return Ok(EventCursor::StaleSubject);
            }
            Ok(EventCursor::Matched(json!({
                "attempt_id":scope.attempt_id,
                "expected_state":address["expected_state"],
                "disposition":disposition,
                "released_at_ms":released_at_ms,
            })))
        }
        "submission_reviewed" => {
            if !submission_subject_is_retained_exact(tx, &scope, address)? {
                return Ok(EventCursor::StaleSubject);
            }
            match submission_review_cursor(tx, &scope, address)? {
                Some(cursor) => Ok(EventCursor::Matched(cursor)),
                None => Ok(EventCursor::Pending),
            }
        }
        "exact_deadline_reached" => {
            let operation_id = model::text(address, "operation_id")?;
            let operation = match operations::get_operation(tx, operation_id) {
                Ok(operation) => operation,
                Err(error) if error.code == "NOT_FOUND" => {
                    return Ok(EventCursor::StaleSubject);
                }
                Err(error) => return Err(error),
            };
            if operation["method"] != "coordination.consult"
                || operation["task_id"] != scope.task_id
                || operation["attempt_id"] != scope.attempt_id
                || operation["state"] != "settled"
                || operation["result"]["ask"]["reply_deadline_ms"]
                    != address["expected_deadline_ms"]
            {
                return Ok(EventCursor::StaleSubject);
            }
            let deadline = address["expected_deadline_ms"].as_i64().unwrap_or_default();
            if now < deadline {
                return Ok(EventCursor::Pending);
            }
            Ok(EventCursor::Matched(json!({
                "operation_id":operation_id,
                "deadline_field":"reply_deadline_ms",
                "deadline_at_ms":deadline,
                "observed_at_ms":now,
            })))
        }
        _ => Err(damaged("watch record names an unsupported kind")),
    }
}

fn submission_subject_is_current(
    db: &Connection,
    scope: &ExactScope,
    address: &Value,
) -> Result<bool> {
    let Some((task, attempt)) = retained_submission_subject(db, scope, address)? else {
        return Ok(false);
    };
    Ok(attempt["released_at_ms"].is_null()
        && matches!(
            attempt["state"].as_str(),
            Some("submitted" | "needs_correction")
        )
        && task["state"].as_str() == Some("open")
        && task["revision"].as_i64() == Some(scope.task_revision)
        && task["current_attempt_id"].as_str() == Some(scope.attempt_id.as_str()))
}

fn submission_subject_is_retained_exact(
    db: &Connection,
    scope: &ExactScope,
    address: &Value,
) -> Result<bool> {
    Ok(retained_submission_subject(db, scope, address)?.is_some())
}

/// Resolve only the immutable Task/Attempt/submission/candidate identity.
/// Unlike admission, event delivery remains valid after the Attempt is released
/// or the Task advances, provided its retained applied submission still matches.
fn retained_submission_subject(
    db: &Connection,
    scope: &ExactScope,
    address: &Value,
) -> Result<Option<(Value, Value)>> {
    let submission_ref = model::text(address, "submission_ref")?;
    let candidate_ref = model::text(address, "candidate_ref")?;
    let attempt = match tasks::get_attempt(db, &scope.attempt_id) {
        Ok(attempt) => attempt,
        Err(error) if error.code == "NOT_FOUND" => return Ok(None),
        Err(error) => return Err(error),
    };
    let task = match tasks::get_task(db, &scope.task_id) {
        Ok(task) => task,
        Err(error) if error.code == "NOT_FOUND" => return Ok(None),
        Err(error) => return Err(error),
    };
    if attempt["task_id"].as_str() != Some(scope.task_id.as_str())
        || attempt["task_revision"].as_i64() != Some(scope.task_revision)
        || attempt["submission_ref"].as_str() != Some(submission_ref)
        || attempt["candidate_ref"].as_str() != Some(candidate_ref)
    {
        return Ok(None);
    }
    let document = submissions::document(db, submission_ref)?;
    if document["task_id"].as_str() != Some(scope.task_id.as_str())
        || document["attempt_id"].as_str() != Some(scope.attempt_id.as_str())
        || document["task_revision"].as_i64() != Some(scope.task_revision)
        || document["candidate_ref"].as_str() != Some(candidate_ref)
    {
        return Ok(None);
    }
    Ok(Some((task, attempt)))
}

fn submission_review_cursor(
    db: &Connection,
    scope: &ExactScope,
    address: &Value,
) -> Result<Option<Value>> {
    let submission_ref = model::text(address, "submission_ref")?;
    let candidate_ref = model::text(address, "candidate_ref")?;
    let row: Option<(String, String, String, i64)> = db
        .query_row(
            "SELECT source_event_key,operation_id,payload_json,recorded_at_ms FROM observations \
             WHERE source_stream_id='controller:review' AND kind='review.result' \
               AND source_event_key GLOB 'result:*' \
               AND json_extract(payload_json,'$.identity.task_id')=?1 \
               AND json_extract(payload_json,'$.identity.task_revision')=?2 \
               AND json_extract(payload_json,'$.identity.attempt_id')=?3 \
               AND json_extract(payload_json,'$.identity.submission_ref')=?4 \
               AND json_extract(payload_json,'$.identity.candidate_ref')=?5 \
             ORDER BY observation_id LIMIT 1",
            params![
                scope.task_id,
                scope.task_revision,
                scope.attempt_id,
                submission_ref,
                candidate_ref,
            ],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()?;
    let Some((event_key, result_operation_id, result_raw, reviewed_at_ms)) = row else {
        return Ok(None);
    };
    let assignment_id = event_key
        .strip_prefix("result:")
        .filter(|value| !value.is_empty())
        .ok_or_else(|| damaged("review result event key has no assignment identity"))?;
    let result_record: Value = serde_json::from_str(&result_raw)?;
    let assignment_event_key = format!("assignment:{assignment_id}");
    let assignment_row: Option<(String, String)> = db
        .query_row(
            "SELECT operation_id,payload_json FROM observations \
             WHERE source_stream_id='controller:review' AND source_event_key=?1 AND kind='review.assignment'",
            [&assignment_event_key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((assignment_operation_id, assignment_raw)) = assignment_row else {
        return Err(damaged("review result has no retained assignment fact"));
    };
    let assignment: Value = serde_json::from_str(&assignment_raw)?;
    let identity: crate::review::ReviewSlotIdentity =
        serde_json::from_value(assignment["identity"].clone())?;
    if assignment["review_assignment_id"] != assignment_id
        || assignment["operation_id"] != assignment_operation_id
        || result_record["schema_version"] != 1
        || result_record["review_assignment_id"] != assignment_id
        || result_record["operation_id"] != result_operation_id
        || result_record["identity"] != assignment["identity"]
        || result_record["result"]["review_assignment_id"] != assignment_id
        || identity.task_id.as_str() != scope.task_id.as_str()
        || identity.task_revision != scope.task_revision
        || identity.attempt_id.as_str() != scope.attempt_id.as_str()
        || address["submission_ref"].as_str() != Some(identity.submission_ref.as_str())
        || address["candidate_ref"].as_str() != Some(identity.candidate_ref.as_str())
        || reviewed_at_ms <= 0
    {
        return Err(damaged(
            "review result differs from the exact watch subject",
        ));
    }
    let assignment_operation = operations::get_operation(db, &assignment_operation_id)?;
    let result_operation = operations::get_operation(db, &result_operation_id)?;
    if assignment_operation["method"] != "review.assign"
        || assignment_operation["state"] != "settled"
        || assignment_operation["task_id"].as_str() != Some(scope.task_id.as_str())
        || assignment_operation["attempt_id"].as_str() != Some(scope.attempt_id.as_str())
        || assignment_operation["result"]["review_assignment_id"] != assignment_id
        || assignment_operation["result"]["identity"] != assignment["identity"]
        || result_operation["method"] != "review.submit"
        || result_operation["state"] != "settled"
        || result_operation["task_id"].as_str() != Some(scope.task_id.as_str())
        || result_operation["attempt_id"].as_str() != Some(scope.attempt_id.as_str())
        || result_operation["caller_id"] != assignment["reviewer_client_id"]
        || result_operation["result"] != result_record["result"]
        || result_operation["result"]["task_id"].as_str() != Some(scope.task_id.as_str())
        || result_operation["result"]["attempt_id"].as_str() != Some(scope.attempt_id.as_str())
        || result_operation["result"]["task_revision"].as_i64() != Some(scope.task_revision)
        || result_operation["result"]["submission_ref"].as_str() != Some(submission_ref)
        || result_operation["result"]["candidate_ref"].as_str() != Some(candidate_ref)
        || !matches!(
            result_operation["result"]["verdict"].as_str(),
            Some("pass" | "changes_requested")
        )
    {
        return Err(damaged(
            "review result has no matching settled assigned-review Operations",
        ));
    }
    Ok(Some(json!({
        "review_assignment_id":assignment_id,
        "verdict":result_operation["result"]["verdict"],
        "reviewed_at_ms":reviewed_at_ms,
    })))
}

fn contract_revision_cursor(
    tx: &Transaction<'_>,
    scope: &ExactScope,
    address: &Value,
    expected_revision: i64,
) -> Result<Option<Value>> {
    let Some(next_revision) = expected_revision.checked_add(1) else {
        return Ok(None);
    };
    let contract_key = model::text(address, "contract_key")?;
    let client_id = model::text(address, "client_id")?;
    let prefix = format!(
        "coordination:card-revision:{}:contract:{}:{}:",
        scope.id,
        keys::key_component(contract_key),
        keys::key_component(client_id),
    );
    let lower = format!("{prefix}{next_revision:020}");
    let upper = format!("{prefix}g");
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT 1",
            params![lower, upper],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((key, raw)) = row else {
        return Ok(None);
    };
    let card: Value = serde_json::from_str(&raw)?;
    let Some(revision) = card["card_revision"].as_i64() else {
        return Err(damaged(
            "retained contract card revision has no numeric revision",
        ));
    };
    let key_revision = key
        .rsplit(':')
        .next()
        .and_then(|value| value.parse::<i64>().ok());
    if revision < next_revision
        || key_revision != Some(revision)
        || card["card_kind"] != "contract"
        || card["identity"] != contract_key
        || card["task_id"] != scope.task_id
        || card["task_revision"] != scope.task_revision
        || card["attempt_id"] != scope.attempt_id
        || card["client_id"] != client_id
        || !matches!(card["state"].as_str(), Some("current" | "withdrawn"))
    {
        return Err(damaged(
            "retained contract revision differs from its exact watch address",
        ));
    }
    Ok(Some(json!({
        "task_id":scope.task_id,
        "attempt_id":scope.attempt_id,
        "contract_key":contract_key,
        "client_id":client_id,
        "expected_revision":expected_revision,
        "card_revision":revision,
        "state":card["state"],
        "updated_at_ms":card["updated_at_ms"],
    })))
}

fn notification_facts(watch_kind: &str, address: &Value, cursor: &Value) -> Value {
    match watch_kind {
        "operation_terminal" => json!({
            "operation_id":address["operation_id"],
            "state":cursor["state"],
            "settled_at_ms":cursor["settled_at_ms"],
        }),
        "contract_revision_changed" => cursor.clone(),
        "task_revision_changed" => cursor.clone(),
        "attempt_disposition_changed" => cursor.clone(),
        "submission_reviewed" => cursor.clone(),
        "exact_deadline_reached" => cursor.clone(),
        _ => Value::Null,
    }
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

fn stored_creator_scope_is_current(db: &Connection, record: &Value) -> Result<bool> {
    let scope = scope_from_record(record)?;
    let creator = &record["creator"];
    let role = model::text(creator, "role")?;
    let client_id = model::text(creator, "client_id")?;
    Ok(coordination::watch_scope_for_creator(
        db,
        role,
        client_id,
        &scope.task_id,
        scope.task_revision,
        &scope.attempt_id,
    )?
    .is_some())
}

fn event_creator_is_current(db: &Connection, record: &Value) -> Result<bool> {
    let scope = scope_from_record(record)?;
    let creator = &record["creator"];
    coordination::watch_creator_authorized_for_subject(
        db,
        model::text(creator, "role")?,
        model::text(creator, "client_id")?,
        &scope.task_id,
        scope.task_revision,
        &scope.attempt_id,
    )
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

fn retained_transition_scope(
    db: &Connection,
    principal: &Principal,
    task_id: Option<&str>,
    task_revision: Option<i64>,
    attempt_id: Option<&str>,
) -> Result<Option<ExactScope>> {
    let (Some(task_id), Some(task_revision), Some(attempt_id)) =
        (task_id, task_revision, attempt_id)
    else {
        // Participants do not supply subject identity on list calls. Their
        // post-transition receipt is exposed through the owner-only inbox
        // header projection instead.
        return Ok(None);
    };
    if !matches!(principal.role, Role::Manager | Role::Operator) {
        return Ok(None);
    }
    let role = role_tag(&principal.role)?;
    if !coordination::watch_creator_authorized_for_subject(
        db,
        role,
        &principal.client_id,
        task_id,
        task_revision,
        attempt_id,
    )? {
        return Ok(None);
    }
    Ok(Some(ExactScope {
        id: keys::scope_id(task_id, task_revision, attempt_id)?,
        task_id: task_id.to_owned(),
        task_revision,
        attempt_id: attempt_id.to_owned(),
    }))
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
        || !matches!(
            record["watch_kind"].as_str(),
            Some(
                "operation_terminal"
                    | "contract_revision_changed"
                    | "task_revision_changed"
                    | "attempt_disposition_changed"
                    | "submission_reviewed"
                    | "exact_deadline_reached"
            )
        )
        || record["delivery"] != "mailbox_header"
        || record["one_shot"] != true
        || !matches!(
            record["state"].as_str(),
            Some("active" | "matched" | "cancelled" | "expired" | "stale_scope" | "stale_subject")
        )
    {
        return Err(damaged("watch record does not match its supported schema"));
    }
    let scope = scope_from_record(record)?;
    let _ = model::text(&record["creator"], "client_id")?;
    let _ = model::text(&record["creator"], "role")?;
    let watch_kind = model::text(record, "watch_kind")?;
    watch::validate_address(watch_kind, &record["address"])
        .map_err(|_| damaged("watch address does not match its supported schema"))?;
    validate_address_scope(watch_kind, &record["address"], &scope)
        .map_err(|_| damaged("watch address does not match its retained exact scope"))?;
    Ok(())
}

/// At this call site `verify_record` only decodes fields from the retained
/// watch value; INVALID_PARAMS therefore identifies semantic record damage.
/// Keep every other error unchanged so storage and transaction failures stay
/// outside the watch quarantine classifier.
fn verify_reconcile_record(record: &Value, expected_id: &str) -> Result<()> {
    match verify_record(record, expected_id) {
        Err(error) if error.code.as_str() == "INVALID_PARAMS" => {
            Err(damaged("retained watch record has invalid typed fields"))
        }
        result => result,
    }
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

fn valid_revisit_key(key: &str, index_key: &str) -> bool {
    key.strip_prefix(REVISIT_PREFIX).is_some_and(|suffix| {
        suffix.split_once(':').is_some_and(|(sequence, component)| {
            sequence.len() == 20
                && sequence.bytes().all(|byte| byte.is_ascii_digit())
                && index_key.strip_prefix(ACTIVE_PREFIX) == Some(component)
        })
    })
}

fn remove_watch_revisit(tx: &Transaction<'_>, index_key: &str, raw_index: &str) -> Result<()> {
    // Malformed active bytes do not authorize a guessed queue deletion.
    if let Ok(index) = serde_json::from_str::<Value>(raw_index)
        && let Some(key) = index["revisit_key"].as_str()
        && valid_revisit_key(key, index_key)
    {
        tx.execute("DELETE FROM meta WHERE key=?1", [key])?;
    }
    Ok(())
}

fn queue_pending_watch(tx: &Transaction<'_>, index_key: &str, raw_index: &str) -> Result<()> {
    let mut index: Value = serde_json::from_str(raw_index)?;
    if let Some(key) = index["revisit_key"].as_str() {
        let retained: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
            [key],
            |row| row.get(0),
        )?;
        // A forward-page observation must not send an unserved pending
        // subject to the back of the FIFO. Only a due revisit rotates it.
        if retained {
            return Ok(());
        }
    }
    let sequence = match meta(tx, REVISIT_SEQUENCE_KEY)? {
        None => 0,
        Some(value) => value
            .as_i64()
            .filter(|sequence| *sequence >= 0)
            .ok_or_else(|| {
                Error::new("WATCH_CURSOR_DAMAGED", "watch revisit sequence is invalid")
            })?,
    }
    .checked_add(1)
    .ok_or_else(|| Error::new("WATCH_CURSOR_DAMAGED", "watch revisit sequence overflow"))?;
    let component = index_key
        .strip_prefix(ACTIVE_PREFIX)
        .ok_or_else(|| Error::new("WATCH_CURSOR_DAMAGED", "watch revisit subject is invalid"))?;
    let key = format!("{REVISIT_PREFIX}{sequence:020}:{component}");
    remove_watch_revisit(tx, index_key, raw_index)?;
    index["revisit_key"] = json!(key);
    set_meta(tx, &key, &json!({"active_key":index_key}))?;
    set_meta(tx, index_key, &index)?;
    set_meta(tx, REVISIT_SEQUENCE_KEY, &json!(sequence))?;
    Ok(())
}

fn scan_active(
    tx: &Transaction<'_>,
    cursor: Option<&str>,
    limit: i64,
) -> Result<(Vec<(String, String)>, Option<String>)> {
    let cursor = match meta(tx, REVISIT_SEQUENCE_KEY)? {
        None => {
            // One bounded sweep from the start enrolls pending subjects that
            // the previous implementation already passed without scheduling.
            set_meta(tx, REVISIT_SEQUENCE_KEY, &json!(0))?;
            None
        }
        Some(value) if value.as_i64().is_some_and(|sequence| sequence >= 0) => cursor,
        Some(_) => {
            return Err(Error::new(
                "WATCH_CURSOR_DAMAGED",
                "watch revisit sequence is invalid",
            ));
        }
    };
    // Half the bounded page revisits oldest pending subjects. Requeueing
    // appends a monotonically increasing key, so new rows cannot pass them.
    // For a one-row budget alternate the two lanes durably.
    let revisit_limit = if limit == 1 {
        let turn = match meta(tx, REVISIT_TURN_KEY)? {
            None => false,
            Some(value) => value.as_bool().ok_or_else(|| {
                Error::new("WATCH_CURSOR_DAMAGED", "watch revisit turn is invalid")
            })?,
        };
        set_meta(tx, REVISIT_TURN_KEY, &json!(!turn))?;
        i64::from(turn)
    } else {
        limit / 2
    };
    let revisit_upper = format!("{REVISIT_PREFIX}g");
    let revisit_rows = {
        let mut statement = tx.prepare(
            "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key LIMIT ?3",
        )?;
        statement
            .query_map(
                params![REVISIT_PREFIX, revisit_upper, revisit_limit],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    let mut rows = Vec::new();
    for (queue_key, raw) in revisit_rows {
        let queued: Value = serde_json::from_str(&raw)?;
        let index_key = model::text(&queued, "active_key")?;
        if !valid_revisit_key(&queue_key, index_key) {
            return Err(Error::new(
                "WATCH_CURSOR_DAMAGED",
                "watch revisit index is invalid",
            ));
        }
        let raw_index: Option<String> = tx
            .query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [index_key],
                |row| row.get(0),
            )
            .optional()?;
        if let Some(raw_index) = raw_index {
            // A damaged active row is still processed as its own source.
            if watch_id_from_active_index(index_key, &raw_index).is_ok()
                && let Ok(index) = serde_json::from_str::<Value>(&raw_index)
                && index["revisit_key"].as_str() != Some(queue_key.as_str())
            {
                return Err(Error::new(
                    "WATCH_CURSOR_DAMAGED",
                    "watch revisit binding differs",
                ));
            }
            rows.push((index_key.to_owned(), raw_index));
            tx.execute("DELETE FROM meta WHERE key=?1", [queue_key])?;
        } else {
            tx.execute("DELETE FROM meta WHERE key=?1", [queue_key])?;
        }
    }
    let upper = format!("{ACTIVE_PREFIX}g");
    let remaining = limit - rows.len() as i64;
    let (lower, comparison) = cursor.map_or((ACTIVE_PREFIX, ">="), |key| (key, ">"));
    let mut statement = tx.prepare(&format!(
        "SELECT key,value_json FROM meta WHERE key {comparison} ?1 AND key<?2 ORDER BY key LIMIT ?3",
    ))?;
    let mut forward = statement
        .query_map(params![lower, upper, remaining], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if (forward.len() as i64) < remaining
        && let Some(cursor) = cursor
    {
        let mut statement = tx.prepare(
            "SELECT key,value_json FROM meta WHERE key>=?1 AND key<=?2 ORDER BY key LIMIT ?3",
        )?;
        let wrapped = statement
            .query_map(
                params![ACTIVE_PREFIX, cursor, remaining - forward.len() as i64],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        forward.extend(wrapped);
    }
    let next_cursor = forward.last().map(|(key, _)| key.clone());
    for row in forward {
        if !rows.iter().any(|(key, _)| key == &row.0) {
            rows.push(row);
        }
    }
    Ok((rows, next_cursor))
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
    address: &Value,
) -> Result<String> {
    let subject = if watch_kind == "operation_terminal" {
        // Preserve the original O1 fingerprint so retries against existing
        // retained operation watches continue to coalesce after this extension.
        json!({
            "scope_id":scope.id,
            "watcher_id":creator_id,
            "watch_kind":watch_kind,
            "operation_id":address["operation_id"],
        })
    } else {
        json!({
            "scope_id":scope.id,
            "watcher_id":creator_id,
            "watch_kind":watch_kind,
            "address":address,
        })
    };
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

fn owner_notice_prefix(creator_id: &str) -> String {
    format!("{OWNER_NOTICE_PREFIX}{}:", keys::key_component(creator_id))
}

fn owner_notice_key(creator_id: &str, matched_at_ms: i64, watch_id: &str) -> String {
    format!(
        "{}{matched_at_ms:020}:{}",
        owner_notice_prefix(creator_id),
        keys::key_component(watch_id)
    )
}

fn damaged(message: &str) -> Error {
    Error::new("WATCH_RECORD_DAMAGED", message)
}
