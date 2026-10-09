//! Public Task graph projections and stable object paging over Store-owned facts.
use super::{object_scope, operations, projection, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal},
};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

fn missing() -> Error {
    Error::new(
        "NOT_FOUND",
        "object is outside this principal's retained scope",
    )
}

pub(super) fn require_task(
    db: &Connection,
    p: &Principal,
    identity: &object_scope::TaskGraphIdentity,
    level: object_scope::TaskReadLevel,
) -> Result<object_scope::TaskReadGrant> {
    object_scope::resolve_task_read(db, p, identity, level)?.ok_or_else(missing)
}

fn select(value: &Value, fields: &[&str]) -> Value {
    let mut result = serde_json::Map::new();
    for field in fields {
        if let Some(value) = value.get(*field) {
            result.insert((*field).to_owned(), value.clone());
        }
    }
    Value::Object(result)
}

pub(super) fn task(db: &Connection, p: &Principal, id: &str) -> Result<Value> {
    let identity = object_scope::identity_for_task(db, id)?.ok_or_else(missing)?;
    let grant = require_task(db, p, &identity, object_scope::TaskReadLevel::Detail)?;
    let value = tasks::get_task(db, id)?;
    let mut result = select(
        &value,
        &[
            "task_id",
            "project_id",
            "revision",
            "state",
            "current_attempt_id",
            "accepted_attempt_id",
            "accepted_operation_id",
            "accepted_revision",
            "accepted_phase",
            "accepted_candidate_ref",
        ],
    );
    if grant.level >= object_scope::TaskReadLevel::Detail {
        for field in ["spec", "task_brief"] {
            result[field] = value[field].clone();
        }
    }
    Ok(result)
}

pub(super) fn attempt(db: &Connection, p: &Principal, id: &str) -> Result<Value> {
    let identity = object_scope::identity_for_attempt(db, id)?.ok_or_else(missing)?;
    let grant = require_task(db, p, &identity, object_scope::TaskReadLevel::Detail)?;
    let value = tasks::get_attempt(db, id)?;
    let mut result = select(
        &value,
        &[
            "attempt_id",
            "task_id",
            "task_revision",
            "owner_id",
            "state",
            "released_at_ms",
            "binding_id",
            "binding_generation",
            "start_owner",
            "start_operation_id",
            "submission_ref",
            "candidate_ref",
        ],
    );
    if grant.level >= object_scope::TaskReadLevel::Detail {
        for field in ["task_snapshot", "task_brief", "owner_policy"] {
            result[field] = value[field].clone();
        }
    }
    // Native producer data is disclosed through the separately bounded family
    // projection, not copied from the raw internal Attempt materialization.
    Ok(result)
}

pub(super) fn operation(db: &Connection, p: &Principal, id: &str) -> Result<Value> {
    let grant = object_scope::resolve_operation_read(db, p, id)?.ok_or_else(missing)?;
    operations::project_operation(db, p, id, grant)
}

fn require_evidence(
    db: &Connection,
    p: &Principal,
    identity: &object_scope::TaskGraphIdentity,
    operation_id: &str,
) -> Result<()> {
    let producer = object_scope::load_operation(db, operation_id)?.ok_or_else(missing)?;
    if producer.task_id.as_deref() != Some(identity.task_id.as_str())
        || producer.attempt_id.as_deref() != identity.attempt_id.as_deref()
        || identity.attempt_id.is_none()
    {
        return Err(Error::new(
            "OBJECT_SCOPE_DAMAGED",
            "evidence producer differs from the retained frozen Attempt",
        ));
    }
    if object_scope::resolve_task_read(db, p, identity, object_scope::TaskReadLevel::Evidence)?
        .is_some_and(|grant| grant.level >= object_scope::TaskReadLevel::Evidence)
    {
        return Ok(());
    }
    // The exact producing caller retains this one receipt, not the whole graph.
    if object_scope::resolve_operation_read(db, p, operation_id)?
        .is_some_and(|grant| grant.basis == object_scope::OperationReadBasis::ExactCaller)
    {
        return Ok(());
    }
    Err(missing())
}

pub(super) fn submission(db: &Connection, p: &Principal, v: &Value) -> Result<Value> {
    model::fields(v, &["submission_ref", "after", "limit"])?;
    let reference = model::text(v, "submission_ref")?;
    let document = super::submissions::document(db, reference)?;
    let artifact = super::results::get(db, reference)?;
    let identity = object_scope::identity_for_attempt(db, model::text(&document, "attempt_id")?)?
        .ok_or_else(missing)?;
    if identity.task_revision != document["task_revision"].as_i64() {
        return Err(Error::new(
            "SUBMISSION_DAMAGED",
            "submission revision differs from its frozen Attempt",
        ));
    }
    require_evidence(
        db,
        p,
        &identity,
        model::text(&artifact.metadata, "operation_id")?,
    )?;
    super::submissions::describe(db, v)
}

pub(super) fn acceptance(db: &Connection, p: &Principal, v: &Value) -> Result<Value> {
    let receipt = super::acceptance::describe(db, v)?;
    let identity = object_scope::identity_for_attempt(db, model::text(&receipt, "attempt_id")?)?
        .ok_or_else(missing)?;
    if receipt["task_id"] != identity.task_id
        || receipt["task_revision"].as_i64() != identity.task_revision
    {
        return Err(Error::new(
            "NOT_ACCEPTANCE",
            "acceptance scope differs from its exact frozen Attempt",
        ));
    }
    require_evidence(db, p, &identity, model::text(v, "acceptance_operation_id")?)?;
    Ok(receipt)
}

pub(super) fn check(db: &Connection, p: &Principal, v: &Value) -> Result<Value> {
    let receipt = super::checks::describe(db, v)?;
    let identity = object_scope::identity_for_attempt(db, model::text(&receipt, "attempt_id")?)?
        .ok_or_else(missing)?;
    let operation_id = model::text(&receipt, "operation_id")?;
    let operation = operations::get_operation(db, operation_id)?;
    if operation["method"] != "check.run"
        || operation["attempt_id"] != receipt["attempt_id"]
        || operation["task_id"] != identity.task_id
    {
        return Err(Error::new(
            "CHECK_DAMAGED",
            "CheckRun does not match its retained producing Operation",
        ));
    }
    require_evidence(db, p, &identity, operation_id)?;
    Ok(select(
        &receipt,
        &[
            "check_id",
            "operation_id",
            "attempt_id",
            "candidate_ref",
            "cached_from",
            "state",
            "resource_key",
            "resource_claimed_at_ms",
            "resource_released_at_ms",
            "coverage",
            "result_ref",
            "exit_code",
            "profile_id",
            "profile_revision",
            "cancel_request",
            "cancellation",
            "process_diagnostic",
            "cleanup_pending",
            "resource_released",
            "process_facts",
            "release_evidence",
        ],
    ))
}

pub(super) fn family(db: &Connection, p: &Principal, v: &Value) -> Result<Value> {
    model::fields(
        v,
        &[
            "binding_id",
            "generation",
            "observation_id",
            "after",
            "limit",
        ],
    )?;
    if p.role == crate::model::Role::Operator {
        super::require_local_operator(db, &p.client_id)?;
    } else {
        let binding_id = model::text(v, "binding_id")?;
        let generation = model::positive(v, "generation")?;
        let mut statement = db.prepare("SELECT attempt_id FROM attempts WHERE binding_id=?1 AND binding_generation=?2 ORDER BY created_at_ms,attempt_id LIMIT ?3")?;
        let ids = statement
            .query_map(
                params![
                    binding_id,
                    generation,
                    projection::MAX_PROJECTED_ITEMS as i64 + 1
                ],
                |row| row.get::<_, String>(0),
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ids.is_empty() || ids.len() > projection::MAX_PROJECTED_ITEMS {
            return Err(missing());
        }
        // A reused binding can retain several Attempts. The family projection
        // crosses all of them, so every retained relation needs an evidence grant.
        for attempt_id in ids {
            let identity =
                object_scope::identity_for_attempt(db, &attempt_id)?.ok_or_else(missing)?;
            let grant = require_task(db, p, &identity, object_scope::TaskReadLevel::Evidence)?;
            if grant.level < object_scope::TaskReadLevel::Evidence {
                return Err(missing());
            }
        }
    }
    super::producers::family(db, v)
}

#[derive(Clone, Copy)]
pub(super) enum ObjectKind {
    Task,
    Operation,
}

impl ObjectKind {
    fn source(self) -> &'static str {
        match self {
            Self::Task => "controller:read-position:task",
            Self::Operation => "controller:read-position:operation",
        }
    }
    fn table(self) -> (&'static str, &'static str) {
        match self {
            Self::Task => ("tasks", "task_id"),
            Self::Operation => ("operations", "operation_id"),
        }
    }
}

pub(super) fn list(db: &Connection, p: &Principal, kind: ObjectKind, v: &Value) -> Result<Value> {
    model::fields(
        v,
        match kind {
            ObjectKind::Task => &["after", "limit"],
            ObjectKind::Operation => &["after", "limit", "state"],
        },
    )?;
    let (limit, after) = super::page(v)?;
    let state = match v.get("state") {
        None => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or_else(|| Error::invalid("state must be text"))?,
        ),
    };
    let (table, key) = kind.table();
    let condition = if matches!(kind, ObjectKind::Operation) {
        "AND (?3 IS NULL OR target.state=?3)"
    } else {
        "AND ?3 IS NULL"
    };
    let sql = format!(
        "SELECT pos.observation_id,pos.source_event_key FROM observations AS pos JOIN {table} AS target ON target.{key}=pos.source_event_key WHERE pos.source_stream_id=?1 AND pos.observation_id>?2 {condition} ORDER BY pos.observation_id LIMIT ?4"
    );
    let mut statement = db.prepare(&sql)?;
    let rows = statement
        .query_map(params![kind.source(), after, state, limit + 1], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut examined = after;
    let mut before = Vec::new();
    let mut projected = Vec::new();
    let mut filtered = Vec::new();
    let mut damaged = Vec::new();
    for (position, id) in rows.iter().take(limit as usize) {
        let value = match kind {
            ObjectKind::Task => task(db, p, id),
            ObjectKind::Operation => operation(db, p, id),
        };
        let mut value = match value {
            Ok(value) => value,
            Err(error) if error.code == "NOT_FOUND" => {
                filtered.push(*position);
                examined = *position;
                continue;
            }
            // Damage has explicit coverage without naming the foreign object.
            // Store failures remain hard errors and never masquerade as a gap.
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "OBJECT_SCOPE_DAMAGED" | "INVALID_RECEIPT"
                ) =>
            {
                damaged.push(*position);
                json!({"gap":{"reason":"retained_relation_damaged","error_code":error.code}})
            }
            Err(error) => return Err(error),
        };
        before.push(examined);
        value["cursor"] = json!(position);
        projected.push(value);
        examined = *position;
    }
    let limited = projection::limit_items(projected, |entry, reason, length| {
        Ok(
            json!({"cursor":entry["cursor"],"task_id":entry["task_id"],"operation_id":entry["operation_id"],
            "gap":{"reason":reason,"serialized_byte_length":length,"read_method":match kind { ObjectKind::Task => "task.get", ObjectKind::Operation => "operation.get" }}}),
        )
    })?;
    let next = if limited.stopped_early {
        before[limited.consumed]
    } else {
        examined
    };
    let has_newer = limited.stopped_early || rows.len() > limit as usize;
    Ok(
        json!({"items":limited.items,"next_after":next,"pagination":"committed_observation_position",
        "examined_through":next,"filtered_count":filtered.iter().filter(|position| **position<=next).count(),
        "has_newer":has_newer,"coverage_complete":limited.gap_count==0 && damaged.iter().all(|position| *position>next),
        "gap_count":limited.gap_count+damaged.iter().filter(|position| **position<=next).count()}),
    )
}

const TIMELINE_CANDIDATE_SQL: &str = "WHERE o.observation_id>?1 AND o.observation_id<=?6 \
    AND o.source_stream_id NOT IN ('controller:messages','controller:read-position:operation','controller:read-position:task') \
    AND ((?2=1 AND o.kind IN ('message.send','coordination.message.send','task.feedback','check.completed') AND json_extract(o.payload_json,'$.recipient')=?3) \
      OR (?2=0 AND (o.operation_id IS NOT NULL OR ?4=1 \
        OR (o.kind NOT IN ('message.send','message.cancel','coordination.send','coordination.message.send','task.feedback','task.review_stale','check.completed') \
          AND o.kind NOT LIKE 'coordination.%' AND o.kind NOT LIKE 'review.%' AND o.kind NOT LIKE 'automation.%'))))";

/// Establish a concrete authorized boundary before a live subscriber starts.
/// Only bounded metadata is scanned; payloads are never materialized. If the
/// exact resolver cannot establish a boundary within this budget, admission
/// fails explicitly instead of silently starting at zero or a global cut.
fn timeline_head(db: &Connection, p: &Principal, mailbox_only: bool) -> Result<Value> {
    let operator = p.role == crate::model::Role::Operator;
    if operator {
        super::require_local_operator(db, &p.client_id)?;
    }
    let sql = format!(
        "SELECT o.observation_id,o.operation_id FROM observations AS o {TIMELINE_CANDIDATE_SQL} ORDER BY o.observation_id DESC LIMIT ?5"
    );
    let mut statement = db.prepare(&sql)?;
    let rows = statement
        .query_map(
            params![0, mailbox_only, p.client_id, operator, 201, i64::MAX],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for (cursor, operation_id) in rows.iter().take(200) {
        if let Some(id) = operation_id {
            match object_scope::resolve_operation_read(db, p, id) {
                Ok(Some(_)) => {}
                Ok(None) => continue,
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "OBJECT_SCOPE_DAMAGED" | "INVALID_RECEIPT"
                    ) =>
                {
                    return Err(Error::new(
                        "TIMELINE_HEAD_UNAVAILABLE",
                        "retained boundary relation is damaged",
                    ));
                }
                Err(error) => return Err(error),
            }
        }
        return Ok(json!({"cursor":cursor,"source_kind":"observation_timeline","head":true}));
    }
    if rows.len() > 200 {
        return Err(Error::new(
            "TIMELINE_HEAD_UNAVAILABLE",
            "authorized timeline boundary exceeds the bounded metadata scan",
        ));
    }
    Ok(json!({"cursor":0,"source_kind":"observation_timeline","head":true}))
}

/// Linked observations pass the same grant resolver as get/list. Candidate
/// filtering is only a bounded source scan; it does not establish disclosure.
pub(super) fn timeline(
    db: &Connection,
    p: &Principal,
    mailbox_only: bool,
    v: &Value,
) -> Result<Value> {
    model::fields(v, &["after", "limit", "head", "through"])?;
    if let Some(head) = v.get("head") {
        if head.as_bool().is_none() {
            return Err(Error::invalid("head must be boolean"));
        }
        if head == true {
            if ["after", "limit", "through"]
                .iter()
                .any(|field| v.get(*field).is_some())
            {
                return Err(Error::invalid(
                    "head is a separate metadata read and cannot be combined with page bounds",
                ));
            }
            return timeline_head(db, p, mailbox_only);
        }
    }
    let (limit, after) = super::page(v)?;
    let through = match v.get("through") {
        None => i64::MAX,
        Some(value) => value
            .as_i64()
            .filter(|cut| *cut >= after)
            .ok_or_else(|| Error::invalid("through must be an integer at or after the cursor"))?,
    };
    let operator = p.role == crate::model::Role::Operator;
    if operator {
        super::require_local_operator(db, &p.client_id)?;
    }
    let sql = format!(
        "SELECT o.observation_id,o.kind,o.payload_json,o.recorded_at_ms,o.operation_id FROM observations AS o {TIMELINE_CANDIDATE_SQL} ORDER BY o.observation_id LIMIT ?5"
    );
    let mut statement = db.prepare(&sql)?;
    let rows = statement
        .query_map(
            params![
                after,
                mailbox_only,
                p.client_id,
                operator,
                limit + 1,
                through
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<String>>(4)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut examined = after;
    let mut before = Vec::new();
    let mut filtered = Vec::new();
    let mut damaged = Vec::new();
    let mut entries = Vec::new();
    for (cursor, kind, raw, time, operation_id) in rows.iter().take(limit as usize) {
        let projected: Result<Option<Value>> = (|| {
            let grant = if let Some(id) = operation_id {
                let Some(grant) = object_scope::resolve_operation_read(db, p, id)? else {
                    return Ok(None);
                };
                Some(grant)
            } else {
                None
            };
            let retained: Value = serde_json::from_str(raw).map_err(|_| {
                Error::new(
                    "OBSERVATION_DAMAGED",
                    "retained observation payload is invalid",
                )
            })?;
            let payload = match grant {
                Some(grant) => project_scoped_observation(
                    db,
                    p,
                    kind,
                    &retained,
                    operation_id.as_deref().ok_or_else(missing)?,
                    grant,
                )?,
                None if kind == "runtime.state" => operations::public_observation(&retained),
                None => crate::redaction::value(retained),
            };
            Ok(Some(
                json!({"cursor":cursor,"kind":kind,"payload":payload,"recorded_at_ms":time,"operation_id":operation_id}),
            ))
        })();
        let entry = match projected {
            Ok(Some(entry)) => entry,
            Ok(None) => {
                filtered.push(*cursor);
                examined = *cursor;
                continue;
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "OBJECT_SCOPE_DAMAGED" | "INVALID_RECEIPT" | "OBSERVATION_DAMAGED"
                ) =>
            {
                damaged.push(*cursor);
                // Damage cannot reveal a foreign object ID, event kind or payload.
                json!({"cursor":cursor,"gap":{"reason":"retained_relation_damaged","error_code":error.code}})
            }
            Err(error) => return Err(error),
        };
        before.push(examined);
        entries.push(entry);
        examined = *cursor;
    }
    let limited = projection::limit_items(entries, projection::timeline_gap_reference)?;
    let next = if limited.stopped_early {
        before[limited.consumed]
    } else {
        examined
    };
    let delivered_damage = damaged.iter().filter(|cursor| **cursor <= next).count();
    let mut frame = projection::frame(
        if mailbox_only {
            "mailbox"
        } else {
            "observation_timeline"
        },
        json!({"after":after,"next_cursor":next,"examined_through":next,
            "filtered_count":filtered.iter().filter(|cursor| **cursor<=next).count()}),
        &limited,
        limit,
        after > 0,
        limited.stopped_early || rows.len() > limit as usize,
        limited.gap_count == 0 && delivered_damage == 0,
        Vec::new(),
    )?;
    frame["gap_count"] = json!(limited.gap_count + delivered_damage);
    if delivered_damage > 0 && frame["gap_reason"].is_null() {
        frame["gap_reason"] = json!("retained_relation_damaged");
    }
    let reminders = if mailbox_only {
        super::goals::notifications(db, p, limit)?
    } else {
        Value::Null
    };
    Ok(
        json!({"items":limited.items,"next_cursor":next,"projection":frame,"goal_reminders":reminders}),
    )
}

fn project_scoped_observation(
    db: &Connection,
    p: &Principal,
    kind: &str,
    payload: &Value,
    id: &str,
    grant: object_scope::OperationReadGrant,
) -> Result<Value> {
    let fields: &[&str] = match kind {
        "coordination.contract_ratified" | "coordination.contract_rejected" => &[
            "thread_id",
            "proposal_id",
            "proposal_revision_id",
            "decision_operation_id",
        ],
        "message.send"
        | "coordination.send"
        | "coordination.message.send"
        | "task.feedback"
        | "task.request_changes"
        | "task.review_stale" => &[
            "operation_id",
            "message_id",
            "sender",
            "recipient",
            "task_id",
            "text",
            "finding",
            "delivery_id",
            "payload_digest",
            "reply_to",
            "delivery",
            "status",
            "applied",
            "coalesced",
            "native_input_sent",
            "acceptance_changed",
            "repair_started",
            "publication_started",
        ],
        "message.cancel" => &[
            "operation_id",
            "message_id",
            "cancellation",
            "cancelled",
            "status",
        ],
        "check.completed" => &[
            "operation_id",
            "check_id",
            "attempt_id",
            "candidate_ref",
            "recipient",
            "state",
            "result_ref",
            "exit_code",
            "resource_released",
            "coverage",
        ],
        kind if kind.starts_with("coordination.") => &[
            "operation_id",
            "thread_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "proposal_id",
            "proposal_revision_id",
            "proposal_digest",
            "decision_operation_id",
            "message_id",
            "state",
            "state_revision",
            "changed",
            "native_execution",
            "model_work_started",
        ],
        kind if kind.starts_with("concilium.") => &[
            "operation_id",
            "concilium_id",
            "task_id",
            "task_revision",
            "attempt_id",
            "round_id",
            "round_revision",
            "slot_id",
            "proposal_id",
            "state",
            "state_revision",
        ],
        _ => return operations::project_operation(db, p, id, grant),
    };
    Ok(crate::redaction::value(select(payload, fields)))
}
