//! Bounded manager-side views over the existing Task, Attempt, Binding,
//! Operation, capacity and attention authorities. This module is read-only:
//! it does not create launch records, call a runtime, or inspect Git.

use super::{acceptance, capacity, meta, projection, tasks::task_sources};
use crate::{
    config::{Config, McpToolProfile},
    error::{Error, Result},
    launcher::{self, PageRequest},
    model::{self, Dependency, Principal, Role, TaskSpec},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value, json};
use std::path::Path;

const DASHBOARD_PAGE_LIMIT: i64 = 5;
const MAX_INSPECT_OPERATIONS: i64 = 20;
const MAX_INSPECT_CHECKS: i64 = 20;
const MAX_INSPECT_OVERLAPS: i64 = 10;
const MAX_SCOPE_PATHS: usize = 32;
const MAX_MATCHED_PATHS: usize = 8;
const MAX_DEPENDENCIES: usize = 64;
const MAX_SCOPE_PATH_BYTES: usize = 512;
const ATTENTION_SCAN_LIMIT: i64 = 200;
const MAX_INSPECT_SNAPSHOT_FIELD_BYTES: usize = 12_288;
const MAX_INSPECT_SNAPSHOT_FIELD_ITEMS: usize = 64;
const MAX_LAUNCH_OPERATION_ROWS: i64 = 8;
const MAX_LAUNCH_BRIEF_BYTES: usize = 8_192;

#[derive(Debug)]
struct TaskRow {
    task_id: String,
    project_id: String,
    revision: i64,
    state: String,
    accepted_attempt_id: Option<String>,
    accepted_operation_id: Option<String>,
    accepted_revision: Option<i64>,
    accepted_phase: Option<String>,
    accepted_candidate_ref: Option<String>,
    spec: Value,
    created_at_ms: i64,
    updated_at_ms: i64,
    current_attempt_id: Option<String>,
}

#[derive(Debug)]
struct AttemptRow {
    attempt_id: String,
    task_id: String,
    task_revision: i64,
    owner_id: String,
    start_owner: String,
    start_operation_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    state: String,
    released_at_ms: Option<i64>,
    snapshot: Value,
    producers: Value,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

struct AttemptDbRow {
    attempt_id: String,
    task_id: String,
    task_revision: i64,
    owner_id: String,
    start_owner: String,
    start_operation_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    state: String,
    released_at_ms: Option<i64>,
    snapshot: String,
    producers: String,
    submission_ref: Option<String>,
    candidate_ref: Option<String>,
    created_at_ms: i64,
    updated_at_ms: i64,
}

struct CurrentTaskRow {
    revision: i64,
    state: String,
    accepted_attempt_id: Option<String>,
    accepted_operation_id: Option<String>,
    current_attempt_id: Option<String>,
}

struct BindingReadRow {
    state: String,
    native_scope_key: Option<String>,
    native_root_id: Option<String>,
    route_json: String,
    state_json: String,
    released_at_ms: Option<i64>,
}

fn require_manager(db: &Connection, p: &Principal) -> Result<()> {
    match p.role {
        Role::Operator => super::require_local_operator(db, &p.client_id),
        Role::Manager => Ok(()),
        _ => Err(Error::new("FORBIDDEN", "manager authority required")),
    }
}

fn require_dashboard_reader(db: &Connection, p: &Principal) -> Result<()> {
    match p.role {
        Role::Operator => super::require_local_operator(db, &p.client_id),
        Role::Manager | Role::Observer => Ok(()),
        _ => Err(Error::new("FORBIDDEN", "dashboard read authority required")),
    }
}

fn count(db: &Connection, sql: &str) -> Result<i64> {
    Ok(db.query_row(sql, [], |row| row.get(0))?)
}

fn grouped_counts(db: &Connection, sql: &str) -> Result<Value> {
    let mut statement = db.prepare(sql)?;
    let rows = statement
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let mut result = Map::new();
    for (name, value) in rows {
        result.insert(name, json!(value));
    }
    Ok(Value::Object(result))
}

fn task_counts(db: &Connection) -> Result<Value> {
    let by_state = grouped_counts(db, "SELECT state,count(*) FROM tasks GROUP BY state")?;
    Ok(json!({"total": count(db,"SELECT count(*) FROM tasks")?, "by_state": by_state}))
}

fn attempt_counts(db: &Connection) -> Result<Value> {
    let current_by_state = grouped_counts(
        db,
        "SELECT state,count(*) FROM attempts WHERE released_at_ms IS NULL GROUP BY state",
    )?;
    Ok(json!({
        "current_total": count(db,"SELECT count(*) FROM attempts WHERE released_at_ms IS NULL")?,
        "current_by_state": current_by_state,
        "released_total": count(db,"SELECT count(*) FROM attempts WHERE released_at_ms IS NOT NULL")?,
    }))
}

fn operation_counts(db: &Connection) -> Result<Value> {
    let by_state = grouped_counts(db, "SELECT state,count(*) FROM operations GROUP BY state")?;
    let unresolved = count(
        db,
        "SELECT count(*) FROM operations WHERE state IN ('queued','sending','native_accepted','outcome_unknown')",
    )?;
    let outcome_unknown = count(
        db,
        "SELECT count(*) FROM operations WHERE state='outcome_unknown'",
    )?;
    Ok(json!({"by_state":by_state,"unresolved":unresolved,"outcome_unknown":outcome_unknown}))
}

fn binding_counts(db: &Connection) -> Result<Value> {
    let by_state = grouped_counts(db, "SELECT state,count(*) FROM bindings GROUP BY state")?;
    let connected = count(
        db,
        "SELECT count(*) FROM bindings WHERE released_at_ms IS NULL AND json_extract(state_json,'$.connection')='connected'",
    )?;
    let live = count(
        db,
        "SELECT count(*) FROM bindings WHERE released_at_ms IS NULL",
    )?;
    Ok(json!({
        "recorded_total": count(db,"SELECT count(*) FROM bindings")?,
        "live_total": live,
        "by_state": by_state,
        "connected_live": connected,
    }))
}

fn new_work_state(db: &Connection) -> Result<Value> {
    let mode = meta(db, "execution_mode")?.unwrap_or(Value::Null);
    let state = mode["new_work"].as_str().unwrap_or("unknown");
    Ok(json!({"new_work":state,"source":"execution_mode"}))
}

fn page_frame(
    source_kind: &str,
    after: i64,
    next_after: i64,
    limit: i64,
    total: i64,
    limited: &projection::Limited,
) -> Result<Value> {
    projection::frame(
        source_kind,
        json!({"after":after,"next_after":next_after}),
        limited,
        limit,
        after > 0,
        next_after < total,
        limited.gap_count == 0,
        Vec::new(),
    )
}

fn page_gap_reference(item: &Value, reason: &'static str, bytes: usize) -> Result<Value> {
    let canonical = model::canonical(item)?;
    let task_id = item["task_id"].as_str();
    let attempt_id = item["attempt_id"]
        .as_str()
        .or_else(|| item["current_attempt"]["attempt_id"].as_str());
    let operation_id = item["operation_id"].as_str();
    let kind = if operation_id.is_some() {
        "operation"
    } else if attempt_id.is_some() {
        "attempt_assignment"
    } else {
        "task"
    };
    let reference = match (task_id, attempt_id, operation_id) {
        (_, _, Some(id)) => json!({"kind":kind,"operation_id":id,"attempt_id":attempt_id}),
        (Some(task), Some(attempt), _) => json!({"kind":kind,"task_id":task,"attempt_id":attempt}),
        (Some(task), _, _) => json!({"kind":kind,"task_id":task}),
        _ => json!({"kind":kind}),
    };
    Ok(json!({
        "task_id": task_id,
        "attempt_id": attempt_id,
        "operation_id": operation_id,
        "gap": {
            "reason": reason,
            "item_serialized_bytes": bytes,
            "max_single_item_bytes": projection::MAX_SINGLE_ITEM_BYTES,
            "item_digest": model::digest(canonical.as_bytes()),
            "detached_reference": reference,
        }
    }))
}

fn snapshot_field_projection(
    value: Option<&Value>,
    attempt_id: &str,
    snapshot_path: &str,
) -> Result<Value> {
    let Some(value) = value else {
        return Ok(json!({
            "status":"unavailable",
            "reason":"attempt_snapshot_field_missing",
            "reference":{"kind":"attempt_snapshot","attempt_id":attempt_id,"path":snapshot_path},
        }));
    };
    let canonical = model::canonical(value)?;
    let item_count = value
        .as_array()
        .map(|items| items.len())
        .or_else(|| value.as_object().map(|object| object.len()))
        .unwrap_or(0);
    if canonical.len() <= MAX_INSPECT_SNAPSHOT_FIELD_BYTES
        && item_count <= MAX_INSPECT_SNAPSHOT_FIELD_ITEMS
    {
        Ok(json!({
            "status":"included",
            "serialized_bytes":canonical.len(),
            "item_count":item_count,
            "value":value,
        }))
    } else {
        Ok(json!({
            "status":"detached",
            "serialized_bytes":canonical.len(),
            "item_count":item_count,
            "digest":model::digest(canonical.as_bytes()),
            "reference":{"kind":"attempt_snapshot","attempt_id":attempt_id,"path":snapshot_path},
        }))
    }
}

fn bounded_page(
    source_kind: &str,
    after: i64,
    limit: i64,
    total: i64,
    items: Vec<Value>,
) -> Result<Value> {
    let limited = projection::limit_items(items, page_gap_reference)?;
    let next_after = after + limited.consumed as i64;
    let frame = page_frame(source_kind, after, next_after, limit, total, &limited)?;
    Ok(json!({
        "items": limited.items,
        "next_after": next_after,
        "total_items": total,
        "pagination": "offset_snapshot_not_inventory_proof",
        "projection": frame,
    }))
}

fn validate_filter_text(value: Option<&str>, field: &str) -> Result<Option<String>> {
    match value {
        None => Ok(None),
        Some(text) if !text.trim().is_empty() && text.len() <= 512 && !text.contains('\0') => {
            Ok(Some(text.to_owned()))
        }
        Some(_) => Err(Error::invalid(format!("{field} must be 1..512 bytes"))),
    }
}

fn optional_i64(params_value: &Value, field: &str, default: i64) -> Result<i64> {
    match params_value.get(field) {
        None => Ok(default),
        Some(value) => value
            .as_i64()
            .ok_or_else(|| Error::invalid(format!("{field} must be an integer"))),
    }
}

fn query_tasks(
    db: &Connection,
    project_id: Option<&str>,
    task_state: Option<&str>,
    after: i64,
    limit: i64,
) -> Result<(i64, Vec<TaskRow>)> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM tasks WHERE (?1 IS NULL OR project_id=?1) AND (?2 IS NULL OR state=?2)",
        params![project_id, task_state],
        |row| row.get(0),
    )?;
    let after = after.min(total);
    let sql = "SELECT t.task_id,t.project_id,t.revision,t.state,t.accepted_attempt_id,
                      t.accepted_operation_id,t.accepted_revision,t.accepted_phase,
                      t.accepted_candidate_ref,t.spec_json,t.created_at_ms,t.updated_at_ms,
                      (SELECT a.attempt_id FROM attempts a WHERE a.task_id=t.task_id
                       AND a.released_at_ms IS NULL ORDER BY a.created_at_ms DESC,a.attempt_id DESC LIMIT 1)
               FROM tasks t WHERE (?1 IS NULL OR t.project_id=?1) AND (?2 IS NULL OR t.state=?2)
               ORDER BY t.created_at_ms,t.task_id LIMIT ?3 OFFSET ?4";
    let mut statement = db.prepare(sql)?;
    let rows = statement
        .query_map(params![project_id, task_state, limit, after], |row| {
            let spec: String = row.get(9)?;
            Ok(TaskRow {
                task_id: row.get(0)?,
                project_id: row.get(1)?,
                revision: row.get(2)?,
                state: row.get(3)?,
                accepted_attempt_id: row.get(4)?,
                accepted_operation_id: row.get(5)?,
                accepted_revision: row.get(6)?,
                accepted_phase: row.get(7)?,
                accepted_candidate_ref: row.get(8)?,
                spec: serde_json::from_str(&spec).map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        9,
                        rusqlite::types::Type::Text,
                        Box::new(error),
                    )
                })?,
                created_at_ms: row.get(10)?,
                updated_at_ms: row.get(11)?,
                current_attempt_id: row.get(12)?,
            })
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok((total, rows))
}

fn query_task(db: &Connection, task_id: &str) -> Result<TaskRow> {
    let sql = "SELECT t.task_id,t.project_id,t.revision,t.state,t.accepted_attempt_id,
                      t.accepted_operation_id,t.accepted_revision,t.accepted_phase,
                      t.accepted_candidate_ref,t.spec_json,t.created_at_ms,t.updated_at_ms,
                      (SELECT a.attempt_id FROM attempts a WHERE a.task_id=t.task_id
                       AND a.released_at_ms IS NULL ORDER BY a.created_at_ms DESC,a.attempt_id DESC LIMIT 1)
               FROM tasks t WHERE t.task_id=?1";
    db.query_row(sql, [task_id], |row| {
        let spec: String = row.get(9)?;
        Ok(TaskRow {
            task_id: row.get(0)?,
            project_id: row.get(1)?,
            revision: row.get(2)?,
            state: row.get(3)?,
            accepted_attempt_id: row.get(4)?,
            accepted_operation_id: row.get(5)?,
            accepted_revision: row.get(6)?,
            accepted_phase: row.get(7)?,
            accepted_candidate_ref: row.get(8)?,
            spec: serde_json::from_str(&spec).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    9,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
            created_at_ms: row.get(10)?,
            updated_at_ms: row.get(11)?,
            current_attempt_id: row.get(12)?,
        })
    })
    .optional()?
    .ok_or_else(|| Error::new("NOT_FOUND", format!("Task {task_id}")))
}

fn get_attempt_row(db: &Connection, attempt_id: &str) -> Result<Option<AttemptRow>> {
    let raw: Option<AttemptDbRow> = db
        .query_row(
            "SELECT attempt_id,task_id,task_revision,owner_id,start_owner,start_operation_id,
                    binding_id,binding_generation,state,released_at_ms,task_snapshot_json,
                    producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms
             FROM attempts WHERE attempt_id=?1",
            [attempt_id],
            |row| {
                Ok(AttemptDbRow {
                    attempt_id: row.get(0)?,
                    task_id: row.get(1)?,
                    task_revision: row.get(2)?,
                    owner_id: row.get(3)?,
                    start_owner: row.get(4)?,
                    start_operation_id: row.get(5)?,
                    binding_id: row.get(6)?,
                    binding_generation: row.get(7)?,
                    state: row.get(8)?,
                    released_at_ms: row.get(9)?,
                    snapshot: row.get(10)?,
                    producers: row.get(11)?,
                    submission_ref: row.get(12)?,
                    candidate_ref: row.get(13)?,
                    created_at_ms: row.get(14)?,
                    updated_at_ms: row.get(15)?,
                })
            },
        )
        .optional()?;
    raw.map(|raw| {
        Ok(AttemptRow {
            attempt_id: raw.attempt_id,
            task_id: raw.task_id,
            task_revision: raw.task_revision,
            owner_id: raw.owner_id,
            start_owner: raw.start_owner,
            start_operation_id: raw.start_operation_id,
            binding_id: raw.binding_id,
            binding_generation: raw.binding_generation,
            state: raw.state,
            released_at_ms: raw.released_at_ms,
            snapshot: serde_json::from_str(&raw.snapshot)?,
            producers: serde_json::from_str(&raw.producers)?,
            submission_ref: raw.submission_ref,
            candidate_ref: raw.candidate_ref,
            created_at_ms: raw.created_at_ms,
            updated_at_ms: raw.updated_at_ms,
        })
    })
    .transpose()
}

fn current_task(db: &Connection, task_id: &str) -> Result<Option<CurrentTaskRow>> {
    Ok(db
        .query_row(
            "SELECT revision,state,accepted_attempt_id,accepted_operation_id,
                    (SELECT a.attempt_id FROM attempts a WHERE a.task_id=tasks.task_id
                     AND a.released_at_ms IS NULL
                     ORDER BY a.created_at_ms DESC,a.attempt_id DESC LIMIT 1)
             FROM tasks WHERE task_id=?1",
            [task_id],
            |row| {
                Ok(CurrentTaskRow {
                    revision: row.get(0)?,
                    state: row.get(1)?,
                    accepted_attempt_id: row.get(2)?,
                    accepted_operation_id: row.get(3)?,
                    current_attempt_id: row.get(4)?,
                })
            },
        )
        .optional()?)
}

fn owner_profile(db: &Connection, owner_id: &str) -> Result<Value> {
    let Some(profile) = meta(db, &format!("client:{owner_id}"))? else {
        return Ok(json!({"client_id":owner_id,"status":"not_registered"}));
    };
    let role = profile["role"].as_str().unwrap_or("unknown");
    let status = if profile["disabled"] == true {
        "disabled"
    } else if matches!(role, "operator" | "manager") {
        "registered_work_owner"
    } else {
        "registered_role_not_authorized_for_work"
    };
    Ok(json!({"client_id":owner_id,"role":role,"status":status}))
}

fn path_is_absolute(path: &str) -> bool {
    let bytes = path.as_bytes();
    Path::new(path).is_absolute()
        || path.starts_with("\\\\")
        || path.starts_with("//")
        || (bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/'))
}

fn safe_scope_path(path: &str) -> Value {
    if path_is_absolute(path) || path.len() > MAX_SCOPE_PATH_BYTES {
        json!({
            "status":"redacted",
            "path_digest":model::digest(path.as_bytes()),
            "serialized_bytes":path.len(),
        })
    } else {
        json!({"status":"included","path":path})
    }
}

fn scope_projection(spec: Option<&TaskSpec>) -> Value {
    let Some(scope) = spec.and_then(|spec| spec.scope.as_ref()) else {
        return json!({"status":"not_recorded","initial_paths":[],"forbidden_paths":[]});
    };
    let initial_paths: Vec<Value> = scope
        .initial_paths
        .iter()
        .take(MAX_SCOPE_PATHS)
        .map(|path| safe_scope_path(path))
        .collect();
    let forbidden_paths: Vec<Value> = scope
        .forbidden_paths
        .iter()
        .take(MAX_SCOPE_PATHS)
        .map(|path| safe_scope_path(path))
        .collect();
    let total_paths = scope.initial_paths.len() + scope.forbidden_paths.len();
    let returned_paths = initial_paths.len() + forbidden_paths.len();
    let redacted = initial_paths
        .iter()
        .chain(forbidden_paths.iter())
        .filter(|path| path["status"] == "redacted")
        .count();
    json!({
        "status": if returned_paths < total_paths || redacted > 0 {"partial"} else {"recorded"},
        "initial_paths": initial_paths,
        "initial_path_count": scope.initial_paths.len(),
        "forbidden_paths": forbidden_paths,
        "forbidden_path_count": scope.forbidden_paths.len(),
        "prerequisite_policy": &scope.prerequisite_policy,
        "redacted_path_count": redacted,
        "omitted_path_count": total_paths.saturating_sub(returned_paths),
        "coverage_complete": returned_paths == total_paths && redacted == 0,
    })
}

fn source_brief(row: &TaskRow) -> (Value, Value, Option<TaskSpec>) {
    let brief = task_sources::project_brief(&row.spec);
    let spec = serde_json::from_value::<TaskSpec>(row.spec.clone()).ok();
    (
        brief,
        json!({"kind":"task_revision","task_id":row.task_id,"task_revision":row.revision,"method":"task.get"}),
        spec,
    )
}

fn dependency_projection(db: &Connection, spec: Option<&TaskSpec>) -> Result<Value> {
    let Some(spec) = spec else {
        return Ok(json!({"status":"unknown","accepted":[],"waiting":[]}));
    };
    let mut accepted = Vec::new();
    let mut waiting = Vec::new();
    let selected: Vec<&Dependency> = spec.dependencies.iter().take(MAX_DEPENDENCIES).collect();
    let omitted_count = spec.dependencies.len().saturating_sub(selected.len());
    for dependency in selected {
        match acceptance::resolve_dependency(db, dependency) {
            Ok(acceptance_operation_id) => accepted.push(json!({
                "task_id":dependency.task_id,
                "required_revision":dependency.required_revision,
                "required_phase":dependency.required_phase,
                "acceptance_operation_id":acceptance_operation_id,
            })),
            Err(error) if error.code == "DEPENDENCY_NOT_READY" => waiting.push(json!({
                "task_id":dependency.task_id,
                "required_revision":dependency.required_revision,
                "required_phase":dependency.required_phase,
                "status":"not_accepted_or_acceptance_invalidated",
            })),
            Err(error) => return Err(error),
        }
    }
    Ok(json!({
        "status": if !waiting.is_empty() {"waiting"} else if omitted_count > 0 {"unknown"} else {"satisfied"},
        "accepted": accepted,
        "waiting": waiting,
        "omitted_count":omitted_count,
        "gaps":if omitted_count > 0 {vec!["dependency_projection_limit"]} else {Vec::<&str>::new()},
    }))
}

fn readiness(
    db: &Connection,
    task: &TaskRow,
    attempt: Option<&AttemptRow>,
    spec: Option<&TaskSpec>,
    spec_valid: bool,
    dependencies: &Value,
) -> Result<Value> {
    let new_work = meta(db, "execution_mode")?.unwrap_or(Value::Null)["new_work"]
        .as_str()
        .unwrap_or("unknown")
        .to_owned();
    let mut blockers = Vec::new();
    let mut gaps = Vec::new();
    if !spec_valid {
        gaps.push("stored_task_spec_invalid_or_unreadable");
    }
    if task.state != "open" {
        blockers.push("task_not_open");
    }
    if let Some(attempt) = attempt {
        blockers.push("current_attempt_owns_task");
        if attempt.released_at_ms.is_some() {
            gaps.push("attempt_release_state_conflicts_with_current_index");
        }
    }
    if new_work == "disabled" {
        blockers.push("new_work_disabled");
    } else if new_work != "enabled" {
        gaps.push("new_work_admission_unknown");
    }
    if dependencies["status"] == "waiting" {
        blockers.push("dependency_acceptance_missing_or_invalidated");
    } else if dependencies["status"] == "unknown" {
        gaps.push("dependency_state_unknown");
    }
    let owner_policy = spec
        .map(|spec| policy_state(spec.owner_policy_id.as_deref()))
        .transpose()?
        .unwrap_or_else(|| json!({"status":"unknown"}));
    if owner_policy["status"] == "unavailable" {
        blockers.push("owner_policy_not_accepted");
    } else if owner_policy["status"] != "accepted" {
        gaps.push("owner_policy_state_unknown");
    }
    let state = if !blockers.is_empty() {
        "blocked"
    } else {
        // No route candidate or full-scope live capacity qualification is
        // recorded for an unassigned Task, so satisfying Task claim
        // preconditions alone cannot produce a `ready` queue row.
        "unknown"
    };
    if attempt.is_none() {
        gaps.push("eligible_owner_candidates_not_recorded");
        gaps.push("route_and_full_scope_binding_capacity_not_selected");
    }
    Ok(json!({
        "state":state,
        "basis":"derived from current admission, ownership policy, dependency, and Attempt facts; no launch readiness is implied",
        "task_state":task.state,
        "current_attempt_id":task.current_attempt_id,
        "owner_policy":owner_policy,
        "claim_preconditions":if blockers.is_empty() && spec_valid && dependencies["status"] == "satisfied" && new_work == "enabled" {"satisfied"} else {"not_satisfied_or_unknown"},
        "dependencies":dependencies,
        "new_work_admission":new_work,
        "resource_capacity":"unknown_until_exact_binding_and_scope_are_selected",
        "blockers":blockers,
        "gaps":gaps,
        "route_candidates":[],
        "route_candidate_status":"not_recorded; configured routes are not qualification or runtime capacity",
    }))
}

fn policy_state(policy_id: Option<&str>) -> Result<Value> {
    match crate::policy::accepted_edition(policy_id) {
        Ok(edition) => Ok(json!({"status":"accepted","edition":edition})),
        Err(error)
            if matches!(
                error.code.as_str(),
                "OWNER_POLICY_REQUIRED" | "OWNER_POLICY_UNKNOWN"
            ) =>
        {
            Ok(json!({"status":"unavailable","reason":error.code}))
        }
        Err(error) => Err(error),
    }
}

fn task_queue_item(db: &Connection, row: &TaskRow, brief_limit: usize) -> Result<Value> {
    let attempt = row
        .current_attempt_id
        .as_deref()
        .map(|id| get_attempt_row(db, id))
        .transpose()?
        .flatten();
    // Queue readiness and the row's brief are current Task-revision facts.
    // The exact frozen Attempt brief is reserved for `agent.inspect`.
    let (brief, brief_reference, spec) = source_brief(row);
    let spec_valid = spec.as_ref().is_some_and(|spec| spec.validate().is_ok());
    let dependency_status = dependency_projection(db, spec.as_ref())?;
    let ready = readiness(
        db,
        row,
        attempt.as_ref(),
        spec.as_ref(),
        spec_valid,
        &dependency_status,
    )?;
    let brief = launcher::brief_projection(&brief, brief_reference, brief_limit)?;
    let attempt_capacity = attempt
        .as_ref()
        .map(|attempt| exact_attempt_capacity(db, attempt))
        .transpose()?
        .unwrap_or_else(|| {
            json!({
                "status":"unknown",
                "reason":"no_exact_attempt_binding_or_owner_route",
                "capacity_available":null,
            })
        });
    let task_revision_current = attempt
        .as_ref()
        .is_none_or(|attempt| attempt.task_revision == row.revision);
    let attempt_value = if let Some(attempt) = attempt.as_ref() {
        json!({
            "attempt_id":attempt.attempt_id,
            "task_revision":attempt.task_revision,
            "state":attempt.state,
            "owner":owner_profile(db,&attempt.owner_id)?,
            "start_owner":attempt.start_owner,
            "start_operation_id":attempt.start_operation_id,
            "binding": match (&attempt.binding_id,attempt.binding_generation) {
                (Some(id),Some(generation)) => binding_summary(db,id,generation)?,
                _ => Value::Null,
            },
            "task_revision_current":attempt.task_revision == row.revision,
            "submission_ref":attempt.submission_ref,
            "candidate_ref":attempt.candidate_ref,
            "updated_at_ms":attempt.updated_at_ms,
        })
    } else {
        Value::Null
    };
    let work_scope = scope_projection(spec.as_ref());
    let queue_state = if row.state == "accepted" {
        "accepted"
    } else if row.state == "archived" {
        "archived"
    } else if let Some(attempt) = attempt.as_ref() {
        match attempt.state.as_str() {
            "submitted" => "review",
            "reserved" => "claimed",
            "running" | "needs_correction" => "running",
            "recovery_pending" => "blocked",
            _ => "blocked",
        }
    } else {
        ready["state"].as_str().unwrap_or("unknown")
    };
    let mut gaps = Vec::new();
    if !spec_valid {
        gaps.push("task_spec_unavailable_or_invalid");
    }
    if dependency_status["status"] != "satisfied" {
        gaps.push("dependency_projection_not_complete_or_not_satisfied");
    }
    if work_scope["status"] != "recorded" {
        gaps.push("work_scope_not_fully_recorded_inline");
    }
    if attempt.is_none() {
        gaps.push("route_specific_runtime_capacity_not_assessed");
    } else if !task_revision_current {
        gaps.push("current_attempt_snapshot_is_for_an_older_task_revision");
    }
    if brief["status"] != "included" {
        gaps.push("source_brief_not_inline");
    }
    Ok(json!({
        "task_id":row.task_id,
        "project_id":row.project_id,
        "task_revision":row.revision,
        "task_state":row.state,
        "state":queue_state,
        "created_at_ms":row.created_at_ms,
        "updated_at_ms":row.updated_at_ms,
        "accepted_attempt_id":row.accepted_attempt_id,
        "accepted_operation_id":row.accepted_operation_id,
        "accepted_revision":row.accepted_revision,
        "accepted_phase":row.accepted_phase,
        "accepted_candidate_ref":row.accepted_candidate_ref,
        "current_attempt":attempt_value,
        "readiness":ready,
        "capacity":attempt_capacity,
        "task_brief":brief,
        "work_scope":work_scope,
        "ordering":{"created_at_ms":row.created_at_ms,"tie_breaker":"task_id"},
        "priority":{"status":"not_recorded"},
        "coverage":if gaps.is_empty() {"complete"} else {"partial"},
        "gaps":gaps,
    }))
}

fn binding_summary(db: &Connection, binding_id: &str, generation: i64) -> Result<Value> {
    let raw: Option<BindingReadRow> = db
        .query_row(
            "SELECT state,native_scope_key,native_root_id,route_json,state_json,released_at_ms
             FROM bindings WHERE binding_id=?1 AND generation=?2",
            params![binding_id, generation],
            |row| {
                Ok(BindingReadRow {
                    state: row.get(0)?,
                    native_scope_key: row.get(1)?,
                    native_root_id: row.get(2)?,
                    route_json: row.get(3)?,
                    state_json: row.get(4)?,
                    released_at_ms: row.get(5)?,
                })
            },
        )
        .optional()?;
    let Some(raw) = raw else {
        return Ok(json!({
            "binding_id":binding_id,
            "generation":generation,
            "status":"missing_recorded_binding",
        }));
    };
    let route: Value = serde_json::from_str(&raw.route_json)?;
    let observation: Value = serde_json::from_str(&raw.state_json)?;
    Ok(json!({
        "binding_id":binding_id,
        "generation":generation,
        "state":raw.state,
        "released_at_ms":raw.released_at_ms,
        "native_scope_key":raw.native_scope_key,
        "native_root_id":raw.native_root_id,
        "route":{
            "alias":route["alias"],
            "runtime":route["runtime"],
            "recorded":true,
            "live_qualified":false,
        },
        "observation":{
            "connection":observation["connection"],
            "observed_at_ms":observation["observed_at_ms"],
            "native_observation_id":observation["native_observation_id"],
            "recovery_required":observation["recovery_required"],
            "waiting_for":observation["waiting_for"],
            "execution":observation["execution"].as_str(),
        },
        "gaps":["route_live_qualification_not_recorded","runtime_capability_receipt_not_recorded"],
    }))
}

fn exact_attempt_capacity(db: &Connection, attempt: &AttemptRow) -> Result<Value> {
    let (Some(binding_id), Some(generation)) =
        (attempt.binding_id.as_deref(), attempt.binding_generation)
    else {
        return Ok(json!({
            "status":"unknown",
            "reason":"attempt_has_no_exact_binding",
            "capacity_available":null,
        }));
    };
    let raw_binding: Option<(String, Option<String>)> = db
        .query_row(
            "SELECT route_json,native_scope_key FROM bindings
             WHERE binding_id=?1 AND generation=?2",
            params![binding_id, generation],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((route_json, native_scope_key)) = raw_binding else {
        return Ok(json!({
            "status":"unknown",
            "reason":"attempt_binding_record_missing",
            "capacity_available":null,
        }));
    };
    let route: Value = serde_json::from_str(&route_json)?;
    let scope = capacity::scope_facts(&route, native_scope_key.as_deref(), binding_id);
    let scope_key = scope["scope_key"].as_str().unwrap_or_default();
    let ledger = meta(db, &format!("capacity:{scope_key}"))?.unwrap_or(Value::Null);
    let entries: Vec<&Value> = ledger["entries"]
        .as_object()
        .into_iter()
        .flat_map(|entries| entries.values())
        .filter(|entry| entry["attempt_id"] == attempt.attempt_id)
        .collect();
    let reserved = entries
        .iter()
        .filter(|entry| entry["phase"] == "reserved")
        .count();
    let active = entries
        .iter()
        .filter(|entry| entry["phase"] == "active")
        .count();
    let unknown = entries
        .iter()
        .filter(|entry| !entry["outcome_unknown_since_ms"].is_null())
        .count();
    Ok(json!({
        "status":"recorded_attempt_entries",
        "scope":{
            "scope_key":scope["scope_key"],
            "runtime":scope["runtime"],
            "provider":scope["provider"],
            "service":scope["service"],
            "identity":scope["identity"],
        },
        "attempt_entries":{"reserved":reserved,"active":active,"outcome_unknown":unknown,"count":entries.len()},
        "ledger_updated_at_ms":ledger["updated_at_ms"],
        "scope_capacity_available":null,
        "capacity_reason":"single-binding projection does not revalidate the full scope roster",
    }))
}

fn authorize_launch_project(
    db: &Connection,
    p: &Principal,
    attempt: Option<&AttemptRow>,
) -> Result<()> {
    require_manager(db, p)?;
    if p.role == Role::Manager
        && !attempt.is_some_and(|attempt| {
            attempt.owner_id == p.client_id && attempt.released_at_ms.is_none()
        })
    {
        // An unassigned Task has no project-specific manager ACL in the
        // retained schema. Only the current GM can preview that scope.
        super::gm::require_authority(db, p)?;
    }
    Ok(())
}

fn launch_operation_projection(db: &Connection, task_id: &str) -> Result<Value> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE task_id=?1 AND method='swarm.launch'",
        [task_id],
        |row| row.get(0),
    )?;
    let unresolved: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE task_id=?1 AND method='swarm.launch'
         AND state IN ('queued','sending','native_accepted','outcome_unknown')",
        [task_id],
        |row| row.get(0),
    )?;
    let unknown: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE task_id=?1 AND method='swarm.launch'
         AND state='outcome_unknown'",
        [task_id],
        |row| row.get(0),
    )?;
    let mut statement = db.prepare(
        "SELECT operation_id,state,attempt_id,created_at_ms,updated_at_ms FROM operations
         WHERE task_id=?1 AND method='swarm.launch'
         ORDER BY created_at_ms DESC,operation_id DESC LIMIT ?2",
    )?;
    let items = statement
        .query_map(params![task_id, MAX_LAUNCH_OPERATION_ROWS], |row| {
            let operation_id: String = row.get(0)?;
            let state: String = row.get(1)?;
            let attempt_id: Option<String> = row.get(2)?;
            let created_at_ms: i64 = row.get(3)?;
            let updated_at_ms: i64 = row.get(4)?;
            Ok(json!({
                "operation_id":operation_id,
                "state":state,
                "attempt_id":attempt_id,
                "created_at_ms":created_at_ms,
                "updated_at_ms":updated_at_ms,
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(json!({
        "items":items,
        "total_items":total,
        "unresolved_count":unresolved,
        "outcome_unknown_count":unknown,
        "coverage":if total > MAX_LAUNCH_OPERATION_ROWS {"partial"} else {"complete"},
        "next_after":if total > MAX_LAUNCH_OPERATION_ROWS {
            json!(MAX_LAUNCH_OPERATION_ROWS)
        } else {
            Value::Null
        },
    }))
}

struct ConfiguredRouteModel<'a> {
    provider_id: Option<&'a str>,
    model_id: Option<&'a str>,
    variant: Option<&'a str>,
}

fn configured_route_model(route: &crate::config::Route) -> ConfiguredRouteModel<'_> {
    let model = &route.native_options["model"];
    let model_id = model
        .get("id")
        .or_else(|| model.get("modelID"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256);
    let provider_id = model
        .get("providerID")
        .or_else(|| model.get("provider_id"))
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256);
    let variant = model
        .get("variant")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty() && value.len() <= 256);
    ConfiguredRouteModel {
        provider_id,
        model_id,
        variant,
    }
}

fn safe_route_runtime(runtime: &str) -> &'static str {
    if runtime == crate::runtime::codex::RUNTIME {
        "codex"
    } else if runtime == crate::runtime::opencode_v2::RUNTIME {
        "opencode_v2"
    } else {
        "unknown"
    }
}

fn launch_route_projection(
    config: &Config,
    request: &launcher::LaunchPreviewRequest,
    hard_blocks: &mut Vec<&'static str>,
    gaps: &mut Vec<&'static str>,
) -> Value {
    let route = config
        .routes
        .iter()
        .find(|route| route.alias == request.route);
    let Some(route) = route else {
        hard_blocks.push("route_not_configured");
        gaps.push("route_live_qualification_and_provider_capacity_not_observed");
        return json!({
            "alias":request.route,
            "configured":false,
            "enabled":false,
            "live_qualified":false,
            "requested_model":request.requested_model,
            "requested_effort":request.requested_effort,
            "agent_profile":{"requested":request.agent_profile,"validation":"unknown"},
            "capability_gaps":["route_not_configured","provider_model_and_agent_catalog_not_retained"],
        });
    };
    if !route.enabled {
        hard_blocks.push("route_disabled");
    }
    let configured_model = configured_route_model(route);
    let model_status = match request.requested_model.as_deref() {
        None => "not_requested",
        Some(requested) if configured_model.model_id == Some(requested) => {
            "matches_configured_route_model_id"
        }
        Some(_) => "unknown_without_retained_provider_model_catalog",
    };
    let effort_status = match request.requested_effort.as_deref() {
        None => "not_requested",
        Some(requested) if configured_model.variant == Some(requested) => {
            "matches_configured_route_variant"
        }
        Some(_) => "unknown_without_retained_provider_model_catalog",
    };
    if model_status == "unknown_without_retained_provider_model_catalog"
        || effort_status == "unknown_without_retained_provider_model_catalog"
    {
        gaps.push("requested_provider_model_or_variant_not_validated_against_a_retained_catalog");
    }
    gaps.push("route_configuration_does_not_prove_live_runtime_qualification_or_capacity");
    gaps.push("native_agent_profile_catalog_not_retained_for_preview");
    json!({
        "alias":route.alias,
        "configured":true,
        "enabled":route.enabled,
        "runtime_kind":safe_route_runtime(&route.runtime),
        "live_qualified":false,
        "configured_model":{
            "status":if configured_model.provider_id.is_some()
                && configured_model.model_id.is_some()
                && configured_model.variant.is_some() {
                "recorded_in_route_configuration"
            } else {
                "not_recorded_in_route_configuration"
            },
            "provider_id_recorded":configured_model.provider_id.is_some(),
            "model_id_recorded":configured_model.model_id.is_some(),
            "variant_recorded":configured_model.variant.is_some(),
        },
        "requested_model":{
            "value":request.requested_model,
            "validation":model_status,
        },
        "requested_effort":{
            "value":request.requested_effort,
            "validation":effort_status,
        },
        "agent_profile":{
            "requested":request.agent_profile,
            "validation":"unknown_without_retained_native_agent_catalog",
        },
        "capability_gaps":[
            "live_route_qualification_not_observed",
            "provider_model_catalog_not_retained",
            "native_agent_profile_catalog_not_retained",
            "provider_runtime_capacity_not_selected_for_a_new_binding",
        ],
    })
}

fn launch_mcp_profile_projection(
    db: &Connection,
    config: &Config,
    request: &launcher::LaunchPreviewRequest,
    hard_blocks: &mut Vec<&'static str>,
) -> Result<Value> {
    let Some(profile) = config.mcp.profiles.get(&request.mcp_profile) else {
        hard_blocks.push("mcp_profile_not_configured");
        return Ok(json!({
            "profile_name":request.mcp_profile,
            "status":"not_configured",
            "surface":request.mcp_surface,
        }));
    };
    let role = profile.tool_profile;
    let narrow_role = matches!(
        role,
        McpToolProfile::Participant | McpToolProfile::AssignedReviewer
    );
    if !narrow_role {
        hard_blocks.push("mcp_profile_is_not_a_narrow_assignment_role");
    }
    if role == McpToolProfile::AssignedReviewer && request.purpose != "review" {
        hard_blocks.push("assigned_reviewer_profile_requires_review_purpose");
    }
    if role == McpToolProfile::Participant && request.purpose == "review" {
        hard_blocks.push("review_purpose_requires_assigned_reviewer_profile");
    }

    let identity = meta(db, &format!("client:{}", profile.expected_client_id))?;
    let identity_state = match identity.as_ref() {
        None => "not_registered",
        Some(client) if client["disabled"] == true => "disabled",
        Some(client) if client["role"] != "participant" => "role_mismatch",
        Some(_) => "registered_enabled_participant",
    };
    if identity_state != "registered_enabled_participant" {
        hard_blocks.push("configured_mcp_identity_is_not_an_enabled_participant");
    }

    if profile
        .surface
        .as_deref()
        .is_some_and(|configured| configured != request.mcp_surface)
    {
        hard_blocks.push("requested_mcp_surface_differs_from_configured_profile_surface");
    }
    let surface = match crate::mcp::launch_profile_surface(
        role,
        &request.mcp_surface,
        &profile.deferred_groups,
        &profile.manual_tools,
    ) {
        Ok(surface) => surface,
        Err(error) if error.code == "INVALID_PARAMS" => {
            hard_blocks.push("mcp_surface_not_authorized_by_static_catalog");
            json!({
                "status":"invalid_for_profile",
                "profile_name":request.mcp_profile,
                "hard_profile":role,
                "surface":request.mcp_surface,
            })
        }
        Err(error) => return Err(error),
    };
    Ok(json!({
        "status":if surface["surface_id"].is_string() {"validated_against_static_catalog"} else {"invalid_for_profile"},
        "profile_name":request.mcp_profile,
        "hard_profile":role,
        "identity":{"status":identity_state,"role":"participant"},
        "surface":request.mcp_surface,
        "surface_facts":surface,
        "runtime_loaded":"unknown",
        "gaps":["mcp_profile_configuration_does_not_prove_native_runtime_tool_loading"],
    }))
}

fn launch_workspace_projection(request: &launcher::LaunchPreviewRequest) -> Value {
    let supported = request.workspace_policy == "manager_owned_worktree";
    json!({
        "policy":request.workspace_policy,
        "status":if supported {"not_provisioned"} else {"unsupported_policy"},
        "lease":"unknown",
        "dirty":Value::Null,
        "filesystem_inspected":false,
        "repository_handle":Value::Null,
        "worktree_handle":Value::Null,
        "branch":Value::Null,
        "baseline_commit":Value::Null,
    })
}

/// Read-only bounded plan over one exact Task revision and the Store's
/// current Attempt, dependency, policy, capacity, Operation, route, and MCP
/// profile facts. No filesystem or native runtime is consulted here.
pub(super) fn launch_preview(
    db: &Connection,
    p: &Principal,
    params_value: &Value,
    config: &Config,
) -> Result<Value> {
    let request = launcher::LaunchPreviewRequest::parse(params_value)?;
    require_manager(db, p)?;

    let row = query_task(db, &request.task_id)?;
    let attempt = row
        .current_attempt_id
        .as_deref()
        .map(|attempt_id| get_attempt_row(db, attempt_id))
        .transpose()?
        .flatten();
    authorize_launch_project(db, p, attempt.as_ref())?;

    let exact_current_attempt = attempt.as_ref().filter(|attempt| {
        attempt.task_id == row.task_id
            && attempt.released_at_ms.is_none()
            && row.current_attempt_id.as_deref() == Some(attempt.attempt_id.as_str())
    });
    let task_revision_current = request.expected_task_revision == row.revision;
    let (task_brief, task_brief_reference, spec) = source_brief(&row);
    let task_brief =
        launcher::brief_projection(&task_brief, task_brief_reference, MAX_LAUNCH_BRIEF_BYTES)?;
    let spec_valid = spec.as_ref().is_some_and(|spec| spec.validate().is_ok());
    let dependencies = dependency_projection(db, spec.as_ref())?;
    let claim_readiness = readiness(
        db,
        &row,
        exact_current_attempt,
        spec.as_ref(),
        spec_valid,
        &dependencies,
    )?;
    let work_scope = scope_projection(spec.as_ref());

    let mut hard_blocks = Vec::new();
    let mut gaps = vec![
        "workspace_lease_dirty_state_and_git_baseline_not_observed",
        "launch_manifest_and_launch_mutation_are_not_implemented",
        "provider_runtime_and_native_agent_capability_receipts_not_observed",
    ];
    if !task_revision_current {
        hard_blocks.push("task_revision_changed");
    }
    if row.state != "open" {
        hard_blocks.push("task_not_open");
    }
    if !spec_valid {
        hard_blocks.push("task_spec_unavailable_or_invalid");
    }
    if claim_readiness["owner_policy"]["status"] != "accepted" {
        hard_blocks.push("owner_policy_not_recognized");
    }
    if exact_current_attempt.is_none() {
        if claim_readiness["new_work_admission"] == "disabled" {
            hard_blocks.push("new_work_admission_disabled");
        } else if claim_readiness["new_work_admission"] != "enabled" {
            hard_blocks.push("new_work_admission_state_unknown");
            gaps.push("new_work_admission_state_unknown");
        }
    }
    match dependencies["status"].as_str() {
        Some("waiting") => hard_blocks.push("required_dependency_not_accepted"),
        Some("unknown") => hard_blocks.push("dependency_state_not_complete"),
        Some("satisfied") => {}
        _ => hard_blocks.push("dependency_state_unavailable"),
    }
    if task_brief["status"] != "included" {
        gaps.push("exact_current_task_brief_not_inline");
    }
    if work_scope["status"] == "not_recorded" {
        gaps.push("task_work_scope_not_recorded");
    } else if work_scope["status"] != "recorded" {
        gaps.push("some_scope_paths_are_redacted_or_omitted");
    }
    if request.workspace_policy != "manager_owned_worktree" {
        hard_blocks.push("unsupported_workspace_policy");
    } else {
        gaps.push("manager_owned_worktree_not_provisioned_by_preview");
    }

    let route = launch_route_projection(config, &request, &mut hard_blocks, &mut gaps);
    let mcp_profile = launch_mcp_profile_projection(db, config, &request, &mut hard_blocks)?;
    let launch_operations = launch_operation_projection(db, &row.task_id)?;
    if launch_operations["unresolved_count"].as_i64().unwrap_or(0) > 0 {
        hard_blocks.push("prior_launch_operation_unresolved");
    }
    if launch_operations["coverage"] != "complete" {
        gaps.push("prior_launch_operation_page_is_partial");
    }

    let own_initial_paths = spec
        .as_ref()
        .and_then(|spec| spec.scope.as_ref())
        .map(|scope| scope.initial_paths.as_slice())
        .unwrap_or_default();
    let overlaps = current_scope_overlaps(
        db,
        exact_current_attempt.map(|attempt| attempt.attempt_id.as_str()),
        &row.project_id,
        own_initial_paths,
        PageRequest {
            after: 0,
            limit: MAX_INSPECT_OVERLAPS,
        },
    )?;
    if overlaps["coverage"] != "complete" {
        gaps.push("literal_path_overlap_coverage_is_partial_or_unknown");
    }

    let (attempt_action, attempt_block) = match exact_current_attempt {
        Some(attempt) => {
            if attempt.task_revision != row.revision {
                ("forbidden", Some("current_attempt_revision_is_stale"))
            } else if p.role == Role::Manager && attempt.owner_id != p.client_id {
                (
                    "forbidden",
                    Some("current_attempt_is_owned_by_another_manager"),
                )
            } else if !matches!(
                attempt.state.as_str(),
                "reserved" | "running" | "needs_correction"
            ) {
                ("forbidden", Some("current_attempt_state_is_not_reusable"))
            } else {
                ("use_existing", None)
            }
        }
        None if row.current_attempt_id.is_some() => {
            ("forbidden", Some("current_attempt_record_missing"))
        }
        None if hard_blocks.is_empty() => ("claim_new", None),
        None => ("forbidden", None),
    };
    if let Some(block) = attempt_block {
        hard_blocks.push(block);
    }

    let attempt_binding = if let Some(attempt) = exact_current_attempt {
        match (&attempt.binding_id, attempt.binding_generation) {
            (Some(binding_id), Some(generation)) => {
                let detail = binding_summary(db, binding_id, generation)?;
                let route_alias = detail["route"]["alias"].as_str();
                if route_alias.is_some_and(|alias| alias != request.route) {
                    hard_blocks.push("requested_route_differs_from_current_attempt_binding");
                }
                json!({
                    "binding_id":binding_id,
                    "generation":generation,
                    "state":detail["state"],
                    "route_alias":route_alias,
                    "runtime_kind":detail["route"]["runtime"]
                        .as_str()
                        .map(safe_route_runtime)
                        .unwrap_or("unknown"),
                    "connection":detail["observation"]["connection"],
                    "live_qualified":false,
                })
            }
            _ => json!({"status":"no_exact_binding_recorded"}),
        }
    } else {
        Value::Null
    };
    let capacity = match exact_current_attempt {
        Some(attempt) => exact_attempt_capacity(db, attempt)?,
        None => json!({
            "status":"unknown",
            "capacity_available":null,
            "reason":"no_exact_attempt_binding_or_owner_route_selected",
            "full_scope_capacity_claimed":false,
        }),
    };
    if capacity["scope_capacity_available"].is_null() {
        gaps.push("full_provider_runtime_capacity_not_proven");
    }

    let attempt_projection = match exact_current_attempt {
        Some(attempt) => json!({
            "status":"current",
            "attempt_id":attempt.attempt_id,
            "task_revision":attempt.task_revision,
            "state":attempt.state,
            "owner":owner_profile(db,&attempt.owner_id)?,
            "start_owner":attempt.start_owner,
            "start_operation_id":attempt.start_operation_id,
            "created_at_ms":attempt.created_at_ms,
            "updated_at_ms":attempt.updated_at_ms,
            "binding":attempt_binding,
            "submission_ref":attempt.submission_ref,
        }),
        None => json!({
            "status":if row.current_attempt_id.is_some() {"record_missing"} else {"none"},
            "attempt_id":row.current_attempt_id,
        }),
    };
    let candidate_scope = json!({
        "current_attempt_candidate_ref":exact_current_attempt.and_then(|attempt| attempt.candidate_ref.as_deref()),
        "task_accepted_candidate_ref":row.accepted_candidate_ref,
        "candidate_artifact_bytes_read":false,
        "candidate_validation":"reference_only",
    });
    if !candidate_scope["current_attempt_candidate_ref"].is_string()
        && !candidate_scope["task_accepted_candidate_ref"].is_string()
    {
        gaps.push("no_retained_candidate_reference_for_current_scope");
    }

    let action = if hard_blocks.is_empty() {
        attempt_action
    } else {
        "forbidden"
    };
    let readiness = if !hard_blocks.is_empty() {
        "blocked"
    } else {
        // The Store has no launch manifest, route capability receipt,
        // workspace lease, or full-scope capacity proof.
        "unknown"
    };
    let mut plan = json!({
        "preview_only":true,
        "effects":"none",
        "launch_implemented":false,
        "preview_readiness":readiness,
        "task":{
            "task_id":row.task_id,
            "project_id":row.project_id,
            "revision":row.revision,
            "expected_revision":request.expected_task_revision,
            "state":row.state,
            "accepted_attempt_id":row.accepted_attempt_id,
            "accepted_operation_id":row.accepted_operation_id,
            "accepted_candidate_ref":row.accepted_candidate_ref,
            "task_brief":task_brief,
            "work_scope":work_scope,
            "updated_at_ms":row.updated_at_ms,
        },
        "attempt_action":action,
        "current_attempt":attempt_projection,
        "candidate_scope":candidate_scope,
        "queue_context":{
            "rank":{"status":"not_recorded"},
            "claim_readiness":claim_readiness,
            "dependencies":dependencies,
            "prior_launch_operations":launch_operations,
        },
        "workspace":launch_workspace_projection(&request),
        "route":route,
        "mcp":mcp_profile,
        "capacity":capacity,
        "overlap":overlaps,
        "requested_plan_facts":{
            "budget":request.budget,
            "stop_conditions":request.stop_conditions,
            "purpose":request.purpose,
            "enforceability":"not_recorded_until_a_durable_launch_manifest_exists",
        },
        "hard_blocks":hard_blocks,
        "coverage":if gaps.is_empty() {"complete"} else {"partial"},
        "gaps":gaps,
    });
    let mut detached_references = vec![json!({
        "method":"task.get",
        "params":{"task_id":row.task_id},
    })];
    if let Some(attempt_id) = attempt_projection["attempt_id"].as_str() {
        detached_references.push(json!({
            "method":"swarm.agent.inspect",
            "params":{"attempt_id":attempt_id},
        }));
    }
    detached_references.push(json!({
        "method":"swarm.queue.get",
        "params":{"project_id":row.project_id,"limit":DASHBOARD_PAGE_LIMIT},
    }));
    let canonical = model::canonical(&plan)?;
    let plan_digest = format!("sha256:{}", model::digest(canonical.as_bytes()));
    plan["plan_digest"] = json!(plan_digest);
    let canonical = model::canonical(&plan)?;
    if canonical.len() <= projection::MAX_SERIALIZED_BYTES {
        return Ok(plan);
    }

    Ok(json!({
        "plan_digest":plan_digest,
        "preview_only":true,
        "effects":"none",
        "launch_implemented":false,
        "preview_readiness":readiness,
        "task":{
            "task_id":row.task_id,
            "project_id":row.project_id,
            "revision":row.revision,
            "expected_revision":request.expected_task_revision,
            "state":row.state,
        },
        "attempt_action":action,
        "current_attempt":attempt_projection,
        "context_detached":{
            "status":"detached",
            "serialized_bytes":canonical.len(),
            "digest":model::digest(canonical.as_bytes()),
            "references":detached_references,
        },
        "hard_blocks":hard_blocks,
        "coverage":"partial",
        "gaps":["combined_launch_preview_exceeded_serialized_budget; use the exact linked read projections"],
    }))
}

fn queue_page(db: &Connection, params_value: &Value, brief_limit: usize) -> Result<Value> {
    model::fields(
        params_value,
        &["after", "limit", "project_id", "task_state"],
    )?;
    let page = PageRequest::parse(params_value)?;
    let project_id = validate_filter_text(
        params_value.get("project_id").and_then(Value::as_str),
        "project_id",
    )?;
    if params_value.get("project_id").is_some() && project_id.is_none() {
        return Err(Error::invalid("project_id must be a nonempty string"));
    }
    let task_state = validate_filter_text(
        params_value.get("task_state").and_then(Value::as_str),
        "task_state",
    )?;
    if params_value.get("task_state").is_some() && task_state.is_none() {
        return Err(Error::invalid("task_state must be a nonempty string"));
    }
    if task_state
        .as_deref()
        .is_some_and(|state| !matches!(state, "open" | "accepted" | "archived"))
    {
        return Err(Error::invalid(
            "task_state must be open, accepted, or archived",
        ));
    }
    let (total, rows) = query_tasks(
        db,
        project_id.as_deref(),
        task_state.as_deref(),
        page.after,
        page.limit,
    )?;
    let after = page.after.min(total);
    let items = rows
        .iter()
        .map(|row| task_queue_item(db, row, brief_limit))
        .collect::<Result<Vec<_>>>()?;
    bounded_page("task_queue", after, page.limit, total, items)
}

fn operation_page(db: &Connection, attempt_id: &str, page: PageRequest) -> Result<Value> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE attempt_id=?1",
        [attempt_id],
        |row| row.get(0),
    )?;
    let after = page.after.min(total);
    let mut statement = db.prepare(
        "SELECT operation_id,method,state,task_id,attempt_id,binding_id,binding_generation,
                prerequisite_operation_id,created_at_ms,updated_at_ms
         FROM operations WHERE attempt_id=?1
         ORDER BY created_at_ms DESC,operation_id DESC LIMIT ?2 OFFSET ?3",
    )?;
    let items = statement
        .query_map(params![attempt_id, page.limit, after], |row| {
            let state: String = row.get(2)?;
            Ok(json!({
                "operation_id":row.get::<_,String>(0)?,
                "method":row.get::<_,String>(1)?,
                "state":state,
                "task_id":row.get::<_,Option<String>>(3)?,
                "attempt_id":row.get::<_,Option<String>>(4)?,
                "binding_id":row.get::<_,Option<String>>(5)?,
                "binding_generation":row.get::<_,Option<i64>>(6)?,
                "prerequisite_operation_id":row.get::<_,Option<String>>(7)?,
                "created_at_ms":row.get::<_,i64>(8)?,
                "updated_at_ms":row.get::<_,i64>(9)?,
                "outcome_certainty":match state.as_str() {
                    "outcome_unknown" => "unknown",
                    "queued" => "not_dispatched",
                    "sending" | "native_accepted" => "external_effect_pending",
                    _ => "terminal_recorded",
                }
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    bounded_page("attempt_operations", after, page.limit, total, items)
}

fn attempt_operation_state_counts(db: &Connection, attempt_id: &str) -> Result<Value> {
    let by_state = {
        let mut statement = db.prepare(
            "SELECT state,count(*) FROM operations WHERE attempt_id=?1 GROUP BY state ORDER BY state",
        )?;
        let rows = statement
            .query_map([attempt_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut by_state = Map::new();
        for (state, count) in rows {
            by_state.insert(state, json!(count));
        }
        Value::Object(by_state)
    };
    let unresolved: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE attempt_id=?1
         AND state IN ('queued','sending','native_accepted','outcome_unknown')",
        [attempt_id],
        |row| row.get(0),
    )?;
    let outcome_unknown: i64 = db.query_row(
        "SELECT count(*) FROM operations WHERE attempt_id=?1 AND state='outcome_unknown'",
        [attempt_id],
        |row| row.get(0),
    )?;
    Ok(json!({"by_state":by_state,"unresolved":unresolved,"outcome_unknown":outcome_unknown}))
}

fn check_page(db: &Connection, attempt_id: &str, page: PageRequest) -> Result<Value> {
    let total: i64 = db.query_row(
        "SELECT count(*) FROM check_runs WHERE attempt_id=?1",
        [attempt_id],
        |row| row.get(0),
    )?;
    let after = page.after.min(total);
    let mut statement = db.prepare(
        "SELECT check_id,operation_id,candidate_ref,state,exit_code,resource_claimed_at_ms,
                resource_released_at_ms,started_at_ms,finished_at_ms
         FROM check_runs WHERE attempt_id=?1 ORDER BY created_at_ms DESC,check_id DESC LIMIT ?2 OFFSET ?3",
    )?;
    let items = statement
        .query_map(params![attempt_id, page.limit, after], |row| {
            Ok(json!({
                "check_id":row.get::<_,String>(0)?,
                "operation_id":row.get::<_,String>(1)?,
                "candidate_ref":row.get::<_,String>(2)?,
                "state":row.get::<_,String>(3)?,
                "exit_code":row.get::<_,Option<i64>>(4)?,
                "resource_claimed_at_ms":row.get::<_,Option<i64>>(5)?,
                "resource_released_at_ms":row.get::<_,Option<i64>>(6)?,
                "started_at_ms":row.get::<_,Option<i64>>(7)?,
                "finished_at_ms":row.get::<_,Option<i64>>(8)?,
            }))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    bounded_page("attempt_checks", after, page.limit, total, items)
}

fn current_scope_overlaps(
    db: &Connection,
    target_attempt_id: Option<&str>,
    project_id: &str,
    own_paths: &[String],
    page: PageRequest,
) -> Result<Value> {
    if own_paths.is_empty() {
        return Ok(json!({
            "status":"unknown",
            "items":[],
            "next_after":0,
            "has_more":false,
            "coverage":"unknown",
            "gaps":["no_initial_paths_recorded_for_literal_overlap_check","workspace_git_overlap_not_observed"],
        }));
    }
    let capped_paths = own_paths
        .iter()
        .take(MAX_SCOPE_PATHS)
        .cloned()
        .collect::<Vec<_>>();
    let selected_paths: Vec<String> = capped_paths
        .into_iter()
        .filter(|path| path.len() <= MAX_SCOPE_PATH_BYTES)
        .collect();
    let skipped_path_count = own_paths.len().saturating_sub(selected_paths.len());
    if selected_paths.is_empty() {
        return Ok(json!({
            "status":"unknown",
            "items":[],
            "next_after":page.after,
            "has_more":false,
            "coverage":"partial",
            "selected_target_path_count":0,
            "skipped_target_path_count":skipped_path_count,
            "gaps":["no_bounded_literal_initial_paths_available","git_worktree_and_uncommitted_overlap_not_observed"],
        }));
    }
    let paths_json = model::canonical(&json!(selected_paths.clone()))?;
    let mut statement = db.prepare(
        "SELECT a.attempt_id,a.task_id,a.task_revision,a.owner_id,a.state,
                a.created_at_ms,
                (SELECT json_group_array(path) FROM (
                    SELECT DISTINCT own.value AS path
                    FROM json_each(?3) own
                    JOIN json_each(a.task_snapshot_json,'$.spec.scope.initial_paths') other
                      ON own.value=other.value ORDER BY own.value LIMIT ?4
                ))
         FROM attempts a JOIN tasks t ON t.task_id=a.task_id
         WHERE a.released_at_ms IS NULL AND a.attempt_id<>?1 AND t.project_id=?2
           AND EXISTS(SELECT 1 FROM json_each(a.task_snapshot_json,'$.spec.scope.initial_paths') other
                      JOIN json_each(?3) own ON own.value=other.value)
         ORDER BY a.created_at_ms,a.attempt_id LIMIT ?5 OFFSET ?6",
    )?;
    let rows = statement
        .query_map(
            params![
                target_attempt_id.unwrap_or(""),
                project_id,
                paths_json,
                i64::try_from(MAX_MATCHED_PATHS).unwrap_or(8),
                page.limit + 1,
                page.after
            ],
            |row| {
                let matched_paths: String = row.get(6)?;
                let matched_paths: Value =
                    serde_json::from_str(&matched_paths).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                Ok(json!({
                    "attempt_id":row.get::<_,String>(0)?,
                    "task_id":row.get::<_,String>(1)?,
                    "task_revision":row.get::<_,i64>(2)?,
                    "owner_id":row.get::<_,String>(3)?,
                    "attempt_state":row.get::<_,String>(4)?,
                    "created_at_ms":row.get::<_,i64>(5)?,
                    "overlap_kind":"exact_initial_path_string_match",
                    "matched_paths":matched_paths,
                }))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let truncated = rows.len() as i64 > page.limit;
    let items: Vec<Value> = rows
        .into_iter()
        .take(usize::try_from(page.limit).unwrap_or(0))
        .collect();
    let items: Vec<Value> = items
        .into_iter()
        .map(|mut item| {
            if let Some(paths) = item["matched_paths"].as_array() {
                item["matched_paths"] = json!(
                    paths
                        .iter()
                        .map(|path| {
                            path.as_str()
                                .map(safe_scope_path)
                                .unwrap_or_else(|| json!({"status":"unknown"}))
                        })
                        .collect::<Vec<_>>()
                );
            }
            item
        })
        .collect();
    let returned_match_count = items.len() as i64;
    let next_after = page.after + returned_match_count;
    let mut gaps = vec![
        "only_bounded_literal_initial_path_matches_are_compared",
        "git_worktree_and_uncommitted_overlap_not_observed",
    ];
    if skipped_path_count > 0 {
        gaps.push("some_target_paths_were_not_compared");
    }
    if truncated {
        gaps.push("overlap_result_limit_reached");
    }
    Ok(json!({
        "status":"known_exact_matches_only",
        "items":items,
        "returned_match_count":returned_match_count,
        "selected_target_path_count":selected_paths.len(),
        "skipped_target_path_count":skipped_path_count,
        "has_more":truncated,
        "next_after":next_after,
        "limit":page.limit,
        "coverage":"partial",
        "gaps":gaps,
    }))
}

fn manager_exceptions_page(db: &Connection, params_value: &Value) -> Result<Value> {
    model::fields(params_value, &["after", "limit"])?;
    let page = PageRequest::parse(params_value)?;
    let raw = capacity::attention_report(db, ATTENTION_SCAN_LIMIT, page.after)?;
    let raw_items = raw["items"].as_array().cloned().unwrap_or_default();
    let raw_after = page.after.min(raw["total_items"].as_i64().unwrap_or(0));
    let raw_next = raw["next_after"].as_i64().unwrap_or(raw_after);
    let mut filtered = Vec::new();
    let mut raw_offsets = Vec::new();
    for (offset, item) in raw_items.iter().enumerate() {
        if item["manager_actionable"] == true {
            filtered.push(item.clone());
            raw_offsets.push(raw_after + offset as i64);
        } else if item.get("gap").is_some() {
            filtered.push(json!({
                "kind":"attention_coverage_gap",
                "gap":item["gap"],
            }));
            raw_offsets.push(raw_after + offset as i64);
        }
    }
    let filtered_total = filtered.len();
    let requested_count = usize::try_from(page.limit).unwrap_or(0).min(filtered_total);
    let selected: Vec<Value> = filtered.into_iter().take(requested_count).collect();
    let selected_offsets: Vec<i64> = raw_offsets.iter().take(requested_count).copied().collect();
    let limited = projection::limit_items(selected, page_gap_reference)?;
    let next_after = if limited.consumed == 0 {
        raw_next
    } else if limited.consumed < selected_offsets.len() {
        selected_offsets[limited.consumed - 1] + 1
    } else if filtered_total > requested_count {
        selected_offsets.last().copied().unwrap_or(raw_after) + 1
    } else {
        raw_next
    };
    let source_has_more = raw["projection"]["has_newer"] == true;
    let has_more = next_after < raw["total_items"].as_i64().unwrap_or(0) || source_has_more;
    let frame = projection::frame(
        "manager_exceptions",
        json!({"after":page.after,"next_after":next_after,"cursor_kind":"raw_attention_offset"}),
        &limited,
        ATTENTION_SCAN_LIMIT,
        page.after > 0,
        has_more,
        limited.gap_count == 0 && raw["projection"]["coverage_complete"] == true,
        Vec::new(),
    )?;
    Ok(json!({
        "items":limited.items,
        "next_after":next_after,
        "pagination":"filtered_manager_actionable_attention; cursor is the raw attention offset",
        "source_attention":{"total_items":raw["total_items"],"next_after":raw_next,"has_more":source_has_more},
        "projection":frame,
    }))
}

fn sanitize_capacity_item(item: &Value) -> Value {
    if item.get("gap").is_some() {
        return item.clone();
    }
    let scope = &item["scope"];
    json!({
        "scope":{
            "scope_key":scope["scope_key"],
            "runtime":scope["runtime"],
            "provider":scope["provider"],
            "account":scope["account"],
            "service":scope["service"],
            "identity":scope["identity"],
            "route_alias":scope["route_alias"],
        },
        "counts":item["counts"],
        "roster":item["roster"],
        "roster_reason":item["roster_reason"],
        "quota_incident_open":!item["quota_incident"].is_null(),
        "capacity_available":item["capacity_available"],
        "capacity_reason":item["capacity_reason"],
        "new_work_enabled":item["new_work_enabled"],
        "ledger_updated_at_ms":item["ledger_updated_at_ms"],
    })
}

fn dashboard_capacity(db: &Connection) -> Result<Value> {
    let source = capacity::capacity_report(db, DASHBOARD_PAGE_LIMIT, 0)?;
    let total = source["total_items"].as_i64().unwrap_or(0);
    let items = source["items"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(sanitize_capacity_item)
        .collect::<Vec<_>>();
    bounded_page(
        "capacity_accounting_summary",
        0,
        DASHBOARD_PAGE_LIMIT,
        total,
        items,
    )
}

fn dashboard_summary(
    db: &Connection,
    include_manager_details: bool,
    limit: i64,
    p: &Principal,
) -> Result<Value> {
    let task_summary = task_counts(db)?;
    let attempts = attempt_counts(db)?;
    let operations = operation_counts(db)?;
    let bindings = binding_counts(db)?;
    let admission = new_work_state(db)?;
    let generated_at_ms = model::now_ms()?;
    if !include_manager_details {
        return Ok(json!({
            "status":"partial",
            "generated_at_ms":generated_at_ms,
            "host":{"admission":admission},
            "tasks":task_summary,
            "attempts":attempts,
            "bindings":bindings,
            "operations":operations,
            "coverage":"aggregate_only",
            "gaps":["observer_dashboard_omits_assignment_details_and_manager_exceptions"],
        }));
    }
    let queue_params = json!({"after":0,"limit":limit});
    let queue = queue_page(db, &queue_params, 4_096)?;
    let capacity = dashboard_capacity(db)?;
    let exception = manager_exceptions_page(db, &json!({"after":0,"limit":limit}))?;
    let queue_partial = queue["projection"]["has_newer"] == true
        || queue["projection"]["gap_count"].as_i64().unwrap_or(0) > 0;
    let capacity_partial = capacity["projection"]["has_newer"] == true
        || capacity["projection"]["gap_count"].as_i64().unwrap_or(0) > 0;
    let exception_partial = exception["projection"]["has_newer"] == true
        || exception["projection"]["gap_count"].as_i64().unwrap_or(0) > 0;
    let response = json!({
        "status":"partial",
        "generated_at_ms":generated_at_ms,
        "host":{"admission":admission},
        "tasks":task_summary,
        "attempts":attempts,
        "bindings":bindings,
        "operations":operations,
        "queue_preview":queue,
        "capacity":capacity,
        "exceptions":exception,
        "coverage":{
            "task_attempt_binding_operation_counts":"complete_sql_aggregates",
            "queue_preview":if queue_partial {"partial"} else {"complete_page"},
            "capacity_page":if capacity_partial {"partial"} else {"complete_page"},
            "exceptions_page":if exception_partial {"partial"} else {"complete_page"},
        },
        "gaps":[
            "task_priority_not_recorded; queue order is creation time then task ID",
            "route_configuration_is_not_live_qualification_or_task_specific_capacity",
            "workspace_write_lease_and_git_overlap_are_not_recorded",
            "launch_manifest_and_runtime_capability_receipt_are_not_recorded",
            "no_global_participant_roster_or_transcript_is_projected",
        ],
        "viewer":p.client_id,
    });
    let canonical = model::canonical(&response)?;
    if canonical.len() <= projection::MAX_SERIALIZED_BYTES {
        Ok(response)
    } else {
        Ok(json!({
            "status":"partial",
            "generated_at_ms":generated_at_ms,
            "host":{"admission":admission},
            "tasks":task_summary,
            "attempts":attempts,
            "bindings":bindings,
            "operations":operations,
            "coverage":"aggregate_only_details_detached",
            "detail_projection":{
                "status":"detached",
                "serialized_bytes":canonical.len(),
                "digest":model::digest(canonical.as_bytes()),
                "requery":[
                    {"method":"swarm.queue.get","params":{"limit":DASHBOARD_PAGE_LIMIT}},
                    {"method":"report.capacity","params":{"limit":DASHBOARD_PAGE_LIMIT,"after":0}},
                    {"method":"swarm.exceptions.get","params":{"limit":limit,"after":0}},
                ],
            },
            "viewer":p.client_id,
            "gaps":["combined_dashboard_detail_pages_exceeded_serialized_budget; query each bounded projection separately"],
        }))
    }
}

/// Manager and Observer dashboard over current retained Store facts.
pub(super) fn dashboard(db: &Connection, p: &Principal, params_value: &Value) -> Result<Value> {
    require_dashboard_reader(db, p)?;
    model::fields(params_value, &["limit"])?;
    let limit = params_value
        .get("limit")
        .map(|value| {
            let limit = value
                .as_i64()
                .ok_or_else(|| Error::invalid("limit must be an integer"))?;
            if !(1..=DASHBOARD_PAGE_LIMIT).contains(&limit) {
                return Err(Error::invalid(format!(
                    "limit must be 1..={DASHBOARD_PAGE_LIMIT}"
                )));
            }
            Ok(limit)
        })
        .transpose()?
        .unwrap_or(DASHBOARD_PAGE_LIMIT);
    dashboard_summary(db, p.role != Role::Observer, limit, p)
}

/// Bounded manager queue read. SQL filters are applied before offset paging;
/// readiness is then derived from each returned Task/Attempt and acceptance
/// receipt without treating route configuration as runtime capacity.
pub(super) fn queue_get(db: &Connection, p: &Principal, params_value: &Value) -> Result<Value> {
    require_manager(db, p)?;
    queue_page(db, params_value, launcher::MAX_INLINE_BRIEF_BYTES)
}

/// Inspect one exact Attempt and its current binding/operation neighborhood.
/// A Manager sees only its own Attempt; the local Operator may inspect any.
pub(super) fn agent_inspect(db: &Connection, p: &Principal, params_value: &Value) -> Result<Value> {
    require_manager(db, p)?;
    model::fields(
        params_value,
        &[
            "attempt_id",
            "operation_after",
            "operation_limit",
            "check_after",
            "check_limit",
            "peer_after_client_id",
            "peer_limit",
            "overlap_after",
            "overlap_limit",
        ],
    )?;
    let attempt_id = model::text(params_value, "attempt_id")?.to_owned();
    let attempt = get_attempt_row(db, &attempt_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Attempt {attempt_id}")))?;
    if p.role == Role::Manager && attempt.owner_id != p.client_id {
        super::gm::require_authority(db, p)?;
    }
    let current_task = current_task(db, &attempt.task_id)?
        .ok_or_else(|| Error::new("NOT_FOUND", format!("Task {}", attempt.task_id)))?;
    let current_revision = current_task.revision;
    let task_state = current_task.state;
    let accepted_attempt_id = current_task.accepted_attempt_id;
    let accepted_operation_id = current_task.accepted_operation_id;
    let current_attempt_id = current_task.current_attempt_id;
    let task_revision_current = current_revision == attempt.task_revision;
    let current_attempt = attempt.released_at_ms.is_none()
        && current_attempt_id.as_deref() == Some(attempt.attempt_id.as_str());
    let spec: Option<TaskSpec> = attempt
        .snapshot
        .get("spec")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok());
    let exact_brief = attempt.snapshot.get("brief").cloned().unwrap_or_else(
        || json!({"status":"unavailable","reason":"attempt_snapshot_missing_brief"}),
    );
    let brief = launcher::brief_projection(
        &exact_brief,
        json!({"kind":"attempt_snapshot","attempt_id":attempt.attempt_id,"path":"$.brief"}),
        launcher::MAX_INLINE_BRIEF_BYTES,
    )?;
    let dependencies = dependency_projection(db, spec.as_ref())?;
    let retained_dependency_acceptances = snapshot_field_projection(
        attempt.snapshot.get("dependency_acceptances"),
        &attempt.attempt_id,
        "$.dependency_acceptances",
    )?;
    let retained_owner_policy = snapshot_field_projection(
        attempt.snapshot.get("owner_policy"),
        &attempt.attempt_id,
        "$.owner_policy",
    )?;
    let producer_facts = snapshot_field_projection(
        Some(&attempt.producers),
        &attempt.attempt_id,
        "attempt.producers",
    )?;
    let binding = match (&attempt.binding_id, attempt.binding_generation) {
        (Some(id), Some(generation)) => binding_summary(db, id, generation)?,
        _ => Value::Null,
    };
    let capacity = exact_attempt_capacity(db, &attempt)?;
    let operation_counts = attempt_operation_state_counts(db, &attempt.attempt_id)?;
    let operation_cursor = PageRequest {
        after: optional_i64(params_value, "operation_after", 0)?,
        limit: optional_i64(params_value, "operation_limit", 10)?,
    };
    if operation_cursor.after < 0 || !(1..=MAX_INSPECT_OPERATIONS).contains(&operation_cursor.limit)
    {
        return Err(Error::invalid(format!(
            "operation_after must be nonnegative and operation_limit must be 1..={MAX_INSPECT_OPERATIONS}"
        )));
    }
    let operations = operation_page(db, &attempt.attempt_id, operation_cursor)?;
    let check_cursor = PageRequest {
        after: optional_i64(params_value, "check_after", 0)?,
        limit: optional_i64(params_value, "check_limit", 10)?,
    };
    if check_cursor.after < 0 || !(1..=MAX_INSPECT_CHECKS).contains(&check_cursor.limit) {
        return Err(Error::invalid(format!(
            "check_after must be nonnegative and check_limit must be 1..={MAX_INSPECT_CHECKS}"
        )));
    }
    let check_runs = check_page(db, &attempt.attempt_id, check_cursor)?;
    let scope = scope_projection(spec.as_ref());
    let own_initial_paths = spec
        .as_ref()
        .and_then(|spec| spec.scope.as_ref())
        .map(|scope| scope.initial_paths.as_slice())
        .unwrap_or_default();
    let overlap_page = PageRequest {
        after: optional_i64(params_value, "overlap_after", 0)?,
        limit: optional_i64(params_value, "overlap_limit", 10)?,
    };
    if overlap_page.after < 0 || !(1..=MAX_INSPECT_OVERLAPS).contains(&overlap_page.limit) {
        return Err(Error::invalid(format!(
            "overlap_after must be nonnegative and overlap_limit must be 1..={MAX_INSPECT_OVERLAPS}"
        )));
    }
    let overlaps = current_scope_overlaps(
        db,
        Some(&attempt.attempt_id),
        &task_project(db, &attempt.task_id)?,
        own_initial_paths,
        overlap_page,
    )?;
    let peer_page = launcher::PageRequest {
        after: 0,
        limit: optional_i64(params_value, "peer_limit", 8)?,
    };
    if !(1..=20).contains(&peer_page.limit) {
        return Err(Error::invalid("peer_limit must be 1..=20"));
    }
    let peer_after = validate_filter_text(
        params_value
            .get("peer_after_client_id")
            .map(|value| {
                value
                    .as_str()
                    .ok_or_else(|| Error::invalid("peer_after_client_id must be a string"))
            })
            .transpose()?,
        "peer_after_client_id",
    )?;
    let peers = if current_attempt && task_revision_current {
        super::coordination::list_scope_participants(
            db,
            p,
            &attempt.task_id,
            attempt.task_revision,
            &attempt.attempt_id,
            peer_page.limit,
            peer_after.as_deref(),
        )?
    } else {
        json!({
            "items":[],
            "task_id":attempt.task_id,
            "task_revision":attempt.task_revision,
            "attempt_id":attempt.attempt_id,
            "next_after":null,
            "coverage":"not_current",
            "gaps":["participant_index_not_projected_for_stale_attempt_or_task_revision"],
        })
    };
    let mut attention_items = Vec::new();
    if attempt.state == "submitted" {
        attention_items.push(json!({
            "kind":"manager_decision_pending",
            "source":"current_attempt_state",
            "task_id":attempt.task_id,
            "attempt_id":attempt.attempt_id,
        }));
    }
    if attempt.state == "recovery_pending" {
        attention_items.push(json!({
            "kind":"runtime_recovery_pending",
            "source":"current_attempt_state",
            "task_id":attempt.task_id,
            "attempt_id":attempt.attempt_id,
        }));
    }
    if operation_counts["outcome_unknown"].as_i64().unwrap_or(0) > 0 {
        attention_items.push(json!({
            "kind":"external_operation_outcome_unknown",
            "source":"exact_attempt_operation_state_counts",
            "count":operation_counts["outcome_unknown"],
            "task_id":attempt.task_id,
            "attempt_id":attempt.attempt_id,
        }));
    }
    let attention = json!({
        "items":attention_items,
        "attempt_state":attempt.state,
        "unresolved_operations":operation_counts["unresolved"],
        "outcome_unknown_operations":operation_counts["outcome_unknown"],
        "binding_scope_attention":"use swarm.exceptions.get; exact binding attention feed is not independently projected here",
    });
    let mut gaps = vec![
        "workspace_write_lease_and_current_git_state_not_recorded",
        "runtime_capability_receipt_not_recorded",
        "launch_plan_and_route_live_qualification_not_recorded",
    ];
    if !task_revision_current {
        gaps.push("attempt_snapshot_revision_differs_from_current_task_revision");
    }
    if brief["status"] != "included" {
        gaps.push("exact_retained_source_brief_not_inline; inspect attempt.get by attempt ID");
    }
    if scope["status"] == "not_recorded" {
        gaps.push("work_scope_not_recorded_on_attempt_snapshot");
    } else if scope["status"] != "recorded" {
        gaps.push("one_or_more_scope_paths_were_redacted_or_omitted");
    }
    if peers["coverage"].as_str() != Some("complete") {
        gaps.push("current_scope_participant_index_coverage_is_partial");
    }
    if overlaps["coverage"] != "complete" {
        gaps.push("git_and_nonliteral_scope_overlap_are_not_covered");
    }
    gaps.push("exact_binding_scope_attention_uses_swarm.exceptions.get");
    gaps.push("relevance_ranked_peers_contracts_and_integration_cells_not_projected");
    let identity = json!({
        "task_id":attempt.task_id,
        "task_revision":attempt.task_revision,
        "current_task_revision":current_revision,
        "attempt_id":attempt.attempt_id,
        "attempt_created_at_ms":attempt.created_at_ms,
        "assignment_owner":owner_profile(db,&attempt.owner_id)?,
        "start_owner":attempt.start_owner,
        "start_operation_id":attempt.start_operation_id,
        "attempt_state":attempt.state,
        "released_at_ms":attempt.released_at_ms,
        "is_current_attempt":current_attempt,
        "task_revision_current":task_revision_current,
        "task_state":task_state,
        "accepted_attempt_id":accepted_attempt_id,
        "accepted_operation_id":accepted_operation_id,
    });
    let response = json!({
        "status":if current_attempt && task_revision_current {"current"} else {"stale"},
        "generated_at_ms":model::now_ms()?,
        "identity":identity.clone(),
        "work":{
            "task_brief":brief,
            "work_scope":scope,
            "dependency_acceptances_retained":retained_dependency_acceptances,
            "dependencies_current":dependencies,
            "owner_policy":retained_owner_policy,
            "producer_facts":producer_facts,
            "submission_ref":attempt.submission_ref,
            "candidate_ref":attempt.candidate_ref,
        },
        "runtime":{
            "binding":binding,
            "operations":operations,
            "operation_state_counts":operation_counts,
            "checks":check_runs,
            "capacity":capacity,
            "attention":attention,
        },
        "coordination":{
            "current_scope_participants":peers,
            "relevant_peers":{"status":"not_ranked_without_a_specific_relevance_selector","items":[]},
            "exact_scope_overlap":overlaps,
            "contracts":{"status":"not_recorded_in_task_assignment_store","items":[]},
            "integration_cells":{"status":"not_recorded","items":[]},
        },
        "coverage":if gaps.is_empty() {"complete"} else {"partial"},
        "gaps":gaps,
    });
    let canonical = model::canonical(&response)?;
    if canonical.len() <= projection::MAX_SERIALIZED_BYTES {
        Ok(response)
    } else {
        Ok(json!({
            "status":"partial",
            "identity":identity,
            "projection":{
                "status":"detached",
                "serialized_bytes":canonical.len(),
                "digest":model::digest(canonical.as_bytes()),
                "reference":{"kind":"exact_attempt_inspection","attempt_id":attempt.attempt_id},
            },
            "requery":{
                "method":"swarm.agent.inspect",
                "params":{
                    "attempt_id":attempt.attempt_id,
                    "operation_limit":1,
                    "check_limit":1,
                    "peer_limit":1,
                    "overlap_limit":1,
                },
            },
            "gaps":["combined_inspect_projection_exceeded_serialized_page_budget; requery with smaller detail pages"],
        }))
    }
}

fn task_project(db: &Connection, task_id: &str) -> Result<String> {
    db.query_row(
        "SELECT project_id FROM tasks WHERE task_id=?1",
        [task_id],
        |row| row.get(0),
    )
    .map_err(Into::into)
}

/// Manager exceptions are the `manager_actionable` subset of the existing
/// attention projection. Filtering happens before the returned page limit;
/// the cursor remains the raw attention offset so filtered rows are not lost.
pub(super) fn exceptions_get(
    db: &Connection,
    p: &Principal,
    params_value: &Value,
) -> Result<Value> {
    require_manager(db, p)?;
    manager_exceptions_page(db, params_value)
}
