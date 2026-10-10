//! Public Task graph projections and stable object paging over Store-owned facts.
use super::{object_scope, operations, projection, tasks};
use crate::{
    error::{Error, Result},
    model::{self, Principal},
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
            "attempt_id",
            "observation_id",
            "after",
            "limit",
        ],
    )?;
    if p.role == crate::model::Role::Operator {
        super::require_local_operator(db, &p.client_id)?;
        let mut projection_request = v.clone();
        if let Some(fields) = projection_request.as_object_mut() {
            fields.remove("attempt_id");
        }
        return super::producers::family(db, &projection_request);
    }

    let binding_id = model::text(v, "binding_id")?;
    let generation = model::positive(v, "generation")?;
    let attempt_id = family_attempt_id(db, p, v)?;
    let identity = object_scope::identity_for_attempt(db, &attempt_id)?.ok_or_else(missing)?;
    if identity.binding_id.as_deref() != Some(binding_id)
        || identity.binding_generation != Some(generation)
    {
        return Err(missing());
    }
    let grant = require_task(db, p, &identity, object_scope::TaskReadLevel::Evidence)?;
    if grant.level < object_scope::TaskReadLevel::Evidence {
        return Err(missing());
    }

    // Resolve one exact retained Attempt by its primary key. This keeps both
    // current and historical Task grants addressable without enumerating the
    // binding's lifetime Attempt history.
    let attempt = tasks::get_attempt(db, &attempt_id)?;
    let mut projection_request = v.clone();
    if let Some(fields) = projection_request.as_object_mut() {
        fields.remove("attempt_id");
    }
    let mut result = super::producers::family(db, &projection_request)?;
    if result["available"] != true {
        return Ok(result);
    }

    let observation_id = result["observation_id"].as_i64().ok_or_else(|| {
        Error::new(
            "OBJECT_SCOPE_DAMAGED",
            "family page has no exact observation ID",
        )
    })?;
    let raw: Option<String> = db
        .query_row(
            "SELECT payload_json FROM observations \
             WHERE observation_id=?1 AND binding_id=?2 AND binding_generation=?3 \
               AND kind='runtime.state'",
            params![observation_id, binding_id, generation],
            |row| row.get(0),
        )
        .optional()?;
    let raw = raw.ok_or_else(|| {
        Error::new(
            "OBSERVATION_NOT_FOUND",
            "the selected family observation is no longer retained for this binding generation",
        )
    })?;
    let state: Value = serde_json::from_str(&raw)?;
    let binding = operations::get_binding(db, binding_id, generation)?;
    scope_family_projection(&mut result, &state, &attempt, &identity, &binding)?;
    Ok(result)
}

const FAMILY_SCOPE_GAP_REASON: &str = "family_member_outside_task_scope";
const FAMILY_SCOPE_GAP_CODE: &str = "FAMILY_SCOPE_FILTERED";

fn family_attempt_id(db: &Connection, p: &Principal, v: &Value) -> Result<String> {
    if v.get("attempt_id").is_some() {
        return Ok(model::text(v, "attempt_id")?.to_owned());
    }
    if p.role == crate::model::Role::Participant {
        let registration =
            super::meta(db, &format!("client:{}", p.client_id))?.ok_or_else(missing)?;
        if registration["participation_basis"]["kind"] == "sponsored_reviewer" {
            return registration["participation_basis"]["review_scope"]["attempt_id"]
                .as_str()
                .filter(|id| !id.is_empty())
                .map(str::to_owned)
                .ok_or_else(missing);
        }
        let scope = super::coordination::current_scope(db, p)?;
        return scope["attempt"]["attempt_id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or_else(missing);
    }
    // Managers must select the exact Task Attempt whose native projection they
    // are entitled to read. Binding identity alone is shared across reused Tasks.
    Err(missing())
}

fn scope_family_projection(
    result: &mut Value,
    state: &Value,
    attempt: &Value,
    identity: &object_scope::TaskGraphIdentity,
    binding: &Value,
) -> Result<()> {
    let children = state["observed_children"].as_array().ok_or_else(|| {
        Error::new(
            "OBSERVATION_INCOMPLETE",
            "selected family observation has no child inventory",
        )
    })?;
    let root_id = state["native_root_id"]
        .as_str()
        .or_else(|| state["session"]["sessionId"].as_str());
    if state["native_root_id"].as_str().is_some_and(|root| {
        binding["native_root_id"]
            .as_str()
            .is_some_and(|retained| root != retained)
    }) || state["native_scope_key"].as_str().is_some_and(|scope| {
        binding["native_scope_key"]
            .as_str()
            .is_some_and(|retained| scope != retained)
    }) {
        return Err(Error::new(
            "OBJECT_SCOPE_DAMAGED",
            "family observation native identity differs from its retained binding",
        ));
    }
    let producers = attempt["producers"].as_array().ok_or_else(|| {
        Error::new(
            "OBJECT_SCOPE_DAMAGED",
            "retained Attempt producer history is not an array",
        )
    })?;

    let mut producer_members = BTreeSet::new();
    let mut allowed_runs = BTreeSet::new();
    let attempt_id = model::text(attempt, "attempt_id")?;
    for producer in producers {
        if producer
            .get("attempt_id")
            .and_then(Value::as_str)
            .is_some_and(|id| id != attempt_id)
            || producer
                .get("task_id")
                .and_then(Value::as_str)
                .is_some_and(|id| id != identity.task_id)
            || producer
                .get("task_revision")
                .and_then(Value::as_i64)
                .is_some_and(|revision| Some(revision) != identity.task_revision)
            || producer
                .get("binding_id")
                .and_then(Value::as_str)
                .is_some_and(|id| Some(id) != identity.binding_id.as_deref())
            || producer
                .get("binding_generation")
                .and_then(Value::as_i64)
                .is_some_and(|generation| Some(generation) != identity.binding_generation)
        {
            return Err(Error::new(
                "OBJECT_SCOPE_DAMAGED",
                "retained family producer differs from its exact Attempt identity",
            ));
        }
        let Some(session_id) = producer
            .get("native_session_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        else {
            continue;
        };
        if let Some(run_id) = producer
            .get("native_run_id")
            .and_then(Value::as_str)
            .filter(|id| !id.is_empty())
        {
            allowed_runs.insert((session_id.to_owned(), run_id.to_owned()));
        }
        // The binding root is shared when a module is reused. A producer on
        // that root authorizes only its exact run; it does not authorize every
        // child or turn retained by the root's family observation.
        if Some(session_id) != root_id {
            producer_members.insert(session_id.to_owned());
        }
    }

    let mut children_by_parent = BTreeMap::<String, Vec<String>>::new();
    let mut observed_members = BTreeSet::new();
    for child in children {
        let Some(session_id) = child["sessionId"].as_str() else {
            continue;
        };
        if !observed_members.insert(session_id.to_owned()) {
            return Err(Error::new(
                "OBSERVATION_INCOMPLETE",
                "selected family observation repeats a child identity",
            ));
        }
        if let Some(parent_id) = child["parentSessionId"].as_str() {
            children_by_parent
                .entry(parent_id.to_owned())
                .or_default()
                .push(session_id.to_owned());
        }
    }
    let mut authorized_members = producer_members.clone();
    let mut pending = VecDeque::from_iter(producer_members);
    while let Some(parent_id) = pending.pop_front() {
        if let Some(children) = children_by_parent.get(&parent_id) {
            for child_id in children {
                if authorized_members.insert(child_id.clone()) {
                    pending.push_back(child_id.clone());
                }
            }
        }
    }
    let has_hidden_members = children.iter().any(|child| {
        child["sessionId"]
            .as_str()
            .is_none_or(|session_id| !authorized_members.contains(session_id))
    });

    let after = result["projection"]["range"]["after"]
        .as_u64()
        .and_then(|value| usize::try_from(value).ok())
        .ok_or_else(|| {
            Error::new(
                "OBJECT_SCOPE_DAMAGED",
                "family projection has no valid source cursor",
            )
        })?;
    let mut items = result["items"]
        .as_array()
        .cloned()
        .ok_or_else(|| Error::new("OBJECT_SCOPE_DAMAGED", "family projection has no item page"))?;
    let mut filtered = has_hidden_members;
    let mut added_gap_count = 0usize;
    let mut scoped_gap_count = 0usize;
    for (offset, item) in items.iter_mut().enumerate() {
        let source = children.get(after + offset).ok_or_else(|| {
            Error::new(
                "OBJECT_SCOPE_DAMAGED",
                "family projection cursor exceeds its selected observation",
            )
        })?;
        if item["sessionId"] != source["sessionId"] {
            return Err(Error::new(
                "OBJECT_SCOPE_DAMAGED",
                "family page member differs from its exact observation position",
            ));
        }
        let Some(session_id) = source["sessionId"].as_str() else {
            filtered = true;
            scoped_gap_count += 1;
            let was_gap = item["gap"].is_object();
            let serialized = model::canonical(source)?;
            *item = projection::family_gap_reference(
                source,
                FAMILY_SCOPE_GAP_REASON,
                serialized.len(),
            )?;
            if !was_gap {
                added_gap_count += 1;
            }
            continue;
        };
        if !authorized_members.contains(session_id) {
            filtered = true;
            scoped_gap_count += 1;
            let was_gap = item["gap"].is_object();
            let serialized = model::canonical(source)?;
            *item = projection::family_gap_reference(
                source,
                FAMILY_SCOPE_GAP_REASON,
                serialized.len(),
            )?;
            if !was_gap {
                added_gap_count += 1;
            }
        }
    }

    if let Some(turns) = result["turns"].as_array_mut() {
        let original_len = turns.len();
        turns.retain(|turn| {
            let (Some(session_id), Some(run_id)) =
                (turn["sessionId"].as_str(), turn["turnId"].as_str())
            else {
                return false;
            };
            if Some(session_id) == root_id {
                allowed_runs.contains(&(session_id.to_owned(), run_id.to_owned()))
            } else {
                authorized_members.contains(session_id)
                    || allowed_runs.contains(&(session_id.to_owned(), run_id.to_owned()))
            }
        });
        filtered |= turns.len() != original_len;
    }

    if let Some(gaps) = result["gaps"].as_array_mut() {
        let original_len = gaps.len();
        gaps.retain(|gap| {
            gap["session_id"].as_str().is_none_or(|session_id| {
                Some(session_id) == root_id || authorized_members.contains(session_id)
            })
        });
        filtered |= gaps.len() != original_len;
    }

    let projection_frame = result["projection"].as_object_mut().ok_or_else(|| {
        Error::new(
            "OBJECT_SCOPE_DAMAGED",
            "family projection frame is malformed",
        )
    })?;
    if let Some(stale) = projection_frame
        .get_mut("retained_stale_members")
        .and_then(Value::as_array_mut)
    {
        let original_len = stale.len();
        stale.retain(|session| {
            session
                .as_str()
                .is_some_and(|session_id| authorized_members.contains(session_id))
        });
        filtered |= stale.len() != original_len;
    }

    if added_gap_count > 0 {
        let previous = projection_frame
            .get("gap_count")
            .and_then(Value::as_u64)
            .unwrap_or_default() as usize;
        projection_frame.insert(
            "gap_count".to_owned(),
            json!(previous.saturating_add(added_gap_count)),
        );
    }
    if scoped_gap_count > 0 {
        projection_frame.insert("gap_reason".to_owned(), json!(FAMILY_SCOPE_GAP_REASON));
    }
    let items_json = Value::Array(items.clone());
    let serialized_items = model::canonical(&items_json)?;
    projection_frame.insert(
        "projection_revision".to_owned(),
        json!(model::digest(serialized_items.as_bytes())),
    );
    projection_frame.insert(
        "serialized_byte_length".to_owned(),
        json!(serialized_items.len()),
    );
    if filtered {
        projection_frame.insert("coverage_complete".to_owned(), json!(false));
        projection_frame.insert(
            "scope_filter".to_owned(),
            json!({"code":FAMILY_SCOPE_GAP_CODE,"source":"task_scope"}),
        );
        result["family_completeness"] = json!("partial");
    }
    result["items"] = items_json;
    Ok(())
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
    // The source scan remains bounded independently of the requested visible
    // page size. Unauthorized rows advance coverage without consuming a slot.
    let scan_limit = projection::MAX_PROJECTED_ITEMS as i64;
    let rows = statement
        .query_map(
            params![kind.source(), after, state, scan_limit + 1],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut examined = after;
    let mut before = Vec::new();
    let mut projected = Vec::new();
    let mut filtered = Vec::new();
    let mut damaged = Vec::new();
    for (position, id) in rows.iter().take(scan_limit as usize) {
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
        if projected.len() == limit as usize {
            break;
        }
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
    let has_newer = limited.stopped_early || rows.iter().any(|(position, _)| *position > next);
    Ok(
        json!({"items":limited.items,"next_after":next,"pagination":"committed_observation_position",
        "examined_through":next,"filtered_count":filtered.iter().filter(|position| **position<=next).count(),
        "has_newer":has_newer,"coverage_complete":limited.gap_count==0 && damaged.iter().all(|position| *position>next),
        "gap_count":limited.gap_count+damaged.iter().filter(|position| **position<=next).count()}),
    )
}

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

const TIMELINE_CANDIDATE_SQL: &str = "WHERE o.observation_id>?1 AND o.observation_id<=?6 \
    AND (?7 IS NULL OR o.observation_id<?7) \
    AND o.source_stream_id NOT IN ('controller:messages','controller:read-position:operation','controller:read-position:task') \
    AND ((?2=1 AND o.kind IN ('message.send','coordination.message.send','task.feedback','check.completed') AND json_extract(o.payload_json,'$.recipient')=?3) \
      OR (?2=0 AND (o.operation_id IS NOT NULL OR ?4=1 \
        OR (o.kind NOT IN ('message.send','message.cancel','coordination.send','coordination.message.send','task.feedback','task.review_stale','check.completed') \
          AND o.kind NOT LIKE 'coordination.%' AND o.kind NOT LIKE 'review.%' AND o.kind NOT LIKE 'automation.%'))))";

const TIMELINE_HEAD_SCAN_LIMIT: usize = 200;

#[derive(Debug, Serialize, Deserialize)]
struct TimelineHeadContinuation {
    version: u8,
    client_id: String,
    role: crate::model::Role,
    mailbox_only: bool,
    authorization_revision: String,
    cut: i64,
    before: i64,
}

fn timeline_head_authorization_revision(db: &Connection, p: &Principal) -> Result<String> {
    let method_scope = match super::mcp_authorization(db, p, &json!({})) {
        Ok(value) => value,
        Err(error) if error.code == "FORBIDDEN" => json!({"role":p.role}),
        Err(error) => return Err(error),
    };
    let registration = super::meta(db, &format!("client:{}", p.client_id))?;
    let mut context = json!({
        "client_id":p.client_id,
        "role":p.role,
        "method_scope":method_scope,
        "client_registration":registration,
    });
    if p.role == crate::model::Role::Manager {
        // Current Task ownership changes Manager Operation visibility without
        // changing the MCP method grant, so bind continuations to that scope too.
        let mut statement = db.prepare(
            "SELECT a.task_id,t.project_id,a.attempt_id,a.task_revision \
             FROM attempts AS a JOIN tasks AS t ON t.task_id=a.task_id \
             WHERE a.owner_id=?1 AND a.released_at_ms IS NULL \
             ORDER BY a.task_id,a.attempt_id",
        )?;
        let scopes = statement
            .query_map([&p.client_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        context["current_manager_attempt_scopes"] = json!(scopes);
    }
    Ok(model::digest(model::canonical(&context)?.as_bytes()))
}

fn timeline_head_signing_key(db: &Connection) -> Result<String> {
    let operator_id = super::meta(db, super::LOCAL_OPERATOR_CLIENT_ID_KEY)?
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| {
            Error::new(
                "TIMELINE_HEAD_UNAVAILABLE",
                "Store has no local continuation signing identity",
            )
        })?;
    let registration = super::meta(db, &format!("client:{operator_id}"))?
        .ok_or_else(|| Error::new("TIMELINE_HEAD_UNAVAILABLE", "signing identity is missing"))?;
    registration["token_hash"]
        .as_str()
        .filter(|key| key.len() == 64 && key.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::new(
                "TIMELINE_HEAD_UNAVAILABLE",
                "subscription principal has no valid continuation key",
            )
        })
}

fn timeline_head_mac(key: &str, payload: &[u8]) -> [u8; 32] {
    let mut inner_pad = [0x36; 64];
    let mut outer_pad = [0x5c; 64];
    for (index, byte) in key.as_bytes().iter().enumerate() {
        inner_pad[index] ^= *byte;
        outer_pad[index] ^= *byte;
    }
    let mut inner = Sha256::new();
    inner.update(inner_pad);
    inner.update(payload);
    let inner_digest = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(outer_pad);
    outer.update(inner_digest);
    outer.finalize().into()
}

fn encode_timeline_head_continuation(
    key: &str,
    claims: &TimelineHeadContinuation,
) -> Result<String> {
    let payload = serde_json::to_vec(claims).map_err(|_| {
        Error::new(
            "TIMELINE_HEAD_UNAVAILABLE",
            "could not encode the bounded timeline continuation",
        )
    })?;
    let signature = timeline_head_mac(key, &payload);
    Ok(format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(payload),
        URL_SAFE_NO_PAD.encode(signature)
    ))
}

fn decode_timeline_head_continuation(
    key: &str,
    p: &Principal,
    mailbox_only: bool,
    authorization_revision: &str,
    token: &str,
) -> Result<TimelineHeadContinuation> {
    let invalid = || Error::invalid("head_continuation is invalid");
    if token.len() > 2048 {
        return Err(invalid());
    }
    let (encoded_payload, encoded_signature) = token.split_once('.').ok_or_else(invalid)?;
    let payload = URL_SAFE_NO_PAD
        .decode(encoded_payload)
        .map_err(|_| invalid())?;
    let signature = URL_SAFE_NO_PAD
        .decode(encoded_signature)
        .map_err(|_| invalid())?;
    let expected = timeline_head_mac(key, &payload);
    if signature.len() != expected.len()
        || signature
            .iter()
            .zip(expected.iter())
            .fold(0_u8, |difference, (actual, expected)| {
                difference | (*actual ^ *expected)
            })
            != 0
    {
        return Err(invalid());
    }
    let claims: TimelineHeadContinuation =
        serde_json::from_slice(&payload).map_err(|_| invalid())?;
    if claims.version != 1
        || claims.client_id != p.client_id
        || claims.role != p.role
        || claims.mailbox_only != mailbox_only
        || claims.cut <= 0
        || claims.before <= 0
        || claims.before >= claims.cut
    {
        return Err(Error::new(
            "TIMELINE_HEAD_STALE",
            "subscription head continuation no longer matches this principal or cut",
        ));
    }
    if claims.authorization_revision != authorization_revision {
        return Err(Error::new(
            "TIMELINE_HEAD_STALE",
            "subscription authorization changed during head scan; restart admission",
        ));
    }
    Ok(claims)
}

/// Establish a concrete authorized boundary before a live subscriber starts.
/// Each call examines one bounded keyset page and never materializes payloads.
/// The first page fixes a committed finite cut; signed continuations bind
/// later pages to that cut and the same principal authorization revision.
fn timeline_head(
    db: &Connection,
    p: &Principal,
    mailbox_only: bool,
    continuation: Option<&str>,
) -> Result<Value> {
    let operator = p.role == crate::model::Role::Operator;
    if operator {
        super::require_local_operator(db, &p.client_id)?;
    }
    let authorization_revision = timeline_head_authorization_revision(db, p)?;
    let signing_key = timeline_head_signing_key(db)?;
    let claims = continuation
        .map(|token| {
            decode_timeline_head_continuation(
                &signing_key,
                p,
                mailbox_only,
                &authorization_revision,
                token,
            )
        })
        .transpose()?;
    let cut_bound = claims.as_ref().map_or(i64::MAX, |claims| claims.cut);
    let before = claims.as_ref().map(|claims| claims.before);
    let sql = format!(
        "SELECT o.observation_id,o.operation_id FROM observations AS o {TIMELINE_CANDIDATE_SQL} ORDER BY o.observation_id DESC LIMIT ?5"
    );
    let mut statement = db.prepare(&sql)?;
    let rows = statement
        .query_map(
            params![
                0,
                mailbox_only,
                p.client_id,
                operator,
                TIMELINE_HEAD_SCAN_LIMIT as i64 + 1,
                cut_bound,
                before
            ],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<String>>(1)?)),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let cut = claims
        .as_ref()
        .map(|claims| claims.cut)
        .or_else(|| rows.first().map(|(cursor, _)| *cursor))
        .unwrap_or(0);
    for (cursor, operation_id) in rows.iter().take(TIMELINE_HEAD_SCAN_LIMIT) {
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
        return Ok(
            json!({"cursor":cursor,"source_kind":"observation_timeline","head":true,
            "admission":{"state":"established","empty":false}}),
        );
    }
    if rows.len() > TIMELINE_HEAD_SCAN_LIMIT {
        let before = rows[TIMELINE_HEAD_SCAN_LIMIT - 1].0;
        let token = encode_timeline_head_continuation(
            &signing_key,
            &TimelineHeadContinuation {
                version: 1,
                client_id: p.client_id.clone(),
                role: p.role.clone(),
                mailbox_only,
                authorization_revision,
                cut,
                before,
            },
        )?;
        return Ok(json!({"source_kind":"observation_timeline","head":true,
            "admission":{"state":"continuation","continuation":token}}));
    }
    Ok(
        json!({"cursor":cut,"source_kind":"observation_timeline","head":true,
        "admission":{"state":"established","empty":true}}),
    )
}

/// Linked observations pass the same grant resolver as get/list. Candidate
/// filtering is only a bounded source scan; it does not establish disclosure.
pub(super) fn timeline(
    db: &Connection,
    p: &Principal,
    mailbox_only: bool,
    v: &Value,
) -> Result<Value> {
    model::fields(
        v,
        &["after", "limit", "head", "through", "head_continuation"],
    )?;
    let head_continuation = match v.get("head_continuation") {
        None => None,
        Some(Value::String(token)) => Some(token.as_str()),
        Some(_) => return Err(Error::invalid("head_continuation must be text")),
    };
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
            return timeline_head(db, p, mailbox_only, head_continuation);
        }
    }
    if head_continuation.is_some() {
        return Err(Error::invalid("head_continuation requires head=true"));
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
        "SELECT o.observation_id,o.kind,o.recorded_at_ms,o.operation_id FROM observations AS o {TIMELINE_CANDIDATE_SQL} ORDER BY o.observation_id LIMIT ?5"
    );
    let mut statement = db.prepare(&sql)?;
    let scan_limit = projection::MAX_PROJECTED_ITEMS as i64;
    let rows = statement
        .query_map(
            params![
                after,
                mailbox_only,
                p.client_id,
                operator,
                scan_limit + 1,
                through,
                None::<i64>
            ],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut examined = after;
    let mut before = Vec::new();
    let mut filtered = Vec::new();
    let mut damaged = Vec::new();
    let mut entries = Vec::new();
    for (cursor, kind, time, operation_id) in rows.iter().take(scan_limit as usize) {
        let projected: Result<Option<Value>> = (|| {
            let grant = if let Some(id) = operation_id {
                let Some(grant) = object_scope::resolve_operation_read(db, p, id)? else {
                    return Ok(None);
                };
                Some(grant)
            } else {
                None
            };
            // The candidate scan retains metadata only. Load a payload only
            // after the exact receipt grant allows its projection.
            let raw: String = db.query_row(
                "SELECT payload_json FROM observations WHERE observation_id=?1",
                [cursor],
                |row| row.get(0),
            )?;
            let retained: Value = serde_json::from_str(&raw).map_err(|_| {
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
        // Hidden observations advance coverage even after the visible page
        // fills; stop before consuming the next authorized observation.
        if entries.len() == limit as usize {
            break;
        }
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
        scan_limit + 1,
        after > 0,
        limited.stopped_early || rows.iter().any(|(cursor, ..)| *cursor > next),
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
        "operation.rejected" | "operation.outcome_unknown" | "operation.cancelled" => &[
            "schema_version",
            "phase",
            "status",
            "occurrence_id",
            "error_code",
        ],
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

#[cfg(test)]
#[path = "object_reads_family_tests.rs"]
mod family_tests;
