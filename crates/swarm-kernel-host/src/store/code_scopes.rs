//! Retained advisory code-scope proposals and manager decisions.
//!
//! These records describe coordination only. They do not lock files, inspect
//! Git, start work, or release an Attempt. Immutable revisions and bounded
//! indexes live in the existing `meta` table; Operations remain the mutation
//! receipts.

use super::{coordination, meta, set_meta, tasks};
use crate::{
    coordination::code_scope as wire,
    error::{Error, Result},
    model::{self, Principal, Role},
};
use rusqlite::{Connection, Transaction, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;
use swarm_contracts::coordination_limits as limits;

const SCHEMA: &str = "eliot.code_scope.v1";
const RECORD_PREFIX: &str = "code.scope:v1:record:";
const REVISION_PREFIX: &str = "code.scope:v1:revision:";
const TASK_PAGE_PREFIX: &str = "code.scope:v1:task-page:";
const TASK_SEQUENCE_PREFIX: &str = "code.scope:v1:task-sequence:";
const ACTIVE_PREFIX: &str = "code.scope:v1:active:";

#[derive(Clone)]
struct IndexRow {
    key: String,
    sequence: i64,
    scope_intent_id: String,
}

#[derive(Clone)]
struct Overlap {
    class: &'static str,
    paths: Vec<String>,
    symbols: Vec<String>,
    interfaces: Vec<String>,
    unsupported_paths: Vec<UnsupportedPath>,
}

/// One path pair whose glob form cannot be classified safely. Retained as an
/// explicit uncertainty field on `Overlap` so an unclassified pair survives
/// classification instead of being dropped whenever another pair matched.
#[derive(Clone, Debug, PartialEq, Eq)]
struct UnsupportedPath {
    left: String,
    right: String,
}

impl UnsupportedPath {
    fn pair(left: &str, right: &str) -> Self {
        Self {
            left: left.to_owned(),
            right: right.to_owned(),
        }
    }

    fn projection(&self) -> Value {
        json!({"left": self.left, "right": self.right})
    }
}

/// Store mutation seam. Every code-scope change is a synchronous metadata
/// operation; the caller persists the returned receipt as the Operation.
pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    method: &str,
    value: &Value,
    operation_id: &str,
    now: i64,
) -> Result<(Value, bool)> {
    let principal = super::current_principal(tx, principal.clone())?;
    let receipt = match method {
        "code.scope.propose" => propose(
            tx,
            &principal,
            wire::parse_propose_request(value)?,
            operation_id,
            now,
        )?,
        "code.scope.accept" => accept(
            tx,
            &principal,
            wire::parse_accept_request(value)?,
            operation_id,
            now,
        )?,
        "code.scope.release" => release(
            tx,
            &principal,
            wire::parse_release_request(value)?,
            operation_id,
            now,
        )?,
        _ => return Err(Error::new("METHOD_NOT_FOUND", method)),
    };
    let task_id = model::text(&receipt, "task_id")?;
    let attempt_id = model::text(&receipt, "attempt_id")?;
    let attempt = tasks::get_attempt(tx, attempt_id)?;
    coordination::attach_operation_scope(tx, operation_id, task_id, attempt_id, &attempt)?;
    Ok((receipt, false))
}

/// Read a bounded exact-Task page. `inspect` and `conflicts` share the same
/// retained projection; the latter adds conservative overlap findings.
pub(super) fn read(
    db: &Connection,
    principal: &Principal,
    method: &str,
    value: &Value,
) -> Result<Value> {
    if !matches!(method, "code.scope.inspect" | "code.scope.conflicts") {
        return Err(Error::new("METHOD_NOT_FOUND", method));
    }
    let principal = super::current_principal(db, principal.clone())?;
    let request = wire::parse_read_request(value)?;
    let (task_revision, attempt_id) = authorize_read_scope(
        db,
        &principal,
        &request.task_id,
        request.task_revision,
        request.attempt_id.as_deref(),
    )?;
    let task_current = tasks::get_task(db, &request.task_id)?;
    let current_task_revision = task_current["revision"]
        .as_i64()
        .ok_or_else(|| damaged("Task has no current revision"))?;
    let current_attempt_id = if task_current["state"] == "open" {
        task_current["current_attempt_id"]
            .as_str()
            .unwrap_or_default()
            .to_owned()
    } else {
        String::new()
    };
    let current_scope_is_live = task_current["state"] == "open"
        && task_revision == current_task_revision
        && attempt_id == current_attempt_id;
    if request.scope_intent_id.is_some() && request.after_scope_id.is_some() {
        return Err(Error::invalid(
            "after_scope_id cannot be combined with scope_intent_id",
        ));
    }
    let (scanned_ids, records, has_more, mut gaps) =
        if let Some(scope_id) = request.scope_intent_id.as_deref() {
            let record = load_record(db, scope_id)?;
            if record["task_id"] != request.task_id {
                return Err(Error::new(
                    "NOT_FOUND",
                    "scope intent is outside the requested Task",
                ));
            }
            let matches = request
                .attempt_id
                .as_deref()
                .is_none_or(|attempt| record["attempt_id"] == attempt)
                && request
                    .client_id
                    .as_deref()
                    .is_none_or(|client_id| record["actor"]["client_id"] == client_id)
                && matches_read_terms(&record, &request);
            (
                Vec::new(),
                if matches { vec![record] } else { Vec::new() },
                false,
                Vec::new(),
            )
        } else {
            let after_sequence = request
                .after_scope_id
                .as_deref()
                .map(|id| cursor_sequence(db, id, &request.task_id))
                .transpose()?;
            let prefix = task_page_prefix(&request.task_id)?;
            let rows = task_index_page(db, &prefix, after_sequence, request.limit)?;
            let has_more = rows.len() as i64 > request.limit;
            let scanned = rows.iter().take(request.limit as usize).collect::<Vec<_>>();
            let scanned_ids = scanned
                .iter()
                .map(|row| row.scope_intent_id.clone())
                .collect::<Vec<_>>();
            let mut records = Vec::with_capacity(scanned.len());
            let mut gaps = Vec::new();
            for index in scanned {
                let Some(record) = meta(db, &record_key(&index.scope_intent_id)?)? else {
                    gaps.push("scope_index_record_missing".to_owned());
                    continue;
                };
                if verify_record(&record, &index.scope_intent_id).is_err()
                    || record["task_id"] != request.task_id
                    || record["list_sequence"] != index.sequence
                {
                    gaps.push("scope_index_record_mismatch".to_owned());
                    continue;
                }
                if request
                    .attempt_id
                    .as_deref()
                    .is_some_and(|attempt| record["attempt_id"] != attempt)
                    || request
                        .client_id
                        .as_deref()
                        .is_some_and(|client_id| record["actor"]["client_id"] != client_id)
                    || !matches_read_terms(&record, &request)
                {
                    continue;
                }
                records.push(record);
            }
            (scanned_ids, records, has_more, gaps)
        };
    let active = if method == "code.scope.conflicts" && current_scope_is_live {
        let (active, active_more) = active_records_page(
            db,
            &request.task_id,
            task_revision,
            &attempt_id,
            limits::MAX_READ_PAGE_SIZE,
        )?;
        if active_more {
            gaps.push("active_scope_page_truncated".to_owned());
        }
        active
    } else {
        if method == "code.scope.conflicts" {
            gaps.push("requested_scope_attempt_not_current".to_owned());
        }
        Vec::new()
    };
    let now = model::now_ms()?;
    let mut items = records
        .iter()
        .map(|record| project_record(record, current_task_revision, &current_attempt_id, now))
        .collect::<Result<Vec<_>>>()?;
    if method == "code.scope.conflicts" {
        for item in &mut items {
            let current = records
                .iter()
                .find(|record| record["scope_intent_id"] == item["scope_intent_id"]);
            if let Some(current) = current.filter(|record| {
                current_scope_is_live
                    && record["state"] == "active"
                    && record["task_revision"] == task_revision
                    && record["attempt_id"] == attempt_id
            }) {
                item["conflicts"] = json!(conflicts_for(current, &active));
            }
        }
    }
    let next_after_scope_id = if has_more {
        scanned_ids
            .last()
            .map(|scope_id| json!(scope_id))
            .unwrap_or(Value::Null)
    } else {
        Value::Null
    };
    gaps.sort();
    gaps.dedup();
    Ok(json!({
        "items":items,
        "task_id":request.task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "next_after_scope_id":next_after_scope_id,
        "coverage":if has_more || !gaps.is_empty() { "partial" } else { "complete" },
        "gaps":gaps,
    }))
}

/// Return the exact retained accepted-scope revisions for a contract decision
/// lane. Incomplete scope coverage is explicit and must never be interpreted
/// as absence. No registration fingerprint is emitted as a scope receipt.
pub(crate) fn current_scope_revisions(
    db: &Connection,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<Value> {
    let task = match tasks::get_task(db, task_id) {
        Ok(task) => task,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(scope_refs_unavailable(
                task_id,
                task_revision,
                attempt_id,
                "task_unavailable",
            ));
        }
        Err(error) => return Err(error),
    };
    let attempt = match tasks::get_attempt(db, attempt_id) {
        Ok(attempt) => attempt,
        Err(error) if error.code == "NOT_FOUND" => {
            return Ok(scope_refs_unavailable(
                task_id,
                task_revision,
                attempt_id,
                "attempt_unavailable",
            ));
        }
        Err(error) => return Err(error),
    };
    if task["task_id"] != task_id
        || task["revision"] != task_revision
        || task["current_attempt_id"] != attempt_id
        || task["state"] != "open"
        || attempt["task_id"] != task_id
        || attempt["task_revision"] != task_revision
        || !attempt["released_at_ms"].is_null()
    {
        return Ok(scope_refs_unavailable(
            task_id,
            task_revision,
            attempt_id,
            "task_attempt_not_current",
        ));
    }
    let prefix = active_prefix(task_id, task_revision, attempt_id)?;
    let (rows, more) = active_index_page(db, &prefix, None, limits::MAX_READ_PAGE_SIZE)?;
    let now = model::now_ms()?;
    let mut items = Vec::new();
    let mut gaps = Vec::new();
    for row in rows.iter().take(limits::MAX_READ_PAGE_SIZE as usize) {
        let Some(record) = meta(db, &record_key(&row.scope_intent_id)?)? else {
            gaps.push("active_scope_record_missing".to_owned());
            continue;
        };
        if verify_record(&record, &row.scope_intent_id).is_err()
            || record["state"] != "active"
            || record["task_id"] != task_id
            || record["task_revision"] != task_revision
            || record["attempt_id"] != attempt_id
        {
            gaps.push("active_scope_index_mismatch".to_owned());
            continue;
        }
        if record["expires_at_ms"]
            .as_i64()
            .is_some_and(|expires| expires <= now)
        {
            // Expiry makes an advisory scope unusable, but it does not prove
            // the retained active writer was explicitly released or otherwise
            // disposed. Omit the stale scope revision and fail coverage closed.
            gaps.push("active_scope_expired_without_release".to_owned());
            continue;
        }
        items.push(scope_revision_ref(&record));
    }
    if more {
        gaps.push("active_scope_page_truncated".to_owned());
    }
    gaps.sort();
    gaps.dedup();
    Ok(json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "items":items,
        "coverage":if more || !gaps.is_empty() { "partial" } else { "complete" },
        "gaps":gaps,
    }))
}

/// Return the current accepted scope revisions relevant to one contract
/// proposal. This uses the same overlap relation as code-scope conflict reads;
/// an unclassifiable path pair keeps the coverage partial instead of being
/// treated as disjoint.
pub(crate) fn affected_scope_revisions(
    db: &Connection,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    affected: &Value,
) -> Result<Value> {
    let mut snapshot = current_scope_revisions(db, task_id, task_revision, attempt_id)?;
    if snapshot["coverage"] != "complete" {
        return Ok(snapshot);
    }

    model::fields(affected, &["paths", "symbols", "schemas"]).map_err(|_| {
        Error::new(
            "PROPOSAL_DAMAGED",
            "proposal affected scope contains unknown fields",
        )
    })?;
    let strings = |field: &str| -> Result<Vec<String>> {
        let items = affected
            .get(field)
            .and_then(Value::as_array)
            .ok_or_else(|| {
                Error::new(
                    "PROPOSAL_DAMAGED",
                    format!("proposal affected {field} is not an array"),
                )
            })?;
        items
            .iter()
            .map(|item| {
                item.as_str().map(str::to_owned).ok_or_else(|| {
                    Error::new(
                        "PROPOSAL_DAMAGED",
                        format!("proposal affected {field} contains a non-text item"),
                    )
                })
            })
            .collect()
    };
    let proposal_scope = json!({
        "mode":"exclusive_edit",
        "paths":strings("paths")?,
        "symbols":strings("symbols")?,
        "interfaces":strings("schemas")?,
    });
    let current_items = snapshot["items"].as_array().cloned().ok_or_else(|| {
        Error::new(
            "SCOPE_COVERAGE_INCOMPLETE",
            "current accepted-scope snapshot has no item list",
        )
    })?;
    let mut relevant = Vec::new();
    let mut overlap_unknown = false;
    for scope in current_items {
        let relation = overlap(&proposal_scope, &scope);
        if relation.class == "unknown" || !relation.unsupported_paths.is_empty() {
            overlap_unknown = true;
        }
        if relation.class != "none" {
            relevant.push(scope);
        }
    }
    relevant.sort_by(|left: &Value, right: &Value| {
        left["scope_intent_id"]
            .as_str()
            .cmp(&right["scope_intent_id"].as_str())
    });
    snapshot["items"] = json!(relevant);
    if overlap_unknown {
        snapshot["coverage"] = json!("partial");
        let mut gaps = snapshot["gaps"].as_array().cloned().unwrap_or_default();
        gaps.push(json!("affected_scope_overlap_unknown"));
        gaps.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        gaps.dedup();
        snapshot["gaps"] = json!(gaps);
    }
    Ok(snapshot)
}

fn propose(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::ProposeRequest,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    principal.require_participant()?;
    let authority = coordination::participant_authority_scope(tx, principal)?;
    let task_id = model::text(&authority["scope"], "task_id")?;
    let task_revision = authority["scope"]["task_revision"]
        .as_i64()
        .ok_or_else(|| damaged("participant scope has no Task revision"))?;
    let attempt_id = model::text(&authority["scope"], "attempt_id")?;
    if request.task_id != task_id
        || request
            .task_revision
            .is_some_and(|revision| revision != task_revision)
        || request.attempt_id != attempt_id
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "scope proposal must name the caller's exact current Task revision and Attempt",
        ));
    }
    let basis = authority
        .get("participation_basis")
        .cloned()
        .unwrap_or(Value::Null);
    let basis_kind = basis["kind"].as_str().unwrap_or("");
    if !matches!(basis_kind, "attempt_owner" | "producer_ref") {
        return Err(Error::new(
            "FORBIDDEN",
            "review-only Participant grants cannot propose implementation scope",
        ));
    }
    let actual_assignment = basis["assignment_id"].as_str();
    if request
        .assignment_id
        .as_deref()
        .is_some_and(|provided| Some(provided) != actual_assignment)
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "assignment_id differs from the authenticated Attempt participation basis",
        ));
    }
    let scope_intent_id = format!("cscope-{}", model::new_id());
    let sequence = next_task_sequence(tx, task_id)?;
    let proposal = json!({
        "mode":request.mode.as_str(),
        "paths":request.paths,
        "symbols":request.symbols,
        "interfaces":request.interfaces,
        "baseline_candidate_ref":request.baseline_candidate_ref,
        "reason":request.reason,
        "suggested_expires_at_ms":request.suggested_expires_at_ms,
    });
    let mut record = json!({
        "schema":SCHEMA,
        "scope_intent_id":scope_intent_id,
        "state":"proposed",
        "state_revision":1,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "assignment_id":actual_assignment,
        "actor":authority["actor"],
        "participation_basis":basis,
        "registration_fingerprint":authority["registration_fingerprint"],
        "proposal":proposal,
        "accepted":Value::Null,
        "expires_at_ms":Value::Null,
        "override_scope_intent_ids":[],
        "coordination_required_with":[],
        "overridden_by_scope_id":Value::Null,
        "created_at_ms":now,
        "updated_at_ms":now,
        "list_sequence":sequence,
        "source_operation_id":operation_id,
    });
    let digest = record_digest(&record)?;
    record["digest"] = json!(digest);
    persist_revision(tx, &record, operation_id, now)?;
    set_meta(tx, &record_key(&scope_intent_id)?, &record)?;
    set_meta(
        tx,
        &task_page_key(task_id, sequence)?,
        &json!({"task_id":task_id,"scope_intent_id":scope_intent_id,"sequence":sequence}),
    )?;
    Ok(json!({
        "operation_id":operation_id,
        "scope_intent_id":scope_intent_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "state":"proposed",
        "state_revision":1,
        "digest":record["digest"],
        "changed":true,
        "advisory_only":true,
        "native_execution":false,
    }))
}

fn accept(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::AcceptRequest,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let mut record = load_record(tx, &request.scope_intent_id)?;
    require_revision(&record, request.expected_state_revision)?;
    if record["state"] != "proposed" || record["digest"] != request.proposal_digest {
        return Err(Error::new(
            "STALE_REVISION",
            "scope proposal state or digest changed before manager acceptance",
        ));
    }
    let task_id = model::text(&record, "task_id")?.to_owned();
    let task_revision = record["task_revision"]
        .as_i64()
        .ok_or_else(|| damaged("scope record has no Task revision"))?;
    let attempt_id = model::text(&record, "attempt_id")?.to_owned();
    require_current_scope_identity(tx, &record)?;
    coordination::concilium_manager_scope(tx, principal, &task_id, task_revision, &attempt_id)?;
    if request
        .expires_at_ms
        .is_some_and(|expires_at_ms| expires_at_ms <= now)
    {
        return Err(Error::invalid(
            "expires_at_ms must be later than the acceptance time",
        ));
    }
    let accepted_mode = request
        .mode
        .map(|mode| mode.as_str().to_owned())
        .unwrap_or_else(|| {
            record["proposal"]["mode"]
                .as_str()
                .unwrap_or("read_review")
                .to_owned()
        });
    let paths = request
        .paths
        .unwrap_or_else(|| string_array(&record["proposal"]["paths"]));
    let symbols = request
        .symbols
        .unwrap_or_else(|| string_array(&record["proposal"]["symbols"]));
    let interfaces = request
        .interfaces
        .unwrap_or_else(|| string_array(&record["proposal"]["interfaces"]));
    if paths.is_empty() && symbols.is_empty() && interfaces.is_empty() {
        return Err(Error::invalid(
            "accepted scope must name a path, symbol or interface",
        ));
    }
    if is_broad_scope(&paths) && !request.acknowledge_broad_scope {
        return Err(Error::new(
            "MANAGER_CONFIRMATION_REQUIRED",
            "broad repository scope requires acknowledge_broad_scope=true",
        ));
    }
    let accepted_scope = json!({
        "mode":accepted_mode,
        "paths":paths,
        "symbols":symbols,
        "interfaces":interfaces,
    });
    let active = active_records_complete(tx, &task_id, task_revision, &attempt_id)?;
    let live_active = active
        .iter()
        .filter(|other| {
            other["scope_intent_id"] != request.scope_intent_id
                && !is_expired(other, now)
                && other["state"] == "active"
        })
        .cloned()
        .collect::<Vec<_>>();
    let overridden = live_active
        .iter()
        .flat_map(|other| string_array(&other["override_scope_intent_ids"]))
        .collect::<BTreeSet<_>>();
    let mut required_overrides = BTreeSet::new();
    let mut coordination_required = BTreeSet::new();
    let mut unknown_overlap = false;
    for other in &live_active {
        let other_id = model::text(other, "scope_intent_id")?;
        if overridden.contains(other_id) {
            continue;
        }
        let overlap = overlap(&accepted_scope, &accepted_scope_from_record(other));
        match overlap.class {
            "conflict" => {
                required_overrides.insert(other_id.to_owned());
            }
            "coordination_required" => {
                coordination_required.insert(other_id.to_owned());
            }
            "unknown" => unknown_overlap = true,
            _ => {}
        }
    }
    if unknown_overlap {
        return Err(Error::new(
            "SCOPE_OVERLAP_UNKNOWN",
            "unsupported glob overlap prevents treating this acceptance as conflict-free",
        ));
    }
    let requested_overrides = request
        .override_scope_intent_ids
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    if requested_overrides != required_overrides {
        return Err(Error::new(
            "SCOPE_CONFLICT",
            "manager acceptance must name exactly the current exclusive-edit overlaps to override",
        ));
    }
    let mut changed_targets = Vec::new();
    for target_id in &requested_overrides {
        let mut target = load_record(tx, target_id)?;
        if target["state"] != "active"
            || target["task_id"] != task_id
            || target["task_revision"] != task_revision
            || target["attempt_id"] != attempt_id
            || is_expired(&target, now)
        {
            return Err(Error::new(
                "STALE_REVISION",
                "override target is no longer an active scope in this exact Attempt",
            ));
        }
        target["state"] = json!("overridden");
        target["overridden_by_scope_id"] = json!(request.scope_intent_id);
        target["override_reason"] = json!(request.reason);
        target["updated_at_ms"] = json!(now);
        let target_revision = next_revision(&target)?;
        target["state_revision"] = json!(target_revision);
        target["source_operation_id"] = json!(operation_id);
        let target_digest = record_digest(&target)?;
        target["digest"] = json!(target_digest);
        delete_active_index(tx, &target)?;
        persist_revision(tx, &target, operation_id, now)?;
        set_meta(tx, &record_key(target_id)?, &target)?;
        changed_targets.push(target_id.clone());
    }
    record["state"] = json!("active");
    let accepted_revision = next_revision(&record)?;
    record["state_revision"] = json!(accepted_revision);
    record["accepted"] = json!({
        "mode":accepted_scope["mode"],
        "paths":accepted_scope["paths"],
        "symbols":accepted_scope["symbols"],
        "interfaces":accepted_scope["interfaces"],
        "proposal_digest":request.proposal_digest,
        "manager_actor":{"client_id":principal.client_id,"role":role_name(&principal.role)},
        "reason":request.reason,
        "acknowledge_broad_scope":request.acknowledge_broad_scope,
        "override_scope_intent_ids":request.override_scope_intent_ids,
        "coordination_required_with":coordination_required,
        "accepted_at_ms":now,
        "accepted_by_operation_id":operation_id,
    });
    record["expires_at_ms"] = json!(request.expires_at_ms);
    record["override_scope_intent_ids"] = record["accepted"]["override_scope_intent_ids"].clone();
    record["coordination_required_with"] = record["accepted"]["coordination_required_with"].clone();
    record["updated_at_ms"] = json!(now);
    record["source_operation_id"] = json!(operation_id);
    let accepted_digest = record_digest(&record)?;
    record["digest"] = json!(accepted_digest);
    persist_revision(tx, &record, operation_id, now)?;
    set_meta(tx, &record_key(&request.scope_intent_id)?, &record)?;
    add_active_index(tx, &record)?;
    Ok(json!({
        "operation_id":operation_id,
        "scope_intent_id":request.scope_intent_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "state":"active",
        "state_revision":record["state_revision"],
        "digest":record["digest"],
        "proposal_digest":request.proposal_digest,
        "overridden_scope_intent_ids":changed_targets,
        "coordination_required_with":record["coordination_required_with"],
        "changed":true,
        "advisory_only":true,
        "native_execution":false,
    }))
}

fn release(
    tx: &Transaction<'_>,
    principal: &Principal,
    request: wire::ReleaseRequest,
    operation_id: &str,
    now: i64,
) -> Result<Value> {
    let mut record = load_record(tx, &request.scope_intent_id)?;
    require_revision(&record, request.expected_state_revision)?;
    let task_id = model::text(&record, "task_id")?.to_owned();
    let task_revision = record["task_revision"]
        .as_i64()
        .ok_or_else(|| damaged("scope record has no Task revision"))?;
    let attempt_id = model::text(&record, "attempt_id")?.to_owned();
    match &principal.role {
        Role::Participant => require_exact_scope_owner(tx, principal, &record)?,
        Role::Manager | Role::Operator => {
            coordination::concilium_manager_scope(
                tx,
                principal,
                &task_id,
                task_revision,
                &attempt_id,
            )?;
        }
        _ => {
            return Err(Error::new(
                "FORBIDDEN",
                "only the exact scope owner or current Manager may release this scope",
            ));
        }
    }
    if record["state"] != "active" {
        return Err(Error::new(
            "SCOPE_NOT_ACTIVE",
            "only an active accepted code scope can be released",
        ));
    }
    let previous_state = record["state"].clone();
    record["state"] = json!("released");
    record["release_reason"] = json!(request.reason);
    record["released_at_ms"] = json!(now);
    record["updated_at_ms"] = json!(now);
    let released_revision = next_revision(&record)?;
    record["state_revision"] = json!(released_revision);
    record["source_operation_id"] = json!(operation_id);
    let released_digest = record_digest(&record)?;
    record["digest"] = json!(released_digest);
    delete_active_index(tx, &record)?;
    persist_revision(tx, &record, operation_id, now)?;
    set_meta(tx, &record_key(&request.scope_intent_id)?, &record)?;
    let readback = load_record(tx, &request.scope_intent_id)?;
    let verified = readback["state"] == "released"
        && readback["state_revision"] == record["state_revision"]
        && readback["digest"] == record["digest"];
    if !verified {
        return Err(Error::new(
            "STORE_INVARIANT",
            "released scope current readback does not match its retained revision",
        ));
    }
    Ok(json!({
        "operation_id":operation_id,
        "scope_intent_id":request.scope_intent_id,
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "previous_state":previous_state,
        "current_state":"released",
        "current_revision":record["state_revision"],
        "state_revision":record["state_revision"],
        "digest":record["digest"],
        "readback_verified":verified,
        "changed":true,
        "advisory_only":true,
        "native_execution":false,
    }))
}

fn authorize_read_scope(
    db: &Connection,
    principal: &Principal,
    task_id: &str,
    requested_revision: Option<i64>,
    requested_attempt: Option<&str>,
) -> Result<(i64, String)> {
    match &principal.role {
        Role::Participant => {
            let scope = coordination::participant_authority_scope(db, principal)?;
            let current_task = model::text(&scope["scope"], "task_id")?;
            let current_revision = scope["scope"]["task_revision"]
                .as_i64()
                .ok_or_else(|| damaged("participant scope has no Task revision"))?;
            let current_attempt = model::text(&scope["scope"], "attempt_id")?;
            if current_task != task_id
                || requested_revision.is_some_and(|revision| revision != current_revision)
                || requested_attempt.is_some_and(|attempt| attempt != current_attempt)
            {
                return Err(Error::new(
                    "FORBIDDEN",
                    "Participant may inspect only the exact current Task and Attempt",
                ));
            }
            Ok((current_revision, current_attempt.to_owned()))
        }
        Role::Manager | Role::Operator => {
            let task = tasks::get_task(db, task_id)?;
            let attempt_id = requested_attempt
                .map(str::to_owned)
                .or_else(|| task["current_attempt_id"].as_str().map(str::to_owned))
                .ok_or_else(|| Error::new("NOT_FOUND", "Task has no current Attempt"))?;
            let attempt = tasks::get_attempt(db, &attempt_id)?;
            let task_revision = requested_revision
                .or_else(|| attempt["task_revision"].as_i64())
                .ok_or_else(|| damaged("Attempt has no Task revision"))?;
            if attempt["task_id"] != task_id || attempt["task_revision"] != task_revision {
                return Err(Error::new(
                    "SCOPE_MISMATCH",
                    "requested code-scope read differs from the retained Attempt identity",
                ));
            }
            match &principal.role {
                Role::Operator => super::require_local_operator(db, &principal.client_id)?,
                Role::Manager if attempt["owner_id"] == principal.client_id => {}
                Role::Manager => super::gm::require_authority(db, principal)?,
                _ => unreachable!("role branch is restricted above"),
            }
            Ok((task_revision, attempt_id))
        }
        _ => Err(Error::new(
            "FORBIDDEN",
            "code-scope inspection requires exact Task authority",
        )),
    }
}

fn require_current_scope_identity(db: &Connection, record: &Value) -> Result<()> {
    let client_id = model::text(&record["actor"], "client_id")?;
    let task_id = model::text(record, "task_id")?;
    let task_revision = record["task_revision"]
        .as_i64()
        .ok_or_else(|| damaged("scope record has no Task revision"))?;
    let attempt_id = model::text(record, "attempt_id")?;
    let current = coordination::concilium_participant_scope_for_client(
        db,
        client_id,
        task_id,
        task_revision,
        attempt_id,
    )?;
    if current["actor"] != record["actor"]
        || current["participation_basis"] != record["participation_basis"]
        || current["registration_fingerprint"] != record["registration_fingerprint"]
    {
        return Err(Error::new(
            "STALE_PARTICIPANT",
            "scope owner registration no longer matches the retained exact identity",
        ));
    }
    Ok(())
}

fn require_exact_scope_owner(db: &Connection, principal: &Principal, record: &Value) -> Result<()> {
    principal.require_participant()?;
    if record["actor"]["client_id"] != principal.client_id {
        return Err(Error::new(
            "FORBIDDEN",
            "only the exact code-scope owner or current Manager may release this scope",
        ));
    }
    require_current_scope_identity(db, record)
}

fn matches_read_terms(record: &Value, request: &wire::ReadRequest) -> bool {
    let effective = accepted_scope_from_record(record);
    request.path.as_deref().is_none_or(|path| {
        string_array(&effective["paths"])
            .iter()
            .any(|item| path_overlap(item, path).0 != Some(false))
    }) && request.symbol.as_deref().is_none_or(|symbol| {
        string_array(&effective["symbols"])
            .iter()
            .any(|item| item == symbol)
    }) && request.interface.as_deref().is_none_or(|interface| {
        string_array(&effective["interfaces"])
            .iter()
            .any(|item| item == interface)
    })
}

fn project_record(
    record: &Value,
    current_task_revision: i64,
    current_attempt_id: &str,
    now: i64,
) -> Result<Value> {
    verify_record(record, model::text(record, "scope_intent_id")?)?;
    let effective_state = if record["state"] == "active"
        && (record["task_revision"] != current_task_revision
            || record["attempt_id"] != current_attempt_id
            || is_expired(record, now))
    {
        "stale"
    } else {
        record["state"].as_str().unwrap_or("unknown")
    };
    let accepted = &record["accepted"];
    let proposed = &record["proposal"];
    let scope = if accepted.is_object() {
        accepted
    } else {
        proposed
    };
    Ok(json!({
        "scope_intent_id":record["scope_intent_id"],
        "state":record["state"],
        "effective_state":effective_state,
        "state_revision":record["state_revision"],
        "digest":record["digest"],
        "task_id":record["task_id"],
        "task_revision":record["task_revision"],
        "attempt_id":record["attempt_id"],
        "assignment_id":record["assignment_id"],
        "actor":record["actor"],
        "participation_basis":record["participation_basis"],
        "registration_fingerprint":record["registration_fingerprint"],
        "mode":scope["mode"],
        "paths":scope["paths"],
        "symbols":scope["symbols"],
        "interfaces":scope["interfaces"],
        "baseline_candidate_ref":proposed["baseline_candidate_ref"],
        "proposal_digest":if accepted.is_object() { accepted["proposal_digest"].clone() } else { record["digest"].clone() },
        "reason":if accepted.is_object() { accepted["reason"].clone() } else { proposed["reason"].clone() },
        "expires_at_ms":record["expires_at_ms"],
        "created_at_ms":record["created_at_ms"],
        "updated_at_ms":record["updated_at_ms"],
        "accepted":record["accepted"],
        "override_scope_intent_ids":record["override_scope_intent_ids"],
        "overridden_by_scope_id":record["overridden_by_scope_id"],
        "coordination_required_with":record["coordination_required_with"],
        "stale_reason":if effective_state == "stale" { json!(if is_expired(record, now) { "expired" } else { "attempt_or_task_changed" }) } else { Value::Null },
        "advisory_only":true,
        "native_execution":false,
    }))
}

fn conflicts_for(record: &Value, active: &[Value]) -> Vec<Value> {
    let scope = accepted_scope_from_record(record);
    let mut findings = active
        .iter()
        .filter(|other| other["scope_intent_id"] != record["scope_intent_id"])
        .filter_map(|other| {
            let overlap = overlap(&scope, &accepted_scope_from_record(other));
            (overlap.class != "none").then(|| {
                json!({
                    "scope_intent_id":other["scope_intent_id"],
                    "state_revision":other["state_revision"],
                    "digest":other["digest"],
                    "actor":other["actor"],
                    "classification":overlap.class,
                    "paths":overlap.paths,
                    "symbols":overlap.symbols,
                    "interfaces":overlap.interfaces,
                    "unsupported_paths":overlap
                        .unsupported_paths
                        .iter()
                        .map(UnsupportedPath::projection)
                        .collect::<Vec<_>>(),
                    "advisory_only":true,
                    "native_execution":false,
                })
            })
        })
        .collect::<Vec<_>>();
    findings.sort_by(|left, right| {
        left["scope_intent_id"]
            .as_str()
            .cmp(&right["scope_intent_id"].as_str())
    });
    findings
}

fn overlap(left: &Value, right: &Value) -> Overlap {
    let left_mode = left["mode"].as_str().unwrap_or("unknown");
    let right_mode = right["mode"].as_str().unwrap_or("unknown");
    let mut matched_paths = BTreeSet::new();
    let mut matched_symbols = BTreeSet::new();
    let mut matched_interfaces = BTreeSet::new();
    let mut unknown_path = false;
    let mut unsupported_paths = Vec::new();
    for left_path in string_array(&left["paths"]) {
        for right_path in string_array(&right["paths"]) {
            match path_overlap(&left_path, &right_path) {
                (Some(true), _) => {
                    matched_paths.insert(format!("{left_path} <> {right_path}"));
                }
                (None, _) => {
                    unknown_path = true;
                    let pair = UnsupportedPath::pair(&left_path, &right_path);
                    if !unsupported_paths.contains(&pair) {
                        unsupported_paths.push(pair);
                    }
                }
                _ => {}
            }
        }
    }
    for symbol in string_array(&left["symbols"]) {
        if string_array(&right["symbols"]).contains(&symbol) {
            matched_symbols.insert(symbol);
        }
    }
    for interface in string_array(&left["interfaces"]) {
        if string_array(&right["interfaces"]).contains(&interface) {
            matched_interfaces.insert(interface);
        }
    }
    let matched =
        !matched_paths.is_empty() || !matched_symbols.is_empty() || !matched_interfaces.is_empty();
    // Classification describes only the pairs that were classified. Keep
    // unsupported pairs alongside it so uncertainty cannot erase a known
    // conflict or imply complete path coverage.
    let class = if matched {
        if left_mode == "read_review" || right_mode == "read_review" {
            "informational"
        } else if left_mode == "exclusive_edit" || right_mode == "exclusive_edit" {
            "conflict"
        } else {
            "coordination_required"
        }
    } else if unknown_path {
        "unknown"
    } else {
        "none"
    };
    Overlap {
        class,
        paths: matched_paths.into_iter().collect(),
        symbols: matched_symbols.into_iter().collect(),
        interfaces: matched_interfaces.into_iter().collect(),
        unsupported_paths,
    }
}

/// Returns `None` when a glob form cannot be classified safely.
fn path_overlap(left: &str, right: &str) -> (Option<bool>, &'static str) {
    if left == "**/*" || left == "**" || right == "**/*" || right == "**" {
        return (Some(true), "broad_glob");
    }
    if left == right {
        return (Some(true), "exact");
    }
    let left_prefix = terminal_glob_prefix(left);
    let right_prefix = terminal_glob_prefix(right);
    if let Some(prefix) = left_prefix
        && (right_prefix.is_some_and(|other| prefixes_overlap(prefix, other))
            || (right_prefix.is_none() && right.starts_with(&format!("{prefix}/"))))
    {
        return (Some(true), "prefix_glob");
    }
    if let Some(prefix) = right_prefix
        && left_prefix.is_none()
        && left.starts_with(&format!("{prefix}/"))
    {
        return (Some(true), "prefix_glob");
    }
    if has_unsupported_glob(left) || has_unsupported_glob(right) {
        return (None, "unsupported_glob");
    }
    (Some(false), "disjoint")
}

fn terminal_glob_prefix(path: &str) -> Option<&str> {
    path.strip_suffix("/**").filter(|prefix| !prefix.is_empty())
}

fn prefixes_overlap(left: &str, right: &str) -> bool {
    left == right
        || left.starts_with(&format!("{right}/"))
        || right.starts_with(&format!("{left}/"))
}

fn has_unsupported_glob(path: &str) -> bool {
    path.contains('*') || path.contains('?') || path.contains('[') || path.contains('{')
}

fn is_broad_scope(paths: &[String]) -> bool {
    paths.iter().any(|path| path == "**/*" || path == "**")
}

fn accepted_scope_from_record(record: &Value) -> Value {
    if record["accepted"].is_object() {
        json!({
            "mode":record["accepted"]["mode"],
            "paths":record["accepted"]["paths"],
            "symbols":record["accepted"]["symbols"],
            "interfaces":record["accepted"]["interfaces"],
        })
    } else {
        json!({
            "mode":record["proposal"]["mode"],
            "paths":record["proposal"]["paths"],
            "symbols":record["proposal"]["symbols"],
            "interfaces":record["proposal"]["interfaces"],
        })
    }
}

fn active_records_complete(
    db: &Connection,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
) -> Result<Vec<Value>> {
    let prefix = active_prefix(task_id, task_revision, attempt_id)?;
    let mut cursor: Option<String> = None;
    let mut records = Vec::new();
    loop {
        let (rows, more) =
            active_index_page(db, &prefix, cursor.as_deref(), limits::MAX_READ_PAGE_SIZE)?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let record = load_record(db, &row.scope_intent_id)?;
            if record["state"] == "active"
                && record["task_id"] == task_id
                && record["task_revision"] == task_revision
                && record["attempt_id"] == attempt_id
            {
                records.push(record);
            }
        }
        if !more {
            break;
        }
        cursor = rows.last().map(|row| row.key.clone());
    }
    Ok(records)
}

fn active_records_page(
    db: &Connection,
    task_id: &str,
    task_revision: i64,
    attempt_id: &str,
    limit: i64,
) -> Result<(Vec<Value>, bool)> {
    let prefix = active_prefix(task_id, task_revision, attempt_id)?;
    let (rows, more) = active_index_page(db, &prefix, None, limit)?;
    let now = model::now_ms()?;
    let mut records = Vec::new();
    for row in rows {
        let record = load_record(db, &row.scope_intent_id)?;
        if record["state"] == "active"
            && record["task_id"] == task_id
            && record["task_revision"] == task_revision
            && record["attempt_id"] == attempt_id
            && !is_expired(&record, now)
        {
            records.push(record);
        }
    }
    Ok((records, more))
}

fn task_index_page(
    db: &Connection,
    prefix: &str,
    after_sequence: Option<i64>,
    limit: i64,
) -> Result<Vec<IndexRow>> {
    let lower = after_sequence
        .map(|sequence| format!("{}{:020}", prefix, sequence))
        .unwrap_or_else(|| prefix.to_owned());
    let compare = if after_sequence.is_some() { ">" } else { ">=" };
    let upper = format!("{prefix}g");
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {compare} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![lower, upper, limit.saturating_add(1)], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    rows.into_iter()
        .map(|(key, raw)| {
            let index: Value = serde_json::from_str(&raw)?;
            let sequence = index["sequence"]
                .as_i64()
                .ok_or_else(|| damaged("Task scope index has no sequence"))?;
            let scope_intent_id = model::text(&index, "scope_intent_id")?.to_owned();
            if key != format!("{prefix}{sequence:020}") {
                return Err(damaged("Task scope index key differs from its sequence"));
            }
            Ok(IndexRow {
                key,
                sequence,
                scope_intent_id,
            })
        })
        .collect()
}

fn active_index_page(
    db: &Connection,
    prefix: &str,
    after_key: Option<&str>,
    limit: i64,
) -> Result<(Vec<IndexRow>, bool)> {
    let lower = after_key.unwrap_or(prefix);
    let compare = if after_key.is_some() { ">" } else { ">=" };
    let upper = format!("{prefix}g");
    let sql = format!(
        "SELECT key,value_json FROM meta WHERE key {compare} ?1 AND key < ?2 ORDER BY key LIMIT ?3"
    );
    let mut statement = db.prepare(&sql)?;
    let rows: Vec<(String, String)> = statement
        .query_map(params![lower, upper, limit.saturating_add(1)], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    let has_more = rows.len() as i64 > limit;
    let parsed = rows
        .into_iter()
        .take(limit as usize)
        .map(|(key, raw)| {
            let index: Value = serde_json::from_str(&raw)?;
            let scope_intent_id = model::text(&index, "scope_intent_id")?.to_owned();
            if key != active_key(prefix, &scope_intent_id) {
                return Err(damaged("active scope index key differs from its record"));
            }
            Ok(IndexRow {
                key,
                sequence: 0,
                scope_intent_id,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((parsed, has_more))
}

fn cursor_sequence(db: &Connection, scope_intent_id: &str, task_id: &str) -> Result<i64> {
    let record = load_record(db, scope_intent_id)?;
    if record["task_id"] != task_id {
        return Err(Error::invalid(
            "after_scope_id must belong to the requested Task",
        ));
    }
    record["list_sequence"]
        .as_i64()
        .ok_or_else(|| damaged("scope record has no list sequence"))
}

fn next_task_sequence(tx: &Transaction<'_>, task_id: &str) -> Result<i64> {
    let key = task_sequence_key(task_id)?;
    let previous = match meta(tx, &key)? {
        None => 0,
        Some(value) => value
            .as_i64()
            .filter(|sequence| *sequence >= 0)
            .ok_or_else(|| damaged("Task scope sequence is invalid"))?,
    };
    let next = previous
        .checked_add(1)
        .ok_or_else(|| Error::new("STORE_INVARIANT", "Task scope sequence overflow"))?;
    set_meta(tx, &key, &json!(next))?;
    Ok(next)
}

fn add_active_index(tx: &Transaction<'_>, record: &Value) -> Result<()> {
    let prefix = active_prefix(
        model::text(record, "task_id")?,
        record["task_revision"]
            .as_i64()
            .ok_or_else(|| damaged("scope has no Task revision"))?,
        model::text(record, "attempt_id")?,
    )?;
    let scope_id = model::text(record, "scope_intent_id")?;
    set_meta(
        tx,
        &active_key(&prefix, scope_id),
        &json!({"scope_intent_id":scope_id}),
    )
}

fn delete_active_index(tx: &Transaction<'_>, record: &Value) -> Result<()> {
    let prefix = active_prefix(
        model::text(record, "task_id")?,
        record["task_revision"]
            .as_i64()
            .ok_or_else(|| damaged("scope has no Task revision"))?,
        model::text(record, "attempt_id")?,
    )?;
    tx.execute(
        "DELETE FROM meta WHERE key=?1",
        [active_key(&prefix, model::text(record, "scope_intent_id")?)],
    )?;
    Ok(())
}

fn persist_revision(
    tx: &Transaction<'_>,
    record: &Value,
    operation_id: &str,
    now: i64,
) -> Result<()> {
    let scope_id = model::text(record, "scope_intent_id")?;
    verify_record(record, scope_id)?;
    let revision = record["state_revision"]
        .as_i64()
        .ok_or_else(|| damaged("scope record has no state revision"))?;
    let key = revision_key(scope_id, revision);
    if meta(tx, &key)?.is_some() {
        return Err(Error::new(
            "STORE_INVARIANT",
            "immutable scope revision key already exists",
        ));
    }
    set_meta(
        tx,
        &key,
        &json!({
            "schema":SCHEMA,
            "scope_intent_id":scope_id,
            "state_revision":revision,
            "digest":record["digest"],
            "operation_id":operation_id,
            "created_at_ms":now,
            "record":record,
        }),
    )
}

fn load_record(db: &Connection, scope_id: &str) -> Result<Value> {
    let record = meta(db, &record_key(scope_id)?)?
        .ok_or_else(|| Error::new("NOT_FOUND", "code-scope intent was not found"))?;
    verify_record(&record, scope_id)?;
    Ok(record)
}

fn verify_record(record: &Value, scope_id: &str) -> Result<()> {
    if record["schema"] != SCHEMA
        || record["scope_intent_id"] != scope_id
        || record["state_revision"]
            .as_i64()
            .is_none_or(|revision| revision <= 0)
        || record["digest"].as_str().is_none_or(|digest| {
            digest.len() != 64
                || !digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(damaged(
            "scope current record failed its identity or digest check",
        ));
    }
    if record_digest(record)? != record["digest"] {
        return Err(damaged(
            "scope current record digest does not bind its content",
        ));
    }
    Ok(())
}

fn require_revision(record: &Value, expected: i64) -> Result<()> {
    verify_record(record, model::text(record, "scope_intent_id")?)?;
    if record["state_revision"] != expected {
        return Err(Error::new(
            "STALE_REVISION",
            "scope state_revision changed before the requested decision",
        ));
    }
    Ok(())
}

fn next_revision(record: &Value) -> Result<i64> {
    record["state_revision"]
        .as_i64()
        .and_then(|revision| revision.checked_add(1))
        .ok_or_else(|| Error::new("STORE_INVARIANT", "scope state_revision overflow"))
}

fn record_digest(record: &Value) -> Result<String> {
    let mut basis = record.clone();
    if let Some(object) = basis.as_object_mut() {
        object.remove("digest");
    }
    Ok(model::digest(model::canonical(&basis)?.as_bytes()))
}

fn scope_revision_ref(record: &Value) -> Value {
    json!({
        "scope_intent_id":record["scope_intent_id"],
        "state_revision":record["state_revision"],
        "digest":record["digest"],
        "state":record["state"],
        "owner_client_id":record["actor"]["client_id"],
        "actor":record["actor"],
        "assignment_id":record["assignment_id"],
        "participation_basis":record["participation_basis"],
        "mode":record["accepted"]["mode"],
        "paths":record["accepted"]["paths"],
        "symbols":record["accepted"]["symbols"],
        "interfaces":record["accepted"]["interfaces"],
        "expires_at_ms":record["expires_at_ms"],
        "override_scope_intent_ids":record["override_scope_intent_ids"],
    })
}

fn scope_refs_unavailable(task_id: &str, task_revision: i64, attempt_id: &str, gap: &str) -> Value {
    json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
        "items":[],
        "coverage":"unavailable",
        "gaps":[gap],
    })
}

fn is_expired(record: &Value, now: i64) -> bool {
    record["expires_at_ms"]
        .as_i64()
        .is_some_and(|expires| expires <= now)
}

fn string_array(value: &Value) -> Vec<String> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn task_page_prefix(task_id: &str) -> Result<String> {
    Ok(format!(
        "{TASK_PAGE_PREFIX}{}:",
        model::digest(task_id.as_bytes())
    ))
}

fn task_sequence_key(task_id: &str) -> Result<String> {
    Ok(format!(
        "{TASK_SEQUENCE_PREFIX}{}",
        model::digest(task_id.as_bytes())
    ))
}

fn active_prefix(task_id: &str, task_revision: i64, attempt_id: &str) -> Result<String> {
    let scope = json!({
        "task_id":task_id,
        "task_revision":task_revision,
        "attempt_id":attempt_id,
    });
    Ok(format!(
        "{ACTIVE_PREFIX}{}:",
        model::digest(model::canonical(&scope)?.as_bytes())
    ))
}

fn active_key(prefix: &str, scope_id: &str) -> String {
    format!("{prefix}{scope_id}")
}

fn task_page_key(task_id: &str, sequence: i64) -> Result<String> {
    Ok(format!("{}{:020}", task_page_prefix(task_id)?, sequence))
}

fn record_key(scope_id: &str) -> Result<String> {
    Ok(format!("{RECORD_PREFIX}{}", valid_scope_id(scope_id)?))
}

fn revision_key(scope_id: &str, revision: i64) -> String {
    format!("{REVISION_PREFIX}{scope_id}:{revision:020}")
}

fn valid_scope_id(scope_id: &str) -> Result<&str> {
    if scope_id
        .strip_prefix("cscope-")
        .and_then(|tail| uuid::Uuid::parse_str(tail).ok())
        .is_some()
    {
        Ok(scope_id)
    } else {
        Err(Error::invalid(
            "scope_intent_id must be an exact cscope UUID",
        ))
    }
}

fn role_name(role: &Role) -> &'static str {
    match role {
        Role::Operator => "operator",
        Role::Manager => "manager",
        Role::HookSource => "hook_source",
        Role::Participant => "participant",
        Role::Observer => "observer",
        Role::Module => "module",
        Role::ModuleSupervisor => "module_supervisor",
        Role::Scheduler => "scheduler",
    }
}

fn damaged(message: &str) -> Error {
    Error::new("STORE_INVARIANT", message)
}
