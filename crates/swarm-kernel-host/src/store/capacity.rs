//! Durable capacity accounting (Documentation Program §20, R23) and the
//! manager-facing capacity / attention projections (§6 and §8.3, R12).
//!
//! ## Accounting
//!
//! Every native admission on a binding — an `agent.*` command, a
//! `task.dispatch`, and each producer registered on an Attempt — holds a
//! reservation in a per-scope ledger. The ledger is a durable record in
//! the Store's `meta` table under `capacity:<scope_key>`: one entry per
//! admission, keyed by the operation (or producer assignment) identity,
//! with the phase the recorded evidence supports:
//!
//! - `reserved` — admitted (locally queued, sent, or natively admitted)
//!   but no execution-start evidence exists yet. Pending admissions are
//!   counted here, before any running status: admission is not execution.
//! - `active` — execution start is evidenced: an exact execution-log
//!   proof (`native_refs.input_execution`) names the started run, an
//!   outcome recorded a native turn, or the linked producer carries a
//!   native run ID. The entry names that evidence as
//!   `execution_start_ref`; the accounting never invents it.
//! - `released` — terminal: the execution proof or producer disposition
//!   reached a terminal outcome, the operation was rejected/cancelled or
//!   settled at its own contract boundary, or the owning Attempt was
//!   resolved/released. A release is final; later syncs never resurrect
//!   an entry.
//!
//! An operation in `outcome_unknown` keeps its phase and gains
//! `outcome_unknown_since_ms`: an unknown outcome is not a terminal and
//! frees nothing. Entries are re-derived from the durable operation /
//! attempt / producer rows at every lifecycle transition of those rows
//! (admission, outcome, observation evidence, cancellation, release), so
//! the ledger is a materialized record of facts, never a second opinion
//! about them. Execution disposition, Task acceptance and reservation
//! state remain three different facts.
//!
//! A scope is the provider/account/service a binding's recorded route
//! names. Scope identity is `complete` only when the route records both
//! a runtime and a service identity; otherwise it is `partial`, the
//! scope is keyed by its binding, and no capacity claim is ever made for
//! it: an unknown roster does not become an instruction to start
//! another writer.
//!
//! ## Quota incidents
//!
//! When a native outcome rejects work with a quota/rate error code, the
//! scope gains one open incident in the `incidents` table (deduplicated
//! per scope) carrying the native scope and whatever reset evidence the
//! native outcome recorded. A later applied/accepted outcome for the
//! scope resolves it. Recording or resolving an incident writes only
//! the incident and ledger records — there is deliberately no path from
//! here to owner-selected configuration.
//!
//! ## Projections
//!
//! `report.capacity` and `report.attention` are read-only projections
//! over the ledger, the retained binding observations and the operation
//! / attempt rows. They mutate nothing, call no native runtime, and
//! suggest addressed operations without performing them. Both use the
//! §8.1 projection frame (see `super::projection`): oversized scope or
//! attention items become explicit gap references, never silent
//! truncations.

use super::{meta, operations, set_meta, tasks};
use crate::{error::Result, model, runtime::RuntimeOutcome};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::BTreeMap;

/// A binding observation older than this is stale for attention
/// purposes. The same reporting bound the doctor applies to recorded
/// snapshots; a reporting threshold only — nothing here refreshes an
/// observation or calls a native service.
pub(super) const STALE_AFTER_MS: i64 = 900_000;

/// Methods whose operations hold native capacity on a binding while
/// unresolved. Checks carry their own resource accounting and mailbox
/// deliveries are not native work; neither appears here.
const ADMISSION_METHODS: &[&str] = &[
    "agent.open",
    "task.dispatch",
    "agent.send",
    "agent.reply",
    "agent.configure",
    "agent.goal",
    "agent.refresh",
    "agent.reconcile",
    "agent.result",
    "agent.recover",
];

/// Native error codes that evidence quota/rate exhaustion of a scope.
/// Any other code a native outcome reports is not a quota incident.
const QUOTA_ERROR_CODES: &[&str] = &[
    "QUOTA_EXCEEDED",
    "QUOTA",
    "RATE_LIMITED",
    "RATE_LIMIT",
    "USAGE_LIMIT",
    "USAGE_LIMIT_REACHED",
    "TOO_MANY_REQUESTS",
    "HTTP_429",
];

fn is_admission(method: &str) -> bool {
    ADMISSION_METHODS.contains(&method)
}

fn terminal_disposition(value: &Value) -> Option<&str> {
    value
        .as_str()
        .filter(|s| matches!(*s, "completed" | "failed" | "cancelled"))
}

// ---------------------------------------------------------------------------
// Scope facts
// ---------------------------------------------------------------------------

/// Derives the capacity scope from a binding's recorded route facts.
/// Values the route does not record stay `null`: an account or service
/// is never inferred from a lane name or a model string's shape beyond
/// the provider prefix the recorded model ID itself carries.
pub(super) fn scope_facts(
    route: &Value,
    native_scope_key: Option<&str>,
    binding_id: &str,
) -> Value {
    let options = &route["native_options"];
    let runtime = route["runtime"].as_str().filter(|s| !s.is_empty());
    let service = options["service_id"].as_str().filter(|s| !s.is_empty());
    let provider = options["model"]["providerID"]
        .as_str()
        .or_else(|| options["model"]["provider_id"].as_str())
        .or_else(|| options["provider"].as_str())
        .or_else(|| {
            options["model"]
                .as_str()
                .and_then(|m| m.split_once('/').map(|(p, _)| p))
        })
        .filter(|s| !s.is_empty());
    let account = options["account"]
        .as_str()
        .or_else(|| options["account_id"].as_str())
        .filter(|s| !s.is_empty());
    let identity = if runtime.is_some() && service.is_some() {
        "complete"
    } else {
        "partial"
    };
    let scope_key = match (runtime, service) {
        (Some(runtime), Some(service)) => format!("{runtime}:{service}"),
        (Some(runtime), None) => format!("{runtime}:binding:{binding_id}"),
        (None, _) => format!("unknown:binding:{binding_id}"),
    };
    json!({
        "scope_key": scope_key,
        "runtime": runtime,
        "provider": provider,
        "account": account,
        "service": service,
        "native_scope_key": native_scope_key,
        "route_alias": route["alias"],
        "identity": identity,
    })
}

// ---------------------------------------------------------------------------
// Ledger storage
// ---------------------------------------------------------------------------

fn ledger_key(scope_key: &str) -> String {
    format!("capacity:{scope_key}")
}

fn load_ledger(db: &Connection, scope_key: &str) -> Result<Value> {
    Ok(match meta(db, &ledger_key(scope_key))? {
        Some(mut ledger) if ledger.is_object() => {
            if !ledger["entries"].is_object() {
                ledger["entries"] = json!({});
            }
            ledger
        }
        _ => json!({"scope": null, "entries": {}, "updated_at_ms": 0}),
    })
}

fn store_ledger(db: &Connection, scope_key: &str, ledger: &Value) -> Result<()> {
    set_meta(db, &ledger_key(scope_key), ledger)
}

fn all_ledgers(db: &Connection) -> Result<Vec<(String, Value)>> {
    let mut stmt = db.prepare("SELECT key,value_json FROM meta WHERE key LIKE 'capacity:%'")?;
    let rows = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    rows.into_iter()
        .map(|(key, raw)| Ok((key, serde_json::from_str(&raw)?)))
        .collect()
}

// ---------------------------------------------------------------------------
// Phase derivation
// ---------------------------------------------------------------------------

struct Derived {
    phase: &'static str,
    start_ref: Value,
    release_reason: Option<String>,
    unknown_since: Option<i64>,
}

fn released(reason: String) -> Derived {
    Derived {
        phase: "released",
        start_ref: Value::Null,
        release_reason: Some(reason),
        unknown_since: None,
    }
}

fn active(start_ref: Value, unknown: bool, op: &Value) -> Derived {
    Derived {
        phase: "active",
        start_ref,
        release_reason: None,
        unknown_since: unknown.then(|| op["updated_at_ms"].as_i64().unwrap_or(0)),
    }
}

fn reserved(unknown: bool, op: &Value) -> Derived {
    Derived {
        phase: "reserved",
        start_ref: Value::Null,
        release_reason: None,
        unknown_since: unknown.then(|| op["updated_at_ms"].as_i64().unwrap_or(0)),
    }
}

/// Finds the Attempt one operation is linked to and the producer the
/// operation started on it, if any.
fn linked_attempt(db: &Connection, op: &Value) -> Result<Option<(Value, Option<Value>)>> {
    let Some(attempt_id) = op["attempt_id"].as_str() else {
        return Ok(None);
    };
    let attempt = tasks::get_attempt(db, attempt_id)?;
    let producer = attempt["producers"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|p| p["assignment_id"] == op["operation_id"])
        .cloned();
    Ok(Some((attempt, producer)))
}

fn attempt_resolved(attempt: &Value) -> bool {
    !attempt["released_at_ms"].is_null()
        || matches!(
            attempt["state"].as_str(),
            Some("accepted" | "failed" | "cancelled" | "superseded")
        )
}

/// Derives one operation entry's phase from the durable rows alone:
/// the operation itself, its execution proof, its linked producer and
/// its Attempt. Nothing is inferred from absence, idle or age.
fn derive_operation(db: &Connection, op: &Value) -> Result<Derived> {
    let state = op["state"].as_str().unwrap_or_default();
    let unknown = state == "outcome_unknown";
    match state {
        "cancelled" => return Ok(released("operation_cancelled".into())),
        "rejected" => return Ok(released("operation_rejected".into())),
        _ => {}
    }
    // Exact execution-log proof recorded on the operation itself.
    let proof = &op["native_refs"]["input_execution"];
    if proof.is_object() {
        if let Some(terminal) = terminal_disposition(&proof["disposition"]) {
            return Ok(released(format!("execution_terminal:{terminal}")));
        }
        if !proof["execution_started"].is_null() || proof["native_run_id"].is_string() {
            return Ok(active(
                json!({
                    "kind": "execution_started_event",
                    "event": proof["execution_started"],
                    "native_run_id": proof["native_run_id"],
                    "native_session_id": proof["native_session_id"],
                    "native_input_id": proof["native_input_id"],
                }),
                unknown,
                op,
            ));
        }
        // A proof that shows admission/delivery only: still reserved.
    }
    // Producer evidence on the linked Attempt (the dispatch path).
    let linked = linked_attempt(db, op)?;
    if let Some((_, Some(producer))) = &linked {
        if let Some(terminal) = terminal_disposition(&producer["disposition"])
            .or_else(|| terminal_disposition(&producer["execution_disposition"]))
        {
            return Ok(released(format!("execution_terminal:{terminal}")));
        }
        if producer["native_run_id"].is_string() {
            return Ok(active(
                json!({
                    "kind": "native_turn",
                    "turn_id": producer["native_run_id"],
                    "native_session_id": producer["native_session_id"],
                }),
                unknown,
                op,
            ));
        }
    }
    if op["method"] == "task.dispatch" {
        if let Some((attempt, _)) = &linked
            && attempt_resolved(attempt)
        {
            return Ok(released(format!(
                "attempt_resolved:{}",
                attempt["state"].as_str().unwrap_or("released")
            )));
        }
        // A settled dispatch settled at admission (its contract
        // boundary), not at execution end: the reservation stands
        // until execution or Attempt evidence resolves it. Queued,
        // sending and native_accepted dispatches are reserved by the
        // same rule.
        return Ok(reserved(unknown, op));
    }
    match state {
        "settled" => Ok(released("operation_settled".into())),
        _ => {
            // queued / sending / native_accepted / outcome_unknown.
            // A recorded native turn on the operation's own refs is
            // execution-start evidence for module runtimes.
            if op["native_refs"]["turn_id"].is_string() {
                return Ok(active(
                    json!({
                        "kind": "native_turn",
                        "turn_id": op["native_refs"]["turn_id"],
                        "native_session_id": op["native_refs"]["session_id"],
                    }),
                    unknown,
                    op,
                ));
            }
            Ok(reserved(unknown, op))
        }
    }
}

fn derive_producer(attempt: &Value, producer: &Value) -> Derived {
    if attempt_resolved(attempt) {
        return released(format!(
            "attempt_resolved:{}",
            attempt["state"].as_str().unwrap_or("released")
        ));
    }
    if let Some(terminal) = terminal_disposition(&producer["disposition"])
        .or_else(|| terminal_disposition(&producer["execution_disposition"]))
    {
        return released(format!("execution_terminal:{terminal}"));
    }
    if producer["native_run_id"].is_string() {
        return Derived {
            phase: "active",
            start_ref: json!({
                "kind": "native_turn",
                "turn_id": producer["native_run_id"],
                "native_session_id": producer["native_session_id"],
            }),
            release_reason: None,
            unknown_since: None,
        };
    }
    Derived {
        phase: "reserved",
        start_ref: Value::Null,
        release_reason: None,
        unknown_since: None,
    }
}

/// Merges a derived phase into a ledger entry. A release is final, the
/// first activation time and release facts are kept, and an entry never
/// moves backwards.
fn merge_entry(entry: &mut Value, derived: &Derived, now: i64) {
    if entry["phase"] == "released" {
        entry["last_synced_at_ms"] = json!(now);
        return;
    }
    match derived.phase {
        "active" => {
            if entry["phase"] != "active" {
                entry["activated_at_ms"] = json!(now);
            }
            entry["phase"] = json!("active");
            entry["execution_start_ref"] = derived.start_ref.clone();
        }
        "released" => {
            entry["phase"] = json!("released");
            entry["released_at_ms"] = json!(now);
            entry["release_reason"] = json!(derived.release_reason);
            entry["outcome_unknown_since_ms"] = Value::Null;
        }
        _ => {
            entry["phase"] = json!("reserved");
        }
    }
    if derived.phase != "released" {
        entry["outcome_unknown_since_ms"] = match derived.unknown_since {
            Some(since) => json!(since),
            None => Value::Null,
        };
    }
    entry["last_synced_at_ms"] = json!(now);
}

// ---------------------------------------------------------------------------
// Lifecycle sync — called from the transitions that change the rows
// ---------------------------------------------------------------------------

fn binding_scope(db: &Connection, binding_id: &str, generation: i64) -> Result<Option<Value>> {
    let binding = match operations::get_binding(db, binding_id, generation) {
        Ok(b) => b,
        Err(e) if e.code == "NOT_FOUND" => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(Some(scope_facts(
        &binding["route"],
        binding["native_scope_key"].as_str(),
        binding_id,
    )))
}

/// Re-derives the ledger entry for one operation from its durable row.
/// A no-op for operations that hold no native capacity (checks,
/// mailbox, unbound operations).
pub(super) fn sync_operation(db: &Connection, operation_id: &str, now: i64) -> Result<()> {
    let op = match operations::get_operation(db, operation_id) {
        Ok(op) => op,
        Err(e) if e.code == "NOT_FOUND" => return Ok(()),
        Err(e) => return Err(e),
    };
    if !is_admission(op["method"].as_str().unwrap_or_default()) {
        return Ok(());
    }
    let (Some(binding_id), Some(generation)) =
        (op["binding_id"].as_str(), op["binding_generation"].as_i64())
    else {
        return Ok(());
    };
    let Some(scope) = binding_scope(db, binding_id, generation)? else {
        return Ok(());
    };
    let scope_key = scope["scope_key"].as_str().unwrap_or_default().to_owned();
    let derived = derive_operation(db, &op)?;
    let mut ledger = load_ledger(db, &scope_key)?;
    ledger["scope"] = scope;
    let entry = ledger["entries"]
        .as_object_mut()
        .expect("entries normalized to an object")
        .entry(operation_id.to_owned())
        .or_insert_with(|| {
            json!({
                "entry_id": operation_id,
                "kind": "operation",
                "phase": "reserved",
                "admitted_at_ms": op["created_at_ms"],
                "activated_at_ms": null,
                "released_at_ms": null,
                "release_reason": null,
                "execution_start_ref": null,
                "outcome_unknown_since_ms": null,
            })
        });
    entry["method"] = op["method"].clone();
    entry["binding_id"] = json!(binding_id);
    entry["binding_generation"] = json!(generation);
    entry["task_id"] = op["task_id"].clone();
    entry["attempt_id"] = op["attempt_id"].clone();
    merge_entry(entry, &derived, now);
    ledger["updated_at_ms"] = json!(now);
    store_ledger(db, &scope_key, &ledger)
}

/// Re-derives the ledger entry for one producer registered on an
/// Attempt (native-manager-started work has no dispatch operation).
pub(super) fn sync_producer(
    db: &Connection,
    attempt: &Value,
    producer: &Value,
    now: i64,
) -> Result<()> {
    let (Some(binding_id), Some(generation)) = (
        attempt["binding_id"].as_str(),
        attempt["binding_generation"].as_i64(),
    ) else {
        return Ok(());
    };
    let Some(assignment) = producer["assignment_id"].as_str() else {
        return Ok(());
    };
    let Some(scope) = binding_scope(db, binding_id, generation)? else {
        return Ok(());
    };
    let scope_key = scope["scope_key"].as_str().unwrap_or_default().to_owned();
    let entry_id = format!(
        "producer:{}:{assignment}",
        attempt["attempt_id"].as_str().unwrap_or_default()
    );
    let derived = derive_producer(attempt, producer);
    let mut ledger = load_ledger(db, &scope_key)?;
    ledger["scope"] = scope;
    let entry = ledger["entries"]
        .as_object_mut()
        .expect("entries normalized to an object")
        .entry(entry_id.clone())
        .or_insert_with(|| {
            json!({
                "entry_id": entry_id,
                "kind": "producer",
                "phase": "reserved",
                "admitted_at_ms": attempt["created_at_ms"],
                "activated_at_ms": null,
                "released_at_ms": null,
                "release_reason": null,
                "execution_start_ref": null,
                "outcome_unknown_since_ms": null,
            })
        });
    entry["assignment_id"] = json!(assignment);
    entry["binding_id"] = json!(binding_id);
    entry["binding_generation"] = json!(generation);
    entry["task_id"] = attempt["task_id"].clone();
    entry["attempt_id"] = attempt["attempt_id"].clone();
    entry["native_session_id"] = producer["native_session_id"].clone();
    merge_entry(entry, &derived, now);
    ledger["updated_at_ms"] = json!(now);
    store_ledger(db, &scope_key, &ledger)
}

/// Re-derives every entry fed by one Attempt: its operations and its
/// registered producers.
pub(super) fn sync_attempt(db: &Connection, attempt_id: &str, now: i64) -> Result<()> {
    let attempt = match tasks::get_attempt(db, attempt_id) {
        Ok(a) => a,
        Err(e) if e.code == "NOT_FOUND" => return Ok(()),
        Err(e) => return Err(e),
    };
    let mut stmt = db.prepare("SELECT operation_id FROM operations WHERE attempt_id=?1")?;
    let op_ids = stmt
        .query_map([attempt_id], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    for op_id in op_ids {
        sync_operation(db, &op_id, now)?;
    }
    if let Some(producers) = attempt["producers"].as_array() {
        for producer in producers {
            sync_producer(db, &attempt, producer, now)?;
        }
    }
    Ok(())
}

/// Re-derives every entry on one binding generation (used when a
/// bridge transition rewrites many operation rows at once, e.g. the
/// outcome-unknown marking at module reconnect).
pub(super) fn sync_binding(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    now: i64,
) -> Result<()> {
    let mut stmt = db.prepare(
        "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2",
    )?;
    let op_ids = stmt
        .query_map(params![binding_id, generation], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    for op_id in op_ids {
        sync_operation(db, &op_id, now)?;
    }
    let mut stmt = db
        .prepare("SELECT attempt_id FROM attempts WHERE binding_id=?1 AND binding_generation=?2")?;
    let attempt_ids = stmt
        .query_map(params![binding_id, generation], |r| r.get::<_, String>(0))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    for attempt_id in attempt_ids {
        sync_attempt(db, &attempt_id, now)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Quota incidents
// ---------------------------------------------------------------------------

fn quota_code(details: &Value) -> Option<String> {
    let mut candidates: Vec<String> = Vec::new();
    for value in [
        &details["code"],
        &details["error_code"],
        &details["error"]["code"],
        &details["error"]["error_code"],
    ] {
        if let Some(code) = value.as_str() {
            candidates.push(code.to_uppercase());
        }
    }
    for value in [&details["status"], &details["error"]["status"]] {
        if value.as_i64() == Some(429) {
            candidates.push("HTTP_429".into());
        }
    }
    candidates
        .into_iter()
        .find(|code| QUOTA_ERROR_CODES.contains(&code.as_str()))
}

fn reset_evidence(details: &Value) -> Value {
    let mut evidence = serde_json::Map::new();
    for source in [details, &details["error"]] {
        for key in [
            "reset_at_ms",
            "resets_at_ms",
            "reset_after_ms",
            "retry_after_ms",
            "reset",
        ] {
            if let Some(value) = source.get(key).filter(|v| !v.is_null()) {
                evidence
                    .entry(key.to_owned())
                    .or_insert_with(|| value.clone());
            }
        }
    }
    if evidence.is_empty() {
        Value::Null
    } else {
        Value::Object(evidence)
    }
}

/// Records the capacity consequence of one native outcome on its
/// scope: a quota rejection opens (or re-counts) the scope's single
/// open quota incident; an applied/accepted outcome resolves it. Only
/// the incident row is written — owner configuration is never touched.
pub(super) fn note_outcome(
    db: &Connection,
    op: &Value,
    outcome: &RuntimeOutcome,
    now: i64,
) -> Result<()> {
    let (Some(binding_id), Some(generation)) =
        (op["binding_id"].as_str(), op["binding_generation"].as_i64())
    else {
        return Ok(());
    };
    let Some(scope) = binding_scope(db, binding_id, generation)? else {
        return Ok(());
    };
    let scope_key = scope["scope_key"].as_str().unwrap_or_default().to_owned();
    let dedup = format!("quota:{scope_key}");
    match outcome.outcome {
        crate::runtime::EffectOutcome::Rejected => {
            let Some(code) = quota_code(&outcome.details) else {
                return Ok(());
            };
            let existing: Option<(String, i64, String)> = db
                .query_row(
                    "SELECT incident_id,occurrences,details_json FROM incidents WHERE dedup_key=?1 AND state='open'",
                    [&dedup],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )
                .optional()?;
            if let Some((incident_id, occurrences, raw)) = existing {
                let mut details: Value = serde_json::from_str(&raw)?;
                details["last_operation_id"] = json!(outcome.operation_id);
                let reset = reset_evidence(&outcome.details);
                if !reset.is_null() {
                    details["reset_evidence"] = reset;
                }
                db.execute(
                    "UPDATE incidents SET occurrences=?2,last_seen_at_ms=?3,details_json=?4 WHERE incident_id=?1",
                    params![incident_id, occurrences + 1, now, model::canonical(&details)?],
                )?;
            } else {
                let details = json!({
                    "kind": "quota",
                    "scope": scope,
                    "native_scope_key": outcome.native_scope_key,
                    "error_code": code,
                    "reset_evidence": reset_evidence(&outcome.details),
                    "operation_id": outcome.operation_id,
                    "binding_id": binding_id,
                    "binding_generation": generation,
                });
                db.execute(
                    "INSERT INTO incidents(incident_id,dedup_key,state,occurrences,evidence_ref,action_operation_id,details_json,opened_at_ms,last_seen_at_ms) VALUES(?1,?2,'open',1,NULL,NULL,?3,?4,?4)",
                    params![model::new_id(), dedup, model::canonical(&details)?, now],
                )?;
            }
            Ok(())
        }
        crate::runtime::EffectOutcome::Applied | crate::runtime::EffectOutcome::Accepted => {
            let existing: Option<(String, String)> = db
                .query_row(
                    "SELECT incident_id,details_json FROM incidents WHERE dedup_key=?1 AND state='open'",
                    [&dedup],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            if let Some((incident_id, raw)) = existing {
                let mut details: Value = serde_json::from_str(&raw)?;
                details["resolved_at_ms"] = json!(now);
                details["resolution"] = json!("native_outcome_applied");
                details["resolved_by_operation_id"] = json!(outcome.operation_id);
                db.execute(
                    "UPDATE incidents SET state='resolved',details_json=?2 WHERE incident_id=?1",
                    params![incident_id, model::canonical(&details)?],
                )?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn open_quota_incident(db: &Connection, scope_key: &str) -> Result<Option<Value>> {
    let dedup = format!("quota:{scope_key}");
    let row: Option<(String, i64, String, i64, i64)> = db
        .query_row(
            "SELECT incident_id,occurrences,details_json,opened_at_ms,last_seen_at_ms FROM incidents WHERE dedup_key=?1 AND state='open'",
            [&dedup],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()?;
    match row {
        Some((id, occurrences, raw, opened, seen)) => {
            let details: Value = serde_json::from_str(&raw)?;
            Ok(Some(json!({
                "incident_id": id,
                "error_code": details["error_code"],
                "reset_evidence": details["reset_evidence"],
                "occurrences": occurrences,
                "opened_at_ms": opened,
                "last_seen_at_ms": seen,
                "first_operation_id": details["operation_id"],
                "last_operation_id": details["last_operation_id"],
            })))
        }
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Scope aggregation and roster verification
// ---------------------------------------------------------------------------

struct ScopeAgg {
    scope: Value,
    bindings: Vec<Value>,
    entries: Vec<Value>,
    ledger_updated_at_ms: i64,
}

fn collect_scopes(db: &Connection) -> Result<BTreeMap<String, ScopeAgg>> {
    let mut scopes: BTreeMap<String, ScopeAgg> = BTreeMap::new();
    let mut stmt =
        db.prepare("SELECT binding_id,generation FROM bindings ORDER BY binding_id,generation")?;
    let keys = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    for (binding_id, generation) in keys {
        let binding = operations::get_binding(db, &binding_id, generation)?;
        let scope = scope_facts(
            &binding["route"],
            binding["native_scope_key"].as_str(),
            &binding_id,
        );
        let key = scope["scope_key"].as_str().unwrap_or_default().to_owned();
        let agg = scopes.entry(key).or_insert_with(|| ScopeAgg {
            scope: scope.clone(),
            bindings: Vec::new(),
            entries: Vec::new(),
            ledger_updated_at_ms: 0,
        });
        if agg.scope["native_scope_key"].is_null() && !scope["native_scope_key"].is_null() {
            agg.scope["native_scope_key"] = scope["native_scope_key"].clone();
        }
        agg.bindings.push(binding);
    }
    for (key, ledger) in all_ledgers(db)? {
        let scope_key = key["capacity:".len()..].to_owned();
        let agg = scopes.entry(scope_key.clone()).or_insert_with(|| ScopeAgg {
            scope: if ledger["scope"].is_object() {
                ledger["scope"].clone()
            } else {
                json!({"scope_key": scope_key, "runtime": null, "provider": null,
                       "account": null, "service": null, "native_scope_key": null,
                       "route_alias": null, "identity": "partial"})
            },
            bindings: Vec::new(),
            entries: Vec::new(),
            ledger_updated_at_ms: 0,
        });
        agg.entries = ledger["entries"]
            .as_object()
            .map(|entries| entries.values().cloned().collect())
            .unwrap_or_default();
        agg.entries
            .sort_by(|a, b| a["entry_id"].as_str().cmp(&b["entry_id"].as_str()));
        agg.ledger_updated_at_ms = ledger["updated_at_ms"].as_i64().unwrap_or(0);
    }
    Ok(scopes)
}

fn is_writer_entry(entry: &Value) -> bool {
    entry["kind"] == "producer" || entry["method"] == "task.dispatch"
}

fn scope_counts(entries: &[Value]) -> Value {
    let mut counts = json!({
        "reserved": 0, "active": 0, "unknown_outcomes": 0,
        "desired_writers": 0, "effective_writers": 0, "pending_admissions": 0,
        "commands_in_flight": 0, "released_entries": 0,
    });
    for entry in entries {
        if entry["phase"] == "released" {
            counts["released_entries"] = json!(counts["released_entries"].as_i64().unwrap() + 1);
            continue;
        }
        match entry["phase"].as_str() {
            Some("reserved") => {
                counts["reserved"] = json!(counts["reserved"].as_i64().unwrap() + 1)
            }
            Some("active") => counts["active"] = json!(counts["active"].as_i64().unwrap() + 1),
            _ => {}
        }
        if !entry["outcome_unknown_since_ms"].is_null() {
            counts["unknown_outcomes"] = json!(counts["unknown_outcomes"].as_i64().unwrap() + 1);
        }
        if is_writer_entry(entry) {
            counts["desired_writers"] = json!(counts["desired_writers"].as_i64().unwrap() + 1);
            if entry["phase"] == "active" {
                counts["effective_writers"] =
                    json!(counts["effective_writers"].as_i64().unwrap() + 1);
            } else {
                counts["pending_admissions"] =
                    json!(counts["pending_admissions"].as_i64().unwrap() + 1);
            }
        } else {
            counts["commands_in_flight"] =
                json!(counts["commands_in_flight"].as_i64().unwrap() + 1);
        }
    }
    counts
}

/// Verifies the scope roster from recorded facts only. Returns
/// `(known, reason)`: the roster is known only when the scope identity
/// is complete, at least one binding is recorded, the durable ledger
/// agrees entry-for-entry with phases re-derived from the operation /
/// attempt rows, and no natively executing member of a binding's
/// retained family is unattributable to that family.
fn roster_check(db: &Connection, agg: &ScopeAgg) -> Result<(bool, Option<String>)> {
    if agg.scope["identity"] != "complete" {
        return Ok((false, Some("scope_identity_partial".into())));
    }
    if agg.bindings.is_empty() {
        return Ok((false, Some("no_recorded_binding".into())));
    }
    let ledger_phase = |entry_id: &str| -> Option<&str> {
        agg.entries
            .iter()
            .find(|e| e["entry_id"] == entry_id)
            .and_then(|e| e["phase"].as_str())
    };
    for binding in &agg.bindings {
        let (binding_id, generation) = (
            binding["binding_id"].as_str().unwrap_or_default(),
            binding["generation"].as_i64().unwrap_or_default(),
        );
        let mut stmt = db.prepare(
            "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2",
        )?;
        let op_ids = stmt
            .query_map(params![binding_id, generation], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        for op_id in op_ids {
            let op = operations::get_operation(db, &op_id)?;
            if !is_admission(op["method"].as_str().unwrap_or_default()) {
                continue;
            }
            let derived = derive_operation(db, &op)?;
            match ledger_phase(&op_id) {
                Some(phase) if phase == derived.phase => {}
                other => {
                    return Ok((
                        false,
                        Some(format!(
                            "ledger_diverged:{op_id}:{}:{}",
                            other.unwrap_or("missing"),
                            derived.phase
                        )),
                    ));
                }
            }
        }
        let mut stmt = db.prepare(
            "SELECT attempt_id FROM attempts WHERE binding_id=?1 AND binding_generation=?2",
        )?;
        let attempt_ids = stmt
            .query_map(params![binding_id, generation], |r| r.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        for attempt_id in attempt_ids {
            let attempt = tasks::get_attempt(db, &attempt_id)?;
            if let Some(producers) = attempt["producers"].as_array() {
                for producer in producers {
                    let Some(assignment) = producer["assignment_id"].as_str() else {
                        continue;
                    };
                    let entry_id = format!("producer:{attempt_id}:{assignment}");
                    let derived = derive_producer(&attempt, producer);
                    match ledger_phase(&entry_id) {
                        Some(phase) if phase == derived.phase => {}
                        other => {
                            return Ok((
                                false,
                                Some(format!(
                                    "ledger_diverged:{entry_id}:{}:{}",
                                    other.unwrap_or("missing"),
                                    derived.phase
                                )),
                            ));
                        }
                    }
                }
            }
        }
        // Native roster: an executing member whose parent chain leaves
        // the retained family is activity this scope cannot attribute.
        let native = &binding["observation"]["native"];
        if native.is_object() {
            let mut family: Vec<&str> = vec![native["native_root_id"].as_str().unwrap_or_default()];
            if let Some(children) = native["observed_children"].as_array() {
                for child in children {
                    if let Some(id) = child["sessionId"].as_str() {
                        family.push(id);
                    }
                }
                for child in children {
                    if child["execution_disposition"] == "running"
                        && child["observed_now"] == true
                        && let Some(parent) = child["parentSessionId"].as_str()
                        && !family.contains(&parent)
                    {
                        return Ok((
                            false,
                            Some(format!(
                                "unattributed_native_activity:{}",
                                child["sessionId"].as_str().unwrap_or_default()
                            )),
                        ));
                    }
                }
            }
        }
    }
    Ok((true, None))
}

fn new_work_enabled(db: &Connection) -> Result<bool> {
    Ok(meta(db, "execution_mode")?.unwrap_or(Value::Null)["new_work"] == "enabled")
}

/// Computes one capacity item per scope from the ledger and the
/// recorded bindings. Shared by `report.capacity`, `report.attention`
/// (the `capacity_available` items) and the doctor projection.
pub(crate) fn capacity_items(db: &Connection) -> Result<Vec<Value>> {
    let scopes = collect_scopes(db)?;
    let new_work = new_work_enabled(db)?;
    let mut items = Vec::new();
    for (_, agg) in scopes {
        let scope_key = agg.scope["scope_key"]
            .as_str()
            .unwrap_or_default()
            .to_owned();
        let counts = scope_counts(&agg.entries);
        let (roster_known, roster_reason) = roster_check(db, &agg)?;
        let quota = open_quota_incident(db, &scope_key)?;
        let pending = counts["pending_admissions"].as_i64().unwrap_or(0);
        let (available, capacity_reason) = if !roster_known {
            (false, "roster_unknown")
        } else if !new_work {
            (false, "new_work_not_enabled")
        } else if quota.is_some() {
            (false, "quota_incident_open")
        } else if pending == 0 {
            (false, "no_pending_admission")
        } else {
            (true, "recorded_capacity_available")
        };
        let bindings: Vec<Value> = agg
            .bindings
            .iter()
            .map(|b| {
                json!({
                    "binding_id": b["binding_id"],
                    "generation": b["generation"],
                    "state": b["state"],
                    "released": !b["released_at_ms"].is_null(),
                })
            })
            .collect();
        items.push(json!({
            "scope": agg.scope,
            "bindings": bindings,
            "counts": counts,
            "roster": if roster_known { "known" } else { "unknown" },
            "roster_reason": roster_reason,
            "quota_incident": quota,
            "capacity_available": available,
            "capacity_reason": capacity_reason,
            "new_work_enabled": new_work,
            "entries": agg.entries,
            "ledger_updated_at_ms": agg.ledger_updated_at_ms,
        }));
    }
    Ok(items)
}

// ---------------------------------------------------------------------------
// Projections (read-only)
// ---------------------------------------------------------------------------

fn capacity_gap_reference(item: &Value, reason: &'static str, item_bytes: usize) -> Result<Value> {
    let canonical = model::canonical(item)?;
    Ok(json!({
        "scope": {"scope_key": item["scope"]["scope_key"]},
        "counts": item["counts"],
        "roster": item["roster"],
        "gap": {
            "reason": reason,
            "item_serialized_bytes": item_bytes,
            "max_single_item_bytes": super::projection::MAX_SINGLE_ITEM_BYTES,
            "item_digest": model::digest(canonical.as_bytes()),
            "detached_reference": {
                "kind": "capacity_scope",
                "scope_key": item["scope"]["scope_key"],
                "source": "capacity_ledger",
            },
        },
    }))
}

fn attention_gap_reference(item: &Value, reason: &'static str, item_bytes: usize) -> Result<Value> {
    let canonical = model::canonical(item)?;
    Ok(json!({
        "kind": item["kind"],
        "binding_id": item["binding_id"],
        "gap": {
            "reason": reason,
            "item_serialized_bytes": item_bytes,
            "max_single_item_bytes": super::projection::MAX_SINGLE_ITEM_BYTES,
            "item_digest": model::digest(canonical.as_bytes()),
            "detached_reference": {
                "kind": "attention_item",
                "item_kind": item["kind"],
            },
        },
    }))
}

fn paginate(
    items: Vec<Value>,
    source_kind: &str,
    limit: i64,
    after: i64,
    gap_reference: fn(&Value, &'static str, usize) -> Result<Value>,
) -> Result<Value> {
    let total = items.len();
    let start = usize::try_from(after).unwrap_or(0).min(total);
    let end = total.min(start + usize::try_from(limit).unwrap_or(0));
    let limited = super::projection::limit_items(items[start..end].to_vec(), gap_reference)?;
    let next_after = start + limited.consumed;
    let frame = super::projection::frame(
        source_kind,
        json!({"after": after, "next_after": next_after}),
        &limited,
        limit,
        after > 0,
        next_after < total,
        limited.gap_count == 0,
        Vec::new(),
    )?;
    Ok(json!({
        "items": limited.items,
        "next_after": next_after,
        "total_items": total,
        "projection": frame,
    }))
}

/// `report.capacity`: per-scope active + reserved accounting with the
/// manager-facing desired-vs-effective writer view. Read-only.
pub(crate) fn capacity_report(db: &Connection, limit: i64, after: i64) -> Result<Value> {
    let mut report = paginate(
        capacity_items(db)?,
        "capacity_accounting",
        limit,
        after,
        capacity_gap_reference,
    )?;
    report["generated_at_ms"] = json!(model::now_ms()?);
    Ok(report)
}

fn attention_source(kind: &str, observed_at_ms: Option<i64>, stale: bool) -> Value {
    json!({"kind": kind, "observed_at_ms": observed_at_ms, "stale": stale})
}

#[allow(clippy::too_many_arguments)]
fn attention_item(
    kind: &str,
    scope_key: &str,
    binding_id: Option<&str>,
    generation: Option<i64>,
    address: Value,
    source: Value,
    suggested_action: Value,
    manager_actionable: bool,
) -> Value {
    json!({
        "kind": kind,
        "scope_key": scope_key,
        "binding_id": binding_id,
        "generation": generation,
        "address": address,
        "source": source,
        "suggested_action": suggested_action,
        "manager_actionable": manager_actionable,
    })
}

fn safe_health_code(value: &Value) -> Option<&str> {
    value.as_str().filter(|code| {
        !code.is_empty()
            && code.len() <= 64
            && code
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    })
}

/// Surface existing independent-worker health through the same read-only
/// Manager attention projection used by binding observations. These facts
/// already have bounded Store projections; this helper adds no journal, queue,
/// retry, or ownership mutation.
fn append_independent_module_attention(
    db: &Connection,
    now: i64,
    items: &mut Vec<Value>,
) -> Result<()> {
    let lifecycle = super::host_lifecycle::status(db)?;
    if lifecycle["latest_failure"]["manager_action_required"] == true
        && let Some(error_code) = safe_health_code(&lifecycle["latest_failure"]["error_code"])
    {
        let observed_at_ms = lifecycle["latest_failure"]["observed_at_ms"].as_i64();
        let stale = observed_at_ms
            .map(|observed| now.saturating_sub(observed) > STALE_AFTER_MS)
            .unwrap_or(true);
        items.push(attention_item(
            "host_failure",
            "host:lifecycle",
            None,
            None,
            json!({
                "error_code":error_code,
                "failed_supervisor":lifecycle["latest_failure"]["failed_supervisor"],
                "failure_category":lifecycle["latest_failure"]["failure_category"],
                "observed_at_ms":observed_at_ms,
                "retry_authorized":false,
                "next_step":"read host.status and retain affected ownership until the required supervisor or Store recovery is explicit",
            }),
            attention_source("host_lifecycle", observed_at_ms, stale),
            json!({"method":"host.status"}),
            true,
        ));
    }
    if let Some(workers) = lifecycle["optional_workers"].as_object() {
        let mut names = workers.keys().map(|name| name.as_str()).collect::<Vec<_>>();
        names.sort_unstable();
        for name in names {
            let health = &workers[name];
            let state = health["state"].as_str();
            let degraded = matches!(state, Some("retry_wait" | "isolated"));
            let historical_failure = health["last_failure"].clone();
            let Some(error_code) = safe_health_code(&health["last_error_code"])
                .or_else(|| safe_health_code(&historical_failure["code"]))
            else {
                continue;
            };
            if !degraded && !historical_failure.is_object() {
                continue;
            }
            let observed_at_ms = if degraded {
                health["updated_at_ms"].as_i64()
            } else {
                historical_failure["observed_at_ms"].as_i64()
            };
            let stale = observed_at_ms
                .map(|observed| now.saturating_sub(observed) > STALE_AFTER_MS)
                .unwrap_or(true);
            let scope_key = format!("host:optional:{name}");
            let next_step = if degraded {
                "read host.status for this bounded worker health and await its recorded retry or changed configuration"
            } else {
                "read host.status for the retained worker failure history and restart count; do not replay work from this fact"
            };
            items.push(attention_item(
                "optional_module_failure",
                &scope_key,
                None,
                None,
                json!({
                    "module_id":name,
                    "state":state,
                    "error_code":error_code,
                    "consecutive_failures":health["consecutive_failures"],
                    "retry_after_ms":health["retry_after_ms"],
                    "last_failure":historical_failure,
                    "restart_count":health["restart_count"],
                    "retry_authorized":false,
                    "next_step":next_step,
                }),
                attention_source("host_lifecycle", observed_at_ms, stale),
                json!({"method":"host.status"}),
                true,
            ));
        }
    }

    let bus_health = super::bus_kernel::managed_health_projection(db)?;
    if let Some(services) = bus_health.as_array() {
        for health in services {
            let Some(service_key) = health["service_key"].as_str() else {
                continue;
            };
            let state = health["state"].as_str();
            let owner_state = health["owner_state"].as_str();
            let error_code = safe_health_code(&health["last_error_code"]);
            let owner_uncertain = matches!(owner_state, Some("launch_uncertain" | "unknown"));
            let state_degraded = matches!(state, Some("retry_wait" | "isolated" | "unknown"));
            if !owner_uncertain && (error_code.is_none() || !state_degraded) {
                continue;
            }
            let observed_at_ms = health["updated_at_ms"].as_i64();
            let stale = observed_at_ms
                .map(|observed| now.saturating_sub(observed) > STALE_AFTER_MS)
                .unwrap_or(true);
            let scope_key = format!("managed-bus:{service_key}");
            items.push(attention_item(
                "managed_bus_failure",
                &scope_key,
                None,
                None,
                json!({
                    "service_key":service_key,
                    "state":state,
                    "owner_state":owner_state,
                    "error_code":error_code,
                    "consecutive_failures":health["consecutive_failures"],
                    "retry_after_ms":health["retry_after_ms"],
                    "retry_authorized":false,
                    "next_step":"read host.status for this managed service and retain the owner until its exact readback is resolved",
                }),
                attention_source("managed_bus_health", observed_at_ms, stale),
                json!({"method":"host.status"}),
                true,
            ));
        }
    }
    Ok(())
}

/// Builds every attention item the recorded facts support. The kinds
/// are those of §8.3; an item exists only when an exact recorded fact
/// addresses it — a native request by its recorded ID and fingerprint,
/// a child run by its session and turn, an input by its operation or
/// native input ID, a scope by its recorded capacity facts.
fn build_attention_items(db: &Connection, now: i64) -> Result<Vec<Value>> {
    let mut items: Vec<Value> = Vec::new();
    append_independent_module_attention(db, now, &mut items)?;
    let mut stmt = db.prepare(
        "SELECT binding_id,generation FROM bindings WHERE released_at_ms IS NULL ORDER BY binding_id,generation",
    )?;
    let keys = stmt
        .query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    for (binding_id, generation) in keys {
        let binding = operations::get_binding(db, &binding_id, generation)?;
        let scope = scope_facts(
            &binding["route"],
            binding["native_scope_key"].as_str(),
            &binding_id,
        );
        let scope_key = scope["scope_key"].as_str().unwrap_or_default().to_owned();
        if let Some(item) = super::module_supervisor_observation::manager_attention_item(
            &binding["observation"]["module_supervisor"],
            &scope_key,
            &binding_id,
            generation,
            now,
            STALE_AFTER_MS,
        ) {
            items.push(item);
        }
        let native = &binding["observation"]["native"];
        let observed_at = binding["observation"]["observed_at_ms"].as_i64();
        let has_native = native.is_object();
        let stale = has_native
            && observed_at
                .map(|at| now - at > STALE_AFTER_MS)
                .unwrap_or(true);
        let observation_live = has_native && !stale;
        // --- observation_stale --------------------------------------
        if binding["native_root_id"].is_string() {
            let reason = if !has_native {
                Some("never_observed")
            } else if binding["observation"]["connection"] == "disconnected" {
                Some("module_disconnected")
            } else if binding["state"] == "reconciling" {
                Some("binding_reconciling")
            } else if stale {
                Some("observation_stale")
            } else {
                None
            };
            if let Some(reason) = reason {
                items.push(attention_item(
                    "observation_stale",
                    &scope_key,
                    Some(&binding_id),
                    Some(generation),
                    json!({
                        "binding_id": binding_id, "generation": generation,
                        "native_root_id": binding["native_root_id"],
                        "binding_state": binding["state"],
                        "connection": binding["observation"]["connection"],
                        "reason": reason,
                    }),
                    attention_source("binding_observation", observed_at, true),
                    if reason == "observation_stale" || reason == "never_observed" {
                        json!({"method": "agent.refresh", "binding_id": binding_id, "generation": generation})
                    } else {
                        Value::Null
                    },
                    false,
                ));
            }
        }
        if observation_live {
            // --- waiting_for_native_request --------------------------
            if let Some(requests) = native["pending_requests"].as_array() {
                for request in requests {
                    if request["observed_now"] == false {
                        // A retained request the newest enumeration no
                        // longer shows is not current: never re-address
                        // a decision to it.
                        continue;
                    }
                    let (session_id, request_id, request_kind, fingerprint) =
                        if let (Some(s), Some(r), Some(k)) = (
                            request["session_id"].as_str(),
                            request["request_id"].as_str(),
                            request["kind"].as_str(),
                        ) {
                            (
                                Some(s),
                                Some(r.to_owned()),
                                k.to_owned(),
                                request["fingerprint"].clone(),
                            )
                        } else if let Some(method) = request["method"].as_str() {
                            let params = &request["params"];
                            let id = params["approvalId"]
                                .as_str()
                                .or_else(|| params["userInputId"].as_str());
                            let kind = if method.starts_with("approval") {
                                "approval"
                            } else if method.starts_with("userInput") {
                                "user_input"
                            } else {
                                method
                            };
                            (
                                params["sessionId"].as_str(),
                                id.map(str::to_owned),
                                kind.to_owned(),
                                Value::Null,
                            )
                        } else {
                            continue;
                        };
                    let Some(request_id) = request_id else {
                        continue;
                    };
                    items.push(attention_item(
                        "waiting_for_native_request",
                        &scope_key,
                        Some(&binding_id),
                        Some(generation),
                        json!({
                            "binding_id": binding_id, "generation": generation,
                            "session_id": session_id, "request_id": request_id,
                            "request_kind": request_kind, "fingerprint": fingerprint,
                        }),
                        attention_source("binding_observation", observed_at, false),
                        json!({
                            "method": "agent.reply", "binding_id": binding_id,
                            "generation": generation, "session_id": session_id,
                            "request_id": request_id,
                        }),
                        true,
                    ));
                }
            }
            // --- waiting_for_child_result / foreground_tool_blocking -
            let root_id = native["native_root_id"]
                .as_str()
                .or_else(|| binding["native_root_id"].as_str());
            let root_turn = native["turns"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|t| t["sessionId"].as_str() == root_id);
            let root_running = root_turn.is_some_and(|t| t["terminal"].is_null());
            if let Some(children) = native["observed_children"].as_array() {
                for child in children {
                    if child["observed_now"] == false {
                        continue;
                    }
                    let last_turn = &child["last_turn"];
                    let running = (last_turn.is_object() && last_turn["terminal"].is_null())
                        || child["execution_disposition"] == "running";
                    if !running {
                        continue;
                    }
                    let session_id = child["sessionId"].clone();
                    let turn_id = last_turn["turnId"].clone();
                    items.push(attention_item(
                        "waiting_for_child_result",
                        &scope_key,
                        Some(&binding_id),
                        Some(generation),
                        json!({
                            "binding_id": binding_id, "generation": generation,
                            "session_id": session_id, "turn_id": turn_id,
                            "parent_session_id": root_id,
                        }),
                        attention_source("binding_observation", observed_at, false),
                        Value::Null,
                        false,
                    ));
                    // The root's own turn is still open and this direct
                    // child's execution is what it waits on: the child
                    // occupies the foreground. Recorded turns carry no
                    // tool identity on this adapter, so the item
                    // addresses the session and turn, with tool null.
                    if root_running
                        && root_id.is_some()
                        && child["parentSessionId"].as_str() == root_id
                    {
                        items.push(attention_item(
                            "foreground_tool_blocking",
                            &scope_key,
                            Some(&binding_id),
                            Some(generation),
                            json!({
                                "binding_id": binding_id, "generation": generation,
                                "session_id": session_id, "turn_id": turn_id,
                                "tool": null,
                                "blocked_session_id": root_id,
                            }),
                            attention_source("binding_observation", observed_at, false),
                            Value::Null,
                            false,
                        ));
                    }
                }
            }
        }
        // --- input_queued_not_consumed -------------------------------
        let mut stmt = db.prepare(
            "SELECT operation_id,method,state,created_at_ms,updated_at_ms,native_refs_json FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND state IN ('queued','sending','native_accepted','outcome_unknown','settled') ORDER BY operation_id",
        )?;
        let ops = stmt
            .query_map(params![binding_id, generation], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, i64>(3)?,
                    r.get::<_, i64>(4)?,
                    r.get::<_, String>(5)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        for (op_id, method, op_state, created, updated, refs_raw) in ops {
            if method == "agent.open" {
                continue;
            }
            if op_state == "queued" && is_admission(&method) {
                items.push(attention_item(
                    "input_queued_not_consumed",
                    &scope_key,
                    Some(&binding_id),
                    Some(generation),
                    json!({
                        "binding_id": binding_id, "generation": generation,
                        "operation_id": op_id, "method": method,
                        "stage": "queued_not_sent",
                    }),
                    attention_source("operations", Some(created), false),
                    Value::Null,
                    false,
                ));
                continue;
            }
            if matches!(method.as_str(), "task.dispatch" | "agent.send") {
                let refs: Value = serde_json::from_str(&refs_raw)?;
                let proof = &refs["input_execution"];
                if proof.is_object() && proof["disposition"] == "queued" {
                    items.push(attention_item(
                        "input_queued_not_consumed",
                        &scope_key,
                        Some(&binding_id),
                        Some(generation),
                        json!({
                            "binding_id": binding_id, "generation": generation,
                            "operation_id": op_id, "method": method,
                            "stage": "admitted_not_delivered",
                            "native_input_id": proof["native_input_id"],
                            "native_session_id": proof["native_session_id"],
                        }),
                        attention_source("operations", Some(updated), false),
                        Value::Null,
                        false,
                    ));
                }
            }
        }
        // --- manager_actionable (submitted work awaits a decision) ---
        let mut stmt = db.prepare(
            "SELECT attempt_id,task_id,updated_at_ms FROM attempts WHERE binding_id=?1 AND binding_generation=?2 AND state='submitted' AND released_at_ms IS NULL ORDER BY attempt_id",
        )?;
        let submitted = stmt
            .query_map(params![binding_id, generation], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, i64>(2)?,
                ))
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        for (attempt_id, task_id, updated) in submitted {
            items.push(attention_item(
                "manager_actionable",
                &scope_key,
                Some(&binding_id),
                Some(generation),
                json!({
                    "binding_id": binding_id, "generation": generation,
                    "task_id": task_id, "attempt_id": attempt_id,
                    "awaiting": "manager_acceptance_decision",
                }),
                attention_source("attempts", Some(updated), false),
                json!({"method": "task.accept", "task_id": task_id, "attempt_id": attempt_id}),
                true,
            ));
        }
    }
    // --- capacity_available ------------------------------------------
    for scope_item in capacity_items(db)? {
        if scope_item["capacity_available"] != true {
            continue;
        }
        items.push(attention_item(
            "capacity_available",
            scope_item["scope"]["scope_key"].as_str().unwrap_or_default(),
            None,
            None,
            json!({
                "scope": scope_item["scope"],
                "pending_admissions": scope_item["counts"]["pending_admissions"],
                "desired_writers": scope_item["counts"]["desired_writers"],
                "effective_writers": scope_item["counts"]["effective_writers"],
            }),
            attention_source(
                "capacity_ledger",
                Some(scope_item["ledger_updated_at_ms"].as_i64().unwrap_or(0)),
                false,
            ),
            json!({"method": "task.dispatch", "note": "recorded scope facts do not block the pending admission"}),
            true,
        ));
    }
    items.sort_by(|a, b| {
        let key = |item: &Value| {
            format!(
                "{}|{}|{}|{}",
                item["kind"].as_str().unwrap_or_default(),
                item["scope_key"].as_str().unwrap_or_default(),
                item["binding_id"].as_str().unwrap_or_default(),
                model::canonical(&item["address"]).unwrap_or_default(),
            )
        };
        key(a).cmp(&key(b))
    });
    Ok(items)
}

/// `report.attention`: the unified read-only attention projection of
/// §8.3 across all adapters and scopes. Read-only: it suggests
/// addressed operations and performs none.
pub(crate) fn attention_report(db: &Connection, limit: i64, after: i64) -> Result<Value> {
    let items = build_attention_items(db, model::now_ms()?)?;
    let mut report = paginate(
        items,
        "attention_projection",
        limit,
        after,
        attention_gap_reference,
    )?;
    report["generated_at_ms"] = json!(model::now_ms()?);
    Ok(report)
}
