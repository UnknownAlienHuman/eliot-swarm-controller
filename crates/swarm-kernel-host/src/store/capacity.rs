//! Durable capacity accounting (Documentation Program §20, R23) and the
//! manager-facing capacity / attention projections (§6 and §8.3, R12).
//!
//! ## Accounting
//!
//! Every native admission on a binding — an `agent.*` command, a
//! `task.dispatch`, a `native.opencode.loop_step`, or an Attempt producer —
//! holds a reservation in a per-scope ledger. The ledger is a durable record in
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
//! scope is keyed by its binding, and no shared ledger-scope capacity
//! claim is made for it. An explicit route root limit can still use the
//! exact retained route identity; an unknown roster never authorizes more
//! work.
//!
//! ## Legacy quota incident evidence
//!
//! Existing `quota:*` incident rows remain readable as historical outcome
//! evidence. Runtime outcomes are not text-classified here to create,
//! update, or resolve them, and these legacy rows do not decide current
//! admission or root claims. Current provider conditions are owned by
//! `provider_conditions`.
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
use crate::{
    error::{Error, Result},
    model,
    runtime::RuntimeOutcome,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};

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
    // This starts work on an existing root: account the operation and its
    // active writer evidence without adding a root claim.
    "native.opencode.loop_step",
    "agent.reply",
    "agent.configure",
    "agent.goal",
    "agent.refresh",
    "agent.reconcile",
    "agent.result",
    "agent.recover",
];

const CAPACITY_LEDGER_SCHEMA_VERSION: i64 = 1;
const MAX_CAPACITY_LEDGER_BYTES: usize = 1024 * 1024;
const MAX_CAPACITY_LEDGER_ENTRIES: usize = 4096;
const MAX_CAPACITY_ID_BYTES: usize = 512;
const MAX_ATTEMPT_PRODUCERS: usize = 4096;
const MAX_ROSTER_ROWS: usize = 10_000;
const MAX_CAPACITY_LEDGER_SQL_BYTES: i64 = MAX_CAPACITY_LEDGER_BYTES as i64 + 1;
const MAX_ROSTER_SQL_ROWS: i64 = MAX_ROSTER_ROWS as i64 + 1;

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

/// Canonical capacity collision key shared by Store helpers that need to
/// address the same provider/resource scope. Complete identities use the
/// recorded runtime and service; incomplete identities stay binding-scoped.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct ScopeKey(String);

impl ScopeKey {
    pub(crate) fn from_route(route: &Value, binding_id: &str) -> Self {
        let options = &route["native_options"];
        let runtime = route["runtime"].as_str().filter(|value| !value.is_empty());
        let service = options["service_id"]
            .as_str()
            .filter(|value| !value.is_empty());
        let key = match (runtime, service) {
            (Some(runtime), Some(service)) => format!("{runtime}:{service}"),
            (Some(runtime), None) => format!("{runtime}:binding:{binding_id}"),
            (None, _) => format!("unknown:binding:{binding_id}"),
        };
        Self(key)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

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
    let scope_key = ScopeKey::from_route(route, binding_id);
    json!({
        "scope_key": scope_key.as_str(),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ResourceEntryKind {
    Operation,
    Producer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct NativeEventRef {
    id: String,
    seq: i64,
    sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionIdentity {
    operation_id: Option<String>,
    native_session_id: Option<String>,
    native_input_id: Option<String>,
    native_run_id: Option<String>,
    source_observation_id: Option<i64>,
    source_stream_id: Option<String>,
    source_event_key: Option<String>,
    admission_event_ref: Option<NativeEventRef>,
    delivery_event_ref: Option<NativeEventRef>,
    execution_start_event_ref: Option<NativeEventRef>,
    terminal_event_ref: Option<NativeEventRef>,
    terminal_disposition: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapacityEntry {
    entry_id: String,
    kind: ResourceEntryKind,
    operation_id: Option<String>,
    attempt_id: Option<String>,
    assignment_id: Option<String>,
    method: Option<String>,
    task_id: Option<String>,
    binding_id: String,
    binding_generation: i64,
    phase: DerivedPhase,
    native_session_id: Option<String>,
    admitted_at_ms: i64,
    activated_at_ms: Option<i64>,
    released_at_ms: Option<i64>,
    release_reason: Option<String>,
    execution_start_ref: Value,
    execution_identity: Option<ExecutionIdentity>,
    outcome_unknown_since_ms: Option<i64>,
    last_synced_at_ms: i64,
}

/// One closed, source-derived resource fact. JSON is only the durable
/// serialization boundary; lifecycle merge and roster comparison use this
/// typed representation so identity and phase cannot drift independently.
#[derive(Debug, Clone)]
struct ResourceEvidence {
    entry_id: String,
    kind: ResourceEntryKind,
    operation_id: Option<String>,
    attempt_id: Option<String>,
    assignment_id: Option<String>,
    method: Option<String>,
    task_id: Option<String>,
    binding_id: String,
    binding_generation: i64,
    admitted_at_ms: i64,
    phase: DerivedPhase,
    execution_identity: Option<ExecutionIdentity>,
    execution_start_ref: Value,
    release_reason: Option<String>,
    unknown_since_ms: Option<i64>,
}

struct EvidenceDerivation {
    evidence: Option<ResourceEvidence>,
    damage: Option<CapacityLedgerDamage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MergeResult {
    Applied,
    PreservedHigherPhase,
    Conflict,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CapacityLedger {
    schema_version: i64,
    scope: Value,
    entries: BTreeMap<String, CapacityEntry>,
    updated_at_ms: i64,
}

#[derive(Debug, Clone)]
struct CapacityLedgerDamage {
    code: String,
    raw_digest: String,
    raw_size_bytes: usize,
    digest_complete: bool,
}

enum LedgerLoad {
    Missing,
    Valid(CapacityLedger),
    Damaged(CapacityLedgerDamage),
}

fn damage_for_raw(code: &str, raw: &[u8], size: usize) -> CapacityLedgerDamage {
    CapacityLedgerDamage {
        code: code.to_owned(),
        raw_digest: model::digest(raw),
        raw_size_bytes: size,
        digest_complete: raw.len() == size,
    }
}

fn valid_identity_text(value: &str) -> bool {
    !value.trim().is_empty() && value.len() <= MAX_CAPACITY_ID_BYTES
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn valid_event_ref(event: &NativeEventRef) -> bool {
    valid_identity_text(&event.id) && event.seq > 0 && valid_digest(&event.sha256)
}

fn validate_scope(scope: &Value, scope_key: &str) -> bool {
    let Some(object) = scope.as_object() else {
        return false;
    };
    let expected = [
        "scope_key",
        "runtime",
        "provider",
        "account",
        "service",
        "native_scope_key",
        "route_alias",
        "identity",
    ];
    if object.len() != expected.len() || object.keys().any(|key| !expected.contains(&key.as_str()))
    {
        return false;
    }
    if scope["scope_key"].as_str() != Some(scope_key) || !valid_identity_text(scope_key) {
        return false;
    }
    for name in [
        "runtime",
        "provider",
        "account",
        "service",
        "native_scope_key",
        "route_alias",
    ] {
        if !scope[name].is_null()
            && scope[name]
                .as_str()
                .is_none_or(|value| !valid_identity_text(value))
        {
            return false;
        }
    }
    match scope["identity"].as_str() {
        Some("complete") => {
            let (Some(runtime), Some(service)) =
                (scope["runtime"].as_str(), scope["service"].as_str())
            else {
                return false;
            };
            scope_key == format!("{runtime}:{service}")
        }
        Some("partial") => {
            scope["runtime"]
                .as_str()
                .is_some_and(|runtime| scope_key.starts_with(&format!("{runtime}:binding:")))
                || scope_key.starts_with("unknown:binding:")
        }
        _ => false,
    }
}

fn valid_entry_identity(entry: &CapacityEntry, key: &str) -> bool {
    if entry.entry_id != key
        || !valid_identity_text(key)
        || !valid_identity_text(&entry.binding_id)
        || entry.binding_generation < 1
        || entry.admitted_at_ms < 0
        || entry.last_synced_at_ms < 0
        || entry
            .task_id
            .as_deref()
            .is_some_and(|value| !valid_identity_text(value))
        || entry
            .attempt_id
            .as_deref()
            .is_some_and(|value| !valid_identity_text(value))
    {
        return false;
    }
    match entry.kind {
        ResourceEntryKind::Operation => {
            entry.operation_id.as_deref() == Some(key)
                && entry
                    .method
                    .as_deref()
                    .is_some_and(|method| is_admission(method) && valid_identity_text(method))
                && entry.assignment_id.is_none()
                && (entry.method.as_deref() != Some("task.dispatch") || entry.attempt_id.is_some())
        }
        ResourceEntryKind::Producer => {
            let (Some(attempt_id), Some(assignment_id)) =
                (entry.attempt_id.as_deref(), entry.assignment_id.as_deref())
            else {
                return false;
            };
            entry.operation_id.is_none()
                && entry.method.is_none()
                && entry
                    .native_session_id
                    .as_deref()
                    .is_none_or(valid_identity_text)
                && valid_identity_text(assignment_id)
                && key == format!("producer:{attempt_id}:{assignment_id}")
        }
    }
}

fn validate_ledger(
    ledger: &CapacityLedger,
    expected_scope_key: &str,
    expected_scope: Option<&Value>,
) -> bool {
    if ledger.schema_version != CAPACITY_LEDGER_SCHEMA_VERSION
        || ledger.updated_at_ms < 0
        || ledger.entries.len() > MAX_CAPACITY_LEDGER_ENTRIES
        || !validate_scope(&ledger.scope, expected_scope_key)
        || expected_scope.is_some_and(|scope| ledger.scope != *scope)
    {
        return false;
    }
    let mut identities = BTreeMap::<String, String>::new();
    for (entry_id, entry) in &ledger.entries {
        if !valid_entry_identity(entry, entry_id) {
            return false;
        }
        let source_identity = match entry.kind {
            ResourceEntryKind::Operation => format!(
                "operation:{}",
                entry.operation_id.as_deref().unwrap_or_default()
            ),
            ResourceEntryKind::Producer => format!("producer:{entry_id}"),
        };
        if identities
            .insert(source_identity, entry_id.clone())
            .is_some()
        {
            return false;
        }
        match entry.phase {
            DerivedPhase::Reserved => {
                if entry.activated_at_ms.is_some()
                    || entry.released_at_ms.is_some()
                    || entry.release_reason.is_some()
                {
                    return false;
                }
            }
            DerivedPhase::Active => {
                if entry
                    .activated_at_ms
                    .is_none_or(|at| at < entry.admitted_at_ms)
                    || entry.released_at_ms.is_some()
                    || entry.release_reason.is_some()
                    || entry.execution_identity.is_none()
                    || entry.execution_start_ref.is_null()
                {
                    return false;
                }
            }
            DerivedPhase::Released => {
                if entry
                    .released_at_ms
                    .is_none_or(|at| at < entry.admitted_at_ms)
                    || entry.release_reason.as_deref().is_none_or(str::is_empty)
                    || entry.outcome_unknown_since_ms.is_some()
                {
                    return false;
                }
            }
        }
        if entry
            .activated_at_ms
            .is_some_and(|at| at < entry.admitted_at_ms || at < 0)
            || entry
                .released_at_ms
                .is_some_and(|at| at < entry.admitted_at_ms || at < 0)
            || entry.outcome_unknown_since_ms.is_some_and(|at| at < 0)
        {
            return false;
        }
        if let Some(identity) = &entry.execution_identity {
            if identity
                .native_session_id
                .as_deref()
                .is_some_and(|value| !valid_identity_text(value))
                || identity
                    .operation_id
                    .as_deref()
                    .is_some_and(|value| !valid_identity_text(value))
                || identity
                    .native_input_id
                    .as_deref()
                    .is_some_and(|value| !valid_identity_text(value))
                || identity
                    .native_run_id
                    .as_deref()
                    .is_some_and(|value| !valid_identity_text(value))
                || identity.source_observation_id.is_some_and(|id| id < 1)
                || identity
                    .source_stream_id
                    .as_deref()
                    .is_some_and(|value| !valid_identity_text(value))
                || identity
                    .source_event_key
                    .as_deref()
                    .is_some_and(|value| !valid_identity_text(value))
                || identity
                    .admission_event_ref
                    .as_ref()
                    .is_some_and(|event| !valid_event_ref(event))
                || identity
                    .delivery_event_ref
                    .as_ref()
                    .is_some_and(|event| !valid_event_ref(event))
                || identity
                    .execution_start_event_ref
                    .as_ref()
                    .is_some_and(|event| !valid_event_ref(event))
                || identity
                    .terminal_event_ref
                    .as_ref()
                    .is_some_and(|event| !valid_event_ref(event))
                || identity
                    .terminal_disposition
                    .as_deref()
                    .is_some_and(|value| terminal_disposition(&json!(value)).is_none())
            {
                return false;
            }
            if entry.kind == ResourceEntryKind::Operation
                && identity.operation_id.as_deref() != entry.operation_id.as_deref()
            {
                return false;
            }
        }
        if entry.native_session_id
            != entry
                .execution_identity
                .as_ref()
                .and_then(|identity| identity.native_session_id.clone())
        {
            return false;
        }
        if entry.phase == DerivedPhase::Active
            && entry.execution_identity.as_ref().is_none_or(|identity| {
                identity.native_run_id.is_none() || identity.native_session_id.is_none()
            })
        {
            return false;
        }
    }
    true
}

fn read_ledger_bytes(db: &Connection, key: &str) -> Result<Option<(usize, Vec<u8>)>> {
    let row: Option<(i64, Vec<u8>)> = db
        .query_row(
            "SELECT length(CAST(value_json AS BLOB)),\
                    substr(CAST(value_json AS BLOB),1,?2) FROM meta WHERE key=?1",
            params![key, MAX_CAPACITY_LEDGER_SQL_BYTES],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    row.map(|(size, bytes)| {
        usize::try_from(size)
            .map(|size| (size, bytes))
            .map_err(|_| Error::new("CAPACITY_LEDGER_DAMAGED", "ledger byte length is invalid"))
    })
    .transpose()
}

fn parse_ledger_bytes(
    scope_key: &str,
    expected_scope: Option<&Value>,
    size: usize,
    raw: &[u8],
) -> LedgerLoad {
    if size > MAX_CAPACITY_LEDGER_BYTES || raw.len() != size {
        return LedgerLoad::Damaged(damage_for_raw("ledger_size_exceeded", raw, size));
    }
    let ledger: CapacityLedger = match serde_json::from_slice(raw) {
        Ok(ledger) => ledger,
        Err(_) => return LedgerLoad::Damaged(damage_for_raw("ledger_invalid_json", raw, size)),
    };
    if !validate_ledger(&ledger, scope_key, expected_scope) {
        return LedgerLoad::Damaged(damage_for_raw("ledger_invalid_shape", raw, size));
    }
    LedgerLoad::Valid(ledger)
}

fn load_ledger(db: &Connection, scope: &Value) -> Result<LedgerLoad> {
    let scope_key = scope["scope_key"]
        .as_str()
        .filter(|value| valid_identity_text(value))
        .ok_or_else(|| Error::new("CAPACITY_SCOPE_DAMAGED", "capacity scope key is invalid"))?;
    let key = ledger_key(scope_key);
    let Some((size, raw)) = read_ledger_bytes(db, &key)? else {
        return Ok(LedgerLoad::Missing);
    };
    Ok(parse_ledger_bytes(scope_key, Some(scope), size, &raw))
}

/// Projects bounded accounting for one exact Attempt from its validated
/// scope ledger. Missing or damaged rows keep every count explicitly unknown.
pub(super) fn attempt_accounting(
    db: &Connection,
    scope: &Value,
    attempt_id: &str,
) -> Result<Value> {
    match load_ledger(db, scope)? {
        LedgerLoad::Missing => Ok(json!({
            "status": "ledger_missing",
            "attempt_entries": {
                "reserved": null,
                "active": null,
                "outcome_unknown": null,
                "count": null,
            },
            "ledger_updated_at_ms": null,
        })),
        LedgerLoad::Damaged(damage) => Ok(json!({
            "status": "ledger_damaged",
            "attempt_entries": {
                "reserved": null,
                "active": null,
                "outcome_unknown": null,
                "count": null,
            },
            "ledger_updated_at_ms": null,
            "damage": damage_value(&damage),
        })),
        LedgerLoad::Valid(ledger) => {
            let mut reserved = 0usize;
            let mut active = 0usize;
            let mut outcome_unknown = 0usize;
            let mut count = 0usize;
            for entry in ledger
                .entries
                .values()
                .filter(|entry| entry.attempt_id.as_deref() == Some(attempt_id))
            {
                count += 1;
                match entry.phase {
                    DerivedPhase::Reserved => reserved += 1,
                    DerivedPhase::Active => active += 1,
                    DerivedPhase::Released => {}
                }
                if entry.outcome_unknown_since_ms.is_some() {
                    outcome_unknown += 1;
                }
            }
            Ok(json!({
                "status": "recorded_attempt_entries",
                "attempt_entries": {
                    "reserved": reserved,
                    "active": active,
                    "outcome_unknown": outcome_unknown,
                    "count": count,
                },
                "ledger_updated_at_ms": ledger.updated_at_ms,
            }))
        }
    }
}

fn store_ledger(db: &Connection, ledger: &CapacityLedger) -> Result<()> {
    let value = serde_json::to_value(ledger)?;
    let scope_key = ledger.scope["scope_key"]
        .as_str()
        .ok_or_else(|| Error::new("CAPACITY_SCOPE_DAMAGED", "capacity scope key is missing"))?;
    set_meta(db, &ledger_key(scope_key), &value)
}

enum LedgerRow {
    Valid(String, CapacityLedger),
    Damaged(String, CapacityLedgerDamage),
}

fn all_ledgers(db: &Connection) -> Result<Vec<LedgerRow>> {
    let mut stmt = db.prepare(
        "SELECT key,length(CAST(value_json AS BLOB)),\
                substr(CAST(value_json AS BLOB),1,?1) FROM meta \
         WHERE key LIKE 'capacity:%' ORDER BY key LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(
            params![MAX_CAPACITY_LEDGER_SQL_BYTES, MAX_ROSTER_SQL_ROWS],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            },
        )?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let overflow = rows.len() > MAX_ROSTER_ROWS;
    let mut parsed = rows
        .into_iter()
        .take(MAX_ROSTER_ROWS)
        .map(|(key, size, raw)| {
            let scope_key = key.strip_prefix("capacity:").unwrap_or_default();
            let size = usize::try_from(size).map_err(|_| {
                Error::new("CAPACITY_LEDGER_DAMAGED", "ledger byte length is invalid")
            })?;
            if !valid_identity_text(scope_key) {
                return Ok(LedgerRow::Damaged(
                    "unknown:damaged-ledger-key".into(),
                    damage_for_raw("ledger_invalid_key", key.as_bytes(), key.len()),
                ));
            }
            Ok(match parse_ledger_bytes(scope_key, None, size, &raw) {
                LedgerLoad::Valid(ledger) => LedgerRow::Valid(scope_key.to_owned(), ledger),
                LedgerLoad::Damaged(damage) => LedgerRow::Damaged(scope_key.to_owned(), damage),
                LedgerLoad::Missing => unreachable!("a selected ledger row cannot be missing"),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if overflow {
        parsed.push(LedgerRow::Damaged(
            "unknown:capacity-ledger-roster-limit".into(),
            damage_for_raw("ledger_roster_limit_exceeded", &[], 0),
        ));
    }
    Ok(parsed)
}

// ---------------------------------------------------------------------------
// Phase derivation
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DerivedPhase {
    Reserved,
    Active,
    Released,
}

fn damage_for_value(code: &str, value: &Value) -> Result<CapacityLedgerDamage> {
    let raw = serde_json::to_vec(value)?;
    let size = raw.len();
    let bounded = &raw[..raw.len().min(MAX_CAPACITY_LEDGER_BYTES)];
    Ok(damage_for_raw(code, bounded, size))
}

fn damaged_evidence(code: &str, value: &Value) -> Result<EvidenceDerivation> {
    Ok(EvidenceDerivation {
        evidence: None,
        damage: Some(damage_for_value(code, value)?),
    })
}

fn exact_event_ref(value: &Value) -> Option<NativeEventRef> {
    serde_json::from_value::<NativeEventRef>(value.clone())
        .ok()
        .filter(valid_event_ref)
}

fn object_has_only(value: &Value, allowed: &[&str]) -> bool {
    value
        .as_object()
        .is_some_and(|object| object.keys().all(|key| allowed.contains(&key.as_str())))
}

fn exact_input_execution(
    db: &Connection,
    op: &Value,
    proof: &Value,
) -> Result<Option<(ExecutionIdentity, i64)>> {
    const PROOF_FIELDS: &[&str] = &[
        "reader_revision",
        "operation_id",
        "native_session_id",
        "native_input_id",
        "native_run_id",
        "native_run_id_kind",
        "admission",
        "delivery",
        "execution_started",
        "terminal",
        "disposition",
        "uncertainty",
        "log_watermark",
        "correlation",
        "assistant_result_correlation",
        "assistant_result_correlation_reason",
        "family_complete",
        "native_scope_key",
        "native_service_version",
        "goal_terminal_evidence",
    ];
    if !object_has_only(proof, PROOF_FIELDS)
        || proof["reader_revision"] != "opencode-execution-log-v1"
        || proof["correlation"] != "durable_serialized_execution"
        || proof["operation_id"] != op["operation_id"]
        || proof["family_complete"] != false
        || proof["uncertainty"]
            .as_str()
            .is_some_and(|value| !valid_identity_text(value))
        || (!proof["uncertainty"].is_null() && !proof["uncertainty"].is_string())
    {
        return Ok(None);
    }
    let (Some(operation_id), Some(binding_id), Some(generation), Some(session_id), Some(input_id)) = (
        op["operation_id"].as_str(),
        op["binding_id"].as_str(),
        op["binding_generation"].as_i64(),
        proof["native_session_id"].as_str(),
        proof["native_input_id"].as_str(),
    ) else {
        return Ok(None);
    };
    if !valid_identity_text(operation_id)
        || !valid_identity_text(binding_id)
        || generation < 1
        || !valid_identity_text(session_id)
        || !valid_identity_text(input_id)
        || !op["native_refs"].is_object()
    {
        return Ok(None);
    }
    for (field, proof_field) in [
        ("input_id", "native_input_id"),
        ("session_id", "native_session_id"),
    ] {
        if !op["native_refs"][field].is_null()
            && op["native_refs"][field]
                .as_str()
                .filter(|value| valid_identity_text(value))
                != proof[proof_field].as_str()
        {
            return Ok(None);
        }
    }
    let Some(admission) = exact_event_ref(&proof["admission"]) else {
        return Ok(None);
    };
    let delivery = if proof["delivery"].is_null() {
        None
    } else {
        let Some(event) = exact_event_ref(&proof["delivery"]) else {
            return Ok(None);
        };
        Some(event)
    };
    let execution_start = if proof["execution_started"].is_null() {
        None
    } else {
        let Some(event) = exact_event_ref(&proof["execution_started"]) else {
            return Ok(None);
        };
        Some(event)
    };
    let native_run_id = if proof["native_run_id"].is_null() {
        None
    } else {
        let Some(run_id) = proof["native_run_id"].as_str() else {
            return Ok(None);
        };
        if !valid_identity_text(run_id) {
            return Ok(None);
        }
        Some(run_id.to_owned())
    };
    if execution_start.as_ref().map(|event| event.id.as_str()) != native_run_id.as_deref()
        || native_run_id.is_some()
            && (delivery.is_none() || proof["native_run_id_kind"] != "execution_started_event")
        || native_run_id.is_none() && !proof["native_run_id_kind"].is_null()
    {
        return Ok(None);
    }
    let terminal = &proof["terminal"];
    let terminal_ref = if terminal.is_null() {
        None
    } else {
        if !object_has_only(
            terminal,
            &["event", "outcome", "reason", "stage", "error_code"],
        ) {
            return Ok(None);
        }
        let Some(event) = exact_event_ref(&terminal["event"]) else {
            return Ok(None);
        };
        if terminal["outcome"].as_str().is_none()
            || (!terminal["reason"].is_null() && terminal["reason"].as_str().is_none())
            || (!terminal["stage"].is_null() && terminal["stage"].as_str().is_none())
            || (!terminal["error_code"].is_null() && terminal["error_code"].as_str().is_none())
        {
            return Ok(None);
        }
        Some(event)
    };
    let terminal_status = terminal_disposition(&proof["disposition"]);
    if terminal_status.is_some()
        && (terminal_ref.is_none()
            || terminal["outcome"].as_str() != terminal_status
            || proof["uncertainty"].is_string())
    {
        return Ok(None);
    }
    if !matches!(
        proof["disposition"].as_str(),
        Some(
            "queued"
                | "running"
                | "completed"
                | "failed"
                | "cancelled"
                | "unknown"
                | "recovery_pending"
        )
    ) || proof["disposition"] == "running" && native_run_id.is_none()
    {
        return Ok(None);
    }
    let payload = model::canonical(proof)?;
    if payload.len() > MAX_CAPACITY_LEDGER_BYTES {
        return Ok(None);
    }
    let stream = format!("opencode-execution:{binding_id}:{generation}");
    let event_key = format!("{operation_id}:{}", model::digest(payload.as_bytes()));
    let row: Option<(i64, String, String, i64, String)> = db
        .query_row(
            "SELECT observation_id,source_stream_id,source_event_key,binding_generation,payload_json \
             FROM observations WHERE source_stream_id=?1 AND source_event_key=?2 \
               AND binding_id=?3 AND operation_id=?4 AND kind='opencode.input_execution' LIMIT 1",
            params![stream, event_key, binding_id, operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                ))
            },
        )
        .optional()?;
    let Some((observation_id, source_stream_id, source_event_key, observed_generation, raw)) = row
    else {
        return Ok(None);
    };
    if observation_id < 1
        || observed_generation != generation
        || source_stream_id != stream
        || source_event_key != event_key
        || raw != payload
    {
        return Ok(None);
    }
    Ok(Some((
        ExecutionIdentity {
            operation_id: Some(operation_id.to_owned()),
            native_session_id: Some(session_id.to_owned()),
            native_input_id: Some(input_id.to_owned()),
            native_run_id,
            source_observation_id: Some(observation_id),
            source_stream_id: Some(source_stream_id),
            source_event_key: Some(source_event_key),
            admission_event_ref: Some(admission),
            delivery_event_ref: delivery,
            execution_start_event_ref: execution_start,
            terminal_event_ref: terminal_ref,
            terminal_disposition: terminal_status.map(str::to_owned),
        },
        observation_id,
    )))
}

fn attempt_resolved(attempt: &Value) -> bool {
    attempt["released_at_ms"].as_i64().is_some_and(|at| at >= 0)
        || matches!(
            attempt["state"].as_str(),
            Some("accepted" | "failed" | "cancelled" | "superseded")
        )
}

fn validated_producers(attempt: &Value) -> Option<&[Value]> {
    let producers = attempt["producers"].as_array()?;
    if producers.len() > MAX_ATTEMPT_PRODUCERS {
        return None;
    }
    let (attempt_id, task_id, _, _) = validate_attempt(attempt)?;
    let encoded = serde_json::to_vec(producers).ok()?;
    if encoded.len() > MAX_CAPACITY_LEDGER_BYTES {
        return None;
    }
    let mut assignments = BTreeMap::<&str, ()>::new();
    let mut dispatches = BTreeMap::<&str, ()>::new();
    let mut native_inputs = BTreeMap::<(String, String), ()>::new();
    let mut native_runs = BTreeMap::<(String, String), ()>::new();
    for producer in producers {
        if !producer.is_object()
            || (!producer["attempt_id"].is_null()
                && producer["attempt_id"].as_str() != Some(attempt_id.as_str()))
            || (!producer["task_id"].is_null()
                && producer["task_id"].as_str() != Some(task_id.as_str()))
        {
            return None;
        }
        let assignment = producer["assignment_id"].as_str()?;
        if !valid_identity_text(assignment) || assignments.insert(assignment, ()).is_some() {
            return None;
        }
        match producer["dispatch_operation_id"].as_str() {
            Some(operation_id)
                if valid_identity_text(operation_id)
                    && operation_id == assignment
                    && dispatches.insert(operation_id, ()).is_none() => {}
            None if producer["dispatch_operation_id"].is_null() => {}
            _ => return None,
        }
        for field in ["native_session_id", "native_input_id", "native_run_id"] {
            if !producer[field].is_null()
                && producer[field]
                    .as_str()
                    .is_none_or(|value| !valid_identity_text(value))
            {
                return None;
            }
        }
        if let (Some(session_id), Some(input_id)) = (
            producer["native_session_id"].as_str(),
            producer["native_input_id"].as_str(),
        ) && native_inputs
            .insert((session_id.to_owned(), input_id.to_owned()), ())
            .is_some()
        {
            return None;
        }
        if let (Some(session_id), Some(run_id)) = (
            producer["native_session_id"].as_str(),
            producer["native_run_id"].as_str(),
        ) && native_runs
            .insert((session_id.to_owned(), run_id.to_owned()), ())
            .is_some()
        {
            return None;
        }
        for field in ["observed_in", "execution_observation_id"] {
            if !producer[field].is_null() && producer[field].as_i64().is_none_or(|value| value < 1)
            {
                return None;
            }
        }
        for field in ["disposition", "execution_disposition"] {
            if !producer[field].is_null()
                && !matches!(
                    producer[field].as_str(),
                    Some(
                        "admitted"
                            | "queued"
                            | "running"
                            | "completed"
                            | "failed"
                            | "cancelled"
                            | "unknown"
                            | "recovery_pending"
                    )
                )
            {
                return None;
            }
        }
        if !producer["terminal_evidence"].is_null() && !producer["terminal_evidence"].is_object() {
            return None;
        }
        let execution_shape = producer["execution_shape"].as_str();
        let is_batch = execution_shape == Some(crate::runtime::batch::EXECUTION_SHAPE);
        if (!producer["execution_shape"].is_null() && !is_batch)
            || (is_batch
                && (producer["dispatch_operation_id"].as_str().is_none()
                    || producer["batch_run_id"]
                        .as_str()
                        .is_none_or(|value| !valid_identity_text(value))
                    || (!producer["per_run_native_session_id"].is_null()
                        && producer["per_run_native_session_id"]
                            .as_str()
                            .is_none_or(|value| !valid_identity_text(value)))
                    || !producer["terminal_evidence"].is_object()
                    || !matches!(
                        producer["disposition"].as_str(),
                        Some("completed" | "failed")
                    )
                    || !producer["native_session_id"].is_null()
                    || !producer["native_input_id"].is_null()
                    || !producer["native_run_id"].is_null()))
            || (!is_batch
                && (!producer["batch_run_id"].is_null()
                    || !producer["per_run_native_session_id"].is_null()))
        {
            return None;
        }
        let disposition = producer["disposition"].as_str();
        let execution_disposition = producer["execution_disposition"].as_str();
        let disposition_terminal = terminal_disposition(&producer["disposition"]);
        let execution_terminal = terminal_disposition(&producer["execution_disposition"]);
        let disposition_unknown = matches!(disposition, Some("unknown" | "recovery_pending"));
        let execution_unknown =
            matches!(execution_disposition, Some("unknown" | "recovery_pending"));
        if disposition_terminal
            .zip(execution_terminal)
            .is_some_and(|(left, right)| left != right)
            || disposition_terminal.is_some() && execution_unknown
            || execution_terminal.is_some() && disposition_unknown
        {
            return None;
        }
    }
    Some(producers)
}

fn attempt_created_at(db: &Connection, attempt_id: &str) -> Result<Option<i64>> {
    db.query_row(
        "SELECT created_at_ms FROM attempts WHERE attempt_id=?1",
        [attempt_id],
        |row| row.get(0),
    )
    .optional()
    .map(|value| value.flatten())
    .map_err(Into::into)
}

fn native_observation(
    db: &Connection,
    observation_id: i64,
    binding_id: &str,
    generation: i64,
) -> Result<Option<(String, String, Value)>> {
    let row: Option<(String, String, String, i64, i64, Vec<u8>)> = db
        .query_row(
            "SELECT source_stream_id,source_event_key,kind,binding_generation, \
                    length(CAST(payload_json AS BLOB)), \
                    substr(CAST(payload_json AS BLOB),1,?4) \
             FROM observations WHERE observation_id=?1 AND binding_id=?2 \
               AND binding_generation=?3 AND kind='runtime.state' \
               AND length(CAST(payload_json AS BLOB))<=?4 LIMIT 1",
            params![
                observation_id,
                binding_id,
                generation,
                MAX_CAPACITY_LEDGER_BYTES as i64
            ],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()?;
    let Some((stream, key, kind, observed_generation, size, raw)) = row else {
        return Ok(None);
    };
    if kind != "runtime.state"
        || observed_generation != generation
        || size < 0
        || size as usize > MAX_CAPACITY_LEDGER_BYTES
        || raw.len() != size as usize
        || !valid_identity_text(&stream)
        || !valid_identity_text(&key)
    {
        return Ok(None);
    }
    let Ok(state) = serde_json::from_slice::<Value>(&raw) else {
        return Ok(None);
    };
    Ok(Some((stream, key, state)))
}

fn exact_native_turn(state: &Value, session_id: &str, run_id: &str) -> Option<Value> {
    let mut matched: Option<Value> = None;
    let mut consider = |turn: &Value| {
        if turn["sessionId"] == session_id && turn["turnId"] == run_id {
            if matched.as_ref().is_some_and(|previous| previous != turn) {
                return false;
            }
            matched = Some(turn.clone());
        }
        true
    };
    if let Some(turns) = state["turns"].as_array()
        && (turns.len() > MAX_ROSTER_ROWS || !turns.iter().all(&mut consider))
    {
        return None;
    }
    if let Some(children) = state["observed_children"].as_array() {
        if children.len() > MAX_ROSTER_ROWS {
            return None;
        }
        for child in children {
            if !consider(&child["last_turn"]) {
                return None;
            }
        }
    }
    matched.or_else(|| {
        (state["session"]["sessionId"] == session_id && state["session"]["activeTurnId"] == run_id)
            .then(|| json!({"sessionId":session_id,"turnId":run_id,"disposition":"running"}))
    })
}

fn exact_attempt(db: &Connection, op: &Value) -> Result<Option<Value>> {
    let Some(attempt_id) = op["attempt_id"].as_str() else {
        return Ok(None);
    };
    match tasks::get_attempt(db, attempt_id) {
        Ok(attempt) => Ok(Some(attempt)),
        Err(error) if error.code == "NOT_FOUND" => Ok(None),
        Err(error) => Err(error),
    }
}

fn attempt_matches_operation(attempt: &Value, op: &Value) -> bool {
    attempt["attempt_id"] == op["attempt_id"]
        && attempt["task_id"] == op["task_id"]
        && attempt["binding_id"] == op["binding_id"]
        && attempt["binding_generation"] == op["binding_generation"]
        && (op["method"] != "task.dispatch" || attempt["start_operation_id"] == op["operation_id"])
        && validate_attempt(attempt).is_some()
        && validated_producers(attempt).is_some()
}

fn operation_identity(op: &Value) -> Option<(String, String, i64, i64)> {
    let (Some(operation_id), Some(binding_id), Some(generation), Some(admitted_at_ms)) = (
        op["operation_id"].as_str(),
        op["binding_id"].as_str(),
        op["binding_generation"].as_i64(),
        op["created_at_ms"].as_i64(),
    ) else {
        return None;
    };
    if !valid_identity_text(operation_id)
        || !valid_identity_text(binding_id)
        || generation < 1
        || admitted_at_ms < 0
    {
        return None;
    }
    Some((
        operation_id.to_owned(),
        binding_id.to_owned(),
        generation,
        admitted_at_ms,
    ))
}

fn operation_evidence(
    op: &Value,
    phase: DerivedPhase,
    identity: Option<ExecutionIdentity>,
    start_ref: Value,
    release_reason: Option<String>,
    unknown_since_ms: Option<i64>,
) -> Option<ResourceEvidence> {
    let (operation_id, binding_id, binding_generation, admitted_at_ms) = operation_identity(op)?;
    Some(ResourceEvidence {
        entry_id: operation_id.clone(),
        kind: ResourceEntryKind::Operation,
        operation_id: Some(operation_id),
        attempt_id: op["attempt_id"].as_str().map(str::to_owned),
        assignment_id: None,
        method: op["method"].as_str().map(str::to_owned),
        task_id: op["task_id"].as_str().map(str::to_owned),
        binding_id,
        binding_generation,
        admitted_at_ms,
        phase,
        execution_identity: identity,
        execution_start_ref: start_ref,
        release_reason,
        unknown_since_ms,
    })
}

struct ProducerEvidenceContext<'a> {
    attempt_id: &'a str,
    assignment_id: &'a str,
    task_id: &'a str,
    binding_id: &'a str,
    binding_generation: i64,
    admitted_at_ms: i64,
}

struct ProducerEvidenceState {
    phase: DerivedPhase,
    identity: Option<ExecutionIdentity>,
    start_ref: Value,
    release_reason: Option<String>,
    unknown_since_ms: Option<i64>,
}

fn producer_evidence(
    context: &ProducerEvidenceContext<'_>,
    state: ProducerEvidenceState,
) -> ResourceEvidence {
    ResourceEvidence {
        entry_id: format!("producer:{}:{}", context.attempt_id, context.assignment_id),
        kind: ResourceEntryKind::Producer,
        operation_id: None,
        attempt_id: Some(context.attempt_id.to_owned()),
        assignment_id: Some(context.assignment_id.to_owned()),
        method: None,
        task_id: Some(context.task_id.to_owned()),
        binding_id: context.binding_id.to_owned(),
        binding_generation: context.binding_generation,
        admitted_at_ms: context.admitted_at_ms,
        phase: state.phase,
        execution_identity: state.identity,
        execution_start_ref: state.start_ref,
        release_reason: state.release_reason,
        unknown_since_ms: state.unknown_since_ms,
    }
}

fn validate_attempt(attempt: &Value) -> Option<(String, String, String, i64)> {
    let (Some(attempt_id), Some(task_id), Some(binding_id), Some(generation)) = (
        attempt["attempt_id"].as_str(),
        attempt["task_id"].as_str(),
        attempt["binding_id"].as_str(),
        attempt["binding_generation"].as_i64(),
    ) else {
        return None;
    };
    if !valid_identity_text(attempt_id)
        || !valid_identity_text(task_id)
        || !valid_identity_text(binding_id)
        || generation < 1
    {
        return None;
    }
    Some((
        attempt_id.to_owned(),
        task_id.to_owned(),
        binding_id.to_owned(),
        generation,
    ))
}

fn derive_producer_evidence(
    db: &Connection,
    attempt: &Value,
    producer: &Value,
) -> Result<EvidenceDerivation> {
    let Some((attempt_id, task_id, binding_id, generation)) = validate_attempt(attempt) else {
        return damaged_evidence("attempt_identity_invalid", attempt);
    };
    let Some(producers) = validated_producers(attempt) else {
        return damaged_evidence("producer_roster_invalid", &attempt["producers"]);
    };
    let Some(assignment_id) = producer["assignment_id"].as_str() else {
        return damaged_evidence("producer_assignment_missing", producer);
    };
    let matches = producers
        .iter()
        .filter(|candidate| candidate["assignment_id"] == assignment_id)
        .collect::<Vec<_>>();
    if matches.len() != 1 || matches[0] != producer {
        return damaged_evidence("producer_assignment_ambiguous", producer);
    }
    let Some(admitted_at_ms) = attempt_created_at(db, &attempt_id)?.filter(|value| *value >= 0)
    else {
        return damaged_evidence("attempt_admission_time_missing", attempt);
    };
    let evidence_context = ProducerEvidenceContext {
        attempt_id: &attempt_id,
        assignment_id,
        task_id: &task_id,
        binding_id: &binding_id,
        binding_generation: generation,
        admitted_at_ms,
    };
    let disposition = terminal_disposition(&producer["disposition"])
        .or_else(|| terminal_disposition(&producer["execution_disposition"]));
    let unknown = matches!(
        producer["disposition"].as_str(),
        Some("unknown" | "recovery_pending")
    ) || matches!(
        producer["execution_disposition"].as_str(),
        Some("unknown" | "recovery_pending")
    );
    if attempt_resolved(attempt) {
        return Ok(EvidenceDerivation {
            evidence: Some(producer_evidence(
                &evidence_context,
                ProducerEvidenceState {
                    phase: DerivedPhase::Released,
                    identity: None,
                    start_ref: Value::Null,
                    release_reason: Some(format!(
                        "attempt_resolved:{}",
                        attempt["state"].as_str().unwrap_or("released")
                    )),
                    unknown_since_ms: None,
                },
            )),
            damage: None,
        });
    }
    let dispatch_id = producer["dispatch_operation_id"].as_str();
    if let Some(dispatch_id) = dispatch_id {
        if dispatch_id != assignment_id || !valid_identity_text(dispatch_id) {
            return damaged_evidence("producer_dispatch_identity_mismatch", producer);
        }
        let dispatch = match operations::get_operation(db, dispatch_id) {
            Ok(op) => op,
            Err(error) if error.code == "NOT_FOUND" => {
                return damaged_evidence("producer_dispatch_operation_missing", producer);
            }
            Err(error) => return Err(error),
        };
        if dispatch["attempt_id"] != attempt_id
            || dispatch["task_id"] != task_id
            || dispatch["binding_id"] != binding_id
            || dispatch["binding_generation"] != generation
        {
            return damaged_evidence("producer_dispatch_lineage_mismatch", producer);
        }
        if producer["execution_shape"] == crate::runtime::batch::EXECUTION_SHAPE {
            let Some(terminal) = disposition else {
                return damaged_evidence("batch_producer_terminal_missing", producer);
            };
            let details = &dispatch["result"]["details"];
            let expected_state = if terminal == "completed" {
                "settled"
            } else {
                "rejected"
            };
            if !matches!(terminal, "completed" | "failed")
                || dispatch["state"] != expected_state
                || details["execution_shape"] != crate::runtime::batch::EXECUTION_SHAPE
                || details["batch_run_id"] != producer["batch_run_id"]
                || !producer["batch_run_id"]
                    .as_str()
                    .is_some_and(valid_identity_text)
                || dispatch["native_refs"]["execution_shape"]
                    != crate::runtime::batch::EXECUTION_SHAPE
                || dispatch["native_refs"]["dispatch_operation_id"] != dispatch_id
                || dispatch["native_refs"]["batch_run_id"] != producer["batch_run_id"]
                || producer["terminal_evidence"]["completion_condition"]
                    != details["completion_condition"]
                || !details["completion_condition"].is_string()
                || ["exit_code", "result_subtype", "result_sha256"]
                    .into_iter()
                    .any(|field| {
                        producer["terminal_evidence"][field]
                            != details.get(field).cloned().unwrap_or(Value::Null)
                    })
            {
                return damaged_evidence("batch_producer_result_mismatch", producer);
            }
            let native_session_id = match details["native_session_id"].as_str() {
                Some(value) if valid_identity_text(value) => Some(value.to_owned()),
                None if details["native_session_id"].is_null() => None,
                _ => return damaged_evidence("batch_result_session_invalid", details),
            };
            if producer["per_run_native_session_id"].as_str() != native_session_id.as_deref() {
                return damaged_evidence("batch_producer_session_mismatch", producer);
            }
            return Ok(EvidenceDerivation {
                evidence: Some(producer_evidence(
                    &evidence_context,
                    ProducerEvidenceState {
                        phase: DerivedPhase::Released,
                        identity: Some(ExecutionIdentity {
                            operation_id: Some(dispatch_id.to_owned()),
                            native_session_id,
                            native_input_id: None,
                            native_run_id: producer["batch_run_id"].as_str().map(str::to_owned),
                            source_observation_id: None,
                            source_stream_id: None,
                            source_event_key: None,
                            admission_event_ref: None,
                            delivery_event_ref: None,
                            execution_start_event_ref: None,
                            terminal_event_ref: None,
                            terminal_disposition: Some(terminal.to_owned()),
                        }),
                        start_ref: Value::Null,
                        release_reason: Some(format!("execution_terminal:{terminal}")),
                        unknown_since_ms: None,
                    },
                )),
                damage: None,
            });
        }
        let proof = &dispatch["native_refs"]["input_execution"];
        if proof.is_object() {
            let Some((identity, observation_id)) = exact_input_execution(db, &dispatch, proof)?
            else {
                return damaged_evidence("producer_execution_proof_invalid", proof);
            };
            if identity.native_session_id.as_deref() != producer["native_session_id"].as_str()
                || identity.native_input_id.as_deref() != producer["native_input_id"].as_str()
                || identity.native_run_id.as_deref() != producer["native_run_id"].as_str()
                || identity.terminal_disposition.as_deref() != disposition
            {
                return damaged_evidence("producer_execution_identity_mismatch", producer);
            }
            if let Some(terminal) = disposition {
                if producer["terminal_evidence"]["observation_id"]
                    .as_i64()
                    .is_some_and(|id| id != observation_id)
                {
                    return damaged_evidence("producer_terminal_identity_mismatch", producer);
                }
                return Ok(EvidenceDerivation {
                    evidence: Some(producer_evidence(
                        &evidence_context,
                        ProducerEvidenceState {
                            phase: DerivedPhase::Released,
                            identity: Some(identity),
                            start_ref: json!({"kind":"execution_started_event",
                                "event":proof["execution_started"],
                                "native_run_id":proof["native_run_id"]}),
                            release_reason: Some(format!("execution_terminal:{terminal}")),
                            unknown_since_ms: None,
                        },
                    )),
                    damage: None,
                });
            }
            if let Some(run_id) = identity.native_run_id.as_deref() {
                if producer["native_session_id"].as_str() != identity.native_session_id.as_deref()
                    || producer["native_input_id"].as_str() != identity.native_input_id.as_deref()
                    || producer["native_run_id"] != run_id
                {
                    return damaged_evidence("producer_execution_identity_mismatch", producer);
                }
                return Ok(EvidenceDerivation {
                    evidence: Some(producer_evidence(
                        &evidence_context,
                        ProducerEvidenceState {
                            phase: DerivedPhase::Active,
                            identity: Some(identity.clone()),
                            start_ref: json!({"kind":"execution_started_event",
                                "event":proof["execution_started"],
                                "native_run_id":run_id}),
                            release_reason: None,
                            unknown_since_ms: unknown
                                .then_some(dispatch["updated_at_ms"].as_i64().unwrap_or(0)),
                        },
                    )),
                    damage: None,
                });
            }
            return Ok(EvidenceDerivation {
                evidence: Some(producer_evidence(
                    &evidence_context,
                    ProducerEvidenceState {
                        phase: DerivedPhase::Reserved,
                        identity: Some(identity),
                        start_ref: Value::Null,
                        release_reason: None,
                        unknown_since_ms: unknown
                            .then_some(dispatch["updated_at_ms"].as_i64().unwrap_or(0)),
                    },
                )),
                damage: None,
            });
        }
    }
    if let Some(terminal) = disposition {
        let (Some(session_id), Some(run_id), Some(observation_id)) = (
            producer["native_session_id"].as_str(),
            producer["native_run_id"].as_str(),
            producer["terminal_evidence"]["observation_id"].as_i64(),
        ) else {
            return damaged_evidence("producer_terminal_proof_incomplete", producer);
        };
        let Some((_, _, state)) = native_observation(db, observation_id, &binding_id, generation)?
        else {
            return damaged_evidence("producer_terminal_observation_missing", producer);
        };
        let Some(turn) = exact_native_turn(&state, session_id, run_id) else {
            return damaged_evidence("producer_terminal_run_unmatched", producer);
        };
        if turn["terminal"] != terminal
            || producer["terminal_evidence"]["event"] != turn["event"]
            || producer["terminal_evidence"]["view_cursor"].is_i64()
                && producer["terminal_evidence"]["view_cursor"] != turn["viewCursor"]
        {
            return damaged_evidence("producer_terminal_event_mismatch", producer);
        }
        let terminal_ref = exact_event_ref(&turn["event"]);
        if terminal_ref.is_none() {
            return damaged_evidence("producer_terminal_event_invalid", producer);
        }
        let start_observation_id = producer["observed_in"]
            .as_i64()
            .or_else(|| producer["execution_observation_id"].as_i64());
        let Some(start_observation_id) = start_observation_id else {
            return damaged_evidence("producer_start_observation_missing", producer);
        };
        let Some((start_stream, start_key, start_state)) =
            native_observation(db, start_observation_id, &binding_id, generation)?
        else {
            return damaged_evidence("producer_start_observation_invalid", producer);
        };
        let Some(start_turn) = exact_native_turn(&start_state, session_id, run_id) else {
            return damaged_evidence("producer_start_run_unmatched", producer);
        };
        return Ok(EvidenceDerivation {
            evidence: Some(producer_evidence(
                &evidence_context,
                ProducerEvidenceState {
                    phase: DerivedPhase::Released,
                    identity: Some(ExecutionIdentity {
                        operation_id: dispatch_id.map(str::to_owned),
                        native_session_id: Some(session_id.to_owned()),
                        native_input_id: producer["native_input_id"].as_str().map(str::to_owned),
                        native_run_id: Some(run_id.to_owned()),
                        source_observation_id: Some(start_observation_id),
                        source_stream_id: Some(start_stream.clone()),
                        source_event_key: Some(start_key.clone()),
                        admission_event_ref: None,
                        delivery_event_ref: None,
                        execution_start_event_ref: exact_event_ref(&start_turn["event"]),
                        terminal_event_ref: terminal_ref,
                        terminal_disposition: Some(terminal.to_owned()),
                    }),
                    start_ref: json!({"kind":"native_turn","turn_id":run_id,
                        "native_session_id":session_id,"observation_id":start_observation_id,
                        "source_stream_id":start_stream,"source_event_key":start_key}),
                    release_reason: Some(format!("execution_terminal:{terminal}")),
                    unknown_since_ms: None,
                },
            )),
            damage: None,
        });
    }
    if let Some(run_id) = producer["native_run_id"].as_str() {
        let Some(session_id) = producer["native_session_id"].as_str() else {
            return damaged_evidence("producer_run_session_missing", producer);
        };
        let Some(observation_id) = producer["observed_in"]
            .as_i64()
            .or_else(|| producer["execution_observation_id"].as_i64())
        else {
            return damaged_evidence("producer_run_observation_missing", producer);
        };
        let Some((stream, key, state)) =
            native_observation(db, observation_id, &binding_id, generation)?
        else {
            return damaged_evidence("producer_run_observation_invalid", producer);
        };
        let Some(turn) = exact_native_turn(&state, session_id, run_id) else {
            return damaged_evidence("producer_run_unmatched", producer);
        };
        let identity = ExecutionIdentity {
            operation_id: producer["dispatch_operation_id"]
                .as_str()
                .map(str::to_owned),
            native_session_id: Some(session_id.to_owned()),
            native_input_id: producer["native_input_id"].as_str().map(str::to_owned),
            native_run_id: Some(run_id.to_owned()),
            source_observation_id: Some(observation_id),
            source_stream_id: Some(stream.clone()),
            source_event_key: Some(key.clone()),
            admission_event_ref: None,
            delivery_event_ref: None,
            execution_start_event_ref: exact_event_ref(&turn["event"]),
            terminal_event_ref: None,
            terminal_disposition: None,
        };
        return Ok(EvidenceDerivation {
            evidence: Some(producer_evidence(
                &evidence_context,
                ProducerEvidenceState {
                    phase: DerivedPhase::Active,
                    identity: Some(identity),
                    start_ref: json!({"kind":"native_turn","turn_id":run_id,
                        "native_session_id":session_id,"observation_id":observation_id,
                        "source_stream_id":stream,"source_event_key":key}),
                    release_reason: None,
                    unknown_since_ms: unknown.then_some(0),
                },
            )),
            damage: None,
        });
    }
    if producer["native_session_id"].is_string()
        && producer["disposition"]
            .as_str()
            .is_none_or(|value| value == "admitted")
    {
        let session_id = producer["native_session_id"].as_str().unwrap_or_default();
        if !valid_identity_text(session_id) {
            return damaged_evidence("producer_session_invalid", producer);
        }
    }
    Ok(EvidenceDerivation {
        evidence: Some(producer_evidence(
            &evidence_context,
            ProducerEvidenceState {
                phase: DerivedPhase::Reserved,
                identity: None,
                start_ref: Value::Null,
                release_reason: None,
                unknown_since_ms: unknown.then_some(0),
            },
        )),
        damage: None,
    })
}

/// Re-derives operation state and its exact execution source through one
/// function shared with the roster check. Invalid source facts are evidence
/// gaps; they never become a release or an empty reservation.
fn derive_operation_evidence(db: &Connection, op: &Value) -> Result<EvidenceDerivation> {
    let Some((operation_id, _, _, _)) = operation_identity(op) else {
        return damaged_evidence("operation_identity_invalid", op);
    };
    if !is_admission(op["method"].as_str().unwrap_or_default())
        || !matches!(
            op["state"].as_str(),
            Some(
                "queued"
                    | "sending"
                    | "native_accepted"
                    | "outcome_unknown"
                    | "settled"
                    | "rejected"
                    | "cancelled"
            )
        )
    {
        return damaged_evidence("operation_state_or_method_invalid", op);
    }
    let attempt = exact_attempt(db, op)?;
    if op["method"] == "task.dispatch"
        && attempt
            .as_ref()
            .is_none_or(|attempt| !attempt_matches_operation(attempt, op))
    {
        return damaged_evidence("dispatch_attempt_link_invalid", op);
    }
    if let Some(attempt) = &attempt
        && (!attempt_matches_operation(attempt, op) || validated_producers(attempt).is_none())
    {
        return damaged_evidence("operation_attempt_roster_invalid", attempt);
    }
    let unknown = op["state"] == "outcome_unknown";
    let proof = &op["native_refs"]["input_execution"];
    let mut identity: Option<ExecutionIdentity> = None;
    let mut start_ref = Value::Null;
    let mut phase = DerivedPhase::Reserved;
    let mut release_reason = None;
    let mut unknown_since_ms = unknown.then(|| op["updated_at_ms"].as_i64().unwrap_or(0));
    if !proof.is_null() {
        if !proof.is_object() {
            return damaged_evidence("operation_execution_proof_shape_invalid", proof);
        }
        let Some((proof_identity, _)) = exact_input_execution(db, op, proof)? else {
            return damaged_evidence("operation_execution_proof_invalid", proof);
        };
        if let Some(terminal) = proof_identity.terminal_disposition.as_deref() {
            phase = DerivedPhase::Released;
            release_reason = Some(format!("execution_terminal:{terminal}"));
            unknown_since_ms = None;
        } else if let Some(run_id) = proof_identity.native_run_id.as_deref() {
            phase = DerivedPhase::Active;
            start_ref = json!({"kind":"execution_started_event",
                "event":proof["execution_started"],"native_run_id":run_id,
                "native_session_id":proof_identity.native_session_id,
                "native_input_id":proof_identity.native_input_id,
                "observation_id":proof_identity.source_observation_id});
            unknown_since_ms = unknown.then(|| op["updated_at_ms"].as_i64().unwrap_or(0));
        }
        identity = Some(proof_identity);
    }
    if phase == DerivedPhase::Reserved
        && op["method"] == "task.dispatch"
        && let Some(attempt) = &attempt
    {
        if let Some(producers) = validated_producers(attempt) {
            let matches = producers
                .iter()
                .filter(|producer| producer["assignment_id"] == operation_id)
                .collect::<Vec<_>>();
            if matches.len() > 1 {
                return damaged_evidence("dispatch_producer_ambiguous", attempt);
            }
            if let Some(producer) = matches.first() {
                let derived = derive_producer_evidence(db, attempt, producer)?;
                let Some(producer_evidence) = derived.evidence else {
                    return Ok(derived);
                };
                match producer_evidence.phase {
                    DerivedPhase::Released => {
                        phase = DerivedPhase::Released;
                        release_reason = producer_evidence.release_reason;
                        identity = producer_evidence.execution_identity;
                        unknown_since_ms = None;
                    }
                    DerivedPhase::Active => {
                        phase = DerivedPhase::Active;
                        identity = producer_evidence.execution_identity;
                        start_ref = producer_evidence.execution_start_ref;
                    }
                    DerivedPhase::Reserved => {}
                }
            }
        }
        if phase != DerivedPhase::Released && attempt_resolved(attempt) {
            phase = DerivedPhase::Released;
            release_reason = Some(format!(
                "attempt_resolved:{}",
                attempt["state"].as_str().unwrap_or("released")
            ));
            unknown_since_ms = None;
        }
    }
    if phase == DerivedPhase::Reserved && !matches!(op["native_refs"]["turn_id"], Value::Null) {
        let (Some(session_id), Some(run_id)) = (
            op["native_refs"]["session_id"].as_str(),
            op["native_refs"]["turn_id"].as_str(),
        ) else {
            return damaged_evidence("operation_native_turn_identity_invalid", &op["native_refs"]);
        };
        if !valid_identity_text(session_id) || !valid_identity_text(run_id) {
            return damaged_evidence("operation_native_turn_identity_invalid", &op["native_refs"]);
        }
        identity = Some(ExecutionIdentity {
            operation_id: Some(operation_id.clone()),
            native_session_id: Some(session_id.to_owned()),
            native_input_id: op["native_refs"]["input_id"].as_str().map(str::to_owned),
            native_run_id: Some(run_id.to_owned()),
            source_observation_id: None,
            source_stream_id: None,
            source_event_key: None,
            admission_event_ref: None,
            delivery_event_ref: None,
            execution_start_event_ref: None,
            terminal_event_ref: None,
            terminal_disposition: None,
        });
        phase = DerivedPhase::Active;
        start_ref = json!({"kind":"native_turn","turn_id":run_id,
            "native_session_id":session_id});
    }
    if phase == DerivedPhase::Reserved {
        match op["state"].as_str() {
            Some("cancelled") => {
                phase = DerivedPhase::Released;
                release_reason = Some("operation_cancelled".into());
                unknown_since_ms = None;
            }
            Some("rejected") => {
                phase = DerivedPhase::Released;
                release_reason = Some("operation_rejected".into());
                unknown_since_ms = None;
            }
            Some("settled") if op["method"] != "task.dispatch" => {
                phase = DerivedPhase::Released;
                release_reason = Some("operation_settled".into());
                unknown_since_ms = None;
            }
            _ => {}
        }
    }
    let Some(evidence) = operation_evidence(
        op,
        phase,
        identity,
        start_ref,
        release_reason,
        unknown_since_ms,
    ) else {
        return damaged_evidence("operation_evidence_invalid", op);
    };
    Ok(EvidenceDerivation {
        evidence: Some(evidence),
        damage: None,
    })
}

fn same_execution_identity(left: &ExecutionIdentity, right: &ExecutionIdentity) -> bool {
    let compatible = |left: &Option<String>, right: &Option<String>| {
        left.as_ref()
            .zip(right.as_ref())
            .is_none_or(|(left, right)| left == right)
    };
    left.operation_id == right.operation_id
        && compatible(&left.native_session_id, &right.native_session_id)
        && compatible(&left.native_input_id, &right.native_input_id)
        && compatible(&left.native_run_id, &right.native_run_id)
        && left
            .execution_start_event_ref
            .as_ref()
            .zip(right.execution_start_event_ref.as_ref())
            .is_none_or(|(left, right)| left == right)
}

fn enrich_execution_identity(
    current: Option<ExecutionIdentity>,
    incoming: Option<ExecutionIdentity>,
) -> Option<ExecutionIdentity> {
    let (mut current, incoming) = match (current, incoming) {
        (Some(current), Some(incoming)) => (current, incoming),
        (current, incoming) => return current.or(incoming),
    };
    current.native_session_id = incoming.native_session_id.or(current.native_session_id);
    current.native_input_id = incoming.native_input_id.or(current.native_input_id);
    current.native_run_id = incoming.native_run_id.or(current.native_run_id);
    current.source_observation_id = incoming
        .source_observation_id
        .or(current.source_observation_id);
    current.source_stream_id = incoming.source_stream_id.or(current.source_stream_id);
    current.source_event_key = incoming.source_event_key.or(current.source_event_key);
    current.admission_event_ref = incoming.admission_event_ref.or(current.admission_event_ref);
    current.delivery_event_ref = incoming.delivery_event_ref.or(current.delivery_event_ref);
    current.execution_start_event_ref = incoming
        .execution_start_event_ref
        .or(current.execution_start_event_ref);
    current.terminal_event_ref = incoming.terminal_event_ref.or(current.terminal_event_ref);
    current.terminal_disposition = incoming
        .terminal_disposition
        .or(current.terminal_disposition);
    Some(current)
}

fn entry_identity_matches(entry: &CapacityEntry, evidence: &ResourceEvidence) -> bool {
    entry.entry_id == evidence.entry_id
        && entry.kind == evidence.kind
        && entry.operation_id == evidence.operation_id
        && entry.assignment_id == evidence.assignment_id
        && entry.method == evidence.method
        && entry.binding_id == evidence.binding_id
        && entry.binding_generation == evidence.binding_generation
        && entry.admitted_at_ms == evidence.admitted_at_ms
        && (entry.task_id.is_none()
            || evidence.task_id.is_none()
            || entry.task_id == evidence.task_id)
        && (entry.attempt_id.is_none()
            || evidence.attempt_id.is_none()
            || entry.attempt_id == evidence.attempt_id)
        && match (&entry.execution_identity, &evidence.execution_identity) {
            (Some(left), Some(right)) => same_execution_identity(left, right),
            _ => true,
        }
}

fn entry_from_evidence(evidence: &ResourceEvidence, now: i64) -> CapacityEntry {
    CapacityEntry {
        entry_id: evidence.entry_id.clone(),
        kind: evidence.kind,
        operation_id: evidence.operation_id.clone(),
        attempt_id: evidence.attempt_id.clone(),
        assignment_id: evidence.assignment_id.clone(),
        method: evidence.method.clone(),
        task_id: evidence.task_id.clone(),
        binding_id: evidence.binding_id.clone(),
        binding_generation: evidence.binding_generation,
        phase: evidence.phase,
        native_session_id: evidence
            .execution_identity
            .as_ref()
            .and_then(|identity| identity.native_session_id.clone()),
        admitted_at_ms: evidence.admitted_at_ms,
        activated_at_ms: (evidence.phase == DerivedPhase::Active).then_some(now),
        released_at_ms: (evidence.phase == DerivedPhase::Released).then_some(now),
        release_reason: evidence.release_reason.clone(),
        execution_start_ref: evidence.execution_start_ref.clone(),
        execution_identity: evidence.execution_identity.clone(),
        outcome_unknown_since_ms: evidence.unknown_since_ms,
        last_synced_at_ms: now,
    }
}

/// Merge only exact evidence. A lower phase cannot rewrite an observed
/// active/released fact, and changed source identity is reported as a gap.
fn merge_entry(entry: &mut CapacityEntry, evidence: &ResourceEvidence, now: i64) -> MergeResult {
    if !entry_identity_matches(entry, evidence) {
        return MergeResult::Conflict;
    }
    if entry.phase == DerivedPhase::Released {
        entry.last_synced_at_ms = now;
        return MergeResult::PreservedHigherPhase;
    }
    if entry.phase == DerivedPhase::Active && evidence.phase == DerivedPhase::Released {
        let Some((current, next)) = entry
            .execution_identity
            .as_ref()
            .zip(evidence.execution_identity.as_ref())
        else {
            return MergeResult::Conflict;
        };
        if !same_execution_identity(current, next) {
            return MergeResult::Conflict;
        }
    }
    match evidence.phase {
        DerivedPhase::Released => {
            entry.phase = DerivedPhase::Released;
            entry.released_at_ms = Some(now);
            entry.release_reason = evidence.release_reason.clone();
            entry.outcome_unknown_since_ms = None;
            if evidence.execution_identity.is_some() {
                entry.execution_identity = enrich_execution_identity(
                    entry.execution_identity.clone(),
                    evidence.execution_identity.clone(),
                );
            }
        }
        DerivedPhase::Active => {
            if entry.phase == DerivedPhase::Reserved {
                entry.activated_at_ms = Some(now);
                entry.phase = DerivedPhase::Active;
            }
            if evidence.execution_identity.is_some() {
                entry.execution_identity = enrich_execution_identity(
                    entry.execution_identity.clone(),
                    evidence.execution_identity.clone(),
                );
                entry.execution_start_ref = evidence.execution_start_ref.clone();
            }
            entry.outcome_unknown_since_ms = evidence.unknown_since_ms;
        }
        DerivedPhase::Reserved if entry.phase == DerivedPhase::Reserved => {
            if let Some(identity) = &evidence.execution_identity {
                entry.execution_identity = enrich_execution_identity(
                    entry.execution_identity.clone(),
                    Some(identity.clone()),
                );
            }
            entry.outcome_unknown_since_ms = evidence.unknown_since_ms;
        }
        DerivedPhase::Reserved => {
            if entry.outcome_unknown_since_ms.is_none() {
                entry.outcome_unknown_since_ms = evidence.unknown_since_ms;
            }
        }
    }
    if entry.task_id.is_none() {
        entry.task_id = evidence.task_id.clone();
    }
    if entry.attempt_id.is_none() {
        entry.attempt_id = evidence.attempt_id.clone();
    }
    entry.native_session_id = entry
        .execution_identity
        .as_ref()
        .and_then(|identity| identity.native_session_id.clone());
    entry.last_synced_at_ms = now;
    MergeResult::Applied
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

fn sync_evidence(
    db: &Connection,
    scope: &Value,
    derivation: EvidenceDerivation,
    now: i64,
) -> Result<()> {
    let (Some(evidence), None) = (derivation.evidence, derivation.damage) else {
        // Projection damage is retained in its authoritative source and
        // reported by the shared roster derivation. It must not roll back
        // the native effect that caused this sync hook to run.
        return Ok(());
    };
    let Some(scope_key) = scope["scope_key"].as_str() else {
        return Ok(());
    };
    let mut ledger = match load_ledger(db, scope)? {
        LedgerLoad::Missing => CapacityLedger {
            schema_version: CAPACITY_LEDGER_SCHEMA_VERSION,
            scope: scope.clone(),
            entries: BTreeMap::new(),
            updated_at_ms: now,
        },
        LedgerLoad::Valid(ledger) => ledger,
        LedgerLoad::Damaged(_) => return Ok(()),
    };
    if ledger.scope != *scope {
        return Ok(());
    }
    match ledger.entries.get_mut(&evidence.entry_id) {
        Some(entry) => {
            if merge_entry(entry, &evidence, now) == MergeResult::Conflict {
                return Ok(());
            }
        }
        None => {
            if ledger.entries.len() >= MAX_CAPACITY_LEDGER_ENTRIES {
                return Ok(());
            }
            ledger.entries.insert(
                evidence.entry_id.clone(),
                entry_from_evidence(&evidence, now),
            );
        }
    }
    ledger.updated_at_ms = now;
    // `scope_key` was validated by load_ledger and is kept here to make the
    // metadata write's identity explicit at the call site.
    if ledger.scope["scope_key"] != scope_key {
        return Ok(());
    }
    store_ledger(db, &ledger)
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
    let derived = derive_operation_evidence(db, &op)?;
    sync_evidence(db, &scope, derived, now)
}

/// Re-derives the ledger entry for one producer registered on an
/// Attempt (native-manager-started work has no dispatch operation).
pub(super) fn sync_producer(
    db: &Connection,
    attempt: &Value,
    producer: &Value,
    now: i64,
) -> Result<()> {
    let Some((_, _, binding_id, generation)) = validate_attempt(attempt) else {
        return Ok(());
    };
    let Some(scope) = binding_scope(db, &binding_id, generation)? else {
        return Ok(());
    };
    let derived = derive_producer_evidence(db, attempt, producer)?;
    sync_evidence(db, &scope, derived, now)
}

/// Re-derives every entry fed by one Attempt: its operations and its
/// registered producers.
pub(super) fn sync_attempt(db: &Connection, attempt_id: &str, now: i64) -> Result<()> {
    let attempt = match tasks::get_attempt(db, attempt_id) {
        Ok(a) => a,
        Err(e) if e.code == "NOT_FOUND" => return Ok(()),
        Err(e) => return Err(e),
    };
    if validated_producers(&attempt).is_none() {
        return Ok(());
    }
    let mut stmt = db.prepare(
        "SELECT operation_id FROM operations WHERE attempt_id=?1 ORDER BY operation_id LIMIT ?2",
    )?;
    let op_ids = stmt
        .query_map(params![attempt_id, MAX_ROSTER_SQL_ROWS], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    if op_ids.len() > MAX_ROSTER_ROWS {
        return Ok(());
    }
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
        "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 \
         ORDER BY operation_id LIMIT ?3",
    )?;
    let op_ids = stmt
        .query_map(params![binding_id, generation, MAX_ROSTER_SQL_ROWS], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    if op_ids.len() > MAX_ROSTER_ROWS {
        return Ok(());
    }
    for op_id in op_ids {
        sync_operation(db, &op_id, now)?;
    }
    let mut stmt = db.prepare(
        "SELECT attempt_id FROM attempts WHERE binding_id=?1 AND binding_generation=?2 \
         ORDER BY attempt_id LIMIT ?3",
    )?;
    let attempt_ids = stmt
        .query_map(params![binding_id, generation, MAX_ROSTER_SQL_ROWS], |r| {
            r.get::<_, String>(0)
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    if attempt_ids.len() > MAX_ROSTER_ROWS {
        return Ok(());
    }
    for attempt_id in attempt_ids {
        sync_attempt(db, &attempt_id, now)?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Legacy quota incident history
// ---------------------------------------------------------------------------

/// Runtime outcomes are not typed provider-condition evidence. Keep this
/// hook for the shared outcome transaction without changing legacy incidents;
/// current provider-condition state is owned by `provider_conditions`.
pub(super) fn note_outcome(
    _db: &Connection,
    _op: &Value,
    _outcome: &RuntimeOutcome,
    _now: i64,
) -> Result<()> {
    Ok(())
}

fn historical_quota_incident(db: &Connection, scope_key: &str) -> Result<Option<Value>> {
    let dedup = format!("quota:{scope_key}");
    let row: Option<(String, String, i64, String, i64, i64)> = db
        .query_row(
            "SELECT incident_id,state,occurrences,details_json,opened_at_ms,last_seen_at_ms FROM incidents WHERE dedup_key=?1 ORDER BY last_seen_at_ms DESC,opened_at_ms DESC,incident_id DESC LIMIT 1",
            [&dedup],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?)),
        )
        .optional()?;
    match row {
        Some((id, state, occurrences, raw, opened, seen)) => {
            let details: Value = serde_json::from_str(&raw)?;
            Ok(Some(json!({
                "incident_id": id,
                "state": state,
                "historical": true,
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
    entries: Vec<CapacityEntry>,
    ledger_updated_at_ms: i64,
    damage: Option<CapacityLedgerDamage>,
}

fn damage_value(damage: &CapacityLedgerDamage) -> Value {
    json!({
        "code": damage.code,
        "raw_digest": damage.raw_digest,
        "raw_size_bytes": damage.raw_size_bytes,
        "digest_complete": damage.digest_complete,
    })
}

fn empty_scope(scope_key: &str) -> Value {
    json!({"scope_key":scope_key,"runtime":null,"provider":null,
        "account":null,"service":null,"native_scope_key":null,
        "route_alias":null,"identity":"partial"})
}

fn collect_scopes(db: &Connection) -> Result<BTreeMap<String, ScopeAgg>> {
    let mut scopes: BTreeMap<String, ScopeAgg> = BTreeMap::new();
    let mut stmt = db.prepare(
        "SELECT binding_id,generation FROM bindings ORDER BY binding_id,generation LIMIT ?1",
    )?;
    let keys = stmt
        .query_map([MAX_ROSTER_SQL_ROWS], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    let binding_overflow = keys.len() > MAX_ROSTER_ROWS;
    for (binding_id, generation) in keys.into_iter().take(MAX_ROSTER_ROWS) {
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
            damage: None,
        });
        if agg.scope["native_scope_key"].is_null() && !scope["native_scope_key"].is_null() {
            agg.scope["native_scope_key"] = scope["native_scope_key"].clone();
        } else if !agg.scope["native_scope_key"].is_null()
            && !scope["native_scope_key"].is_null()
            && agg.scope["native_scope_key"] != scope["native_scope_key"]
        {
            agg.damage.get_or_insert(damage_for_value(
                "scope_native_identity_conflict",
                &json!([agg.scope["native_scope_key"], scope["native_scope_key"]]),
            )?);
        }
        agg.bindings.push(binding);
    }
    let mut global_damage =
        binding_overflow.then(|| damage_for_raw("binding_roster_limit_exceeded", &[], 0));
    for row in all_ledgers(db)? {
        match row {
            LedgerRow::Valid(scope_key, ledger) => {
                let agg = scopes.entry(scope_key.clone()).or_insert_with(|| ScopeAgg {
                    scope: ledger.scope.clone(),
                    bindings: Vec::new(),
                    entries: Vec::new(),
                    ledger_updated_at_ms: 0,
                    damage: None,
                });
                if agg.scope["scope_key"] != ledger.scope["scope_key"]
                    || (!agg.bindings.is_empty()
                        && agg.scope["native_scope_key"].is_string()
                        && ledger.scope["native_scope_key"].is_string()
                        && agg.scope["native_scope_key"] != ledger.scope["native_scope_key"])
                {
                    agg.damage.get_or_insert(damage_for_value(
                        "ledger_scope_identity_mismatch",
                        &ledger.scope,
                    )?);
                }
                agg.entries = ledger.entries.into_values().collect();
                agg.entries
                    .sort_by(|left, right| left.entry_id.cmp(&right.entry_id));
                agg.ledger_updated_at_ms = ledger.updated_at_ms;
            }
            LedgerRow::Damaged(scope_key, damage) => {
                if matches!(
                    scope_key.as_str(),
                    "unknown:damaged-ledger-key" | "unknown:capacity-ledger-roster-limit"
                ) {
                    global_damage.get_or_insert(damage);
                    continue;
                }
                let agg = scopes.entry(scope_key.clone()).or_insert_with(|| ScopeAgg {
                    scope: empty_scope(&scope_key),
                    bindings: Vec::new(),
                    entries: Vec::new(),
                    ledger_updated_at_ms: 0,
                    damage: None,
                });
                agg.damage = Some(damage);
            }
        }
    }
    if let Some(damage) = global_damage {
        for agg in scopes.values_mut() {
            agg.damage.get_or_insert_with(|| damage.clone());
        }
        let key = "unknown:capacity-roster-limit".to_owned();
        scopes.entry(key.clone()).or_insert_with(|| ScopeAgg {
            scope: empty_scope(&key),
            bindings: Vec::new(),
            entries: Vec::new(),
            ledger_updated_at_ms: 0,
            damage: Some(damage),
        });
    }
    Ok(scopes)
}

fn is_writer_entry(entry: &CapacityEntry) -> bool {
    entry.kind == ResourceEntryKind::Producer
        || matches!(
            entry.method.as_deref(),
            Some("task.dispatch" | "native.opencode.loop_step")
        )
}

fn scope_counts(entries: &[CapacityEntry]) -> Value {
    let mut counts = json!({
        "reserved": 0, "active": 0, "unknown_outcomes": 0,
        "desired_writers": 0, "effective_writers": 0, "pending_admissions": 0,
        "commands_in_flight": 0, "released_entries": 0,
    });
    for entry in entries {
        if entry.phase == DerivedPhase::Released {
            counts["released_entries"] = json!(counts["released_entries"].as_i64().unwrap() + 1);
            continue;
        }
        match entry.phase {
            DerivedPhase::Reserved => {
                counts["reserved"] = json!(counts["reserved"].as_i64().unwrap() + 1)
            }
            DerivedPhase::Active => {
                counts["active"] = json!(counts["active"].as_i64().unwrap() + 1)
            }
            DerivedPhase::Released => unreachable!(),
        }
        if entry.outcome_unknown_since_ms.is_some() {
            counts["unknown_outcomes"] = json!(counts["unknown_outcomes"].as_i64().unwrap() + 1);
        }
        if is_writer_entry(entry) {
            counts["desired_writers"] = json!(counts["desired_writers"].as_i64().unwrap() + 1);
            if entry.phase == DerivedPhase::Active {
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

fn ledger_matches_evidence(entry: &CapacityEntry, evidence: &ResourceEvidence) -> bool {
    entry_identity_matches(entry, evidence)
        && entry.phase == evidence.phase
        && entry.execution_identity == evidence.execution_identity
        && entry.execution_start_ref == evidence.execution_start_ref
        && entry.release_reason == evidence.release_reason
        && entry.outcome_unknown_since_ms == evidence.unknown_since_ms
}

fn gap_reason(code: &str, damage: &CapacityLedgerDamage) -> String {
    format!("{code}:{}:{}", damage.raw_size_bytes, damage.raw_digest)
}

/// Verifies expected sources against the typed ledger in both directions.
/// The operation derivation is exactly the same path used by lifecycle sync.
fn roster_check(db: &Connection, agg: &ScopeAgg) -> Result<(bool, Option<String>)> {
    roster_check_inner(db, agg, true)
}

/// Root admission can also address a known service-less route. Those routes
/// retain binding-scoped ledger keys, so validate their complete evidence
/// roster while permitting the scope identity itself to remain partial.
fn root_claim_roster_check(db: &Connection, agg: &ScopeAgg) -> Result<(bool, Option<String>)> {
    roster_check_inner(db, agg, false)
}

fn roster_check_inner(
    db: &Connection,
    agg: &ScopeAgg,
    require_complete_scope: bool,
) -> Result<(bool, Option<String>)> {
    if let Some(damage) = &agg.damage {
        return Ok((false, Some(gap_reason(&damage.code, damage))));
    }
    if require_complete_scope && agg.scope["identity"] != "complete" {
        return Ok((false, Some("scope_identity_partial".into())));
    }
    if agg.bindings.is_empty() {
        return Ok((false, Some("no_recorded_binding".into())));
    }
    let mut expected = BTreeMap::<String, ResourceEvidence>::new();
    let mut visited_rows = 0usize;
    for binding in &agg.bindings {
        let (Some(binding_id), Some(generation)) = (
            binding["binding_id"].as_str(),
            binding["generation"].as_i64(),
        ) else {
            return Ok((false, Some("binding_identity_invalid".into())));
        };
        let mut stmt = db.prepare(
            "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 \
             ORDER BY operation_id LIMIT ?3",
        )?;
        let op_ids = stmt
            .query_map(
                params![binding_id, generation, MAX_ROSTER_SQL_ROWS],
                |row| row.get::<_, String>(0),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        visited_rows = visited_rows.saturating_add(op_ids.len());
        if op_ids.len() > MAX_ROSTER_ROWS || visited_rows > MAX_ROSTER_ROWS {
            return Ok((false, Some("operation_roster_limit_exceeded".into())));
        }
        for op_id in op_ids {
            let op = operations::get_operation(db, &op_id)?;
            if !is_admission(op["method"].as_str().unwrap_or_default()) {
                continue;
            }
            let derived = derive_operation_evidence(db, &op)?;
            let Some(evidence) = derived.evidence else {
                let damage = derived
                    .damage
                    .unwrap_or_else(|| damage_for_raw("operation_evidence_missing", &[], 0));
                return Ok((
                    false,
                    Some(gap_reason("operation_evidence_damaged", &damage)),
                ));
            };
            if evidence.binding_id != binding_id || evidence.binding_generation != generation {
                return Ok((false, Some("operation_binding_lineage_mismatch".into())));
            }
            if expected
                .insert(evidence.entry_id.clone(), evidence)
                .is_some()
            {
                return Ok((false, Some("duplicate_operation_identity".into())));
            }
        }
        let mut stmt = db.prepare(
            "SELECT attempt_id FROM attempts WHERE binding_id=?1 AND binding_generation=?2 \
             ORDER BY attempt_id LIMIT ?3",
        )?;
        let attempt_ids = stmt
            .query_map(
                params![binding_id, generation, MAX_ROSTER_SQL_ROWS],
                |row| row.get::<_, String>(0),
            )?
            .collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        visited_rows = visited_rows.saturating_add(attempt_ids.len());
        if attempt_ids.len() > MAX_ROSTER_ROWS || visited_rows > MAX_ROSTER_ROWS {
            return Ok((false, Some("attempt_roster_limit_exceeded".into())));
        }
        for attempt_id in attempt_ids {
            let attempt = tasks::get_attempt(db, &attempt_id)?;
            let Some((source_attempt, _, source_binding, source_generation)) =
                validate_attempt(&attempt)
            else {
                let damage = damage_for_value("attempt_identity_invalid", &attempt)?;
                return Ok((false, Some(gap_reason("attempt_evidence_damaged", &damage))));
            };
            if source_attempt != attempt_id
                || source_binding != binding_id
                || source_generation != generation
            {
                return Ok((false, Some("attempt_binding_lineage_mismatch".into())));
            }
            let Some(producers) = validated_producers(&attempt) else {
                let damage = damage_for_value("producer_roster_invalid", &attempt["producers"])?;
                return Ok((
                    false,
                    Some(gap_reason("producer_evidence_damaged", &damage)),
                ));
            };
            visited_rows = visited_rows.saturating_add(producers.len());
            if visited_rows > MAX_ROSTER_ROWS {
                return Ok((false, Some("producer_roster_limit_exceeded".into())));
            }
            for producer in producers {
                let derived = derive_producer_evidence(db, &attempt, producer)?;
                let Some(evidence) = derived.evidence else {
                    let damage = derived
                        .damage
                        .unwrap_or_else(|| damage_for_raw("producer_evidence_missing", &[], 0));
                    return Ok((
                        false,
                        Some(gap_reason("producer_evidence_damaged", &damage)),
                    ));
                };
                if expected
                    .insert(evidence.entry_id.clone(), evidence)
                    .is_some()
                {
                    return Ok((false, Some("duplicate_producer_identity".into())));
                }
            }
        }
        // Native activity outside the retained family remains unknown.
        let native = &binding["observation"]["native"];
        if native.is_object() {
            let children = native["observed_children"].as_array();
            if children.is_some_and(|children| children.len() > MAX_ROSTER_ROWS) {
                return Ok((false, Some("native_roster_limit_exceeded".into())));
            }
            let mut family = BTreeMap::<&str, ()>::new();
            if let Some(root) = native["native_root_id"].as_str() {
                family.insert(root, ());
            }
            if let Some(children) = children {
                for child in children {
                    if let Some(id) = child["sessionId"].as_str() {
                        family.insert(id, ());
                    }
                }
                for child in children {
                    if child["execution_disposition"] == "running"
                        && child["observed_now"] == true
                        && let Some(parent) = child["parentSessionId"].as_str()
                        && !family.contains_key(parent)
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
    let mut ledger = BTreeMap::<&str, &CapacityEntry>::new();
    for entry in &agg.entries {
        if ledger.insert(&entry.entry_id, entry).is_some() {
            return Ok((false, Some("duplicate_ledger_entry_identity".into())));
        }
    }
    for (entry_id, evidence) in &expected {
        let Some(entry) = ledger.get(entry_id.as_str()) else {
            return Ok((false, Some(format!("ledger_entry_missing:{entry_id}"))));
        };
        if !ledger_matches_evidence(entry, evidence) {
            return Ok((false, Some(format!("ledger_entry_diverged:{entry_id}"))));
        }
    }
    if let Some(orphan) = ledger
        .keys()
        .find(|entry_id| !expected.contains_key(**entry_id))
    {
        return Ok((false, Some(format!("ledger_entry_orphaned:{orphan}"))));
    }
    Ok((true, None))
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RootClaimIdentity {
    NativeRoot {
        native_scope_key: String,
        native_root_id: String,
    },
    Binding {
        binding_id: String,
        generation: i64,
    },
    LaunchOperation {
        operation_id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RootRouteIdentity {
    route_alias: String,
    runtime: String,
    module_artifact_id: String,
    provider: Option<String>,
    model: Option<String>,
}

enum LaunchRouteMatch {
    OtherRoute,
    Unknown,
    Matched(Option<(String, i64)>),
}

struct LaunchRouteOperation<'a> {
    operation_id: &'a str,
    state: &'a str,
    task_id: Option<&'a str>,
    attempt_id: Option<&'a str>,
    binding_id: Option<&'a str>,
    binding_generation: Option<i64>,
    original_json: &'a str,
    effective_json: &'a str,
    result_json: Option<&'a str>,
}

struct LaunchRouteTarget<'a> {
    route_digest: &'a str,
    route_alias: &'a str,
}

#[derive(Debug, Clone)]
struct RootBindingClaim {
    binding_id: String,
    generation: i64,
    scope: Value,
    route_identity: Option<RootRouteIdentity>,
    identity: RootClaimIdentity,
    released: bool,
    state: String,
}

fn same_route_scope_facts(left: &Value, right: &Value) -> bool {
    [
        "scope_key",
        "runtime",
        "provider",
        "account",
        "service",
        "route_alias",
        "identity",
    ]
    .iter()
    .all(|field| left[*field] == right[*field])
}

fn root_route_text(value: Option<&Value>) -> Option<String> {
    root_route_component(value?.as_str()?)
}

fn root_route_component(value: &str) -> Option<String> {
    if !valid_identity_text(value) || value.chars().any(char::is_control) {
        return None;
    }
    Some(value.to_owned())
}

/// Service-less routes deliberately keep their historical binding-scoped
/// ScopeKey. This identity is only for exact route attribution during root
/// admission; it never becomes a replacement ledger namespace.
fn root_route_identity(route: &Value) -> Option<RootRouteIdentity> {
    let options = match route.get("native_options")? {
        Value::Null => None,
        Value::Object(options) => Some(options),
        _ => return None,
    };
    if options.is_some_and(|options| options.contains_key("service_id")) {
        return None;
    }
    let route_alias = root_route_text(route.get("alias"))?;
    let runtime = root_route_text(route.get("runtime"))?;
    let module_artifact_id = root_route_text(route.get("module_artifact_id"))?;
    // Known standalone routes add their validated provider/model dimensions.
    // Generic optionless routes are still attributable by their exact retained
    // alias/runtime/module tuple; no synthetic options or ledger key is added.
    let (provider, model) = match (runtime.as_str(), module_artifact_id.as_str()) {
        ("codex", "codex-rust-controller.1") => (
            Some(root_route_text(options?.get("modelProvider"))?),
            Some(root_route_text(options?.get("model"))?),
        ),
        ("command", "eliot-command.rust-headless.1" | "eliot-command.acp-rust.1") => {
            let model_id = root_route_text(options?.get("modelId"))?;
            let (provider, model) = model_id.split_once('/')?;
            (
                Some(root_route_component(provider)?),
                Some(root_route_component(model)?),
            )
        }
        ("antigravity", "eliot-antigravity.rust-headless.1") => (
            Some("google".to_owned()),
            Some(root_route_text(options?.get("modelId"))?),
        ),
        _ if options.is_none_or(|options| options.is_empty()) => (None, None),
        _ => return None,
    };
    Some(RootRouteIdentity {
        route_alias,
        runtime,
        module_artifact_id,
        provider,
        model,
    })
}

fn root_binding_matches_route(
    binding: &RootBindingClaim,
    target_scope: Option<&Value>,
    target_route: Option<&RootRouteIdentity>,
) -> bool {
    match target_route {
        Some(target) => binding.route_identity.as_ref() == Some(target),
        None => target_scope.is_some_and(|scope| same_route_scope_facts(&binding.scope, scope)),
    }
}

fn root_claim_identity(binding: &Value) -> Option<RootClaimIdentity> {
    let binding_id = binding["binding_id"].as_str()?;
    let generation = binding["generation"].as_i64()?;
    if !valid_identity_text(binding_id) || generation < 1 {
        return None;
    }
    let native_scope_key = match &binding["native_scope_key"] {
        Value::Null => None,
        Value::String(value) if valid_identity_text(value) => Some(value.as_str()),
        _ => return None,
    };
    let native_root_id = match &binding["native_root_id"] {
        Value::Null => None,
        Value::String(value) if valid_identity_text(value) => Some(value.as_str()),
        _ => return None,
    };
    match (native_scope_key, native_root_id) {
        (Some(native_scope_key), Some(native_root_id))
            if valid_identity_text(native_scope_key) && valid_identity_text(native_root_id) =>
        {
            Some(RootClaimIdentity::NativeRoot {
                native_scope_key: native_scope_key.to_owned(),
                native_root_id: native_root_id.to_owned(),
            })
        }
        (None, None) => Some(RootClaimIdentity::Binding {
            binding_id: binding_id.to_owned(),
            generation,
        }),
        _ => None,
    }
}

fn binding_claim_is_released(binding: &Value) -> Option<bool> {
    let state = binding["state"].as_str()?;
    if !matches!(
        state,
        "opening" | "ready" | "reconciling" | "draining" | "closed"
    ) {
        return None;
    }
    let released_at_ms = match &binding["released_at_ms"] {
        Value::Null => None,
        Value::Number(value) => Some(value.as_i64().filter(|value| *value >= 0)?),
        _ => return None,
    };
    match (state, released_at_ms) {
        ("closed", Some(_)) => Some(true),
        ("closed", None) | (_, Some(_)) => None,
        _ => Some(false),
    }
}

fn launch_route_match(
    db: &Connection,
    operation: LaunchRouteOperation<'_>,
    target: LaunchRouteTarget<'_>,
) -> Result<LaunchRouteMatch> {
    let LaunchRouteOperation {
        operation_id,
        state: operation_state,
        task_id: operation_task_id,
        attempt_id: operation_attempt_id,
        binding_id: operation_binding_id,
        binding_generation: operation_binding_generation,
        original_json,
        effective_json,
        result_json,
    } = operation;
    let LaunchRouteTarget {
        route_digest: target_route_digest,
        route_alias: target_route_alias,
    } = target;
    let (Ok(original), Ok(effective), Some(Ok(result))) = (
        serde_json::from_str::<Value>(original_json),
        serde_json::from_str::<Value>(effective_json),
        result_json.map(serde_json::from_str::<Value>),
    ) else {
        return Ok(LaunchRouteMatch::Unknown);
    };
    let (Some(manifest), Some(authority)) = (
        effective.get("launch_manifest"),
        effective.get("launch_plan_authority"),
    ) else {
        return Ok(LaunchRouteMatch::Unknown);
    };
    if !manifest.is_object() || !authority.is_object() {
        return Ok(LaunchRouteMatch::Unknown);
    }
    let authority_digest = format!(
        "sha256:{}",
        crate::model::digest(crate::model::canonical(authority)?.as_bytes())
    );
    let Some(plan_digest) = manifest["plan_digest"].as_str() else {
        return Ok(LaunchRouteMatch::Unknown);
    };
    if plan_digest != authority_digest
        || original["plan_digest"].as_str() != Some(plan_digest)
        || result["plan_digest"].as_str() != Some(plan_digest)
        || result["operation_id"].as_str() != Some(operation_id)
        || authority["requested_plan_facts"] != manifest["request"]
        || authority["task"]["task_id"] != manifest["task"]["task_id"]
        || authority["task"]["expected_revision"] != manifest["task"]["expected_revision"]
        || authority["task"]["revision"] != manifest["task"]["observed_revision"]
        || authority["attempt_action"] != manifest["task"]["attempt_action"]
        || authority["current_attempt"]["attempt_id"] != manifest["task"]["attempt_id"]
        || operation_task_id != manifest["task"]["task_id"].as_str()
        || operation_attempt_id != manifest["task"]["attempt_id"].as_str()
        || result["task_id"] != manifest["task"]["task_id"]
        || result["task_revision"] != manifest["task"]["observed_revision"]
        || result["attempt_id"] != manifest["task"]["attempt_id"]
        || manifest["manifest_version"] != "eliot-launch-manifest-v1"
    {
        return Ok(LaunchRouteMatch::Unknown);
    }
    let Some(selected_route_digest) = authority["selected_route_sha256"].as_str() else {
        return Ok(LaunchRouteMatch::Unknown);
    };
    if !valid_digest(selected_route_digest) {
        return Ok(LaunchRouteMatch::Unknown);
    }
    if selected_route_digest != target_route_digest {
        return Ok(LaunchRouteMatch::OtherRoute);
    }
    if manifest["request"]["route"].as_str() != Some(target_route_alias)
        || authority["requested_plan_facts"]["route"].as_str() != Some(target_route_alias)
        || manifest["runtime"]["route"]["alias"].as_str() != Some(target_route_alias)
        || authority["route"]["alias"].as_str() != Some(target_route_alias)
    {
        return Ok(LaunchRouteMatch::Unknown);
    }

    let launch_state = manifest["state"].as_str();
    let result_launch_state = result["launch_state"].as_str();
    if launch_state == Some("blocked") && result_launch_state == Some("blocked") {
        return Ok(LaunchRouteMatch::OtherRoute);
    }
    let active_launch_state = matches!(
        launch_state,
        Some(
            "pending_workspace"
                | "outcome_unknown"
                | "awaiting_binding"
                | "awaiting_capability"
                | "awaiting_participant_credential"
        )
    );
    if !active_launch_state
        || (operation_state == "queued" && result["state"] != "queued")
        || (operation_state == "outcome_unknown" && result["state"] != "outcome_unknown")
        || !matches!(operation_state, "queued" | "outcome_unknown")
        || manifest["runtime"]["route"]["admission"]["decision"] != "admit"
    {
        return Ok(LaunchRouteMatch::Unknown);
    }
    let active_result_matches = match launch_state {
        Some("pending_workspace") => result_launch_state == Some("pending_workspace"),
        Some("outcome_unknown") => result_launch_state == Some("outcome_unknown"),
        Some("awaiting_binding") => result_launch_state == Some("awaiting_binding"),
        Some("awaiting_capability") => result_launch_state == Some("awaiting_capability"),
        Some("awaiting_participant_credential") => {
            result_launch_state == Some("awaiting_participant_credential")
        }
        _ => false,
    };
    if !active_result_matches {
        return Ok(LaunchRouteMatch::Unknown);
    }

    let manifest_binding_id = manifest["binding"]["binding_id"].as_str();
    let manifest_binding_generation = manifest["binding"]["generation"].as_i64();
    let manifest_binding = match (manifest_binding_id, manifest_binding_generation) {
        (None, None)
            if manifest["binding"]["binding_id"].is_null()
                && manifest["binding"]["generation"].is_null() =>
        {
            None
        }
        (Some(binding_id), Some(generation))
            if valid_identity_text(binding_id) && generation > 0 =>
        {
            Some((binding_id.to_owned(), generation))
        }
        _ => return Ok(LaunchRouteMatch::Unknown),
    };
    let operation_binding = match (operation_binding_id, operation_binding_generation) {
        (None, None) => None,
        (Some(binding_id), Some(generation))
            if valid_identity_text(binding_id) && generation > 0 =>
        {
            Some((binding_id.to_owned(), generation))
        }
        _ => return Ok(LaunchRouteMatch::Unknown),
    };
    if manifest_binding != operation_binding {
        return Ok(LaunchRouteMatch::Unknown);
    }
    match launch_state {
        Some("pending_workspace" | "outcome_unknown") if manifest_binding.is_some() => {
            return Ok(LaunchRouteMatch::Unknown);
        }
        Some("awaiting_binding" | "awaiting_capability" | "awaiting_participant_credential")
            if manifest_binding.is_none() =>
        {
            return Ok(LaunchRouteMatch::Unknown);
        }
        _ => {}
    }
    if matches!(launch_state, Some("pending_workspace"))
        && !matches!(
            manifest["workspace"]["lease_state"].as_str(),
            Some("pending" | "held")
        )
    {
        return Ok(LaunchRouteMatch::Unknown);
    }

    let actor = &manifest["actor"];
    let link = match super::automation_work_dispatch::operation_link(db, operation_id) {
        Ok(link) => link,
        Err(error)
            if matches!(
                error.code.as_str(),
                "AUTOMATION_LINK_CORRUPT" | "LAUNCH_SLOT_CORRUPT"
            ) =>
        {
            return Ok(LaunchRouteMatch::Unknown);
        }
        Err(error) => return Err(error),
    };
    match actor["kind"].as_str() {
        Some("work_dispatch") => {
            let Some(link) = link else {
                return Ok(LaunchRouteMatch::Unknown);
            };
            if link.operation_id != operation_id
                || link.action != "swarm.launch"
                || actor["semantic_slot_id"].as_str() != Some(link.semantic_slot_id.as_str())
                || link.task_id != manifest["task"]["task_id"].as_str().unwrap_or_default()
                || link.task_revision != manifest["task"]["expected_revision"].as_i64().unwrap_or(0)
                || link.attempt_id.as_deref() != manifest["task"]["attempt_id"].as_str()
                || actor["effective_manager_id"].as_str()
                    != Some(link.effective_manager_id.as_str())
            {
                return Ok(LaunchRouteMatch::Unknown);
            }
        }
        Some("direct") if link.is_none() => {}
        _ => return Ok(LaunchRouteMatch::Unknown),
    }
    Ok(LaunchRouteMatch::Matched(manifest_binding))
}

/// Counts exact route root claims from bounded durable evidence. Bindings,
/// admitted launch reservations, binding operations and owned service starts
/// share one native-root/binding identity, so lifecycle representation changes
/// and retries never multiply a root slot. Producer/subagent and unrelated
/// Task operations do not create claims. The current root can be excluded for
/// same-root rechecks; an `agent.open` prerequisite is followed only through
/// its exact retained launch and binding generation.
pub(super) fn root_claims_for_route(
    db: &Connection,
    route: &crate::config::Route,
    exclude_operation_id: Option<&str>,
    exclude_binding: Option<(&str, i64)>,
) -> Result<Option<u32>> {
    let route_value = serde_json::to_value(route)?;
    let options = match route_value.get("native_options") {
        Some(Value::Null) => None,
        Some(Value::Object(options)) => Some(options),
        _ => return Ok(None),
    };
    let target_service = match options.and_then(|options| options.get("service_id")) {
        None => None,
        Some(Value::String(service)) if valid_identity_text(service) => Some(service.as_str()),
        Some(_) => return Ok(None),
    };
    let target_route_alias = route.alias.as_str();
    if !valid_identity_text(target_route_alias) || target_route_alias.chars().any(char::is_control)
    {
        return Ok(None);
    }
    let target_route_digest =
        crate::model::digest(crate::model::canonical(&route_value)?.as_bytes());
    let target_route_identity = if target_service.is_none() {
        let Some(identity) = root_route_identity(&route_value) else {
            return Ok(None);
        };
        Some(identity)
    } else {
        None
    };
    let target_scope = if target_service.is_some() {
        let scope = scope_facts(&route_value, None, "");
        let scope_key = scope["scope_key"].as_str().unwrap_or_default();
        let expected_key = ScopeKey::from_route(&route_value, "");
        if scope["identity"] != "complete"
            || !valid_identity_text(scope_key)
            || expected_key.as_str() != scope_key
        {
            return Ok(None);
        }
        Some(scope)
    } else {
        None
    };

    let scopes = collect_scopes(db)?;
    if scopes.contains_key("unknown:capacity-roster-limit")
        || scopes.contains_key("unknown:damaged-ledger-key")
        || scopes.contains_key("unknown:capacity-ledger-roster-limit")
    {
        return Ok(None);
    }
    if let Some(target_scope) = target_scope.as_ref()
        && scopes.values().any(|agg| {
            agg.scope["identity"] != "complete"
                && (agg.scope["runtime"].as_str().is_none()
                    || agg.scope["runtime"] == target_scope["runtime"])
        })
    {
        return Ok(None);
    }

    let mut binding_claims = BTreeMap::<(String, i64), RootBindingClaim>::new();
    let mut all_bindings = BTreeMap::<(String, i64), RootBindingClaim>::new();
    let mut target_scope_keys = BTreeSet::<String>::new();
    if let Some(target_scope) = target_scope.as_ref()
        && let Some(scope_key) = target_scope["scope_key"].as_str()
        && scopes.contains_key(scope_key)
    {
        target_scope_keys.insert(scope_key.to_owned());
    }
    for candidate_agg in scopes.values() {
        for binding in &candidate_agg.bindings {
            let (Some(binding_id), Some(generation), Some(identity), Some(released)) = (
                binding["binding_id"].as_str(),
                binding["generation"].as_i64(),
                root_claim_identity(binding),
                binding_claim_is_released(binding),
            ) else {
                return Ok(None);
            };
            let state = binding["state"].as_str().unwrap_or_default().to_owned();
            let facts = scope_facts(
                &binding["route"],
                binding["native_scope_key"].as_str(),
                binding_id,
            );
            let expected_key = ScopeKey::from_route(&binding["route"], binding_id);
            if facts["scope_key"].as_str() != Some(expected_key.as_str()) {
                return Ok(None);
            }
            let route_identity = root_route_identity(&binding["route"]);
            if let Some(target) = target_route_identity.as_ref() {
                let recorded_runtime = binding["route"]["runtime"].as_str();
                let recorded_alias = binding["route"]["alias"].as_str();
                let could_be_target = recorded_runtime
                    .is_none_or(|runtime| runtime == target.runtime)
                    && recorded_alias.is_none_or(|alias| alias == target.route_alias);
                if could_be_target && route_identity.is_none() {
                    return Ok(None);
                }
            }
            let claim = RootBindingClaim {
                binding_id: binding_id.to_owned(),
                generation,
                scope: facts.clone(),
                route_identity,
                identity,
                released,
                state,
            };
            let key = (binding_id.to_owned(), generation);
            if all_bindings.insert(key.clone(), claim.clone()).is_some() {
                return Ok(None);
            }
            if root_binding_matches_route(
                &claim,
                target_scope.as_ref(),
                target_route_identity.as_ref(),
            ) {
                let scope_key = facts["scope_key"].as_str().unwrap_or_default();
                if !valid_identity_text(scope_key) {
                    return Ok(None);
                }
                target_scope_keys.insert(scope_key.to_owned());
                binding_claims.insert(key, claim);
            }
        }
    }

    if let Some(target) = target_route_identity.as_ref() {
        for agg in scopes.values().filter(|agg| agg.bindings.is_empty()) {
            let same_runtime = agg.scope["runtime"]
                .as_str()
                .is_none_or(|runtime| runtime == target.runtime);
            let route_alias = agg.scope["route_alias"].as_str();
            if same_runtime
                && route_alias.is_none_or(|alias| alias == target.route_alias)
                && (agg.damage.is_some() || !agg.entries.is_empty())
            {
                return Ok(None);
            }
        }
    }

    let mut target_entries = Vec::<&CapacityEntry>::new();
    for scope_key in &target_scope_keys {
        let Some(agg) = scopes.get(scope_key) else {
            return Ok(None);
        };
        if agg.damage.is_some() {
            return Ok(None);
        }
        if let Some(target) = target_route_identity.as_ref()
            && agg
                .bindings
                .iter()
                .any(|binding| root_route_identity(&binding["route"]).as_ref() != Some(target))
        {
            return Ok(None);
        }
        let roster_ok = if target_route_identity.is_some() {
            root_claim_roster_check(db, agg)?.0
        } else {
            roster_check(db, agg)?.0
        };
        if !roster_ok {
            return Ok(None);
        }
        target_entries.extend(agg.entries.iter());
    }

    let mut claims = BTreeSet::<RootClaimIdentity>::new();
    for claim in binding_claims.values() {
        if !claim.released {
            if claim.state == "opening"
                && matches!(&claim.identity, RootClaimIdentity::Binding { .. })
                && !target_entries.iter().any(|entry| {
                    entry.kind == ResourceEntryKind::Operation
                        && entry.method.as_deref() == Some("agent.open")
                        && entry.binding_id == claim.binding_id
                        && entry.binding_generation == claim.generation
                        && entry.phase != DerivedPhase::Released
                })
            {
                return Ok(None);
            }
            claims.insert(claim.identity.clone());
        }
    }

    // Count only root-bearing binding operations. Their root/binding identity
    // already exists above, so Task dispatches and retries do not add slots.
    for entry in &target_entries {
        if entry.method.as_deref() == Some("task.dispatch")
            && entry.kind != ResourceEntryKind::Operation
        {
            return Ok(None);
        }
        if entry.kind != ResourceEntryKind::Operation
            || !matches!(
                entry.method.as_deref(),
                Some("task.dispatch" | "agent.open")
            )
            || entry.phase == DerivedPhase::Released
        {
            continue;
        }
        let key = (entry.binding_id.clone(), entry.binding_generation);
        let Some(binding) = all_bindings.get(&key) else {
            return Ok(None);
        };
        if root_binding_matches_route(
            binding,
            target_scope.as_ref(),
            target_route_identity.as_ref(),
        ) {
            claims.insert(binding.identity.clone());
        }
    }

    let mut launch_bindings = BTreeMap::<String, Option<(String, i64)>>::new();
    let mut launch_stmt = db.prepare(
        "SELECT operation_id,state,task_id,attempt_id,binding_id,binding_generation, \
         original_request_json,effective_request_json,result_json,client_request_id FROM operations \
         WHERE method='swarm.launch' AND state IN ('queued','outcome_unknown') \
         ORDER BY operation_id LIMIT ?1",
    )?;
    let mut launch_rows = launch_stmt.query([MAX_ROSTER_SQL_ROWS])?;
    let mut launch_row_count = 0usize;
    while let Some(row) = launch_rows.next()? {
        launch_row_count = launch_row_count.saturating_add(1);
        if launch_row_count > MAX_ROSTER_ROWS {
            return Ok(None);
        }
        let operation_id = row.get::<_, String>(0)?;
        let operation_state = row.get::<_, String>(1)?;
        let task_id = row.get::<_, Option<String>>(2)?;
        let attempt_id = row.get::<_, Option<String>>(3)?;
        let binding_id = row.get::<_, Option<String>>(4)?;
        let binding_generation = row.get::<_, Option<i64>>(5)?;
        let original_json = row.get::<_, String>(6)?;
        let effective_json = row.get::<_, String>(7)?;
        let result_json = row.get::<_, Option<String>>(8)?;
        let client_request_id = row.get::<_, String>(9)?;
        // Store inserts the caller's exact launch operation before the
        // admission preview has retained its manifest. That queued `{}` row
        // has no root identity yet; skip only this exact excluded operation
        // when its parsed request matches the retained request ID and target
        // route. Malformed or mismatched rows fall through to normal matching.
        if exclude_operation_id == Some(operation_id.as_str())
            && operation_state == "queued"
            && task_id.is_none()
            && attempt_id.is_none()
            && binding_id.is_none()
            && binding_generation.is_none()
            && effective_json == "{}"
            && result_json.is_none()
            && serde_json::from_str::<Value>(&original_json)
                .ok()
                .and_then(|request| crate::launcher::LaunchRequest::parse(&request).ok())
                .is_some_and(|request| {
                    request.client_request_id.as_str() == client_request_id.as_str()
                        && request.preview.route.as_str() == target_route_alias
                })
        {
            continue;
        }
        match launch_route_match(
            db,
            LaunchRouteOperation {
                operation_id: &operation_id,
                state: &operation_state,
                task_id: task_id.as_deref(),
                attempt_id: attempt_id.as_deref(),
                binding_id: binding_id.as_deref(),
                binding_generation,
                original_json: &original_json,
                effective_json: &effective_json,
                result_json: result_json.as_deref(),
            },
            LaunchRouteTarget {
                route_digest: &target_route_digest,
                route_alias: target_route_alias,
            },
        )? {
            LaunchRouteMatch::OtherRoute => {}
            LaunchRouteMatch::Unknown => return Ok(None),
            LaunchRouteMatch::Matched(binding_ref) => {
                if let Some((binding_id, generation)) = binding_ref.as_ref() {
                    let Some(binding) = all_bindings.get(&(binding_id.clone(), *generation)) else {
                        return Ok(None);
                    };
                    if binding.released
                        || !root_binding_matches_route(
                            binding,
                            target_scope.as_ref(),
                            target_route_identity.as_ref(),
                        )
                    {
                        return Ok(None);
                    }
                } else {
                    claims.insert(RootClaimIdentity::LaunchOperation {
                        operation_id: operation_id.clone(),
                    });
                }
                launch_bindings.insert(operation_id, binding_ref);
            }
        }
    }

    let mut service_stmt = db.prepare(
        "SELECT binding_id,binding_generation,state FROM owned_service_starts \
         ORDER BY binding_id,binding_generation LIMIT ?1",
    )?;
    let service_rows = service_stmt
        .query_map([MAX_ROSTER_SQL_ROWS], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if service_rows.len() > MAX_ROSTER_ROWS {
        return Ok(None);
    }
    for (binding_id, generation, state) in service_rows {
        let active = match state.as_str() {
            "reserved" | "outcome_unknown" | "service_observed" => true,
            "service_departed" | "failed_no_effect" => false,
            _ => return Ok(None),
        };
        if !active {
            continue;
        }
        let Some(binding) = all_bindings.get(&(binding_id, generation)) else {
            return Ok(None);
        };
        if root_binding_matches_route(
            binding,
            target_scope.as_ref(),
            target_route_identity.as_ref(),
        ) {
            claims.insert(binding.identity.clone());
        }
    }

    // Apply same-root exclusions only after all durable claim sources have
    // been reconciled, so an owned service start cannot re-add the excluded
    // binding later in the projection.
    if let Some((binding_id, generation)) = exclude_binding
        && let Some(binding) = binding_claims.get(&(binding_id.to_owned(), generation))
    {
        claims.remove(&binding.identity);
    }

    if let Some(operation_id) = exclude_operation_id {
        let operation = match operations::get_operation(db, operation_id) {
            Ok(operation) => Some(operation),
            Err(error) if error.code == "NOT_FOUND" => None,
            Err(error) => return Err(error),
        };
        if let Some(operation) = operation {
            match operation["method"].as_str() {
                Some("swarm.launch") => {
                    claims.remove(&RootClaimIdentity::LaunchOperation {
                        operation_id: operation_id.to_owned(),
                    });
                    if let Some(Some((binding_id, generation))) = launch_bindings.get(operation_id)
                        && let Some(binding) = all_bindings.get(&(binding_id.clone(), *generation))
                    {
                        claims.remove(&binding.identity);
                    }
                }
                Some("task.dispatch") => {
                    if let (Some(binding_id), Some(generation)) = (
                        operation["binding_id"].as_str(),
                        operation["binding_generation"].as_i64(),
                    ) && let Some(binding) =
                        all_bindings.get(&(binding_id.to_owned(), generation))
                        && root_binding_matches_route(
                            binding,
                            target_scope.as_ref(),
                            target_route_identity.as_ref(),
                        )
                    {
                        claims.remove(&binding.identity);
                    }
                }
                Some("agent.open") => {
                    if let (Some(binding_id), Some(generation), Some(parent_id)) = (
                        operation["binding_id"].as_str(),
                        operation["binding_generation"].as_i64(),
                        operation["prerequisite_operation_id"].as_str(),
                    ) && let Some(binding) =
                        all_bindings.get(&(binding_id.to_owned(), generation))
                        && root_binding_matches_route(
                            binding,
                            target_scope.as_ref(),
                            target_route_identity.as_ref(),
                        )
                    {
                        let parent = match operations::get_operation(db, parent_id) {
                            Ok(parent) => Some(parent),
                            Err(error) if error.code == "NOT_FOUND" => None,
                            Err(error) => return Err(error),
                        };
                        let parent_matches_open = if let Some(parent) = parent
                            && parent["method"] == "swarm.launch"
                        {
                            let parent_manifest_raw: Option<String> = db
                                .query_row(
                                    "SELECT effective_request_json FROM operations \
                                 WHERE operation_id=?1 AND method='swarm.launch'",
                                    [parent_id],
                                    |row| row.get(0),
                                )
                                .optional()?;
                            let parent_manifest = parent_manifest_raw
                                .as_deref()
                                .and_then(|raw| serde_json::from_str::<Value>(raw).ok())
                                .and_then(|effective| effective.get("launch_manifest").cloned());
                            parent["caller_id"] == operation["caller_id"]
                                && parent["task_id"] == operation["task_id"]
                                && parent["attempt_id"] == operation["attempt_id"]
                                && parent["binding_id"] == operation["binding_id"]
                                && parent["binding_generation"] == operation["binding_generation"]
                                && parent_manifest.is_some_and(|manifest| {
                                    manifest["binding"]["operation_id"] == operation_id
                                        && manifest["binding"]["binding_id"] == binding_id
                                        && manifest["binding"]["generation"] == generation
                                })
                                && parent_manifest_raw.is_some()
                                && launch_bindings.get(parent_id)
                                    == Some(&Some((binding_id.to_owned(), generation)))
                        } else {
                            false
                        };
                        if parent_matches_open {
                            claims.remove(&binding.identity);
                            claims.remove(&RootClaimIdentity::LaunchOperation {
                                operation_id: parent_id.to_owned(),
                            });
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok(u32::try_from(claims.len()).ok())
}

fn new_work_enabled(db: &Connection) -> Result<bool> {
    Ok(meta(db, "execution_mode")?.unwrap_or(Value::Null)["new_work"] == "enabled")
}

/// Computes one accounting item per scope from the ledger and recorded
/// bindings. Legacy quota incidents are included as historical evidence;
/// this projection does not assert provider capacity or admission.
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
        let historical_quota = historical_quota_incident(db, &scope_key)?;
        let pending = counts["pending_admissions"].as_i64().unwrap_or(0);
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
            "historical_quota_incident": historical_quota,
            "pending_admission_recorded": pending > 0,
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
