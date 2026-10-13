//! Store-owned projection of already-admitted module work.
//!
//! The page performs only the idempotent Store intent reservation needed to
//! carry a fresh owner nonce across the process boundary; it does not read the
//! metadata catalogue to activate a worker. It starts from exact retained
//! Operation rows and active native-session rows, then resolves each candidate
//! through the immutable selector on its binding.

use super::{launcher_owned_service, module_handshake, operations};
use crate::{
    config::Config,
    error::{Error, Result},
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params, types::Type};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
use swarm_contracts::{
    module_catalog::ModuleDescriptor,
    module_command::{MODULE_DEMAND_METHODS, capability_satisfies, classify_runtime_command},
};

const MAX_DEMAND_OPERATIONS: usize = 4096;
const MAX_SCOPE_UNRESOLVED_OPERATIONS: usize = 4096;
// Keep this vocabulary lexicographically sorted for indexed gap probes below.
const VALID_OPERATION_STATES: [&str; 7] = [
    "cancelled",
    "native_accepted",
    "outcome_unknown",
    "queued",
    "rejected",
    "sending",
    "settled",
];
const UNRESOLVED_OPERATION_STATES: [&str; 3] = ["sending", "native_accepted", "outcome_unknown"];

/// Exact status-only unresolved Operation data used for scoped recovery
/// readback. Inputs, results, identities of callers, and native payloads are
/// deliberately absent.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoredOperation {
    pub(crate) operation_id: String,
    pub(crate) method: String,
    pub(crate) binding_id: String,
    pub(crate) generation: u64,
    pub(crate) state: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleDemand {
    pub(crate) descriptor: ModuleDescriptor,
    pub(crate) descriptor_revision: u64,
    pub(crate) module_id: String,
    pub(crate) artifact_id: String,
    pub(crate) artifact_version: String,
    pub(crate) operation_id: String,
    pub(crate) required_capability: String,
    pub(crate) binding_id: String,
    pub(crate) generation: u64,
    pub(crate) module_client_id: Option<String>,
    pub(crate) credential_ref: Option<String>,
    pub(crate) route_native_options: Value,
    pub(crate) operation_readback: Vec<StoredOperation>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleDemandBlock {
    pub(crate) binding_id: String,
    pub(crate) generation: u64,
    pub(crate) operation_id: String,
    pub(crate) error_code: String,
    /// Present only when the exact retained catalog descriptor was resolved.
    /// The host may report a scoped failure only with this identity; a corrupt
    /// or missing selector never gets replaced with a guessed module identity.
    pub(crate) descriptor: Option<ModuleDescriptor>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleDemandSnapshot {
    pub(crate) demands: Vec<ModuleDemand>,
    pub(crate) blocked: Vec<ModuleDemandBlock>,
    pub(crate) truncated: bool,
    pub(crate) next_cursor: Option<ModuleDemandCursor>,
}

/// Complete status-only unresolved Operation readback used before releasing
/// the last host demand lease for a service scope. `native_identity_retained`
/// is a conservative Store fact, not proof that an external/native process is
/// currently alive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleScopeReadback {
    pub(crate) operations: Vec<StoredOperation>,
    pub(crate) native_identity_retained: bool,
    pub(crate) module_hello_boot_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleDemandCursor {
    pub(crate) created_at_ms: i64,
    pub(crate) operation_id: String,
}

#[derive(Debug, Clone)]
struct OperationRow {
    operation_id: String,
    method: String,
    binding_id: String,
    generation: i64,
    created_at_ms: i64,
    input: Value,
}

fn operation_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OperationRow> {
    let raw_input: String = row.get(5)?;
    let input = serde_json::from_str(&raw_input).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(error))
    })?;
    Ok(OperationRow {
        operation_id: row.get(0)?,
        method: row.get(1)?,
        binding_id: row.get(2)?,
        generation: row.get(3)?,
        created_at_ms: row.get(4)?,
        input,
    })
}

fn operation_candidates(
    db: &Connection,
    cursor: Option<&ModuleDemandCursor>,
) -> Result<(Vec<OperationRow>, bool, Option<ModuleDemandCursor>)> {
    let cursor_created_at = cursor.map_or(i64::MIN, |value| value.created_at_ms);
    let cursor_operation_id = cursor.map_or("", |value| value.operation_id.as_str());
    let methods_json = serde_json::to_string(&MODULE_DEMAND_METHODS)?;
    let mut statement = db.prepare(
        "SELECT o.operation_id,o.method,o.binding_id,o.binding_generation,o.created_at_ms, \
                o.original_request_json \
         FROM operations AS o JOIN bindings AS b \
           ON b.binding_id=o.binding_id AND b.generation=o.binding_generation \
         WHERE b.released_at_ms IS NULL \
           AND (o.created_at_ms>?1 OR (o.created_at_ms=?1 AND o.operation_id>?2)) \
           AND o.state IN ('queued','sending','native_accepted','outcome_unknown') \
           AND o.method IN (SELECT value FROM json_each(?3)) \
         ORDER BY o.created_at_ms,o.operation_id LIMIT ?4",
    )?;
    let mut rows = statement
        .query_map(
            params![
                cursor_created_at,
                cursor_operation_id,
                methods_json,
                MAX_DEMAND_OPERATIONS as i64 + 1
            ],
            operation_row,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let pending_truncated = rows.len() > MAX_DEMAND_OPERATIONS;
    rows.truncate(MAX_DEMAND_OPERATIONS + 1);

    // A retained live native session is an ongoing attachment obligation even
    // between turns. Anchor its adapter demand to the original admitted
    // agent.open Operation; this starts only the adapter and never replays it.
    let mut statement = db.prepare(
        "SELECT o.operation_id,o.method,o.binding_id,o.binding_generation,o.created_at_ms, \
                o.original_request_json \
         FROM operations AS o JOIN bindings AS b \
           ON b.binding_id=o.binding_id AND b.generation=o.binding_generation \
         WHERE b.released_at_ms IS NULL AND b.native_root_id IS NOT NULL \
           AND b.native_scope_key IS NOT NULL AND o.method='agent.open' \
           AND (o.created_at_ms>?1 OR (o.created_at_ms=?1 AND o.operation_id>?2)) \
           AND o.state IN ('settled','outcome_unknown') \
         ORDER BY o.created_at_ms,o.operation_id LIMIT ?3",
    )?;
    let native_rows = statement
        .query_map(
            params![
                cursor_created_at,
                cursor_operation_id,
                MAX_DEMAND_OPERATIONS as i64 + 1
            ],
            operation_row,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let native_truncated = native_rows.len() > MAX_DEMAND_OPERATIONS;
    rows.extend(native_rows);

    let mut unique = BTreeMap::<(String, i64, String), OperationRow>::new();
    for row in rows {
        unique
            .entry((
                row.binding_id.clone(),
                row.generation,
                row.operation_id.clone(),
            ))
            .or_insert(row);
    }
    let mut values = unique.into_values().collect::<Vec<_>>();
    values.sort_by(|left, right| {
        (left.created_at_ms, &left.operation_id).cmp(&(right.created_at_ms, &right.operation_id))
    });
    let truncated = pending_truncated || native_truncated || values.len() > MAX_DEMAND_OPERATIONS;
    if values.len() > MAX_DEMAND_OPERATIONS {
        values.truncate(MAX_DEMAND_OPERATIONS);
    }
    let next_cursor = if truncated {
        values.last().map(|row| ModuleDemandCursor {
            created_at_ms: row.created_at_ms,
            operation_id: row.operation_id.clone(),
        })
    } else {
        None
    };
    Ok((values, truncated, next_cursor))
}

fn scoped_operation_readback(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Vec<StoredOperation>> {
    let tx = db.unchecked_transaction()?;
    let rows = scoped_operation_readback_in_snapshot(&tx, binding_id, generation)?;
    tx.commit()?;
    Ok(rows)
}

fn scoped_operation_readback_in_snapshot(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Vec<StoredOperation>> {
    if generation <= 0 {
        return Err(Error::new(
            "MODULE_READBACK_SCOPE_INVALID",
            "module Operation readback requires a positive binding generation",
        ));
    }

    // Keep the state-integrity probe and unresolved projection on one SQLite
    // snapshot. The scope/state index lets the probe seek the fixed gaps in
    // the Store's state vocabulary without walking terminal lifetime history.
    if let Some(state) = unknown_operation_state(db, binding_id, generation)? {
        return Err(Error::new(
            "MODULE_READBACK_STATE_UNKNOWN",
            format!("binding Operation has an unrecognized state: {state:?}"),
        ));
    }

    let mut statement = db.prepare(
        "SELECT operation_id,method,state,binding_id,binding_generation FROM operations \
         INDEXED BY unresolved_target_operations \
         WHERE binding_id=?1 AND binding_generation=?2 \
           AND state IN ('sending','native_accepted','outcome_unknown') \
         ORDER BY created_at_ms,operation_id LIMIT ?3",
    )?;
    let rows = statement
        .query_map(
            params![
                binding_id,
                generation,
                MAX_SCOPE_UNRESOLVED_OPERATIONS as i64 + 1
            ],
            |row| {
                let operation_generation: i64 = row.get(4)?;
                let operation_generation =
                    u64::try_from(operation_generation).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(4, Type::Integer, Box::new(error))
                    })?;
                Ok(StoredOperation {
                    operation_id: row.get(0)?,
                    method: row.get(1)?,
                    state: row.get(2)?,
                    binding_id: row.get(3)?,
                    generation: operation_generation,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(statement);
    if rows.len() > MAX_SCOPE_UNRESOLVED_OPERATIONS {
        return Err(Error::new(
            "MODULE_READBACK_UNRESOLVED_OVERFLOW",
            format!(
                "exact binding scope exceeds the bounded unresolved Operation readback of {MAX_SCOPE_UNRESOLVED_OPERATIONS}"
            ),
        ));
    }
    if rows.iter().any(|row| {
        row.binding_id != binding_id
            || row.generation != generation as u64
            || !UNRESOLVED_OPERATION_STATES.contains(&row.state.as_str())
    }) {
        return Err(Error::new(
            "MODULE_READBACK_CORRUPT",
            "binding unresolved Operation snapshot contains invalid scope or state data",
        ));
    }

    Ok(rows)
}

fn unknown_operation_state(
    db: &Connection,
    binding_id: &str,
    generation: i64,
) -> Result<Option<String>> {
    let mut lower = None;
    for upper in VALID_OPERATION_STATES {
        if let Some(state) = operation_state_in_gap(db, binding_id, generation, lower, Some(upper))?
        {
            return Ok(Some(state));
        }
        lower = Some(upper);
    }
    operation_state_in_gap(db, binding_id, generation, lower, None)
}

fn operation_state_in_gap(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    lower: Option<&str>,
    upper: Option<&str>,
) -> Result<Option<String>> {
    let state = match (lower, upper) {
        (None, Some(upper)) => db
            .query_row(
                "SELECT state FROM operations INDEXED BY unresolved_target_operations \
                 WHERE binding_id=?1 AND binding_generation=?2 AND state<?3 LIMIT 1",
                params![binding_id, generation, upper],
                |row| row.get(0),
            )
            .optional()?,
        (Some(lower), Some(upper)) => db
            .query_row(
                "SELECT state FROM operations INDEXED BY unresolved_target_operations \
                 WHERE binding_id=?1 AND binding_generation=?2 \
                   AND state>?3 AND state<?4 LIMIT 1",
                params![binding_id, generation, lower, upper],
                |row| row.get(0),
            )
            .optional()?,
        (Some(lower), None) => db
            .query_row(
                "SELECT state FROM operations INDEXED BY unresolved_target_operations \
                 WHERE binding_id=?1 AND binding_generation=?2 AND state>?3 LIMIT 1",
                params![binding_id, generation, lower],
                |row| row.get(0),
            )
            .optional()?,
        (None, None) => {
            return Err(Error::new(
                "MODULE_READBACK_STATE_UNKNOWN",
                "operation state vocabulary is empty",
            ));
        }
    };
    Ok(state)
}

/// Reconcile one already-held module scope before its final demand lease can
/// be released. The exact retained selector is checked again, while no
/// descriptor/catalog read starts a worker. A retained native identity or any
/// unresolved Operation keeps the adapter attached until an exact later
/// readback clears that obligation.
pub(super) fn scope_readback(
    db: &Connection,
    module_id: &str,
    binding_id: &str,
    generation: i64,
) -> Result<ModuleScopeReadback> {
    if generation <= 0 {
        return Err(Error::new(
            "MODULE_READBACK_SCOPE_INVALID",
            "module scope readback requires a positive binding generation",
        ));
    }
    let binding = operations::get_binding(db, binding_id, generation)?;
    let selector = binding["observation"].get("module_contract_selector");
    let retained = module_handshake::retained_contract_identity(
        db,
        binding["module_artifact_id"].as_str().unwrap_or_default(),
        selector,
    )?
    .ok_or_else(|| {
        Error::new(
            "MODULE_READBACK_DESCRIPTOR_MISSING",
            "held module scope no longer has its exact retained descriptor selector",
        )
    })?;
    if retained.module_id.as_str() != module_id {
        return Err(Error::new(
            "MODULE_READBACK_SCOPE_MISMATCH",
            "held module scope resolves to another retained module identity",
        ));
    }
    // Resolve the immutable descriptor as well as its selector. A damaged or
    // missing catalog snapshot is not evidence that the scope is safe to drop.
    let _descriptor = module_handshake::retained_descriptor(db, &retained)?;
    let operations = scoped_operation_readback(db, binding_id, generation)?;
    let native_root = binding["native_root_id"].as_str();
    let native_scope = binding["native_scope_key"].as_str();
    if native_root.is_some() != native_scope.is_some() {
        return Err(Error::new(
            "MODULE_READBACK_NATIVE_IDENTITY_INCOMPLETE",
            "binding retains only one half of its native identity",
        ));
    }
    let module_hello_boot_id = match binding["observation"].get("bridge_boot_id") {
        None | Some(Value::Null) => None,
        Some(Value::String(value))
            if !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control) =>
        {
            Some(value.clone())
        }
        Some(_) => {
            return Err(Error::new(
                "MODULE_READBACK_BOOT_INVALID",
                "binding module hello boot identity is malformed",
            ));
        }
    };
    let managed_owner_retained =
        managed_owner_requires_custody(db, &binding, module_id, generation)?;
    let native_identity_retained =
        native_root.is_some() || native_scope.is_some() || managed_owner_retained;
    Ok(ModuleScopeReadback {
        operations,
        native_identity_retained,
        module_hello_boot_id,
    })
}

fn managed_owner_requires_custody(
    db: &Connection,
    binding: &Value,
    module_id: &str,
    generation: i64,
) -> Result<bool> {
    if binding["observation"]["managed_owner"].is_null() {
        return Ok(false);
    }
    let observation = &binding["observation"];
    let current = &observation["module_supervisor"];
    let Some(binding_id) = binding["binding_id"].as_str() else {
        return Ok(true);
    };
    let Some(current_boot_id) = observation["bridge_boot_id"].as_str() else {
        return Ok(true);
    };
    let Some(actor_instance_id) = current["actor_instance_id"].as_str() else {
        return Ok(true);
    };
    let Some(artifact_id) = binding["module_artifact_id"].as_str() else {
        return Ok(true);
    };
    let Some(artifact_version) =
        observation["module_contract_selector"]["artifact"]["version"].as_str()
    else {
        return Ok(true);
    };
    let exact_departed_projection = current["schema_version"].as_u64() == Some(1)
        && current["binding_id"].as_str() == Some(binding_id)
        && current["generation"].as_i64() == Some(generation)
        && binding["generation"].as_i64() == Some(generation)
        && current["module_id"].as_str() == Some(module_id)
        && current["artifact_id"].as_str() == Some(artifact_id)
        && current["artifact_version"].as_str() == Some(artifact_version)
        && current["boot_id"].as_str() == Some(current_boot_id)
        && matches!(
            current["phase"].as_str(),
            Some("exited_proven" | "completed")
        )
        && current["effect_certainty"] == "unknown"
        && current.get("stage").is_some_and(Value::is_null);
    if !exact_departed_projection {
        return Ok(true);
    }

    let family_departure_retained = db.query_row(
        "SELECT EXISTS(
             SELECT 1 FROM observations
             WHERE binding_id=?1 AND binding_generation=?2 AND kind=?3
               AND json_extract(payload_json,'$.schema_version')=1
               AND json_extract(payload_json,'$.event_kind')=?3
               AND json_extract(payload_json,'$.occurrence_phase')='module_family_exit'
               AND json_extract(payload_json,'$.proof')='proven_owner_family_departure'
               AND json_extract(payload_json,'$.module_id')=?4
               AND json_extract(payload_json,'$.binding_id')=?1
               AND json_extract(payload_json,'$.generation')=?2
               AND json_extract(payload_json,'$.boot_id')=?5
               AND json_extract(payload_json,'$.actor_instance_id')=?6
               AND json_extract(payload_json,'$.phase') IN ('exited_proven','completed')
               AND json_extract(payload_json,'$.effect_certainty')='unknown'
               AND json_type(payload_json,'$.stage')='null'
         )",
        params![
            binding_id,
            generation,
            super::module_supervisor_observation::FAMILY_EXIT_EVENT_KIND,
            module_id,
            current_boot_id,
            actor_instance_id,
        ],
        |row| row.get::<_, bool>(0),
    )?;
    Ok(!family_departure_retained)
}

/// Unique owned-service scopes required by the exact current page.
///
/// Bounded replacement for the former all-queue owned-intent prepass: at most
/// one indexed probe per unique page scope, so a future-page subject can never
/// abort or delay the current page and repeated pages never rescan all queued
/// state.
fn page_owned_scopes(
    db: &Connection,
    candidates: &[OperationRow],
) -> Result<BTreeSet<(String, i64)>> {
    let mut scopes = BTreeSet::new();
    for operation in candidates {
        if operation.generation <= 0 {
            continue;
        }
        let scope = (operation.binding_id.clone(), operation.generation);
        if scopes.contains(&scope) {
            continue;
        }
        let owned: bool = db.query_row(
            "SELECT EXISTS(
                 SELECT 1 FROM bindings AS b
                 WHERE b.binding_id=?1 AND b.generation=?2
                   AND b.released_at_ms IS NULL
                   AND json_type(b.route_json,'$.owned_service')='object'
                   AND json_type(b.state_json,'$.module_contract_selector')='object'
                   AND EXISTS(
                         SELECT 1 FROM operations AS o
                         WHERE o.binding_id=b.binding_id
                           AND o.binding_generation=b.generation
                           AND o.method='agent.open' AND o.state='queued'
                       )
             )",
            params![scope.0, scope.1],
            |row| row.get(0),
        )?;
        if owned {
            scopes.insert(scope);
        }
    }
    Ok(scopes)
}

/// Retain one exact owned-service intent for each selected module opening
/// before the demand page crosses the Store/process boundary. This is an
/// idempotent Store write containing only route/Task/Attempt/lease facts; all
/// filesystem reads and native effects remain outside the Store writer.
/// Callers pass only the current page's scopes: a future-page subject must
/// never abort the current page.
fn prepare_owned_intents_for_scopes(
    tx: &rusqlite::Transaction<'_>,
    config: &Config,
    scopes: &BTreeSet<(String, i64)>,
) -> Result<BTreeMap<(String, i64), String>> {
    let mut errors = BTreeMap::new();
    for (binding_id, generation) in scopes {
        match launcher_owned_service::ensure_module_owned_service_intent(
            tx,
            config,
            binding_id,
            *generation,
        ) {
            Ok(()) => {}
            // Only failures of this exact retained launch subject are blocks.
            // SQL, shared metadata, conversion and future error codes remain hard.
            Err(error) if owned_intent_error_is_subject_local(&error.code) => {
                errors.insert((binding_id.clone(), *generation), error.code);
            }
            Err(error) => return Err(error),
        }
    }
    Ok(errors)
}

fn owned_intent_error_is_subject_local(code: &str) -> bool {
    matches!(
        code,
        "BINDING_CLOSED"
            | "BINDING_NOT_READY"
            | "CONFIG_ERROR"
            | "FORBIDDEN"
            | "INVALID_LAUNCH_MANIFEST"
            | "LAUNCH_MANIFEST_CORRUPT"
            | "LAUNCH_OPEN_CORRUPT"
            | "LAUNCH_OPEN_MISSING"
            | "NOT_FOUND"
            | "OWNED_SERVICE_CONFIG"
            | "OWNED_SERVICE_RECEIPT_CORRUPT"
            | "OWNED_SERVICE_ROUTE_REQUIRED"
            | "OWNED_SERVICE_SCOPE"
            | "OWNED_SERVICE_SCOPE_STALE"
            | "OWNED_SERVICE_VERSION"
            | "ROUTE_DISABLED"
            | "STALE_LAUNCH"
            | "UNKNOWN_ROUTE"
            | "WORKSPACE_LEASE_AMBIGUOUS"
            | "WORKSPACE_LEASE_INVALID"
            | "WORKSPACE_LEASE_MISSING"
            | "WORKSPACE_LEASE_STALE"
            | "WORKSPACE_REGISTRATION_CHANGED"
            | "WORKSPACE_UNREGISTERED"
    )
}

/// Build a bounded in-process snapshot from committed Operations. Automation
/// dispatches are included because admitted automation actions use the same
/// `operations` rows and binding tuple as direct manager admissions.
pub(super) fn pending(
    db: &mut Connection,
    config: &Config,
    cursor: Option<&ModuleDemandCursor>,
) -> Result<ModuleDemandSnapshot> {
    // Bounded read-only probe first: empty demand and pages whose exact owned
    // intents already exist must never acquire the IMMEDIATE writer
    // reservation. A missing-intent path always revalidates the exact current
    // page under IMMEDIATE before reserving and projecting it atomically.
    let read_tx = db.unchecked_transaction()?;
    let (probe_candidates, probe_truncated, probe_next) = operation_candidates(&read_tx, cursor)?;
    let probe_scopes = page_owned_scopes(&read_tx, &probe_candidates)?;
    let mut probe_preparation_errors = BTreeMap::new();
    let mut missing_intent = false;
    for (binding_id, generation) in &probe_scopes {
        match launcher_owned_service::validate_existing_module_owned_service_intent(
            &read_tx,
            config,
            binding_id,
            *generation,
        ) {
            Ok(true) => {}
            Ok(false) => missing_intent = true,
            Err(error) if owned_intent_error_is_subject_local(&error.code) => {
                probe_preparation_errors
                    .insert((binding_id.clone(), *generation), error.code);
            }
            Err(error) => return Err(error),
        }
    }
    if !missing_intent {
        let snapshot = pending_in_transaction(
            &read_tx,
            config,
            probe_candidates,
            probe_truncated,
            probe_next,
            &probe_preparation_errors,
        )?;
        read_tx.commit()?;
        return Ok(snapshot);
    }
    read_tx.commit()?;
    // Reserve only current-page intents and project the same revalidated page
    // atomically, so a late hard failure rolls back only that page.
    let write_tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (candidates, truncated, next_cursor) = operation_candidates(&write_tx, cursor)?;
    let scopes = page_owned_scopes(&write_tx, &candidates)?;
    let preparation_errors = prepare_owned_intents_for_scopes(&write_tx, config, &scopes)?;
    let snapshot = pending_in_transaction(
        &write_tx,
        config,
        candidates,
        truncated,
        next_cursor,
        &preparation_errors,
    )?;
    write_tx.commit()?;
    Ok(snapshot)
}

fn pending_in_transaction(
    db: &Connection,
    config: &Config,
    candidates: Vec<OperationRow>,
    truncated: bool,
    next_cursor: Option<ModuleDemandCursor>,
    preparation_errors: &BTreeMap<(String, i64), String>,
) -> Result<ModuleDemandSnapshot> {
    let mut output = ModuleDemandSnapshot {
        truncated,
        next_cursor,
        ..Default::default()
    };
    let mut readbacks = BTreeMap::<(String, i64), Vec<StoredOperation>>::new();

    for operation in candidates {
        if operation.generation <= 0 || !MODULE_DEMAND_METHODS.contains(&operation.method.as_str())
        {
            continue;
        }
        let generation = match u64::try_from(operation.generation) {
            Ok(value) if value > 0 => value,
            _ => continue,
        };
        let binding = operations::get_binding(db, &operation.binding_id, operation.generation)?;
        if !binding["released_at_ms"].is_null()
            || binding["route"].is_null()
            || binding["observation"].is_null()
        {
            continue;
        }
        let selector = binding["observation"]
            .get("module_contract_selector")
            .filter(|value| !value.is_null());
        let route_runtime = binding["route"]["runtime"].as_str().unwrap_or_default();
        let artifact_id = binding["module_artifact_id"].as_str().unwrap_or_default();
        let selectorless_builtin =
            crate::config::is_selectorless_builtin_route(route_runtime, artifact_id);
        if selector.is_none() && selectorless_builtin {
            continue;
        }
        if selectorless_builtin {
            output.blocked.push(ModuleDemandBlock {
                binding_id: operation.binding_id.clone(),
                generation,
                operation_id: operation.operation_id.clone(),
                error_code: "MODULE_ROUTE_STALE".to_owned(),
                descriptor: None,
            });
            continue;
        }
        let Some(selector) = selector else {
            output.blocked.push(ModuleDemandBlock {
                binding_id: operation.binding_id.clone(),
                generation,
                operation_id: operation.operation_id.clone(),
                error_code: "MODULE_CONTRACT_REQUIRED".to_owned(),
                descriptor: None,
            });
            continue;
        };
        let retained =
            match module_handshake::retained_contract_identity(db, artifact_id, Some(selector)) {
                Ok(Some(retained)) => retained,
                Ok(None) => {
                    output.blocked.push(ModuleDemandBlock {
                        binding_id: operation.binding_id,
                        generation,
                        operation_id: operation.operation_id,
                        error_code: "MODULE_DESCRIPTOR_MISSING".to_owned(),
                        descriptor: None,
                    });
                    continue;
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "MODULE_ROUTE_CORRUPT"
                            | "MODULE_DESCRIPTOR_MISSING"
                            | "MODULE_DISABLED"
                            | "MODULE_PROTOCOL_INCOMPATIBLE"
                    ) =>
                {
                    output.blocked.push(ModuleDemandBlock {
                        binding_id: operation.binding_id,
                        generation,
                        operation_id: operation.operation_id,
                        error_code: error.code,
                        descriptor: None,
                    });
                    continue;
                }
                Err(error) => return Err(error),
            };
        let descriptor = match module_handshake::retained_descriptor(db, &retained) {
            Ok(descriptor) => descriptor,
            Err(error) if error.code == "MODULE_DESCRIPTOR_MISSING" => {
                output.blocked.push(ModuleDemandBlock {
                    binding_id: operation.binding_id,
                    generation,
                    operation_id: operation.operation_id,
                    error_code: error.code,
                    descriptor: None,
                });
                continue;
            }
            Err(error) => return Err(error),
        };
        let command = match classify_runtime_command(&operation.method, &operation.input) {
            Ok(command) => command,
            Err(error) => {
                output.blocked.push(ModuleDemandBlock {
                    binding_id: operation.binding_id,
                    generation,
                    operation_id: operation.operation_id,
                    error_code: error.code().to_owned(),
                    descriptor: Some(descriptor.clone()),
                });
                continue;
            }
        };
        let required = command.capability();
        if !descriptor
            .capabilities
            .iter()
            .any(|capability| capability_satisfies(capability.as_str(), required))
        {
            output.blocked.push(ModuleDemandBlock {
                binding_id: operation.binding_id,
                generation,
                operation_id: operation.operation_id,
                error_code: "MODULE_CAPABILITY_MISSING".to_owned(),
                descriptor: Some(descriptor.clone()),
            });
            continue;
        }
        if descriptor.launch.credential_ref.is_none() {
            output.blocked.push(ModuleDemandBlock {
                binding_id: operation.binding_id,
                generation,
                operation_id: operation.operation_id,
                error_code: "MODULE_CREDENTIAL_REF_REQUIRED".to_owned(),
                descriptor: Some(descriptor.clone()),
            });
            continue;
        }
        let scope_key = (operation.binding_id.clone(), operation.generation);
        if let Some(error_code) = preparation_errors.get(&scope_key) {
            output.blocked.push(ModuleDemandBlock {
                binding_id: operation.binding_id,
                generation,
                operation_id: operation.operation_id,
                error_code: error_code.clone(),
                descriptor: Some(descriptor.clone()),
            });
            continue;
        }
        if !readbacks.contains_key(&scope_key) {
            match scoped_operation_readback_in_snapshot(db, &scope_key.0, scope_key.1) {
                Ok(readback) => {
                    readbacks.insert(scope_key.clone(), readback);
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "MODULE_READBACK_SCOPE_INVALID"
                            | "MODULE_READBACK_STATE_UNKNOWN"
                            | "MODULE_READBACK_UNRESOLVED_OVERFLOW"
                            | "MODULE_READBACK_CORRUPT"
                    ) =>
                {
                    output.blocked.push(ModuleDemandBlock {
                        binding_id: operation.binding_id,
                        generation,
                        operation_id: operation.operation_id,
                        error_code: error.code,
                        descriptor: Some(descriptor.clone()),
                    });
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        let operation_readback = readbacks.get(&scope_key).cloned().unwrap_or_default();
        let route_native_options = match launcher_owned_service::module_owned_route_options(
            db,
            config,
            &operation.binding_id,
            operation.generation,
            binding["route"]["native_options"].clone(),
        ) {
            Ok(options) => options,
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "BINDING_CLOSED"
                        | "CONFIG_ERROR"
                        | "MODULE_CONFIG_INVALID"
                        | "OWNED_SERVICE_CONFIG"
                        | "OWNED_SERVICE_INTENT_MISSING"
                        | "OWNED_SERVICE_RECOVERY_REQUIRED"
                        | "OWNED_SERVICE_RECEIPT_CORRUPT"
                        | "OWNED_SERVICE_SCOPE"
                        | "OWNED_SERVICE_SCOPE_STALE"
                        | "ROUTE_DISABLED"
                        | "UNKNOWN_ROUTE"
                ) =>
            {
                output.blocked.push(ModuleDemandBlock {
                    binding_id: operation.binding_id,
                    generation,
                    operation_id: operation.operation_id,
                    error_code: error.code,
                    descriptor: Some(descriptor.clone()),
                });
                continue;
            }
            Err(error) => return Err(error),
        };
        let launch_credential_ref = descriptor
            .launch
            .credential_ref
            .as_ref()
            .map(|reference| reference.as_str().to_owned());
        output.demands.push(ModuleDemand {
            module_id: retained.module_id.to_string(),
            artifact_id: retained.artifact.artifact_id.to_string(),
            artifact_version: retained.artifact.version.to_string(),
            descriptor,
            descriptor_revision: retained.descriptor_revision,
            operation_id: operation.operation_id,
            required_capability: required.to_owned(),
            binding_id: operation.binding_id,
            generation,
            module_client_id: binding["observation"]["module_client_id"]
                .as_str()
                .map(str::to_owned),
            credential_ref: launch_credential_ref,
            route_native_options,
            operation_readback,
        });
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model;
    use serde_json::json;

    fn operations_db() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE operations(
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 method TEXT NOT NULL,
                 binding_id TEXT NOT NULL,
                 binding_generation INTEGER NOT NULL,
                 state TEXT NOT NULL,
                 created_at_ms INTEGER NOT NULL
             ) STRICT;
             CREATE INDEX unresolved_target_operations
                 ON operations(binding_id,binding_generation,state);",
        )
        .unwrap();
        db
    }

    fn insert_operation(
        db: &Connection,
        operation_id: &str,
        binding_id: &str,
        generation: i64,
        state: &str,
        created_at_ms: i64,
    ) {
        db.execute(
            "INSERT INTO operations(
                 operation_id,method,binding_id,binding_generation,state,created_at_ms
             ) VALUES(?1,'agent.send',?2,?3,?4,?5)",
            params![operation_id, binding_id, generation, state, created_at_ms],
        )
        .unwrap();
    }

    fn insert_many(
        db: &Connection,
        prefix: &str,
        count: usize,
        binding_id: &str,
        generation: i64,
        state: &str,
    ) {
        let tx = db.unchecked_transaction().unwrap();
        for index in 0..count {
            insert_operation(
                &tx,
                &format!("{prefix}-{index:05}"),
                binding_id,
                generation,
                state,
                index as i64,
            );
        }
        tx.commit().unwrap();
    }

    #[test]
    fn owned_intent_preparation_stays_within_the_bounded_operation_page() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE bindings(
                 binding_id TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 released_at_ms INTEGER,
                 native_root_id TEXT,
                 native_scope_key TEXT,
                 route_json TEXT NOT NULL,
                 state_json TEXT NOT NULL,
                 PRIMARY KEY(binding_id,generation)
             );
             CREATE TABLE operations(
                 operation_id TEXT PRIMARY KEY NOT NULL,
                 method TEXT NOT NULL,
                 binding_id TEXT NOT NULL,
                 binding_generation INTEGER NOT NULL,
                 created_at_ms INTEGER NOT NULL,
                 original_request_json TEXT NOT NULL,
                 state TEXT NOT NULL
             );",
        )
        .unwrap();
        let tx = db.unchecked_transaction().unwrap();
        {
            let mut insert_binding = tx
                .prepare(
                    "INSERT INTO bindings(
                         binding_id,generation,released_at_ms,native_root_id,native_scope_key,
                         route_json,state_json
                     ) VALUES(?1,1,NULL,NULL,NULL,?2,?3)",
                )
                .unwrap();
            let mut insert_open = tx
                .prepare(
                    "INSERT INTO operations(
                         operation_id,method,binding_id,binding_generation,created_at_ms,
                         original_request_json,state
                     ) VALUES(?1,'agent.open',?2,1,1,'{}','queued')",
                )
                .unwrap();
            for index in 0..=MAX_DEMAND_OPERATIONS {
                let binding_id = format!("owned-binding-{index:05}");
                insert_binding
                    .execute(params![
                        binding_id,
                        r#"{"owned_service":{}}"#,
                        r#"{"module_contract_selector":{}}"#
                    ])
                    .unwrap();
                insert_open
                    .execute(params![format!("owned-open-{index:05}"), binding_id])
                    .unwrap();
            }
        }
        tx.commit().unwrap();

        let (candidates, truncated, next_cursor) = operation_candidates(&db, None).unwrap();
        assert!(truncated);
        assert_eq!(candidates.len(), MAX_DEMAND_OPERATIONS);
        assert_eq!(next_cursor.unwrap().operation_id, "owned-open-04095");
        let scopes = page_owned_scopes(&db, &candidates).unwrap();
        assert_eq!(scopes.len(), MAX_DEMAND_OPERATIONS);
        assert!(scopes.contains(&("owned-binding-00000".to_owned(), 1)));
        assert!(!scopes.contains(&("owned-binding-04096".to_owned(), 1)));
    }

    #[test]
    fn terminal_lifetime_history_does_not_limit_empty_unresolved_readback() {
        let db = operations_db();
        insert_many(
            &db,
            "terminal",
            MAX_SCOPE_UNRESOLVED_OPERATIONS + 1,
            "binding-a",
            1,
            "settled",
        );

        let readback = scoped_operation_readback(&db, "binding-a", 1).unwrap();
        assert!(readback.is_empty());
    }

    #[test]
    fn unresolved_readback_is_complete_and_exact_after_terminal_history() {
        let db = operations_db();
        insert_many(&db, "terminal", 5000, "binding-a", 1, "settled");
        insert_many(&db, "active", 255, "binding-a", 1, "sending");
        insert_operation(
            &db,
            "active-native-accepted",
            "binding-a",
            1,
            "native_accepted",
            6000,
        );
        insert_operation(
            &db,
            "active-outcome-unknown",
            "binding-a",
            1,
            "outcome_unknown",
            6001,
        );
        insert_operation(&db, "queued", "binding-a", 1, "queued", 6002);
        insert_operation(&db, "other-binding", "binding-b", 1, "sending", 6003);
        insert_operation(
            &db,
            "other-generation",
            "binding-a",
            2,
            "outcome_unknown",
            6004,
        );

        let readback = scoped_operation_readback(&db, "binding-a", 1).unwrap();
        assert_eq!(readback.len(), 257);
        assert!(readback.iter().all(|operation| {
            operation.binding_id == "binding-a"
                && operation.generation == 1
                && UNRESOLVED_OPERATION_STATES.contains(&operation.state.as_str())
        }));
        assert!(
            readback
                .iter()
                .any(|operation| operation.operation_id == "active-native-accepted")
        );
        assert!(
            readback
                .iter()
                .any(|operation| operation.operation_id == "active-outcome-unknown")
        );
        assert!(!readback.iter().any(|operation| {
            matches!(
                operation.operation_id.as_str(),
                "queued" | "other-binding" | "other-generation"
            )
        }));
    }

    #[test]
    fn unresolved_overflow_is_explicit_and_fails_closed() {
        let db = operations_db();
        insert_many(
            &db,
            "active",
            MAX_SCOPE_UNRESOLVED_OPERATIONS + 1,
            "binding-a",
            1,
            "outcome_unknown",
        );

        let error = scoped_operation_readback(&db, "binding-a", 1).unwrap_err();
        assert_eq!(error.code, "MODULE_READBACK_UNRESOLVED_OVERFLOW");
    }

    #[test]
    fn unknown_state_and_database_errors_are_not_projected_as_empty() {
        let db = operations_db();
        insert_many(&db, "terminal", 5000, "binding-a", 1, "settled");
        insert_operation(&db, "corrupt", "binding-a", 1, "unknown-state", 6000);

        let error = scoped_operation_readback(&db, "binding-a", 1).unwrap_err();
        assert_eq!(error.code, "MODULE_READBACK_STATE_UNKNOWN");

        let missing_table = Connection::open_in_memory().unwrap();
        assert!(scoped_operation_readback(&missing_table, "binding-a", 1).is_err());
    }

    #[test]
    fn managed_owner_presence_settles_only_with_exact_family_departure() {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE observations(
                 binding_id TEXT NOT NULL,
                 binding_generation INTEGER NOT NULL,
                 kind TEXT NOT NULL,
                 payload_json TEXT NOT NULL
             );",
        )
        .unwrap();
        let mut binding = json!({
            "binding_id":"binding-a",
            "generation":1,
            "module_artifact_id":"artifact-a",
            "observation":{
                "bridge_boot_id":"boot-a",
                "managed_owner":{"receipt":"historical-owner"},
                "module_contract_selector":{"artifact":{"version":"1.0.0"}},
                "module_supervisor":{
                    "schema_version":1,
                    "actor_instance_id":"actor-a",
                    "binding_id":"binding-a",
                    "generation":1,
                    "module_id":"module-a",
                    "artifact_id":"artifact-a",
                    "artifact_version":"1.0.0",
                    "boot_id":"boot-a",
                    "phase":"exited_proven",
                    "effect_certainty":"unknown",
                    "stage":null
                }
            }
        });

        assert!(managed_owner_requires_custody(&db, &binding, "module-a", 1).unwrap());

        let departure = json!({
            "schema_version":1,
            "event_kind":"module.family_exited",
            "occurrence_phase":"module_family_exit",
            "proof":"proven_owner_family_departure",
            "module_id":"module-a",
            "binding_id":"binding-a",
            "generation":1,
            "boot_id":"boot-a",
            "actor_instance_id":"actor-a",
            "phase":"exited_proven",
            "effect_certainty":"unknown",
            "stage":null
        });
        db.execute(
            "INSERT INTO observations(binding_id,binding_generation,kind,payload_json)
             VALUES(?1,1,?2,?3)",
            params![
                "binding-a",
                super::super::module_supervisor_observation::FAMILY_EXIT_EVENT_KIND,
                model::canonical(&departure).unwrap(),
            ],
        )
        .unwrap();

        assert!(!managed_owner_requires_custody(&db, &binding, "module-a", 1).unwrap());

        binding["observation"]["module_supervisor"]["phase"] = json!("owner_retained");
        assert!(managed_owner_requires_custody(&db, &binding, "module-a", 1).unwrap());

        binding["observation"]["module_supervisor"]["phase"] = json!("exited_proven");
        binding["observation"]["bridge_boot_id"] = json!("boot-b");
        assert!(managed_owner_requires_custody(&db, &binding, "module-a", 1).unwrap());
    }
}
