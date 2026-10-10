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
use std::collections::BTreeMap;
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
    if generation <= 0 {
        return Err(Error::new(
            "MODULE_READBACK_SCOPE_INVALID",
            "module Operation readback requires a positive binding generation",
        ));
    }

    // Keep the state-integrity probe and unresolved projection on one SQLite
    // snapshot. The scope/state index lets the probe seek the fixed gaps in
    // the Store's state vocabulary without walking terminal lifetime history.
    let tx = db.unchecked_transaction()?;
    if let Some(state) = unknown_operation_state(&tx, binding_id, generation)? {
        return Err(Error::new(
            "MODULE_READBACK_STATE_UNKNOWN",
            format!("binding Operation has an unrecognized state: {state:?}"),
        ));
    }

    let mut statement = tx.prepare(
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

    tx.commit()?;
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
    let native_identity_retained = native_root.is_some()
        || native_scope.is_some()
        || !binding["observation"]["managed_owner"].is_null();
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
    Ok(ModuleScopeReadback {
        operations,
        native_identity_retained,
        module_hello_boot_id,
    })
}

/// Retain one exact owned-service intent for each selected module opening
/// before the demand page crosses the Store/process boundary. This is an
/// idempotent Store write containing only route/Task/Attempt/lease facts; all
/// filesystem reads and native effects remain outside the Store writer.
fn prepare_owned_intents(
    db: &mut Connection,
    config: &Config,
) -> Result<BTreeMap<(String, i64), String>> {
    let candidates = {
        let mut statement = db.prepare(
            "SELECT DISTINCT b.binding_id,b.generation
             FROM bindings AS b
             JOIN operations AS o
               ON o.binding_id=b.binding_id AND o.binding_generation=b.generation
             WHERE b.released_at_ms IS NULL
               AND json_type(b.route_json,'$.owned_service')='object'
               AND json_type(b.state_json,'$.module_contract_selector')='object'
               AND o.method='agent.open' AND o.state='queued'",
        )?;
        statement
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?
    };
    if candidates.is_empty() {
        return Ok(BTreeMap::new());
    }
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut errors = BTreeMap::new();
    for (binding_id, generation) in candidates {
        if let Err(error) = launcher_owned_service::ensure_module_owned_service_intent(
            &tx,
            config,
            &binding_id,
            generation,
        ) {
            errors.insert((binding_id, generation), error.code);
        }
    }
    tx.commit()?;
    Ok(errors)
}

/// Build a bounded in-process snapshot from committed Operations. Automation
/// dispatches are included because admitted automation actions use the same
/// `operations` rows and binding tuple as direct manager admissions.
pub(super) fn pending(
    db: &mut Connection,
    config: &Config,
    cursor: Option<&ModuleDemandCursor>,
) -> Result<ModuleDemandSnapshot> {
    let preparation_errors = prepare_owned_intents(db, config)?;
    let (candidates, truncated, next_cursor) = operation_candidates(db, cursor)?;
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
        let selector = binding["observation"].get("module_contract_selector");
        let retained = match module_handshake::retained_contract_identity(
            db,
            binding["module_artifact_id"].as_str().unwrap_or_default(),
            selector,
        ) {
            Ok(Some(retained)) => retained,
            Ok(None) => continue, // Legacy bindings are not inferred as modules.
            Err(error) => {
                output.blocked.push(ModuleDemandBlock {
                    binding_id: operation.binding_id,
                    generation,
                    operation_id: operation.operation_id,
                    error_code: error.code,
                    descriptor: None,
                });
                continue;
            }
        };
        let descriptor = match module_handshake::retained_descriptor(db, &retained) {
            Ok(descriptor) => descriptor,
            Err(error) => {
                output.blocked.push(ModuleDemandBlock {
                    binding_id: operation.binding_id,
                    generation,
                    operation_id: operation.operation_id,
                    error_code: error.code,
                    descriptor: None,
                });
                continue;
            }
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
            match scoped_operation_readback(db, &scope_key.0, scope_key.1) {
                Ok(readback) => {
                    readbacks.insert(scope_key.clone(), readback);
                }
                Err(error) => {
                    output.blocked.push(ModuleDemandBlock {
                        binding_id: operation.binding_id,
                        generation,
                        operation_id: operation.operation_id,
                        error_code: error.code,
                        descriptor: Some(descriptor.clone()),
                    });
                    continue;
                }
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
            Err(error) => {
                output.blocked.push(ModuleDemandBlock {
                    binding_id: operation.binding_id,
                    generation,
                    operation_id: operation.operation_id,
                    error_code: error.code,
                    descriptor: Some(descriptor.clone()),
                });
                continue;
            }
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
}
