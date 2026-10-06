//! Read-only, Store-owned projection of already-admitted module work.
//!
//! This source does not read the metadata catalogue to activate a worker. It
//! starts from exact retained Operation rows and active native-session rows,
//! then resolves each candidate through the immutable selector on its binding.

use super::{module_handshake, operations};
use crate::error::{Error, Result};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use swarm_contracts::module_catalog::ModuleDescriptor;

const MAX_DEMAND_OPERATIONS: usize = 4096;
const MAX_SCOPE_OPERATIONS: usize = 4096;

const MODULE_METHODS: &[&str] = &[
    "agent.open",
    "task.dispatch",
    "agent.send",
    "agent.reply",
    "agent.configure",
    "agent.goal",
    "agent.background",
    "agent.refresh",
    "agent.reconcile",
    "agent.result",
    "agent.recover",
];

/// Exact status-only Operation data used for scoped recovery readback. Inputs,
/// results, identities of callers, and native payloads are deliberately absent.
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

/// Complete status-only readback used before releasing the last host demand
/// lease for a service scope. `native_identity_retained` is a conservative
/// Store fact, not proof that an external/native process is currently alive.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ModuleScopeReadback {
    pub(crate) operations: Vec<StoredOperation>,
    pub(crate) native_identity_retained: bool,
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
}

fn operation_candidates(
    db: &Connection,
    cursor: Option<&ModuleDemandCursor>,
) -> Result<(Vec<OperationRow>, bool, Option<ModuleDemandCursor>)> {
    let cursor_created_at = cursor.map_or(i64::MIN, |value| value.created_at_ms);
    let cursor_operation_id = cursor.map_or("", |value| value.operation_id.as_str());
    let mut statement = db.prepare(
        "SELECT o.operation_id,o.method,o.binding_id,o.binding_generation,o.created_at_ms \
         FROM operations AS o JOIN bindings AS b \
           ON b.binding_id=o.binding_id AND b.generation=o.binding_generation \
         WHERE b.released_at_ms IS NULL \
           AND (o.created_at_ms>?1 OR (o.created_at_ms=?1 AND o.operation_id>?2)) \
           AND o.state IN ('queued','sending','native_accepted','outcome_unknown') \
           AND o.method IN ('agent.open','task.dispatch','agent.send','agent.reply', \
                            'agent.configure','agent.goal','agent.background', \
                            'agent.refresh','agent.reconcile','agent.result','agent.recover') \
         ORDER BY o.created_at_ms,o.operation_id LIMIT ?3",
    )?;
    let mut rows = statement
        .query_map(
            params![
                cursor_created_at,
                cursor_operation_id,
                MAX_DEMAND_OPERATIONS as i64 + 1
            ],
            |row| {
                Ok(OperationRow {
                    operation_id: row.get(0)?,
                    method: row.get(1)?,
                    binding_id: row.get(2)?,
                    generation: row.get(3)?,
                    created_at_ms: row.get(4)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let pending_truncated = rows.len() > MAX_DEMAND_OPERATIONS;
    rows.truncate(MAX_DEMAND_OPERATIONS + 1);

    // A retained live native session is an ongoing attachment obligation even
    // between turns. Anchor its adapter demand to the original admitted
    // agent.open Operation; this starts only the adapter and never replays it.
    let mut statement = db.prepare(
        "SELECT o.operation_id,o.method,o.binding_id,o.binding_generation,o.created_at_ms \
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
            |row| {
                Ok(OperationRow {
                    operation_id: row.get(0)?,
                    method: row.get(1)?,
                    binding_id: row.get(2)?,
                    generation: row.get(3)?,
                    created_at_ms: row.get(4)?,
                })
            },
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
    let mut statement = db.prepare(
        "SELECT operation_id,method,state,binding_id,binding_generation FROM operations \
         WHERE binding_id=?1 AND binding_generation=?2 \
         ORDER BY created_at_ms,operation_id LIMIT ?3",
    )?;
    let rows = statement
        .query_map(
            params![binding_id, generation, MAX_SCOPE_OPERATIONS as i64 + 1],
            |row| {
                let operation_generation: i64 = row.get(4)?;
                Ok(StoredOperation {
                    operation_id: row.get(0)?,
                    method: row.get(1)?,
                    state: row.get(2)?,
                    binding_id: row.get(3)?,
                    generation: u64::try_from(operation_generation).unwrap_or(0),
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if rows.len() > MAX_SCOPE_OPERATIONS {
        return Err(Error::new(
            "MODULE_READBACK_LIMIT",
            "binding has more retained Operations than the bounded module readback supports",
        ));
    }
    if rows.iter().any(|row| row.generation == 0) {
        return Err(Error::new(
            "MODULE_READBACK_CORRUPT",
            "binding Operation has an invalid generation",
        ));
    }
    Ok(rows)
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

/// Build a bounded in-process snapshot from committed Operations. Automation
/// dispatches are included because admitted automation actions use the same
/// `operations` rows and binding tuple as direct manager admissions.
pub(super) fn pending(
    db: &Connection,
    cursor: Option<&ModuleDemandCursor>,
) -> Result<ModuleDemandSnapshot> {
    let (candidates, truncated, next_cursor) = operation_candidates(db, cursor)?;
    let mut output = ModuleDemandSnapshot {
        truncated,
        next_cursor,
        ..Default::default()
    };
    let mut readbacks = BTreeMap::<(String, i64), Vec<StoredOperation>>::new();

    for operation in candidates {
        if operation.generation <= 0 || !MODULE_METHODS.contains(&operation.method.as_str()) {
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
        let required = descriptor
            .capabilities
            .iter()
            .find(|capability| capability.as_str() == operation.method.as_str())
            .or_else(|| {
                (operation.method == "agent.send")
                    .then(|| {
                        descriptor
                            .capabilities
                            .iter()
                            .find(|capability| capability.as_str() == "agent.send/next_turn")
                    })
                    .flatten()
            })
            .map(|capability| capability.as_str().to_owned());
        let Some(required) = required else {
            output.blocked.push(ModuleDemandBlock {
                binding_id: operation.binding_id,
                generation,
                operation_id: operation.operation_id,
                error_code: "MODULE_CAPABILITY_MISSING".to_owned(),
                descriptor: Some(descriptor.clone()),
            });
            continue;
        };
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
            required_capability: required,
            binding_id: operation.binding_id,
            generation,
            module_client_id: binding["observation"]["module_client_id"]
                .as_str()
                .map(str::to_owned),
            credential_ref: launch_credential_ref,
            route_native_options: binding["route"]["native_options"].clone(),
            operation_readback,
        });
    }
    Ok(output)
}
