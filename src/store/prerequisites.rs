//! Short, fixed native preparation chains. This is not a workflow engine:
//! one Operation may name one earlier setup Operation on the same binding.
//!
//! This module is the generic receipt/barrier only: Operation and
//! binding/generation ownership, earlier-Operation ordering, the typed
//! completion boundary, digest/revision equality over validated evidence,
//! the later-conflicting-evidence check, and observation freshness. All
//! vendor interpretation of configuration evidence lives behind the
//! runtime-side validator registry (`crate::runtime::prerequisites`),
//! keyed by runtime kind; this module names no vendor types, kinds, or
//! contract revisions.
use crate::{
    error::{Error, Result},
    model,
    runtime::prerequisites::{
        EffectiveRecord, EffectiveSlot, Expectation, ValidatedEvidence, Validator, validator_for,
    },
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::Value;

#[derive(Debug)]
pub(super) enum Gate {
    None,
    Pending {
        operation_id: String,
        blocking_operation_id: Option<String>,
        contract_revision: String,
    },
    Ready {
        operation_id: String,
        contract_revision: String,
    },
    Failed(Error),
}

impl Gate {
    pub(super) fn receipt_state(&self) -> Option<&'static str> {
        match self {
            Self::None => None,
            Self::Pending { .. } => Some("pending"),
            Self::Ready { .. } => Some("satisfied"),
            Self::Failed(_) => Some("failed"),
        }
    }

    pub(super) fn operation_id(&self) -> Option<&str> {
        match self {
            Self::None | Self::Failed(_) => None,
            Self::Pending { operation_id, .. } | Self::Ready { operation_id, .. } => {
                Some(operation_id)
            }
        }
    }

    pub(super) fn contract_revision(&self) -> Option<&str> {
        match self {
            Self::None | Self::Failed(_) => None,
            Self::Pending {
                contract_revision, ..
            }
            | Self::Ready {
                contract_revision, ..
            } => Some(contract_revision),
        }
    }
}

struct StoredOperation {
    rowid: i64,
    operation_id: String,
    method: String,
    state: String,
    binding_id: Option<String>,
    binding_generation: Option<i64>,
    prerequisite_operation_id: Option<String>,
    settled_at_ms: Option<i64>,
    original: Value,
    effective: Value,
    result: Option<Value>,
}

type StoredOperationRow = (
    i64,
    String,
    String,
    String,
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<i64>,
    String,
    String,
    Option<String>,
);

fn load(db: &Connection, operation_id: &str) -> Result<StoredOperation> {
    let row: Option<StoredOperationRow> = db
        .query_row(
            "SELECT rowid,operation_id,method,state,binding_id,binding_generation,prerequisite_operation_id,settled_at_ms,original_request_json,effective_request_json,result_json FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                    row.get(8)?,
                    row.get(9)?,
                    row.get(10)?,
                ))
            },
        )
        .optional()?;
    let (
        rowid,
        operation_id,
        method,
        state,
        binding_id,
        binding_generation,
        prerequisite_operation_id,
        settled_at_ms,
        original,
        effective,
        result,
    ) = row.ok_or_else(|| {
        Error::new(
            "PREREQUISITE_NOT_FOUND",
            "prerequisite Operation does not exist",
        )
    })?;
    Ok(StoredOperation {
        rowid,
        operation_id,
        method,
        state,
        binding_id,
        binding_generation,
        prerequisite_operation_id,
        settled_at_ms,
        original: serde_json::from_str(&original)?,
        effective: serde_json::from_str(&effective)?,
        result: result
            .map(|value| serde_json::from_str(&value))
            .transpose()?,
    })
}

fn binding_validator(binding: &Value) -> Option<Validator> {
    validator_for(binding["route"]["runtime"].as_str().unwrap_or_default())
}

/// The generic half of contract validation: the recorded contract must
/// order its effects on this exact binding generation. Every other
/// contract field is adapter vocabulary and is checked by the validator.
fn validate_order_scope(operation: &StoredOperation, binding: &Value) -> Result<()> {
    let contract = &operation.effective["operation_contract"];
    if contract["order_scope"]["binding_id"] != binding["binding_id"]
        || contract["order_scope"]["generation"] != binding["generation"]
    {
        return Err(Error::new(
            "PREREQUISITE_CONTRACT_MISMATCH",
            "prerequisite does not carry the required native-configuration contract",
        ));
    }
    Ok(())
}

fn validate_contract(
    validator: &Validator,
    operation: &StoredOperation,
    binding: &Value,
    expectation: &Expectation,
) -> Result<()> {
    validate_order_scope(operation, binding)?;
    validator.validate_contract(expectation, &operation.effective["operation_contract"])
}

/// The generic half of result validation: the settled result envelope
/// must belong to this Operation and to this binding's native root. The
/// adapter-owned details are validated by the validator.
fn validate_applied_result(
    validator: &Validator,
    operation: &StoredOperation,
    binding: &Value,
    expectation: &Expectation,
    result: &Value,
) -> Result<ValidatedEvidence> {
    if result["operation_id"] != operation.operation_id
        || result["outcome"] != "applied"
        || result["native_root_id"] != binding["native_root_id"]
        || result["native_scope_key"] != binding["native_scope_key"]
    {
        return Err(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "configure result does not prove the required typed native application",
        ));
    }
    validator.validate_applied(
        expectation,
        &operation.effective["operation_contract"],
        &result["details"],
    )
}

fn current_configuration(
    db: &Connection,
    binding: &Value,
    validator: &Validator,
    prerequisite: &StoredOperation,
    current_rowid: i64,
    validated: &ValidatedEvidence,
) -> Result<Gate> {
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let validated_scope = validator.evidence_scope(validated)?;
    let mut latest_settled_at = prerequisite.settled_at_ms.ok_or_else(|| {
        Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "settled prerequisite is missing its settlement boundary",
        )
    })?;
    let mut latest_matches = true;
    let mut stmt = db.prepare(
        "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.configure' AND rowid>?3 AND rowid<?4 ORDER BY rowid",
    )?;
    let ids = stmt
        .query_map(
            params![binding_id, generation, prerequisite.rowid, current_rowid],
            |row| row.get::<_, String>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    for operation_id in ids {
        let operation = load(db, &operation_id)?;
        let later_expectation = validator.parse_expectation(&operation.original)?;
        if validator.scope(&later_expectation)? != validated_scope {
            continue;
        }
        validate_contract(validator, &operation, binding, &later_expectation)?;
        match operation.state.as_str() {
            "queued" | "sending" | "native_accepted" | "outcome_unknown" => {
                return Ok(Gate::Pending {
                    operation_id: prerequisite.operation_id.clone(),
                    blocking_operation_id: Some(operation_id),
                    contract_revision: validator.evidence_contract_revision(validated)?.to_owned(),
                });
            }
            "rejected" | "cancelled" => continue,
            "settled" => {
                let result = operation.result.as_ref().ok_or_else(|| {
                    Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "later settled configuration has no typed result",
                    )
                })?;
                let later = validate_applied_result(
                    validator,
                    &operation,
                    binding,
                    &later_expectation,
                    result,
                )?;
                if !validator.evidence_same_scope(validated, &later)? {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "later configuration changed scope during validation",
                    ));
                }
                latest_matches = validator.same_effective(validated, &later)?;
                latest_settled_at = operation.settled_at_ms.ok_or_else(|| {
                    Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "later settled configuration is missing its settlement boundary",
                    )
                })?;
            }
            _ => {
                return Err(Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "later configuration has an unsupported state",
                ));
            }
        }
    }

    let snapshot_time = binding["observation"]["observed_at_ms"].as_i64();
    if snapshot_time.is_some_and(|observed| observed >= latest_settled_at)
        && let Some(matches) =
            validator.snapshot_matches(validated, &binding["observation"]["native"])?
    {
        latest_matches = matches;
    }
    if latest_matches {
        Ok(Gate::Ready {
            operation_id: prerequisite.operation_id.clone(),
            contract_revision: validator.evidence_contract_revision(validated)?.to_owned(),
        })
    } else {
        Ok(Gate::Failed(Error::new(
            "PREREQUISITE_STALE",
            "a later effective change invalidated the referenced setup scope",
        )))
    }
}

fn gate_for_id(
    db: &Connection,
    binding: &Value,
    current_rowid: i64,
    operation_id: &str,
) -> Result<Gate> {
    let Some(validator) = binding_validator(binding) else {
        return Ok(Gate::Failed(Error::new(
            "UNSUPPORTED_PREREQUISITE",
            "this runtime does not implement persisted configure-to-input prerequisites",
        )));
    };
    let prerequisite = load(db, operation_id)?;
    if prerequisite.rowid >= current_rowid {
        return Ok(Gate::Failed(Error::new(
            "INVALID_PREREQUISITE",
            "prerequisite must be an earlier Operation",
        )));
    }
    if prerequisite.method != "agent.configure"
        || prerequisite.binding_id.as_deref() != binding["binding_id"].as_str()
        || prerequisite.binding_generation != binding["generation"].as_i64()
    {
        return Ok(Gate::Failed(Error::new(
            "INVALID_PREREQUISITE",
            "prerequisite must be an agent.configure Operation on this exact binding generation",
        )));
    }
    let expectation = match validator.parse_expectation(&prerequisite.original) {
        Ok(expectation) => expectation,
        Err(error) => return Ok(Gate::Failed(error)),
    };
    if let Err(error) = validate_contract(&validator, &prerequisite, binding, &expectation) {
        return Ok(Gate::Failed(error));
    }
    let contract_revision = match validator.contract_revision(&expectation) {
        Ok(revision) => revision.to_owned(),
        Err(error) => return Ok(Gate::Failed(error)),
    };
    match prerequisite.state.as_str() {
        "queued" | "sending" | "native_accepted" | "outcome_unknown" => Ok(Gate::Pending {
            operation_id: prerequisite.operation_id,
            blocking_operation_id: None,
            contract_revision,
        }),
        "settled" => {
            let Some(result) = prerequisite.result.as_ref() else {
                return Ok(Gate::Failed(Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "settled prerequisite has no typed result",
                )));
            };
            match validate_applied_result(&validator, &prerequisite, binding, &expectation, result)
                .and_then(|validated| {
                    current_configuration(
                        db,
                        binding,
                        &validator,
                        &prerequisite,
                        current_rowid,
                        &validated,
                    )
                }) {
                Ok(gate) => Ok(gate),
                Err(error) => Ok(Gate::Failed(error)),
            }
        }
        "rejected" | "cancelled" => Ok(Gate::Failed(Error::new(
            "PREREQUISITE_FAILED",
            "referenced configuration was rejected or cancelled",
        ))),
        _ => Ok(Gate::Failed(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "referenced configuration has an unsupported state",
        ))),
    }
}

fn requested_id(value: &Value) -> Result<Option<String>> {
    value
        .get("prerequisite_operation_id")
        .map(|_| {
            let operation_id = model::text(value, "prerequisite_operation_id")?;
            if operation_id.is_empty()
                || operation_id.len() > 128
                || operation_id
                    .bytes()
                    .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            {
                return Err(Error::invalid("invalid prerequisite_operation_id"));
            }
            Ok(operation_id.to_owned())
        })
        .transpose()
}

pub(super) fn validate_request(
    db: &Connection,
    binding: &Value,
    request: &Value,
    current_operation_id: &str,
) -> Result<Gate> {
    let Some(operation_id) = requested_id(request)? else {
        return Ok(Gate::None);
    };
    let current = load(db, current_operation_id)?;
    match gate_for_id(db, binding, current.rowid, &operation_id)? {
        Gate::Failed(error) => Err(error),
        gate => Ok(gate),
    }
}

pub(super) fn for_operation(
    db: &Connection,
    binding: &Value,
    current_operation_id: &str,
) -> Result<Gate> {
    let current = load(db, current_operation_id)?;
    let Some(operation_id) = current.prerequisite_operation_id.as_deref() else {
        return Ok(Gate::None);
    };
    gate_for_id(db, binding, current.rowid, operation_id)
}

/// Validate an applied configure result and build the effective record
/// the caller folds into binding state. Generic ownership checks happen
/// here; every adapter-owned check happens in the validator.
pub(super) fn applied_configuration(
    db: &Connection,
    binding: &Value,
    operation_id: &str,
    result: &Value,
    observed_at_ms: i64,
) -> Result<EffectiveRecord> {
    let operation = load(db, operation_id)?;
    if operation.method != "agent.configure"
        || operation.binding_id.as_deref() != binding["binding_id"].as_str()
        || operation.binding_generation != binding["generation"].as_i64()
    {
        return Err(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "configuration result is outside this exact binding generation",
        ));
    }
    let validator = binding_validator(binding).ok_or_else(|| {
        Error::new(
            "UNSUPPORTED_PREREQUISITE",
            "this runtime does not implement persisted configure-to-input prerequisites",
        )
    })?;
    let expectation = validator.parse_expectation(&operation.original)?;
    let validated = validate_applied_result(&validator, &operation, binding, &expectation, result)?;
    validator.applied_record(&validated, operation_id, result, observed_at_ms)
}

/// Fold a native observation into effective-configuration records via the
/// binding runtime's validator. Runtimes without a registered validator
/// produce no records.
pub(super) fn observed_configurations(
    binding: &Value,
    native_state: &Value,
    observation_id: i64,
    observed_at_ms: i64,
) -> Result<Vec<EffectiveRecord>> {
    let Some(validator) = binding_validator(binding) else {
        return Ok(Vec::new());
    };
    validator.observed_records(
        &binding["observation"],
        native_state,
        observation_id,
        observed_at_ms,
    )
}

/// The binding-state JSON path a folded effective record belongs to.
pub(super) fn slot_path(slot: EffectiveSlot) -> &'static str {
    match slot {
        EffectiveSlot::Settings => "$.effective_settings",
        EffectiveSlot::Agent => "$.effective_agent",
        EffectiveSlot::Model => "$.effective_model",
    }
}
