//! Short, fixed native preparation chains. This is not a workflow engine:
//! one Operation may name one earlier setup Operation on the same binding.
use crate::{
    error::{Error, Result},
    model,
    runtime::opencode_v2,
};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Value, json};
use std::collections::BTreeSet;

const CONFIG_CONTRACT_REVISION: &str = "opencode-instruction-entry-v1";
const SETTINGS_REVISION_KIND: &str = "eliot_owned_instruction_entries_v1";
const SETTINGS_STATE_CONTRACT_REVISION: &str = "opencode-configure-prerequisite-v1";

#[derive(Debug)]
pub(super) enum Gate {
    None,
    Pending { operation_id: String },
    Ready { operation_id: String },
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
            Self::Pending { operation_id } | Self::Ready { operation_id } => Some(operation_id),
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

fn sha256(value: &Value, field: &str) -> Result<String> {
    let digest = model::text(value, field)?;
    let hex = digest
        .strip_prefix("sha256:")
        .filter(|hex| hex.len() == 64 && hex.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| {
            Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                format!("{field} is not a SHA-256 reference"),
            )
        })?;
    Ok(format!("sha256:{}", hex.to_ascii_lowercase()))
}

fn expected_configuration(original: &Value) -> Result<(String, String, Option<String>)> {
    let entry = &original["settings"]["instruction_entry"];
    let action = model::text(entry, "action")?.to_owned();
    let key = model::text(entry, "key")?.to_owned();
    let desired = match action.as_str() {
        "put" => {
            let value = entry.get("value").ok_or_else(|| {
                Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "saved configure put has no value",
                )
            })?;
            let canonical = model::canonical(value)?;
            Some(format!("sha256:{}", model::digest(canonical.as_bytes())))
        }
        "remove" if entry.get("value").is_none() => None,
        _ => {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "saved configure request is not a supported instruction-entry change",
            ));
        }
    };
    Ok((action, key, desired))
}

fn validate_contract(operation: &StoredOperation, binding: &Value) -> Result<()> {
    let contract = &operation.effective["operation_contract"];
    if contract["effect_scope"] != "native_session"
        || contract["completion_condition"] != "native_configuration_applied"
        || contract["application_boundary"] != "next_step_boundary"
        || contract["replay_policy"] != "readback_only_no_mutation_replay"
        || contract["fallback_used"] != false
        || contract["contract_revision"] != CONFIG_CONTRACT_REVISION
        || contract["order_scope"]["binding_id"] != binding["binding_id"]
        || contract["order_scope"]["generation"] != binding["generation"]
    {
        return Err(Error::new(
            "PREREQUISITE_CONTRACT_MISMATCH",
            "prerequisite does not carry the required native-configuration contract",
        ));
    }
    Ok(())
}

fn validate_applied_result(
    operation: &StoredOperation,
    binding: &Value,
    result: &Value,
) -> Result<(String, String, Option<String>, String)> {
    validate_contract(operation, binding)?;
    let (action, key, desired_digest) = expected_configuration(&operation.original)?;
    let details = &result["details"];
    if result["operation_id"] != operation.operation_id
        || result["outcome"] != "applied"
        || result["native_root_id"] != binding["native_root_id"]
        || result["native_scope_key"] != binding["native_scope_key"]
        || details["completion_condition"] != "native_configuration_applied"
        || details["configuration_kind"] != "instruction_entry"
        || details["action"] != action
        || details["key"] != key
        || details["application_scope"] != "session"
        || details["application_boundary"] != "next_step_boundary"
        || details["native_applied"] != true
        || details["model_work_started"] != false
        || details["replay_policy"] != "readback_only_no_mutation_replay"
        || details["contract_revision"] != CONFIG_CONTRACT_REVISION
        || details["settings_revision_kind"] != SETTINGS_REVISION_KIND
    {
        return Err(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "configure result does not prove the required typed native application",
        ));
    }
    match desired_digest.as_deref() {
        Some(expected) if details["desired_digest"].as_str() == Some(expected) => {}
        None if details["desired_digest"].is_null() => {}
        _ => {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "configure result does not match the saved requested value",
            ));
        }
    }
    let evidence = model::text(details, "evidence")?;
    let mutation_sent = details["mutation_sent"]
        .as_bool()
        .ok_or_else(|| Error::new("PREREQUISITE_EVIDENCE_INVALID", "missing mutation evidence"))?;
    let evidence_matches = matches!(
        (evidence, mutation_sent),
        ("preexisting_exact_readback", false)
            | ("post_mutation_exact_readback", true)
            | ("exact_state_reconciliation", false)
    );
    if !evidence_matches || details["read_method"] != "experimental.session.instructions.entry.list"
    {
        return Err(Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "configure result has an unsupported readback proof",
        ));
    }
    let settings_revision = sha256(details, "settings_revision")?;
    sha256(details, "entries_revision")?;
    Ok((action, key, desired_digest, settings_revision))
}

fn snapshot_matches(
    binding: &Value,
    action: &str,
    key: &str,
    desired_digest: Option<&str>,
) -> Result<Option<bool>> {
    let configuration = &binding["observation"]["native"]["configuration"];
    if configuration["complete"] != true {
        return Ok(None);
    }
    sha256(configuration, "revision")?;
    let entries = configuration["owned_entries"].as_array().ok_or_else(|| {
        Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "invalid native configuration snapshot",
        )
    })?;
    let mut keys = BTreeSet::new();
    let mut found = None;
    for entry in entries {
        let observed_key = model::text(entry, "key")?;
        if !keys.insert(observed_key.to_owned()) {
            return Err(Error::new(
                "PREREQUISITE_EVIDENCE_INVALID",
                "native configuration snapshot contains a duplicate key",
            ));
        }
        let digest = sha256(entry, "value_digest")?;
        if observed_key == key {
            found = Some(digest);
        }
    }
    Ok(Some(match action {
        "put" => found.as_deref() == desired_digest,
        "remove" => found.is_none(),
        _ => false,
    }))
}

fn current_settings(
    db: &Connection,
    binding: &Value,
    prerequisite: &StoredOperation,
    current_rowid: i64,
    action: &str,
    key: &str,
    desired_digest: Option<&str>,
) -> Result<Gate> {
    let binding_id = model::text(binding, "binding_id")?;
    let generation = model::positive(binding, "generation")?;
    let mut latest_settled_at = prerequisite.settled_at_ms.ok_or_else(|| {
        Error::new(
            "PREREQUISITE_EVIDENCE_INVALID",
            "settled prerequisite is missing its settlement boundary",
        )
    })?;
    let mut latest_matches = true;
    let mut stmt = db.prepare(
        "SELECT operation_id FROM operations WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.configure' AND rowid>?3 AND rowid<?4 AND json_extract(original_request_json,'$.settings.instruction_entry.key')=?5 ORDER BY rowid",
    )?;
    let ids = stmt
        .query_map(
            params![
                binding_id,
                generation,
                prerequisite.rowid,
                current_rowid,
                key
            ],
            |row| row.get::<_, String>(0),
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    for operation_id in ids {
        let operation = load(db, &operation_id)?;
        match operation.state.as_str() {
            "queued" | "sending" | "native_accepted" | "outcome_unknown" => {
                return Ok(Gate::Pending { operation_id });
            }
            "rejected" | "cancelled" => continue,
            "settled" => {
                let result = operation.result.as_ref().ok_or_else(|| {
                    Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "later settled configuration has no typed result",
                    )
                })?;
                let (later_action, later_key, later_digest, _) =
                    validate_applied_result(&operation, binding, result)?;
                if later_key != key {
                    return Err(Error::new(
                        "PREREQUISITE_EVIDENCE_INVALID",
                        "later configuration changed scope during validation",
                    ));
                }
                latest_matches =
                    later_action == action && later_digest.as_deref() == desired_digest;
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
        && let Some(matches) = snapshot_matches(binding, action, key, desired_digest)?
    {
        latest_matches = matches;
    }
    if latest_matches {
        Ok(Gate::Ready {
            operation_id: prerequisite.operation_id.clone(),
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
    if binding["route"]["runtime"] != opencode_v2::RUNTIME {
        return Ok(Gate::Failed(Error::new(
            "UNSUPPORTED_PREREQUISITE",
            "this runtime does not implement persisted configure-to-input prerequisites",
        )));
    }
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
    match prerequisite.state.as_str() {
        "queued" | "sending" | "native_accepted" | "outcome_unknown" => Ok(Gate::Pending {
            operation_id: prerequisite.operation_id,
        }),
        "settled" => {
            let Some(result) = prerequisite.result.as_ref() else {
                return Ok(Gate::Failed(Error::new(
                    "PREREQUISITE_EVIDENCE_INVALID",
                    "settled prerequisite has no typed result",
                )));
            };
            match validate_applied_result(&prerequisite, binding, result).and_then(
                |(action, key, desired_digest, _)| {
                    current_settings(
                        db,
                        binding,
                        &prerequisite,
                        current_rowid,
                        &action,
                        &key,
                        desired_digest.as_deref(),
                    )
                },
            ) {
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

pub(super) fn applied_settings(
    db: &Connection,
    binding: &Value,
    operation_id: &str,
    result: &Value,
    observed_at_ms: i64,
) -> Result<Value> {
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
    let (_, _, _, settings_revision) = validate_applied_result(&operation, binding, result)?;
    Ok(json!({
        "kind":SETTINGS_REVISION_KIND,
        "revision":settings_revision,
        "entries_revision":result["details"]["entries_revision"],
        "operation_id":operation_id,
        "key":result["details"]["key"],
        "action":result["details"]["action"],
        "desired_digest":result["details"]["desired_digest"],
        "complete":true,
        "source":"exact_configuration_result",
        "observed_at_ms":observed_at_ms,
        "contract_revision":SETTINGS_STATE_CONTRACT_REVISION
    }))
}

pub(super) fn observed_settings(
    binding: &Value,
    native_state: &Value,
    observation_id: i64,
    observed_at_ms: i64,
) -> Result<Option<Value>> {
    let configuration = &native_state["configuration"];
    if configuration["complete"] != true {
        return Ok(None);
    }
    let revision = sha256(configuration, "revision")?;
    let prior = &binding["observation"]["effective_settings"];
    let operation_id = if prior["kind"] == SETTINGS_REVISION_KIND
        && prior["contract_revision"] == SETTINGS_STATE_CONTRACT_REVISION
        && prior["complete"] == true
        && prior["revision"] == revision
    {
        prior["operation_id"].clone()
    } else {
        Value::Null
    };
    Ok(Some(json!({
        "kind":SETTINGS_REVISION_KIND,
        "revision":revision,
        "operation_id":operation_id,
        "complete":true,
        "source":"native_snapshot",
        "observation_id":observation_id,
        "observed_at_ms":observed_at_ms,
        "contract_revision":SETTINGS_STATE_CONTRACT_REVISION
    })))
}
