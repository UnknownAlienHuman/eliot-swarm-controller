//! Explicit current-GM transfer of one manager-owned automation entry.
//!
//! The source entry remains as a disabled historical snapshot. Its Operations,
//! callers, causal links, and global cursors are not rewritten. Module-owned
//! per-entry ledgers are relocated in this transaction.

use super::{
    automation_cron, automation_dispatch, automation_goal_progression, automation_publication,
    automation_work_dispatch, review_disposition,
};
use crate::{
    automation::{
        authorization,
        config::{self, TransferProvenance, TransferRequest},
    },
    error::{Error, Result},
    model::{Principal, Role},
};
use rusqlite::{OptionalExtension, Transaction};
use serde_json::{Value, json};

type GmScopeRow = (String, i64);

pub(super) fn apply(
    tx: &Transaction<'_>,
    principal: &Principal,
    value: &Value,
    operation_id: &str,
    now_ms: i64,
) -> Result<Value> {
    if !matches!(principal.role, Role::Manager | Role::Operator) {
        return Err(Error::new(
            "FORBIDDEN",
            "automation ownership transfer requires current GM or Operator authority",
        ));
    }
    super::gm::require_authority(tx, principal)?;
    if principal.role == Role::Manager {
        authorization::require_registered_manager(tx, &principal.client_id)?;
    } else {
        super::require_local_operator(tx, &principal.client_id)?;
    }
    if now_ms < 0 {
        return Err(Error::invalid("transfer timestamp must be non-negative"));
    }

    let request = TransferRequest::parse(value)?;
    let gm: Option<GmScopeRow> = tx
        .query_row(
            "SELECT json_extract(value_json,'$.client_id'),json_extract(value_json,'$.epoch') FROM meta WHERE key='gm'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    let Some((new_owner_manager_id, gm_epoch)) = gm else {
        return Err(Error::new(
            "FORBIDDEN",
            "automation ownership transfer requires a current GM designation",
        ));
    };
    if gm_epoch <= 0 || new_owner_manager_id == request.former_owner_manager_id {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CONFLICT",
            "the current GM must be different from the former automation owner",
        ));
    }
    authorization::require_registered_manager(tx, &new_owner_manager_id)?;
    config::require_not_transferred(
        tx,
        &request.former_owner_manager_id,
        &request.project_id,
        &request.automation_id,
    )?;

    let former = config::load_entry(
        tx,
        &request.former_owner_manager_id,
        &request.project_id,
        &request.automation_id,
    )?
    .ok_or_else(|| Error::new("NOT_FOUND", "former-owner automation entry was not found"))?;
    if former.revision != request.expected_revision {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_STALE",
            "former-owner automation revision changed before transfer",
        ));
    }
    if config::load_entry(
        tx,
        &new_owner_manager_id,
        &request.project_id,
        &request.automation_id,
    )?
    .is_some()
        || config::transfer_into_target(
            tx,
            &new_owner_manager_id,
            &request.project_id,
            &request.automation_id,
        )?
        .is_some()
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CONFLICT",
            "current GM already has this automation identity or transfer history",
        ));
    }
    if operation_id.is_empty() || operation_id.len() > 128 {
        return Err(Error::invalid("transfer Operation ID is invalid"));
    }
    if config::read_record(
        tx,
        &config::transfer_record_key(operation_id)?,
        "automation ownership transfer",
    )?
    .is_some()
    {
        return Err(Error::new(
            "AUTOMATION_TRANSFER_CONFLICT",
            "this Operation ID already has an ownership transfer record",
        ));
    }

    let entry_count: i64 = tx.query_row(
        "SELECT COUNT(*) FROM meta WHERE key LIKE ?1",
        [format!(
            "{}%",
            config::entry_prefix(&new_owner_manager_id, &request.project_id)?
        )],
        |row| row.get(0),
    )?;
    if entry_count >= config::MAX_AUTOMATIONS_PER_SCOPE as i64 {
        return Err(Error::new(
            "AUTOMATION_SCOPE_LIMIT",
            "current GM automation scope is at its entry limit",
        ));
    }

    let mut successor = former.clone();
    successor.owner_manager_id.clone_from(&new_owner_manager_id);
    successor.revision = former.revision.checked_add(1).ok_or_else(|| {
        Error::new(
            "AUTOMATION_REVISION_EXHAUSTED",
            "automation revision exhausted",
        )
    })?;
    successor.updated_at_ms = now_ms;
    config::validate_entry(&successor)?;

    let mut historical = former.clone();
    historical.enabled = false;
    config::validate_entry(&historical)?;

    // The module-owned loaders validate each source ledger against the exact
    // source entry. All copies and deletions remain inside this Store txn.
    automation_dispatch::relocate_state(tx, &former, &successor)?;
    automation_work_dispatch::relocate_state(tx, &former, &successor)?;
    automation_publication::relocate_state(tx, &former, &successor)?;
    review_disposition::relocate_state(tx, &former, &successor)?;
    automation_goal_progression::relocate_state(tx, &former, &successor)?;

    config::write_record(
        tx,
        &config::entry_key(
            &request.former_owner_manager_id,
            &request.project_id,
            &request.automation_id,
        )?,
        &historical.value()?,
    )?;
    config::write_record(
        tx,
        &config::entry_key(
            &new_owner_manager_id,
            &request.project_id,
            &request.automation_id,
        )?,
        &successor.value()?,
    )?;

    let record = TransferProvenance {
        schema_version: 1,
        transfer_operation_id: operation_id.to_owned(),
        project_id: request.project_id.clone(),
        automation_id: request.automation_id.clone(),
        former_owner_manager_id: request.former_owner_manager_id.clone(),
        new_owner_manager_id: new_owner_manager_id.clone(),
        former_owner_revision: former.revision,
        new_owner_revision: successor.revision,
        created_at_ms: now_ms,
    };
    config::validate_transfer_record(&record, operation_id)?;
    config::write_record(
        tx,
        &config::transfer_record_key(operation_id)?,
        &serde_json::to_value(&record)?,
    )?;
    config::write_record(
        tx,
        &config::transfer_source_key(
            &request.former_owner_manager_id,
            &request.project_id,
            &request.automation_id,
        )?,
        &config::transfer_pointer_value(operation_id)?,
    )?;
    config::write_record(
        tx,
        &config::transfer_target_key(
            &new_owner_manager_id,
            &request.project_id,
            &request.automation_id,
        )?,
        &config::transfer_pointer_value(operation_id)?,
    )?;
    // The cron ledger is keyed by the original logical-entry identity. Call
    // only after the sealed source/target pointers are present so A→B→C can
    // resolve the same origin without copying occurrence or Operation state.
    automation_cron::relocate_state(tx, &former, &successor)?;

    Ok(json!({
        "operation_id":operation_id,
        "status":"transferred",
        "transfer_operation_id":operation_id,
        "project_id":request.project_id,
        "automation_id":request.automation_id,
        "former_owner_manager_id":request.former_owner_manager_id,
        "new_owner_manager_id":new_owner_manager_id,
        "former_owner_revision":former.revision,
        "new_owner_revision":successor.revision,
        "gm_epoch":gm_epoch,
        "state_ledgers_relocated":7
    }))
}
