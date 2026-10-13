use super::super::automation_reconcile::{self, QuarantineEvidence, QuarantinedSubject};
use super::super::{meta, set_meta};
use crate::{
    automation::config,
    error::{Error, Result},
    model::{self, Principal},
    runtime::RuntimeCommand,
};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const CURSOR_PREFIX: &str = "opencode:v1:cursor:";
const QUARANTINE_PREFIX: &str = "opencode:v1:row-quarantine:";
const SEMANTIC_COMMAND_ERROR: &str = "RUNTIME_COMMAND_INVALID";
const MAX_ROW_SCAN: usize = 16;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CursorRecord {
    schema_version: u16,
    reader: String,
    binding_id: String,
    generation: i64,
    after_operation_id: String,
}

struct RetainedRow {
    operation_id: String,
    original_request_json: String,
}

enum DecodedRow {
    Ready(Box<RuntimeCommand>),
    SemanticDamage,
}

fn isolation_error(code: &'static str, message: &'static str) -> Error {
    Error::new(code, message)
}

fn valid_key_part(value: &str, max_len: usize, allow_empty: bool) -> bool {
    (allow_empty || !value.is_empty())
        && value.len() <= max_len
        && !value.bytes().any(|byte| byte.is_ascii_control())
}

fn cursor_key(reader: &str, binding_id: &str, generation: i64) -> Result<String> {
    if !matches!(reader, "pending" | "result" | "execution")
        || !valid_key_part(binding_id, 256, false)
        || generation <= 0
    {
        return Err(isolation_error(
            "OPENCODE_ISOLATION_CURSOR_SCOPE_INVALID",
            "OpenCode isolation cursor scope is invalid",
        ));
    }
    let identity = format!("{binding_id}\0{generation}");
    Ok(format!(
        "{CURSOR_PREFIX}{reader}:{}",
        model::digest(identity.as_bytes())
    ))
}

fn validate_cursor(
    record: &CursorRecord,
    reader: &str,
    binding_id: &str,
    generation: i64,
) -> Result<()> {
    if record.schema_version != 1
        || record.reader != reader
        || record.binding_id != binding_id
        || record.generation != generation
        || !valid_key_part(&record.after_operation_id, 256, true)
    {
        return Err(isolation_error(
            "OPENCODE_ISOLATION_CURSOR_CORRUPT",
            "retained OpenCode isolation cursor fields are invalid",
        ));
    }
    Ok(())
}

fn read_cursor(
    db: &Connection,
    reader: &str,
    binding_id: &str,
    generation: i64,
    fallback_after: &str,
) -> Result<CursorRecord> {
    let key = cursor_key(reader, binding_id, generation)?;
    match meta(db, &key)? {
        Some(value) => {
            let record: CursorRecord = serde_json::from_value(value).map_err(|_| {
                isolation_error(
                    "OPENCODE_ISOLATION_CURSOR_CORRUPT",
                    "retained OpenCode isolation cursor is invalid",
                )
            })?;
            validate_cursor(&record, reader, binding_id, generation)?;
            Ok(record)
        }
        None => {
            if !valid_key_part(fallback_after, 256, true) {
                return Err(isolation_error(
                    "OPENCODE_ISOLATION_CURSOR_INVALID",
                    "OpenCode readback cursor is outside its bound",
                ));
            }
            Ok(CursorRecord {
                schema_version: 1,
                reader: reader.to_owned(),
                binding_id: binding_id.to_owned(),
                generation,
                after_operation_id: fallback_after.to_owned(),
            })
        }
    }
}

fn write_cursor(tx: &Transaction<'_>, record: &CursorRecord) -> Result<()> {
    validate_cursor(
        record,
        &record.reader,
        &record.binding_id,
        record.generation,
    )?;
    let key = cursor_key(&record.reader, &record.binding_id, record.generation)?;
    let value = serde_json::to_value(record)?;
    set_meta(tx, &key, &value)
}

fn quarantine_key(row: &RetainedRow) -> Result<(String, QuarantineEvidence)> {
    if !valid_key_part(&row.operation_id, 256, false) {
        return Err(isolation_error(
            "OPENCODE_ISOLATION_SUBJECT_INVALID",
            "retained OpenCode operation identity is invalid",
        ));
    }
    let evidence = QuarantineEvidence {
        subject_identity: format!("operation_id:{}", row.operation_id),
        source_pointer: Some("operations.original_request_json".to_owned()),
        source_digest: Some(model::digest(row.original_request_json.as_bytes())),
    };
    let key = automation_reconcile::quarantine_record_key(QUARANTINE_PREFIX, &evidence)?;
    Ok((key, evidence))
}

fn validate_quarantine(record: &QuarantinedSubject, evidence: &QuarantineEvidence) -> Result<()> {
    if record.code != SEMANTIC_COMMAND_ERROR
        || &record.evidence != evidence
        || record.first_seen_at_ms < 0
        || record.last_seen_at_ms < record.first_seen_at_ms
        || record.occurrences == 0
    {
        return Err(isolation_error(
            "OPENCODE_ISOLATION_QUARANTINE_CORRUPT",
            "retained OpenCode quarantine evidence is invalid",
        ));
    }
    Ok(())
}

fn read_quarantine(tx: &Transaction<'_>, row: &RetainedRow) -> Result<bool> {
    let (key, evidence) = quarantine_key(row)?;
    match config::read_record(tx, &key, "OpenCode row quarantine")? {
        Some(value) => {
            let record: QuarantinedSubject = serde_json::from_value(value).map_err(|_| {
                isolation_error(
                    "OPENCODE_ISOLATION_QUARANTINE_CORRUPT",
                    "retained OpenCode quarantine evidence is invalid",
                )
            })?;
            validate_quarantine(&record, &evidence)?;
            Ok(true)
        }
        None => Ok(false),
    }
}

fn persist_quarantine(tx: &Transaction<'_>, row: &RetainedRow) -> Result<()> {
    let (key, evidence) = quarantine_key(row)?;
    let now_ms = model::now_ms()?;
    automation_reconcile::persist_quarantine(tx, &key, SEMANTIC_COMMAND_ERROR, evidence, now_ms)?;
    Ok(())
}

fn decode_command(
    db: &Connection,
    principal: &Principal,
    row: &RetainedRow,
    config: &crate::config::Config,
) -> Result<DecodedRow> {
    match super::original_with_config(db, principal, &row.operation_id, config) {
        Ok(command) => Ok(DecodedRow::Ready(Box::new(command))),
        Err(error) if error.code == SEMANTIC_COMMAND_ERROR => Ok(DecodedRow::SemanticDamage),
        Err(error) => Err(error),
    }
}

fn pending_page(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    after: &str,
) -> Result<Vec<RetainedRow>> {
    let mut statement = db.prepare(
        "SELECT operation_id,original_request_json FROM operations
         WHERE binding_id=?1 AND binding_generation=?2
           AND state IN ('sending','native_accepted','outcome_unknown')
           AND method IN ('agent.open','task.dispatch','agent.send','agent.configure','agent.goal','agent.background')
           AND operation_id>?3
         ORDER BY operation_id LIMIT 16",
    )?;
    let rows = statement.query_map(params![binding_id, generation, after], |row| {
        Ok(RetainedRow {
            operation_id: row.get(0)?,
            original_request_json: row.get(1)?,
        })
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?)
}

pub(super) fn pending_commands(
    db: &Connection,
    principal: &Principal,
    config: &crate::config::Config,
) -> Result<Vec<RuntimeCommand>> {
    let tx = db.unchecked_transaction()?;
    let (binding_id, generation, _) = super::runtime::scope(&tx, principal, true)?;
    let mut cursor = read_cursor(&tx, "pending", &binding_id, generation, "")?;
    let mut rows = pending_page(&tx, &binding_id, generation, &cursor.after_operation_id)?;
    if rows.is_empty() && !cursor.after_operation_id.is_empty() {
        cursor.after_operation_id.clear();
        write_cursor(&tx, &cursor)?;
        rows = pending_page(&tx, &binding_id, generation, "")?;
    }

    let mut commands = Vec::with_capacity(rows.len());
    for row in rows {
        if !read_quarantine(&tx, &row)? {
            match decode_command(&tx, principal, &row, config)? {
                DecodedRow::Ready(command) => commands.push(*command),
                DecodedRow::SemanticDamage => persist_quarantine(&tx, &row)?,
            }
        }
        cursor.after_operation_id = row.operation_id;
        write_cursor(&tx, &cursor)?;
    }
    tx.commit()?;
    Ok(commands)
}

fn result_row(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    after: &str,
) -> Result<Option<RetainedRow>> {
    Ok(db
        .query_row(
            "SELECT operation_id,original_request_json FROM operations
             WHERE binding_id=?1 AND binding_generation=?2 AND method='agent.result'
               AND state IN ('sending','native_accepted','outcome_unknown')
               AND operation_id>?3
             ORDER BY operation_id LIMIT 1",
            params![binding_id, generation, after],
            |row| {
                Ok(RetainedRow {
                    operation_id: row.get(0)?,
                    original_request_json: row.get(1)?,
                })
            },
        )
        .optional()?)
}

pub(super) fn next_result_read(
    db: &Connection,
    principal: &Principal,
    fallback_after: &str,
    config: &crate::config::Config,
) -> Result<Option<RuntimeCommand>> {
    let tx = db.unchecked_transaction()?;
    let (binding_id, generation, _) = super::runtime::scope(&tx, principal, true)?;
    let mut cursor = read_cursor(&tx, "result", &binding_id, generation, fallback_after)?;
    let mut wrapped = false;

    for _ in 0..MAX_ROW_SCAN {
        let row = match result_row(&tx, &binding_id, generation, &cursor.after_operation_id)? {
            Some(row) => row,
            None if !cursor.after_operation_id.is_empty() && !wrapped => {
                cursor.after_operation_id.clear();
                write_cursor(&tx, &cursor)?;
                wrapped = true;
                continue;
            }
            None => {
                tx.commit()?;
                return Ok(None);
            }
        };

        if read_quarantine(&tx, &row)? {
            cursor.after_operation_id = row.operation_id;
            write_cursor(&tx, &cursor)?;
            continue;
        }
        match decode_command(&tx, principal, &row, config)? {
            DecodedRow::Ready(command) => {
                cursor.after_operation_id = row.operation_id;
                write_cursor(&tx, &cursor)?;
                tx.commit()?;
                return Ok(Some(*command));
            }
            DecodedRow::SemanticDamage => {
                persist_quarantine(&tx, &row)?;
                cursor.after_operation_id = row.operation_id;
                write_cursor(&tx, &cursor)?;
            }
        }
    }
    tx.commit()?;
    Ok(None)
}

fn execution_row(
    db: &Connection,
    binding_id: &str,
    generation: i64,
    after: &str,
) -> Result<Option<RetainedRow>> {
    Ok(db
        .query_row(
            "SELECT operation_id,original_request_json FROM operations
             WHERE binding_id=?1 AND binding_generation=?2
               AND method IN ('task.dispatch','agent.send','agent.goal')
               AND state IN ('sending','native_accepted','outcome_unknown','settled')
               AND COALESCE(json_extract(native_refs_json,'$.input_execution.disposition'),'')
                   NOT IN ('completed','failed','cancelled')
               AND operation_id>?3
             ORDER BY operation_id LIMIT 1",
            params![binding_id, generation, after],
            |row| {
                Ok(RetainedRow {
                    operation_id: row.get(0)?,
                    original_request_json: row.get(1)?,
                })
            },
        )
        .optional()?)
}

pub(super) fn next_execution_read(
    db: &Connection,
    principal: &Principal,
    fallback_after: &str,
    config: &crate::config::Config,
) -> Result<Option<(RuntimeCommand, Option<Value>)>> {
    let tx = db.unchecked_transaction()?;
    let (binding_id, generation, _) = super::runtime::scope(&tx, principal, true)?;
    let mut cursor = read_cursor(&tx, "execution", &binding_id, generation, fallback_after)?;
    let mut wrapped = false;

    for _ in 0..MAX_ROW_SCAN {
        let row = match execution_row(&tx, &binding_id, generation, &cursor.after_operation_id)? {
            Some(row) => row,
            None if !cursor.after_operation_id.is_empty() && !wrapped => {
                cursor.after_operation_id.clear();
                write_cursor(&tx, &cursor)?;
                wrapped = true;
                continue;
            }
            None => {
                tx.commit()?;
                return Ok(None);
            }
        };

        if read_quarantine(&tx, &row)? {
            cursor.after_operation_id = row.operation_id;
            write_cursor(&tx, &cursor)?;
            continue;
        }
        match decode_command(&tx, principal, &row, config)? {
            DecodedRow::Ready(command) => {
                if command.method == "agent.goal" && command.input["action"] != "continue" {
                    cursor.after_operation_id = row.operation_id;
                    write_cursor(&tx, &cursor)?;
                    continue;
                }
                let operation = super::operations::get_operation(&tx, &row.operation_id)?;
                let saved = operation["native_refs"].get("execution_scan").cloned();
                cursor.after_operation_id = row.operation_id;
                write_cursor(&tx, &cursor)?;
                tx.commit()?;
                return Ok(Some((*command, saved)));
            }
            DecodedRow::SemanticDamage => {
                persist_quarantine(&tx, &row)?;
                cursor.after_operation_id = row.operation_id;
                write_cursor(&tx, &cursor)?;
            }
        }
    }
    tx.commit()?;
    Ok(None)
}
