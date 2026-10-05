//! Immutable installer and live-DDL verifier for committed cancellation
//! Operation events.
//!
//! This is a forward extension beside the failure-event migration. Keeping a
//! new marker and trigger names preserves already-installed failure-event DDL;
//! an existing Store can adopt the cancellation adapter atomically at startup.

use super::{meta, set_meta};
use crate::{
    error::{Error, Result},
    model,
};
use rusqlite::{Connection, Transaction};
use serde_json::json;

const CANCEL_EVENT_SCHEMA: &str = include_str!("../../migrations/011_operation_cancel_events.sql");
const CANCEL_EVENT_MARKER: &str = "schema_extension:operations:cancel_events:v1";
const TRIGGER_NAMES: [&str; 2] = [
    "operation_cancel_event_after_insert",
    "operation_cancel_event_after_state_update",
];

type TriggerShape = (String, String, Option<String>);

/// Install or verify the additive triggers inside the caller's startup tx.
pub(super) fn install(tx: &Transaction<'_>) -> Result<()> {
    let expected_digest = json!(model::digest(CANCEL_EVENT_SCHEMA.as_bytes()));
    match meta(tx, CANCEL_EVENT_MARKER)? {
        Some(actual) if actual == expected_digest => verify_shape(tx),
        Some(_) => Err(schema_mismatch(
            "Operation cancellation-event migration content differs".into(),
        )),
        None => {
            ensure_core_tables(tx)?;
            ensure_triggers_absent(tx)?;
            tx.execute_batch(CANCEL_EVENT_SCHEMA)?;
            verify_shape(tx)?;
            set_meta(tx, CANCEL_EVENT_MARKER, &expected_digest)
        }
    }
}

fn ensure_core_tables(db: &Connection) -> Result<()> {
    for table in ["operations", "observations"] {
        let present: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
            [table],
            |row| row.get(0),
        )?;
        if !present {
            return Err(schema_mismatch(format!(
                "required {table} table is missing"
            )));
        }
    }
    Ok(())
}

fn ensure_triggers_absent(db: &Connection) -> Result<()> {
    let present: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='trigger' \
         AND name IN (?1, ?2))",
        TRIGGER_NAMES,
        |row| row.get(0),
    )?;
    if present {
        return Err(schema_mismatch(
            "unregistered Operation cancellation-event trigger exists".into(),
        ));
    }
    Ok(())
}

fn verify_shape(db: &Connection) -> Result<()> {
    ensure_core_tables(db)?;
    let expected = reference_triggers()?;
    let actual = trigger_shapes(db)?;
    if actual != expected {
        return Err(schema_mismatch(
            "Operation cancellation-event trigger definitions differ".into(),
        ));
    }
    Ok(())
}

/// Build expected trigger rows using the running Store's SQLite engine.
fn reference_triggers() -> Result<Vec<TriggerShape>> {
    let db = Connection::open_in_memory()?;
    db.execute_batch(super::SCHEMA)?;
    db.execute_batch(CANCEL_EVENT_SCHEMA)?;
    trigger_shapes(&db)
}

fn trigger_shapes(db: &Connection) -> Result<Vec<TriggerShape>> {
    let mut statement = db.prepare(
        "SELECT name,tbl_name,sql FROM sqlite_schema \
         WHERE type='trigger' AND name IN (?1, ?2) ORDER BY name",
    )?;
    let rows = statement.query_map(TRIGGER_NAMES, |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })?;
    Ok(rows.collect::<rusqlite::Result<Vec<TriggerShape>>>()?)
}

fn schema_mismatch(message: String) -> Error {
    Error::new("SCHEMA_MISMATCH", message)
}
