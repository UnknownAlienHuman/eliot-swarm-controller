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

#[cfg(test)]
mod tests {
    use super::super::{SCHEMA, meta, set_meta};
    use super::CANCEL_EVENT_MARKER;
    use rusqlite::{Connection, TransactionBehavior};
    use serde_json::json;

    fn empty_database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(SCHEMA).unwrap();
        db
    }

    fn install_in_transaction(db: &mut Connection) -> crate::error::Result<()> {
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        super::install(&tx)?;
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn changed_marker_is_rejected_by_production_installer() {
        let mut db = empty_database();
        {
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            set_meta(
                &tx,
                CANCEL_EVENT_MARKER,
                &json!("deliberately-not-the-schema-digest"),
            )
            .unwrap();
            tx.commit().unwrap();
        }

        let error = install_in_transaction(&mut db).unwrap_err();
        assert_eq!(error.code, "SCHEMA_MISMATCH");
    }

    #[test]
    fn changed_registered_cancellation_trigger_definition_is_rejected_by_production_installer() {
        let mut db = empty_database();
        install_in_transaction(&mut db).unwrap();
        db.execute_batch(
            "DROP TRIGGER operation_cancel_event_after_insert; CREATE TRIGGER operation_cancel_event_after_insert AFTER INSERT ON operations BEGIN SELECT 1; END;",
        )
        .unwrap();

        let error = install_in_transaction(&mut db).unwrap_err();
        assert_eq!(error.code, "SCHEMA_MISMATCH");
    }

    #[test]
    fn unregistered_cancellation_trigger_is_not_adopted() {
        let mut db = empty_database();
        db.execute_batch(
            "CREATE TRIGGER operation_cancel_event_after_insert AFTER INSERT ON operations BEGIN SELECT 1; END;",
        )
        .unwrap();

        let error = install_in_transaction(&mut db).unwrap_err();
        assert_eq!(error.code, "SCHEMA_MISMATCH");
        assert_eq!(meta(&db, CANCEL_EVENT_MARKER).unwrap(), None);
    }

    #[test]
    fn missing_required_core_table_is_rejected_without_marker_or_trigger_adoption() {
        let mut db = empty_database();
        db.execute_batch("DROP TABLE observations;").unwrap();

        let operations_present: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
                ["operations"],
                |row| row.get(0),
            )
            .unwrap();
        let observations_present: bool = db
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name=?1)",
                ["observations"],
                |row| row.get(0),
            )
            .unwrap();
        assert!(operations_present);
        assert!(!observations_present);
        assert_eq!(meta(&db, CANCEL_EVENT_MARKER).unwrap(), None);

        let initial_trigger_count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND name IN (?1, ?2)",
                super::TRIGGER_NAMES,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(initial_trigger_count, 0);

        let error = install_in_transaction(&mut db).unwrap_err();
        assert_eq!(error.code, "SCHEMA_MISMATCH");
        assert_eq!(error.message, "required observations table is missing");
        assert_eq!(meta(&db, CANCEL_EVENT_MARKER).unwrap(), None);

        let final_trigger_count: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM sqlite_schema WHERE type='trigger' AND name IN (?1, ?2)",
                super::TRIGGER_NAMES,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(final_trigger_count, initial_trigger_count);
    }
}
