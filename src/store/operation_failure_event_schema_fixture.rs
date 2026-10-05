//! Startup-attestation fixture for the Operation failure-event extension.

#[cfg(test)]
mod tests {
    use super::super::*;
    use rusqlite::{Connection, TransactionBehavior};
    use serde_json::json;
    use std::{env, fs, path::PathBuf};

    const FAILURE_EVENT_SCHEMA: &str =
        include_str!("../../migrations/010_operation_failure_events.sql");
    const FAILURE_EVENT_MARKER: &str = "schema_extension:operations:failure_events:v1";

    fn empty_database() -> Connection {
        let db = Connection::open_in_memory().unwrap();
        db.execute_batch(SCHEMA).unwrap();
        db
    }

    fn install(db: &mut Connection) -> crate::error::Result<()> {
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        operation_failure_event_schema::install(&tx)?;
        tx.commit()?;
        Ok(())
    }

    fn trigger_count(db: &Connection) -> i64 {
        db.query_row(
            "SELECT count(*) FROM sqlite_schema WHERE type='trigger' \
             AND name IN ('operation_failure_event_after_insert', \
                          'operation_failure_event_after_state_update')",
            [],
            |row| row.get(0),
        )
        .unwrap()
    }

    fn database_path() -> PathBuf {
        env::temp_dir().join(format!(
            "eliot-operation-failure-events-{}.sqlite",
            crate::model::new_id()
        ))
    }

    #[test]
    fn reopened_store_revalidates_marker_and_both_live_triggers() {
        let path = database_path();
        {
            let mut db = Connection::open(&path).unwrap();
            db.execute_batch(SCHEMA).unwrap();
            install(&mut db).unwrap();
            assert_eq!(trigger_count(&db), 2);
        }

        {
            let mut db = Connection::open(&path).unwrap();
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            operation_failure_event_schema::install(&tx).unwrap();
            tx.commit().unwrap();
            assert_eq!(trigger_count(&db), 2);
            assert_eq!(
                meta(&db, FAILURE_EVENT_MARKER).unwrap(),
                Some(json!(crate::model::digest(FAILURE_EVENT_SCHEMA.as_bytes())))
            );
        }
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn missing_or_changed_registered_trigger_is_rejected() {
        let mut missing = empty_database();
        install(&mut missing).unwrap();
        missing
            .execute_batch("DROP TRIGGER operation_failure_event_after_insert")
            .unwrap();
        let error = install(&mut missing).unwrap_err();
        assert_eq!(error.code, "SCHEMA_MISMATCH");

        let mut changed = empty_database();
        install(&mut changed).unwrap();
        changed
            .execute_batch(
                "DROP TRIGGER operation_failure_event_after_insert; \
                 CREATE TRIGGER operation_failure_event_after_insert \
                 AFTER INSERT ON operations BEGIN SELECT 1; END;",
            )
            .unwrap();
        let error = install(&mut changed).unwrap_err();
        assert_eq!(error.code, "SCHEMA_MISMATCH");
    }

    #[test]
    fn unregistered_trigger_name_is_not_adopted() {
        let mut db = empty_database();
        db.execute_batch(
            "CREATE TRIGGER operation_failure_event_after_insert \
             AFTER INSERT ON operations BEGIN SELECT 1; END;",
        )
        .unwrap();
        let error = install(&mut db).unwrap_err();
        assert_eq!(error.code, "SCHEMA_MISMATCH");
        assert_eq!(meta(&db, FAILURE_EVENT_MARKER).unwrap(), None);
    }

    #[test]
    fn migration_triggers_and_marker_rollback_together() {
        let mut db = empty_database();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        operation_failure_event_schema::install(&tx).unwrap();
        assert_eq!(trigger_count(&tx), 2);
        tx.rollback().unwrap();

        assert_eq!(trigger_count(&db), 0);
        assert_eq!(meta(&db, FAILURE_EVENT_MARKER).unwrap(), None);
    }
}
