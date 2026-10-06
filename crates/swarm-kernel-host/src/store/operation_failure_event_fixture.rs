//! Focused fixture for committed Operation failure-event trigger behavior.
//!
//! This source belongs beside `operation_failure_event_schema.rs` and is
//! declared from `src/store/mod.rs` with `#[cfg(test)]` when integrated.

#[cfg(test)]
mod tests {
    use super::super::*;
    use rusqlite::{Connection, TransactionBehavior, params};
    use serde_json::{Value, json};

    fn database() -> Connection {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(SCHEMA).unwrap();
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        operation_failure_event_schema::install(&tx).unwrap();
        tx.commit().unwrap();
        db
    }

    fn insert_queued_operation(db: &Connection, operation_id: &str) {
        db.execute(
            "INSERT INTO operations(\
                operation_id,caller_id,client_request_id,method,\
                original_request_json,effective_request_json,state,\
                due_at_ms,created_at_ms,updated_at_ms\
             ) VALUES(?1,'fixture-caller',?2,'fixture.method','{}','{}','queued',1,1,1)",
            params![operation_id, format!("request-{operation_id}")],
        )
        .unwrap();
    }

    fn insert_rejected_operation(db: &Connection, operation_id: &str) {
        db.execute(
            "INSERT INTO operations(\
                operation_id,caller_id,client_request_id,method,\
                original_request_json,effective_request_json,state,result_json,\
                due_at_ms,settled_at_ms,created_at_ms,updated_at_ms\
             ) VALUES(?1,'fixture-caller',?2,'fixture.method','{}','{}',\
                'rejected','{}',1,2,1,2)",
            params![operation_id, format!("request-{operation_id}")],
        )
        .unwrap();
    }

    fn event_rows(db: &Connection, operation_id: &str) -> Vec<(String, String, String, i64)> {
        let mut statement = db
            .prepare(
                "SELECT source_event_key,kind,payload_json,recorded_at_ms \
                 FROM observations WHERE source_stream_id='controller:operations' \
                 AND operation_id=?1 ORDER BY source_event_key",
            )
            .unwrap();
        statement
            .query_map([operation_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    #[test]
    fn committed_failure_states_emit_closed_facts_and_helper_replay_dedupes() {
        let mut db = database();
        insert_queued_operation(&db, "fixture-rejected");
        {
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            tx.execute(
                "UPDATE operations SET state='rejected',result_json='{}',\
                 settled_at_ms=2,updated_at_ms=2 WHERE operation_id='fixture-rejected'",
                [],
            )
            .unwrap();
            // Existing callers remain source-compatible; their event aliases
            // collapse onto the trigger's immutable key inside this tx.
            record_operation_failure_event(&tx, "fixture-rejected", "rejected", 3).unwrap();
            tx.commit().unwrap();
        }

        let rejected = event_rows(&db, "fixture-rejected");
        assert_eq!(rejected.len(), 1);
        assert_eq!(
            rejected[0].0,
            "operation:fixture-rejected:operation_rejected"
        );
        assert_eq!(rejected[0].1, "operation.rejected");
        assert!(rejected[0].3 > 2);
        let rejected_payload: Value = serde_json::from_str(&rejected[0].2).unwrap();
        assert_eq!(
            rejected_payload,
            json!({
                "schema_version": 1,
                "phase": "operation_rejected",
                "status": "rejected",
                "occurrence_id": "operation:fixture-rejected:operation_rejected",
                "error_code": "OPERATION_REJECTED"
            })
        );

        insert_queued_operation(&db, "fixture-unknown");
        {
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            tx.execute(
                "UPDATE operations SET state='outcome_unknown',updated_at_ms=4 \
                 WHERE operation_id='fixture-unknown'",
                [],
            )
            .unwrap();
            tx.commit().unwrap();
        }
        let unknown = event_rows(&db, "fixture-unknown");
        assert_eq!(unknown.len(), 1);
        assert_eq!(
            unknown[0].0,
            "operation:fixture-unknown:operation_outcome_unknown"
        );
        assert_eq!(unknown[0].1, "operation.outcome_unknown");
        let unknown_payload: Value = serde_json::from_str(&unknown[0].2).unwrap();
        assert_eq!(unknown_payload["status"], "unknown");
        assert_eq!(unknown_payload["error_code"], "OUTCOME_UNKNOWN");
        assert!(unknown[0].3 > 4);

        insert_rejected_operation(&db, "fixture-inserted-rejected");
        let inserted = event_rows(&db, "fixture-inserted-rejected");
        assert_eq!(inserted.len(), 1);
        assert_eq!(
            inserted[0].0,
            "operation:fixture-inserted-rejected:operation_rejected"
        );

        insert_queued_operation(&db, "fixture-settled");
        db.execute(
            "UPDATE operations SET state='settled',result_json='{}',\
             settled_at_ms=5,updated_at_ms=5 WHERE operation_id='fixture-settled'",
            [],
        )
        .unwrap();
        assert!(event_rows(&db, "fixture-settled").is_empty());
    }

    #[test]
    fn trigger_facts_rollback_with_the_operation_transition() {
        let mut db = database();
        insert_queued_operation(&db, "fixture-rollback");
        {
            let tx = db
                .transaction_with_behavior(TransactionBehavior::Immediate)
                .unwrap();
            tx.execute(
                "UPDATE operations SET state='outcome_unknown',updated_at_ms=6 \
                 WHERE operation_id='fixture-rollback'",
                [],
            )
            .unwrap();
            tx.rollback().unwrap();
        }
        let state: String = db
            .query_row(
                "SELECT state FROM operations WHERE operation_id='fixture-rollback'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(state, "queued");
        assert!(event_rows(&db, "fixture-rollback").is_empty());
    }
}
