//! Focused source fixture for O6 event-only ScriptRun schema installation.
//!
//! Graft this file as `src/store/script_event_schema_fixture.rs` and declare
//! it from `src/store/mod.rs` with `#[cfg(test)]`. It exercises the Store
//! transaction sequence and the schema guards without starting a Store,
//! runner, interpreter, or product migration in this private overlay.

#[cfg(test)]
mod tests {
    use super::super::*;
    use crate::model;
    use rusqlite::{
        Connection, OptionalExtension, TransactionBehavior, params, types::Value as SqlValue,
    };

    const SCRIPT_MARKER: &str = "schema_extension:scripts:v1";
    const EVENT_SCOPE_MARKER: &str = "schema_extension:scripts:event_scope:v1";
    const SCRIPT_ID: &str = "event-scope-fixture-script";
    const TASK_ID: &str = "event-scope-fixture-task";
    const ATTEMPT_ID: &str = "event-scope-fixture-attempt";
    const LEGACY_RUN_ID: &str = "event-scope-fixture-legacy-run";
    const SCRIPT_RUN_COLUMNS: [&str; 19] = [
        "run_id",
        "operation_id",
        "script_id",
        "revision",
        "bundle_ref",
        "task_id",
        "task_revision",
        "attempt_id",
        "work_digest",
        "spec_json",
        "state",
        "process_identity_json",
        "result_ref",
        "stdout_ref",
        "stderr_ref",
        "exit_code",
        "started_at_ms",
        "finished_at_ms",
        "created_at_ms",
    ];
    const SCRIPT_RUN_SELECT: &str = "SELECT run_id,operation_id,script_id,revision,bundle_ref,task_id,task_revision,attempt_id,work_digest,spec_json,state,process_identity_json,result_ref,stdout_ref,stderr_ref,exit_code,started_at_ms,finished_at_ms,created_at_ms FROM script_runs ORDER BY run_id";
    const SCRIPT_RUN_INDEX_DDL: &str = "CREATE INDEX script_runs_pending ON script_runs(state, created_at_ms, run_id) WHERE state IN ('queued', 'sending', 'running', 'reconciling', 'outcome_unknown'); CREATE INDEX script_runs_task ON script_runs(task_id, attempt_id, created_at_ms, run_id);";

    type ColumnShape = (i64, String, String, i64, Option<String>, i64, i64);
    type ForeignKeyShape = (
        i64,
        i64,
        String,
        Option<String>,
        Option<String>,
        String,
        String,
        String,
    );
    type IndexColumnShape = (i64, i64, Option<String>, i64, String, i64);
    type IndexShape = (
        String,
        i64,
        String,
        i64,
        Vec<IndexColumnShape>,
        Option<String>,
    );

    fn legacy_database() -> Connection {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        db.execute_batch(SCHEMA).unwrap();
        install_scripts_v1(&mut db);
        seed_script_scope(&db);
        db
    }

    fn install_scripts_v1(db: &mut Connection) {
        let tx = db
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .unwrap();
        install_schema_extension(
            &tx,
            SCRIPT_MARKER,
            SCRIPT_SCHEMA,
            &["scripts", "script_revisions", "script_runs"],
        )
        .unwrap();
        tx.commit().unwrap();
    }

    fn run_current_installer(db: &mut Connection) -> crate::error::Result<()> {
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        install_schema_extension(
            &tx,
            SCRIPT_MARKER,
            SCRIPT_SCHEMA,
            &["scripts", "script_revisions", "script_runs"],
        )?;
        super::super::script_event_schema::install(&tx)?;
        Ok(tx.commit()?)
    }

    fn seed_script_scope(db: &Connection) {
        let digest = "a".repeat(64);
        db.execute(
            "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) VALUES('event-scope-fixture-bundle','artifacts/event-scope-fixture-bundle.bin','script_bundle',1,?1,1,'{}')",
            [digest.as_str()],
        )
        .unwrap();
        db.execute(
            "INSERT INTO scripts(script_id,owner_id,active_revision,created_at_ms,updated_at_ms) VALUES(?1,'event-scope-manager',NULL,1,1)",
            [SCRIPT_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO script_revisions(script_id,revision,bundle_ref,bundle_sha256,interpreter_json,validated_at_ms,created_by,created_at_ms) VALUES(?1,1,'event-scope-fixture-bundle',?2,'{}',1,'event-scope-manager',1)",
            params![SCRIPT_ID, digest],
        )
        .unwrap();
        db.execute(
            "UPDATE scripts SET active_revision=1 WHERE script_id=?1",
            [SCRIPT_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,'event-scope-project',1,'open','{}',1,1)",
            [TASK_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,state,producers_json,created_at_ms,updated_at_ms) VALUES(?1,?2,1,'{}','event-scope-owner','controller','running','[]',1,1)",
            params![ATTEMPT_ID, TASK_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,state,due_at_ms,created_at_ms,updated_at_ms) VALUES('event-scope-fixture-legacy-op','event-scope-manager','event-scope-legacy-request','script.run','{}','{}',?1,?2,'queued',1,1,1)",
            params![TASK_ID, ATTEMPT_ID],
        )
        .unwrap();
        db.execute(
            "INSERT INTO script_runs(run_id,operation_id,script_id,revision,bundle_ref,task_id,task_revision,attempt_id,work_digest,spec_json,state,created_at_ms) VALUES(?1,'event-scope-fixture-legacy-op',?2,1,'event-scope-fixture-bundle',?3,1,?4,?5,'{}','queued',1)",
            params![LEGACY_RUN_ID, SCRIPT_ID, TASK_ID, ATTEMPT_ID, "b".repeat(64)],
        )
        .unwrap();
    }

    fn script_run_rows(db: &Connection) -> Vec<Vec<SqlValue>> {
        let mut statement = db.prepare(SCRIPT_RUN_SELECT).unwrap();
        statement
            .query_map([], |row| {
                (0..SCRIPT_RUN_COLUMNS.len())
                    .map(|index| row.get(index))
                    .collect::<rusqlite::Result<Vec<SqlValue>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    fn column_shapes(db: &Connection) -> Vec<ColumnShape> {
        let mut statement = db.prepare("PRAGMA table_xinfo('script_runs')").unwrap();
        statement
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap()
    }

    fn foreign_key_shapes(db: &Connection) -> Vec<ForeignKeyShape> {
        let mut statement = db
            .prepare("PRAGMA foreign_key_list('script_runs')")
            .unwrap();
        let mut rows = statement
            .query_map([], |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        rows.sort();
        rows
    }

    fn index_shapes(db: &Connection) -> Vec<IndexShape> {
        let mut statement = db.prepare("PRAGMA index_list('script_runs')").unwrap();
        let indexes = statement
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, i64>(4)?,
                ))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        let mut result = indexes
            .into_iter()
            .map(|(name, unique, origin, partial)| {
                let mut statement = db
                    .prepare(&format!(
                        "PRAGMA index_xinfo(\"{}\")",
                        name.replace('"', "\"\"")
                    ))
                    .unwrap();
                let columns = statement
                    .query_map([], |row| {
                        Ok((
                            row.get(0)?,
                            row.get(1)?,
                            row.get(2)?,
                            row.get(3)?,
                            row.get(4)?,
                            row.get(5)?,
                        ))
                    })
                    .unwrap()
                    .collect::<rusqlite::Result<Vec<IndexColumnShape>>>()
                    .unwrap();
                let sql = db
                    .query_row(
                        "SELECT sql FROM sqlite_schema WHERE type='index' AND name=?1",
                        [name.as_str()],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .optional()
                    .unwrap()
                    .flatten();
                (name, unique, origin, partial, columns, sql)
            })
            .collect::<Vec<_>>();
        result.sort_by(|left, right| left.0.cmp(&right.0));
        result
    }

    fn replace_script_runs_definition(db: &Connection, old: &str, new: &str) {
        let current: String = db
            .query_row(
                "SELECT sql FROM sqlite_schema WHERE type='table' AND name='script_runs'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let changed = current.replacen(old, new, 1);
        assert_ne!(current, changed, "fixture DDL anchor must exist");
        db.execute("DELETE FROM script_runs", []).unwrap();
        db.execute_batch("DROP TABLE script_runs;").unwrap();
        db.execute_batch(&changed).unwrap();
        db.execute_batch(SCRIPT_RUN_INDEX_DDL).unwrap();
    }

    #[test]
    fn event_scope_preserves_all_legacy_run_data_and_schema_links() {
        let mut db = legacy_database();
        let rows_before = script_run_rows(&db);
        let columns_before = column_shapes(&db);
        let foreign_keys_before = foreign_key_shapes(&db);
        let indexes_before = index_shapes(&db);

        run_current_installer(&mut db).unwrap();

        assert_eq!(columns_before.len(), SCRIPT_RUN_COLUMNS.len());
        assert_eq!(
            columns_before
                .iter()
                .map(|column| column.1.as_str())
                .collect::<Vec<_>>(),
            SCRIPT_RUN_COLUMNS
        );
        assert_eq!(script_run_rows(&db), rows_before);
        assert_eq!(
            column_shapes(&db)
                .iter()
                .map(|column| column.1.as_str())
                .collect::<Vec<_>>(),
            SCRIPT_RUN_COLUMNS
        );
        assert_eq!(foreign_key_shapes(&db), foreign_keys_before);
        assert_eq!(index_shapes(&db), indexes_before);

        let legacy: (Option<String>, Option<i64>, Option<String>) = db
            .query_row(
                "SELECT task_id,task_revision,attempt_id FROM script_runs WHERE run_id=?1",
                [LEGACY_RUN_ID],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(legacy.0.as_deref(), Some(TASK_ID));
        assert_eq!(legacy.1, Some(1));
        assert_eq!(legacy.2.as_deref(), Some(ATTEMPT_ID));

        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES('event-scope-fixture-event-op','event-scope-manager','event-scope-event-request','script.run','{}','{}','queued',1,1,1)",
            [],
        )
        .unwrap();
        db.execute(
            "INSERT INTO script_runs(run_id,operation_id,script_id,revision,bundle_ref,work_digest,spec_json,state,created_at_ms) VALUES('event-scope-fixture-event-run','event-scope-fixture-event-op',?1,1,'event-scope-fixture-bundle',?2,'{}','queued',1)",
            params![SCRIPT_ID, "c".repeat(64)],
        )
        .unwrap();

        db.execute(
            "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) VALUES('event-scope-fixture-partial-op','event-scope-manager','event-scope-partial-request','script.run','{}','{}','queued',1,1,1)",
            [],
        )
        .unwrap();
        let partial = db.execute(
            "INSERT INTO script_runs(run_id,operation_id,script_id,revision,bundle_ref,task_id,work_digest,spec_json,state,created_at_ms) VALUES('event-scope-fixture-partial-run','event-scope-fixture-partial-op',?1,1,'event-scope-fixture-bundle',?2,?3,'{}','queued',1)",
            params![SCRIPT_ID, TASK_ID, "d".repeat(64)],
        );
        assert!(
            partial.is_err(),
            "partial Task/Attempt scope must fail closed"
        );
    }

    #[test]
    fn current_installer_is_idempotent_and_rejects_event_marker_or_index_tampering() {
        let mut db = legacy_database();
        run_current_installer(&mut db).unwrap();
        let marker_before: String = db
            .query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [EVENT_SCOPE_MARKER],
                |row| row.get(0),
            )
            .unwrap();
        let count_before: i64 = db
            .query_row("SELECT COUNT(*) FROM script_runs", [], |row| row.get(0))
            .unwrap();

        // This repeats open_database's installer sequence in a fresh
        // transaction; it is not a process or Store reopen.
        run_current_installer(&mut db).unwrap();
        let count_after: i64 = db
            .query_row("SELECT COUNT(*) FROM script_runs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            marker_before,
            db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [EVENT_SCOPE_MARKER],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
        );
        assert_eq!(count_before, count_after);

        db.execute(
            "UPDATE meta SET value_json='\"tampered\"' WHERE key=?1",
            [EVENT_SCOPE_MARKER],
        )
        .unwrap();
        assert!(run_current_installer(&mut db).is_err());

        db.execute(
            "UPDATE meta SET value_json=?1 WHERE key=?2",
            params![marker_before, EVENT_SCOPE_MARKER],
        )
        .unwrap();
        db.execute("DROP INDEX script_runs_task", []).unwrap();
        assert!(run_current_installer(&mut db).is_err());
    }

    #[test]
    fn exact_ddl_guard_rejects_extra_check_default_collation_generated_column_and_table_option() {
        let cases = [
            (
                "an extra CHECK",
                "task_revision > 0",
                "task_revision > 0 AND task_revision < 100",
            ),
            (
                "a default",
                "exit_code INTEGER",
                "exit_code INTEGER DEFAULT 0",
            ),
            (
                "a collation",
                "script_id TEXT NOT NULL",
                "script_id TEXT COLLATE NOCASE NOT NULL",
            ),
            (
                "a generated column",
                "created_at_ms INTEGER NOT NULL,",
                "created_at_ms INTEGER NOT NULL, schema_guard INTEGER GENERATED ALWAYS AS (1) VIRTUAL,",
            ),
            (
                "an extra table option",
                ") STRICT",
                ") STRICT, WITHOUT ROWID",
            ),
        ];

        for (description, old, new) in cases {
            let mut db = legacy_database();
            run_current_installer(&mut db).unwrap();
            replace_script_runs_definition(&db, old, new);
            assert!(
                run_current_installer(&mut db).is_err(),
                "installer must reject {description} even with the expected marker"
            );
        }
    }

    #[test]
    fn current_installer_revalidates_live_004_tables_after_the_marker_exists() {
        for table in ["scripts", "script_revisions"] {
            let mut db = legacy_database();
            run_current_installer(&mut db).unwrap();
            db.execute_batch(&format!("ALTER TABLE {table} ADD COLUMN schema_guard TEXT"))
                .unwrap();
            assert!(
                run_current_installer(&mut db).is_err(),
                "installer must revalidate live 004 table {table}"
            );
        }
    }

    #[test]
    fn fixture_keeps_script_scope_migration_marker_unique() {
        let mut db = legacy_database();
        run_current_installer(&mut db).unwrap();
        let markers: i64 = db
            .query_row(
                "SELECT COUNT(*) FROM meta WHERE key LIKE 'schema_extension:scripts:%'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(markers, 2);
        let marker_json: String = db
            .query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [EVENT_SCOPE_MARKER],
                |row| row.get(0),
            )
            .optional()
            .unwrap()
            .unwrap();
        let marker_digest: String = serde_json::from_str(&marker_json).unwrap();
        assert_eq!(
            marker_digest,
            model::digest(include_str!("../../migrations/009_script_event_scope.sql").as_bytes())
        );
    }
}
