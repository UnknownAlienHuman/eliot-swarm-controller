//! Public Store consumers for the durable, bounded pending ScriptRun cursor.
use super::*;

use crate::{
    config::Config,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const FAIRNESS_LABEL: &str = "reg23-script-fairness";
const FAIRNESS_OWNER_ID: &str = "reg23-script-fairness-owner";
const FAIRNESS_PROJECT_ID: &str = "reg23-script-fairness-project";
const FAIRNESS_SCRIPT_ID: &str = "reg23_script_fairness_script";
const ADMITTED_RUN_COUNT: usize = MAX_PENDING_RUN_PAGE + 1;

#[derive(Debug, Clone, PartialEq, Eq)]
struct StoredRunState {
    run_id: String,
    operation_id: String,
    created_at_ms: i64,
    run_state: String,
    operation_state: String,
    started_at_ms: Option<i64>,
    process_identity_json: Option<String>,
}

async fn fairness_store() -> (StoreOwner, PathBuf, Principal) {
    let directory = std::env::temp_dir().join(format!("{FAIRNESS_LABEL}-{}", model::new_id()));
    std::fs::create_dir_all(&directory).expect("create temporary Store directory");
    let root = DataRoot::acquire(&directory).expect("acquire temporary Store root");
    let credential = bootstrap_credential(&root.path).expect("bootstrap Operator credential");
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.scripts.executor =
        Some(crate::store::o6_taskless_path_fixture::store_only_script_executor_pin());
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .expect("start Store");
    let operator = owner
        .store
        .authenticate(credential)
        .await
        .expect("authenticate Operator");

    let token = format!("reg23-fairness-{}", model::new_id());
    owner
        .store
        .call(
            operator.clone(),
            "client.register".into(),
            json!({
                "client_request_id":format!("reg23-fairness-register-{}", model::new_id()),
                "client_id":FAIRNESS_OWNER_ID,
                "role":"manager",
                "token_hash":model::digest(token.as_bytes()),
            }),
        )
        .await
        .expect("register Manager through Store");
    let manager = owner
        .store
        .authenticate(Credential {
            client_id: FAIRNESS_OWNER_ID.to_owned(),
            token,
        })
        .await
        .expect("authenticate Manager");
    (owner, directory, manager)
}

fn powershell_path() -> PathBuf {
    let executable = if cfg!(windows) { "pwsh.exe" } else { "pwsh" };
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|path| std::env::split_paths(&path).collect::<Vec<_>>())
        .map(|directory| directory.join(executable))
        .filter_map(|path| std::fs::canonicalize(path).ok())
        .find(|path| {
            std::fs::symlink_metadata(path)
                .is_ok_and(|metadata| metadata.is_file() && !metadata.file_type().is_symlink())
        })
        .expect("pwsh is needed to validate the retained script bundle")
}

fn script_bundle() -> Value {
    json!({
        "script_id":FAIRNESS_SCRIPT_ID,
        "interpreter_kind":"powershell",
        "interpreter_path":powershell_path(),
        "entrypoint":"main.ps1",
        "argv":[],
        "trust":"trusted_local",
        "inherit_environment":[],
        "controller_effects":[],
        "input_schema":{"type":"object","properties":{},"required":[],"additional_properties":true},
        "result_schema":{"type":"null"},
        "files":[{"path":"main.ps1","content_base64":STANDARD.encode(b"Write-Output {}")}],
    })
}

async fn register_active_script(store: &Store, manager: &Principal) {
    store
        .call(
            manager.clone(),
            "script.register".into(),
            json!({
                "client_request_id":format!("reg23-fairness-script-register-{}", model::new_id()),
                "bundle":script_bundle(),
            }),
        )
        .await
        .expect("register script bundle through Store");
    store
        .call(
            manager.clone(),
            "script.activate".into(),
            json!({
                "client_request_id":format!("reg23-fairness-script-activate-{}", model::new_id()),
                "script_id":FAIRNESS_SCRIPT_ID,
                "revision":1,
            }),
        )
        .await
        .expect("activate script revision through Store");
}

async fn create_current_task_attempt(
    store: &Store,
    manager: &Principal,
    index: usize,
) -> (String, String) {
    let task = store
        .call(
            manager.clone(),
            "task.create".into(),
            json!({
                "client_request_id":format!("reg23-fairness-task-create-{index}-{}", model::new_id()),
                "project_id":FAIRNESS_PROJECT_ID,
                "spec":{
                    "objective":format!("Exercise pending-run fairness admission {index}"),
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Retain an exact current Task and Attempt"}],
                    "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                },
            }),
        )
        .await
        .expect("create Task through public Store method");
    let task_id = task["task_id"]
        .as_str()
        .expect("Task receipt contains task_id")
        .to_owned();
    let claim = store
        .call(
            manager.clone(),
            "task.claim".into(),
            json!({
                "client_request_id":format!("reg23-fairness-task-claim-{index}-{}", model::new_id()),
                "task_id":task_id,
                "expected_revision":1,
                "owner_id":manager.client_id,
            }),
        )
        .await
        .expect("claim Task through public Store method");
    let attempt_id = claim["attempt_id"]
        .as_str()
        .expect("Task claim receipt contains attempt_id")
        .to_owned();
    (task_id, attempt_id)
}

async fn admit_script_run(
    store: &Store,
    manager: &Principal,
    attempt_id: &str,
    index: usize,
) -> (String, String) {
    let receipt = store
        .call(
            manager.clone(),
            "script.run".into(),
            json!({
                "client_request_id":format!("reg23-fairness-script-run-{index}-{}", model::new_id()),
                "script_id":FAIRNESS_SCRIPT_ID,
                "expected_script_revision":1,
                "attempt_id":attempt_id,
                "expected_task_revision":1,
                "input":{"ordinal":index},
            }),
        )
        .await
        .expect("admit ScriptRun through public Store method");
    let run_id = receipt["run_id"]
        .as_str()
        .expect("ScriptRun receipt contains run_id")
        .to_owned();
    let operation_id = receipt["operation_id"]
        .as_str()
        .expect("ScriptRun receipt contains Operation ID")
        .to_owned();
    (run_id, operation_id)
}

async fn stored_run_states(store: &Store) -> Vec<StoredRunState> {
    store
        .run(|db| {
            let mut statement = db.prepare(
                "SELECT r.run_id,r.operation_id,r.created_at_ms,r.state,o.state,\
                        r.started_at_ms,r.process_identity_json \
                   FROM script_runs r JOIN operations o ON o.operation_id=r.operation_id \
                  ORDER BY r.created_at_ms,r.run_id",
            )?;
            let rows = statement
                .query_map([], |row| {
                    Ok(StoredRunState {
                        run_id: row.get(0)?,
                        operation_id: row.get(1)?,
                        created_at_ms: row.get(2)?,
                        run_state: row.get(3)?,
                        operation_state: row.get(4)?,
                        started_at_ms: row.get(5)?,
                        process_identity_json: row.get(6)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(rows)
        })
        .await
        .expect("read actual Store-admitted run and Operation rows")
}

async fn selected_pending_run_ids(store: &Store) -> Vec<String> {
    store
        .run(|db| {
            Ok(pending_runs(db)?
                .into_iter()
                .map(|pending| pending.run_id)
                .collect())
        })
        .await
        .expect("select pending runs through the production Store selector")
}

async fn pending_run_cursor(store: &Store) -> Option<Value> {
    store
        .run(|db| meta(db, PENDING_RUN_CURSOR_KEY))
        .await
        .expect("read durable pending-run cursor")
}

#[tokio::test]
async fn reg23_pending_run_cursor_reaches_the_65th_public_admission_after_reopen() {
    let (owner, directory, manager) = fairness_store().await;
    register_active_script(&owner.store, &manager).await;

    let mut receipts = Vec::with_capacity(ADMITTED_RUN_COUNT);
    for index in 0..ADMITTED_RUN_COUNT {
        let (_task_id, attempt_id) =
            create_current_task_attempt(&owner.store, &manager, index).await;
        receipts.push(admit_script_run(&owner.store, &manager, &attempt_id, index).await);
    }

    let before = stored_run_states(&owner.store).await;
    assert_eq!(before.len(), ADMITTED_RUN_COUNT);
    assert!(before.iter().all(|run| {
        run.run_state == "queued"
            && run.operation_state == "queued"
            && run.started_at_ms.is_none()
            && run.process_identity_json.is_none()
    }));

    let mut admitted_pairs = receipts;
    admitted_pairs.sort();
    let mut stored_pairs = before
        .iter()
        .map(|run| (run.run_id.clone(), run.operation_id.clone()))
        .collect::<Vec<_>>();
    stored_pairs.sort();
    assert_eq!(
        stored_pairs, admitted_pairs,
        "all rows came from public receipts"
    );

    let first_page = selected_pending_run_ids(&owner.store).await;
    assert_eq!(first_page.len(), MAX_PENDING_RUN_PAGE);
    let expected_first = before[..MAX_PENDING_RUN_PAGE]
        .iter()
        .map(|run| run.run_id.clone())
        .collect::<Vec<_>>();
    assert_eq!(first_page, expected_first);
    assert!(pending_run_cursor(&owner.store).await.is_none());

    let (advanced_page, committed_cursor) = owner
        .store
        .run(|db| {
            let rows = pending_run_page(db, None)?;
            let page_ids = rows
                .iter()
                .map(|row| row.run_id.clone())
                .collect::<Vec<_>>();
            let last = rows.last().expect("the first pending page is non-empty");
            advance_pending_run_cursor(db, &last.cursor_key())?;
            Ok((page_ids, meta(db, PENDING_RUN_CURSOR_KEY)?))
        })
        .await
        .expect("advance only from the last exact row in the selected page");
    assert_eq!(advanced_page, expected_first);
    let expected_later = &before[MAX_PENDING_RUN_PAGE];
    assert_eq!(
        committed_cursor
            .as_ref()
            .and_then(|cursor| cursor["run_id"].as_str()),
        Some(before[MAX_PENDING_RUN_PAGE - 1].run_id.as_str())
    );
    assert_eq!(
        committed_cursor
            .as_ref()
            .and_then(|cursor| cursor["created_at_ms"].as_i64()),
        Some(before[MAX_PENDING_RUN_PAGE - 1].created_at_ms)
    );

    let after_cursor = selected_pending_run_ids(&owner.store).await;
    assert_eq!(after_cursor.first(), Some(&expected_later.run_id));
    assert_eq!(after_cursor.len(), MAX_PENDING_RUN_PAGE);
    assert_eq!(stored_run_states(&owner.store).await, before);
    assert_eq!(pending_run_cursor(&owner.store).await, committed_cursor);

    owner.close().await.expect("close Store before reopen");
    let root = DataRoot::acquire(&directory).expect("reacquire Store root");
    let credential = bootstrap_credential(&root.path).expect("read retained Operator credential");
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.scripts.executor =
        Some(crate::store::o6_taskless_path_fixture::store_only_script_executor_pin());
    let reopened = StoreOwner::start(root, Arc::new(config), credential)
        .await
        .expect("reopen Store with the durable pending-run cursor");

    let after_reopen = selected_pending_run_ids(&reopened.store).await;
    assert_eq!(after_reopen.first(), Some(&expected_later.run_id));
    assert_eq!(after_reopen.len(), MAX_PENDING_RUN_PAGE);
    assert_eq!(stored_run_states(&reopened.store).await, before);
    assert_eq!(pending_run_cursor(&reopened.store).await, committed_cursor);

    reopened.close().await.expect("close reopened Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
}

/// A retained cursor value must be exactly the server-written object:
/// missing key is the valid initial state, while null, array, string,
/// incomplete, negative-timestamp, noncanonical and extra-field shapes are
/// cursor-integrity errors with no subject processing or writes.
#[tokio::test]
async fn reg142_corrupt_cursor_blocks_subject_processing() {
    let valid_run = model::new_id();
    let cases: Vec<Value> = vec![
        Value::Null,
        json!([]),
        json!("just-a-string"),
        json!({"created_at_ms": 0}),
        json!({"run_id": valid_run}),
        json!({"created_at_ms": -1, "run_id": valid_run}),
        json!({"created_at_ms": 0, "run_id": "not-a-uuid"}),
        json!({"created_at_ms": 0, "run_id": "550E8400-E29B-41D4-A716-446655440000"}),
        json!({"created_at_ms": 0, "run_id": valid_run, "extra": 1}),
    ];
    for cursor in cases {
        let (owner, directory, manager) = fairness_store().await;
        register_active_script(&owner.store, &manager).await;
        let (_task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager, 0).await;
        let (run_id, _operation_id) =
            admit_script_run(&owner.store, &manager, &attempt_id, 0).await;
        assert!(pending_run_cursor(&owner.store).await.is_none());
        let retained_cursor = cursor.clone();
        owner
            .store
            .run(move |db| {
                set_meta(db, PENDING_RUN_CURSOR_KEY, &retained_cursor)?;
                Ok(())
            })
            .await
            .expect("install corrupt cursor");
        let before = stored_run_states(&owner.store).await;
        let err = owner
            .store
            .reconcile_scripts_once()
            .await
            .expect_err("corrupt cursor must fail before subject processing");
        assert_eq!(err.code, "INVALID_PARAMS");
        assert!(err.message.contains("cursor"));
        let after = stored_run_states(&owner.store).await;
        assert_eq!(before, after, "no subject writes under corrupt cursor");
        assert_eq!(
            reg142_incident_count(&owner.store, &run_id).await,
            0,
            "no incident recorded under corrupt cursor"
        );
        assert_eq!(
            pending_run_cursor(&owner.store).await,
            Some(cursor.clone()),
            "corrupt value retained, not reset"
        );
        owner.close().await.expect("close Store");
        std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
    }
}

/// A retained tuple change between selection and advance never moves the
/// cursor: the captured coordinate is verified, not replaced.
#[tokio::test]
async fn reg142_captured_tuple_change_blocks_cursor_jump() {
    let (owner, directory, manager) = fairness_store().await;
    register_active_script(&owner.store, &manager).await;
    reg142_admit_many(&owner.store, &manager, 0, 2).await;
    let before = stored_run_states(&owner.store).await;
    assert_eq!(before.len(), 2);
    let row1 = before[0].clone();
    let captured = PendingRunCursor {
        created_at_ms: row1.created_at_ms,
        run_id: row1.run_id.clone(),
    };
    let first_captured = captured.clone();
    let moved = owner
        .store
        .run(move |db| {
            verify_captured_tuple(db, &first_captured)?;
            advance_pending_run_cursor(db, &first_captured)
        })
        .await
        .expect("first advance from captured tuple");
    assert!(moved);
    let changed_run_id = row1.run_id.clone();
    let changed_timestamp = row1.created_at_ms + 1_000;
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE script_runs SET created_at_ms=?1 WHERE run_id=?2",
                (changed_timestamp, changed_run_id),
            )?;
            Ok(())
        })
        .await
        .expect("mutate retained tuple");
    let err = owner
        .store
        .run(move |db| {
            verify_captured_tuple(db, &captured)?;
            advance_pending_run_cursor(db, &captured)
        })
        .await
        .expect_err("stale tuple must not advance");
    assert_eq!(err.code, "INVALID_PARAMS");
    let cursor = pending_run_cursor(&owner.store)
        .await
        .expect("cursor is set");
    assert_eq!(
        cursor["created_at_ms"].as_i64(),
        Some(row1.created_at_ms),
        "no jump to the changed coordinate"
    );
    assert_eq!(cursor["run_id"].as_str(), Some(row1.run_id.as_str()));
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
}

async fn reg142_install_cursor_counter(store: &Store) {
    let key = PENDING_RUN_CURSOR_KEY;
    let sql = format!(
        "CREATE TABLE reg142_cursor_writes(n INTEGER NOT NULL DEFAULT 0); INSERT INTO reg142_cursor_writes(n) VALUES(0); CREATE TRIGGER reg142_count_cursor_insert AFTER INSERT ON meta WHEN NEW.key = '{key}' BEGIN UPDATE reg142_cursor_writes SET n = n + 1; END; CREATE TRIGGER reg142_count_cursor_update AFTER UPDATE ON meta WHEN NEW.key = '{key}' BEGIN UPDATE reg142_cursor_writes SET n = n + 1; END;"
    );
    store
        .run(move |db| {
            db.execute_batch(&sql)?;
            Ok(())
        })
        .await
        .expect("install cursor write counter");
}

async fn reg142_cursor_writes(store: &Store) -> i64 {
    store
        .run(|db| {
            let count: i64 =
                db.query_row("SELECT n FROM reg142_cursor_writes", [], |row| row.get(0))?;
            Ok(count)
        })
        .await
        .expect("read cursor write counter")
}

async fn reg142_drop_cursor_counter(store: &Store) {
    store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg142_count_cursor_insert; DROP TRIGGER reg142_count_cursor_update; DROP TABLE reg142_cursor_writes;")?;
            Ok(())
        })
        .await
        .expect("drop cursor write counter");
}

/// An empty page performs no cursor write at all.
#[tokio::test]
async fn reg142_empty_page_writes_no_cursor() {
    let (owner, directory, _manager) = fairness_store().await;
    owner
        .store
        .reconcile_scripts_once()
        .await
        .expect("empty reconcile succeeds");
    assert!(pending_run_cursor(&owner.store).await.is_none());
    assert!(selected_pending_run_ids(&owner.store).await.is_empty());

    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
}

/// One unchanged long-running subject creates no permanent metadata-write
/// loop: the first pass persists the cursor once, the next pass observes the
/// same subject without any further cursor write, and re-advancing the
/// identical value is a no-op.
#[tokio::test]
async fn reg142_single_subject_writes_no_loop() {
    let (owner, directory, manager) = fairness_store().await;
    register_active_script(&owner.store, &manager).await;
    reg142_admit_many(&owner.store, &manager, 0, 1).await;

    let before = stored_run_states(&owner.store).await;
    assert_eq!(before.len(), 1);
    let only = before[0].clone();
    let staged_run = only.run_id.clone();
    let staged_operation = only.operation_id.clone();
    owner
        .store
        .run(move |db| {
            let sends_sending = format!(
                "UPDATE operations SET state = 'sending' WHERE operation_id = '{staged_operation}'"
            );
            let runs_sending =
                format!("UPDATE script_runs SET state = 'sending' WHERE run_id = '{staged_run}'");
            db.execute_batch(&sends_sending)?;
            db.execute_batch(&runs_sending)?;
            Ok(())
        })
        .await
        .expect("stage a sending subject");
    reg142_install_cursor_counter(&owner.store).await;

    owner
        .store
        .reconcile_scripts_once()
        .await
        .expect("first pass observes the sending subject");
    let states = stored_run_states(&owner.store).await;
    assert_eq!(states.len(), 1);
    assert_eq!(states[0].run_state, "sending");
    assert_eq!(states[0].operation_state, "sending");
    let cursor = pending_run_cursor(&owner.store)
        .await
        .expect("cursor is set");
    assert_eq!(cursor["run_id"].as_str(), Some(states[0].run_id.as_str()));
    assert_eq!(reg142_cursor_writes(&owner.store).await, 1);

    owner
        .store
        .reconcile_scripts_once()
        .await
        .expect("second pass observes the same subject");
    let again = stored_run_states(&owner.store).await;
    assert_eq!(again, states);
    assert_eq!(pending_run_cursor(&owner.store).await, Some(cursor));
    assert_eq!(reg142_cursor_writes(&owner.store).await, 1);

    let direct_run = states[0].run_id.clone();
    let advanced = owner
        .store
        .run(move |db| {
            let key = selected_pending_run_key(db, &direct_run)?.expect("subject is selected");
            advance_pending_run_cursor(db, &key.cursor_key())
        })
        .await
        .expect("re-advance the identical value");
    assert!(!advanced);
    assert_eq!(reg142_cursor_writes(&owner.store).await, 1);
    reg142_drop_cursor_counter(&owner.store).await;

    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
}

async fn reg142_install_cursor_abort(store: &Store) {
    let key = PENDING_RUN_CURSOR_KEY;
    let sql = format!(
        "CREATE TRIGGER reg142_cursor_insert_fail BEFORE INSERT ON meta WHEN NEW.key = '{key}' BEGIN SELECT RAISE(ABORT, 'reg142 injected cursor write failure'); END; CREATE TRIGGER reg142_cursor_update_fail BEFORE UPDATE ON meta WHEN NEW.key = '{key}' BEGIN SELECT RAISE(ABORT, 'reg142 injected cursor write failure'); END;"
    );
    store
        .run(move |db| {
            db.execute_batch(&sql)?;
            Ok(())
        })
        .await
        .expect("install cursor abort triggers");
}

async fn reg142_operation_result(store: &Store, operation_id: &str) -> String {
    let sql = format!("SELECT result_json FROM operations WHERE operation_id = '{operation_id}'");
    store
        .run(move |db| {
            let raw: Option<String> = db.query_row(&sql, [], |row| row.get(0))?;
            Ok(raw)
        })
        .await
        .expect("read operation result")
        .expect("operation result is retained")
}

/// Failure before row 1 leaves the previous cursor unchanged: with the hard
/// fault scoped to the first selected subject, the real supervisor loop
/// returns without writing any cursor, and the retry converges afterwards.
#[tokio::test]
async fn reg142_failure_before_row1_keeps_previous_cursor() {
    let (owner, directory, manager) = fairness_store().await;
    register_active_script(&owner.store, &manager).await;
    reg142_admit_many(&owner.store, &manager, 0, 2).await;

    let before = stored_run_states(&owner.store).await;
    assert_eq!(before.len(), 2);
    let row1 = before[0].clone();
    let row2 = before[1].clone();
    reg142_damage_all_digests(&owner.store).await;
    reg142_install_incident_abort(&owner.store, &row1.operation_id).await;
    assert!(pending_run_cursor(&owner.store).await.is_none());

    let err = owner
        .store
        .reconcile_scripts_once()
        .await
        .expect_err("row 1 hard failure must propagate");
    assert_eq!(err.code, "STORE_ERROR");
    assert!(pending_run_cursor(&owner.store).await.is_none());
    let states = stored_run_states(&owner.store).await;
    for state in &states {
        assert_eq!(state.run_state, "queued");
        assert_eq!(state.operation_state, "queued");
    }

    reg142_drop_trigger(&owner.store, "reg142_row_hard_fail").await;
    owner
        .store
        .reconcile_scripts_once()
        .await
        .expect("retry converges after the fault is removed");
    let states = stored_run_states(&owner.store).await;
    assert!(
        states
            .iter()
            .all(|run| run.run_state == "failed" && run.operation_state == "settled")
    );
    let cursor = pending_run_cursor(&owner.store)
        .await
        .expect("cursor is set");
    assert_eq!(cursor["run_id"].as_str(), Some(row2.run_id.as_str()));

    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
}

/// Cursor write failure keeps the subject truth: the failed disposition is
/// already committed when the cursor write aborts, the cursor stays behind,
/// no second execution is authorized, and the retry converges by readback.
#[tokio::test]
async fn reg142_cursor_write_failure_keeps_subject_truth() {
    let (owner, directory, manager) = fairness_store().await;
    register_active_script(&owner.store, &manager).await;
    reg142_admit_many(&owner.store, &manager, 0, 2).await;

    let before = stored_run_states(&owner.store).await;
    assert_eq!(before.len(), 2);
    let row1 = before[0].clone();
    let row2 = before[1].clone();
    reg142_damage_all_digests(&owner.store).await;
    reg142_install_cursor_abort(&owner.store).await;

    let err = owner
        .store
        .reconcile_scripts_once()
        .await
        .expect_err("cursor write failure must propagate");
    assert_eq!(err.code, "STORE_ERROR");
    let states = stored_run_states(&owner.store).await;
    let failed1 = states
        .iter()
        .find(|run| run.run_id == row1.run_id)
        .expect("row 1 is retained")
        .clone();
    assert_eq!(failed1.run_state, "failed");
    assert_eq!(failed1.operation_state, "settled");
    let result1 = reg142_operation_result(&owner.store, &row1.operation_id).await;
    assert!(result1.contains("\"execution_started\":false"));
    assert!(pending_run_cursor(&owner.store).await.is_none());
    let queued2 = states
        .iter()
        .find(|run| run.run_id == row2.run_id)
        .expect("row 2 is retained");
    assert_eq!(queued2.run_state, "queued");

    reg142_drop_trigger(&owner.store, "reg142_cursor_insert_fail").await;
    reg142_drop_trigger(&owner.store, "reg142_cursor_update_fail").await;
    owner
        .store
        .reconcile_scripts_once()
        .await
        .expect("retry converges by readback");
    let states = stored_run_states(&owner.store).await;
    let retried1 = states
        .iter()
        .find(|run| run.run_id == row1.run_id)
        .expect("row 1 is retained");
    assert_eq!(retried1, &failed1);
    assert_eq!(
        reg142_operation_result(&owner.store, &row1.operation_id).await,
        result1
    );
    let cursor = pending_run_cursor(&owner.store)
        .await
        .expect("cursor is set");
    assert_eq!(cursor["run_id"].as_str(), Some(row2.run_id.as_str()));

    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
}

async fn reg142_damage_all_digests(store: &Store) {
    store
        .run(|db| {
            db.execute(
                "UPDATE script_runs SET work_digest = '0000000000000000000000000000000000000000000000000000000000000000'",
                [],
            )?;
            Ok(())
        })
        .await
        .expect("damage retained work digests");
}

async fn reg142_install_incident_abort(store: &Store, operation_id: &str) {
    let sql = format!(
        "CREATE TRIGGER reg142_row_hard_fail BEFORE INSERT ON incidents WHEN NEW.action_operation_id = '{operation_id}' BEGIN SELECT RAISE(ABORT, 'reg142 injected incident persistence failure'); END;"
    );
    store
        .run(move |db| {
            db.execute_batch(&sql)?;
            Ok(())
        })
        .await
        .expect("install incident abort trigger");
}

async fn reg142_drop_trigger(store: &Store, name: &str) {
    let sql = format!("DROP TRIGGER {name}");
    store
        .run(move |db| {
            db.execute_batch(&sql)?;
            Ok(())
        })
        .await
        .expect("drop trigger");
}

async fn reg142_incident_count(store: &Store, run_id: &str) -> i64 {
    let sql = format!(
        "SELECT COUNT(*) FROM incidents WHERE dedup_key = 'script:{run_id}:SCRIPT_WORK_DAMAGED' AND state = 'open'"
    );
    store
        .run(move |db| {
            let count: i64 = db.query_row(&sql, [], |row| row.get(0))?;
            Ok(count)
        })
        .await
        .expect("count open incidents")
}

async fn reg142_reopen(directory: &std::path::Path) -> StoreOwner {
    let root = DataRoot::acquire(directory).expect("reacquire Store root");
    let credential = bootstrap_credential(&root.path).expect("read retained Operator credential");
    let mut config = Config::default();
    config.storage.data_dir = directory.to_path_buf();
    config.scripts.executor =
        Some(crate::store::o6_taskless_path_fixture::store_only_script_executor_pin());
    StoreOwner::start(root, Arc::new(config), credential)
        .await
        .expect("reopen Store")
}

async fn reg142_admit_many(store: &Store, manager: &Principal, first_index: usize, count: usize) {
    for index in first_index..first_index + count {
        let (_task_id, attempt_id) = create_current_task_attempt(store, manager, index).await;
        admit_script_run(store, manager, &attempt_id, index).await;
    }
}

/// Row-2 hard failure on the real supervisor loop: with 65 admitted runs,
/// incident persistence aborts while processing row 2, so the pass returns
/// a hard error with the cursor at row 1 (last completed), never at row 64
/// (last selected). After the fault is removed and the store reopens, row 2
/// is the next eligible subject, every row processes exactly once in order,
/// and newer admissions cannot starve the older subject.
#[tokio::test]
async fn reg142_row2_hard_failure_keeps_cursor_at_row1() {
    let (owner, directory, manager) = fairness_store().await;
    register_active_script(&owner.store, &manager).await;
    reg142_admit_many(&owner.store, &manager, 0, ADMITTED_RUN_COUNT).await;

    let before = stored_run_states(&owner.store).await;
    assert_eq!(before.len(), ADMITTED_RUN_COUNT);
    let row1 = before[0].clone();
    let row2 = before[1].clone();
    let row64 = before[MAX_PENDING_RUN_PAGE - 1].clone();

    reg142_damage_all_digests(&owner.store).await;
    reg142_install_incident_abort(&owner.store, &row2.operation_id).await;

    let err = owner
        .store
        .reconcile_scripts_once()
        .await
        .expect_err("row 2 hard failure must propagate");
    assert_eq!(err.code, "STORE_ERROR");

    let cursor = pending_run_cursor(&owner.store)
        .await
        .expect("cursor is set");
    assert_eq!(cursor["run_id"].as_str(), Some(row1.run_id.as_str()));
    assert_eq!(cursor["created_at_ms"].as_i64(), Some(row1.created_at_ms));
    assert_ne!(cursor["run_id"].as_str(), Some(row64.run_id.as_str()));

    let states = stored_run_states(&owner.store).await;
    let stored1 = states
        .iter()
        .find(|run| run.run_id == row1.run_id)
        .expect("row 1 is retained");
    assert_eq!(stored1.run_state, "failed");
    assert_eq!(stored1.operation_state, "settled");
    let stored2 = states
        .iter()
        .find(|run| run.run_id == row2.run_id)
        .expect("row 2 is retained");
    assert_eq!(stored2.run_state, "queued");
    assert_eq!(stored2.operation_state, "queued");

    reg142_admit_many(&owner.store, &manager, ADMITTED_RUN_COUNT, 10).await;
    reg142_damage_all_digests(&owner.store).await;
    let page = selected_pending_run_ids(&owner.store).await;
    assert_eq!(page.len(), MAX_PENDING_RUN_PAGE);
    assert_eq!(page.first(), Some(&row2.run_id));

    owner.close().await.expect("close Store before reopen");
    let reopened = reg142_reopen(&directory).await;
    let cursor = pending_run_cursor(&reopened.store)
        .await
        .expect("cursor survives reopen");
    assert_eq!(cursor["run_id"].as_str(), Some(row1.run_id.as_str()));
    let page = selected_pending_run_ids(&reopened.store).await;
    assert_eq!(page.len(), MAX_PENDING_RUN_PAGE);
    assert_eq!(page.first(), Some(&row2.run_id));

    reg142_drop_trigger(&reopened.store, "reg142_row_hard_fail").await;
    let recovery_page = selected_pending_run_ids(&reopened.store).await;
    assert_eq!(recovery_page.len(), MAX_PENDING_RUN_PAGE);
    assert_eq!(recovery_page.first(), Some(&row2.run_id));
    reopened
        .store
        .reconcile_scripts_once()
        .await
        .expect("recovery pass processes the selected page exactly once");
    let cursor = pending_run_cursor(&reopened.store)
        .await
        .expect("cursor is set");
    assert_eq!(
        cursor["run_id"].as_str(),
        recovery_page.last().map(String::as_str)
    );
    assert_eq!(
        reg142_incident_count(&reopened.store, &row1.run_id).await,
        1
    );
    assert_eq!(
        reg142_incident_count(&reopened.store, &row2.run_id).await,
        1
    );

    reopened
        .store
        .reconcile_scripts_once()
        .await
        .expect("drain pass processes the newer admissions");
    let states = stored_run_states(&reopened.store).await;
    assert_eq!(states.len(), ADMITTED_RUN_COUNT + 10);
    assert!(
        states
            .iter()
            .all(|run| run.run_state == "failed" && run.operation_state == "settled")
    );
    let last = states.last().expect("ordered states").clone();
    let cursor = pending_run_cursor(&reopened.store)
        .await
        .expect("cursor is set");
    assert_eq!(cursor["run_id"].as_str(), Some(last.run_id.as_str()));
    assert_eq!(cursor["created_at_ms"].as_i64(), Some(last.created_at_ms));
    assert!(selected_pending_run_ids(&reopened.store).await.is_empty());

    reopened.close().await.expect("close reopened Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary Store directory");
}
