//! Connected Store consumers for REG23-P2-09 watch quarantine and rollback.
use super::*;

use crate::{
    config::Config,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rusqlite::params;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const LABEL: &str = "reg23-watch-quarantine";
const OWNER_ID: &str = "reg23-watch-quarantine-owner";
const PROJECT_ID: &str = "reg23-watch-quarantine-project";
const SCRIPT_ID: &str = "reg23_watch_quarantine_script";

fn record_key_for(watch_id: &str) -> String {
    record_key(watch_id)
}

fn active_key_for(watch_id: &str) -> String {
    active_key(watch_id)
}

async fn store_fixture(label: &str) -> (StoreOwner, PathBuf, Principal, Principal) {
    let directory = std::env::temp_dir().join(format!("{label}-{}", model::new_id()));
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

    let manager_token = format!("reg23-watch-{}", model::new_id());
    owner
        .store
        .call(
            operator.clone(),
            "client.register".into(),
            json!({
                "client_request_id":format!("reg23-register-{}", model::new_id()),
                "client_id":OWNER_ID,
                "role":"manager",
                "token_hash":model::digest(manager_token.as_bytes()),
            }),
        )
        .await
        .expect("register Manager through Store");
    let manager = owner
        .store
        .authenticate(Credential {
            client_id: OWNER_ID.to_owned(),
            token: manager_token,
        })
        .await
        .expect("authenticate Manager");
    (owner, directory, operator, manager)
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

fn script_bundle(script_id: &str) -> Value {
    json!({
        "script_id":script_id,
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
                "client_request_id":format!("reg23-script-register-{}", model::new_id()),
                "bundle":script_bundle(SCRIPT_ID),
            }),
        )
        .await
        .expect("register immutable script bundle through Store");
    store
        .call(
            manager.clone(),
            "script.activate".into(),
            json!({
                "client_request_id":format!("reg23-script-activate-{}", model::new_id()),
                "script_id":SCRIPT_ID,
                "revision":1,
            }),
        )
        .await
        .expect("activate script revision through Store");
}

async fn create_current_task_attempt(store: &Store, manager: &Principal) -> (String, String) {
    let task = store
        .call(
            manager.clone(),
            "task.create".into(),
            json!({
                "client_request_id":format!("reg23-task-create-{}", model::new_id()),
                "project_id":PROJECT_ID,
                "spec":{
                    "objective":"Exercise retained operation watch reconciliation",
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Keep an exact current Task and Attempt"}],
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
                "client_request_id":format!("reg23-task-claim-{}", model::new_id()),
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
    let readback = store
        .call(
            manager.clone(),
            "task.get".into(),
            json!({"task_id":task_id}),
        )
        .await
        .expect("read the current Task");
    assert_eq!(readback["revision"], 1);
    assert_eq!(readback["current_attempt_id"], attempt_id);
    (task_id, attempt_id)
}

async fn admit_script_run(
    store: &Store,
    manager: &Principal,
    attempt_id: &str,
    request_suffix: &str,
) -> (String, String) {
    let receipt = store
        .call(
            manager.clone(),
            "script.run".into(),
            json!({
                "client_request_id":format!("reg23-script-run-{request_suffix}"),
                "script_id":SCRIPT_ID,
                "expected_script_revision":1,
                "attempt_id":attempt_id,
                "expected_task_revision":1,
                "input":{},
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

async fn create_operation_watch(
    store: &Store,
    manager: &Principal,
    task_id: &str,
    attempt_id: &str,
    operation_id: &str,
) -> String {
    let receipt = store
        .call(
            manager.clone(),
            "coordination.watch.create".into(),
            json!({
                "client_request_id":format!("reg23-watch-create-{}", model::new_id()),
                "task_id":task_id,
                "task_revision":1,
                "attempt_id":attempt_id,
                "watch_kind":"operation_terminal",
                "address":{"operation_id":operation_id},
                "expires_at_ms":model::now_ms().expect("read test clock")+600_000,
                "delivery":"mailbox_header",
                "one_shot":true,
            }),
        )
        .await
        .expect("create exact-scope operation watch through Store");
    receipt["watch_id"]
        .as_str()
        .expect("watch receipt contains watch_id")
        .to_owned()
}

async fn cancel_operation(store: &Store, manager: &Principal, operation_id: &str) -> Value {
    let receipt = store
        .call(
            manager.clone(),
            "operation.cancel".into(),
            json!({
                "client_request_id":format!("reg23-cancel-{}", model::new_id()),
                "operation_id":operation_id,
                "reason":"exercise the retained terminal watch consumer",
            }),
        )
        .await
        .expect("owner cancels queued ScriptRun through Store");
    assert_eq!(receipt["cancelled_operation_id"], operation_id);
    receipt
}

async fn damage_scope_revision(store: &Store, watch_id: &str) -> String {
    let key = record_key_for(watch_id);
    store
        .run(move |db| {
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            let mut record: Value = serde_json::from_str(&raw)?;
            record["scope"]["task_revision"] = json!(0);
            let damaged_raw = model::canonical(&record)?;
            db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, damaged_raw],
            )?;
            Ok(damaged_raw)
        })
        .await
        .expect("corrupt only the retained inner watch record")
}

async fn record_raw(store: &Store, watch_id: &str) -> String {
    let key = record_key_for(watch_id);
    store
        .run(move |db| {
            let raw = db.query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
                row.get::<_, String>(0)
            })?;
            Ok(raw)
        })
        .await
        .expect("read retained watch record")
}

async fn active_index_count(store: &Store, watch_id: &str) -> i64 {
    let key = active_key_for(watch_id);
    store
        .run(move |db| {
            let count = db.query_row("SELECT COUNT(*) FROM meta WHERE key=?1", [key], |row| {
                row.get::<_, i64>(0)
            })?;
            Ok(count)
        })
        .await
        .expect("count active watch index rows")
}

async fn quarantine_keys(store: &Store) -> Vec<String> {
    store
        .run(|db| {
            let mut statement =
                db.prepare("SELECT key FROM meta WHERE key LIKE ?1 ORDER BY key")?;
            let rows = statement
                .query_map([format!("{QUARANTINE_PREFIX}%")], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<String>>>()?;
            Ok(rows)
        })
        .await
        .expect("read bounded quarantine keys")
}

async fn quarantine_record(store: &Store, key: &str) -> Value {
    let key = key.to_owned();
    store
        .run(move |db| {
            crate::automation::config::read_record(db, &key, "automation quarantine")?
                .ok_or_else(|| Error::new("TEST_QUARANTINE_MISSING", "quarantine row is absent"))
        })
        .await
        .expect("read quarantine evidence")
}

#[tokio::test]
async fn reg23_watch_quarantines_invalid_retained_scope_and_matches_healthy_neighbor() {
    let (owner, directory, _operator, manager) = store_fixture(LABEL).await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (run_a, operation_a) =
        admit_script_run(&owner.store, &manager, &attempt_id, "watch-a").await;
    let (run_b, operation_b) =
        admit_script_run(&owner.store, &manager, &attempt_id, "watch-b").await;
    assert_ne!(run_a, run_b);
    assert_ne!(operation_a, operation_b);
    let watch_a =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_a).await;
    let watch_b =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_b).await;
    let (damaged_watch, damaged_operation, healthy_watch, healthy_operation) =
        if active_key_for(&watch_a) < active_key_for(&watch_b) {
            (watch_a, operation_a.clone(), watch_b, operation_b.clone())
        } else {
            (watch_b, operation_b.clone(), watch_a, operation_a.clone())
        };
    let damaged_snapshot = damage_scope_revision(&owner.store, &damaged_watch).await;

    let cancelled_a = cancel_operation(&owner.store, &manager, &operation_a).await;
    let cancelled_b = cancel_operation(&owner.store, &manager, &operation_b).await;
    assert_eq!(cancelled_a["cancelled_operation_id"], operation_a);
    assert_eq!(cancelled_b["cancelled_operation_id"], operation_b);

    let receipt = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("damaged subject must not abort its healthy neighbor");
    assert_eq!(receipt["processed"], 2, "{receipt}");
    assert_eq!(receipt["quarantined"], 1, "{receipt}");
    assert_eq!(receipt["matched"], 1, "{receipt}");
    assert_eq!(receipt["model_wake"], false);
    assert_eq!(receipt["native_work_queued"], false);

    assert_eq!(
        record_raw(&owner.store, &damaged_watch).await,
        damaged_snapshot
    );
    assert_eq!(active_index_count(&owner.store, &damaged_watch).await, 0);
    assert_eq!(active_index_count(&owner.store, &healthy_watch).await, 0);

    let healthy: Value = serde_json::from_str(&record_raw(&owner.store, &healthy_watch).await)
        .expect("decode matched healthy watch");
    assert_eq!(healthy["state"], "matched");
    assert_eq!(healthy["scope"]["task_id"], task_id);
    assert_eq!(healthy["scope"]["task_revision"], 1);
    assert_eq!(healthy["scope"]["attempt_id"], attempt_id);
    assert_eq!(healthy["cursor"]["state"], "cancelled");
    assert_eq!(healthy["cursor"]["operation_id"], healthy_operation);
    assert_ne!(healthy_operation, damaged_operation);

    let keys = quarantine_keys(&owner.store).await;
    assert_eq!(keys.len(), 1, "one bounded damaged subject");
    let quarantine = quarantine_record(&owner.store, &keys[0]).await;
    assert_eq!(quarantine["code"], "WATCH_RECORD_DAMAGED");
    assert_eq!(quarantine["occurrences"], 1);
    let evidence = quarantine["evidence"]
        .as_object()
        .expect("bounded evidence object");
    assert_eq!(evidence.len(), 3);
    assert!(
        evidence["subject_identity"]
            .as_str()
            .is_some_and(|value| value.starts_with("watch-active-key-sha256:"))
    );
    assert!(
        evidence["source_pointer"]
            .as_str()
            .is_some_and(|value| value.starts_with("meta/key-sha256:"))
    );
    assert_eq!(evidence["source_digest"].as_str().map(str::len), Some(64));
    assert_eq!(
        evidence["source_digest"],
        model::digest(damaged_snapshot.as_bytes())
    );
    assert_eq!(
        evidence["source_pointer"],
        format!(
            "meta/key-sha256:{}",
            model::digest(record_key_for(&damaged_watch).as_bytes()),
        )
    );
    let serialized_evidence = quarantine.to_string();
    assert!(serialized_evidence.len() <= 1024);
    for plaintext in [&damaged_watch, &task_id, &attempt_id, &damaged_operation] {
        assert!(!serialized_evidence.contains(plaintext));
    }
    let retry = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("retry settled page");
    assert_eq!(retry["processed"], 0);
    assert_eq!(quarantine_keys(&owner.store).await, keys);
    assert_eq!(
        quarantine_record(&owner.store, &keys[0]).await["occurrences"],
        1
    );

    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary directory");
}

async fn install_cursor_write_fault(store: &Store) {
    store
        .run(|db| {
            db.execute_batch(
                r#"
                CREATE TEMP TRIGGER reg23_watch_cursor_insert_fault
                BEFORE INSERT ON main.meta
                WHEN NEW.key='coordination:watch:v1:reconcile_cursor'
                BEGIN SELECT RAISE(ABORT, 'REG23 cursor write fault'); END;
                CREATE TEMP TRIGGER reg23_watch_cursor_update_fault
                BEFORE UPDATE OF value_json ON main.meta
                WHEN NEW.key='coordination:watch:v1:reconcile_cursor'
                BEGIN SELECT RAISE(ABORT, 'REG23 cursor write fault'); END;
                "#,
            )?;
            Ok(())
        })
        .await
        .expect("install connection-local reconcile cursor fault");
}

async fn remove_cursor_write_fault(store: &Store) {
    store
        .run(|db| {
            db.execute_batch(
                "DROP TRIGGER temp.reg23_watch_cursor_insert_fault;
                 DROP TRIGGER temp.reg23_watch_cursor_update_fault;",
            )?;
            Ok(())
        })
        .await
        .expect("remove connection-local reconcile cursor fault");
}

#[tokio::test]
async fn reg23_watch_cursor_write_failure_rolls_back_subject_and_retry_matches() {
    let (owner, directory, _operator, manager) = store_fixture("reg23-watch-cursor-rollback").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (run_id, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "cursor-rollback").await;
    assert!(!run_id.is_empty());
    assert_ne!(
        run_id, operation_id,
        "ScriptRun and Operation identities are distinct"
    );
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    cancel_operation(&owner.store, &manager, &operation_id).await;
    let active_snapshot = record_raw(&owner.store, &watch_id).await;
    assert!(
        serde_json::from_str::<Value>(&active_snapshot)
            .is_ok_and(|record| record["state"] == "active")
    );

    install_cursor_write_fault(&owner.store).await;
    let error = owner
        .store
        .reconcile_watches_once()
        .await
        .expect_err("a global cursor write failure must abort the domain transaction");
    assert_ne!(error.code, "WATCH_RECORD_DAMAGED");
    assert_eq!(record_raw(&owner.store, &watch_id).await, active_snapshot);
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    assert!(quarantine_keys(&owner.store).await.is_empty());
    let cursor = owner
        .store
        .run(|db| meta(db, RECONCILE_CURSOR_KEY))
        .await
        .expect("read cursor after failed domain transaction");
    assert!(cursor.is_none(), "failed cursor write leaves no cursor");

    remove_cursor_write_fault(&owner.store).await;
    let retry = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("retry succeeds after the global failure is removed");
    assert_eq!(retry["processed"], 1, "{retry}");
    assert_eq!(retry["matched"], 1, "{retry}");
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 0);
    let matched: Value = serde_json::from_str(&record_raw(&owner.store, &watch_id).await)
        .expect("decode matched watch after retry");
    assert_eq!(matched["state"], "matched");
    assert_eq!(matched["cursor"]["state"], "cancelled");

    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(&directory).expect("remove temporary directory");
}

async fn malformed_index_neighbor_case(raw_index: &str) {
    let (owner, directory, _operator, manager) = store_fixture("watch-malformed-index").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_a) =
        admit_script_run(&owner.store, &manager, &attempt_id, "malformed-a").await;
    let (_, operation_b) =
        admit_script_run(&owner.store, &manager, &attempt_id, "malformed-b").await;
    let watch_a =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_a).await;
    let watch_b =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_b).await;
    let (damaged_watch, healthy_watch) = if active_key_for(&watch_a) < active_key_for(&watch_b) {
        (watch_a, watch_b)
    } else {
        (watch_b, watch_a)
    };
    let retained_raw = record_raw(&owner.store, &damaged_watch).await;
    let key = active_key_for(&damaged_watch);
    let raw = raw_index.to_owned();
    owner
        .store
        .run(move |db| {
            // Inject corrupt retained bytes after public admission; restore
            // the schema guard before the real reconciliation consumer runs.
            db.execute_batch("PRAGMA ignore_check_constraints=ON")?;
            let injected = db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, raw],
            );
            db.execute_batch("PRAGMA ignore_check_constraints=OFF")?;
            injected?;
            Ok(())
        })
        .await
        .expect("damage only active index bytes");
    cancel_operation(&owner.store, &manager, &operation_a).await;
    cancel_operation(&owner.store, &manager, &operation_b).await;

    let result = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("isolate malformed active index");
    assert_eq!(result["processed"], 2);
    assert_eq!(result["quarantined"], 1);
    assert_eq!(result["matched"], 1);
    assert_eq!(result["stale"], 0);
    assert_eq!(result["model_wake"], false);
    assert_eq!(result["native_work_queued"], false);
    assert_eq!(record_raw(&owner.store, &damaged_watch).await, retained_raw);
    assert_eq!(active_index_count(&owner.store, &damaged_watch).await, 0);
    assert_eq!(active_index_count(&owner.store, &healthy_watch).await, 0);
    let keys = quarantine_keys(&owner.store).await;
    assert_eq!(keys.len(), 1);
    let evidence = quarantine_record(&owner.store, &keys[0]).await;
    assert_eq!(evidence["code"], "WATCH_ACTIVE_INDEX_DAMAGED");
    assert_eq!(
        evidence["evidence"]["source_digest"],
        model::digest(raw_index.as_bytes())
    );
    assert_eq!(
        evidence["evidence"]["source_pointer"],
        format!(
            "meta/key-sha256:{}",
            model::digest(active_key_for(&damaged_watch).as_bytes()),
        )
    );
    let retry = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("retry disposed subject");
    assert_eq!(retry["processed"], 0);
    assert_eq!(
        quarantine_record(&owner.store, &keys[0]).await["occurrences"],
        1
    );
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_invalid_active_json_isolated_from_healthy_neighbor() {
    malformed_index_neighbor_case("{invalid").await;
}

#[tokio::test]
async fn reg23_watch_missing_active_watch_id_isolated_from_healthy_neighbor() {
    malformed_index_neighbor_case("{}").await;
}

#[tokio::test]
async fn reg23_watch_changed_active_watch_id_does_not_lookup_other_record() {
    malformed_index_neighbor_case(r#"{"watch_id":"another-watch"}"#).await;
}

#[tokio::test]
async fn reg23_watch_changed_retained_source_aborts_quarantine_retirement() {
    let (owner, directory, _operator, manager) = store_fixture("watch-source-cas").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "source-cas").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let snapshot = damage_scope_revision(&owner.store, &watch_id).await;
    let key = active_key_for(&watch_id);
    let retained_key = record_key_for(&watch_id);
    let error = owner
        .store
        .run(move |db| {
            let tx = db.transaction()?;
            let raw: String =
                tx.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            let disposition = reconcile_watch_subject(&tx, &key, &raw, model::now_ms()?)?;
            let WatchSubjectDisposition::Quarantined {
                code,
                evidence,
                source,
            } = disposition
            else {
                panic!("damaged source must be classified");
            };
            set_meta(&tx, &retained_key, &json!({"different":"source bytes"}))?;
            retire_quarantined_watch(&tx, &key, &raw, &source, &evidence, &code, model::now_ms()?)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .expect_err("changed source cannot be retired with old evidence");
    assert_eq!(error.code, "WATCH_SOURCE_CONFLICT");
    assert_eq!(record_raw(&owner.store, &watch_id).await, snapshot);
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    assert!(quarantine_keys(&owner.store).await.is_empty());
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

async fn quarantine_write_rollback_case(cursor_fault: bool) {
    let (owner, directory, _operator, manager) = store_fixture("watch-quarantine-rollback").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_a) =
        admit_script_run(&owner.store, &manager, &attempt_id, "rollback-a").await;
    let (_, operation_b) =
        admit_script_run(&owner.store, &manager, &attempt_id, "rollback-b").await;
    let watch_a =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_a).await;
    let watch_b =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_b).await;
    let damaged_raw = damage_scope_revision(&owner.store, &watch_a).await;
    let healthy_raw = record_raw(&owner.store, &watch_b).await;
    cancel_operation(&owner.store, &manager, &operation_a).await;
    cancel_operation(&owner.store, &manager, &operation_b).await;
    if cursor_fault {
        install_cursor_write_fault(&owner.store).await;
    } else {
        owner
            .store
            .run(|db| {
                db.execute_batch(
                    r#"
                CREATE TEMP TRIGGER watch_quarantine_write_fault
                BEFORE INSERT ON main.meta
                WHEN NEW.key LIKE 'coordination:watch:v1:quarantine:%'
                BEGIN SELECT RAISE(ABORT, 'watch quarantine write fault'); END;
            "#,
                )?;
                Ok(())
            })
            .await
            .expect("install evidence write fault");
    }
    let error = owner
        .store
        .reconcile_watches_once()
        .await
        .expect_err("write failure stays hard");
    assert_eq!(error.code, "STORE_ERROR");
    assert_eq!(record_raw(&owner.store, &watch_a).await, damaged_raw);
    assert_eq!(record_raw(&owner.store, &watch_b).await, healthy_raw);
    assert_eq!(active_index_count(&owner.store, &watch_a).await, 1);
    assert_eq!(active_index_count(&owner.store, &watch_b).await, 1);
    assert!(quarantine_keys(&owner.store).await.is_empty());
    assert!(
        owner
            .store
            .run(|db| meta(db, RECONCILE_CURSOR_KEY))
            .await
            .expect("cursor read")
            .is_none()
    );
    if cursor_fault {
        remove_cursor_write_fault(&owner.store).await;
    } else {
        owner
            .store
            .run(|db| {
                db.execute_batch("DROP TRIGGER temp.watch_quarantine_write_fault")?;
                Ok(())
            })
            .await
            .expect("remove evidence fault");
    }
    let retry = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("retry exact subjects");
    assert_eq!(retry["quarantined"], 1);
    assert_eq!(retry["matched"], 1);
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_quarantine_evidence_write_failure_rolls_back_healthy_neighbor() {
    quarantine_write_rollback_case(false).await;
}

#[tokio::test]
async fn reg23_watch_quarantine_cursor_write_failure_rolls_back_healthy_neighbor() {
    quarantine_write_rollback_case(true).await;
}

async fn append_pending_page(store: &Store, watch_id: &str, page: usize) {
    let original: Value =
        serde_json::from_str(&record_raw(store, watch_id).await).expect("template record");
    store
        .run(move |db| {
            let tx = db.transaction()?;
            // Fixed 33-row influx exceeds the public 32-row tick budget.
            // Influx rows use canonical server-shaped UUIDv4 ids that sort
            // after a random watch id, so the forward page stays full while
            // the retained cursor remains a valid ordering boundary.
            for index in 0..33 {
                let influx_n = page as u32 * 33 + index as u32;
                let id = format!("ffffffff-ffff-4fff-bfff-{influx_n:012x}");
                let mut record = original.clone();
                record["watch_id"] = json!(id);
                set_meta(&tx, &record_key(&id), &record)?;
                set_meta(&tx, &active_key(&id), &json!({"watch_id":id}))?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .expect("retain bounded influx fixture");
}

#[tokio::test]
async fn reg23_watch_pending_revisited_under_full_page_influx_without_stale_or_effects() {
    let (owner, directory, _operator, manager) = store_fixture("watch-pending-influx").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "pending-influx").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let pending = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("observe pending watch");
    assert_eq!(pending["processed"], 1);
    for field in ["matched", "expired", "stale", "quarantined"] {
        assert_eq!(pending[field], 0);
    }
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    append_pending_page(&owner.store, &watch_id, 0).await;
    let pending_again = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("full pending page");
    assert_eq!(pending_again["stale"], 0);
    assert_eq!(pending_again["quarantined"], 0);
    assert_eq!(pending_again["model_wake"], false);
    assert_eq!(pending_again["native_work_queued"], false);
    append_pending_page(&owner.store, &watch_id, 1).await;
    cancel_operation(&owner.store, &manager, &operation_id).await;
    // Oldest queued pending watch is reached within the next two ticks even
    // though fresh later keys continuously keep the forward page full.
    owner
        .store
        .reconcile_watches_once()
        .await
        .expect("first revisit tick");
    append_pending_page(&owner.store, &watch_id, 2).await;
    owner
        .store
        .reconcile_watches_once()
        .await
        .expect("second revisit tick");
    let record: Value =
        serde_json::from_str(&record_raw(&owner.store, &watch_id).await).expect("watch receipt");
    assert_eq!(record["state"], "matched");
    assert_eq!(record["cursor"]["operation_id"], operation_id);
    assert!(quarantine_keys(&owner.store).await.is_empty());
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_pending_expiry_revisited_under_full_page_influx() {
    let (owner, directory, _operator, manager) = store_fixture("watch-expiry-influx").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "expiry-influx").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let record: Value =
        serde_json::from_str(&record_raw(&owner.store, &watch_id).await).expect("watch record");
    let expires = record["expires_at_ms"].as_i64().expect("expiry");
    owner
        .store
        .reconcile_watches_once()
        .await
        .expect("queue pending subject");
    append_pending_page(&owner.store, &watch_id, 0).await;
    let receipt = owner
        .store
        .run(move |db| {
            let tx = db.transaction()?;
            let receipt = reconcile(&tx, 32, expires)?;
            tx.commit()?;
            Ok(receipt)
        })
        .await
        .expect("exact expiry tick through Store transaction");
    assert!(receipt["expired"].as_u64().is_some_and(|count| count > 0));
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 0);
    let record: Value =
        serde_json::from_str(&record_raw(&owner.store, &watch_id).await).expect("expired receipt");
    assert_eq!(record["state"], "expired");
    assert!(record["notification"].is_null());
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_malformed_global_cursor_remains_hard() {
    let (owner, directory, _operator, _manager) = store_fixture("watch-global-cursor").await;
    owner
        .store
        .run(|db| set_meta(db, RECONCILE_CURSOR_KEY, &json!({"invalid":"cursor"})))
        .await
        .expect("damage global cursor");
    let error = owner
        .store
        .reconcile_watches_once()
        .await
        .expect_err("global cursor must stay hard");
    assert_eq!(error.code, "WATCH_CURSOR_DAMAGED");
    assert!(quarantine_keys(&owner.store).await.is_empty());
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

fn hex_component(value: &str) -> String {
    value
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

async fn meta_snapshot(store: &Store) -> Vec<(String, String)> {
    store
        .run(|db| {
            let mut statement = db.prepare("SELECT key, value_json FROM meta ORDER BY key")?;
            let rows = statement
                .query_map(params![], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<Vec<(String, String)>>>()?;
            Ok(rows)
        })
        .await
        .expect("snapshot retained meta")
}

async fn active_raw(store: &Store, watch_id: &str) -> String {
    let key = active_key_for(watch_id);
    store
        .run(move |db| {
            let raw = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [key],
                |row| row.get::<_, String>(0),
            )?;
            Ok(raw)
        })
        .await
        .expect("read active watch index")
}

async fn cursor_damage_zero_writes_case(label: &str, cursor: Value) {
    let (owner, directory, _operator, manager) = store_fixture(label).await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "cursor-damage").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let retained_record = record_raw(&owner.store, &watch_id).await;
    owner
        .store
        .run(move |db| {
            set_meta(db, RECONCILE_CURSOR_KEY, &cursor)?;
            Ok(())
        })
        .await
        .expect("retain damaged cursor");
    let before = meta_snapshot(&owner.store).await;
    let error = owner
        .store
        .reconcile_watches_once()
        .await
        .expect_err("damaged cursor must stay hard");
    assert_eq!(error.code, "WATCH_CURSOR_DAMAGED");
    assert_eq!(meta_snapshot(&owner.store).await, before);
    assert_eq!(record_raw(&owner.store, &watch_id).await, retained_record);
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    assert!(quarantine_keys(&owner.store).await.is_empty());
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_null_cursor_hard_without_subject_writes() {
    cursor_damage_zero_writes_case("watch-cursor-null", json!(null)).await;
}

#[tokio::test]
async fn reg23_watch_wrong_prefix_cursor_hard_without_subject_writes() {
    cursor_damage_zero_writes_case(
        "watch-cursor-prefix",
        json!("coordination:watch:v1:record:deadbeef"),
    )
    .await;
}

#[tokio::test]
async fn reg23_watch_odd_cursor_hard_without_subject_writes() {
    cursor_damage_zero_writes_case(
        "watch-cursor-odd",
        json!("coordination:watch:v1:active:abc"),
    )
    .await;
}

#[tokio::test]
async fn reg23_watch_non_uuid_cursor_hard_without_subject_writes() {
    let cursor = format!(
        "coordination:watch:v1:active:{}",
        hex_component("another-watch")
    );
    cursor_damage_zero_writes_case("watch-cursor-non-uuid", json!(cursor)).await;
}

#[tokio::test]
async fn reg23_watch_uppercase_cursor_hard_without_subject_writes() {
    let cursor = format!(
        "coordination:watch:v1:active:{}",
        hex_component("another-watch").to_ascii_uppercase()
    );
    cursor_damage_zero_writes_case("watch-cursor-uppercase", json!(cursor)).await;
}

#[tokio::test]
async fn reg23_watch_canonical_removed_row_cursor_allowed() {
    let (owner, directory, _operator, manager) = store_fixture("watch-cursor-removed").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "removed-cursor").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    cancel_operation(&owner.store, &manager, &operation_id).await;
    let matched = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("match watch");
    assert_eq!(matched["matched"], 1);
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 0);
    // The retained cursor now points at a removed canonical row. It stays a
    // valid ordering boundary and the empty retry converges.
    let retry = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("removed-row cursor stays valid");
    assert_eq!(retry["processed"], 0);
    assert_eq!(retry["matched"], 0);
    assert_eq!(retry["quarantined"], 0);
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_active_key_binds_payload_protects_canonical_neighbor() {
    let (owner, directory, _operator, manager) = store_fixture("watch-key-binding").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_a) =
        admit_script_run(&owner.store, &manager, &attempt_id, "binding-a").await;
    let (_, operation_b) =
        admit_script_run(&owner.store, &manager, &attempt_id, "binding-b").await;
    let watch_a =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_a).await;
    let watch_b =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_b).await;
    let (selected, neighbor) = if active_key_for(&watch_a) < active_key_for(&watch_b) {
        (watch_a, watch_b)
    } else {
        (watch_b, watch_a)
    };
    let neighbor_record_before = record_raw(&owner.store, &neighbor).await;
    let selected_key = active_key_for(&selected);
    let neighbor_id = neighbor.clone();
    owner
        .store
        .run(move |db| {
            set_meta(db, &selected_key, &json!({"watch_id": neighbor_id}))?;
            Ok(())
        })
        .await
        .expect("point selected index at neighbor payload");
    let injected_raw = active_raw(&owner.store, &selected).await;
    let receipt = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("bind selected key before lookup");
    assert_eq!(receipt["processed"], 2);
    assert_eq!(receipt["quarantined"], 1);
    assert_eq!(
        record_raw(&owner.store, &neighbor).await,
        neighbor_record_before
    );
    let neighbor_index: Value = serde_json::from_str(&active_raw(&owner.store, &neighbor).await)
        .expect("neighbor index stays valid JSON");
    assert_eq!(neighbor_index["watch_id"].as_str(), Some(neighbor.as_str()));
    assert_eq!(active_index_count(&owner.store, &selected).await, 0);
    assert_eq!(active_index_count(&owner.store, &neighbor).await, 1);
    let keys = quarantine_keys(&owner.store).await;
    assert_eq!(keys.len(), 1);
    let evidence = quarantine_record(&owner.store, &keys[0]).await;
    assert_eq!(evidence["code"], "WATCH_ACTIVE_INDEX_DAMAGED");
    assert_eq!(
        evidence["evidence"]["source_digest"],
        model::digest(injected_raw.as_bytes())
    );
    assert_eq!(
        evidence["evidence"]["source_pointer"],
        format!(
            "meta/key-sha256:{}",
            model::digest(active_key_for(&selected).as_bytes())
        )
    );
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_singleton_pending_reconciles_without_metadata_churn() {
    let (owner, directory, _operator, manager) = store_fixture("watch-singleton-no-churn").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "singleton").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let first = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("observe pending");
    assert_eq!(first["processed"], 1);
    for field in ["matched", "expired", "stale", "quarantined"] {
        assert_eq!(first[field], 0);
    }
    let baseline = meta_snapshot(&owner.store).await;
    assert!(!baseline.is_empty());
    for _ in 0..2 {
        let retry = owner
            .store
            .reconcile_watches_once()
            .await
            .expect("singleton pending stays read-only");
        assert_eq!(retry["processed"], 1);
        for field in ["matched", "expired", "stale", "quarantined"] {
            assert_eq!(retry[field], 0);
        }
        assert_eq!(retry["model_wake"], false);
        assert_eq!(retry["native_work_queued"], false);
        assert_eq!(meta_snapshot(&owner.store).await, baseline);
    }
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    assert!(quarantine_keys(&owner.store).await.is_empty());
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}
async fn inject_raw_meta(store: &Store, key: String, raw: String) {
    store
        .run(move |db| {
            db.execute_batch("PRAGMA ignore_check_constraints=ON")?;
            let injected = db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![key, raw],
            );
            db.execute_batch("PRAGMA ignore_check_constraints=OFF")?;
            injected?;
            Ok(())
        })
        .await
        .expect("inject raw retained bytes");
}

async fn inject_bad_selected_page(store: &Store) {
    store
        .run(|db| {
            let tx = db.transaction()?;
            for index in 0..31 {
                let id = format!("zz-bad-{index:02}");
                let key = format!("{ACTIVE_PREFIX}{}", hex_component(&id));
                set_meta(&tx, &key, &json!({"watch_id": id}))?;
            }
            tx.commit()?;
            Ok(())
        })
        .await
        .expect("retain noncanonical selected page");
}

async fn slot_count(store: &Store, key: &str) -> i64 {
    let key = key.to_owned();
    store
        .run(move |db| {
            let count: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key=?1",
                [key],
                |row| row.get(0),
            )?;
            Ok(count)
        })
        .await
        .expect("count slot locator")
}

async fn slot_raw(store: &Store, key: &str) -> String {
    let key = key.to_owned();
    store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [key],
                |row| row.get(0),
            )?;
            Ok(raw)
        })
        .await
        .expect("read slot bytes")
}

#[tokio::test]
async fn reg23_watch_noncanonical_selected_key_never_persists_as_cursor() {
    let (owner, directory, _operator, manager) = store_fixture("watch-bad-selected").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "bad-selected").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let canonical = active_key_for(&watch_id);
    let first = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("observe pending");
    assert_eq!(first["processed"], 1);
    assert_eq!(first["cursor"], json!(canonical));
    inject_bad_selected_page(&owner.store).await;
    let second = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("retire bad selected page");
    assert_eq!(second["processed"], 32);
    assert_eq!(second["stale"], 31);
    assert_eq!(second["quarantined"], 0);
    assert_eq!(second["cursor"], json!(canonical));
    let stored = owner
        .store
        .run(|db| meta(db, RECONCILE_CURSOR_KEY))
        .await
        .expect("read cursor after bad selection")
        .expect("cursor retained");
    assert_eq!(stored, json!(canonical));
    let third = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("next pass stays valid");
    assert_eq!(third["processed"], 1);
    assert_eq!(third["stale"], 0);
    assert_eq!(third["cursor"], json!(canonical));
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}
#[tokio::test]
async fn reg23_watch_wrong_variant_cursor_hard_without_subject_writes() {
    // Version nibble 4 with NCS variant bits: only the variant gate rejects.
    let suffix = hex_component("123e4567-e89b-42d3-0232-426614174000");
    assert_eq!(suffix.len(), 72);
    cursor_damage_zero_writes_case(
        "watch-cursor-variant",
        json!(format!("coordination:watch:v1:active:{suffix}")),
    )
    .await;
}

#[tokio::test]
async fn reg23_watch_oversized_cursor_hard_without_subject_writes() {
    let oversized = format!("{}00", active_key_for(&model::new_id()));
    cursor_damage_zero_writes_case("watch-cursor-oversized", json!(oversized)).await;
}

#[tokio::test]
async fn reg23_watch_damaged_revisit_slot_quarantined_neighbors_progress() {
    let (owner, directory, _operator, manager) = store_fixture("watch-poison-slot").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "poison-slot").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let first = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("observe pending");
    assert_eq!(first["processed"], 1);
    let retained_record = record_raw(&owner.store, &watch_id).await;
    let retained_index = active_raw(&owner.store, &watch_id).await;
    let slot_key = format!(
        "coordination:watch:v1:revisit:00000000000000000000:{}",
        hex_component("zz-poison")
    );
    let slot_raw_value = "{corrupt revisit slot".to_owned();
    inject_raw_meta(&owner.store, slot_key.clone(), slot_raw_value.clone()).await;
    let second = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("quarantine poison slot");
    assert_eq!(second["processed"], 1);
    assert_eq!(second["quarantined"], 1);
    assert_eq!(second["matched"], 0);
    assert_eq!(record_raw(&owner.store, &watch_id).await, retained_record);
    assert_eq!(active_raw(&owner.store, &watch_id).await, retained_index);
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    assert_eq!(slot_count(&owner.store, &slot_key).await, 0);
    let keys = quarantine_keys(&owner.store).await;
    assert_eq!(keys.len(), 1);
    let evidence = quarantine_record(&owner.store, &keys[0]).await;
    assert_eq!(evidence["code"], "WATCH_REVISIT_SLOT_DAMAGED");
    assert_eq!(
        evidence["evidence"]["source_digest"],
        model::digest(slot_raw_value.as_bytes())
    );
    assert_eq!(
        evidence["evidence"]["source_pointer"],
        format!(
            "meta/key-sha256:{}",
            model::digest(slot_key.as_bytes())
        )
    );
    assert!(evidence["evidence"]["subject_identity"]
        .as_str()
        .is_some_and(|value| value.starts_with("watch-revisit-key-sha256:")));
    let third = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("converged retry");
    assert_eq!(third["processed"], 1);
    assert_eq!(third["quarantined"], 0);
    assert_eq!(quarantine_keys(&owner.store).await, keys);
    assert_eq!(
        quarantine_record(&owner.store, &keys[0]).await["occurrences"],
        1
    );
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}
#[tokio::test]
async fn reg23_watch_stale_revisit_pointer_quarantined_watch_intact() {
    let (owner, directory, _operator, manager) = store_fixture("watch-stale-pointer").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "stale-pointer").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let first = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("observe pending");
    assert_eq!(first["processed"], 1);
    let retained_record = record_raw(&owner.store, &watch_id).await;
    let retained_index = active_raw(&owner.store, &watch_id).await;
    // A well-formed slot whose locator binds a foreign component while its
    // pointer names the healthy watch: only the slot is damage.
    let slot_key = format!(
        "coordination:watch:v1:revisit:00000000000000000000:{}",
        hex_component("zz-other")
    );
    let poisoning = active_key_for(&watch_id);
    owner
        .store
        .run(move |db| {
            set_meta(db, &slot_key, &json!({"active_key": poisoning}))?;
            Ok(())
        })
        .await
        .expect("inject stale pointer slot");
    let injected_raw = slot_raw(&owner.store, &slot_key).await;
    let second = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("quarantine stale pointer");
    assert_eq!(second["processed"], 1);
    assert_eq!(second["quarantined"], 1);
    assert_eq!(record_raw(&owner.store, &watch_id).await, retained_record);
    assert_eq!(active_raw(&owner.store, &watch_id).await, retained_index);
    assert_eq!(active_index_count(&owner.store, &watch_id).await, 1);
    assert_eq!(slot_count(&owner.store, &slot_key).await, 0);
    let keys = quarantine_keys(&owner.store).await;
    assert_eq!(keys.len(), 1);
    let evidence = quarantine_record(&owner.store, &keys[0]).await;
    assert_eq!(evidence["code"], "WATCH_REVISIT_SLOT_DAMAGED");
    assert_eq!(
        evidence["evidence"]["source_digest"],
        model::digest(injected_raw.as_bytes())
    );
    assert_eq!(
        evidence["evidence"]["source_pointer"],
        format!(
            "meta/key-sha256:{}",
            model::digest(slot_key.as_bytes())
        )
    );
    let third = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("converged retry");
    assert_eq!(third["processed"], 1);
    assert_eq!(third["quarantined"], 0);
    assert_eq!(
        quarantine_record(&owner.store, &keys[0]).await["occurrences"],
        1
    );
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}

#[tokio::test]
async fn reg23_watch_slot_quarantine_write_failure_rolls_back() {
    let (owner, directory, _operator, manager) = store_fixture("watch-slot-rollback").await;
    register_active_script(&owner.store, &manager).await;
    let (task_id, attempt_id) = create_current_task_attempt(&owner.store, &manager).await;
    let (_, operation_id) =
        admit_script_run(&owner.store, &manager, &attempt_id, "slot-rollback").await;
    let watch_id =
        create_operation_watch(&owner.store, &manager, &task_id, &attempt_id, &operation_id).await;
    let first = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("observe pending");
    assert_eq!(first["processed"], 1);
    let retained_record = record_raw(&owner.store, &watch_id).await;
    let retained_index = active_raw(&owner.store, &watch_id).await;
    let slot_key = format!(
        "coordination:watch:v1:revisit:00000000000000000000:{}",
        hex_component("zz-poison")
    );
    inject_raw_meta(&owner.store, slot_key.clone(), "{corrupt revisit slot".to_owned()).await;
    owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TEMP TRIGGER watch_slot_quarantine_fault BEFORE INSERT ON main.meta WHEN NEW.key LIKE 'coordination:watch:v1:quarantine:%' BEGIN SELECT RAISE(ABORT, 'watch slot quarantine fault'); END;",
            )?;
            Ok(())
        })
        .await
        .expect("install slot evidence fault");
    let error = owner
        .store
        .reconcile_watches_once()
        .await
        .expect_err("slot evidence failure stays hard");
    assert_eq!(error.code.as_str(), "STORE_ERROR");
    assert_eq!(slot_count(&owner.store, &slot_key).await, 1);
    assert_eq!(record_raw(&owner.store, &watch_id).await, retained_record);
    assert_eq!(active_raw(&owner.store, &watch_id).await, retained_index);
    assert!(quarantine_keys(&owner.store).await.is_empty());
    owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER temp.watch_slot_quarantine_fault")?;
            Ok(())
        })
        .await
        .expect("remove slot evidence fault");
    let retry = owner
        .store
        .reconcile_watches_once()
        .await
        .expect("retry quarantines poison slot");
    assert_eq!(retry["processed"], 1);
    assert_eq!(retry["quarantined"], 1);
    owner.close().await.expect("close Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store");
}



