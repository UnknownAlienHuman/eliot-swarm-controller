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
    let serialized_evidence = quarantine.to_string();
    assert!(serialized_evidence.len() <= 1024);
    for plaintext in [&damaged_watch, &task_id, &attempt_id, &damaged_operation] {
        assert!(!serialized_evidence.contains(plaintext));
    }

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
