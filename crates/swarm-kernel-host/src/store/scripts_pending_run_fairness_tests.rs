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
            advance_pending_run_cursor(db, last)?;
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
