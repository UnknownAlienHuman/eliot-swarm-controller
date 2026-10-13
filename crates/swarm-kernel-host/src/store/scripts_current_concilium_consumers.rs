//! Current-scope producer-to-consumer coverage for retained Concilium events.

use super::automation_dispatch;
use crate::{
    config::Config,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner},
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use rusqlite::TransactionBehavior;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc};

const PROJECT_ID: &str = "high0146-concilium-project";
const AUTOMATION_ID: &str = "high0146-concilium-script";
const SCRIPT_ID: &str = "high0146_concilium_script";

struct SourceOperationSnapshot {
    method: String,
    state: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    observation_count: i64,
}

struct ScriptRunSnapshot {
    count: i64,
    started: i64,
    task_id: Option<String>,
    task_revision: Option<i64>,
    attempt_id: Option<String>,
    request_json: String,
}

async fn start_store() -> (StoreOwner, PathBuf, Principal, Config) {
    let directory =
        std::env::temp_dir().join(format!("swarm-high0146-concilium-{}", model::new_id()));
    std::fs::create_dir_all(&directory).expect("create temporary Store directory");
    let root = DataRoot::acquire(&directory).expect("acquire temporary Store root");
    let credential = bootstrap_credential(&root.path).expect("create Operator credential");
    let mut config = Config::default();
    config.storage.data_dir = directory.clone();
    config.scripts.executor =
        Some(crate::store::o6_taskless_path_fixture::store_only_script_executor_pin());
    let owner = StoreOwner::start(root, Arc::new(config.clone()), credential.clone())
        .await
        .expect("start Store");
    let operator = owner
        .store
        .authenticate(credential)
        .await
        .expect("authenticate Operator");
    (owner, directory, operator, config)
}

async fn register_manager(store: &Store, operator: &Principal) -> Principal {
    let client_id = format!("high0146-manager-{}", model::new_id());
    let token = format!("high0146-manager-token-{}", model::new_id());
    store
        .call(
            operator.clone(),
            "client.register".into(),
            json!({
                "client_request_id":format!("register-{client_id}"),
                "client_id":client_id,
                "role":"manager",
                "token_hash":model::digest(token.as_bytes()),
            }),
        )
        .await
        .expect("register Manager through Store");
    store
        .authenticate(Credential { client_id, token })
        .await
        .expect("authenticate Manager")
}

async fn register_participant(
    store: &Store,
    manager: &Principal,
    client_id: String,
    task_id: &str,
    attempt_id: &str,
) -> Principal {
    let token = format!("high0146-participant-token-{}", model::new_id());
    store
        .call(
            manager.clone(),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":format!("register-{client_id}"),
                "client_id":client_id,
                "token_hash":model::digest(token.as_bytes()),
                "task_id":task_id,
                "task_revision":1,
                "attempt_id":attempt_id,
                "participation_basis":{
                    "kind":"attempt_owner",
                    "assignment_id":null,
                    "review_scope":null,
                },
            }),
        )
        .await
        .expect("register Participant for the exact current Attempt");
    store
        .authenticate(Credential { client_id, token })
        .await
        .expect("authenticate Participant")
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
        .expect("pwsh is needed only to validate the retained script bundle")
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
                "client_request_id":format!("register-{SCRIPT_ID}"),
                "bundle":script_bundle(SCRIPT_ID),
            }),
        )
        .await
        .expect("register immutable test script bundle");
    store
        .call(
            manager.clone(),
            "script.activate".into(),
            json!({
                "client_request_id":format!("activate-{SCRIPT_ID}"),
                "script_id":SCRIPT_ID,
                "revision":1,
            }),
        )
        .await
        .expect("activate test script revision");
}

async fn configure_trigger(store: &Store, manager: &Principal) {
    let changes = json!([{
        "automation_id":AUTOMATION_ID,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "steps":["script_run"],
            "script_run":{"script_id":SCRIPT_ID},
            "event_rules":[{
                "source_id":"controller",
                "event_kind":"concilium.propose",
                "action":"script_run",
            }],
        },
    }]);
    let preview = store
        .call(
            manager.clone(),
            "automation.config.preview".into(),
            json!({"project_id":PROJECT_ID,"changes":changes.clone()}),
        )
        .await
        .expect("preview Concilium ScriptRun rule");
    assert_eq!(preview["valid"], true, "{preview}");
    store
        .call(
            manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":format!("apply-{AUTOMATION_ID}"),
                "project_id":PROJECT_ID,
                "changes":changes,
                "preview_digest":preview["plan_sha256"],
            }),
        )
        .await
        .expect("apply Concilium ScriptRun rule");
}

async fn script_run_counts(store: &Store) -> (i64, i64) {
    store
        .run_fixture(|db| {
            Ok(db.query_row(
                "SELECT COUNT(*),COUNT(started_at_ms) FROM script_runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?)
        })
        .await
        .expect("read ScriptRun counts")
}

#[tokio::test]
async fn public_concilium_proposal_admits_current_scoped_script_run_once() {
    let (owner, directory, operator, dispatch_config) = start_store().await;
    let manager = register_manager(&owner.store, &operator).await;

    let task = owner
        .store
        .call(
            manager.clone(),
            "task.create".into(),
            json!({
                "client_request_id":format!("create-task-{}", model::new_id()),
                "project_id":PROJECT_ID,
                "spec":{
                    "objective":"Exercise current-scope Concilium ScriptRun admission",
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Retain one exact source Task and Attempt"}],
                    "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                },
            }),
        )
        .await
        .expect("create Task through public Store method");
    let task_id = task["task_id"]
        .as_str()
        .expect("Task receipt has an id")
        .to_owned();
    let claim = owner
        .store
        .call(
            manager.clone(),
            "task.claim".into(),
            json!({
                "client_request_id":format!("claim-task-{}", model::new_id()),
                "task_id":task_id,
                "expected_revision":1,
                "owner_id":manager.client_id,
            }),
        )
        .await
        .expect("create current Attempt through public Task claim");
    let task_id = task["task_id"].as_str().unwrap().to_owned();
    let attempt_id = claim["attempt_id"]
        .as_str()
        .expect("Task claim receipt has an Attempt id")
        .to_owned();

    let task_readback = owner
        .store
        .call(
            manager.clone(),
            "task.get".into(),
            json!({"task_id":task_id}),
        )
        .await
        .expect("read current Task before Participant registration");
    let attempt_readback = owner
        .store
        .call(
            manager.clone(),
            "attempt.get".into(),
            json!({"attempt_id":attempt_id}),
        )
        .await
        .expect("read current Attempt before Participant registration");
    assert_eq!(task_readback["task_id"], task_id);
    assert_eq!(task_readback["project_id"], PROJECT_ID);
    assert_eq!(task_readback["revision"], 1);
    assert_eq!(task_readback["current_attempt_id"], attempt_id);
    assert_eq!(attempt_readback["attempt_id"], attempt_id);
    assert_eq!(attempt_readback["task_id"], task_id);
    assert_eq!(attempt_readback["task_revision"], task_readback["revision"]);
    assert_eq!(attempt_readback["owner_id"], manager.client_id);
    assert!(attempt_readback["released_at_ms"].is_null());

    let proposer_id = format!("high0146-proposer-{}", model::new_id());
    let invitee_id = format!("high0146-invitee-{}", model::new_id());
    let proposer =
        register_participant(&owner.store, &manager, proposer_id, &task_id, &attempt_id).await;
    let invitee = register_participant(
        &owner.store,
        &manager,
        invitee_id.clone(),
        &task_id,
        &attempt_id,
    )
    .await;

    register_active_script(&owner.store, &manager).await;
    configure_trigger(&owner.store, &manager).await;

    let proposal_request_id = format!("high0146-proposal-{}", model::new_id());
    let proposal = owner
        .store
        .call(
            proposer,
            "concilium.propose".into(),
            json!({
                "client_request_id":proposal_request_id,
                "task_id":task_id,
                "attempt_id":attempt_id,
                "failed_thread_id":format!("thread-{AUTOMATION_ID}"),
                "decision_question":"Does the retained event preserve the exact current source scope?",
                "material_conflict":"The producer receipt and ScriptRun consumer must agree on one Task revision and Attempt.",
                "participants":[{"client_id":invitee.client_id,"reason":"independent scoped participant"}],
                "proposal_revision_ids":[],
                "evidence_refs":[],
                "expected_output":"A bounded source-scope recommendation.",
                "suggested_max_rounds":1,
                "suggested_budget":{"model_calls":0,"tokens":0},
                "close_condition":"The retained source identities are verified.",
            }),
        )
        .await
        .expect("write Concilium proposal through public Store producer");
    let source_operation_id = proposal["operation_id"]
        .as_str()
        .expect("Concilium receipt identifies its Operation")
        .to_owned();

    let inspect_operation_id = source_operation_id.clone();
    let source = owner
        .store
        .run_fixture(move |db| {
            let (method, state, task_id, attempt_id) = db.query_row(
                "SELECT method,state,task_id,attempt_id FROM operations WHERE operation_id=?1",
                [&inspect_operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            let observation_count = db.query_row(
                "SELECT COUNT(*) FROM observations WHERE source_stream_id='controller' \
                 AND source_event_key=?1 AND operation_id=?1 AND kind='concilium.propose'",
                [&inspect_operation_id],
                |row| row.get(0),
            )?;
            Ok(SourceOperationSnapshot {
                method,
                state,
                task_id,
                attempt_id,
                observation_count,
            })
        })
        .await
        .expect("read public Concilium Operation and Observation facts");
    assert_eq!(source.method, "concilium.propose");
    assert_eq!(source.state, "settled");
    assert_eq!(source.task_id.as_deref(), Some(task_id.as_str()));
    assert_eq!(source.attempt_id.as_deref(), Some(attempt_id.as_str()));
    assert_eq!(source.observation_count, 1);

    // Split the same production source and consumer stages so corrupted in-memory
    // copies can be rejected before the real durable pending intent is admitted.
    let dispatch_config = dispatch_config.clone();
    let _source_reconciliation = owner
        .store
        .run_fixture(move |db| {
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let result =
                automation_dispatch::reconcile(&tx, &dispatch_config, 16, 64, model::now_ms()?)?;
            tx.commit()?;
            Ok(result)
        })
        .await
        .expect("stage real Concilium event through production reconciliation");
    let pending = owner
        .store
        .run_fixture(|db| automation_dispatch::pending_script_triggers(db, 16))
        .await
        .expect("read the real pending ScriptRun intent");
    let mut selected = pending
        .into_iter()
        .filter(|intent| intent.automation_id == AUTOMATION_ID)
        .collect::<Vec<_>>();
    assert_eq!(selected.len(), 1, "the public proposal stages one intent");
    let intent = selected.pop().unwrap();
    assert_eq!(intent.owner_manager_id, manager.client_id);
    assert_eq!(intent.project_id, PROJECT_ID);
    assert_eq!(intent.script_id, SCRIPT_ID);
    assert_eq!(intent.cause["kind"], "system_event");
    assert_eq!(intent.cause["operation_id"], source_operation_id);
    assert_eq!(intent.cause["task_id"], task_id);
    assert_eq!(intent.cause["task_revision"], 1);
    assert_eq!(intent.cause["attempt_id"], attempt_id);

    for (field, changed) in [
        ("operation_id", json!("foreign-high0146-operation")),
        ("task_id", json!("foreign-high0146-task")),
        ("task_revision", json!(2)),
        ("attempt_id", json!("foreign-high0146-attempt")),
    ] {
        let mut altered = intent.clone();
        altered.cause[field] = changed;
        let failure = owner
            .store
            .admit_script_trigger(altered)
            .await
            .expect_err(field);
        assert!(
            matches!(
                failure.error.code.as_str(),
                "SCRIPT_EVENT_SOURCE_UNAUTHORIZED"
                    | "AUTOMATION_LINK_CORRUPT"
                    | "SCRIPT_SCOPE_CHANGED"
            ),
            "{field}: {}",
            failure.error.code
        );
        assert_eq!(
            script_run_counts(&owner.store).await,
            (0, 0),
            "changing {field} cannot create or start a ScriptRun"
        );
    }

    let reconciled = owner
        .store
        .reconcile_script_triggers_once(16)
        .await
        .expect("admit the original pending intent through the production Store consumer");
    assert_eq!(reconciled["considered"], 1, "{reconciled}");
    assert_eq!(reconciled["outcomes"][0]["state"], "admitted");
    assert_eq!(
        reconciled["outcomes"][0]["submission_ref"],
        intent.cause["id"]
    );
    let script_operation_id = reconciled["outcomes"][0]["operation_id"]
        .as_str()
        .expect("admission receipt identifies the ScriptRun Operation")
        .to_owned();

    let inspect_script_operation_id = script_operation_id.clone();
    let script_run = owner
        .store
        .run_fixture(move |db| {
            let (count, started) = db.query_row(
                "SELECT COUNT(*),COUNT(started_at_ms) FROM script_runs",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let (task_id, task_revision, attempt_id, request_json) = db.query_row(
                "SELECT script_run.task_id,script_run.task_revision,script_run.attempt_id, \
                 operation.original_request_json FROM script_runs AS script_run \
                 JOIN operations AS operation ON operation.operation_id=script_run.operation_id \
                 WHERE script_run.operation_id=?1",
                [&inspect_script_operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            Ok(ScriptRunSnapshot {
                count,
                started,
                task_id,
                task_revision,
                attempt_id,
                request_json,
            })
        })
        .await
        .expect("read admitted ScriptRun identity without starting its worker");
    assert_eq!((script_run.count, script_run.started), (1, 0));
    assert_eq!(script_run.task_id.as_deref(), Some(task_id.as_str()));
    assert_eq!(script_run.task_revision, Some(1));
    assert_eq!(script_run.attempt_id.as_deref(), Some(attempt_id.as_str()));
    let script_request: Value =
        serde_json::from_str(&script_run.request_json).expect("decode retained ScriptRun request");
    assert_eq!(script_request["attempt_id"], attempt_id);
    assert_eq!(script_request["expected_task_revision"], 1);
    assert_eq!(script_request["input"]["operation_id"], source_operation_id);
    assert_eq!(script_request["input"]["task_id"], task_id);
    assert_eq!(script_request["input"]["task_revision"], 1);
    assert_eq!(script_request["input"]["attempt_id"], attempt_id);

    let retry = owner
        .store
        .admit_script_trigger(intent)
        .await
        .unwrap_or_else(|failure| panic!("exact pending-intent retry: {}", failure.error.code));
    assert_eq!(retry["operation_id"], script_operation_id);
    assert_eq!(script_run_counts(&owner.store).await, (1, 0));

    owner.close().await.expect("close temporary Store");
    std::fs::remove_dir_all(directory).expect("remove temporary Store directory");
}
