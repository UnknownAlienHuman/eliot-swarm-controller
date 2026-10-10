use crate::{
    config::{Config, Route},
    error::Error,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    runtime::opencode_v2::tests::Fixture,
    store::{Store, StoreOwner},
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

async fn start(fixture: &Fixture) -> (StoreOwner, Principal) {
    let directory = fixture.dir.join("controller");
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory;
    config.routes.push(Route {
        admission_policy: Default::default(),
        workspace_option: None,
        owned_service: None,
        alias: "fixture".into(),
        runtime: crate::runtime::opencode_v2::RUNTIME.into(),
        module_artifact_id: crate::runtime::opencode_v2::ARTIFACT_ID.into(),
        enabled: true,
        native_options: json!(fixture.options),
    });
    let owner = StoreOwner::start(root, Arc::new(config), credential.clone())
        .await
        .unwrap();
    let principal = owner.store.authenticate(credential).await.unwrap();
    (owner, principal)
}

async fn write(store: &Store, principal: &Principal, method: &str, mut input: Value) -> Value {
    input["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.into(), input)
        .await
        .unwrap()
}

async fn read_operation(store: &Store, principal: &Principal, operation_id: &str) -> Value {
    store
        .call(
            principal.clone(),
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap()
}

async fn register_manager(store: &Store, operator: &Principal) -> Principal {
    let client_id = format!("worker-failure-manager-{}", model::new_id());
    let token = model::new_id();
    write(
        store,
        operator,
        "client.register",
        json!({
            "client_id":client_id,
            "role":"manager",
            "token_hash":model::digest(token.as_bytes())
        }),
    )
    .await;
    store
        .authenticate(Credential { client_id, token })
        .await
        .unwrap()
}

async fn stop_worker(stop: watch::Sender<bool>, mut worker: JoinHandle<crate::error::Result<()>>) {
    let _ = stop.send(true);
    match tokio::time::timeout(Duration::from_secs(30), &mut worker).await {
        Ok(result) => result.unwrap().unwrap(),
        Err(_) => {
            worker.abort();
            let _ = worker.await;
            panic!("OpenCode fixture supervisor did not stop within 30 seconds");
        }
    }
}

async fn wait_operation(
    store: &Store,
    principal: &Principal,
    operation_id: &str,
    expected_state: &str,
) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let operation = read_operation(store, principal, operation_id).await;
            if operation["state"] == expected_state {
                return operation;
            }
            assert!(
                !matches!(operation["state"].as_str(), Some("rejected" | "cancelled")),
                "{operation}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}

async fn retained_operation_identity(store: &Store, operation_id: &str) -> (String, String) {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            db.query_row(
                "SELECT state,original_request_json FROM operations WHERE operation_id=?1",
                params![operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn panicked_worker_marks_in_flight_unknown_and_reports_safe_failure() {
    let fixture = Fixture::new().await;
    let (owner, operator) = start(&fixture).await;
    let store = owner.store.clone();

    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(store.clone().supervise_opencode(receiver));
    let opened = write(
        &store,
        &operator,
        "agent.open",
        json!({"lane_id":"worker-failure","route":"fixture"}),
    )
    .await;
    wait_operation(
        &store,
        &operator,
        opened["operation_id"].as_str().unwrap(),
        "settled",
    )
    .await;
    let binding_id = opened["binding_id"].as_str().unwrap().to_owned();

    let task = write(
        &store,
        &operator,
        "task.create",
        json!({
            "project_id":"fixture",
            "spec":serde_json::from_str::<Value>(include_str!("../../../config/task.example.json")).unwrap()
        }),
    )
    .await;
    let attempt = write(
        &store,
        &operator,
        "task.claim",
        json!({
            "task_id":task["task_id"],
            "expected_revision":1,
            "start_owner":"controller",
            "binding_id":binding_id,
            "binding_generation":1
        }),
    )
    .await;
    {
        let mut world = fixture.world.lock().unwrap();
        world.lose_prompt = true;
        world.consume_prompt = true;
    }
    let target = write(
        &store,
        &operator,
        "task.dispatch",
        json!({
            "attempt_id":attempt["attempt_id"],
            "text":"fixture-only dispatch retained across worker failure"
        }),
    )
    .await;
    let target = wait_operation(
        &store,
        &operator,
        target["operation_id"].as_str().unwrap(),
        "outcome_unknown",
    )
    .await;
    let target_id = target["operation_id"].as_str().unwrap().to_owned();
    let target_identity_before = retained_operation_identity(&store, &target_id).await;
    assert_eq!(target_identity_before.0, "outcome_unknown");
    stop_worker(stop, worker).await;

    let reconcile = write(
        &store,
        &operator,
        "agent.reconcile",
        json!({
            "binding_id":binding_id,
            "generation":1,
            "operation_id":target_id
        }),
    )
    .await;
    let attach_binding_id = binding_id.clone();
    let boot = model::new_id();
    let module_principal = store
        .run(move |db| {
            let binding = super::operations::get_binding(db, &attach_binding_id, 1)?;
            super::attach(db, &binding, &boot, "worker_failure_test")
        })
        .await
        .unwrap();
    let select_principal = module_principal.clone();
    let config = store.config.clone();
    let selected = store
        .run(move |db| super::runtime::next_with_config(db, &select_principal, &config))
        .await
        .unwrap();
    let command: crate::runtime::RuntimeCommand =
        serde_json::from_value(selected["command"].clone()).unwrap();
    assert_eq!(command.method, "agent.reconcile");
    assert_eq!(command.operation_id, reconcile["operation_id"]);
    let reconcile_identity_before =
        retained_operation_identity(&store, &command.operation_id).await;
    assert_eq!(reconcile_identity_before.0, "sending");

    let native_request_count_before = fixture.world.lock().unwrap().requests.len();
    let panic_marker = "fixture panic payload is private";
    let result = tokio::spawn(async move {
        panic!("{panic_marker}");
    })
    .await;
    assert!(matches!(&result, Err(error) if error.is_panic()));
    store
        .recover_finished_shared_worker(&fixture.options.service_id, result)
        .await
        .unwrap();

    let manager = register_manager(&store, &operator).await;
    let state = store
        .call(
            manager,
            "agent.state".into(),
            json!({"binding_id":binding_id,"generation":1}),
        )
        .await
        .unwrap();
    assert_eq!(
        state["observation"]["latest_native_failure"]["code"],
        "NATIVE_WORKER_PANICKED"
    );
    assert!(
        state["observation"]["latest_native_failure"]["recorded_at_ms"]
            .as_i64()
            .unwrap()
            > 0
    );
    assert!(!state.to_string().contains(panic_marker));

    let target_after = read_operation(&store, &operator, &target_id).await;
    assert_eq!(target_after["state"], "outcome_unknown");
    let target_identity_after = retained_operation_identity(&store, &target_id).await;
    assert_eq!(target_identity_after, target_identity_before);
    let reconcile_after = read_operation(&store, &operator, &command.operation_id).await;
    assert_eq!(reconcile_after["state"], "outcome_unknown");
    assert_eq!(
        retained_operation_identity(&store, &command.operation_id)
            .await
            .1,
        reconcile_identity_before.1
    );
    assert_eq!(
        fixture.world.lock().unwrap().requests.len(),
        native_request_count_before,
        "worker recovery does not replay native input or perform readback"
    );

    drop(store);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn supervisor_store_error_signals_and_joins_workers_before_returning() {
    use std::sync::atomic::{AtomicBool, Ordering};

    let (stop, receiver) = watch::channel(false);
    let exited = std::sync::Arc::new(AtomicBool::new(false));
    let worker_exited = exited.clone();
    let worker = tokio::spawn(async move {
        let mut receiver = receiver;
        loop {
            if *receiver.borrow() {
                break;
            }
            if receiver.changed().await.is_err() {
                return;
            }
        }
        worker_exited.store(true, Ordering::SeqCst);
    });

    let result = super::finish_opencode_supervisor(
        stop,
        std::collections::BTreeMap::from([("held-fixture-worker".to_owned(), worker)]),
        std::collections::BTreeMap::new(),
        Err(Error::new("STORE_ERROR", "fixture supervisor failure")),
    )
    .await;

    assert_eq!(result.unwrap_err().code, "STORE_ERROR");
    assert!(exited.load(Ordering::SeqCst));
}
