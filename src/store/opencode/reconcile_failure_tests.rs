use crate::{
    config::{Config, Route},
    error::Error,
    model::{self, Credential, Principal},
    platform::{DataRoot, bootstrap_credential},
    runtime::{EffectOutcome, RuntimeCommand, opencode_v2::tests::Fixture},
    store::{Store, StoreOwner},
};
use rusqlite::params;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinHandle};

async fn start(f: &Fixture) -> (StoreOwner, Principal) {
    let directory = f.dir.join("controller");
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = directory;
    config.routes.push(Route {
        owned_service: None,
        alias: "fixture".into(),
        runtime: crate::runtime::opencode_v2::RUNTIME.into(),
        module_artifact_id: crate::runtime::opencode_v2::ARTIFACT_ID.into(),
        enabled: true,
        native_options: json!(f.options),
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

async fn read_operation(store: &Store, principal: &Principal, id: &str) -> Value {
    store
        .call(
            principal.clone(),
            "operation.get".into(),
            json!({"operation_id":id}),
        )
        .await
        .unwrap()
}

async fn register_manager(store: &Store, operator: &Principal) -> Principal {
    let client_id = format!("reconcile-test-manager-{}", model::new_id());
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

async fn read_binding(store: &Store, manager: &Principal, binding_id: &str) -> Value {
    store
        .call(
            manager.clone(),
            "agent.state".into(),
            json!({"binding_id":binding_id,"generation":1}),
        )
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

async fn wait_operation(store: &Store, principal: &Principal, id: &str, state: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let operation = read_operation(store, principal, id).await;
            if operation["state"] == state {
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

fn reconcile_readback_count(f: &Fixture, root: &str) -> usize {
    let inbox = format!("/api/session/{root}/inbox");
    let message_prefix = format!("/api/session/{root}/message/");
    f.world
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|request| {
            request.method == "GET"
                && (request.path == inbox || request.path.starts_with(&message_prefix))
        })
        .count()
}

#[tokio::test]
async fn reconcile_target_load_failure_stays_unknown_without_readback() {
    let f = Fixture::new().await;
    let (owner, principal) = start(&f).await;
    let store = owner.store.clone();

    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(store.clone().supervise_opencode(receiver));
    let open = write(
        &store,
        &principal,
        "agent.open",
        json!({"lane_id":"target-load-failure","route":"fixture"}),
    )
    .await;
    wait_operation(
        &store,
        &principal,
        open["operation_id"].as_str().unwrap(),
        "settled",
    )
    .await;
    let binding_id = open["binding_id"].as_str().unwrap().to_owned();
    let root = crate::runtime::opencode_v2::root_id(&binding_id, 1);

    let task = write(
        &store,
        &principal,
        "task.create",
        json!({
            "project_id":"fixture",
            "spec":serde_json::from_str::<Value>(include_str!("../../../config/task.example.json")).unwrap()
        }),
    )
    .await;
    let attempt = write(
        &store,
        &principal,
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
        let mut world = f.world.lock().unwrap();
        world.lose_prompt = true;
        world.consume_prompt = true;
    }
    let target = write(
        &store,
        &principal,
        "task.dispatch",
        json!({"attempt_id":attempt["attempt_id"],"text":"fixture-only reconciliation target"}),
    )
    .await;
    let target = wait_operation(
        &store,
        &principal,
        target["operation_id"].as_str().unwrap(),
        "outcome_unknown",
    )
    .await;
    let target_id = target["operation_id"].as_str().unwrap().to_owned();

    stop_worker(stop, worker).await;

    let reconcile = write(
        &store,
        &principal,
        "agent.reconcile",
        json!({
            "binding_id":binding_id,
            "generation":1,
            "operation_id":target_id
        }),
    )
    .await;
    let attach_binding_id = binding_id.clone();
    let module_boot = model::new_id();
    let module_principal = store
        .run(move |db| {
            let binding = super::operations::get_binding(db, &attach_binding_id, 1)?;
            super::attach(db, &binding, &module_boot, "reconcile_failure_test")
        })
        .await
        .unwrap();
    let select_principal = module_principal.clone();
    let config = store.config.clone();
    let selected = store
        .run(move |db| super::runtime::next_with_config(db, &select_principal, &config))
        .await
        .unwrap();
    let command: RuntimeCommand = serde_json::from_value(selected["command"].clone()).unwrap();
    assert_eq!(command.method, "agent.reconcile");
    assert_eq!(command.operation_id, reconcile["operation_id"]);

    // Keep the retained Operation and its Unknown receipt intact, but make
    // rebuilding its task.dispatch RuntimeCommand fail at attempt readback.
    let invalidated_attempt_request = json!({"attempt_id":"missing-attempt"}).to_string();
    let changed = store
        .run(move |db| {
            db.execute(
                "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
                params![target_id, invalidated_attempt_request],
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);

    let target_before =
        read_operation(&store, &principal, target["operation_id"].as_str().unwrap()).await;
    assert_eq!(target_before["state"], "outcome_unknown");
    assert_eq!(target_before["result"]["outcome"], "unknown");
    let service = f.service().await;
    let total_requests_before = f.world.lock().unwrap().requests.len();
    let readbacks_before = reconcile_readback_count(&f, &root);
    let outcome = store
        .oc_reconcile_outcome(&module_principal, &service, &f.options, &command)
        .await;
    assert!(matches!(outcome.outcome, EffectOutcome::Unknown));
    assert_eq!(outcome.details["stage"], "reconcile_target_load");
    assert_eq!(outcome.details["code"], "NOT_FOUND");
    assert_eq!(outcome.details["replayed_native_input"], false);
    assert!(outcome.details["completion_condition"].is_null());
    assert!(outcome.details["readback_attempted"].is_null());
    store
        .record_oc_outcome(&module_principal, outcome)
        .await
        .unwrap();

    let reconcile = read_operation(
        &store,
        &principal,
        reconcile["operation_id"].as_str().unwrap(),
    )
    .await;
    assert_eq!(reconcile["state"], "outcome_unknown");
    assert_eq!(reconcile["result"]["outcome"], "unknown");
    assert_eq!(
        reconcile["result"]["details"]["stage"],
        "reconcile_target_load"
    );
    assert_eq!(reconcile["result"]["details"]["code"], "NOT_FOUND");
    assert!(reconcile["result"]["details"]["completion_condition"].is_null());

    let target_after =
        read_operation(&store, &principal, target["operation_id"].as_str().unwrap()).await;
    assert_eq!(target_after["state"], "outcome_unknown");
    assert_eq!(target_after["result"], target_before["result"]);
    assert_eq!(reconcile_readback_count(&f, &root), readbacks_before);
    assert_eq!(
        f.world.lock().unwrap().requests.len(),
        total_requests_before
    );
    drop(store);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn manager_state_retains_safe_latest_connection_failure() {
    let f = Fixture::new().await;
    let (owner, operator) = start(&f).await;
    let store = owner.store.clone();
    let open = write(
        &store,
        &operator,
        "agent.open",
        json!({"lane_id":"connection-failure-history","route":"fixture"}),
    )
    .await;
    let binding_id = open["binding_id"].as_str().unwrap().to_owned();
    let attach_binding_id = binding_id.clone();
    let module_boot = model::new_id();
    let module_principal = store
        .run(move |db| {
            let binding = super::operations::get_binding(db, &attach_binding_id, 1)?;
            super::attach(db, &binding, &module_boot, "connection_failure_test")
        })
        .await
        .unwrap();

    let secret_marker = "raw-native-error-message-must-not-leak";
    let failure = Error::new("NATIVE_TEST_FAILURE", secret_marker);
    store
        .oc_connection(&module_principal, false, Some(&failure))
        .await
        .unwrap();
    let manager = register_manager(&store, &operator).await;
    let failed_state = read_binding(&store, &manager, &binding_id).await;
    let failure_time = failed_state["observation"]["latest_native_failure"]["recorded_at_ms"]
        .as_i64()
        .unwrap();
    assert!(failure_time > 0);

    let inject_binding = binding_id.clone();
    let changed = store
        .run(move |db| {
            db.execute(
                "UPDATE bindings SET state_json=json_set(state_json,'$.latest_native_failure.message',?3) WHERE binding_id=?1 AND generation=?2",
                params![inject_binding, 1, secret_marker],
            )
            .map_err(Into::into)
        })
        .await
        .unwrap();
    assert_eq!(changed, 1);

    // A partial snapshot is successful readback with failed optional axes.
    // Its manager projection must retain diagnoses without native payloads.
    let mut failures = vec![
        json!({"code":"NATIVE_SNAPSHOT_BUDGET_EXHAUSTED","source":"form","session_id":"ses_safe","message":secret_marker,"credential":secret_marker}),
        json!({"code":secret_marker,"source":"permission","session_id":"ses_safe"}),
        json!({"code":"NATIVE_READ_FAILURE","source":secret_marker,"session_id":"C:\\private\\session","body":secret_marker}),
    ];
    failures.extend((0..62).map(|_| json!({"code":"NATIVE_READ_FAILURE"})));
    let native = json!({"failures":failures,"gaps":1,"family_completeness":"partial"});
    let inject_binding = binding_id.clone();
    store
        .run(move |db| {
            db.execute(
                "UPDATE bindings SET state_json=json_set(state_json,'$.native',json(?3)) WHERE binding_id=?1 AND generation=?2",
                params![inject_binding, 1, native.to_string()],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    store
        .oc_connection(&module_principal, true, None)
        .await
        .unwrap();
    store
        .oc_connection(&module_principal, false, None)
        .await
        .unwrap();

    let state = read_binding(&store, &manager, &binding_id).await;
    assert_eq!(state["observation"]["connection"], "native_unavailable");
    let latest = state["observation"]["latest_native_failure"]
        .as_object()
        .unwrap();
    assert_eq!(latest.len(), 2);
    assert_eq!(latest["code"], "NATIVE_TEST_FAILURE");
    assert_eq!(latest["recorded_at_ms"], failure_time);
    let native = &state["observation"]["native"];
    assert_eq!(native["gaps"], 1);
    assert_eq!(native["family_completeness"], "partial");
    assert_eq!(native["failure_count"], 65);
    assert_eq!(native["failures_truncated"], true);
    let failures = native["failures"].as_array().unwrap();
    assert_eq!(failures.len(), 64);
    assert_eq!(
        failures[0],
        json!({"code":"NATIVE_SNAPSHOT_BUDGET_EXHAUSTED","source":"form","session_id":"ses_safe"})
    );
    assert_eq!(
        failures[1],
        json!({"code":"NATIVE_SNAPSHOT_DIAGNOSTIC_CORRUPT"})
    );
    assert_eq!(failures[2], json!({"code":"NATIVE_READ_FAILURE"}));
    assert!(!state.to_string().contains(secret_marker));
    assert!(!state.to_string().contains("private"));
    let corrupt_binding = binding_id.clone();
    store
        .run(move |db| {
            db.execute(
                "UPDATE bindings SET state_json=json_set(state_json,'$.native.failures',json('null')) WHERE binding_id=?1 AND generation=1",
                [corrupt_binding],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let state = read_binding(&store, &manager, &binding_id).await;
    let native = &state["observation"]["native"];
    assert_eq!(
        native["failures"],
        json!([{"code":"NATIVE_SNAPSHOT_DIAGNOSTIC_CORRUPT"}])
    );
    assert!(native.get("failure_count").is_none());
    assert_eq!(
        state["observation"]["latest_native_failure"]["code"],
        "NATIVE_TEST_FAILURE"
    );
    drop(store);
    owner.close().await.unwrap();
}
