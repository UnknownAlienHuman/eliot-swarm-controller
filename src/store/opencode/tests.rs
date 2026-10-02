use super::*;
use crate::{
    config::{Config, Route},
    platform::{DataRoot, bootstrap_credential},
    runtime::opencode_v2::tests::Fixture,
    store::StoreOwner,
};
use std::sync::Arc;

async fn start(f: &Fixture) -> (StoreOwner, Principal) {
    let directory = f.dir.join("controller");
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut cfg = Config::default();
    cfg.storage.data_dir = directory;
    cfg.routes.push(Route {
        alias: "fixture".into(),
        runtime: oc::RUNTIME.into(),
        module_artifact_id: oc::ARTIFACT_ID.into(),
        enabled: true,
        native_options: json!(f.options),
    });
    let owner = StoreOwner::start(root, Arc::new(cfg), credential.clone())
        .await
        .unwrap();
    let p = owner.store.authenticate(credential).await.unwrap();
    (owner, p)
}
async fn write(store: &Store, p: &Principal, method: &str, mut input: Value) -> Result<Value> {
    input["client_request_id"] = json!(model::new_id());
    store.call(p.clone(), method.into(), input).await
}
async fn read(store: &Store, p: &Principal, method: &str, input: Value) -> Value {
    store.call(p.clone(), method.into(), input).await.unwrap()
}
async fn wait_operation(store: &Store, p: &Principal, operation: &Value, state: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed = read(
                store,
                p,
                "operation.get",
                json!({"operation_id":operation["operation_id"]}),
            )
            .await;
            if observed["state"] == state {
                return observed;
            }
            assert!(
                !matches!(observed["state"].as_str(), Some("rejected" | "cancelled")),
                "{observed}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn shared_reader_receipts_survive_host_restart_without_replaying_native_work() {
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let first = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"one","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &first, "settled").await;
    let second = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"two","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &second, "settled").await;
    assert_eq!(
        f.world.lock().unwrap().event_connections,
        1,
        "two bindings must share one service reader"
    );
    let task=write(&owner.store,&p,"task.create",json!({"project_id":"fixture","spec":serde_json::from_str::<Value>(include_str!("../../../config/task.example.json")).unwrap()})).await.unwrap();
    let attempt=write(&owner.store,&p,"task.claim",json!({"task_id":task["task_id"],"expected_revision":1,"start_owner":"controller","binding_id":first["binding_id"],"binding_generation":1})).await.unwrap();
    {
        let mut w = f.world.lock().unwrap();
        w.lose_prompt = true;
        w.consume_prompt = true;
    }
    let dispatch = write(
        &owner.store,
        &p,
        "task.dispatch",
        json!({"attempt_id":attempt["attempt_id"],"text":"fixture-only work"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &dispatch, "outcome_unknown").await;
    write(
        &owner.store,
        &p,
        "host.mode",
        json!({"new_work":"disabled"}),
    )
    .await
    .unwrap();
    stop.send(true).unwrap();
    worker.await.unwrap();
    owner.close().await.unwrap();
    assert_eq!(
        f.world.lock().unwrap().sessions.len(),
        2,
        "host shutdown never deletes native sessions"
    );
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let operation = wait_operation(&owner.store, &p, &dispatch, "settled").await;
    assert!(operation["native_refs"]["turn_id"].is_null());
    assert!(operation["native_refs"]["input_id"].is_string());
    let current = read(
        &owner.store,
        &p,
        "attempt.get",
        json!({"attempt_id":attempt["attempt_id"]}),
    )
    .await;
    assert_eq!(current["producers"].as_array().unwrap().len(), 1);
    assert_eq!(current["producers"][0]["admission_kind"], "native_inbox");
    assert!(current["producers"][0].get("native_run_id").is_none());
    assert!(current["released_at_ms"].is_null());
    let blocked=write(&owner.store,&p,"attempt.release",json!({"attempt_id":attempt["attempt_id"],"outcome":"cancelled","assignment_closed":true,"reason":"test must not erase an unresolved native producer"})).await.unwrap_err();
    assert_eq!(blocked.code, "NATIVE_WORK_UNRESOLVED");
    let rejected = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"disabled","route":"fixture"}),
    )
    .await
    .unwrap_err();
    assert_eq!(rejected.code, "ADMISSION_DISABLED");
    {
        let world = f.world.lock().unwrap();
        assert_eq!(
            world
                .requests
                .iter()
                .filter(|r| r.method == "POST" && r.path.ends_with("/prompt"))
                .count(),
            1
        );
        assert_eq!(
            world
                .requests
                .iter()
                .filter(|r| r.method == "POST" && r.path == "/api/session")
                .count(),
            2
        );
    }
    stop.send(true).unwrap();
    worker.await.unwrap();
    owner.close().await.unwrap();
}
#[tokio::test]
async fn terminal_transcript_settles_the_producer_and_persists_immutable_result() {
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"terminal-result","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
    let task=write(&owner.store,&p,"task.create",json!({"project_id":"fixture","spec":serde_json::from_str::<Value>(include_str!("../../../config/task.example.json")).unwrap()})).await.unwrap();
    let attempt=write(&owner.store,&p,"task.claim",json!({"task_id":task["task_id"],"expected_revision":1,"start_owner":"controller","binding_id":open["binding_id"],"binding_generation":1})).await.unwrap();
    f.world.lock().unwrap().consume_prompt = true;
    let dispatch = write(
        &owner.store,
        &p,
        "task.dispatch",
        json!({"attempt_id":attempt["attempt_id"],"text":"produce a retained fixture result"}),
    )
    .await
    .unwrap();
    let dispatch = wait_operation(&owner.store, &p, &dispatch, "settled").await;
    let input = dispatch["native_refs"]["input_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let root = oc::root_id(open["binding_id"].as_str().unwrap(), 1);
    f.append_message(
        &root,
        json!({"id":"msg_store_assistant","type":"assistant","time":{"created":2,"completed":3},"content":[{"type":"text","text":"durable store result"}]}),
    );
    f.append_message(
        &root,
        json!({"id":"msg_store_idle","type":"idle","time":{"created":4},"outcome":"succeeded"}),
    );
    let refresh = write(
        &owner.store,
        &p,
        "agent.refresh",
        json!({"binding_id":open["binding_id"],"generation":1}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &refresh, "settled").await;
    let current = read(
        &owner.store,
        &p,
        "attempt.get",
        json!({"attempt_id":attempt["attempt_id"]}),
    )
    .await;
    assert_eq!(current["producers"][0]["native_input_id"], input);
    assert_eq!(current["producers"][0]["native_run_id"], "msg_store_idle");
    assert_eq!(current["producers"][0]["disposition"], "completed");
    assert_eq!(
        current["producers"][0]["correlation_evidence"]["identity_kind"],
        "terminal_message"
    );

    let result = write(
        &owner.store,
        &p,
        "agent.result",
        json!({
            "binding_id":open["binding_id"],
            "generation":1,
            "selector":{
                "kind":"turn",
                "session_id":root,
                "input_id":input,
                "turn_id":"msg_store_idle"
            },
            "offset_bytes":0,
            "length_bytes":65536
        }),
    )
    .await
    .unwrap();
    let result = wait_operation(&owner.store, &p, &result, "settled").await;
    let artifact = result["result"]["details"]["artifact_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let page = read(
        &owner.store,
        &p,
        "artifact.read",
        json!({"artifact_id":artifact,"offset_bytes":0,"length_bytes":65536}),
    )
    .await;
    assert_eq!(page["encoding"], "utf8");
    let document: Value = serde_json::from_str(page["content"].as_str().unwrap()).unwrap();
    assert_eq!(
        document["projected_messages"][0]["id"],
        "msg_store_assistant"
    );
    assert_eq!(document["idle"]["id"], "msg_store_idle");
    let released = write(
        &owner.store,
        &p,
        "attempt.release",
        json!({"attempt_id":attempt["attempt_id"],"outcome":"cancelled","assignment_closed":true,"reason":"fixture terminal evidence observed"}),
    )
    .await
    .unwrap();
    assert_eq!(released["released"], true);
    stop.send(true).unwrap();
    worker.await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn builtin_identity_cannot_authenticate_over_external_ipc() {
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"one","route":"fixture"}),
    )
    .await
    .unwrap();
    let b = read(
        &owner.store,
        &p,
        "agent.state",
        json!({"binding_id":open["binding_id"],"generation":1}),
    )
    .await;
    let internal = owner
        .store
        .run(move |db| attach(db, &b, "fixture-boot"))
        .await
        .unwrap();
    let result = owner
        .store
        .authenticate(crate::model::Credential {
            client_id: internal.client_id,
            token: String::new(),
        })
        .await;
    assert_eq!(result.unwrap_err().code, "UNAUTHORIZED");
    owner.close().await.unwrap();
}
