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

#[tokio::test]
async fn goal_receipts_survive_host_restart_without_replaying_native_work() {
async fn bound_child_producers_close_only_from_their_own_logs() {
    use crate::runtime::opencode_v2::tests::child_events;
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"goal-restart","route":"fixture"}),
        json!({"lane_id":"fam","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
    let root = oc::root_id(open["binding_id"].as_str().unwrap(), 1);
    f.world.lock().unwrap().lose_entry_put = true;
    let goal = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":open["binding_id"],"generation":1,"action":"set","objective":"restart-safe goal"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &goal, "outcome_unknown").await;
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
    // Restart recovery must pick up agent.goal and reconcile by readback only:
    // the entry PUT and the (never sent) activation prompt are not replayed.
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let operation = wait_operation(&owner.store, &p, &goal, "settled").await;
    assert_eq!(operation["result"]["outcome"], "applied");
    assert_eq!(
        operation["result"]["details"]["completion_condition"],
        "native_goal_recorded"
    );
    assert_eq!(
        operation["result"]["details"]["goal"]["activation_input_id"],
        Value::Null
    );
    {
        let world = f.world.lock().unwrap();
        assert_eq!(
            world
                .requests
                .iter()
                .filter(
                    |r| r.method == "PUT" && r.path.ends_with("/instructions/entries/eliot.goal")
                )
                .count(),
            1
        );
        assert_eq!(
            world
                .requests
                .iter()
                .filter(|r| r.method == "POST" && r.path == format!("/api/session/{root}/prompt"))
                .count(),
            0
        );
    }
    stop.send(true).unwrap();
    worker.await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn configure_prerequisite_gates_goal_start() {
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"goal-prerequisite","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
    let root = oc::root_id(open["binding_id"].as_str().unwrap(), 1);
    let configure = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":open["binding_id"],"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":{"review_before_submit":true}}}}),
    )
    .await
    .unwrap();
    let configure = wait_operation(&owner.store, &p, &configure, "settled").await;
    assert_eq!(configure["result"]["outcome"], "applied");
    assert_eq!(
        configure["result"]["details"]["completion_condition"],
        "native_configuration_applied"
    );
    let goal = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":open["binding_id"],"generation":1,"action":"set","objective":"gated goal","prerequisite_operation_id":configure["operation_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(goal["prerequisite_state"], "satisfied");
    let goal = wait_operation(&owner.store, &p, &goal, "settled").await;
    assert_eq!(goal["result"]["outcome"], "applied");
    assert_eq!(
        goal["result"]["details"]["completion_condition"],
        "native_goal_recorded"
    );
    assert_eq!(
        goal["operation_contract"]["completion_condition"],
        "native_goal_recorded"
    );
    assert_eq!(goal["operation_contract"]["native_goal_api"], false);
    assert_eq!(
        goal["operation_contract"]["continuation_owner"],
        "controller_record"
    );
    assert_eq!(
        f.puts(&format!(
            "/api/experimental/session/{root}/instructions/entries/eliot.goal"
        )),
        1
    );
    assert_eq!(f.posts(&format!("/api/session/{root}/prompt")), 1);
    // A goal Operation can be a dependent, never a prerequisite: the gate
    // still requires an agent.configure Operation.
    let invalid = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":open["binding_id"],"generation":1,"action":"set","objective":"ungated goal","prerequisite_operation_id":goal["operation_id"]}),
    )
    .await
    .unwrap_err();
    assert_eq!(invalid.code, "INVALID_PREREQUISITE");
    stop.send(true).unwrap();
    worker.await.unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn goal_pause_cancels_queued_goal_set_on_same_binding() {
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"goal-stop","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
    let root = oc::root_id(open["binding_id"].as_str().unwrap(), 1);
    // A configure whose PUT response is lost without landing stays
    // outcome_unknown, so a goal naming it as prerequisite stays queued.
    f.world.lock().unwrap().overrides.insert(
        (
            "PUT".into(),
            format!("/api/experimental/session/{root}/instructions/entries/eliot.policy"),
        ),
        crate::runtime::opencode_v2::tests::Reply::Drop,
    );
    let configure = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":open["binding_id"],"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":{"review_before_submit":true}}}}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &configure, "outcome_unknown").await;
    let set = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":open["binding_id"],"generation":1,"action":"set","objective":"queued goal","prerequisite_operation_id":configure["operation_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(set["prerequisite_state"], "pending");
    let queued = read(
        &owner.store,
        &p,
        "operation.get",
        json!({"operation_id":set["operation_id"]}),
    )
    .await;
    assert_eq!(queued["state"], "queued");
    // The pause admission itself cancels the queued goal start in the same
    // Store transaction, before the pause is dispatched.
    let pause = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":open["binding_id"],"generation":1,"action":"pause"}),
    )
    .await
    .unwrap();
    let cancelled = read(
        &owner.store,
        &p,
        "operation.get",
        json!({"operation_id":set["operation_id"]}),
    )
    .await;
    assert_eq!(cancelled["state"], "cancelled");
    assert_eq!(cancelled["result"]["reason"], "superseded_by_goal_stop");
    assert_eq!(
        cancelled["result"]["stop_operation_id"],
        pause["operation_id"]
    );
    let root = f
        .world
        .lock()
        .unwrap()
        .sessions
        .keys()
        .next()
        .unwrap()
        .clone();
    {
        let mut w = f.world.lock().unwrap();
        for id in ["ses_ca", "ses_cb"] {
            w.sessions.insert(
                id.into(),
                json!({"id":id,"parentID":root,"projectID":"prj_fixture","time":{"created":1,"updated":2}}),
            );
            w.active.insert(id.into(), json!({"type":"running"}));
        }
        w.logs.insert(
            "ses_ca".into(),
            child_events("ses_ca", &root, &[("run_ca", Some("completed"))]),
        );
        w.logs.insert(
            "ses_cb".into(),
            child_events("ses_cb", &root, &[("run_cb", None)]),
        );
    }
    let task=write(&owner.store,&p,"task.create",json!({"project_id":"fixture","spec":serde_json::from_str::<Value>(include_str!("../../../config/task.example.json")).unwrap()})).await.unwrap();
    let attempt=write(&owner.store,&p,"task.claim",json!({"task_id":task["task_id"],"expected_revision":1,"start_owner":"controller","binding_id":open["binding_id"],"binding_generation":1})).await.unwrap();
    // Both children are natively active, so the family snapshot reads their
    // logs; wait until the recorded observation carries both runs.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let observation_id = loop {
        let state = read(
            &owner.store,
            &p,
            "agent.state",
            json!({"binding_id":open["binding_id"],"generation":1}),
        )
        .await;
        let turns = state["observation"]["native"]["turns"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        if turns
            .iter()
            .any(|t| t["turnId"] == "run_ca" && t["terminal"] == "completed")
            && turns.iter().any(|t| t["turnId"] == "run_cb")
        {
            assert_eq!(
                state["observation"]["native"]["family_coverage"]["members_with_terminal_evidence"],
                1
            );
            break state["observation"]["native_observation_id"]
                .as_i64()
                .unwrap();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "child turns never observed: {state}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let bind = |session: &str, run: &str, assignment: &str| {
        json!({"attempt_id":attempt["attempt_id"],"assignment_id":assignment,
        "native_session_id":session,"native_run_id":run,"observation_id":observation_id})
    };
    let bound_a = write(
        &owner.store,
        &p,
        "attempt.bind_producer",
        bind("ses_ca", "run_ca", "w-ca"),
    )
    .await
    .unwrap();
    assert_eq!(bound_a["producer"]["disposition"], "completed");
    let bound_b = write(
        &owner.store,
        &p,
        "attempt.bind_producer",
        bind("ses_cb", "run_cb", "w-cb"),
    )
    .await
    .unwrap();
    assert_eq!(bound_b["producer"]["disposition"], "admitted");
    let blocked = write(
        &owner.store,
        &p,
        "attempt.release",
        json!({"attempt_id":attempt["attempt_id"],"outcome":"completed","assignment_closed":true,"reason":"child B still running"}),
    )
    .await
    .unwrap_err();
    assert_eq!(blocked.code, "NATIVE_WORK_UNRESOLVED");
    // B finishes natively and leaves the active map. Only the bound-producer
    // axis keeps B tracked, so its own log can close exactly its producer.
    {
        let mut w = f.world.lock().unwrap();
        w.active.remove("ses_cb");
        w.logs.get_mut("ses_cb").unwrap().push(
            json!({"id":"evt_run_cb_terminal","type":"session.execution.interrupted","version":1,
            "durable":{"aggregateID":"ses_cb","seq":3},"data":{"reason":"user"}}),
        );
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let current = read(
            &owner.store,
            &p,
            "attempt.get",
            json!({"attempt_id":attempt["attempt_id"]}),
        )
        .await;
        let producers = current["producers"].as_array().unwrap();
        if producers.iter().all(|x| {
            matches!(
                x["disposition"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        }) {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "child B never closed: {current}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    write(
        &owner.store,
        &p,
        "attempt.release",
        json!({"attempt_id":attempt["attempt_id"],"outcome":"completed","assignment_closed":true,"reason":"both children terminal"}),
    )
    .await
    .unwrap();
    let current = read(
        &owner.store,
        &p,
        "attempt.get",
        json!({"attempt_id":attempt["attempt_id"]}),
    )
    .await;
    assert!(current["released_at_ms"].is_number());
    stop.send(true).unwrap();
    worker.await.unwrap();
    owner.close().await.unwrap();
}
