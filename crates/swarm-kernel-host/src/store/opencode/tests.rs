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
        workspace_option: None,
        owned_service: None,
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
    worker.await.unwrap().unwrap();
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
    worker.await.unwrap().unwrap();
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
        .run(move |db| attach(db, &b, "fixture-boot", "external_shared_service"))
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
async fn rejected_open_preflight_retains_diagnostic_without_partial_native_identity() {
    let f = Fixture::new().await;
    f.override_get(
        "/api/model",
        crate::runtime::opencode_v2::tests::Reply::Json(
            200,
            json!({"location":{"directory":f.options.directory.clone()},"data":[]}),
        ),
    );
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"rejected-preflight","route":"fixture"}),
    )
    .await
    .unwrap();
    let operation = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let observed = read(
                &owner.store,
                &p,
                "operation.get",
                json!({"operation_id":open["operation_id"]}),
            )
            .await;
            if observed["state"] == "rejected" {
                return observed;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    assert_eq!(operation["state"], "rejected");
    assert_eq!(operation["result"]["outcome"], "rejected");
    assert_eq!(
        operation["result"]["details"]["code"],
        "NATIVE_MODEL_UNAVAILABLE"
    );
    let binding = read(
        &owner.store,
        &p,
        "agent.state",
        json!({"binding_id":open["binding_id"],"generation":1}),
    )
    .await;
    assert_eq!(binding["native_root_id"], Value::Null);
    assert_eq!(binding["native_scope_key"], Value::Null);
    assert_eq!(
        binding["observation"]["opening_evidence"]["code"],
        "NATIVE_MODEL_UNAVAILABLE"
    );
    assert_eq!(f.posts("/api/session"), 0);

    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn goal_receipts_survive_host_restart_without_replaying_native_work() {
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"goal-restart","route":"fixture"}),
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
    worker.await.unwrap().unwrap();
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
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}

async fn wait_native_entry(store: &Store, p: &Principal, binding: &str, key: &str) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let state = read(
                store,
                p,
                "agent.state",
                json!({"binding_id":binding,"generation":1}),
            )
            .await;
            let entries = &state["observation"]["native"]["configuration"]["owned_entries"];
            if entries
                .as_array()
                .is_some_and(|entries| entries.iter().any(|entry| entry["key"] == key))
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
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
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn setup_snapshot_binds_dependent_to_the_whole_setup() {
    // The implementation review §5.2 scenario: A puts instruction X;
    // B is a later setup step naming A as its prerequisite; D then
    // replaces X; C names only B. C must be blocked even though X is
    // not B's own scope, because B settled with a bounded setup
    // snapshot proving the whole setup (X included).
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"setup-snapshot","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
    let binding = open["binding_id"].as_str().unwrap();
    // A: put the required instruction X.
    let a = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":binding,"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":{"review_before_submit":true}}}}),
    )
    .await
    .unwrap();
    let a = wait_operation(&owner.store, &p, &a, "settled").await;
    assert_eq!(a["result"]["outcome"], "applied");
    wait_native_entry(&owner.store, &p, binding, "eliot.policy").await;
    // B: a second setup step on another owned entry, prerequisite A.
    let b = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":binding,"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.style","value":{"tone":"direct"}}},"prerequisite_operation_id":a["operation_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(b["prerequisite_state"], "satisfied");
    let b = wait_operation(&owner.store, &p, &b, "settled").await;
    // The snapshot frozen at B's settlement names the whole setup.
    let state = read(
        &owner.store,
        &p,
        "agent.state",
        json!({"binding_id":binding,"generation":1}),
    )
    .await;
    let snapshot = &state["observation"]["setup_snapshots"][b["operation_id"].as_str().unwrap()];
    let scopes: Vec<&str> = snapshot["conditions"]
        .as_array()
        .expect("setup snapshot must be persisted at settlement")
        .iter()
        .map(|condition| condition["scope"].as_str().unwrap())
        .collect();
    assert!(scopes.contains(&"instruction:eliot.policy"), "{snapshot}");
    assert!(scopes.contains(&"instruction:eliot.style"), "{snapshot}");
    // C0: a dependent naming only B is satisfied while the setup holds.
    let c0 = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":binding,"generation":1,"action":"set","objective":"control goal","prerequisite_operation_id":b["operation_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(c0["prerequisite_state"], "satisfied");
    wait_operation(&owner.store, &p, &c0, "settled").await;
    // An unrelated instruction change does not block a dependent of B:
    // eliot.other was never part of B's proven setup.
    let unrelated = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":binding,"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.other","value":{"unrelated":true}}}}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &unrelated, "settled").await;
    let c1 = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":binding,"generation":1,"action":"set","objective":"after unrelated change","prerequisite_operation_id":b["operation_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(c1["prerequisite_state"], "satisfied");
    wait_operation(&owner.store, &p, &c1, "settled").await;
    // D: replace the required instruction X. C naming only B is now
    // blocked with PREREQUISITE_STALE.
    let d = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":binding,"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":{"review_before_submit":false}}}}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &d, "settled").await;
    let blocked = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":binding,"generation":1,"action":"set","objective":"blocked goal","prerequisite_operation_id":b["operation_id"]}),
    )
    .await
    .unwrap_err();
    assert_eq!(blocked.code, "PREREQUISITE_STALE");

    // Second binding: a later Operation pending on a snapshot scope
    // makes the gate pending — not satisfied, not stale.
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"setup-snapshot-pending","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
    let binding = open["binding_id"].as_str().unwrap();
    let root = oc::root_id(binding, 1);
    let a = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":binding,"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":{"review_before_submit":true}}}}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &a, "settled").await;
    wait_native_entry(&owner.store, &p, binding, "eliot.policy").await;
    let b = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":binding,"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.style","value":{"tone":"direct"}}},"prerequisite_operation_id":a["operation_id"]}),
    )
    .await
    .unwrap();
    let b = wait_operation(&owner.store, &p, &b, "settled").await;
    f.world.lock().unwrap().overrides.insert(
        (
            "PUT".into(),
            format!("/api/experimental/session/{root}/instructions/entries/eliot.policy"),
        ),
        crate::runtime::opencode_v2::tests::Reply::Drop,
    );
    let pending = write(
        &owner.store,
        &p,
        "agent.configure",
        json!({"binding_id":binding,"generation":1,"settings":{"instruction_entry":{"action":"put","key":"eliot.policy","value":{"review_before_submit":false}}}}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &pending, "outcome_unknown").await;
    let c = write(
        &owner.store,
        &p,
        "agent.goal",
        json!({"binding_id":binding,"generation":1,"action":"set","objective":"pending goal","prerequisite_operation_id":b["operation_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(c["prerequisite_state"], "pending");
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
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
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
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
        json!({"lane_id":"fam","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
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
            .any(|t| t["turnId"] == "evt_run_ca_started" && t["terminal"] == "completed")
            && turns.iter().any(|t| t["turnId"] == "evt_run_cb_started")
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
        bind("ses_ca", "evt_run_ca_started", "w-ca"),
    )
    .await
    .unwrap();
    assert_eq!(bound_a["producer"]["disposition"], "completed");
    let bound_b = write(
        &owner.store,
        &p,
        "attempt.bind_producer",
        bind("ses_cb", "evt_run_cb_started", "w-cb"),
    )
    .await
    .unwrap();
    assert_eq!(bound_b["producer"]["disposition"], "admitted");
    let blocked = write(
        &owner.store,
        &p,
        "attempt.release",
        json!({"attempt_id":attempt["attempt_id"],"outcome":"cancelled","assignment_closed":true,"reason":"child B still running"}),
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
            "created":1.0,
            "durable":{"aggregateID":"ses_cb","seq":3,"version":1},"data":{"sessionID":"ses_cb","reason":"user"}}),
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
        json!({"attempt_id":attempt["attempt_id"],"outcome":"cancelled","assignment_closed":true,"reason":"both children terminal"}),
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
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}

#[tokio::test]
async fn background_operation_settles_from_the_native_notice() {
    let f = Fixture::new().await;
    let (owner, p) = start(&f).await;
    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(owner.store.clone().supervise_opencode(receiver));
    let open = write(
        &owner.store,
        &p,
        "agent.open",
        json!({"lane_id":"background","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&owner.store, &p, &open, "settled").await;
    let root = oc::root_id(open["binding_id"].as_str().unwrap(), 1);
    {
        let mut w = f.world.lock().unwrap();
        w.timeline.insert(
            root.clone(),
            vec![json!({"id":"msg_assistant_1","sessionID":root,"type":"assistant","time":{"created":2},
              "agent":"build","model":{"id":"fixture-model","providerID":"fixture-provider","variant":"explicit-variant"},
              "content":[{"type":"tool","id":"call_fixture_1","name":"task","state":{"status":"running","input":{"description":"fixture child"},"metadata":{}}}]})],
        );
        w.background_jobs.insert(
            root.clone(),
            vec![json!({"id":"job_fixture_1","type":"task","title":"fixture child"})],
        );
    }
    let background = write(
        &owner.store,
        &p,
        "agent.background",
        json!({"binding_id":open["binding_id"],"generation":1}),
    )
    .await
    .unwrap();
    let background = wait_operation(&owner.store, &p, &background, "settled").await;
    assert_eq!(background["result"]["outcome"], "applied");
    assert_eq!(
        background["result"]["details"]["completion_condition"],
        "native_foreground_tools_backgrounded"
    );
    assert_eq!(
        background["result"]["details"]["backgrounded"],
        json!([{"type":"task","label":"fixture child"}])
    );
    assert_eq!(
        background["operation_contract"]["completion_condition"],
        "native_foreground_tools_backgrounded"
    );
    assert_eq!(
        background["operation_contract"]["contract_revision"],
        "opencode-background-v1"
    );
    assert_eq!(
        background["operation_contract"]["replay_policy"],
        "readback_only_no_mutation_replay"
    );
    assert_eq!(f.posts(&format!("/api/session/{root}/background")), 1);
    assert_eq!(f.posts(&format!("/api/session/{root}/prompt")), 0);
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}
