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
        admission_policy: Default::default(),
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

fn registered_opencode_descriptor() -> Value {
    let mut descriptor: Value = serde_json::from_str(include_str!(
        "../../../../swarm-adapter-opencode/registration/descriptor.template.json"
    ))
    .unwrap();
    descriptor["enabled"] = json!(true);
    descriptor["launch"]["executable"] = json!(
        std::env::current_exe()
            .unwrap()
            .to_string_lossy()
            .into_owned()
    );
    descriptor
}

async fn start_module_route(
    f: &Fixture,
    descriptor: Value,
    select_descriptor: bool,
) -> (StoreOwner, Principal, Value) {
    let directory = f.dir.join("module-controller");
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let artifact_id = descriptor["artifact"]["artifact_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let standalone_opencode = artifact_id == crate::config::OPENCODE_RUST_ARTIFACT_ID;
    let mut cfg = Config::default();
    cfg.storage.data_dir = directory;
    cfg.routes.push(Route {
        admission_policy: Default::default(),
        workspace_option: standalone_opencode.then(|| "directory".to_owned()),
        owned_service: None,
        alias: "module-fixture".into(),
        runtime: "module".into(),
        module_artifact_id: artifact_id.clone(),
        enabled: true,
        native_options: if standalone_opencode {
            json!(f.options)
        } else {
            json!({})
        },
    });
    let owner = StoreOwner::start(root, Arc::new(cfg), credential.clone())
        .await
        .unwrap();
    let p = owner.store.authenticate(credential).await.unwrap();
    let supervisor = owner
        .store
        .authenticate(owner.module_supervisor_credential())
        .await
        .unwrap();
    owner
        .store
        .call(
            supervisor,
            "module.descriptor.register".into(),
            json!({"descriptor":descriptor}),
        )
        .await
        .unwrap();
    let selector = if select_descriptor {
        select_module_route(&owner, &p, "module-fixture", &artifact_id).await
    } else {
        Value::Null
    };
    let binding = module_binding("module-fixture", &artifact_id, selector);
    (owner, p, binding)
}

async fn select_module_route(
    owner: &StoreOwner,
    p: &Principal,
    route_alias: &str,
    artifact_id: &str,
) -> Value {
    let catalog = read(&owner.store, p, "module.catalog.get", json!({})).await;
    let selected = write(
        &owner.store,
        p,
        "module.route.select",
        json!({
            "route_alias":route_alias,
            "module_id":"eliot.opencode.v2",
            "artifact_id":artifact_id,
            "version":"0.5.0",
            "expected_catalog_revision":catalog["catalog_revision"],
        }),
    )
    .await
    .unwrap();
    selected["selection"].clone()
}

fn module_binding(route_alias: &str, artifact_id: &str, selector: Value) -> Value {
    let mut observation = json!({});
    if !selector.is_null() {
        observation["module_contract_selector"] = selector;
    }
    json!({
        "binding_id":"module-consumer-fixture",
        "generation":1,
        "module_artifact_id":artifact_id,
        "route":{"alias":route_alias,"runtime":"module","module_artifact_id":artifact_id},
        "observation":observation,
    })
}

async fn loop_step_status_supported(owner: &StoreOwner, binding: Value) -> Result<bool> {
    owner
        .store
        .run(move |db| {
            crate::store::results::input_status_target_supported(
                db,
                &binding,
                &json!({"method":"native.opencode.loop_step"}),
            )
        })
        .await
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

async fn retained_operation(store: &Store, operation_id: &str) -> Value {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| crate::store::operations::get_operation(db, &operation_id))
        .await
        .unwrap()
}

async fn retained_attempt(store: &Store, attempt_id: &str) -> Value {
    let attempt_id = attempt_id.to_owned();
    store
        .run(move |db| crate::store::tasks::get_attempt(db, &attempt_id))
        .await
        .unwrap()
}

#[tokio::test]
async fn loop_step_input_status_accepts_only_the_exact_selected_opencode_module_contract() {
    let exact = registered_opencode_descriptor();
    let artifact_id = crate::config::OPENCODE_RUST_ARTIFACT_ID;
    assert_eq!(exact["artifact"]["artifact_id"], artifact_id);
    assert_eq!(exact["artifact"]["version"], "0.5.0");

    let f = Fixture::new().await;
    let (owner, _p, binding) = start_module_route(&f, exact.clone(), true).await;
    assert_eq!(binding["route"]["runtime"], "module");
    assert!(loop_step_status_supported(&owner, binding).await.unwrap());
    owner.close().await.unwrap();

    let f = Fixture::new().await;
    let mut no_capability = exact.clone();
    no_capability["capabilities"] = json!(
        no_capability["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|capability| capability.as_str() != Some("native.opencode.loop_step"))
            .cloned()
            .collect::<Vec<_>>()
    );
    assert_eq!(no_capability["artifact"], exact["artifact"]);
    let (owner, _p, binding) = start_module_route(&f, no_capability, true).await;
    assert!(!loop_step_status_supported(&owner, binding).await.unwrap());
    owner.close().await.unwrap();

    let f = Fixture::new().await;
    let mut no_schema = exact.clone();
    no_schema["command_schemas"] = json!(
        no_schema["command_schemas"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|schema| schema["schema_id"] != "swarm.opencode_loop_step_command")
            .cloned()
            .collect::<Vec<_>>()
    );
    assert_eq!(no_schema["artifact"], exact["artifact"]);
    let (owner, _p, binding) = start_module_route(&f, no_schema, true).await;
    assert!(!loop_step_status_supported(&owner, binding).await.unwrap());
    owner.close().await.unwrap();

    let f = Fixture::new().await;
    let mut wrong_artifact = exact.clone();
    wrong_artifact["artifact"]["artifact_id"] = json!("eliot-opencode-v2.rust-http-other.1");
    let (owner, _p, binding) = start_module_route(&f, wrong_artifact, true).await;
    assert!(!loop_step_status_supported(&owner, binding).await.unwrap());
    owner.close().await.unwrap();

    let f = Fixture::new().await;
    let (owner, _p, binding) = start_module_route(&f, exact, false).await;
    assert!(!loop_step_status_supported(&owner, binding).await.unwrap());
    owner.close().await.unwrap();
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
    assert_eq!(operation["result"]["outcome"], "applied");
    assert!(operation.get("native_refs").is_none());
    assert!(
        operation["diagnostic"]["native_refs"]
            .get("input_id")
            .is_none()
    );
    assert!(
        operation["diagnostic"]["native_refs"]
            .get("native_input_id")
            .is_none()
    );
    let retained =
        retained_operation(&owner.store, dispatch["operation_id"].as_str().unwrap()).await;
    assert!(retained["native_refs"]["turn_id"].is_null());
    assert!(retained["native_refs"]["input_id"].is_string());
    let projected_attempt = read(
        &owner.store,
        &p,
        "attempt.get",
        json!({"attempt_id":attempt["attempt_id"]}),
    )
    .await;
    assert!(projected_attempt.get("producers").is_none());
    let current = retained_attempt(&owner.store, attempt["attempt_id"].as_str().unwrap()).await;
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
    assert!(operation["result"].get("details").is_none());
    let retained = retained_operation(&owner.store, open["operation_id"].as_str().unwrap()).await;
    assert_eq!(
        retained["result"]["details"]["code"],
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
    assert!(operation["result"].get("details").is_none());
    let retained = retained_operation(&owner.store, goal["operation_id"].as_str().unwrap()).await;
    assert_eq!(
        retained["result"]["details"]["completion_condition"],
        "native_goal_recorded"
    );
    assert_eq!(
        retained["result"]["details"]["goal"]["activation_input_id"],
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
    assert!(configure["result"].get("details").is_none());
    let retained_configure =
        retained_operation(&owner.store, configure["operation_id"].as_str().unwrap()).await;
    assert_eq!(
        retained_configure["result"]["details"]["completion_condition"],
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
    assert!(goal["result"].get("details").is_none());
    assert!(goal.get("operation_contract").is_none());
    let retained_goal =
        retained_operation(&owner.store, goal["operation_id"].as_str().unwrap()).await;
    assert_eq!(
        retained_goal["result"]["details"]["completion_condition"],
        "native_goal_recorded"
    );
    assert_eq!(
        retained_goal["operation_contract"]["completion_condition"],
        "native_goal_recorded"
    );
    assert_eq!(
        retained_goal["operation_contract"]["native_goal_api"],
        false
    );
    assert_eq!(
        retained_goal["operation_contract"]["continuation_owner"],
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
    assert!(cancelled["result"].get("stop_operation_id").is_none());
    let retained_cancelled =
        retained_operation(&owner.store, set["operation_id"].as_str().unwrap()).await;
    assert_eq!(
        retained_cancelled["result"]["stop_operation_id"],
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
    let projected_attempt = read(
        &owner.store,
        &p,
        "attempt.get",
        json!({"attempt_id":attempt["attempt_id"]}),
    )
    .await;
    assert!(projected_attempt.get("producers").is_none());
    loop {
        let current = retained_attempt(&owner.store, attempt["attempt_id"].as_str().unwrap()).await;
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
    assert!(background["result"].get("details").is_none());
    assert!(background.get("operation_contract").is_none());
    let retained_background =
        retained_operation(&owner.store, background["operation_id"].as_str().unwrap()).await;
    assert_eq!(
        retained_background["result"]["details"]["completion_condition"],
        "native_foreground_tools_backgrounded"
    );
    assert_eq!(
        retained_background["result"]["details"]["backgrounded"],
        json!([{"type":"task","label":"fixture child"}])
    );
    assert_eq!(
        retained_background["operation_contract"]["completion_condition"],
        "native_foreground_tools_backgrounded"
    );
    assert_eq!(
        retained_background["operation_contract"]["contract_revision"],
        "opencode-background-v1"
    );
    assert_eq!(
        retained_background["operation_contract"]["replay_policy"],
        "readback_only_no_mutation_replay"
    );
    assert_eq!(f.posts(&format!("/api/session/{root}/background")), 1);
    assert_eq!(f.posts(&format!("/api/session/{root}/prompt")), 0);
    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}

async fn register_execution_diagnostic_manager(
    store: &Store,
    operator: &Principal,
    client_id: &str,
) -> Principal {
    let token = format!("execution-diagnostic-token-{}", model::new_id());
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
    .await
    .unwrap();
    store
        .authenticate(crate::model::Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

fn assert_execution_diagnostic_gap(operation: &Value, card: &str) {
    assert!(
        operation["diagnostic_gaps"].as_array().is_some_and(|gaps| {
            gaps.iter()
                .any(|gap| gap["card"] == card && gap["reason_code"] == "OBJECT_SCOPE_DAMAGED")
        }),
        "missing {card} damage gap: {operation}"
    );
}

#[tokio::test]
async fn task_dispatch_execution_diagnostic_is_scoped_and_rejects_damaged_identity() {
    let f = Fixture::new().await;
    let (owner, operator) = start(&f).await;
    let store = owner.store.clone();
    let owner_id = format!("execution-attempt-owner-{}", model::new_id());
    let gm_id = format!("execution-current-gm-{}", model::new_id());
    let unrelated_id = format!("execution-unrelated-manager-{}", model::new_id());
    let attempt_owner = register_execution_diagnostic_manager(&store, &operator, &owner_id).await;
    let current_gm = register_execution_diagnostic_manager(&store, &operator, &gm_id).await;
    let unrelated = register_execution_diagnostic_manager(&store, &operator, &unrelated_id).await;
    write(
        &store,
        &operator,
        "gm.handover",
        json!({"client_id":current_gm.client_id.clone()}),
    )
    .await
    .unwrap();

    let (stop, receiver) = watch::channel(false);
    let worker = tokio::spawn(store.clone().supervise_opencode(receiver));
    let open = write(
        &store,
        &operator,
        "agent.open",
        json!({"lane_id":"execution-diagnostic","route":"fixture"}),
    )
    .await
    .unwrap();
    wait_operation(&store, &operator, &open, "settled").await;
    let task = write(
        &store,
        &operator,
        "task.create",
        json!({
            "project_id":"fixture",
            "spec":serde_json::from_str::<Value>(include_str!("../../../config/task.example.json")).unwrap()
        }),
    )
    .await
    .unwrap();
    let attempt = write(
        &store,
        &attempt_owner,
        "task.claim",
        json!({
            "task_id":task["task_id"],
            "expected_revision":1,
            "start_owner":"controller",
            "binding_id":open["binding_id"],
            "binding_generation":1
        }),
    )
    .await
    .unwrap();
    let dispatch = write(
        &store,
        &current_gm,
        "task.dispatch",
        json!({"attempt_id":attempt["attempt_id"],"text":"fixture-only diagnostic evidence"}),
    )
    .await
    .unwrap();
    let operation_id = dispatch["operation_id"].as_str().unwrap().to_owned();
    let binding_id = open["binding_id"].as_str().unwrap().to_owned();
    let root = oc::root_id(&binding_id, 1);
    let prompt = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let request = {
                f.world
                    .lock()
                    .unwrap()
                    .requests
                    .iter()
                    .find(|request| {
                        request.method == "POST"
                            && request.path == format!("/api/session/{root}/prompt")
                    })
                    .cloned()
            };
            if let Some(request) = request {
                break request;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    wait_operation(&store, &current_gm, &dispatch, "settled").await;

    // The mock native log carries the exact prompt written by the real Store
    // dispatch. Its terminal failure becomes the retained Attempt producer's
    // Observation/EventRef through the ordinary OpenCode reader.
    let input_id = prompt.body["id"].as_str().unwrap().to_owned();
    let text = prompt.body["text"].as_str().unwrap().to_owned();
    let metadata = prompt.body["metadata"].clone();
    let delivery = prompt.body["delivery"].clone();
    let event = |id: &str, kind: &str, seq: u64, data: Value| {
        json!({
            "id":id,
            "type":kind,
            "version":1,
            "created":seq as f64,
            "durable":{"aggregateID":root,"seq":seq,"version":1},
            "data":data
        })
    };
    let lifecycle = [
        event(
            "evt_diagnostic_input_enqueued",
            "session.inbox.enqueued",
            2,
            json!({
                "sessionID":root,
                "inboxID":input_id,
                "item":{
                    "id":input_id,
                    "sessionID":root,
                    "type":"user",
                    "delivery":delivery,
                    "payload":{"text":text,"metadata":metadata}
                }
            }),
        ),
        event(
            "evt_diagnostic_execution_started",
            "session.execution.started",
            3,
            json!({"sessionID":root}),
        ),
        event(
            "evt_diagnostic_input_delivered",
            "session.inbox.delivered",
            4,
            json!({"sessionID":root,"inboxID":input_id}),
        ),
        event(
            "evt_diagnostic_execution_failed",
            "session.execution.failed",
            5,
            json!({
                "sessionID":root,
                "error":{"code":"NATIVE_ROOT_FAILED","message":"private native error text"}
            }),
        ),
    ];
    {
        let mut world = f.world.lock().unwrap();
        let mut log = world.logs.get(&root).cloned().unwrap();
        assert_eq!(log[0]["type"], "session.created");
        log.extend(lifecycle);
        world.logs.insert(root.clone(), log);
    }
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            let retained = retained_attempt(&store, attempt["attempt_id"].as_str().unwrap()).await;
            if retained["producers"].as_array().is_some_and(|producers| {
                producers.first().is_some_and(|producer| {
                    producer["terminal_evidence"].is_object()
                        && producer["terminal_evidence"]["stage"] == "execution_failed"
                })
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let owner_read = read(
        &store,
        &attempt_owner,
        "operation.get",
        json!({"operation_id":operation_id}),
    )
    .await;
    let manager_read = read(
        &store,
        &current_gm,
        "operation.get",
        json!({"operation_id":operation_id}),
    )
    .await;
    let expected_card = json!({"stage":"execution_failed","error_code":"NATIVE_ROOT_FAILED"});
    assert_eq!(owner_read["execution_diagnostic"], expected_card);
    assert_eq!(manager_read["execution_diagnostic"], expected_card);
    assert!(manager_read["execution_diagnostic"]["message"].is_null());
    let denied = store
        .call(
            unrelated,
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.code, "NOT_FOUND");

    let saved_native_refs = {
        let id = operation_id.clone();
        store
            .run(move |db| {
                db.query_row(
                    "SELECT native_refs_json FROM operations WHERE operation_id=?1",
                    rusqlite::params![id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap()
    };
    for damaged_native_refs in ["[]", "17", "\"scalar\""] {
        let id = operation_id.clone();
        let damaged_native_refs = damaged_native_refs.to_owned();
        store
            .run(move |db| {
                db.execute(
                    "UPDATE operations SET native_refs_json=?2 WHERE operation_id=?1",
                    rusqlite::params![id, damaged_native_refs],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        let projected = read(
            &store,
            &current_gm,
            "operation.get",
            json!({"operation_id":operation_id}),
        )
        .await;
        assert_eq!(projected["method"], "task.dispatch");
        assert_eq!(projected["state"], "settled");
        assert_eq!(projected["task_id"], task["task_id"]);
        assert_eq!(projected["attempt_id"], attempt["attempt_id"]);
        assert_eq!(projected["result"]["operation_id"], operation_id);
        assert!(projected.get("execution_diagnostic").is_none());
        assert_execution_diagnostic_gap(&projected, "execution_diagnostic");
        assert_execution_diagnostic_gap(&projected, "module_recovery_action_required");
        assert_execution_diagnostic_gap(&projected, "module_outcome_readback_required");
    }
    let id = operation_id.clone();
    store
        .run(move |db| {
            db.execute(
                "UPDATE operations SET native_refs_json=?2 WHERE operation_id=?1",
                rusqlite::params![id, saved_native_refs],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let saved_producers = {
        let id = attempt["attempt_id"].as_str().unwrap().to_owned();
        store
            .run(move |db| {
                db.query_row(
                    "SELECT producers_json FROM attempts WHERE attempt_id=?1",
                    rusqlite::params![id],
                    |row| row.get::<_, String>(0),
                )
                .map_err(Into::into)
            })
            .await
            .unwrap()
    };
    let mut damaged_producers: Value = serde_json::from_str(&saved_producers).unwrap();
    assert_eq!(damaged_producers[0]["assignment_id"], operation_id);
    damaged_producers[0]["native_run_id"] = json!(17);
    let id = attempt["attempt_id"].as_str().unwrap().to_owned();
    let producer_json = model::canonical(&damaged_producers).unwrap();
    store
        .run(move |db| {
            db.execute(
                "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                rusqlite::params![id, producer_json],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let projected = read(
        &store,
        &current_gm,
        "operation.get",
        json!({"operation_id":operation_id}),
    )
    .await;
    assert_eq!(projected["method"], "task.dispatch");
    assert_eq!(projected["result"]["operation_id"], operation_id);
    assert!(projected.get("execution_diagnostic").is_none());
    assert_execution_diagnostic_gap(&projected, "execution_diagnostic");
    let id = attempt["attempt_id"].as_str().unwrap().to_owned();
    store
        .run(move |db| {
            db.execute(
                "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                rusqlite::params![id, saved_producers],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    stop.send(true).unwrap();
    worker.await.unwrap().unwrap();
    owner.close().await.unwrap();
}
