//! S6 (R23/R12): durable capacity accounting, quota incidents and the
//! read-only capacity / attention projections.
use super::*;
use crate::config::Config;
use crate::runtime::{EffectOutcome, RuntimeOutcome};
use rusqlite::params;

fn fixture_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(SCHEMA).unwrap();
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES('execution_mode','{\"new_work\":\"enabled\"}')",
        [],
    )
    .unwrap();
    db
}

fn opencode_route(service: &str) -> Value {
    json!({"alias":"oc","runtime":"opencode_v2","module_artifact_id":"eliot-opencode-v2.http.1","enabled":true,
           "native_options":{"service_id":service,"connection_file":"/tmp/conn.json","expected_version":"1",
           "directory":"/tmp","model":{"id":"m1","providerID":"prov-a","variant":"v"}}})
}

fn muse_route() -> Value {
    json!({"alias":"muse-manager","runtime":"muse","module_artifact_id":"muse-sdk-1.3.0-bridge.5","enabled":true,
           "native_options":{"workspaceRoot":"/tmp","modelId":"muse-spark-1.3"}})
}

fn insert_binding(db: &Connection, id: &str, lane: &str, route: &Value, native: Option<Value>) {
    let mut state = json!({"execution":"not_observed","waiting_for":"runtime_adapter","family_completeness":"unknown"});
    let (scope_key, root) = match &native {
        Some(native) => (
            native["native_scope_key"].clone(),
            native["native_root_id"].clone(),
        ),
        None => (Value::Null, Value::Null),
    };
    if let Some(native) = native {
        state["native"] = native;
        state["observed_at_ms"] = json!(model::now_ms().unwrap());
        state["connection"] = json!("connected");
    }
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) VALUES(?1,1,?2,'inst-1',?3,'ready',?4,?5,?6,?7,1000)",
        params![
            id,
            lane,
            route["module_artifact_id"].as_str().unwrap(),
            scope_key.as_str(),
            root.as_str(),
            model::canonical(route).unwrap(),
            model::canonical(&state).unwrap()
        ],
    )
    .unwrap();
}

fn insert_task(db: &Connection, task_id: &str) {
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) VALUES(?1,'proj',1,'open','{}',1000,1000)",
        [task_id],
    )
    .unwrap();
}

fn insert_attempt(
    db: &Connection,
    attempt_id: &str,
    task_id: &str,
    binding_id: &str,
    state: &str,
    producers: Value,
) {
    insert_task(db, task_id);
    let (sub, cand) = if state == "submitted" {
        db.execute("INSERT OR IGNORE INTO artifacts(artifact_id,relative_path,kind,byte_length,created_at_ms) VALUES('art-sub','sub.json','task_submission',1,1000)", []).unwrap();
        db.execute("INSERT OR IGNORE INTO artifacts(artifact_id,relative_path,kind,byte_length,created_at_ms) VALUES('art-cand','cand.json','candidate',1,1000)", []).unwrap();
        ("art-sub", "art-cand")
    } else {
        ("", "")
    };
    let (sub, cand) = if sub.is_empty() {
        (None, None)
    } else {
        (Some(sub), Some(cand))
    };
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,start_owner,binding_id,binding_generation,state,producers_json,submission_ref,candidate_ref,created_at_ms,updated_at_ms) VALUES(?1,?2,1,'{}','op-1','controller',?3,1,?4,?5,?6,?7,1000,1000)",
        params![attempt_id, task_id, binding_id, state, model::canonical(&producers).unwrap(), sub, cand],
    )
    .unwrap();
}

fn insert_op(
    db: &Connection,
    id: &str,
    method: &str,
    state: &str,
    binding_id: Option<&str>,
    attempt_id: Option<&str>,
    native_refs: Value,
) {
    let terminal = matches!(state, "settled" | "rejected" | "cancelled");
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'op-1',?2,?3,'{}','{}',(SELECT task_id FROM attempts WHERE attempt_id=?4),?4,?5,?6,?7,?8,?9,?10,0,1000,1000)",
        params![
            id,
            format!("req-{id}"),
            method,
            attempt_id,
            binding_id,
            binding_id.map(|_| 1),
            state,
            model::canonical(&native_refs).unwrap(),
            terminal.then_some("{}"),
            terminal.then_some(1000),
        ],
    )
    .unwrap();
    if method == "task.dispatch"
        && let Some(attempt_id) = attempt_id
    {
        let linked = db
            .execute(
                "UPDATE attempts SET start_operation_id=?1 WHERE attempt_id=?2 AND start_operation_id IS NULL",
                params![id, attempt_id],
            )
            .unwrap();
        assert_eq!(linked, 1, "dispatch fixture links its exact Attempt");
    }
    retain_input_execution_observation(db, id, &native_refs);
}

fn set_op(db: &Connection, id: &str, state: &str, native_refs: Option<Value>) {
    let terminal = matches!(state, "settled" | "rejected" | "cancelled");
    let settled_at: Option<i64> = terminal.then_some(2000);
    let result: Option<&str> = terminal.then_some("{}");
    match native_refs {
        Some(refs) => {
            let updated = db.execute(
                "UPDATE operations SET state=?2,native_refs_json=?3,settled_at_ms=?4,result_json=COALESCE(result_json,?5),updated_at_ms=2000 WHERE operation_id=?1",
                params![id, state, model::canonical(&refs).unwrap(), settled_at, result],
            );
            if updated.is_ok() {
                retain_input_execution_observation(db, id, &refs);
            }
            updated
        }
        None => db.execute(
            "UPDATE operations SET state=?2,settled_at_ms=?3,result_json=COALESCE(result_json,?4),updated_at_ms=2000 WHERE operation_id=?1",
            params![id, state, settled_at, result],
        ),
    }
    .unwrap();
}

fn retain_input_execution_observation(db: &Connection, operation_id: &str, native_refs: &Value) {
    let Some(proof) = native_refs.get("input_execution") else {
        return;
    };
    let (binding_id, generation): (Option<String>, Option<i64>) = db
        .query_row(
            "SELECT binding_id,binding_generation FROM operations WHERE operation_id=?1",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let binding_id = binding_id.expect("execution proof is bound to its retained binding");
    let generation = generation.expect("execution proof is bound to its retained generation");
    let payload = model::canonical(proof).unwrap();
    let stream = format!("opencode-execution:{binding_id}:{generation}");
    let event_key = format!("{operation_id}:{}", model::digest(payload.as_bytes()));
    db.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) \
         VALUES(?1,?2,?3,?4,?5,'opencode.input_execution',?6,1000)",
        params![stream, event_key, binding_id, generation, operation_id, payload],
    )
    .unwrap();
}

fn meta_rows(db: &Connection) -> Vec<(String, String)> {
    let mut stmt = db
        .prepare("SELECT key,value_json FROM meta ORDER BY key")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

fn ledger(db: &Connection, scope_key: &str) -> Value {
    meta(db, &format!("capacity:{scope_key}"))
        .unwrap()
        .unwrap_or(Value::Null)
}

fn scope_item(db: &Connection, scope_key: &str) -> Value {
    capacity::capacity_items(db)
        .unwrap()
        .into_iter()
        .find(|item| item["scope"]["scope_key"] == scope_key)
        .unwrap_or_else(|| panic!("no capacity item for {scope_key}"))
}

fn assert_no_admission_claim(item: &Value) {
    assert!(
        item["may_admit"].is_null(),
        "capacity accounting does not assert admission: {item}"
    );
    assert!(
        item["capacity_available"].is_null(),
        "capacity accounting does not assert full-scope capacity: {item}"
    );
}

fn insert_historical_quota_incident(
    db: &Connection,
    scope_key: &str,
    error_code: &str,
    reset_at_ms: i64,
    operation_id: &str,
    opened_at_ms: i64,
) {
    let details = json!({
        "kind":"quota",
        "scope":{
            "scope_key":scope_key,
            "runtime":"opencode_v2",
            "provider":"prov-a",
            "account":null,
            "service":"svc-a",
            "native_scope_key":"opencode-v2:svc-a",
            "route_alias":"oc",
            "identity":"complete",
        },
        "native_scope_key":"opencode-v2:svc-a",
        "error_code":error_code,
        "reset_evidence":{"reset_at_ms":reset_at_ms},
        "operation_id":operation_id,
        "last_operation_id":operation_id,
    });
    db.execute(
        "INSERT INTO incidents(incident_id,dedup_key,state,occurrences,details_json,opened_at_ms,last_seen_at_ms) \
         VALUES(?1,?2,'open',1,?3,?4,?4)",
        params![
            format!("quota-fixture-{}", scope_key.replace(':', "-")),
            format!("quota:{scope_key}"),
            model::canonical(&details).unwrap(),
            opened_at_ms,
        ],
    )
    .unwrap();
}

fn proof(operation_id: &str, disposition: &str, started: bool) -> Value {
    let terminal = matches!(disposition, "completed" | "failed" | "cancelled");
    json!({"input_execution": {
        "reader_revision":"opencode-execution-log-v1",
        "operation_id":operation_id,
        "native_session_id":"ses_root",
        "native_input_id":format!("input-{operation_id}"),
        "native_run_id":if started { json!("evt_start") } else { Value::Null },
        "native_run_id_kind":if started { json!("execution_started_event") } else { Value::Null },
        "admission":{"id":"evt_enq","seq":1,"sha256":model::digest(b"capacity admission event")},
        "delivery":if started { json!({"id":"evt_del","seq":2,"sha256":model::digest(b"capacity delivery event")}) } else { Value::Null },
        "execution_started":if started { json!({"id":"evt_start","seq":3,"sha256":model::digest(b"capacity execution start event")}) } else { Value::Null },
        "terminal":if terminal { json!({
            "event":{"id":"evt_terminal","seq":4,"sha256":model::digest(b"capacity terminal event")},
            "outcome":disposition,"reason":null,"stage":null,"error_code":null,
        }) } else { Value::Null },
        "disposition":disposition,
        "uncertainty":null,
        "correlation":"durable_serialized_execution",
        "family_complete":false,
    }})
}

#[test]
fn dispatch_reservation_lifecycle_reserved_active_released() {
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    insert_attempt(&db, "att-1", "task-1", "b1", "reserved", json!([]));
    insert_op(
        &db,
        "op-1",
        "task.dispatch",
        "queued",
        Some("b1"),
        Some("att-1"),
        json!({}),
    );

    capacity::sync_operation(&db, "op-1", 1_500).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"]["op-1"];
    assert_eq!(entry["phase"], "reserved", "{entry}");
    assert_eq!(entry["kind"], "operation");
    assert_eq!(entry["admitted_at_ms"], 1000);
    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["counts"]["desired_writers"], 1);
    assert_eq!(
        item["counts"]["pending_admissions"], 1,
        "pending admissions count before running"
    );
    assert_eq!(item["counts"]["effective_writers"], 0);
    assert_eq!(item["roster"], "known");
    assert_no_admission_claim(&item);

    // Execution-start evidence on the operation's own proof: the entry
    // activates and names the evidence; admission alone never did this.
    set_op(&db, "op-1", "settled", Some(proof("op-1", "running", true)));
    capacity::sync_operation(&db, "op-1", 2_500).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"]["op-1"];
    assert_eq!(entry["phase"], "active", "{entry}");
    assert_eq!(
        entry["execution_start_ref"]["kind"],
        "execution_started_event"
    );
    assert_eq!(entry["execution_start_ref"]["native_run_id"], "evt_start");
    assert!(entry["activated_at_ms"].as_i64().unwrap() >= 2_500);
    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["counts"]["effective_writers"], 1);
    assert_eq!(item["counts"]["pending_admissions"], 0);
    assert_no_admission_claim(&item);

    // Terminal proof releases the reservation.
    set_op(
        &db,
        "op-1",
        "settled",
        Some(proof("op-1", "completed", true)),
    );
    capacity::sync_operation(&db, "op-1", 3_500).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"]["op-1"];
    assert_eq!(entry["phase"], "released", "{entry}");
    assert_eq!(entry["release_reason"], "execution_terminal:completed");
    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["counts"]["desired_writers"], 0);
    assert_eq!(item["counts"]["released_entries"], 1);
}

#[test]
fn outcome_unknown_keeps_the_reservation_until_resolution() {
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    insert_op(
        &db,
        "op-9",
        "agent.send",
        "sending",
        Some("b1"),
        None,
        json!({}),
    );
    capacity::sync_operation(&db, "op-9", 1_500).unwrap();
    assert_eq!(
        ledger(&db, "opencode_v2:svc-a")["entries"]["op-9"]["phase"],
        "reserved"
    );

    set_op(&db, "op-9", "outcome_unknown", None);
    capacity::sync_operation(&db, "op-9", 2_500).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"]["op-9"];
    assert_eq!(
        entry["phase"], "reserved",
        "unknown is not a terminal: {entry}"
    );
    assert_eq!(entry["outcome_unknown_since_ms"], 2000);
    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["counts"]["unknown_outcomes"], 1);
    assert_eq!(item["counts"]["commands_in_flight"], 1);

    // Resolution releases it (here: the operation settles at its own
    // contract boundary); the unknown marker clears.
    set_op(&db, "op-9", "settled", None);
    capacity::sync_operation(&db, "op-9", 3_500).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"]["op-9"];
    assert_eq!(entry["phase"], "released");
    assert_eq!(entry["release_reason"], "operation_settled");
    assert!(entry["outcome_unknown_since_ms"].is_null());
}

#[test]
fn cancelled_releases_and_producer_entries_follow_the_attempt() {
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    // A queued command cancelled before send releases immediately.
    insert_op(
        &db,
        "op-c",
        "agent.send",
        "queued",
        Some("b1"),
        None,
        json!({}),
    );
    capacity::sync_operation(&db, "op-c", 1_500).unwrap();
    set_op(&db, "op-c", "cancelled", None);
    capacity::sync_operation(&db, "op-c", 1_600).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"]["op-c"];
    assert_eq!(entry["phase"], "released");
    assert_eq!(entry["release_reason"], "operation_cancelled");

    // Native-manager-started work: the producer is the reservation.
    insert_attempt(
        &db,
        "att-2",
        "task-2",
        "b1",
        "running",
        json!([{"assignment_id":"asg-1","native_session_id":"ses_root","disposition":"admitted"}]),
    );
    capacity::sync_attempt(&db, "att-2", 1_700).unwrap();
    let key = "producer:att-2:asg-1";
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"][key];
    assert_eq!(entry["phase"], "reserved", "{entry}");
    assert_eq!(entry["kind"], "producer");

    let start_event = json!({
        "id":"turn-7",
        "seq":3,
        "sha256":model::digest(b"producer start event"),
    });
    let start_observation = insert_runtime_state_observation(
        &db,
        "b1",
        "runtime-state-asg-1-start",
        &json!({"turns":[{
            "sessionId":"ses_root",
            "turnId":"turn-7",
            "disposition":"running",
            "event":start_event,
        }]}),
        1_800,
    );
    db.execute(
        "UPDATE attempts SET producers_json=?1 WHERE attempt_id='att-2'",
        params![
            model::canonical(&json!([{
                "assignment_id":"asg-1",
                "attempt_id":"att-2",
                "task_id":"task-2",
                "native_session_id":"ses_root",
                "native_input_id":"input-asg-1",
                "native_run_id":"turn-7",
                "observed_in":start_observation,
                "disposition":"running",
            }]))
            .unwrap()
        ],
    )
    .unwrap();
    capacity::sync_attempt(&db, "att-2", 1_800).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"][key];
    assert_eq!(entry["phase"], "active");
    assert_eq!(entry["execution_start_ref"]["turn_id"], "turn-7");
    assert_eq!(
        entry["execution_start_ref"]["observation_id"],
        start_observation
    );

    let terminal_event = json!({
        "id":"turn-7-terminal",
        "seq":4,
        "sha256":model::digest(b"producer terminal event"),
    });
    let terminal_observation = insert_runtime_state_observation(
        &db,
        "b1",
        "runtime-state-asg-1-terminal",
        &json!({"turns":[{
            "sessionId":"ses_root",
            "turnId":"turn-7",
            "terminal":"failed",
            "event":terminal_event,
        }]}),
        1_900,
    );
    db.execute(
        "UPDATE attempts SET producers_json=?1 WHERE attempt_id='att-2'",
        params![
            model::canonical(&json!([{
                "assignment_id":"asg-1",
                "attempt_id":"att-2",
                "task_id":"task-2",
                "native_session_id":"ses_root",
                "native_input_id":"input-asg-1",
                "native_run_id":"turn-7",
                "observed_in":start_observation,
                "disposition":"failed",
                "terminal_evidence":{"observation_id":terminal_observation,"event":terminal_event},
            }]))
            .unwrap()
        ],
    )
    .unwrap();
    capacity::sync_attempt(&db, "att-2", 1_900).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"][key];
    assert_eq!(entry["phase"], "released");
    assert_eq!(entry["release_reason"], "execution_terminal:failed");
}

fn root_admission_route() -> crate::config::Route {
    serde_json::from_value(json!({
        "alias":"oc",
        "runtime":"opencode_v2",
        "module_artifact_id":"eliot-opencode-v2.http.1",
        "enabled":true,
        "native_options":{
            "service_id":"svc-placeholder",
            "connection_file":"/tmp/conn.json",
            "expected_version":"1",
            "directory":"/tmp",
            "model":{"id":"m1","providerID":"prov-a","variant":"v"}
        },
        "admission_policy":{"max_concurrent_roots":2}
    }))
    .unwrap()
}

fn launch_request(client_request_id: &str, route: &str) -> Value {
    json!({
        "client_request_id":client_request_id,
        "plan_digest":format!("sha256:{}", "a".repeat(64)),
        "task_id":"task-placeholder",
        "expected_task_revision":1,
        "route":route,
        "agent_profile":"default",
        "mcp_profile":"default",
        "mcp_surface":"default",
        "workspace_policy":"default",
        "requested_model":null,
        "requested_effort":null,
        "budget":{"max_turns":null,"max_duration_ms":null,"max_cost_units":null},
        "stop_conditions":[],
        "purpose":"root claim exclusion fixture"
    })
}

fn assert_root_admission_unknown(db: &Connection, route: &crate::config::Route) {
    let projection =
        launcher::route_admission(db, route, None, Some("launch-placeholder")).unwrap();
    assert_eq!(
        projection.root_limit,
        provider_conditions::RootLimitProjection::CapacityUnknown {
            max_concurrent_roots: 2
        }
    );
    assert!(matches!(
        projection.decision,
        provider_conditions::RouteAdmissionDecision::Hold { code, .. }
            if code == "ROUTE_CAPACITY_UNKNOWN"
    ));
}

fn insert_retained_root_claim_operation(
    db: &Connection,
    route: &crate::config::Route,
    operation_id: &str,
    admission: Value,
) {
    let route_value = serde_json::to_value(route).unwrap();
    let route_digest = model::digest(model::canonical(&route_value).unwrap().as_bytes());
    let mut retained_route = route_value;
    retained_route["admission"] = admission;
    let route_alias = route.alias.as_str();
    let task_id = "task-placeholder";
    let requested_facts = json!({"route":route_alias});
    let authority = json!({
        "schema_version":1,
        "task":{
            "task_id":task_id,
            "expected_revision":1,
            "revision":1
        },
        "attempt_action":"claim_new",
        "current_attempt":{"attempt_id":null},
        "route":retained_route.clone(),
        "selected_route_sha256":route_digest,
        "requested_plan_facts":requested_facts.clone()
    });
    let plan_digest = format!(
        "sha256:{}",
        model::digest(model::canonical(&authority).unwrap().as_bytes())
    );
    let client_request_id = format!("client-{operation_id}");
    let mut original = launch_request(&client_request_id, route_alias);
    original["plan_digest"] = json!(plan_digest.clone());
    let manifest = json!({
        "manifest_version":"eliot-launch-manifest-v1",
        "state":"pending_workspace",
        "client_request_id":client_request_id,
        "plan_digest":plan_digest.clone(),
        "request":requested_facts,
        "task":{
            "task_id":task_id,
            "expected_revision":1,
            "observed_revision":1,
            "attempt_action":"claim_new",
            "attempt_id":null
        },
        "workspace":{"lease_state":"pending"},
        "runtime":{"route":retained_route},
        "binding":{"binding_id":null,"generation":null},
        "actor":{"kind":"direct"}
    });
    let effective = json!({
        "launch_manifest":manifest,
        "launch_plan_authority":authority
    });
    let result = json!({
        "operation_id":operation_id,
        "plan_digest":plan_digest,
        "task_id":task_id,
        "task_revision":1,
        "attempt_id":null,
        "state":"queued",
        "launch_state":"pending_workspace"
    });
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES(?1,'manager',?2,'swarm.launch',?3,?4,?5,NULL,NULL,NULL,'queued','{}',?6,NULL,0,1000,1000)",
        params![
            operation_id,
            client_request_id,
            model::canonical(&original).unwrap(),
            model::canonical(&effective).unwrap(),
            task_id,
            model::canonical(&result).unwrap()
        ],
    )
    .unwrap();
}

fn replace_retained_root_admission(
    db: &Connection,
    operation_id: &str,
    manifest_admission: Value,
    authority_admission: Value,
) {
    let (original_json, effective_json, result_json): (String, String, String) = db
        .query_row(
            "SELECT original_request_json,effective_request_json,result_json \
             FROM operations WHERE operation_id=?1 AND method='swarm.launch'",
            [operation_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .unwrap();
    let mut original: Value = serde_json::from_str(&original_json).unwrap();
    let mut effective: Value = serde_json::from_str(&effective_json).unwrap();
    let mut result: Value = serde_json::from_str(&result_json).unwrap();
    effective["launch_manifest"]["runtime"]["route"]["admission"] = manifest_admission;
    effective["launch_plan_authority"]["route"]["admission"] = authority_admission;
    let plan_digest = format!(
        "sha256:{}",
        model::digest(
            model::canonical(&effective["launch_plan_authority"])
                .unwrap()
                .as_bytes()
        )
    );
    effective["launch_manifest"]["plan_digest"] = json!(plan_digest.clone());
    original["plan_digest"] = json!(plan_digest.clone());
    result["plan_digest"] = json!(plan_digest);
    let changed = db
        .execute(
            "UPDATE operations SET original_request_json=?2,effective_request_json=?3,result_json=?4 \
             WHERE operation_id=?1 AND method='swarm.launch' AND state='queued'",
            params![
                operation_id,
                model::canonical(&original).unwrap(),
                model::canonical(&effective).unwrap(),
                model::canonical(&result).unwrap()
            ],
        )
        .unwrap();
    assert_eq!(
        changed, 1,
        "the retained root-claim fixture must remain exact"
    );
}

#[test]
fn root_claim_requires_the_complete_typed_admission_projection() {
    let db = fixture_db();
    db.execute_batch(WORKSPACE_SCHEMA).unwrap();
    db.execute_batch(OWNED_SERVICE_SCHEMA).unwrap();
    let route = root_admission_route();
    let operation_id = "launch-typed-admission";
    insert_task(&db, "task-placeholder");

    // Use the producer's complete projection, including its tagged decision
    // object, in both immutable launch authorities.
    let producer_admission = serde_json::to_value(
        launcher::route_admission(&db, &route, None, Some(operation_id)).unwrap(),
    )
    .unwrap();
    assert_eq!(
        producer_admission["decision"],
        json!({"decision":"admit"}),
        "RouteAdmissionDecision is an internally tagged object"
    );
    assert!(
        serde_json::from_value::<provider_conditions::RouteAdmissionProjection>(
            producer_admission.clone()
        )
        .is_ok()
    );
    insert_retained_root_claim_operation(&db, &route, operation_id, producer_admission.clone());
    assert_eq!(
        capacity::root_claims_for_route(&db, &route, None, None).unwrap(),
        Some(1),
        "a retained, digest-consistent typed Admit launch is one exact root claim"
    );

    // The old synthetic scalar is malformed as a complete projection and
    // cannot turn retained launch data into a root claim.
    let legacy_flat_admission = json!("admit");
    replace_retained_root_admission(
        &db,
        operation_id,
        legacy_flat_admission.clone(),
        legacy_flat_admission,
    );
    assert_eq!(
        capacity::root_claims_for_route(&db, &route, None, None).unwrap(),
        None,
        "the complete projection decoder rejects the old flat admission value"
    );

    // Even a structurally valid changed HOLD remains non-claiming.
    let mut changed_hold = producer_admission.clone();
    changed_hold["decision"] = json!({
        "decision":"hold",
        "code":"ROUTE_CAPACITY_UNKNOWN",
        "until_ms":null
    });
    changed_hold["root_limit"] = json!({
        "status":"capacity_unknown",
        "max_concurrent_roots":2
    });
    assert!(
        serde_json::from_value::<provider_conditions::RouteAdmissionProjection>(
            changed_hold.clone()
        )
        .is_ok()
    );
    replace_retained_root_admission(
        &db,
        operation_id,
        changed_hold.clone(),
        changed_hold.clone(),
    );
    assert_eq!(
        capacity::root_claims_for_route(&db, &route, None, None).unwrap(),
        None,
        "a valid retained Hold decision is not an admitted root"
    );

    // The manifest and private authority must agree on the exact projection.
    replace_retained_root_admission(&db, operation_id, producer_admission, changed_hold);
    assert_eq!(
        capacity::root_claims_for_route(&db, &route, None, None).unwrap(),
        None,
        "a changed authority projection cannot diverge from the manifest"
    );
}

#[test]
fn only_the_exact_pristine_launch_placeholder_is_excluded_from_root_claims() {
    let db = fixture_db();
    db.execute_batch(WORKSPACE_SCHEMA).unwrap();
    db.execute_batch(OWNED_SERVICE_SCHEMA).unwrap();
    let route = root_admission_route();
    let request_id = "placeholder-client-request";
    let original = model::canonical(&launch_request(request_id, "oc")).unwrap();
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,due_at_ms,created_at_ms,updated_at_ms) \
         VALUES('launch-placeholder','manager',?1,'swarm.launch',?2,'{}','queued',0,1000,1000)",
        params![request_id, original],
    )
    .unwrap();

    assert_eq!(
        capacity::root_claims_for_route(&db, &route, Some("launch-placeholder"), None).unwrap(),
        Some(0),
        "the exact queued request placeholder is excluded before its manifest is retained"
    );
    let admitted =
        launcher::route_admission(&db, &route, None, Some("launch-placeholder")).unwrap();
    assert_eq!(
        admitted.root_limit,
        provider_conditions::RootLimitProjection::WithinLimit {
            max_concurrent_roots: 2,
            current_claims: 0,
        }
    );

    assert!(
        capacity::root_claims_for_route(&db, &route, Some("another-operation"), None)
            .unwrap()
            .is_none(),
        "an otherwise identical row is unknown unless its exact operation is excluded"
    );

    db.execute_batch("PRAGMA ignore_check_constraints=ON;")
        .unwrap();
    db.execute(
        "UPDATE operations SET original_request_json='{truncated' WHERE operation_id='launch-placeholder'",
        [],
    )
    .unwrap();
    db.execute_batch("PRAGMA ignore_check_constraints=OFF;")
        .unwrap();
    assert_root_admission_unknown(&db, &route);

    let request = launch_request(request_id, "oc");
    db.execute(
        "UPDATE operations SET original_request_json=?1,client_request_id='different-client' WHERE operation_id='launch-placeholder'",
        [model::canonical(&request).unwrap()],
    )
    .unwrap();
    assert_root_admission_unknown(&db, &route);

    let request = launch_request(request_id, "another-route");
    db.execute(
        "UPDATE operations SET original_request_json=?2,client_request_id=?1 WHERE operation_id='launch-placeholder'",
        params![request_id, model::canonical(&request).unwrap()],
    )
    .unwrap();
    assert_root_admission_unknown(&db, &route);
}

fn insert_runtime_state_observation(
    db: &Connection,
    binding_id: &str,
    key: &str,
    state: &Value,
    recorded_at_ms: i64,
) -> i64 {
    db.execute(
        "INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,kind,payload_json,recorded_at_ms) \
         VALUES(?1,?2,?3,1,'runtime.state',?4,?5)",
        params![
            format!("runtime-state:{binding_id}:1"),
            key,
            binding_id,
            model::canonical(state).unwrap(),
            recorded_at_ms,
        ],
    )
    .unwrap();
    db.last_insert_rowid()
}

#[test]
fn terminal_producer_release_preserves_original_dispatch_and_start_evidence_once() {
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    let start_event = json!({
        "id":"turn-7",
        "seq":3,
        "sha256":model::digest(b"original execution-start event"),
    });
    let start_observation = insert_runtime_state_observation(
        &db,
        "b1",
        "runtime-state-start-turn-7",
        &json!({
            "turns":[{
                "sessionId":"ses_root",
                "turnId":"turn-7",
                "disposition":"running",
                "event":start_event,
            }]
        }),
        1_700,
    );
    let producer = json!([{
        "assignment_id":"dispatch-producer-1",
        "dispatch_operation_id":"dispatch-producer-1",
        "attempt_id":"att-producer-1",
        "task_id":"task-producer-1",
        "native_session_id":"ses_root",
        "native_input_id":"input-producer-1",
        "native_run_id":"turn-7",
        "observed_in":start_observation,
        "disposition":"running",
    }]);
    insert_attempt(
        &db,
        "att-producer-1",
        "task-producer-1",
        "b1",
        "running",
        producer,
    );
    insert_op(
        &db,
        "dispatch-producer-1",
        "task.dispatch",
        "sending",
        Some("b1"),
        Some("att-producer-1"),
        json!({}),
    );
    db.execute(
        "UPDATE operations SET task_id='task-producer-1' WHERE operation_id='dispatch-producer-1'",
        [],
    )
    .unwrap();
    db.execute(
        "UPDATE attempts SET start_operation_id='dispatch-producer-1' WHERE attempt_id='att-producer-1'",
        [],
    )
    .unwrap();

    let producer_key = "producer:att-producer-1:dispatch-producer-1";
    capacity::sync_attempt(&db, "att-producer-1", 1_700).unwrap();
    let started = ledger(&db, "opencode_v2:svc-a")["entries"][producer_key].clone();
    assert_eq!(started["phase"], "active", "{started}");
    assert_eq!(
        started["execution_identity"]["operation_id"],
        "dispatch-producer-1"
    );
    assert_eq!(
        started["execution_identity"]["source_observation_id"],
        start_observation
    );
    assert_eq!(
        started["execution_identity"]["execution_start_event_ref"],
        start_event
    );

    let terminal_event = json!({
        "id":"terminal-event-7",
        "seq":4,
        "sha256":model::digest(b"terminal event for turn-7"),
    });
    let terminal_observation = insert_runtime_state_observation(
        &db,
        "b1",
        "runtime-state-terminal-turn-7",
        &json!({
            "turns":[{
                "sessionId":"ses_root",
                "turnId":"turn-7",
                "terminal":"failed",
                "event":terminal_event,
            }]
        }),
        1_800,
    );
    let terminal_producer = json!([{
        "assignment_id":"dispatch-producer-1",
        "dispatch_operation_id":"dispatch-producer-1",
        "attempt_id":"att-producer-1",
        "task_id":"task-producer-1",
        "native_session_id":"ses_root",
        "native_input_id":"input-producer-1",
        "native_run_id":"turn-7",
        "observed_in":start_observation,
        "disposition":"failed",
        "terminal_evidence":{
            "observation_id":terminal_observation,
            "event":terminal_event,
        },
    }]);
    db.execute(
        "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
        params![
            "att-producer-1",
            model::canonical(&terminal_producer).unwrap()
        ],
    )
    .unwrap();

    capacity::sync_attempt(&db, "att-producer-1", 1_800).unwrap();
    let released = ledger(&db, "opencode_v2:svc-a")["entries"][producer_key].clone();
    assert_eq!(released["phase"], "released", "{released}");
    assert_eq!(released["release_reason"], "execution_terminal:failed");
    assert_eq!(released["released_at_ms"], 1_800);
    assert_eq!(
        released["execution_identity"]["operation_id"],
        "dispatch-producer-1"
    );
    assert_eq!(
        released["execution_identity"]["source_observation_id"],
        start_observation
    );
    assert_eq!(
        released["execution_identity"]["execution_start_event_ref"],
        start_event
    );
    assert_eq!(
        released["execution_identity"]["terminal_event_ref"],
        terminal_event
    );
    assert_eq!(
        released["execution_identity"]["terminal_disposition"],
        "failed"
    );
    assert_eq!(
        released["execution_start_ref"],
        started["execution_start_ref"]
    );

    capacity::sync_attempt(&db, "att-producer-1", 1_900).unwrap();
    let after_repeat = ledger(&db, "opencode_v2:svc-a");
    let entry = &after_repeat["entries"][producer_key];
    assert_eq!(entry["phase"], "released");
    assert_eq!(
        entry["released_at_ms"], 1_800,
        "release is retained exactly once"
    );
    assert_eq!(
        after_repeat["entries"]
            .as_object()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with("producer:att-producer-1:"))
            .count(),
        1
    );
}

#[test]
fn scopes_are_isolated_per_service() {
    // §13 scenario shape: several managers share the infrastructure;
    // each scope's counts name only its own recorded work.
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    insert_binding(&db, "b2", "lane-2", &opencode_route("svc-a"), None);
    insert_binding(&db, "b3", "lane-3", &opencode_route("svc-b"), None);
    insert_attempt(&db, "att-1", "task-1", "b1", "running", json!([]));
    insert_op(
        &db,
        "op-a",
        "task.dispatch",
        "settled",
        Some("b1"),
        Some("att-1"),
        proof("op-a", "running", true),
    );
    insert_attempt(&db, "att-3", "task-3", "b3", "reserved", json!([]));
    insert_op(
        &db,
        "op-b",
        "task.dispatch",
        "queued",
        Some("b3"),
        Some("att-3"),
        json!({}),
    );
    capacity::sync_operation(&db, "op-a", 1_500).unwrap();
    capacity::sync_operation(&db, "op-b", 1_500).unwrap();

    let a = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(a["bindings"].as_array().unwrap().len(), 2);
    assert_eq!(a["counts"]["desired_writers"], 1);
    assert_eq!(a["counts"]["effective_writers"], 1);
    assert_no_admission_claim(&a);
    let b = scope_item(&db, "opencode_v2:svc-b");
    assert_eq!(b["counts"]["desired_writers"], 1);
    assert_eq!(b["counts"]["pending_admissions"], 1);
    assert_eq!(b["counts"]["effective_writers"], 0);
    assert_no_admission_claim(&b);

    // The accounting and attention projections do not turn roster counts
    // into an admission decision.
    let attention = capacity::attention_report(&db, 200, 0).unwrap();
    assert!(
        attention["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["kind"] != "capacity_available"),
        "attention must not assert admission from accounting: {}",
        attention["items"]
    );
}

#[test]
fn unknown_roster_never_yields_a_capacity_claim() {
    let db = fixture_db();
    // A route with no recorded service identity: partial scope.
    insert_binding(&db, "b1", "lane-1", &muse_route(), None);
    insert_attempt(&db, "att-1", "task-1", "b1", "reserved", json!([]));
    insert_op(
        &db,
        "op-1",
        "task.dispatch",
        "queued",
        Some("b1"),
        Some("att-1"),
        json!({}),
    );
    capacity::sync_operation(&db, "op-1", 1_500).unwrap();
    let item = scope_item(&db, "muse:binding:b1");
    assert_eq!(item["scope"]["identity"], "partial");
    assert_eq!(item["counts"]["pending_admissions"], 1);
    assert_eq!(item["roster"], "unknown");
    assert_eq!(item["roster_reason"], "scope_identity_partial");
    assert_no_admission_claim(&item);
    let attention = capacity::attention_report(&db, 200, 0).unwrap();
    assert!(
        attention["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|i| i["kind"] != "capacity_available"),
        "an unknown roster must not advise starting another writer: {}",
        attention["items"]
    );
}

#[test]
fn diverged_or_unattributed_roster_is_unknown() {
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    db.execute(
        "UPDATE bindings SET native_root_id='ses_root',native_scope_key='opencode-v2:svc-a' WHERE binding_id='b1'",
        [],
    )
    .unwrap();
    insert_attempt(&db, "att-1", "task-1", "b1", "reserved", json!([]));
    insert_op(
        &db,
        "op-1",
        "task.dispatch",
        "queued",
        Some("b1"),
        Some("att-1"),
        json!({}),
    );

    // Seed the real source-derived ledger entry first. Then change the durable
    // operation outcome without syncing it: the retained ledger must expose
    // the evidence divergence instead of treating a never-created ledger as
    // the test fixture.
    capacity::sync_operation(&db, "op-1", 1_500).unwrap();
    let seeded = ledger(&db, "opencode_v2:svc-a");
    assert_eq!(seeded["entries"]["op-1"]["kind"], "operation");
    assert_eq!(seeded["entries"]["op-1"]["attempt_id"], "att-1");
    assert_eq!(seeded["entries"]["op-1"]["phase"], "reserved");
    set_op(&db, "op-1", "outcome_unknown", None);

    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["roster"], "unknown");
    assert_eq!(
        item["roster_reason"], "ledger_entry_diverged:op-1",
        "{item}"
    );
    assert_no_admission_claim(&item);

    // Syncing the retained unknown outcome restores a matching roster without
    // releasing its reservation. Native activity outside the retained family
    // must then make the roster unknown for the independent attribution gap.
    capacity::sync_operation(&db, "op-1", 2_500).unwrap();
    let synced = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(synced["roster"], "known", "{synced}");
    assert_eq!(synced["counts"]["unknown_outcomes"], 1, "{synced}");
    assert_eq!(
        ledger(&db, "opencode_v2:svc-a")["entries"]["op-1"]["phase"],
        "reserved"
    );
    let native = json!({
        "native_root_id": "ses_root", "native_scope_key": "opencode-v2:svc-a",
        "session": {"sessionId": "ses_root"}, "turns": [],
        "observed_children": [
            {"sessionId": "ses_child", "parentSessionId": "ses_foreign", "observed_now": true,
             "execution_disposition": "running", "last_turn": null},
        ],
        "pending_requests": [], "family_completeness": "partial",
    });
    db.execute(
        "UPDATE bindings SET native_root_id='ses_root',state_json=json_set(state_json,'$.native',json(?1),'$.observed_at_ms',?2) WHERE binding_id='b1'",
        params![model::canonical(&native).unwrap(), model::now_ms().unwrap()],
    )
    .unwrap();
    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["roster"], "unknown", "{item}");
    assert!(
        item["roster_reason"]
            .as_str()
            .unwrap()
            .starts_with("unattributed_native_activity:ses_child"),
        "{item}"
    );
}

#[test]
fn quota_incident_carries_scope_and_reset_and_never_edits_config() {
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    insert_op(
        &db,
        "op-1",
        "agent.send",
        "sending",
        Some("b1"),
        None,
        json!({}),
    );
    capacity::sync_operation(&db, "op-1", 1_500).unwrap();
    let op = operations::get_operation(&db, "op-1").unwrap();
    let rejected = RuntimeOutcome {
        operation_id: "op-1".into(),
        outcome: EffectOutcome::Rejected,
        native_scope_key: Some("opencode-v2:svc-a".into()),
        native_root_id: Some("ses_root".into()),
        turn_id: None,
        native_input_id: None,
        details: json!({"code": "RATE_LIMITED", "reset_at_ms": 4102444800000i64}),
    };

    let routes_before: String = db
        .query_row(
            "SELECT route_json FROM bindings WHERE binding_id='b1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let meta_before = meta_rows(&db);

    // RuntimeOutcome is not typed provider-condition evidence and cannot
    // create an incident, even when its details resemble a quota response.
    capacity::note_outcome(&db, &op, &rejected, 2_000).unwrap();
    let untyped_incidents: i64 = db
        .query_row(
            "SELECT count(*) FROM incidents WHERE dedup_key LIKE 'quota:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(untyped_incidents, 0);

    // This fixture models an incident retained by the historical typed
    // writer, which projections may still expose without mutating it.
    insert_historical_quota_incident(
        &db,
        "opencode_v2:svc-a",
        "RATE_LIMITED",
        4102444800000,
        "op-1",
        1_900,
    );
    let retained_before: (String, String, i64, String, i64, i64) = db
        .query_row(
            "SELECT incident_id,state,occurrences,details_json,opened_at_ms,last_seen_at_ms \
             FROM incidents WHERE dedup_key='quota:opencode_v2:svc-a'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    let details: Value = serde_json::from_str(&retained_before.3).unwrap();
    assert_eq!(details["kind"], "quota");
    assert_eq!(details["scope"]["scope_key"], "opencode_v2:svc-a");
    assert_eq!(details["scope"]["provider"], "prov-a");
    assert_eq!(details["native_scope_key"], "opencode-v2:svc-a");
    assert_eq!(details["error_code"], "RATE_LIMITED");
    assert_eq!(details["reset_evidence"]["reset_at_ms"], 4102444800000i64);

    let item = scope_item(&db, "opencode_v2:svc-a");
    assert!(item["quota_incident"].is_null());
    assert_eq!(item["historical_quota_incident"]["historical"], true);
    assert_eq!(
        item["historical_quota_incident"]["error_code"],
        "RATE_LIMITED"
    );
    assert_eq!(
        item["historical_quota_incident"]["reset_evidence"]["reset_at_ms"],
        4102444800000i64
    );
    assert_eq!(
        item["historical_quota_incident"]["first_operation_id"],
        "op-1"
    );
    assert_eq!(
        item["historical_quota_incident"]["last_operation_id"],
        "op-1"
    );
    assert_no_admission_claim(&item);

    // Repeated and unrelated outcomes cannot increment, resolve, or rewrite
    // a typed historical incident.
    capacity::note_outcome(&db, &op, &rejected, 2_100).unwrap();
    let other = RuntimeOutcome {
        operation_id: "op-1".into(),
        outcome: EffectOutcome::Rejected,
        native_scope_key: Some("opencode-v2:svc-a".into()),
        native_root_id: Some("ses_root".into()),
        turn_id: None,
        native_input_id: None,
        details: json!({"code": "NATIVE_SCHEMA_ERROR"}),
    };
    capacity::note_outcome(&db, &op, &other, 2_200).unwrap();
    let applied = RuntimeOutcome {
        operation_id: "op-1".into(),
        outcome: EffectOutcome::Applied,
        native_scope_key: Some("opencode-v2:svc-a".into()),
        native_root_id: Some("ses_root".into()),
        turn_id: None,
        native_input_id: None,
        details: json!({}),
    };
    capacity::note_outcome(&db, &op, &applied, 2_300).unwrap();

    let retained_after: (String, String, i64, String, i64, i64) = db
        .query_row(
            "SELECT incident_id,state,occurrences,details_json,opened_at_ms,last_seen_at_ms \
             FROM incidents WHERE dedup_key='quota:opencode_v2:svc-a'",
            [],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .unwrap();
    assert_eq!(retained_before, retained_after);
    let quota_count: i64 = db
        .query_row(
            "SELECT count(*) FROM incidents WHERE dedup_key LIKE 'quota:%'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(quota_count, 1);
    let routes_after: String = db
        .query_row(
            "SELECT route_json FROM bindings WHERE binding_id='b1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(routes_before, routes_after);
    assert_eq!(
        meta_before,
        meta_rows(&db),
        "untyped outcome handling writes no meta/config record"
    );
}
fn dump_tables(db: &Connection) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for table in [
        "meta",
        "bindings",
        "operations",
        "attempts",
        "observations",
        "incidents",
        "tasks",
    ] {
        let mut stmt = db.prepare(&format!("SELECT * FROM {table}")).unwrap();
        let cols = stmt.column_count();
        let rows = stmt
            .query_map([], |r| {
                let mut parts = Vec::new();
                for i in 0..cols {
                    parts.push(format!(
                        "{:?}",
                        r.get::<_, rusqlite::types::Value>(i).unwrap()
                    ));
                }
                Ok(parts.join("|"))
            })
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let mut rows = rows;
        rows.sort();
        for row in rows {
            out.push((table.to_string(), row));
        }
    }
    out
}

#[test]
fn attention_projection_addresses_exact_facts_and_is_read_only() {
    let db = fixture_db();
    let native = json!({
        "native_root_id": "ses_root", "native_scope_key": "opencode-v2:svc-a",
        "session": {"sessionId": "ses_root"},
        "turns": [{"sessionId": "ses_root", "turnId": "turn-root", "terminal": null, "disposition": "running"}],
        "observed_children": [
            {"sessionId": "ses_child", "parentSessionId": "ses_root", "observed_now": true,
             "execution_disposition": "running",
             "last_turn": {"sessionId": "ses_child", "turnId": "turn-child", "terminal": null, "disposition": "running"}},
            {"sessionId": "ses_done", "parentSessionId": "ses_root", "observed_now": true,
             "execution_disposition": "completed",
             "last_turn": {"sessionId": "ses_done", "turnId": "turn-done", "terminal": "completed", "disposition": "completed"}},
        ],
        "pending_requests": [
            {"session_id": "ses_child", "request_id": "frm_1", "kind": "form", "fingerprint": "fp-1", "observed_now": true},
            {"session_id": "ses_child", "request_id": "per_1", "kind": "permission", "fingerprint": "fp-2", "observed_now": false},
        ],
        "family_completeness": "partial",
    });
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), Some(native));
    insert_op(
        &db,
        "op-q",
        "agent.send",
        "queued",
        Some("b1"),
        None,
        json!({}),
    );
    insert_attempt(&db, "att-1", "task-1", "b1", "submitted", json!([]));

    let before = dump_tables(&db);
    let attention = capacity::attention_report(&db, 200, 0).unwrap();
    let capacity = capacity::capacity_report(&db, 200, 0).unwrap();
    assert_eq!(before, dump_tables(&db), "projections write nothing");
    assert_eq!(capacity["projection"]["source_kind"], "capacity_accounting");
    assert_eq!(
        attention["projection"]["source_kind"],
        "attention_projection"
    );

    let items = attention["items"].as_array().unwrap();
    let kinds: Vec<&str> = items.iter().map(|i| i["kind"].as_str().unwrap()).collect();
    // The live form request is addressed by its exact ID + fingerprint;
    // the retained, no-longer-observed permission request is not.
    let waiting: Vec<&Value> = items
        .iter()
        .filter(|i| i["kind"] == "waiting_for_native_request")
        .collect();
    assert_eq!(waiting.len(), 1, "{kinds:?}");
    assert_eq!(waiting[0]["address"]["request_id"], "frm_1");
    assert_eq!(waiting[0]["address"]["fingerprint"], "fp-1");
    assert_eq!(waiting[0]["address"]["session_id"], "ses_child");
    assert_eq!(waiting[0]["suggested_action"]["method"], "agent.reply");
    assert_eq!(waiting[0]["source"]["kind"], "binding_observation");
    assert_eq!(waiting[0]["source"]["stale"], false);
    // The running child is awaited, and it blocks the root's open turn.
    assert!(kinds.contains(&"waiting_for_child_result"), "{kinds:?}");
    let blocking: Vec<&Value> = items
        .iter()
        .filter(|i| i["kind"] == "foreground_tool_blocking")
        .collect();
    assert_eq!(blocking.len(), 1, "{kinds:?}");
    assert_eq!(blocking[0]["address"]["session_id"], "ses_child");
    assert_eq!(blocking[0]["address"]["turn_id"], "turn-child");
    assert_eq!(blocking[0]["address"]["blocked_session_id"], "ses_root");
    // The queued controller input has not been consumed.
    let queued: Vec<&Value> = items
        .iter()
        .filter(|i| i["kind"] == "input_queued_not_consumed")
        .collect();
    assert_eq!(queued.len(), 1, "{kinds:?}");
    assert_eq!(queued[0]["address"]["operation_id"], "op-q");
    assert_eq!(queued[0]["address"]["stage"], "queued_not_sent");
    // Submitted work awaits the manager's own decision.
    let actionable: Vec<&Value> = items
        .iter()
        .filter(|i| i["kind"] == "manager_actionable")
        .collect();
    assert_eq!(actionable.len(), 1, "{kinds:?}");
    assert_eq!(actionable[0]["address"]["attempt_id"], "att-1");
    assert_eq!(actionable[0]["suggested_action"]["method"], "task.accept");
    // A fresh observation is not stale.
    assert!(!kinds.contains(&"observation_stale"), "{kinds:?}");

    // Age the observation past the stale bound: the stale item appears
    // and the live-evidence items (requests, children) disappear rather
    // than being re-addressed from old facts.
    db.execute(
        "UPDATE bindings SET state_json=json_set(state_json,'$.observed_at_ms',?1) WHERE binding_id='b1'",
        params![model::now_ms().unwrap() - capacity::STALE_AFTER_MS - 1],
    )
    .unwrap();
    let attention = capacity::attention_report(&db, 200, 0).unwrap();
    let kinds: Vec<&str> = attention["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["kind"].as_str().unwrap())
        .collect();
    assert!(kinds.contains(&"observation_stale"), "{kinds:?}");
    assert!(!kinds.contains(&"waiting_for_native_request"), "{kinds:?}");
    assert!(!kinds.contains(&"waiting_for_child_result"), "{kinds:?}");
    let stale_item = attention["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["kind"] == "observation_stale")
        .unwrap();
    assert_eq!(stale_item["source"]["stale"], true);
    assert_eq!(stale_item["suggested_action"]["method"], "agent.refresh");
}

#[test]
fn doctor_embeds_capacity_attention_and_the_quota_finding() {
    let db = fixture_db();
    insert_binding(&db, "b1", "lane-1", &opencode_route("svc-a"), None);
    insert_historical_quota_incident(
        &db,
        "opencode_v2:svc-a",
        "QUOTA_EXCEEDED",
        4102444800000,
        "op-1",
        1_900,
    );
    let inspection = crate::doctor::inspect(&db, &Config::default()).unwrap();
    let report = &inspection.report;
    assert_eq!(
        report["capacity"]["projection"]["source_kind"],
        "capacity_accounting"
    );
    assert_eq!(
        report["attention"]["projection"]["source_kind"],
        "attention_projection"
    );
    let scope = report["capacity"]["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["scope"]["scope_key"] == "opencode_v2:svc-a")
        .unwrap();
    assert_eq!(
        scope["historical_quota_incident"]["error_code"],
        "QUOTA_EXCEEDED"
    );
    let codes: Vec<&str> = report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|finding| finding["code"].as_str().unwrap())
        .collect();
    assert!(codes.contains(&"CAPACITY_QUOTA_INCIDENT"), "{codes:?}");
}
// ---------------------------------------------------------------------------
// Hook-level test: admission through the real Store records the
// reservation, and cancelling the operation releases it.
// ---------------------------------------------------------------------------

use crate::platform::{DataRoot, bootstrap_credential};
use std::sync::Arc;

#[tokio::test]
async fn store_admission_records_and_cancel_releases_the_reservation() {
    let directory = std::env::temp_dir().join(format!("eliot-capacity-test-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut cfg = Config::default();
    cfg.storage.data_dir = directory;
    cfg.routes = vec![crate::config::Route {
        workspace_option: None,
        owned_service: None,
        // `None` is the documented default for routes without an explicit root cap.
        admission_policy: None,
        alias: "oc".into(),
        runtime: "opencode_v2".into(),
        module_artifact_id: "eliot-opencode-v2.http.1".into(),
        enabled: true,
        native_options: json!({
            "service_id": "svc-hook",
            "connection_file": "/tmp/conn.json",
            "expected_version": "1",
            "directory": "/tmp",
            "model": {"id": "m1", "providerID": "prov-a", "variant": "v"},
        }),
    }];
    let owner = StoreOwner::start(root, Arc::new(cfg), credential.clone())
        .await
        .unwrap();
    let operator = owner.store.authenticate(credential).await.unwrap();
    let store = &owner.store;

    let mut open = json!({"lane_id": "lane-hook", "route": "oc"});
    open["client_request_id"] = json!(model::new_id());
    let opened = store
        .call(operator.clone(), "agent.open".into(), open)
        .await
        .unwrap();
    let capacity = store
        .call(
            operator.clone(),
            "report.capacity".into(),
            json!({"after": 0, "limit": 50}),
        )
        .await
        .unwrap();
    let item = capacity["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["scope"]["scope_key"] == "opencode_v2:svc-hook")
        .unwrap_or_else(|| panic!("no hook scope in {capacity}"));
    assert_eq!(item["roster"], "known", "{item}");
    assert_eq!(item["counts"]["commands_in_flight"], 1, "{item}");
    assert_eq!(item["entries"][0]["entry_id"], opened["operation_id"]);
    assert_eq!(item["entries"][0]["phase"], "reserved");
    assert_eq!(item["entries"][0]["method"], "agent.open");

    let mut cancel = json!({"operation_id": opened["operation_id"], "reason": "test done"});
    cancel["client_request_id"] = json!(model::new_id());
    store
        .call(operator.clone(), "operation.cancel".into(), cancel)
        .await
        .unwrap();
    let capacity = store
        .call(
            operator.clone(),
            "report.capacity".into(),
            json!({"after": 0, "limit": 50}),
        )
        .await
        .unwrap();
    let item = capacity["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["scope"]["scope_key"] == "opencode_v2:svc-hook")
        .unwrap();
    assert_eq!(item["counts"]["commands_in_flight"], 0, "{item}");
    assert_eq!(item["entries"][0]["phase"], "released");
    assert_eq!(item["entries"][0]["release_reason"], "operation_cancelled");
    owner.close().await.unwrap();
}
