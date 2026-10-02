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
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,result_json,settled_at_ms,due_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'op-1',?2,?3,'{}','{}',NULL,?4,?5,?6,?7,?8,?9,?10,0,1000,1000)",
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
}

fn set_op(db: &Connection, id: &str, state: &str, native_refs: Option<Value>) {
    let terminal = matches!(state, "settled" | "rejected" | "cancelled");
    let settled_at: Option<i64> = terminal.then_some(2000);
    let result: Option<&str> = terminal.then_some("{}");
    match native_refs {
        Some(refs) => db.execute(
            "UPDATE operations SET state=?2,native_refs_json=?3,settled_at_ms=?4,result_json=COALESCE(result_json,?5),updated_at_ms=2000 WHERE operation_id=?1",
            params![id, state, model::canonical(&refs).unwrap(), settled_at, result],
        ),
        None => db.execute(
            "UPDATE operations SET state=?2,settled_at_ms=?3,result_json=COALESCE(result_json,?4),updated_at_ms=2000 WHERE operation_id=?1",
            params![id, state, settled_at, result],
        ),
    }
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

fn proof(disposition: &str, started: bool) -> Value {
    json!({"input_execution": {
        "operation_id": "op-1", "native_session_id": "ses_root",
        "native_input_id": "input-op-1",
        "native_run_id": if started { json!("evt_start") } else { Value::Null },
        "admission": {"id": "evt_enq"}, "delivery": if started { json!({"id": "evt_del"}) } else { Value::Null },
        "execution_started": if started { json!({"id": "evt_start", "seq": 3}) } else { Value::Null },
        "terminal": null, "disposition": disposition, "uncertainty": null,
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
    assert_eq!(item["capacity_available"], true);

    // Execution-start evidence on the operation's own proof: the entry
    // activates and names the evidence; admission alone never did this.
    set_op(&db, "op-1", "settled", Some(proof("running", true)));
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
    assert_eq!(item["capacity_available"], false);
    assert_eq!(item["capacity_reason"], "no_pending_admission");

    // Terminal proof releases the reservation.
    set_op(&db, "op-1", "settled", Some(proof("completed", true)));
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

    db.execute(
        "UPDATE attempts SET producers_json=?1 WHERE attempt_id='att-2'",
        params![model::canonical(&json!([{"assignment_id":"asg-1","native_session_id":"ses_root","native_run_id":"turn-7","disposition":"admitted"}])).unwrap()],
    )
    .unwrap();
    capacity::sync_attempt(&db, "att-2", 1_800).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"][key];
    assert_eq!(entry["phase"], "active");
    assert_eq!(entry["execution_start_ref"]["turn_id"], "turn-7");

    db.execute(
        "UPDATE attempts SET producers_json=?1 WHERE attempt_id='att-2'",
        params![model::canonical(&json!([{"assignment_id":"asg-1","native_session_id":"ses_root","native_run_id":"turn-7","disposition":"failed"}])).unwrap()],
    )
    .unwrap();
    capacity::sync_attempt(&db, "att-2", 1_900).unwrap();
    let entry = &ledger(&db, "opencode_v2:svc-a")["entries"][key];
    assert_eq!(entry["phase"], "released");
    assert_eq!(entry["release_reason"], "execution_terminal:failed");
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
        proof("running", true),
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
    assert_eq!(
        a["capacity_available"], false,
        "svc-a has no pending admission"
    );
    let b = scope_item(&db, "opencode_v2:svc-b");
    assert_eq!(b["counts"]["desired_writers"], 1);
    assert_eq!(b["counts"]["pending_admissions"], 1);
    assert_eq!(b["counts"]["effective_writers"], 0);
    assert_eq!(b["capacity_available"], true);

    // The attention projection offers capacity only for svc-b.
    let attention = capacity::attention_report(&db, 200, 0).unwrap();
    let available: Vec<&Value> = attention["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|i| i["kind"] == "capacity_available")
        .collect();
    assert_eq!(available.len(), 1, "{}", attention["items"]);
    assert_eq!(available[0]["scope_key"], "opencode_v2:svc-b");
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
    assert_eq!(item["capacity_available"], false);
    assert_eq!(item["capacity_reason"], "roster_unknown");
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
    // Rows exist but the ledger was never synced: the projection must
    // not claim a known roster.
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
    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["roster"], "unknown");
    assert!(
        item["roster_reason"]
            .as_str()
            .unwrap()
            .starts_with("ledger_diverged:op-1:missing:reserved"),
        "{item}"
    );
    assert_eq!(item["capacity_available"], false);

    // After sync the roster is known; native activity outside the
    // retained family makes it unknown again.
    capacity::sync_operation(&db, "op-1", 1_500).unwrap();
    assert_eq!(scope_item(&db, "opencode_v2:svc-a")["roster"], "known");
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
        "UPDATE bindings SET native_root_id='ses_root',native_scope_key='opencode-v2:svc-a',state_json=json_set(state_json,'$.native',json(?1),'$.observed_at_ms',?2) WHERE binding_id='b1'",
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
    // Owner-selected configuration as recorded before the incident.
    let routes_before: String = db
        .query_row(
            "SELECT route_json FROM bindings WHERE binding_id='b1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let meta_before = meta_rows(&db);

    let rejected = RuntimeOutcome {
        operation_id: "op-1".into(),
        outcome: EffectOutcome::Rejected,
        native_scope_key: Some("opencode-v2:svc-a".into()),
        native_root_id: Some("ses_root".into()),
        turn_id: None,
        native_input_id: None,
        details: json!({"code": "RATE_LIMITED", "reset_at_ms": 4102444800000i64}),
    };
    capacity::note_outcome(&db, &op, &rejected, 2_000).unwrap();
    let (state, occurrences, details_raw): (String, i64, String) = db
        .query_row(
            "SELECT state,occurrences,details_json FROM incidents WHERE dedup_key='quota:opencode_v2:svc-a'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .unwrap();
    assert_eq!(state, "open");
    assert_eq!(occurrences, 1);
    let details: Value = serde_json::from_str(&details_raw).unwrap();
    assert_eq!(details["kind"], "quota");
    assert_eq!(details["scope"]["scope_key"], "opencode_v2:svc-a");
    assert_eq!(details["scope"]["provider"], "prov-a");
    assert_eq!(details["native_scope_key"], "opencode-v2:svc-a");
    assert_eq!(details["error_code"], "RATE_LIMITED");
    assert_eq!(details["reset_evidence"]["reset_at_ms"], 4102444800000i64);
    // The capacity projection surfaces the incident and blocks claims.
    let item = scope_item(&db, "opencode_v2:svc-a");
    assert_eq!(item["quota_incident"]["error_code"], "RATE_LIMITED");
    assert_eq!(item["capacity_available"], false);
    assert_eq!(item["capacity_reason"], "quota_incident_open");

    // A repeat rejection re-counts the same single open incident.
    capacity::note_outcome(&db, &op, &rejected, 2_100).unwrap();
    let count: i64 = db
        .query_row(
            "SELECT count(*) FROM incidents WHERE state='open'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(count, 1);
    let occurrences: i64 = db
        .query_row("SELECT occurrences FROM incidents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(occurrences, 2);

    // Owner configuration is byte-identical: the incident path has no
    // route or settings write at all.
    let routes_after: String = db
        .query_row(
            "SELECT route_json FROM bindings WHERE binding_id='b1'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(routes_before, routes_after);
    let meta_after = meta_rows(&db);
    assert_eq!(
        meta_before, meta_after,
        "quota recording writes no meta/config record"
    );

    // A non-quota rejection opens nothing.
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
    let count: i64 = db
        .query_row("SELECT count(*) FROM incidents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 1, "still only the quota incident");

    // A later applied outcome for the scope resolves the incident.
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
    let state: String = db
        .query_row("SELECT state FROM incidents", [], |r| r.get(0))
        .unwrap();
    assert_eq!(state, "resolved");
    assert!(scope_item(&db, "opencode_v2:svc-a")["quota_incident"].is_null());
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
    insert_op(
        &db,
        "op-1",
        "agent.send",
        "sending",
        Some("b1"),
        None,
        json!({}),
    );
    let op = operations::get_operation(&db, "op-1").unwrap();
    capacity::note_outcome(
        &db,
        &op,
        &RuntimeOutcome {
            operation_id: "op-1".into(),
            outcome: EffectOutcome::Rejected,
            native_scope_key: Some("opencode-v2:svc-a".into()),
            native_root_id: Some("ses_root".into()),
            turn_id: None,
            native_input_id: None,
            details: json!({"code": "QUOTA_EXCEEDED", "reset_at_ms": 4102444800000i64}),
        },
        2_000,
    )
    .unwrap();
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
        .find(|i| i["scope"]["scope_key"] == "opencode_v2:svc-a")
        .unwrap();
    assert_eq!(scope["quota_incident"]["error_code"], "QUOTA_EXCEEDED");
    let codes: Vec<&str> = report["findings"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["code"].as_str().unwrap())
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
