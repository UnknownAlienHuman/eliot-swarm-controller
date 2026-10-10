use super::FAMILY_SCOPE_GAP_REASON;
use crate::model::{self, Principal, Role};
use rusqlite::{Connection, params};
use serde_json::{Value, json};

const BINDING_ID: &str = "binding-family-history";
const GENERATION: i64 = 1;
const OBSERVATION_ID: i64 = 7;

fn fixture_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(super::super::SCHEMA).unwrap();
    db.execute_batch(super::super::WORKSPACE_SCHEMA).unwrap();
    super::super::set_meta(
        &db,
        "client:family-manager",
        &json!({"role":"manager","disabled":false}),
    )
    .unwrap();
    db.execute(
        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,\
         module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
         VALUES(?1,?2,'lane-family','instance-family','artifact-family','ready',\
         'native:family-scope','ses_root','{}',?3,1)",
        params![
            BINDING_ID,
            GENERATION,
            model::canonical(&json!({
                "connection":"connected",
                "native_observation_id":OBSERVATION_ID
            }))
            .unwrap(),
        ],
    )
    .unwrap();
    db
}

fn insert_task(db: &Connection, task_id: &str, state: &str) {
    db.execute(
        "INSERT INTO tasks(task_id,project_id,revision,state,spec_json,created_at_ms,updated_at_ms) \
         VALUES(?1,'project-family',1,?2,'{}',1,1)",
        params![task_id, state],
    )
    .unwrap();
}

fn insert_attempt(
    db: &Connection,
    attempt_id: &str,
    task_id: &str,
    owner_id: &str,
    released_at_ms: Option<i64>,
    producers: &Value,
) {
    let state = if released_at_ms.is_some() {
        "failed"
    } else {
        "running"
    };
    db.execute(
        "INSERT INTO attempts(attempt_id,task_id,task_revision,task_snapshot_json,owner_id,\
         start_owner,binding_id,binding_generation,state,producers_json,released_at_ms,\
         created_at_ms,updated_at_ms) \
         VALUES(?1,?2,1,'{\"revision\":1}',?3,'controller',?4,?5,?6,?7,?8,1,1)",
        params![
            attempt_id,
            task_id,
            owner_id,
            BINDING_ID,
            GENERATION,
            state,
            model::canonical(producers).unwrap(),
            released_at_ms,
        ],
    )
    .unwrap();
}

#[test]
fn family_uses_exact_attempt_after_long_binding_history_and_gaps_foreign_members() {
    let db = fixture_db();

    for index in 0..257 {
        let task_id = format!("historical-task-{index}");
        let attempt_id = format!("historical-attempt-{index}");
        insert_task(&db, &task_id, "archived");
        insert_attempt(
            &db,
            &attempt_id,
            &task_id,
            "former-manager",
            Some(10 + index),
            &json!([]),
        );
    }

    insert_task(&db, "task-owned", "open");
    let owned_producers = json!([{
        "attempt_id":"attempt-owned",
        "task_id":"task-owned",
        "task_revision":1,
        "binding_id":BINDING_ID,
        "binding_generation":GENERATION,
        "native_session_id":"ses_owned",
        "native_run_id":"run-owned"
    }]);
    insert_attempt(
        &db,
        "attempt-owned",
        "task-owned",
        "family-manager",
        None,
        &owned_producers,
    );

    insert_task(&db, "task-foreign", "open");
    let foreign_producers = json!([{
        "attempt_id":"attempt-foreign",
        "task_id":"task-foreign",
        "task_revision":1,
        "binding_id":BINDING_ID,
        "binding_generation":GENERATION,
        "native_session_id":"ses_foreign",
        "native_run_id":"run-foreign"
    }]);
    insert_attempt(
        &db,
        "attempt-foreign",
        "task-foreign",
        "another-manager",
        None,
        &foreign_producers,
    );

    let state = json!({
        "native_root_id":"ses_root",
        "native_scope_key":"native:family-scope",
        "session":{"sessionId":"ses_root"},
        "turns":[
            {"sessionId":"ses_owned","turnId":"run-owned","text":"owned turn"},
            {"sessionId":"ses_foreign","turnId":"run-foreign","text":"FOREIGN_TURN_SECRET"},
            {"sessionId":"ses_root","turnId":"unrelated-root-run","text":"FOREIGN_ROOT_SECRET"}
        ],
        "observed_children":[
            {"sessionId":"ses_owned","parentSessionId":"ses_root","agent":"owned","last_turn":{"sessionId":"ses_owned","turnId":"run-owned"}},
            {"sessionId":"ses_foreign","parentSessionId":"ses_root","agent":"foreign","private":"FOREIGN_MEMBER_SECRET","last_turn":{"sessionId":"ses_foreign","turnId":"run-foreign"}}
        ],
        "family_completeness":"complete",
        "gaps":3
    });
    db.execute(
        "INSERT INTO observations(observation_id,source_stream_id,source_event_key,binding_id,\
         binding_generation,kind,payload_json,recorded_at_ms) \
         VALUES(?1,'module:family','family-observation',?2,?3,'runtime.state',?4,100)",
        params![
            OBSERVATION_ID,
            BINDING_ID,
            GENERATION,
            model::canonical(&state).unwrap(),
        ],
    )
    .unwrap();

    let principal = Principal {
        link_id: "family-manager-link".into(),
        client_id: "family-manager".into(),
        role: Role::Manager,
    };
    let request = json!({
        "binding_id":BINDING_ID,
        "generation":GENERATION,
        "attempt_id":"attempt-owned",
        "observation_id":OBSERVATION_ID,
        "after":0,
        "limit":10
    });
    let family = super::family(&db, &principal, &request).unwrap();

    assert_eq!(family["observation_id"], OBSERVATION_ID);
    assert_eq!(family["items"][0]["sessionId"], "ses_owned");
    assert_eq!(family["items"][1]["gap"]["reason"], FAMILY_SCOPE_GAP_REASON);
    assert_eq!(family["turns"].as_array().unwrap().len(), 1);
    assert_eq!(family["turns"][0]["turnId"], "run-owned");
    assert_eq!(family["family_completeness"], "partial");
    assert_eq!(family["gaps"], 3);
    assert_eq!(family["projection"]["coverage_complete"], false);
    assert_eq!(
        family["projection"]["scope_filter"]["code"],
        "FAMILY_SCOPE_FILTERED"
    );
    assert_eq!(family["projection"]["scope_filter"]["source"], "task_scope");

    let encoded = model::canonical(&family).unwrap();
    assert!(!encoded.contains("FOREIGN_TURN_SECRET"));
    assert!(!encoded.contains("FOREIGN_ROOT_SECRET"));
    assert!(!encoded.contains("FOREIGN_MEMBER_SECRET"));
}
