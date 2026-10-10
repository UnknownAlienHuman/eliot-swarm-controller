//! Focused Store regression module for O9 terminal-event continuation.
//!
//! Integrate as a child of `store::program_tests` so this fixture reuses its
//! real Store setup, manager registration, Task/Attempt subject and readback
//! helpers. The only seeded effect evidence is one settled source Goal
//! Operation with its exact completed OpenCode EventRef. The continuation and
//! global slot must be produced by `reconcile_automations_once`.

use super::*;
use rusqlite::params;
use serde_json::{Value, json};

const AUTOMATION_ID: &str = "o9-terminal-goal-followup";
const GOAL_ID: &str = "o9-terminal-goal";
const SOURCE_OPERATION_ID: &str = "o9-completed-native-goal-turn";
const EVENT_STREAM_REPLAY_SUFFIX: &str = ":post-transfer-replay";

fn fixture_route(directory: &std::path::Path) -> Value {
    // This fixture seeds a pre-cutover `.1` binding directly and reconciles a
    // Goal continuation through that retained binding; it does not admit a new
    // binding, so keep the historical artifact identity for readback coverage.
    json!({
        "alias":"o9-opencode-fixture",
        "runtime":crate::runtime::opencode_v2::RUNTIME,
        "module_artifact_id":"eliot-opencode-v2.http.1",
        "enabled":true,
        "native_options":{
            "service_id":"o9-service",
            "connection_file":directory.join("opencode.json").display().to_string(),
            "expected_version":"1",
            "directory":directory.display().to_string(),
            "model":{"id":"o9-fixture-model","providerID":"o9-fixture-provider","variant":"default"}
        }
    })
}

#[tokio::test]
async fn completed_goal_event_admits_once_and_remains_readable_across_transfer() {
    let fixture_directory = std::env::temp_dir().join(format!(
        "eliot-o9-opencode-fixture-{}",
        crate::model::new_id()
    ));
    let route = fixture_route(&fixture_directory);
    let mut config = Config::default();
    config
        .routes
        .push(serde_json::from_value(route.clone()).unwrap());
    let (owner, _directory, bootstrap) =
        super::start_store_with_config("o9-goal-transfer", config).await;
    super::seed_clients(&owner.store).await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    let former = super::principal("review-gm", Role::Manager);
    let successor = super::principal("review-owner-v1", Role::Manager);

    // Keep the Attempt owned by a third manager so successor access after
    // handover comes from current-GM scope, not a creator/Attempt-owner shortcut.
    let subject = super::seed_subject(
        &owner.store,
        "o9-goal-transfer",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let binding_id = format!("o9-goal-binding-{}", subject.attempt_id);
    let native_root_id = "ses_o9goalfixture0001";
    let objective = "Continue this exact active Goal after its completed native turn.";

    let attempt_id = subject.attempt_id.clone();
    let binding_id_for_db = binding_id.clone();
    let state = json!({
        "connection":"connected",
        "bridge_boot_id":"o9-fixture-boot",
        "native_scope_key":"opencode-v2:o9-service",
        "native_root_id":native_root_id
    });
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let now = crate::model::now_ms()?;
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,?2,'o9-fixture-instance','eliot-opencode-v2.http.1','ready','opencode-v2:o9-service',?3,?4,?5,?6)",
                params![
                    binding_id_for_db,
                    format!("o9-lane-{attempt_id}"),
                    native_root_id,
                    crate::model::canonical(&route)?,
                    crate::model::canonical(&state)?,
                    now,
                ],
            )?;
            tx.execute(
                "UPDATE attempts SET binding_id=?2,binding_generation=1 WHERE attempt_id=?1 AND released_at_ms IS NULL",
                params![attempt_id, binding_id_for_db],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();

    owner
        .store
        .call(
            former.clone(),
            "goal.create".into(),
            json!({
                "client_request_id":"create-o9-transfer-goal",
                "project_id":"fixture",
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "goal_id":GOAL_ID,
                "expected_revision":0,
                "objective":objective,
                "completion_evidence":{"kind":"task_acceptance"},
                "reminder":null,
                "enabled":false
            }),
        )
        .await
        .unwrap();

    let changes = json!([{
        "automation_id":AUTOMATION_ID,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "scope":{"work_pool_id":null},
            "steps":["goal_progression"],
            "goal_progression":{"goal_id":GOAL_ID}
        }
    }]);
    let preview = owner
        .store
        .call(
            former.clone(),
            "automation.config.preview".into(),
            json!({"project_id":"fixture","changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true);
    let enabled = owner
        .store
        .call(
            former.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"enable-o9-transfer-goal",
                "project_id":"fixture",
                "changes":changes,
                "preview_digest":preview["plan_sha256"]
            }),
        )
        .await
        .unwrap();
    assert_eq!(enabled["applied"], true);

    // Seed only the exact completed input proof and its settled predecessor
    // receipt. The actual follow-up Operation and terminal slot are not seeded.
    let event_hash = crate::model::digest(b"o9-exact-terminal-event");
    let objective_digest = format!(
        "sha256:{}",
        crate::model::digest(
            crate::model::canonical(&json!(objective))
                .unwrap()
                .as_bytes()
        ),
    );
    let native_input_id = crate::runtime::opencode_v2::input_id(SOURCE_OPERATION_ID);
    let proof = json!({
        "reader_revision":"opencode-execution-log-v1",
        "operation_id":SOURCE_OPERATION_ID,
        "native_session_id":native_root_id,
        "native_input_id":native_input_id,
        "native_run_id":"run_o9goalfixture0001",
        "native_run_id_kind":"execution_started_event",
        "admission":{"id":"evt_o9_admission","seq":2,"sha256":crate::model::digest(b"o9-admission")},
        "delivery":{"id":"evt_o9_delivery","seq":3,"sha256":crate::model::digest(b"o9-delivery")},
        "execution_started":{"id":"evt_o9_started","seq":4,"sha256":crate::model::digest(b"o9-started")},
        "terminal":{
            "event":{"id":"evt_o9_terminal","seq":7,"sha256":event_hash},
            "outcome":"completed",
            "reason":null
        },
        "disposition":"completed",
        "uncertainty":null,
        "log_watermark":7,
        "correlation":"durable_serialized_execution",
        "family_complete":false
    });
    let source_request = json!({
        "client_request_id":"o9-source-goal-turn",
        "binding_id":binding_id,
        "generation":1,
        "action":"continue",
        "objective":objective,
        "expected_revision":6
    });
    let source_result = json!({
        "operation_id":SOURCE_OPERATION_ID,
        "outcome":"applied",
        "details":{"goal":{
            "present":true,
            "status":"active",
            "revision":7,
            "objective_digest":objective_digest
        }}
    });
    let source_operation_id = SOURCE_OPERATION_ID.to_owned();
    let binding_id_for_source = binding_id.clone();
    let proof_for_source = proof.clone();
    let source_observation_id = owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let now = crate::model::now_ms()?;
            let encoded_proof = crate::model::canonical(&proof_for_source)?;
            let event_key = format!(
                "{}:{}",
                source_operation_id,
                crate::model::digest(encoded_proof.as_bytes())
            );
            tx.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,task_id,attempt_id,binding_id,binding_generation,state,native_refs_json,result_json,due_at_ms,sent_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
                 VALUES(?1,'review-owner-v2','o9-source-goal-turn','agent.goal',?2,'{}',?3,?4,?5,1,'settled',?6,?7,?8,?8,?8,?8,?8)",
                params![
                    source_operation_id,
                    crate::model::canonical(&source_request)?,
                    subject.task_id,
                    subject.attempt_id,
                    binding_id_for_source,
                    crate::model::canonical(&json!({"input_execution":proof_for_source}))?,
                    crate::model::canonical(&source_result)?,
                    now,
                ],
            )?;
            tx.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES(?1,?2,?3,1,?4,'opencode.input_execution',?5,?6)",
                params![
                    format!("opencode-execution:{binding_id_for_source}:1"),
                    event_key,
                    binding_id_for_source,
                    source_operation_id,
                    encoded_proof,
                    now,
                ],
            )?;
            let observation_id = tx.last_insert_rowid();
            tx.commit()?;
            Ok(observation_id)
        })
        .await
        .unwrap();

    let first_pass = owner.store.reconcile_automations_once().await.unwrap();
    let first_entry = first_pass["goal_progression"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["automation_id"] == AUTOMATION_ID)
        .expect("enabled manager Goal entry was reconciled");
    assert_eq!(first_entry["processed"], 1);
    assert_eq!(first_entry["cursor"], source_observation_id);
    assert_eq!(first_entry["recent"][0]["disposition"], "admitted");

    let technical_requester = crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
    let (continuation_id, original, effective, continuation_state, caller_id, linked_count) = owner
        .store
        .run(move |db| {
            let row: (String, String, String, String, String, i64) = db.query_row(
                "SELECT operation_id,original_request_json,effective_request_json,state,caller_id,COUNT(*) OVER() \
                 FROM operations WHERE caller_id=?1 AND method='agent.goal' AND operation_id<>?2 \
                 ORDER BY created_at_ms,operation_id LIMIT 1",
                params![technical_requester, SOURCE_OPERATION_ID],
                |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?,row.get(4)?,row.get(5)?)),
            )?;
            Ok(row)
        })
        .await
        .unwrap();
    let original: Value = serde_json::from_str(&original).unwrap();
    let effective: Value = serde_json::from_str(&effective).unwrap();
    assert_eq!(linked_count, 1, "one normal continuation was admitted");
    assert_eq!(continuation_state, "queued");
    assert_eq!(caller_id, technical_requester);
    assert_eq!(original["action"], "continue");
    assert_eq!(original["binding_id"], binding_id);
    assert_eq!(original["expected_revision"], 7);
    assert_eq!(
        effective["operation_contract"]["completion_condition"],
        "native_input_admitted"
    );
    assert_eq!(
        effective["automation_on_behalf"]["effective_manager_id"],
        former.client_id
    );
    assert_eq!(
        effective["automation_on_behalf"]["automation_id"],
        AUTOMATION_ID
    );
    assert_eq!(effective["automation_on_behalf"]["action"], "agent.goal");
    assert_eq!(
        effective["automation_on_behalf"]["cause"]["source_operation_id"],
        SOURCE_OPERATION_ID
    );
    assert_eq!(
        effective["automation_on_behalf"]["cause"]["source_observation_id"],
        source_observation_id
    );
    assert_eq!(
        effective["automation_on_behalf"]["cause"]["goal_owner_manager_id"],
        former.client_id
    );
    assert_eq!(
        effective["automation_on_behalf"]["cause"]["goal_last_reviser_manager_id"],
        former.client_id
    );

    let admitted_readback = owner
        .store
        .call(
            former.clone(),
            "operation.get".into(),
            json!({"operation_id":continuation_id}),
        )
        .await
        .unwrap();
    assert_eq!(admitted_readback["method"], "agent.goal");
    assert_eq!(admitted_readback["state"], "queued");
    assert_eq!(admitted_readback["result"]["operation_id"], continuation_id);
    assert_eq!(admitted_readback["result"]["state"], "queued");
    assert!(
        admitted_readback["result"]
            .get("native_admission")
            .is_none(),
        "native admission stays outside the public operation receipt"
    );

    let (slot_count, slot, retained_link) = owner
        .store
        .run({
            let continuation_id = continuation_id.clone();
            move |db| {
                let slot_count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM meta WHERE key LIKE 'goal-progression:v1:terminal-slot:%'",
                    [],
                    |row| row.get(0),
                )?;
                let slot_key: String = db.query_row(
                    "SELECT key FROM meta WHERE key LIKE 'goal-progression:v1:terminal-slot:%' ORDER BY key LIMIT 1",
                    [],
                    |row| row.get(0),
                )?;
                let slot = crate::automation::config::read_record(
                    db,
                    &slot_key,
                    "Goal progression terminal slot fixture",
                )?
                .ok_or_else(|| {
                    crate::error::Error::new(
                        "TEST_GOAL_SLOT_MISSING",
                        "terminal slot has no retained record",
                    )
                })?;
                let link = crate::automation::authorization::operation_link(db, &continuation_id)?
                    .ok_or_else(|| crate::error::Error::new("TEST_GOAL_LINK_MISSING", "continuation has no durable on-behalf link"))?;
                Ok((slot_count, slot, serde_json::to_value(link)?))
            }
        })
        .await
        .unwrap();
    assert_eq!(slot_count, 1, "one global EventRef slot was retained");
    assert_eq!(slot["disposition"], "admitted");
    assert_eq!(slot["operation_id"], continuation_id);
    assert_eq!(slot["manager_id"], former.client_id);
    assert_eq!(slot["source_observation_id"], source_observation_id);
    assert_eq!(slot["terminal_event"]["id"], "evt_o9_terminal");
    assert_eq!(retained_link["operation_id"], continuation_id);
    assert_eq!(retained_link["action"], "agent.goal");
    assert_eq!(retained_link["effective_manager_id"], former.client_id);
    assert_eq!(retained_link["cause"]["goal_revision"], 1);

    // Transfer after the admission. The successor's state readback must retain
    // the old cursor and linked history; a repeated exact EventRef is added
    // under a new observation identity only after handover.
    owner
        .store
        .call(
            operator.clone(),
            "gm.handover".into(),
            json!({"client_request_id":"handover-o9-goal-entry","client_id":successor.client_id}),
        )
        .await
        .unwrap();
    let transfer = owner
        .store
        .call(
            successor.clone(),
            "automation.config.transfer".into(),
            json!({
                "client_request_id":"transfer-o9-goal-entry",
                "project_id":"fixture",
                "former_owner_manager_id":former.client_id,
                "automation_id":AUTOMATION_ID,
                "expected_revision":1
            }),
        )
        .await
        .unwrap();
    assert_eq!(transfer["status"], "transferred");
    assert_eq!(transfer["state_ledgers_relocated"], 6);
    assert_eq!(transfer["new_owner_manager_id"], successor.client_id);

    let successor_readback = owner
        .store
        .call(
            successor.clone(),
            "automation.config.explain".into(),
            json!({"project_id":"fixture","automation_id":AUTOMATION_ID}),
        )
        .await
        .unwrap();
    assert_eq!(successor_readback["owner_manager_id"], successor.client_id);
    assert_eq!(successor_readback["entry"]["revision"], 2);
    assert_eq!(
        successor_readback["goal_progression"]["owner_manager_id"],
        successor.client_id
    );
    assert_eq!(
        successor_readback["goal_progression"]["cursor"],
        source_observation_id
    );
    assert_eq!(
        successor_readback["linked_operation_history"]["items"][0]["operation_id"],
        continuation_id
    );
    assert_eq!(
        successor_readback["linked_operation_history"]["items"][0]["historical_owner_manager_id"],
        former.client_id
    );

    let proof_for_replay = proof.clone();
    let binding_id_for_replay = binding_id.clone();
    owner
        .store
        .run(move |db| {
            let now = crate::model::now_ms()?;
            let encoded = crate::model::canonical(&proof_for_replay)?;
            db.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,binding_id,binding_generation,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES(?1,?2,?3,1,?4,'opencode.input_execution',?5,?6)",
                params![
                    format!("opencode-execution:{binding_id_for_replay}:1{EVENT_STREAM_REPLAY_SUFFIX}"),
                    format!("{SOURCE_OPERATION_ID}:replayed-after-transfer"),
                    binding_id_for_replay,
                    SOURCE_OPERATION_ID,
                    encoded,
                    now,
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    // Current-GM scope allows successor readback while preserving the former
    // manager as immutable effective actor. Reconcile is the real Store pump;
    // the exact same EventRef must be consumed from the transferred cursor but
    // cannot create a second Operation or replace its original link/slot.
    let successor_operation_readback = owner
        .store
        .call(
            successor.clone(),
            "operation.get".into(),
            json!({"operation_id":continuation_id}),
        )
        .await
        .unwrap();
    assert_eq!(successor_operation_readback["method"], "agent.goal");
    assert!(
        successor_operation_readback.get("caller_id").is_none(),
        "the scoped Operation receipt does not expose its retained issuer"
    );

    let successor_pass = owner.store.reconcile_automations_once().await.unwrap();
    let successor_entry = successor_pass["goal_progression"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["automation_id"] == AUTOMATION_ID)
        .expect("transferred Goal entry was reconciled under its successor");
    assert_eq!(successor_entry["owner_manager_id"], successor.client_id);
    assert_eq!(successor_entry["processed"], 1);
    assert_eq!(
        successor_entry["recent"][1]["disposition"],
        "duplicate_event"
    );
    assert_eq!(
        successor_entry["recent"][1]["operation_id"],
        continuation_id
    );

    let successor_after_replay = owner
        .store
        .call(
            successor.clone(),
            "automation.config.explain".into(),
            json!({"project_id":"fixture","automation_id":AUTOMATION_ID}),
        )
        .await
        .unwrap();
    let final_cursor = successor_after_replay["goal_progression"]["cursor"]
        .as_i64()
        .expect("sealed cursor is exposed by normal automation readback");
    let (operation_count, slot_count) = owner
        .store
        .run(move |db| {
            let operation_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE caller_id=?1 AND method='agent.goal' AND operation_id<>?2",
                params![technical_requester, SOURCE_OPERATION_ID],
                |row| row.get(0),
            )?;
            let slot_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key LIKE 'goal-progression:v1:terminal-slot:%'",
                [],
                |row| row.get(0),
            )?;
            Ok((operation_count, slot_count))
        })
        .await
        .unwrap();
    assert_eq!(
        operation_count, 1,
        "replayed EventRef admitted no second continuation"
    );
    assert_eq!(
        slot_count, 1,
        "the original global terminal slot survived transfer"
    );
    assert!(
        final_cursor > source_observation_id,
        "successor cursor consumed the replay row"
    );

    let repeated_pass = owner.store.reconcile_automations_once().await.unwrap();
    let repeated_entry = repeated_pass["goal_progression"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["automation_id"] == AUTOMATION_ID)
        .expect("transferred Goal entry remains enabled");
    assert_eq!(repeated_entry["processed"], 0);
}
