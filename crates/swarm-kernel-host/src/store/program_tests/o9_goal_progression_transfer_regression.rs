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

fn codex_rust_descriptor(version: &str) -> swarm_contracts::module_catalog::ModuleDescriptor {
    use swarm_contracts::module_catalog::{
        ActivationPolicy, ArtifactVersion, LaunchSpec, LifecycleOwnership, RestartPolicy,
    };
    use swarm_contracts::module_contract::ModuleContractTemplate;

    let mut template = ModuleContractTemplate::codex_rust_controller_v6().unwrap();
    template.artifact.version = ArtifactVersion::new(version).unwrap();
    template
        .descriptor(
            LaunchSpec {
                executable: if cfg!(windows) {
                    std::path::PathBuf::from(format!(
                        r"C:\modules\codex-rust-controller-{version}.exe"
                    ))
                } else {
                    std::path::PathBuf::from(format!("/modules/codex-rust-controller-{version}"))
                },
                argv: Vec::new(),
                environment: Vec::new(),
                credential_ref: None,
                working_directory: None,
                inherited_environment_allowlist: Default::default(),
                executable_sha256: None,
            },
            LifecycleOwnership::OwnedService,
            ActivationPolicy::OnDemand,
            true,
            RestartPolicy::default(),
        )
        .unwrap()
}

fn codex_module_receipt(
    command: &Value,
    descriptor: &swarm_contracts::module_catalog::ModuleDescriptor,
) -> swarm_contracts::runtime::ModuleReceiptIdentity {
    swarm_contracts::runtime::ModuleReceiptIdentity {
        schema_version: 1,
        module_id: descriptor.module_id.clone(),
        artifact: descriptor.artifact.clone(),
        protocol: swarm_contracts::module_contract::MODULE_PROTOCOL_V1,
        binding_id: command["binding_id"].as_str().unwrap().to_owned(),
        binding_generation: command["generation"].as_i64().unwrap(),
        operation_id: command["operation_id"].as_str().unwrap().to_owned(),
        input_sha256: command["input_sha256"].as_str().unwrap().to_owned(),
    }
}

fn codex_terminal_marker(
    operation_id: &str,
    native_root_id: &str,
    turn_id: &str,
    native_input_id: &str,
    sequence: u64,
    prompt: &str,
) -> Value {
    let event_id = format!("evt_o9_codex_{sequence}");
    let event_record = json!({
        "id":event_id,
        "seq":sequence,
        "kind":"turn.completed",
        "operation_id":operation_id,
        "native_root_id":native_root_id,
        "turn_id":turn_id,
        "native_input_id":native_input_id,
        "status":"completed",
        "input_sha256":crate::model::digest(prompt.as_bytes())
    });
    json!({
        "schema_id":swarm_contracts::module_contract::GOAL_TERMINAL_EVIDENCE_SCHEMA_ID,
        "schema_version":1,
        "source":"codex",
        "reader_revision":"codex-turn-journal-v1",
        "event":{
            "id":event_id,
            "seq":sequence,
            "sha256":crate::model::digest(crate::model::canonical(&event_record).unwrap().as_bytes())
        },
        "event_record":event_record
    })
}

fn codex_completed_details(
    module_receipt: &swarm_contracts::runtime::ModuleReceiptIdentity,
    admission_field: &str,
    admission: Value,
    terminal_marker: Value,
    operation_id: &str,
    prompt: &str,
) -> Value {
    let mut details = json!({
        "completion_condition":"native_turn_completed",
        "execution_complete":true,
        "task_completion":"unknown",
        "disposition":"completed",
        "native_turn_status":"completed",
        "native_input_readback":"verified",
        "native_turn_readback":"verified",
        "client_user_message_id":operation_id,
        "requested_model_provider":"o9-fixture-provider",
        "requested_model":"o9-fixture-model",
        "prompt_sha256":crate::model::digest(prompt.as_bytes()),
        "prompt_bytes":prompt.len()
    });
    details["module_receipt"] = serde_json::to_value(module_receipt).unwrap();
    details[admission_field] = admission;
    details["goal_terminal_event"] = terminal_marker;
    details
}

fn codex_task_dispatch_receipt(
    command: &Value,
    descriptor: &swarm_contracts::module_catalog::ModuleDescriptor,
    native_input_id: &str,
) -> (Value, String) {
    use swarm_contracts::runtime::{TaskDispatchAdmissionReceipt, TaskDispatchContext};

    let context: TaskDispatchContext =
        serde_json::from_value(command["input"]["task_dispatch_context"].clone()).unwrap();
    let prompt = command["input"]["task_prompt"]["prompt"]
        .as_str()
        .unwrap()
        .to_owned();
    let receipt = TaskDispatchAdmissionReceipt {
        schema_version: 1,
        module_receipt: codex_module_receipt(command, descriptor),
        operation_id: context.operation_id.clone(),
        binding_id: context.binding_id.clone(),
        binding_generation: context.binding_generation,
        worker_boot_id: context.worker_boot_id.clone(),
        attempt_id: context.attempt_id.clone(),
        task_id: context.task_id.clone(),
        task_revision: context.task_revision,
        task_snapshot_sha256: context.task_snapshot_sha256.clone(),
        source_text_sha256: context.source_text_sha256.clone(),
        source_text_bytes: context.source_text_bytes,
        native_payload_sha256: crate::model::digest(prompt.as_bytes()),
        native_payload_bytes: prompt.len() as u64,
        native_input_id: Some(native_input_id.to_owned()),
    };
    receipt.validate().unwrap();
    (serde_json::to_value(receipt).unwrap(), prompt)
}

fn codex_goal_continuation_receipt(
    command: &Value,
    descriptor: &swarm_contracts::module_catalog::ModuleDescriptor,
    native_input_id: &str,
) -> (Value, String) {
    use swarm_contracts::runtime::{
        GoalContinuationAdmissionContext, GoalContinuationAdmissionReceipt,
    };

    let context: GoalContinuationAdmissionContext =
        serde_json::from_value(command["input"]["goal_continuation_context"].clone()).unwrap();
    let prompt = command["input"]["text"].as_str().unwrap().to_owned();
    let payload = json!({
        "threadId":command["native_root_id"],
        "input":[{"type":"text","text":prompt}],
        "model":command["route"]["native_options"]["model"],
        "clientUserMessageId":command["operation_id"]
    });
    let payload = serde_json::to_vec(&payload).unwrap();
    let receipt = GoalContinuationAdmissionReceipt {
        schema_id: swarm_contracts::module_contract::GOAL_CONTINUATION_ADMISSION_SCHEMA_ID
            .to_owned(),
        schema_version: GoalContinuationAdmissionReceipt::VERSION,
        context,
        module_receipt: codex_module_receipt(command, descriptor),
        native_payload_sha256: crate::model::digest(&payload),
        native_payload_bytes: payload.len() as u64,
        native_input_id: Some(native_input_id.to_owned()),
    };
    receipt.validate().unwrap();
    (serde_json::to_value(receipt).unwrap(), prompt)
}

fn codex_completed_outcome(
    command: &Value,
    descriptor: &swarm_contracts::module_catalog::ModuleDescriptor,
    native_root_id: &str,
    native_scope_key: &str,
    sequence: u64,
) -> crate::runtime::RuntimeOutcome {
    let operation_id = command["operation_id"].as_str().unwrap();
    let turn_id = format!("turn_o9_codex_{sequence}");
    let native_input_id = format!("input_o9_codex_{sequence}");
    let module_receipt = codex_module_receipt(command, descriptor);
    let (admission_field, admission, prompt) = match command["method"].as_str().unwrap() {
        "task.dispatch" => {
            let (receipt, prompt) =
                codex_task_dispatch_receipt(command, descriptor, &native_input_id);
            ("dispatch_admission", receipt, prompt)
        }
        "agent.send" => {
            let (receipt, prompt) =
                codex_goal_continuation_receipt(command, descriptor, &native_input_id);
            ("goal_continuation_admission", receipt, prompt)
        }
        method => panic!("unexpected Codex Goal fixture command: {method}"),
    };
    let marker = codex_terminal_marker(
        operation_id,
        native_root_id,
        &turn_id,
        &native_input_id,
        sequence,
        &prompt,
    );
    crate::runtime::RuntimeOutcome {
        operation_id: operation_id.to_owned(),
        outcome: crate::runtime::EffectOutcome::Applied,
        native_scope_key: Some(native_scope_key.to_owned()),
        native_root_id: Some(native_root_id.to_owned()),
        turn_id: Some(turn_id),
        native_input_id: Some(native_input_id),
        details: codex_completed_details(
            &module_receipt,
            admission_field,
            admission,
            marker,
            operation_id,
            &prompt,
        ),
    }
}

async fn record_codex_completed_command(
    owner: &super::StoreOwner,
    module: &crate::model::Principal,
    command: &Value,
    descriptor: &swarm_contracts::module_catalog::ModuleDescriptor,
    native_root_id: &str,
    native_scope_key: &str,
    sequence: u64,
) {
    let outcome = codex_completed_outcome(
        command,
        descriptor,
        native_root_id,
        native_scope_key,
        sequence,
    );
    owner
        .store
        .call(
            module.clone(),
            "module.outcome".into(),
            serde_json::to_value(outcome).unwrap(),
        )
        .await
        .unwrap();
}

async fn next_codex_command(
    owner: &super::StoreOwner,
    module: &crate::model::Principal,
    config: &Config,
) -> Value {
    let module = module.clone();
    let config = config.clone();
    owner
        .store
        .run(move |db| super::super::runtime::next_with_config(db, &module, &config))
        .await
        .unwrap()["command"]
        .clone()
}

async fn set_codex_descriptor_enabled(owner: &super::StoreOwner, version: &str, enabled: bool) {
    let version = version.to_owned();
    owner
        .store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT value_json FROM meta WHERE key='module_catalog:trusted_descriptors:v1'",
                [],
                |row| row.get(0),
            )?;
            let mut registry: Value = serde_json::from_str(&raw)?;
            let descriptor = registry["descriptors"]
                .as_array_mut()
                .and_then(|descriptors| {
                    descriptors
                        .iter_mut()
                        .find(|entry| entry["descriptor"]["artifact"]["version"] == version)
                })
                .ok_or_else(|| {
                    crate::error::Error::new(
                        "TEST_CODEX_DESCRIPTOR_MISSING",
                        "fixture descriptor version is not retained",
                    )
                })?;
            descriptor["descriptor"]["enabled"] = json!(enabled);
            db.execute(
                "UPDATE meta SET value_json=?1 WHERE key='module_catalog:trusted_descriptors:v1'",
                [crate::model::canonical(&registry)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

fn remove_owned_o9_temp_root(path: &std::path::Path, expected_prefix: &str) -> std::io::Result<()> {
    let temp_root = std::env::temp_dir();
    let owned_parent = path.parent() == Some(temp_root.as_path());
    let owned_name = path
        .file_name()
        .is_some_and(|name| name.to_string_lossy().starts_with(expected_prefix));
    if !owned_parent || !owned_name {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to remove non-owned O9 fixture root {}",
                path.display()
            ),
        ));
    }
    match std::fs::remove_dir_all(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    }
}

#[tokio::test]
async fn selected_codex_store_receipts_advance_goal_slots_and_preserve_python_history() {
    use rusqlite::params;

    const BINDING_ID: &str = "o9-codex-goal-binding";
    const MODULE_CLIENT_ID: &str = "o9-codex-goal-module";
    const NATIVE_ROOT_ID: &str = "thread_o9_codex_goal";
    const NATIVE_SCOPE_KEY: &str = "codex-o9-service";
    const MODULE_LINK_ID: &str = "link-o9-codex-module";
    const HISTORY_OPERATION_ID: &str = "o9-historical-python-operation";
    const HISTORY_BINDING_ID: &str = "o9-historical-python-binding";

    let fixture_directory =
        std::env::temp_dir().join(format!("eliot-o9-codex-fixture-{}", crate::model::new_id()));
    std::fs::create_dir(&fixture_directory).unwrap();
    let route = json!({
        "alias":"o9-codex-goal",
        "runtime":"codex",
        "module_artifact_id":swarm_contracts::module_contract::CODEX_RUST_CONTROLLER_ARTIFACT_ID,
        "enabled":true,
        "workspace_option":"workspaceRoot",
        "native_options":{
            "modelProvider":"o9-fixture-provider",
            "model":"o9-fixture-model",
            "workspaceRoot":fixture_directory.display().to_string()
        }
    });
    let mut config = Config::default();
    config
        .routes
        .push(serde_json::from_value(route.clone()).unwrap());
    let (owner, _directory, bootstrap) =
        super::start_store_with_config("o9-codex-goal", config.clone()).await;
    super::seed_clients(&owner.store).await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    let manager = super::principal("review-gm", Role::Manager);
    let subject = super::seed_subject(
        &owner.store,
        "o9-codex-goal",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let descriptor = codex_rust_descriptor("7");
    let route_for_selection: crate::config::Route = serde_json::from_value(route.clone()).unwrap();
    let route_for_initial_selection = route_for_selection.clone();
    let selection_config = config.clone();
    let descriptor_for_store = descriptor.clone();
    let attempt_id_for_store = subject.attempt_id.clone();
    let route_for_store = route.clone();
    let manager_id = manager.client_id.clone();
    let module_link = MODULE_LINK_ID.to_owned();
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let registration = super::super::module_handshake::register_trusted_descriptor(
                &tx,
                descriptor_for_store.clone(),
            )?;
            super::super::module_handshake::select_route(
                &tx,
                &crate::model::Principal {
                    link_id: "o9-codex-manager-link".to_owned(),
                    client_id: manager_id.clone(),
                    role: Role::Manager,
                },
                &json!({
                    "route_alias":route_for_initial_selection.alias,
                    "module_id":descriptor_for_store.module_id,
                    "artifact_id":descriptor_for_store.artifact.artifact_id,
                    "version":descriptor_for_store.artifact.version,
                    "expected_catalog_revision":registration["catalog_revision"]
                }),
                &selection_config,
                "select-o9-codex-goal",
            )?;
            let selector = super::super::module_handshake::selection_for_new_binding(
                &tx,
                &manager_id,
                &route_for_initial_selection.alias,
                &route_for_initial_selection.runtime,
                &route_for_initial_selection.module_artifact_id,
            )?
            .ok_or_else(|| {
                crate::error::Error::new(
                    "TEST_CODEX_SELECTION_MISSING",
                    "fixture selected no Codex descriptor",
                )
            })?;
            let state = json!({
                "connection":"connected",
                "bridge_boot_id":"o9-codex-boot",
                "module_link_id":module_link,
                "module_client_id":MODULE_CLIENT_ID,
                "module_contract_selector":selector,
                "module_contract_selection_owner_manager_id":manager_id,
                "native_scope_key":NATIVE_SCOPE_KEY,
                "native_root_id":NATIVE_ROOT_ID
            });
            let now = crate::model::now_ms()?;
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,'o9-codex-goal-lane','o9-codex-goal-instance',?2,'ready',?3,?4,?5,?6,?7)",
                params![
                    BINDING_ID,
                    descriptor_for_store.artifact.artifact_id.as_str(),
                    NATIVE_SCOPE_KEY,
                    NATIVE_ROOT_ID,
                    crate::model::canonical(&route_for_store)?,
                    crate::model::canonical(&state)?,
                    now,
                ],
            )?;
            tx.execute(
                "UPDATE attempts SET state='reserved',binding_id=?2,binding_generation=1 \
                 WHERE attempt_id=?1 AND released_at_ms IS NULL",
                params![attempt_id_for_store, BINDING_ID],
            )?;
            tx.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    format!("client:{MODULE_CLIENT_ID}"),
                    crate::model::canonical(&json!({
                        "role":"module",
                        "disabled":false,
                        "binding_id":BINDING_ID,
                        "binding_generation":1
                    }))?,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    let module = crate::model::Principal {
        link_id: MODULE_LINK_ID.to_owned(),
        client_id: MODULE_CLIENT_ID.to_owned(),
        role: Role::Module,
    };

    owner
        .store
        .call(
            manager.clone(),
            "goal.create".into(),
            json!({
                "client_request_id":"create-o9-codex-goal",
                "project_id":"fixture",
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "goal_id":GOAL_ID,
                "expected_revision":0,
                "objective":"Continue the exact selected Codex Goal.",
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
            manager.clone(),
            "automation.config.preview".into(),
            json!({"project_id":"fixture","changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true);
    owner
        .store
        .call(
            manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"enable-o9-codex-goal",
                "project_id":"fixture",
                "changes":changes,
                "preview_digest":preview["plan_sha256"]
            }),
        )
        .await
        .unwrap();

    let dispatch = owner
        .store
        .call(
            manager.clone(),
            "task.dispatch".into(),
            json!({
                "client_request_id":"dispatch-o9-codex-goal",
                "attempt_id":subject.attempt_id,
                "text":"Begin the selected Codex Goal."
            }),
        )
        .await
        .unwrap();
    assert_eq!(dispatch["state"], "queued");
    let source_operation_id = dispatch["operation_id"].as_str().unwrap().to_owned();
    let source_command = next_codex_command(&owner, &module, &config).await;
    assert_eq!(source_command["method"], "task.dispatch");

    let mut mismatched_dispatch = codex_completed_outcome(
        &source_command,
        &descriptor,
        NATIVE_ROOT_ID,
        NATIVE_SCOPE_KEY,
        1,
    );
    mismatched_dispatch.details["dispatch_admission"]["module_receipt"]["artifact"]["version"] =
        json!("8");
    let dispatch_error = owner
        .store
        .call(
            module.clone(),
            "module.outcome".into(),
            serde_json::to_value(mismatched_dispatch).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(dispatch_error.code, "TASK_DISPATCH_ADMISSION_INVALID");

    let mut invalid_terminal = codex_completed_outcome(
        &source_command,
        &descriptor,
        NATIVE_ROOT_ID,
        NATIVE_SCOPE_KEY,
        1,
    );
    invalid_terminal.details["goal_terminal_event"]["event_record"]["status"] = json!("running");
    let terminal_error = owner
        .store
        .call(
            module.clone(),
            "module.outcome".into(),
            serde_json::to_value(invalid_terminal).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(terminal_error.code, "NATIVE_IDENTITY_MISMATCH");

    record_codex_completed_command(
        &owner,
        &module,
        &source_command,
        &descriptor,
        NATIVE_ROOT_ID,
        NATIVE_SCOPE_KEY,
        1,
    )
    .await;

    set_codex_descriptor_enabled(&owner, "7", false).await;
    let disabled_admission = owner.store.reconcile_automations_once().await.unwrap_err();
    assert_eq!(disabled_admission.code, "MODULE_DISABLED");
    let task_id = subject.task_id.clone();
    let continuation_count_while_disabled = owner
        .store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM operations WHERE caller_id=?1 AND task_id=?2 \
                 AND method='agent.send'",
                params![
                    crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                    task_id
                ],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(continuation_count_while_disabled, 0);
    set_codex_descriptor_enabled(&owner, "7", true).await;

    let first_pass = owner.store.reconcile_automations_once().await.unwrap();
    let first_entry = first_pass["goal_progression"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["automation_id"] == AUTOMATION_ID)
        .expect("selected Codex terminal evidence reaches Goal progression");
    assert_eq!(first_entry["recent"][0]["disposition"], "admitted");
    let technical_requester = crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID;
    let first_continuation_id = owner
        .store
        .run({
            let task_id = subject.task_id.clone();
            move |db| {
                Ok(db.query_row(
                    "SELECT operation_id FROM operations WHERE caller_id=?1 AND task_id=?2 \
                     AND method='agent.send' AND state='queued' ORDER BY created_at_ms LIMIT 1",
                    params![technical_requester, task_id],
                    |row| row.get::<_, String>(0),
                )?)
            }
        })
        .await
        .unwrap();
    let first_continuation_command = next_codex_command(&owner, &module, &config).await;
    assert_eq!(
        first_continuation_command["operation_id"],
        first_continuation_id
    );
    assert_eq!(
        first_continuation_command["input"]["goal_continuation_context"]["continuation"]["source_operation_id"],
        source_operation_id
    );

    let mut mismatched_continuation = codex_completed_outcome(
        &first_continuation_command,
        &descriptor,
        NATIVE_ROOT_ID,
        NATIVE_SCOPE_KEY,
        2,
    );
    mismatched_continuation.details["goal_continuation_admission"]["native_input_id"] =
        json!("another-input");
    let continuation_error = owner
        .store
        .call(
            module.clone(),
            "module.outcome".into(),
            serde_json::to_value(mismatched_continuation).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(
        continuation_error.code,
        "GOAL_CONTINUATION_ADMISSION_INVALID"
    );

    record_codex_completed_command(
        &owner,
        &module,
        &first_continuation_command,
        &descriptor,
        NATIVE_ROOT_ID,
        NATIVE_SCOPE_KEY,
        2,
    )
    .await;

    let second_pass = owner.store.reconcile_automations_once().await.unwrap();
    let second_entry = second_pass["goal_progression"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["automation_id"] == AUTOMATION_ID)
        .expect("typed continuation terminal receipt reaches the same slot path");
    assert_eq!(second_entry["recent"][0]["disposition"], "admitted");
    let second_continuation_id = owner
        .store
        .run({
            let task_id = subject.task_id.clone();
            move |db| {
                Ok(db.query_row(
                    "SELECT operation_id FROM operations WHERE caller_id=?1 AND task_id=?2 \
                     AND method='agent.send' AND state='queued' ORDER BY created_at_ms LIMIT 1",
                    params![technical_requester, task_id],
                    |row| row.get::<_, String>(0),
                )?)
            }
        })
        .await
        .unwrap();

    let newer_descriptor = codex_rust_descriptor("8");
    let newer_for_store = newer_descriptor.clone();
    let selection_config = config.clone();
    let manager_for_selection = manager.clone();
    let route_for_selection = route_for_selection.clone();
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let registration = super::super::module_handshake::register_trusted_descriptor(
                &tx,
                newer_for_store.clone(),
            )?;
            super::super::module_handshake::select_route(
                &tx,
                &manager_for_selection,
                &json!({
                    "route_alias":route_for_selection.alias,
                    "module_id":newer_for_store.module_id,
                    "artifact_id":newer_for_store.artifact.artifact_id,
                    "version":newer_for_store.artifact.version,
                    "expected_catalog_revision":registration["catalog_revision"]
                }),
                &selection_config,
                "supersede-o9-codex-goal",
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    let retained_selection = owner
        .store
        .run(|db| {
            let binding = super::super::operations::get_binding(db, BINDING_ID, 1)?;
            let contract = super::super::runtime::selected_codex_controller_contract(db, &binding)?
                .ok_or_else(|| {
                    crate::error::Error::new(
                        "TEST_CODEX_CONTRACT_MISSING",
                        "the immutable selected Codex contract disappeared",
                    )
                })?;
            Ok((
                contract.identity.artifact.version.to_string(),
                contract.current_for_new_goal,
            ))
        })
        .await
        .unwrap();
    assert_eq!(retained_selection.0, "7");
    assert!(!retained_selection.1);

    let second_continuation_command = next_codex_command(&owner, &module, &config).await;
    assert_eq!(
        second_continuation_command["operation_id"],
        second_continuation_id
    );
    record_codex_completed_command(
        &owner,
        &module,
        &second_continuation_command,
        &descriptor,
        NATIVE_ROOT_ID,
        NATIVE_SCOPE_KEY,
        3,
    )
    .await;
    let superseded_pass = owner.store.reconcile_automations_once().await.unwrap();
    let superseded_entry = superseded_pass["goal_progression"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["automation_id"] == AUTOMATION_ID)
        .expect("superseded terminal receipt remains retained");
    let superseded_recent = superseded_entry["recent"]
        .as_array()
        .unwrap()
        .iter()
        .find(|recent| recent["source_operation_id"] == second_continuation_id)
        .expect("the superseded continuation's terminal receipt is present in recent history");
    assert_eq!(superseded_recent["disposition"], "unsupported_runtime");
    let (continuation_count, slot_values) = owner
        .store
        .run({
            let task_id = subject.task_id.clone();
            move |db| {
                let continuation_count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM operations WHERE caller_id=?1 AND task_id=?2 \
                     AND method='agent.send'",
                    params![technical_requester, task_id],
                    |row| row.get(0),
                )?;
                let mut statement = db.prepare(
                    "SELECT value_json FROM meta WHERE key LIKE 'goal-progression:v1:terminal-slot:%' ORDER BY key",
                )?;
                let rows = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let slots = rows
                    .into_iter()
                    .map(|raw| serde_json::from_str::<Value>(&raw).map(|value| value["record"].clone()))
                    .collect::<serde_json::Result<Vec<_>>>()?;
                Ok((continuation_count, slots))
            }
        })
        .await
        .unwrap();
    assert_eq!(continuation_count, 2);
    assert_eq!(slot_values.len(), 3);
    assert_eq!(
        slot_values
            .iter()
            .filter(|slot| slot["disposition"] == "admitted")
            .count(),
        2
    );
    assert!(slot_values.iter().any(|slot| {
        slot["source_operation_id"] == second_continuation_id
            && slot["disposition"] == "unsupported_runtime"
            && slot["operation_id"].is_null()
    }));

    let historical_route = json!({
        "alias":"o9-historical-codex-python",
        "runtime":"codex",
        "module_artifact_id":crate::runtime::codex::HISTORICAL_PYTHON_BRIDGE_ARTIFACT_ID,
        "enabled":true,
        "native_options":{}
    });
    assert!(crate::runtime::codex::is_historical_python_route(
        &historical_route
    ));
    let mut legacy_config = Config::default();
    legacy_config
        .routes
        .push(serde_json::from_value(historical_route.clone()).unwrap());
    // Route validation is part of Config::load, so a retained historical
    // Python route is rejected at load time rather than by a separate
    // Config::validate entry point.
    let legacy_config_file = fixture_directory.join("legacy-codex-python-config.toml");
    std::fs::write(
        &legacy_config_file,
        toml::to_string_pretty(&legacy_config).unwrap(),
    )
    .unwrap();
    assert_eq!(
        Config::load(Some(&legacy_config_file), None)
            .unwrap_err()
            .code,
        "CONFIG_ERROR"
    );

    let history_route_for_store = historical_route.clone();
    owner
        .store
        .run(move |db| {
            let now = crate::model::now_ms()?;
            // Closed historical bindings retain the release timestamp required
            // by the production schema; their native root is no longer live.
            db.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,released_at_ms,created_at_ms) \
                 VALUES(?1,1,'o9-python-history-lane','o9-python-history-instance',?2,'closed','python-history-scope','thread_python_history',?3,'{}',?4,?4)",
                params![
                    HISTORY_BINDING_ID,
                    crate::runtime::codex::HISTORICAL_PYTHON_BRIDGE_ARTIFACT_ID,
                    crate::model::canonical(&history_route_for_store)?,
                    now,
                ],
            )?;
            let request = json!({
                "client_request_id":"retained-python-history",
                "binding_id":HISTORY_BINDING_ID,
                "generation":1,
                "text":"historical input"
            });
            let result = json!({
                "operation_id":HISTORY_OPERATION_ID,
                "state":"settled",
                "outcome":"applied",
                "binding_id":HISTORY_BINDING_ID,
                "binding_generation":1
            });
            db.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,binding_id,binding_generation,state,native_refs_json,result_json,due_at_ms,sent_at_ms,settled_at_ms,created_at_ms,updated_at_ms) \
                 VALUES(?1,'review-gm','retained-python-history','agent.send',?2,'{}',?3,1,'settled','{}',?4,?5,?5,?5,?5,?5)",
                params![
                    HISTORY_OPERATION_ID,
                    crate::model::canonical(&request)?,
                    HISTORY_BINDING_ID,
                    crate::model::canonical(&result)?,
                    now,
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let history_before = owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT state,effective_request_json,result_json FROM operations WHERE operation_id=?1",
                [HISTORY_OPERATION_ID],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
            )?)
        })
        .await
        .unwrap();
    let history_readback = owner
        .store
        .call(
            manager.clone(),
            "operation.get".into(),
            json!({"operation_id":HISTORY_OPERATION_ID}),
        )
        .await
        .unwrap();
    assert_eq!(history_readback["method"], "agent.send");
    assert_eq!(history_readback["state"], "settled");
    assert_eq!(
        history_readback["result"]["operation_id"],
        HISTORY_OPERATION_ID
    );
    let history_after = owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT state,effective_request_json,result_json FROM operations WHERE operation_id=?1",
                [HISTORY_OPERATION_ID],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(history_after, history_before);
    let legacy_command_error = owner
        .store
        .run(|db| {
            super::super::module_handshake::require_selected_native_command(
                db,
                HISTORY_BINDING_ID,
                (
                    crate::runtime::codex::RUNTIME,
                    crate::runtime::codex::HISTORICAL_PYTHON_BRIDGE_ARTIFACT_ID,
                ),
                None,
                "agent.send",
                &json!({"text":"must not start"}),
            )
        })
        .await
        .unwrap_err();
    assert_eq!(legacy_command_error.code, "MODULE_CONTRACT_REQUIRED");

    owner.close().await.unwrap();
    remove_owned_o9_temp_root(&_directory, "swarm-program-o9-codex-goal-").unwrap();
    remove_owned_o9_temp_root(&fixture_directory, "eliot-o9-codex-fixture-").unwrap();
    drop(operator);
}

struct QueuedCodexGoalFixture {
    owner: super::StoreOwner,
    store_directory: std::path::PathBuf,
    workspace_directory: std::path::PathBuf,
    config: Config,
    manager: crate::model::Principal,
    module: crate::model::Principal,
    task_id: String,
    attempt_id: String,
    automation_id: String,
    operation_id: String,
}

async fn queued_codex_goal_fixture(label: &str) -> QueuedCodexGoalFixture {
    use rusqlite::params;

    let workspace_directory =
        std::env::temp_dir().join(format!("eliot-o9-{label}-{}", crate::model::new_id()));
    std::fs::create_dir_all(&workspace_directory).unwrap();
    let route_alias = format!("o9-codex-{label}");
    let binding_id = format!("o9-codex-{label}-binding");
    let module_client_id = format!("o9-codex-{label}-module");
    let native_root_id = format!("thread_o9_codex_{label}");
    let native_scope_key = format!("codex-o9-{label}");
    let module_link_id = format!("link-o9-codex-{label}");
    let goal_id = format!("o9-codex-{label}-goal");
    let automation_id = format!("o9-codex-{label}-automation");
    let route = json!({
        "alias":route_alias,
        "runtime":"codex",
        "module_artifact_id":swarm_contracts::module_contract::CODEX_RUST_CONTROLLER_ARTIFACT_ID,
        "enabled":true,
        "workspace_option":"workspaceRoot",
        "native_options":{
            "modelProvider":"o9-fixture-provider",
            "model":"o9-fixture-model",
            "workspaceRoot":workspace_directory.display().to_string()
        }
    });
    let mut config = Config::default();
    config
        .routes
        .push(serde_json::from_value(route.clone()).unwrap());
    let (owner, store_directory, bootstrap) =
        super::start_store_with_config(&route_alias, config.clone()).await;
    super::seed_clients(&owner.store).await;
    let _operator = owner.store.authenticate(bootstrap).await.unwrap();
    let manager = super::principal("review-gm", Role::Manager);
    let subject = super::seed_subject(
        &owner.store,
        &format!("o9-codex-{label}"),
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let descriptor = codex_rust_descriptor("7");
    let route_for_selection: crate::config::Route = serde_json::from_value(route.clone()).unwrap();
    let selection_config = config.clone();
    let descriptor_for_store = descriptor.clone();
    let attempt_id = subject.attempt_id.clone();
    let route_for_store = route.clone();
    let manager_id = manager.client_id.clone();
    let binding_id_for_store = binding_id.clone();
    let module_client_id_for_store = module_client_id.clone();
    let native_root_id_for_store = native_root_id.clone();
    let native_scope_key_for_store = native_scope_key.clone();
    let module_link_id_for_store = module_link_id.clone();
    let route_for_initial_selection = route_for_selection.clone();
    let label_for_store = label.to_owned();
    owner
        .store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let registration = super::super::module_handshake::register_trusted_descriptor(
                &tx,
                descriptor_for_store.clone(),
            )?;
            super::super::module_handshake::select_route(
                &tx,
                &super::principal(&manager_id, Role::Manager),
                &json!({
                    "route_alias":route_for_initial_selection.alias,
                    "module_id":descriptor_for_store.module_id,
                    "artifact_id":descriptor_for_store.artifact.artifact_id,
                    "version":descriptor_for_store.artifact.version,
                    "expected_catalog_revision":registration["catalog_revision"]
                }),
                &selection_config,
                &format!("select-o9-codex-{label_for_store}"),
            )?;
            let selector = super::super::module_handshake::selection_for_new_binding(
                &tx,
                &manager_id,
                &route_for_initial_selection.alias,
                &route_for_initial_selection.runtime,
                &route_for_initial_selection.module_artifact_id,
            )?
            .ok_or_else(|| {
                crate::error::Error::new(
                    "TEST_CODEX_SELECTION_MISSING",
                    "fixture selected no Codex descriptor",
                )
            })?;
            let state = json!({
                "connection":"connected",
                "bridge_boot_id":format!("o9-{label_for_store}-boot"),
                "module_link_id":module_link_id_for_store,
                "module_client_id":module_client_id_for_store,
                "module_contract_selector":selector,
                "module_contract_selection_owner_manager_id":manager_id,
                "native_scope_key":native_scope_key_for_store,
                "native_root_id":native_root_id_for_store
            });
            let now = crate::model::now_ms()?;
            tx.execute(
                "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,module_artifact_id,state,native_scope_key,native_root_id,route_json,state_json,created_at_ms) \
                 VALUES(?1,1,?2,?3,?4,'ready',?5,?6,?7,?8,?9)",
                params![
                    binding_id_for_store.as_str(),
                    format!("o9-{label_for_store}-lane"),
                    format!("o9-{label_for_store}-instance"),
                    descriptor_for_store.artifact.artifact_id.as_str(),
                    native_scope_key_for_store.as_str(),
                    native_root_id_for_store.as_str(),
                    crate::model::canonical(&route_for_store)?,
                    crate::model::canonical(&state)?,
                    now,
                ],
            )?;
            tx.execute(
                "UPDATE attempts SET state='reserved',binding_id=?2,binding_generation=1 \
                 WHERE attempt_id=?1 AND released_at_ms IS NULL",
                params![attempt_id, binding_id_for_store.as_str()],
            )?;
            tx.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    format!("client:{module_client_id_for_store}"),
                    crate::model::canonical(&json!({
                        "role":"module",
                        "disabled":false,
                        "binding_id":binding_id_for_store,
                        "binding_generation":1
                    }))?,
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
    let module = crate::model::Principal {
        link_id: module_link_id,
        client_id: module_client_id,
        role: Role::Module,
    };
    // Goal.enabled controls its reminder; current Goal-progression authority
    // is the Manager-owned Automation entry exercised below.
    owner
        .store
        .call(
            manager.clone(),
            "goal.create".into(),
            json!({
                "client_request_id":format!("create-o9-codex-{label}"),
                "project_id":"fixture",
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "goal_id":goal_id,
                "expected_revision":0,
                "objective":"Continue only while current Goal authority remains valid.",
                "completion_evidence":{"kind":"task_acceptance"},
                "reminder":null,
                "enabled":false
            }),
        )
        .await
        .unwrap();
    let changes = json!([{
        "automation_id":automation_id,
        "expected_revision":0,
        "include_existing":false,
        "patch":{
            "enabled":true,
            "scope":{"work_pool_id":null},
            "steps":["goal_progression"],
            "goal_progression":{"goal_id":goal_id}
        }
    }]);
    let preview = owner
        .store
        .call(
            manager.clone(),
            "automation.config.preview".into(),
            json!({"project_id":"fixture","changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true);
    owner
        .store
        .call(
            manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":format!("enable-o9-codex-{label}"),
                "project_id":"fixture",
                "changes":changes,
                "preview_digest":preview["plan_sha256"]
            }),
        )
        .await
        .unwrap();
    let dispatch = owner
        .store
        .call(
            manager,
            "task.dispatch".into(),
            json!({
                "client_request_id":format!("dispatch-o9-codex-{label}"),
                "attempt_id":subject.attempt_id,
                "text":"Begin the queued Codex Goal fixture."
            }),
        )
        .await
        .unwrap();
    assert_eq!(dispatch["state"], "queued");
    let source_command = next_codex_command(&owner, &module, &config).await;
    assert_eq!(source_command["method"], "task.dispatch");
    record_codex_completed_command(
        &owner,
        &module,
        &source_command,
        &descriptor,
        &native_root_id,
        &native_scope_key,
        1,
    )
    .await;
    let pass = owner.store.reconcile_automations_once().await.unwrap();
    let entry = pass["goal_progression"]["entries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["automation_id"] == automation_id)
        .expect("terminal dispatch creates the exact Goal continuation");
    assert_eq!(entry["recent"][0]["disposition"], "admitted");
    let task_id = subject.task_id.clone();
    let (operation_id, state, action, cause_kind) = owner
        .store
        .run(move |db| {
            let (operation_id, state): (String, String) = db.query_row(
                "SELECT operation_id,state FROM operations WHERE caller_id=?1 AND task_id=?2 \
                 AND method='agent.send' AND state='queued' ORDER BY created_at_ms LIMIT 1",
                params![
                    crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID,
                    task_id
                ],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            let link = crate::automation::authorization::operation_link(db, &operation_id)?
                .ok_or_else(|| {
                    crate::error::Error::new(
                        "TEST_GOAL_LINK_MISSING",
                        "queued continuation has no durable Goal authority link",
                    )
                })?;
            Ok((operation_id, state, link.action, link.cause["kind"].clone()))
        })
        .await
        .unwrap();
    assert_eq!(state, "queued");
    assert_eq!(action, "agent.send");
    assert_eq!(cause_kind, "goal_progression");
    QueuedCodexGoalFixture {
        owner,
        store_directory,
        workspace_directory,
        config,
        manager: super::principal("review-gm", Role::Manager),
        module,
        task_id: subject.task_id,
        attempt_id: subject.attempt_id,
        automation_id,
        operation_id,
    }
}

async fn assert_queued_codex_goal_rejected(fixture: &QueuedCodexGoalFixture, expected_error: &str) {
    let module = fixture.module.clone();
    let config = fixture.config.clone();
    let next = fixture
        .owner
        .store
        .run(move |db| super::super::runtime::next_with_config(db, &module, &config))
        .await
        .unwrap();
    assert!(
        next["command"].is_null(),
        "unexpected native command: {next}"
    );
    assert_eq!(next["rejected_operation_id"], fixture.operation_id);
    assert_eq!(next["error"]["code"], expected_error);
    let operation_id = fixture.operation_id.clone();
    let (state, sent_at_ms) = fixture
        .owner
        .store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT state,sent_at_ms FROM operations WHERE operation_id=?1",
                [operation_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<i64>>(1)?)),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(state, "rejected");
    assert!(sent_at_ms.is_none(), "rejected Operation was marked sent");
}

async fn close_queued_codex_goal_fixture(fixture: QueuedCodexGoalFixture) {
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.store_directory).unwrap();
    std::fs::remove_dir_all(fixture.workspace_directory).unwrap();
}

#[tokio::test]
async fn queued_codex_continuation_rejects_disabled_goal_progression_before_dispatch() {
    let fixture = queued_codex_goal_fixture("goal-disabled").await;
    let changes = json!([{
        "automation_id":fixture.automation_id,
        "expected_revision":1,
        "include_existing":false,
        "patch":{"enabled":false}
    }]);
    let preview = fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "automation.config.preview".into(),
            json!({"project_id":"fixture","changes":changes.clone()}),
        )
        .await
        .unwrap();
    assert_eq!(preview["valid"], true);
    fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":"disable-o9-codex-goal-progression",
                "project_id":"fixture",
                "changes":changes,
                "preview_digest":preview["plan_sha256"]
            }),
        )
        .await
        .unwrap();
    assert_queued_codex_goal_rejected(&fixture, "AUTOMATION_ACTION_CHANGED").await;
    close_queued_codex_goal_fixture(fixture).await;
}

#[tokio::test]
async fn queued_codex_continuation_rejects_manager_after_current_scope_loss() {
    let fixture = queued_codex_goal_fixture("manager-scope").await;
    fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "gm.handover".into(),
            json!({
                "client_request_id":"handover-o9-codex-queued-goal",
                "client_id":"unrelated-manager"
            }),
        )
        .await
        .unwrap();
    let task_id = fixture.task_id.clone();
    let manager_in_scope = fixture
        .owner
        .store
        .run(move |db| {
            crate::automation::authorization::current_manager_id_has_task_scope(
                db,
                "review-gm",
                &task_id,
                "fixture",
            )
        })
        .await
        .unwrap();
    assert!(!manager_in_scope);
    assert_queued_codex_goal_rejected(&fixture, "GOAL_MANAGER_SCOPE_CONFLICT").await;
    close_queued_codex_goal_fixture(fixture).await;
}
