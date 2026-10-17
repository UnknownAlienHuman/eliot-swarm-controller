//! Regressions at the real Store transaction/ACK boundary. No native process
//! or provider is involved; loss is modeled by discarding the first durable ACK.

use super::*;
use crate::{
    config::Config,
    platform::{DataRoot, bootstrap_credential},
    store::{Store, StoreOwner, operations},
};
use std::{fs, path::PathBuf, sync::Arc};

const HELD_BINDING: &str = "rejection-retry-held";
const OTHER_BINDING: &str = "rejection-retry-other";
const GENERATION: i64 = 7;
const ACTOR: &str = "11400000-0000-4000-8000-000000000003";
const ORIGINAL_CODE: &str = "MODULE_OBSERVATION_IDENTITY_MISMATCH";
const PRIVATE_INPUT: &str = "PRIVATE_REJECTION_RETRY_OPERATION_INPUT";

struct Fixture {
    root: PathBuf,
    owner: StoreOwner,
}

impl Fixture {
    async fn start() -> Self {
        let root = std::env::temp_dir().join(format!("swarm-rejection-retry-{}", model::new_id()));
        let data_root = DataRoot::acquire(&root).unwrap();
        let root = data_root.path.clone();
        let mut config = Config::default();
        config.storage.data_dir = root.clone();
        let credential = bootstrap_credential(&root).unwrap();
        let owner = StoreOwner::start(data_root, Arc::new(config), credential)
            .await
            .unwrap();
        owner
            .store
            .admit_module_supervisor_actor(ACTOR)
            .await
            .unwrap();
        owner
            .store
            .run_fixture(|db| {
                let selector = model::canonical(&json!({
                    "module_contract_selector":{
                        "schema_version":1, "registered_revision":1, "selected_revision":1,
                        "module_id":"rejection-retry-module",
                        "artifact":{"artifact_id":"rejection-retry-artifact",
                            "version":"1.0.0", "build_id":null},
                    },
                }))?;
                for binding in [HELD_BINDING, OTHER_BINDING] {
                    db.execute(
                        "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,\
                         module_artifact_id,state,route_json,state_json,created_at_ms) \
                     VALUES(?1,?2,?1,?1,'rejection-retry-artifact','opening','{}',?3,1)",
                        params![binding, GENERATION, selector],
                    )?;
                }
                for (id, state) in [
                    ("queued-sentinel", "queued"),
                    ("unresolved-sentinel", "outcome_unknown"),
                ] {
                    db.execute(
                        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,\
                         original_request_json,effective_request_json,binding_id,\
                         binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) \
                     VALUES(?1,'retry-fixture',?1,'agent.send',?2,'{}',?3,?4,?5,1,1,1)",
                        params![
                            id,
                            model::canonical(&json!({"input":PRIVATE_INPUT}))?,
                            HELD_BINDING,
                            GENERATION,
                            state
                        ],
                    )?;
                }
                Ok(())
            })
            .await
            .unwrap();
        Self { root, owner }
    }

    fn store(&self) -> &Store {
        &self.owner.store
    }

    async fn held_snapshot(&self) -> Value {
        self.store()
            .run_fixture(|db| {
                let binding = operations::get_binding(db, HELD_BINDING, GENERATION)?;
                let mut statement = db.prepare(
                    "SELECT payload_json FROM observations WHERE binding_id=?1 AND kind=?2 \
                 ORDER BY observation_id",
                )?;
                let receipts = statement
                    .query_map(params![HELD_BINDING, REJECTION_EVENT_KIND], |row| {
                        row.get::<_, String>(0)
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let mut statement = db.prepare(
                    "SELECT json_object('id',operation_id,'state',state,\
                     'original',original_request_json,'effective',effective_request_json,\
                     'result',result_json,'native_refs',native_refs_json,\
                     'updated',updated_at_ms,'settled',settled_at_ms) \
                 FROM operations ORDER BY operation_id",
                )?;
                let operations = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(json!({"binding":binding,"receipts":receipts,"operations":operations}))
            })
            .await
            .unwrap()
    }

    async fn close(self) {
        self.owner.close().await.unwrap();
        fs::remove_dir_all(self.root).unwrap();
    }
}

fn observation(binding: &str, sequence: u64) -> Value {
    json!({
        "schema_version":1, "actor_instance_id":ACTOR,
        "event_id":format!("{ACTOR}:{sequence}"), "sequence":sequence,
        "module_id":"rejection-retry-module", "artifact_id":"rejection-retry-artifact",
        "artifact_version":"1.0.0", "build_id":null,
        "scope":{"binding_id":binding,"generation":GENERATION}, "boot_id":null,
        "phase":"isolated", "effect_certainty":"unknown", "stage":"validate_launch",
        "error_code":"MODULE_CONFIG_SCHEMA_UNSUPPORTED", "unknown_operation_ids":[],
        "unknown_operation_count":0, "unknown_operation_ids_truncated":false,
    })
}

fn foreign_observation() -> Value {
    let mut value = observation(HELD_BINDING, 1);
    value["artifact_id"] = json!("different-artifact");
    value
}

async fn discard_first_denial(fixture: &Fixture, event: &Value) -> Value {
    // The response is deliberately discarded only after the production facade
    // has committed both its rejection receipt and its exact scope hold.
    let _ = fixture
        .store()
        .record_module_supervisor_observation(event)
        .await
        .unwrap();
    let retained = fixture.held_snapshot().await;
    assert_eq!(retained["receipts"].as_array().unwrap().len(), 1);
    let facts: Value = serde_json::from_str(retained["receipts"][0].as_str().unwrap()).unwrap();
    assert_eq!(facts["error_code"], ORIGINAL_CODE);
    let digest = facts["observation_sha256"].as_str().unwrap();
    assert_eq!(digest.len(), 64);
    assert!(
        digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    );
    assert_eq!(
        digest,
        model::digest(model::canonical(event).unwrap().as_bytes())
    );
    assert!(!facts.to_string().contains(PRIVATE_INPUT));
    assert!(facts.get("unknown_operation_ids").is_none());
    assert_eq!(
        retained["binding"]["observation"]["module_supervisor_quarantine"]["error_code"],
        ORIGINAL_CODE
    );
    retained
}

#[tokio::test]
async fn lost_first_permanent_ack_replays_original_denial_and_other_scope_progresses() {
    let fixture = Fixture::start().await;
    let event = foreign_observation();
    let retained = discard_first_denial(&fixture, &event).await;
    // Its existing quarantine would now derive SCOPE_QUARANTINED; an exact
    // callback retry must instead return the original retained denial.
    for _ in 0..2 {
        assert_eq!(
            fixture
                .store()
                .record_module_supervisor_observation(&event)
                .await
                .unwrap(),
            ObservationDisposition::ScopedPermanentRejection {
                error_code: ORIGINAL_CODE.to_owned(),
            }
        );
        assert_eq!(fixture.held_snapshot().await, retained);
    }
    assert_eq!(
        fixture
            .store()
            .record_module_supervisor_observation(observation(OTHER_BINDING, 2))
            .await
            .unwrap(),
        ObservationDisposition::Committed
    );
    let other = fixture
        .store()
        .run_fixture(|db| operations::get_binding_public(db, OTHER_BINDING, GENERATION))
        .await
        .unwrap();
    assert_eq!(
        other["observation"]["module_supervisor"]["event_id"],
        format!("{ACTOR}:2")
    );
    assert_eq!(fixture.held_snapshot().await, retained);
    fixture.close().await;
}

#[tokio::test]
async fn changed_rejected_payload_cannot_replay_the_original_denial() {
    let fixture = Fixture::start().await;
    let event = foreign_observation();
    let retained = discard_first_denial(&fixture, &event).await;
    let mut changed_version = event.clone();
    changed_version["artifact_version"] = json!("1.0.1");
    let mut changed_stage = event.clone();
    changed_stage["stage"] = json!("worker");
    let mut changed_code = event.clone();
    changed_code["error_code"] = json!("MODULE_CONFIG_INVALID");
    let mut changed_unknown = event.clone();
    changed_unknown["unknown_operation_ids"] = json!(["unresolved-sentinel"]);
    changed_unknown["unknown_operation_count"] = json!(1);
    for changed in [
        changed_version,
        changed_stage,
        changed_code,
        changed_unknown,
    ] {
        assert_eq!(
            fixture
                .store()
                .record_module_supervisor_observation(changed)
                .await
                .unwrap_err()
                .code,
            "MODULE_OBSERVATION_REJECTION_CONFLICT"
        );
        assert_eq!(fixture.held_snapshot().await, retained);
    }
    assert_eq!(
        fixture
            .store()
            .record_module_supervisor_observation(event)
            .await
            .unwrap(),
        ObservationDisposition::ScopedPermanentRejection {
            error_code: ORIGINAL_CODE.to_owned(),
        }
    );
    fixture.close().await;
}

#[tokio::test]
async fn legacy_rejection_without_digest_remains_held_and_is_not_upgraded() {
    let fixture = Fixture::start().await;
    let event = foreign_observation();
    discard_first_denial(&fixture, &event).await;
    // Historical input fixture, not a production migration or rewrite path.
    fixture
        .store()
        .run_fixture(|db| {
            db.execute(
            "UPDATE observations SET payload_json=json_remove(payload_json,'$.observation_sha256') \
             WHERE binding_id=?1 AND kind=?2",
            params![HELD_BINDING, REJECTION_EVENT_KIND],
        )?;
            Ok(())
        })
        .await
        .unwrap();
    let legacy = fixture.held_snapshot().await;
    assert_eq!(
        fixture
            .store()
            .record_module_supervisor_observation(event)
            .await
            .unwrap_err()
            .code,
        "MODULE_OBSERVATION_REJECTION_CONFLICT"
    );
    assert_eq!(fixture.held_snapshot().await, legacy);
    fixture.close().await;
}

#[tokio::test]
async fn operation_state_denials_replay_without_relaxing_queued_or_terminal_guards() {
    for state in ["queued", "settled"] {
        let fixture = Fixture::start().await;
        if state == "settled" {
            fixture
                .store()
                .run_fixture(|db| {
                    db.execute(
                        "UPDATE operations SET state='settled',result_json='{}',settled_at_ms=2 \
                     WHERE operation_id='queued-sentinel'",
                        [],
                    )?;
                    Ok(())
                })
                .await
                .unwrap();
        }
        let mut event = observation(HELD_BINDING, 1);
        event["unknown_operation_ids"] = json!(["queued-sentinel"]);
        event["unknown_operation_count"] = json!(1);
        // Both states intentionally retain today's denial policy. This test
        // changes ACK replay, not the terminal-settlement race semantics.
        let _ = fixture
            .store()
            .record_module_supervisor_observation(&event)
            .await
            .unwrap();
        let retained = fixture.held_snapshot().await;
        let facts: Value = serde_json::from_str(retained["receipts"][0].as_str().unwrap()).unwrap();
        assert_eq!(facts["error_code"], "MODULE_OBSERVATION_OPERATION_TERMINAL");
        assert_eq!(
            fixture
                .store()
                .record_module_supervisor_observation(event)
                .await
                .unwrap(),
            ObservationDisposition::ScopedPermanentRejection {
                error_code: "MODULE_OBSERVATION_OPERATION_TERMINAL".to_owned(),
            }
        );
        assert_eq!(fixture.held_snapshot().await, retained);
        fixture.close().await;
    }
}
