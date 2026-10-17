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

    async fn settle_sampled_operation(&self, state: &str) {
        let state = state.to_owned();
        self.store()
            .run_fixture(move |db| {
                db.execute(
                "UPDATE operations SET state=?1,result_json='{}',settled_at_ms=2,updated_at_ms=2 \
                 WHERE operation_id='unresolved-sentinel'",
                [state],
            )?;
                Ok(())
            })
            .await
            .unwrap();
    }

    async fn held_observations(&self) -> Vec<(String, Option<String>, String)> {
        self.store()
            .run_fixture(|db| {
                let mut statement = db.prepare(
                    "SELECT kind,operation_id,payload_json FROM observations \
                 WHERE binding_id=?1 ORDER BY observation_id",
                )?;
                let rows = statement
                    .query_map([HELD_BINDING], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
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
    let mut changes = fixture.store().subscribe_module_demand_changes();
    let initial_revision = *changes.borrow_and_update();
    let event = foreign_observation();
    let retained = discard_first_denial(&fixture, &event).await;
    assert!(changes.has_changed().unwrap());
    let denial_revision = *changes.borrow_and_update();
    assert_eq!(denial_revision, initial_revision.wrapping_add(1));
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
        assert!(
            !changes.has_changed().unwrap(),
            "a denial retry must not wake consumers"
        );
        assert_eq!(*changes.borrow(), denial_revision);
    }
    assert_eq!(
        fixture
            .store()
            .record_module_supervisor_observation(observation(OTHER_BINDING, 2))
            .await
            .unwrap(),
        ObservationDisposition::Committed
    );
    assert!(changes.has_changed().unwrap());
    assert_eq!(
        *changes.borrow_and_update(),
        denial_revision.wrapping_add(1)
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
async fn operation_state_denials_replay_without_relaxing_other_stage_guards() {
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
        event["stage"] = json!("worker");
        event["unknown_operation_ids"] = json!(["queued-sentinel"]);
        event["unknown_operation_count"] = json!(1);
        // Neither queued IDs nor terminal IDs at another stage qualify for
        // the narrow bootless ValidateLaunch sample exception.
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

fn sampled_event() -> Value {
    let mut event = observation(HELD_BINDING, 1);
    event["unknown_operation_ids"] = json!(["unresolved-sentinel"]);
    event["unknown_operation_count"] = json!(1);
    event
}

#[tokio::test]
async fn terminal_prelaunch_sample_preserves_snapshot_without_terminal_cause_and_can_progress() {
    for (terminal, mixed) in [
        ("settled", false),
        ("rejected", false),
        ("cancelled", false),
        ("settled", true),
    ] {
        let fixture = Fixture::start().await;
        if mixed {
            fixture.store().run_fixture(|db| {
                db.execute("UPDATE operations SET state='sending' WHERE operation_id='queued-sentinel'", [])?;
                Ok(())
            }).await.unwrap();
        }
        // Construct the producer sample while the actual fixture Operation is
        // unresolved, then make the Store settlement win the delivery race.
        let sampled = fixture.held_snapshot().await;
        assert!(sampled["operations"].as_array().unwrap().iter().any(|row| {
            let value: Value = serde_json::from_str(row.as_str().unwrap()).unwrap();
            value["id"] == "unresolved-sentinel" && value["state"] == "outcome_unknown"
        }));
        let mut event = sampled_event();
        if mixed {
            event["unknown_operation_ids"] = json!(["queued-sentinel", "unresolved-sentinel"]);
            event["unknown_operation_count"] = json!(2);
        }
        fixture.settle_sampled_operation(terminal).await;
        let before = fixture.held_snapshot().await;
        assert_eq!(
            fixture
                .store()
                .record_module_supervisor_observation(&event)
                .await
                .unwrap(),
            ObservationDisposition::Committed
        );
        let retained = fixture.held_snapshot().await;
        assert_eq!(retained["operations"], before["operations"]);
        assert!(retained["receipts"].as_array().unwrap().is_empty());
        assert!(
            retained["binding"]["observation"]
                .get("module_supervisor_quarantine")
                .is_none()
        );
        let rows = fixture.held_observations().await;
        assert_eq!(
            rows.len(),
            1,
            "raced sample must not emit any derived cause trigger"
        );
        assert_eq!(rows[0].0, EVENT_KIND);
        assert!(rows[0].1.is_none());
        let original =
            serde_json::to_value(parse_retained_observation(&rows[0].2).unwrap()).unwrap();
        assert_eq!(
            original, event,
            "preserve every sampled fact, including ID/count/event identity"
        );

        let public = fixture
            .store()
            .run_fixture(|db| operations::get_binding_public(db, HELD_BINDING, GENERATION))
            .await
            .unwrap();
        let diagnostic = &public["observation"]["module_supervisor"];
        let attention = manager_attention_item(
            diagnostic,
            "fixture-scope",
            HELD_BINDING,
            GENERATION,
            model::now_ms().unwrap(),
            60_000,
        )
        .unwrap();
        assert_eq!(
            diagnostic["unknown_operation_ids"],
            event["unknown_operation_ids"]
        );
        assert_eq!(
            diagnostic["unknown_operation_count"],
            event["unknown_operation_count"]
        );
        assert_eq!(diagnostic["stage"], "validate_launch");
        assert_eq!(
            attention["address"]["unknown_operation_ids"],
            event["unknown_operation_ids"]
        );
        assert_eq!(attention["address"]["retry_authorized"], false);

        // A fresh preparation/readback sample can follow without a local or
        // durable rejection fence. It no longer lists the settled work.
        let mut fresh = observation(HELD_BINDING, 2);
        if mixed {
            fresh["unknown_operation_ids"] = json!(["queued-sentinel"]);
            fresh["unknown_operation_count"] = json!(1);
        }
        assert_eq!(
            fixture
                .store()
                .record_module_supervisor_observation(&fresh)
                .await
                .unwrap(),
            ObservationDisposition::Committed
        );
        let mut changes = fixture.store().subscribe_module_demand_changes();
        let revision = *changes.borrow_and_update();
        assert_eq!(
            fixture
                .store()
                .record_module_supervisor_observation(&event)
                .await
                .unwrap(),
            ObservationDisposition::ExactDuplicate
        );
        assert!(!changes.has_changed().unwrap());
        assert_eq!(*changes.borrow(), revision);
        let after = fixture.held_snapshot().await;
        assert_eq!(after["operations"], before["operations"]);
        assert_eq!(
            after["binding"]["observation"]["module_supervisor"]["event_id"],
            fresh["event_id"]
        );
        let rows = fixture.held_observations().await;
        assert_eq!(
            serde_json::to_value(parse_retained_observation(&rows[0].2).unwrap()).unwrap(),
            event
        );
        assert!(
            rows.iter()
                .all(|row| row.1.as_deref() != Some("unresolved-sentinel")),
            "terminal ID must never become a cause"
        );
        fixture.close().await;
    }
}

#[tokio::test]
async fn prelaunch_terminal_tolerance_validates_all_ids_and_exact_generation() {
    for (case, expected) in [
        ("queued", "MODULE_OBSERVATION_OPERATION_TERMINAL"),
        ("missing", "MODULE_OBSERVATION_OPERATION_SCOPE"),
        ("foreign", "MODULE_OBSERVATION_OPERATION_SCOPE"),
        ("generation", "MODULE_OBSERVATION_OPERATION_SCOPE"),
        ("invalid", "MODULE_OBSERVATION_OPERATION_TERMINAL"),
    ] {
        let fixture = Fixture::start().await;
        let mut event = sampled_event();
        // Terminal ID sorts first: accepting it must not skip validation of
        // any later queued, foreign, missing or corrupt scoped Operation.
        event["unknown_operation_ids"] = json!(["unresolved-sentinel", "z-bad-id"]);
        event["unknown_operation_count"] = json!(2);
        fixture.settle_sampled_operation("settled").await;
        fixture.store().run_fixture(move |db| {
            if case == "missing" {
                return Ok(());
            }
            if case == "generation" {
                db.execute(
                    "INSERT INTO bindings(binding_id,generation,lane_id,module_instance_id,\
                         module_artifact_id,state,route_json,state_json,created_at_ms) \
                     SELECT binding_id,8,'other-generation','other-generation',\
                         module_artifact_id,state,route_json,state_json,created_at_ms \
                     FROM bindings WHERE binding_id=?1 AND generation=?2",
                    params![HELD_BINDING, GENERATION],
                )?;
            }
            if case == "invalid" {
                db.execute_batch("PRAGMA ignore_check_constraints=ON;")?;
            }
            db.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,\
                     original_request_json,effective_request_json,binding_id,\
                     binding_generation,state,due_at_ms,created_at_ms,updated_at_ms) \
                 VALUES('z-bad-id','retry-fixture','z-bad-id','agent.send','{}','{}',?1,?2,?3,1,1,1)",
                params![if case == "foreign" { OTHER_BINDING } else { HELD_BINDING },
                    if case == "generation" { 8 } else { GENERATION },
                    match case { "queued" => "queued", "invalid" => "invalid-state", _ => "sending" }],
            )?;
            if case == "invalid" {
                db.execute_batch("PRAGMA ignore_check_constraints=OFF;")?;
            }
            Ok(())
        }).await.unwrap();
        let before = fixture.held_snapshot().await;
        assert_eq!(
            fixture
                .store()
                .record_module_supervisor_observation(event)
                .await
                .unwrap(),
            ObservationDisposition::ScopedPermanentRejection {
                error_code: expected.to_owned()
            }
        );
        let after = fixture.held_snapshot().await;
        assert_eq!(after["operations"], before["operations"]);
        assert_eq!(after["receipts"].as_array().unwrap().len(), 1);
        assert!(after["binding"]["observation"]["module_supervisor_quarantine"].is_object());
        assert!(
            after["binding"]["observation"]
                .get("module_supervisor")
                .is_none()
        );
        assert!(
            fixture
                .held_observations()
                .await
                .iter()
                .all(|row| row.0 == REJECTION_EVENT_KIND)
        );
        fixture.close().await;
    }
}

#[tokio::test]
async fn terminal_sample_does_not_release_existing_quarantine_or_rewrite_receipts() {
    let fixture = Fixture::start().await;
    let old = discard_first_denial(&fixture, &foreign_observation()).await;
    fixture.settle_sampled_operation("settled").await;
    let before = fixture.held_snapshot().await;
    let mut event = sampled_event();
    event["sequence"] = json!(2);
    event["event_id"] = json!(format!("{ACTOR}:2"));
    assert_eq!(
        fixture
            .store()
            .record_module_supervisor_observation(event)
            .await
            .unwrap(),
        ObservationDisposition::ScopedPermanentRejection {
            error_code: "MODULE_OBSERVATION_SCOPE_QUARANTINED".to_owned(),
        }
    );
    let after = fixture.held_snapshot().await;
    assert_eq!(after["binding"], old["binding"]);
    assert_eq!(after["receipts"][0], old["receipts"][0]);
    assert_eq!(after["operations"], before["operations"]);
    assert_eq!(after["receipts"].as_array().unwrap().len(), 2);
    fixture.close().await;
}

#[tokio::test]
async fn terminal_tolerance_requires_bootless_isolated_unknown_shape() {
    for case in ["boot", "phase", "certainty"] {
        let fixture = Fixture::start().await;
        let mut event = sampled_event();
        fixture.settle_sampled_operation("settled").await;
        // These are adversarial negative callbacks, never evidence of a real
        // helper boot or a certified NotStarted outcome.
        match case {
            "boot" => {
                event["boot_id"] = json!("unproven-test-boot");
                event["event_id"] = json!(format!("unproven-test-boot:{ACTOR}:1"));
            }
            "phase" => event["phase"] = json!("waiting_for_kernel"),
            "certainty" => event["effect_certainty"] = json!("not_started"),
            _ => unreachable!(),
        }
        let before = fixture.held_snapshot().await;
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
        let after = fixture.held_snapshot().await;
        assert_eq!(after["operations"], before["operations"]);
        assert_eq!(after["receipts"].as_array().unwrap().len(), 1);
        assert!(after["binding"]["observation"]["module_supervisor_quarantine"].is_object());
        assert!(
            fixture
                .held_observations()
                .await
                .iter()
                .all(|row| row.0 == REJECTION_EVENT_KIND)
        );
        fixture.close().await;
    }
}
