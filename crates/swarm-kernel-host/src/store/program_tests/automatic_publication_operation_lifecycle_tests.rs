//! Connected Store coverage for automatic Forge publication Operation lifecycles.
//!
//! Admission and cancellation use the real Store calls. The ambiguous outcome
//! and corrupt cancellation reference are injected only after those retained
//! Operations exist, then exercised through public automation reconciliation.

use super::*;
use rusqlite::params;
use serde_json::{Value, json};
use std::path::PathBuf;

const AUTOMATION_ID: &str = "accepted-candidate-publication";
const PROJECT_ID: &str = "fixture";
const TARGET_REF: &str = "refs/heads/main";
const CANDIDATE_BYTES: &[u8] = b"explicit source snapshot fixture";

struct PublicationFixture {
    owner: StoreOwner,
    data_directory: PathBuf,
    repository_path: PathBuf,
    operator: Principal,
    manager: Principal,
    subject: ReviewSubject,
    accepted_operation_id: String,
}

#[derive(Debug)]
struct RetainedForgeOperation {
    operation_id: String,
    caller_id: String,
    state: String,
    result: Value,
    created_at_ms: i64,
    sent_at_ms: Option<i64>,
    settled_at_ms: Option<i64>,
    updated_at_ms: i64,
}

async fn accepted_publication_fixture(label: &str) -> PublicationFixture {
    let repository_path =
        std::env::temp_dir().join(format!("swarm-publication-repository-{}", model::new_id()));
    std::fs::create_dir_all(&repository_path).unwrap();

    let mut config = Config::default();
    config.forge.enabled = true;
    config.forge.git_executable = fixture_git_executable();
    config.forge.projects.insert(
        PROJECT_ID.into(),
        crate::forge::ForgeProject {
            canonical_repository: "github.com/owner/publication-fixture".into(),
            repository_path: repository_path.clone(),
            remote_name: "origin".into(),
            policy_revision: crate::policy::OWNER_POLICY_V2_ID.into(),
            target_refs: vec![TARGET_REF.into()],
        },
    );
    let (owner, data_directory, bootstrap_credential) =
        start_store_with_config(label, config).await;
    let operator = owner
        .store
        .authenticate(bootstrap_credential)
        .await
        .unwrap();
    seed_clients(&owner.store).await;

    let subject = seed_subject(
        &owner.store,
        label,
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    configure_acceptance_fixture(&owner.store, "review-gm", std::slice::from_ref(&subject)).await;

    let candidate_ref = subject.candidate_ref.clone();
    let submission_ref = subject.submission_ref.clone();
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    let (candidate, submission, submission_bytes) = owner
        .store
        .run(move |db| {
            let mut metadata = results::get(db, &candidate_ref)?.metadata;
            metadata["task_id"] = json!(task_id);
            metadata["attempt_id"] = json!(attempt_id);
            metadata["task_revision"] = json!(1);
            metadata["coverage"] = json!("complete");
            metadata["commit"] = json!("1".repeat(40));
            metadata["tree"] = json!("2".repeat(40));
            db.execute(
                "UPDATE artifacts SET metadata_json=?2 WHERE artifact_id=?1 AND kind='source_snapshot'",
                params![candidate_ref, model::canonical(&metadata)?],
            )?;
            let candidate = results::get(db, &candidate_ref)?;
            let submission = results::get(db, &submission_ref)?;
            let document = submissions::document(db, &submission_ref)?;
            Ok((
                candidate,
                submission,
                model::canonical(&document)?.into_bytes(),
            ))
        })
        .await
        .unwrap();
    let candidate_bytes = CANDIDATE_BYTES.to_vec();
    owner
        .store
        .file_io(move |files| {
            files.publish(&candidate, &candidate_bytes)?;
            files.publish(&submission, &submission_bytes)?;
            Ok(())
        })
        .await
        .unwrap();

    let manager = principal("review-gm", Role::Manager);
    let acceptance = owner
        .store
        .call(
            manager.clone(),
            "task.accept".into(),
            json!({
                "client_request_id":format!("accept-publication-lifecycle-{label}"),
                "attempt_id":subject.attempt_id,
                "expected_revision":1,
                "submission_ref":subject.submission_ref,
                "candidate_ref":subject.candidate_ref,
                "expected_feedback_observation_id":0,
                "reason":"The connected lifecycle fixture accepts this retained candidate.",
                "reviews":[{
                    "requirement_id":"R1",
                    "rationale":"The candidate satisfies the fixture requirement.",
                    "evidence":[subject.candidate_ref]
                }],
                "check_ids":[]
            }),
        )
        .await
        .unwrap();
    let accepted_operation_id = acceptance["operation_id"]
        .as_str()
        .expect("task.accept returns its retained Operation ID")
        .to_owned();
    let accepted = retained_operation(&owner.store, &operator, &accepted_operation_id).await;
    assert_eq!(accepted["method"], "task.accept");
    assert_eq!(accepted["state"], "settled");
    assert_eq!(accepted["result"]["outcome"], "applied");

    let configured = owner
        .store
        .call(
            manager.clone(),
            "automation.config.apply".into(),
            json!({
                "client_request_id":format!("enable-publication-lifecycle-{label}"),
                "project_id":PROJECT_ID,
                "changes":[{
                    "automation_id":AUTOMATION_ID,
                    "expected_revision":0,
                    "include_existing":true,
                    "patch":{
                        "enabled":true,
                        "scope":{"work_pool_id":null},
                        "steps":["publication"],
                        "publication":{
                            "target_ref":TARGET_REF,
                            "expected_old_ref":null,
                            "expected_create":true
                        }
                    }
                }]
            }),
        )
        .await
        .unwrap();
    assert_eq!(configured["applied"], true);

    PublicationFixture {
        owner,
        data_directory,
        repository_path,
        operator,
        manager,
        subject,
        accepted_operation_id,
    }
}

fn manual_publication_request(fixture: &PublicationFixture, client_request_id: &str) -> Value {
    json!({
        "client_request_id":client_request_id,
        "attempt_id":fixture.subject.attempt_id,
        "expected_revision":1,
        "submission_ref":fixture.subject.submission_ref,
        "accepted_operation_id":fixture.accepted_operation_id,
        "candidate_ref":fixture.subject.candidate_ref,
        "expected_policy_revision":crate::policy::OWNER_POLICY_V2_ID,
        "target_ref":TARGET_REF,
        "expected_old_ref":null,
        "expected_create":true
    })
}

async fn retained_operation(store: &Store, principal: &Principal, operation_id: &str) -> Value {
    let public = store
        .call(
            principal.clone(),
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    // Public operation.get deliberately does not project arbitrary Forge or
    // cancellation receipts. Inspect exact retained bytes only at this Store
    // fixture boundary, while independently checking the public state read.
    let retained_id = operation_id.to_owned();
    let retained = store
        .run(move |db| super::super::operations::get_operation(db, &retained_id))
        .await
        .unwrap();
    for field in ["operation_id", "method", "state"] {
        assert_eq!(public[field], retained[field]);
    }
    match retained["method"].as_str() {
        Some("forge.publish_ref" | "operation.cancel") => {
            assert_eq!(public["result_status"], "not_projected");
            assert!(public.get("result").is_none());
        }
        Some("task.accept") => {
            assert_eq!(public["result"]["outcome"], retained["result"]["outcome"]);
        }
        method => panic!("unexpected retained fixture method: {method:?}"),
    }
    retained
}

async fn reconcile_publication_fixture(
    fixture: &PublicationFixture,
) -> crate::error::Result<Value> {
    let store = &fixture.owner.store;
    // The fixture configures an acceptance entry beside its publication entry.
    // The existing global entry cursor may visit that other entry first.
    for _ in 0..2 {
        let pass = store.reconcile_automations_once().await?;
        if pass["publication"]["status"] == "degraded" {
            return Ok(pass);
        }
        if pass["publication"]["entries"]
            .as_array()
            .is_some_and(|entries| {
                entries
                    .iter()
                    .any(|entry| entry["automation_id"] == AUTOMATION_ID)
            })
        {
            return Ok(pass);
        }
    }
    Err(crate::error::Error::new(
        "FIXTURE_PUBLICATION_ENTRY_MISSING",
        "two actual entry pages did not reach the configured publication entry",
    ))
}

async fn retained_cancellation_request(store: &Store, operation_id: &str) -> Value {
    let operation_id = operation_id.to_owned();
    let raw = store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT original_request_json FROM operations \
                 WHERE operation_id=?1 AND method='operation.cancel' AND state='settled'",
                [operation_id],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

async fn publication_explanation(fixture: &PublicationFixture) -> Value {
    fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "automation.config.explain".into(),
            json!({"project_id":PROJECT_ID,"automation_id":AUTOMATION_ID}),
        )
        .await
        .unwrap()
}

async fn retained_forge_operations(store: &Store) -> Vec<RetainedForgeOperation> {
    let rows = store
        .run(|db| {
            let mut statement = db.prepare(
                "SELECT operation_id,caller_id,state,result_json,created_at_ms,sent_at_ms, \
                        settled_at_ms,updated_at_ms FROM operations \
                 WHERE method='forge.publish_ref' ORDER BY created_at_ms,operation_id",
            )?;
            let rows = statement
                .query_map([], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                        row.get::<_, Option<i64>>(6)?,
                        row.get::<_, i64>(7)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
        .unwrap();
    rows.into_iter()
        .map(
            |(
                operation_id,
                caller_id,
                state,
                result,
                created_at_ms,
                sent_at_ms,
                settled_at_ms,
                updated_at_ms,
            )| RetainedForgeOperation {
                operation_id,
                caller_id,
                state,
                result: result
                    .map(|value| serde_json::from_str(&value).unwrap())
                    .unwrap_or(Value::Null),
                created_at_ms,
                sent_at_ms,
                settled_at_ms,
                updated_at_ms,
            },
        )
        .collect()
}

fn automatic_operation_id(rows: &[RetainedForgeOperation]) -> String {
    let automatic = rows
        .iter()
        .filter(|row| {
            row.caller_id == crate::automation::authorization::AUTOMATION_TECHNICAL_REQUESTER_ID
        })
        .collect::<Vec<_>>();
    assert_eq!(
        automatic.len(),
        1,
        "one automatic publication wrapper: {rows:?}"
    );
    automatic[0].operation_id.clone()
}

fn assert_one_effect_operation(rows: &[RetainedForgeOperation], operation_id: &str) {
    let effects = rows
        .iter()
        .filter(|row| row.result["outcome"] != "coalesced")
        .collect::<Vec<_>>();
    assert_eq!(effects.len(), 1, "one retained effect Operation: {rows:?}");
    assert_eq!(effects[0].operation_id, operation_id);
}

async fn retained_effective_request(store: &Store, operation_id: &str) -> Value {
    let operation_id = operation_id.to_owned();
    let raw = store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT effective_request_json FROM operations \
                 WHERE operation_id=?1 AND method='forge.publish_ref'",
                [operation_id],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

async fn retained_acceptance_observation(
    store: &Store,
    accepted_operation_id: &str,
) -> (i64, String, String) {
    let accepted_operation_id = accepted_operation_id.to_owned();
    let source_event_key = format!("accept:{accepted_operation_id}");
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT observation_id,source_event_key,operation_id FROM observations \
                 WHERE source_stream_id='controller:acceptance' AND kind='task.acceptance' \
                   AND source_event_key=?1 AND operation_id=?2",
                params![source_event_key, accepted_operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?)
        })
        .await
        .unwrap()
}

async fn retained_publication_state(store: &Store, accepted_operation_id: &str) -> Value {
    let accepted_operation_id = accepted_operation_id.to_owned();
    let key = format!(
        "automation:v1:publication:state:{}:{AUTOMATION_ID}",
        crate::automation::config::scope_digest("review-gm", "fixture").unwrap()
    );
    store
        .run(move |db| {
            let state =
                crate::automation::config::read_record(db, &key, "publication fixture state")?
                    .ok_or_else(|| {
                        crate::error::Error::new("NOT_FOUND", "publication fixture state")
                    })?;
            if state["pending"][0]["accepted_operation_id"] != accepted_operation_id {
                return Err(crate::error::Error::conflict(
                    "publication fixture pending identity differs",
                ));
            }
            Ok(state)
        })
        .await
        .unwrap()
}

fn assert_retained_publication_context(effective: &Value, fixture: &PublicationFixture) {
    assert_eq!(
        effective["automation_on_behalf"]["cause"]["kind"],
        "task.acceptance"
    );
    assert_eq!(
        effective["automation_on_behalf"]["cause"]["operation_id"],
        fixture.accepted_operation_id
    );
    assert_eq!(
        effective["publication_intent"]["accepted_operation_id"],
        fixture.accepted_operation_id
    );
    assert_eq!(
        effective["publication_intent"]["candidate_ref"],
        fixture.subject.candidate_ref
    );
    assert_eq!(
        effective["publication_intent"]["candidate_sha256"],
        model::digest(CANDIDATE_BYTES)
    );
    assert_eq!(effective["publication_intent"]["target_ref"], TARGET_REF);
}

async fn make_pending_publication_due(store: &Store, accepted_operation_id: &str) {
    let accepted_operation_id = accepted_operation_id.to_owned();
    let key = format!(
        "automation:v1:publication:state:{}:{AUTOMATION_ID}",
        crate::automation::config::scope_digest("review-gm", "fixture").unwrap()
    );
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let mut state =
                crate::automation::config::read_record(&tx, &key, "publication fixture state")?
                    .ok_or_else(|| {
                        crate::error::Error::new("NOT_FOUND", "publication fixture state")
                    })?;
            let pending = state["pending"].as_array_mut().ok_or_else(|| {
                crate::error::Error::invalid("publication pending state is invalid")
            })?;
            if pending.len() != 1 || pending[0]["accepted_operation_id"] != accepted_operation_id {
                return Err(crate::error::Error::conflict(
                    "fixture did not retain exactly the expected pending acceptance",
                ));
            }
            pending[0]["next_retry_at_ms"] = json!(0);
            crate::automation::config::write_record(&tx, &key, &state)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn retain_ambiguous_publication_effect(store: &Store, operation_id: &str) {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let state: String = tx.query_row(
                "SELECT state FROM operations WHERE operation_id=?1 \
                 AND method='forge.publish_ref'",
                [&operation_id],
                |row| row.get(0),
            )?;
            if state != "queued" {
                return Err(crate::error::Error::conflict(
                    "fixture can inject uncertainty only after real queued admission",
                ));
            }
            let now_ms = model::now_ms()?;
            let receipt = json!({
                "operation_id":operation_id,
                "outcome":"unknown",
                "publication":"requires_readback",
                "reason":"fixture retains a lost response after a possible Forge effect",
                "publication_may_have_started":true,
                "force":false
            });
            let changed = tx.execute(
                "UPDATE operations SET state='outcome_unknown',sent_at_ms=?2,settled_at_ms=NULL, \
                 result_json=?3,updated_at_ms=?2 \
                 WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued' \
                   AND sent_at_ms IS NULL",
                params![operation_id, now_ms, model::canonical(&receipt)?],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "fixture could not retain the ambiguous publication receipt",
                ));
            }
            crate::store::capacity::sync_operation(&tx, &operation_id, now_ms)?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn cancel_publication(
    fixture: &PublicationFixture,
    target_operation_id: &str,
    request_id: &str,
    reason: &str,
) -> (String, Value, Value) {
    let response = fixture
        .owner
        .store
        .call(
            fixture.operator.clone(),
            "operation.cancel".into(),
            json!({
                "client_request_id":request_id,
                "operation_id":target_operation_id,
                "reason":reason
            }),
        )
        .await
        .unwrap();
    assert_eq!(response["cancelled_operation_id"], target_operation_id);
    assert_eq!(response["native_cancel_sent"], false);
    let cancel_operation_id = response["operation_id"]
        .as_str()
        .expect("operation.cancel returns its retained Operation ID")
        .to_owned();
    let cancel_operation = retained_operation(
        &fixture.owner.store,
        &fixture.operator,
        &cancel_operation_id,
    )
    .await;
    let target =
        retained_operation(&fixture.owner.store, &fixture.operator, target_operation_id).await;
    let cancel_request =
        retained_cancellation_request(&fixture.owner.store, &cancel_operation_id).await;
    assert_eq!(response["operation_id"], cancel_operation_id);
    assert_eq!(cancel_request["operation_id"], target_operation_id);
    assert_eq!(cancel_request["reason"], reason);
    assert_eq!(cancel_operation["method"], "operation.cancel");
    assert_eq!(cancel_operation["state"], "settled");
    assert_eq!(
        cancel_operation["result"]["operation_id"],
        cancel_operation_id
    );
    assert_eq!(
        cancel_operation["result"]["cancelled_operation_id"],
        target_operation_id
    );
    assert_eq!(cancel_operation["result"]["native_cancel_sent"], false);
    assert_eq!(target["state"], "cancelled");
    assert_eq!(target["result"]["cancelled_by"], cancel_operation_id);
    assert_eq!(target["result"]["reason"], reason);
    (cancel_operation_id, cancel_operation, target)
}

async fn corrupt_cancelled_by(store: &Store, target_operation_id: &str) {
    let target_operation_id = target_operation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (state, raw): (String, String) = tx.query_row(
                "SELECT state,result_json FROM operations WHERE operation_id=?1 \
                 AND method='forge.publish_ref'",
                [&target_operation_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            if state != "cancelled" {
                return Err(crate::error::Error::conflict(
                    "negative fixture corruption requires an actual Store cancellation first",
                ));
            }
            let mut receipt: Value = serde_json::from_str(&raw)?;
            if receipt["cancelled_by"].as_str().is_none() {
                return Err(crate::error::Error::conflict(
                    "actual cancellation receipt has no retained cancelled_by identity",
                ));
            }
            receipt["cancelled_by"] = json!("missing-cancel-operation-fixture-corruption");
            tx.execute(
                "UPDATE operations SET result_json=?2 WHERE operation_id=?1 \
                 AND method='forge.publish_ref' AND state='cancelled'",
                params![target_operation_id, model::canonical(&receipt)?],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn admit_coalesced_wrapper(
    fixture: &PublicationFixture,
    owner_request_id: &str,
) -> (String, String) {
    let owner_admission = fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "forge.publish_ref".into(),
            manual_publication_request(fixture, owner_request_id),
        )
        .await
        .unwrap();
    assert_ne!(owner_admission["coalesced"], true);
    let owner_operation_id = owner_admission["operation_id"]
        .as_str()
        .expect("manual Forge admission returns its retained Operation ID")
        .to_owned();
    let admitted_owner =
        retained_operation(&fixture.owner.store, &fixture.operator, &owner_operation_id).await;
    assert_eq!(admitted_owner["state"], "queued");

    let first_pass = reconcile_publication_fixture(fixture).await.unwrap();
    assert_eq!(first_pass["publication"]["processed"], 1);
    let rows = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(rows.len(), 2);
    let wrapper_operation_id = automatic_operation_id(&rows);
    assert_one_effect_operation(&rows, &owner_operation_id);
    let wrapper = rows
        .iter()
        .find(|row| row.operation_id == wrapper_operation_id)
        .unwrap();
    let owner = rows
        .iter()
        .find(|row| row.operation_id == owner_operation_id)
        .unwrap();
    assert_eq!(owner.state, "queued");
    assert!(owner.sent_at_ms.is_none());
    assert!(owner.settled_at_ms.is_none());
    assert!(owner.updated_at_ms >= owner.created_at_ms);
    assert_eq!(wrapper.state, "settled");
    assert!(wrapper.created_at_ms > 0);
    assert!(wrapper.sent_at_ms.is_none());
    assert_eq!(wrapper.settled_at_ms, Some(wrapper.created_at_ms));
    assert_eq!(wrapper.updated_at_ms, wrapper.created_at_ms);
    assert_retained_publication_context(
        &retained_effective_request(&fixture.owner.store, &wrapper_operation_id).await,
        fixture,
    );
    let pending = publication_explanation(fixture).await;
    assert_eq!(
        pending["publication"]["pending"].as_array().unwrap().len(),
        1
    );
    let pending_recent = pending["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(
        pending_recent["publication_status"],
        "publication_in_flight"
    );
    assert_eq!(pending_recent["operation_id"], wrapper_operation_id);
    assert_eq!(pending_recent["operation_state"], "settled");
    assert_eq!(pending_recent["coalesced"], true);
    let wrapper_operation = retained_operation(
        &fixture.owner.store,
        &fixture.operator,
        &wrapper_operation_id,
    )
    .await;
    assert_eq!(wrapper_operation["state"], "settled");
    assert_eq!(
        wrapper_operation["result"],
        json!({
            "operation_id":wrapper_operation_id,
            "outcome":"coalesced",
            "state":"settled",
            "publication":"coalesced",
            "coalesced":true,
            "coalesced_to":owner_operation_id,
            "publication_may_have_started":false,
            "force":false
        })
    );
    let owner =
        retained_operation(&fixture.owner.store, &fixture.operator, &owner_operation_id).await;
    assert_eq!(
        owner["result"],
        json!({
            "operation_id":owner_operation_id,
            "attempt_id":fixture.subject.attempt_id,
            "state":"queued",
            "publication":"not_started",
            "force":false
        })
    );
    assert_eq!(pending_recent["effect_operation_id"], owner_operation_id);
    assert_eq!(pending_recent["effect_operation_state"], owner["state"]);
    (owner_operation_id, wrapper_operation_id)
}

async fn close_fixture(fixture: PublicationFixture) {
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.data_directory).unwrap();
    std::fs::remove_dir_all(fixture.repository_path).unwrap();
}

#[tokio::test]
async fn queued_and_unknown_publications_stay_pending_without_replay() {
    let fixture = accepted_publication_fixture("publication-pending-lifecycle").await;
    let first_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(first_pass["publication"]["processed"], 1);

    let first_rows = retained_forge_operations(&fixture.owner.store).await;
    let operation_id = automatic_operation_id(&first_rows);
    assert_one_effect_operation(&first_rows, &operation_id);
    let admitted = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(admitted["state"], "queued");
    assert_eq!(admitted["result"]["publication"], "not_started");
    assert_retained_publication_context(
        &retained_effective_request(&fixture.owner.store, &operation_id).await,
        &fixture,
    );
    let pending = publication_explanation(&fixture).await;
    assert_eq!(
        pending["publication"]["pending"].as_array().unwrap().len(),
        1
    );
    let (observation_id, source_event_key, observation_operation_id) =
        retained_acceptance_observation(&fixture.owner.store, &fixture.accepted_operation_id).await;
    assert_eq!(
        source_event_key,
        format!("accept:{}", fixture.accepted_operation_id)
    );
    assert_eq!(observation_operation_id, fixture.accepted_operation_id);
    // The scan watermark can include newer non-acceptance Observations. The
    // exact unresolved subject remains owned by the durable bounded queue.
    assert!(pending["publication"]["cursor"].as_i64().unwrap() >= observation_id);
    let retained_state =
        retained_publication_state(&fixture.owner.store, &fixture.accepted_operation_id).await;
    assert_eq!(retained_state["cursor"], pending["publication"]["cursor"]);
    assert_eq!(
        retained_state["pending"][0]["observation_id"],
        observation_id
    );
    assert_eq!(
        retained_state["pending"][0]["accepted_operation_id"],
        fixture.accepted_operation_id
    );

    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let queued_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(queued_pass["publication"]["processed"], 1);
    let queued = publication_explanation(&fixture).await;
    assert_eq!(
        queued["publication"]["pending"].as_array().unwrap().len(),
        1
    );
    let queued_recent = queued["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(queued_recent["publication_status"], "publication_in_flight");
    assert_eq!(queued_recent["operation_id"], operation_id);
    assert_eq!(queued_recent["publication_started"], false);
    let still_queued = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(still_queued.len(), 1);
    assert_one_effect_operation(&still_queued, &operation_id);

    retain_ambiguous_publication_effect(&fixture.owner.store, &operation_id).await;
    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let unknown_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(unknown_pass["publication"]["processed"], 1);
    let unknown = publication_explanation(&fixture).await;
    assert_eq!(
        unknown["publication"]["pending"].as_array().unwrap().len(),
        1
    );
    let unknown_recent = unknown["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(
        unknown_recent["publication_status"],
        "publication_readback_required"
    );
    assert_eq!(unknown_recent["operation_id"], operation_id);
    assert_eq!(unknown_recent["operation_state"], "outcome_unknown");
    assert_eq!(unknown_recent["publication_started"], true);
    assert_eq!(unknown_recent["coalesced"], false);

    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    reconcile_publication_fixture(&fixture).await.unwrap();
    let after_retry = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(after_retry.len(), 1);
    assert_one_effect_operation(&after_retry, &operation_id);
    let retained_unknown =
        retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(retained_unknown["state"], "outcome_unknown");
    assert_eq!(retained_unknown["result"]["outcome"], "unknown");

    close_fixture(fixture).await;
}

#[tokio::test]
async fn automatic_publication_cancel_receipt_is_truthful_not_applied() {
    let fixture = accepted_publication_fixture("publication-cancel-lifecycle").await;
    reconcile_publication_fixture(&fixture).await.unwrap();
    let rows = retained_forge_operations(&fixture.owner.store).await;
    let operation_id = automatic_operation_id(&rows);
    assert_one_effect_operation(&rows, &operation_id);

    let reason = "cancel the admitted publication before dispatch";
    let (cancel_id, cancel_operation, cancelled) = cancel_publication(
        &fixture,
        &operation_id,
        "cancel-publication-lifecycle",
        reason,
    )
    .await;
    assert_eq!(cancel_operation["operation_id"], cancel_id);
    assert_eq!(cancelled["result"]["cancelled_by"], cancel_id);
    assert_ne!(cancelled["result"]["outcome"], "applied");

    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(pass["publication"]["processed"], 1);
    let explanation = publication_explanation(&fixture).await;
    assert_eq!(explanation["publication"]["pending"], json!([]));
    let recent = explanation["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(recent["publication_status"], "publication_cancelled");
    assert_eq!(recent["operation_id"], operation_id);
    assert_eq!(recent["operation_state"], "cancelled");
    assert_eq!(recent["publication_started"], false);
    assert_eq!(recent["coalesced"], false);
    let after = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].state, "cancelled");

    close_fixture(fixture).await;
}

#[tokio::test]
async fn corrupt_cancelled_by_fails_closed_after_actual_store_cancel() {
    let fixture = accepted_publication_fixture("publication-cancel-corruption").await;
    reconcile_publication_fixture(&fixture).await.unwrap();
    let rows = retained_forge_operations(&fixture.owner.store).await;
    let operation_id = automatic_operation_id(&rows);
    assert_one_effect_operation(&rows, &operation_id);

    let reason = "cancel before injecting a corrupt cancellation reference";
    let (cancel_id, _, cancelled) = cancel_publication(
        &fixture,
        &operation_id,
        "cancel-publication-before-corruption",
        reason,
    )
    .await;
    assert_eq!(cancelled["result"]["cancelled_by"], cancel_id);
    assert_eq!(cancelled["result"]["reason"], reason);

    corrupt_cancelled_by(&fixture.owner.store, &operation_id).await;
    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let reconcile = reconcile_publication_fixture(&fixture).await;
    match reconcile {
        Err(error) => assert_eq!(error.code, "AUTOMATION_PUBLICATION_STATE_CORRUPT"),
        Ok(pass) if pass["publication"]["status"] == "degraded" => {
            assert_eq!(
                pass["publication"]["code"],
                "AUTOMATION_PUBLICATION_STATE_CORRUPT"
            );
            assert_eq!(pass["publication"]["cursor_advanced"], false);
        }
        Ok(pass) => {
            assert_eq!(pass["publication"]["quarantined"], 1);
            let explanation = publication_explanation(&fixture).await;
            let recent = explanation["publication"]["recent"]
                .as_array()
                .unwrap()
                .last()
                .unwrap();
            assert_eq!(recent["status"], "quarantined");
        }
    }

    let retained = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(retained["state"], "cancelled");
    assert_ne!(retained["result"]["outcome"], "applied");
    let after = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].state, "cancelled");

    close_fixture(fixture).await;
}

#[tokio::test]
async fn coalesced_wrapper_follows_unknown_owner_without_replay() {
    let fixture = accepted_publication_fixture("publication-coalesced-unknown-owner").await;
    let (owner_operation_id, wrapper_operation_id) =
        admit_coalesced_wrapper(&fixture, "manual-unknown-publication-owner").await;

    retain_ambiguous_publication_effect(&fixture.owner.store, &owner_operation_id).await;
    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(pass["publication"]["processed"], 1);
    let pending = publication_explanation(&fixture).await;
    assert_eq!(
        pending["publication"]["pending"].as_array().unwrap().len(),
        1
    );
    let recent = pending["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(
        recent["publication_status"],
        "publication_readback_required"
    );
    assert_eq!(recent["operation_id"], wrapper_operation_id);
    assert_eq!(recent["operation_state"], "settled");
    assert_eq!(recent["publication_started"], true);
    assert_eq!(recent["coalesced"], true);

    let rows = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(rows.len(), 2);
    assert_one_effect_operation(&rows, &owner_operation_id);
    let unknown_owner =
        retained_operation(&fixture.owner.store, &fixture.operator, &owner_operation_id).await;
    assert_eq!(unknown_owner["state"], "outcome_unknown");
    assert_eq!(
        unknown_owner["result"],
        json!({
            "operation_id":owner_operation_id,
            "outcome":"unknown",
            "publication":"requires_readback",
            "reason":"fixture retains a lost response after a possible Forge effect",
            "publication_may_have_started":true,
            "force":false
        })
    );
    let wrapper = retained_operation(
        &fixture.owner.store,
        &fixture.operator,
        &wrapper_operation_id,
    )
    .await;
    assert_eq!(wrapper["state"], "settled");
    assert_eq!(
        wrapper["result"],
        json!({
            "operation_id":wrapper_operation_id,
            "outcome":"coalesced",
            "state":"settled",
            "publication":"coalesced",
            "coalesced":true,
            "coalesced_to":owner_operation_id,
            "publication_may_have_started":false,
            "force":false
        })
    );
    assert_eq!(recent["effect_operation_id"], owner_operation_id);
    assert_eq!(recent["effect_operation_state"], unknown_owner["state"]);

    close_fixture(fixture).await;
}

#[tokio::test]
async fn settled_coalesced_wrapper_follows_owner_and_closes_after_owner_cancel() {
    let fixture = accepted_publication_fixture("publication-coalesced-owner-lifecycle").await;
    let (owner_operation_id, wrapper_operation_id) =
        admit_coalesced_wrapper(&fixture, "manual-publication-owner-fixture").await;

    let reason = "cancel the exact queued Forge effect owner";
    let (cancel_id, _, cancelled_owner) = cancel_publication(
        &fixture,
        &owner_operation_id,
        "cancel-coalesced-publication-owner",
        reason,
    )
    .await;
    assert_eq!(
        cancelled_owner["result"],
        json!({"reason":reason,"cancelled_by":cancel_id})
    );
    let wrapper_before_reconcile = retained_operation(
        &fixture.owner.store,
        &fixture.operator,
        &wrapper_operation_id,
    )
    .await;
    assert_eq!(
        wrapper_before_reconcile["state"], "settled",
        "the coalesced wrapper is terminal before its owner is cancelled"
    );
    assert_eq!(
        wrapper_before_reconcile["result"],
        json!({
            "operation_id":wrapper_operation_id,
            "outcome":"coalesced",
            "state":"settled",
            "publication":"coalesced",
            "coalesced":true,
            "coalesced_to":owner_operation_id,
            "publication_may_have_started":false,
            "force":false
        })
    );

    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let final_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(final_pass["publication"]["processed"], 1);
    let closed = publication_explanation(&fixture).await;
    assert_eq!(closed["publication"]["pending"], json!([]));
    let recent = closed["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(recent["publication_status"], "publication_cancelled");
    assert_eq!(recent["operation_id"], wrapper_operation_id);
    assert_eq!(recent["operation_state"], "settled");
    assert_eq!(recent["publication_started"], false);
    assert_eq!(recent["coalesced"], true);
    assert_ne!(recent["publication_status"], "publication_applied");

    let after = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(after.len(), 2);
    assert_one_effect_operation(&after, &owner_operation_id);
    let wrapper_after = after
        .iter()
        .find(|row| row.operation_id == wrapper_operation_id)
        .unwrap();
    assert_eq!(wrapper_after.state, "settled");
    assert_eq!(wrapper_after.sent_at_ms, None);
    assert_eq!(
        wrapper_after.settled_at_ms,
        Some(wrapper_after.created_at_ms)
    );
    assert_eq!(wrapper_after.updated_at_ms, wrapper_after.created_at_ms);
    assert_eq!(recent["effect_operation_id"], owner_operation_id);
    assert_eq!(recent["effect_operation_state"], cancelled_owner["state"]);

    close_fixture(fixture).await;
}

/// Retain the exact structured-error shape used for a rejected Store Operation
/// only after the real automatic publication admission has committed. The
/// accepted-candidate automatic producer cannot reach Rejected in this fixture:
/// pre-send Forge failures settle as failed, while admission preparation errors
/// roll back the reservation savepoint. This fixture boundary tests the consumer
/// against a real retained automatic Operation without fabricating application
/// or remote-readback evidence.
async fn retain_rejected_publication_after_admission_fixture_boundary(
    store: &Store,
    operation_id: &str,
) -> Value {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let (state, sent_at_ms, settled_at_ms, effective_json): (
                String,
                Option<i64>,
                Option<i64>,
                String,
            ) = tx.query_row(
                "SELECT state,sent_at_ms,settled_at_ms,effective_request_json \
                 FROM operations WHERE operation_id=?1 AND method='forge.publish_ref'",
                [operation_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )?;
            let effective: Value = serde_json::from_str(&effective_json)?;
            if state != "queued"
                || sent_at_ms.is_some()
                || settled_at_ms.is_some()
                || effective["publication_intent"]["operation_id"] != operation_id
                || !effective["automation_on_behalf"].is_object()
            {
                return Err(crate::error::Error::conflict(
                    "rejection fixture boundary requires the exact newly admitted unsent automatic publication",
                ));
            }

            // Store's retained rejected receipt serializes Error directly:
            // {"code": ..., "message": ...}. The error is a real Forge error
            // shape; this explicit boundary does not claim Forge produced it.
            let receipt = json!(crate::error::Error::new(
                "FORGE_INTENT_INVALID",
                "saved Forge execution pin is invalid",
            ));
            let now_ms = model::now_ms()?;
            let changed = tx.execute(
                "UPDATE operations SET state='rejected',result_json=?2,sent_at_ms=NULL, \
                 settled_at_ms=?3,updated_at_ms=?3 \
                 WHERE operation_id=?1 AND method='forge.publish_ref' AND state='queued' \
                   AND sent_at_ms IS NULL AND settled_at_ms IS NULL",
                params![operation_id, model::canonical(&receipt)?, now_ms],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "rejection fixture boundary could not settle the admitted publication",
                ));
            }
            crate::store::capacity::sync_operation(&tx, &operation_id, now_ms)?;
            tx.commit()?;
            Ok(receipt)
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn rejected_automatic_publication_is_skipped_without_replay_at_consumer_boundary() {
    let fixture = accepted_publication_fixture("publication-rejected-consumer-boundary").await;
    let first_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(first_pass["publication"]["processed"], 1);

    let admitted_rows = retained_forge_operations(&fixture.owner.store).await;
    let operation_id = automatic_operation_id(&admitted_rows);
    assert_one_effect_operation(&admitted_rows, &operation_id);
    let admitted = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(admitted["state"], "queued");
    let immutable_effective_request =
        retained_effective_request(&fixture.owner.store, &operation_id).await;
    assert_retained_publication_context(&immutable_effective_request, &fixture);

    let rejection = retain_rejected_publication_after_admission_fixture_boundary(
        &fixture.owner.store,
        &operation_id,
    )
    .await;
    assert_eq!(
        rejection,
        json!({
            "code":"FORGE_INTENT_INVALID",
            "message":"saved Forge execution pin is invalid"
        })
    );
    let rejected = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(rejected["state"], "rejected");
    assert_eq!(rejected["result"], rejection);
    assert_eq!(
        retained_effective_request(&fixture.owner.store, &operation_id).await,
        immutable_effective_request
    );

    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(pass["publication"]["processed"], 1);

    let explanation = publication_explanation(&fixture).await;
    assert_eq!(explanation["publication"]["pending"], json!([]));
    let recent = explanation["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(recent["status"], "skipped");
    assert_eq!(recent["publication_status"], "publication_rejected");
    assert_eq!(recent["code"], "publication_rejected");
    assert_eq!(recent["operation_id"], operation_id);
    assert_eq!(recent["operation_state"], "rejected");
    assert_eq!(recent["effect_operation_id"], operation_id);
    assert_eq!(recent["effect_operation_state"], "rejected");
    assert_eq!(
        recent["automation_revision"],
        explanation["entry"]["revision"]
    );
    assert_eq!(recent["publication_started"], false);
    assert_eq!(recent["coalesced"], false);
    let (observation_id, _, _) =
        retained_acceptance_observation(&fixture.owner.store, &fixture.accepted_operation_id).await;
    assert!(explanation["publication"]["cursor"].as_i64().unwrap() >= observation_id);

    let repeated_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(repeated_pass["publication"]["processed"], 0);
    let after = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(after.len(), 1);
    assert_one_effect_operation(&after, &operation_id);
    assert_eq!(after[0].state, "rejected");
    assert!(after[0].sent_at_ms.is_none());
    assert!(after[0].settled_at_ms.is_some());
    assert_eq!(
        retained_effective_request(&fixture.owner.store, &operation_id).await,
        immutable_effective_request
    );

    close_fixture(fixture).await;
}

#[tokio::test]
async fn unknown_effect_handover_fences_former_gm_and_continues_exact_owner() {
    // Bound: stale_gm_publication_is_skipped_when_consumer_resumes proves the
    // never-sent queued case settles stale_gm_epoch with no write, and
    // queued_and_unknown_publications_stay_pending_without_replay proves
    // queued/unknown fencing without rotation. Neither rotates GM under an
    // already-sent ambiguous effect, so neither proves the former GM fenced
    // nor the successor continuing the exact retained owner. This fixture
    // proves exactly that combination through public Store calls only.
    let fixture = accepted_publication_fixture("publication-unknown-handover").await;
    let first_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(first_pass["publication"]["processed"], 1);

    let admitted_rows = retained_forge_operations(&fixture.owner.store).await;
    let operation_id = automatic_operation_id(&admitted_rows);
    assert_one_effect_operation(&admitted_rows, &operation_id);
    let admitted = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(admitted["state"], "queued");
    let immutable_effective_request =
        retained_effective_request(&fixture.owner.store, &operation_id).await;
    let admitted_epoch = immutable_effective_request["publication_intent"]["admitted_gm_epoch"]
        .as_i64()
        .expect("the retained immutable intent records the admitting GM epoch");

    // Drive the admitted effect to already-sent ambiguity before rotation: the
    // native effect may have crossed the write boundary, so unlike the
    // never-sent queued case it must never settle stale_gm_epoch.
    retain_ambiguous_publication_effect(&fixture.owner.store, &operation_id).await;
    let ambiguous = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(ambiguous["state"], "outcome_unknown");
    let ambiguous_sent_at = ambiguous["sent_at_ms"].clone();
    assert!(ambiguous_sent_at.is_number());
    let ambiguous_result = ambiguous["result"].clone();

    // Rotate GM while the ambiguous effect is outstanding.
    let handover = fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "gm.handover".into(),
            json!({
                "client_request_id":format!("handover-unknown-effect-{}", model::new_id()),
                "client_id":"review-owner-v2"
            }),
        )
        .await
        .unwrap();
    assert!(handover["gm_epoch"].as_i64().unwrap() > admitted_epoch);
    let successor = principal("review-owner-v2", Role::Manager);

    // The former GM cannot replay the identical effect: admission fails at the
    // current-GM authority gate before any slot logic runs, so no second
    // effect Operation exists and the retained owner is untouched.
    let former_replay = fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "forge.publish_ref".into(),
            manual_publication_request(
                &fixture,
                &format!("replay-unknown-effect-{}", model::new_id()),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(former_replay.code, "FORBIDDEN");
    // A changed effect from the former GM is fenced at the same gate.
    let mut changed =
        manual_publication_request(&fixture, &format!("changed-unknown-effect-{}", model::new_id()));
    changed["candidate_ref"] = json!("changed-candidate-ref");
    let former_change = fixture
        .owner
        .store
        .call(fixture.manager.clone(), "forge.publish_ref".into(), changed)
        .await
        .unwrap_err();
    assert_eq!(former_change.code, "FORBIDDEN");
    let fenced_rows = retained_forge_operations(&fixture.owner.store).await;
    assert_one_effect_operation(&fenced_rows, &operation_id);

    // The successor continues the exact retained owner: the identical
    // admission coalesces to the outstanding ambiguous effect instead of
    // dispatching new native work.
    let continued = fixture
        .owner
        .store
        .call(
            successor.clone(),
            "forge.publish_ref".into(),
            manual_publication_request(
                &fixture,
                &format!("continue-unknown-effect-{}", model::new_id()),
            ),
        )
        .await
        .unwrap();
    assert_eq!(continued["coalesced"], true);
    assert_eq!(continued["coalesced_to"], operation_id);
    assert_eq!(continued["publication_may_have_started"], false);
    let wrapper_id = continued["operation_id"].as_str().unwrap().to_owned();
    assert_ne!(wrapper_id, operation_id);
    let continued_rows = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(continued_rows.len(), 2);
    assert_one_effect_operation(&continued_rows, &operation_id);
    let wrapper = retained_operation(&fixture.owner.store, &fixture.operator, &wrapper_id).await;
    assert_eq!(wrapper["state"], "settled");
    // The outstanding ambiguous owner is byte-identical: no replay, no change.
    let continued_owner =
        retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(continued_owner["state"], "outcome_unknown");
    assert_eq!(continued_owner["sent_at_ms"], ambiguous_sent_at);
    assert_eq!(continued_owner["result"], ambiguous_result);
    assert_eq!(
        retained_effective_request(&fixture.owner.store, &operation_id).await,
        immutable_effective_request
    );

    // Consumer resume performs readback-only continuation: the pending
    // acceptance still names the exact owner, and a repeat pass adds nothing.
    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let resumed = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(resumed["publication"]["processed"], 1);
    let explanation = fixture
        .owner
        .store
        .call(
            successor.clone(),
            "automation.config.explain".into(),
            json!({
                "owner_manager_id":"review-gm",
                "project_id":PROJECT_ID,
                "automation_id":AUTOMATION_ID
            }),
        )
        .await
        .unwrap();
    assert_eq!(explanation["publication"]["pending"].as_array().unwrap().len(), 1);
    let recent = explanation["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(recent["publication_status"], "publication_readback_required");
    assert_eq!(recent["operation_id"], operation_id);
    assert_eq!(recent["operation_state"], "outcome_unknown");
    assert_eq!(recent["publication_started"], true);
    assert_eq!(recent["coalesced"], false);
    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    reconcile_publication_fixture(&fixture).await.unwrap();
    let after = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(after.len(), 2);
    assert_one_effect_operation(&after, &operation_id);
    let replayed_owner =
        retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(replayed_owner["state"], "outcome_unknown");
    assert_eq!(replayed_owner["sent_at_ms"], ambiguous_sent_at);
    assert_eq!(replayed_owner["result"], ambiguous_result);

    close_fixture(fixture).await;
}

#[tokio::test]
async fn stale_gm_publication_is_skipped_when_consumer_resumes() {
    let fixture = accepted_publication_fixture("publication-stale-gm-consumer-resume").await;
    let first_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(first_pass["publication"]["processed"], 1);

    let admitted_rows = retained_forge_operations(&fixture.owner.store).await;
    let operation_id = automatic_operation_id(&admitted_rows);
    assert_one_effect_operation(&admitted_rows, &operation_id);
    let admitted = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(admitted["state"], "queued");
    let immutable_effective_request =
        retained_effective_request(&fixture.owner.store, &operation_id).await;
    assert_retained_publication_context(&immutable_effective_request, &fixture);
    let admitted_epoch = immutable_effective_request["publication_intent"]["admitted_gm_epoch"]
        .as_i64()
        .expect("the retained immutable intent records the admitting GM epoch");

    // Reuse the real producer path from the connected handover fixture. The
    // supervisor sees the changed epoch and retains its structured no-write
    // receipt before any native Git publication can start.
    let handover = fixture
        .owner
        .store
        .call(
            fixture.manager.clone(),
            "gm.handover".into(),
            json!({
                "client_request_id":"handover-before-publication-consumer-resume",
                "client_id":"review-owner-v2"
            }),
        )
        .await
        .unwrap();
    assert!(handover["gm_epoch"].as_i64().unwrap() > admitted_epoch);
    fixture.owner.store.supervise_forge_once().await.unwrap();

    let stale = retained_operation(&fixture.owner.store, &fixture.operator, &operation_id).await;
    assert_eq!(stale["state"], "settled");
    assert_eq!(stale["result"]["operation_id"], operation_id);
    assert_eq!(stale["result"]["outcome"], "stale_gm_epoch");
    assert_eq!(stale["result"]["publication"], "not_started");
    assert_eq!(stale["result"]["admitted_gm_epoch"], admitted_epoch);
    assert_eq!(stale["result"]["current_gm_epoch"], handover["gm_epoch"]);
    assert_eq!(stale["result"]["force"], false);
    assert_ne!(stale["result"]["publication_may_have_started"], true);
    assert_eq!(
        retained_effective_request(&fixture.owner.store, &operation_id).await,
        immutable_effective_request
    );

    make_pending_publication_due(&fixture.owner.store, &fixture.accepted_operation_id).await;
    let resumed_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(resumed_pass["publication"]["processed"], 1);

    // After handover, only the current GM may inspect the former owner's
    // publication state; owner_manager_id keeps this exact entry in scope.
    let explanation = fixture
        .owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "automation.config.explain".into(),
            json!({
                "owner_manager_id":"review-gm",
                "project_id":PROJECT_ID,
                "automation_id":AUTOMATION_ID
            }),
        )
        .await
        .unwrap();
    assert_eq!(explanation["publication"]["pending"], json!([]));
    let recent = explanation["publication"]["recent"]
        .as_array()
        .unwrap()
        .last()
        .unwrap();
    assert_eq!(recent["status"], "skipped");
    assert_eq!(recent["publication_status"], "publication_not_applied");
    assert_eq!(recent["code"], "publication_not_applied");
    assert_eq!(recent["operation_id"], operation_id);
    assert_eq!(recent["operation_state"], "settled");
    assert_eq!(recent["effect_operation_id"], operation_id);
    assert_eq!(recent["effect_operation_state"], "settled");
    assert_eq!(
        recent["automation_revision"],
        explanation["entry"]["revision"]
    );
    assert_eq!(recent["publication_started"], false);
    assert_eq!(recent["coalesced"], false);
    let (observation_id, _, _) =
        retained_acceptance_observation(&fixture.owner.store, &fixture.accepted_operation_id).await;
    assert!(explanation["publication"]["cursor"].as_i64().unwrap() >= observation_id);

    let repeated_pass = reconcile_publication_fixture(&fixture).await.unwrap();
    assert_eq!(repeated_pass["publication"]["processed"], 0);
    let after = retained_forge_operations(&fixture.owner.store).await;
    assert_eq!(after.len(), 1);
    assert_one_effect_operation(&after, &operation_id);
    assert_eq!(after[0].state, "settled");
    assert_eq!(after[0].sent_at_ms, None);
    assert_eq!(
        retained_effective_request(&fixture.owner.store, &operation_id).await,
        immutable_effective_request
    );

    close_fixture(fixture).await;
}
