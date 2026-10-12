//! Public Store consumers for retained Concilium authority and replay boundaries.

use super::*;
use rusqlite::params;
use serde_json::{Value, json};

const CONCILIUM_PROJECTION_PREFIX: &str = "concilium:v1:projection:";
const CONCILIUM_OPERATION_PREFIX: &str = "concilium:v1:operation:";

async fn start_concilium_fixture(
    label: &str,
    participant_ids: &[&str],
) -> (
    StoreOwner,
    PathBuf,
    ReviewSubject,
    Principal,
    Vec<Principal>,
) {
    let (owner, directory, _) = start_store(label).await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        label,
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let manager = principal("review-owner-v2", Role::Manager);
    let mut participants = Vec::with_capacity(participant_ids.len());
    for client_id in participant_ids {
        let token = format!("{client_id}-concilium-fixture-token");
        let participant = if participants.is_empty() {
            assign_sponsored_reviewer(&owner.store, manager.clone(), &subject, client_id, &token)
                .await
                .0
        } else {
            owner
                .store
                .call(
                    manager.clone(),
                    "coordination.participant.register".into(),
                    json!({
                        "client_request_id":format!("register-{client_id}"),
                        "client_id":client_id,
                        "token_hash":model::digest(token.as_bytes()),
                        "task_id":subject.task_id,
                        "task_revision":1,
                        "attempt_id":subject.attempt_id,
                        "participation_basis":{"kind":"attempt_owner"},
                    }),
                )
                .await
                .unwrap();
            owner
                .store
                .authenticate(Credential {
                    client_id: (*client_id).to_owned(),
                    token,
                })
                .await
                .unwrap()
        };
        participants.push(participant);
    }
    (owner, directory, subject, manager, participants)
}

fn proposal_request(
    subject: &ReviewSubject,
    participant_ids: &[&str],
    request_id: &str,
    question: &str,
    evidence_refs: Value,
) -> Value {
    let participants = participant_ids
        .iter()
        .map(|client_id| json!({"client_id":client_id,"reason":"independent bounded review"}))
        .collect::<Vec<_>>();
    json!({
        "client_request_id":request_id,
        "task_id":subject.task_id,
        "attempt_id":subject.attempt_id,
        "failed_thread_id":format!("thread-{request_id}"),
        "decision_question":question,
        "material_conflict":"The producer and consumer disagree on one retained authority boundary.",
        "participants":participants,
        "proposal_revision_ids":[],
        "evidence_refs":evidence_refs,
        "expected_output":"A bounded recommendation with explicit unresolved questions.",
        "suggested_max_rounds":1,
        "suggested_budget":{"model_calls":0,"tokens":0},
        "close_condition":"All invited positions are retained.",
    })
}

async fn propose(
    store: &Store,
    proposer: Principal,
    subject: &ReviewSubject,
    participant_ids: &[&str],
    request_id: &str,
    question: &str,
    evidence_refs: Value,
) -> Value {
    store
        .call(
            proposer,
            "concilium.propose".into(),
            proposal_request(
                subject,
                participant_ids,
                request_id,
                question,
                evidence_refs,
            ),
        )
        .await
        .unwrap()
}

async fn preview_and_open(
    store: &Store,
    manager: Principal,
    proposal: &Value,
    request_id: &str,
) -> Value {
    let proposal_operation_id = proposal["operation_id"]
        .as_str()
        .expect("proposal receipt identifies its originating Operation")
        .to_owned();
    let preview = store
        .call(
            manager.clone(),
            "concilium.preview".into(),
            json!({"proposal_operation_id":proposal_operation_id}),
        )
        .await
        .unwrap();
    store
        .call(
            manager,
            "concilium.open".into(),
            json!({
                "client_request_id":request_id,
                "proposal_operation_id":proposal_operation_id,
                "plan_digest":preview["plan_digest"],
                "confirmed_reasonable":true,
                "manager_reason":"The exact public preview was reviewed.",
            }),
        )
        .await
        .unwrap()
}

async fn manager_projection(store: &Store, manager: Principal, concilium_id: &str) -> Value {
    store
        .call(
            manager,
            "concilium.get".into(),
            json!({"concilium_id":concilium_id,"limit":50}),
        )
        .await
        .unwrap()
}

fn position_request(
    client_request_id: &str,
    concilium_id: &str,
    slot: &Value,
    required_change: &str,
) -> Value {
    json!({
        "client_request_id":client_request_id,
        "concilium_id":concilium_id,
        "slot_id":slot["slot_id"],
        "packet_digest":slot["packet_digest"],
        "position":{
            "position":"support",
            "proposal_revision_id":null,
            "claims":[],
            "required_change":required_change,
            "unresolved_questions":[],
        },
    })
}

fn participant_slot<'a>(projection: &'a Value, client_id: &str) -> &'a Value {
    projection["slots"]
        .as_array()
        .expect("Concilium projection has bounded slots")
        .iter()
        .find(|slot| slot["participant_actor"]["client_id"].as_str() == Some(client_id))
        .expect("invited participant has one exact slot")
}

fn scope_index_key(task_id: &str, attempt_id: &str, concilium_id: &str) -> String {
    let scope = model::canonical(&json!({"task_id":task_id,"attempt_id":attempt_id})).unwrap();
    format!(
        "concilium:v1:index:scope:{}:{concilium_id}",
        model::digest(scope.as_bytes())
    )
}

#[derive(Debug, PartialEq, Eq)]
struct PositionLedgerSnapshot {
    operations: i64,
    observations: i64,
    operation_links: i64,
    projection_json: String,
}

async fn position_ledger_snapshot(
    store: &Store,
    caller_id: &str,
    concilium_id: &str,
) -> PositionLedgerSnapshot {
    let caller_id = caller_id.to_owned();
    let projection_key = format!("{CONCILIUM_PROJECTION_PREFIX}{concilium_id}");
    store
        .run(move |db| {
            let operations = db.query_row(
                "SELECT count(*) FROM operations WHERE caller_id=?1 AND method='concilium.position.submit'",
                [&caller_id],
                |row| row.get(0),
            )?;
            let observations = db.query_row(
                "SELECT count(*) FROM observations WHERE operation_id IN \
                 (SELECT operation_id FROM operations WHERE caller_id=?1 AND method='concilium.position.submit')",
                [&caller_id],
                |row| row.get(0),
            )?;
            let operation_links = db.query_row(
                "SELECT count(*) FROM meta WHERE key IN \
                 (SELECT 'concilium:v1:operation:' || operation_id FROM operations \
                  WHERE caller_id=?1 AND method='concilium.position.submit')",
                [&caller_id],
                |row| row.get(0),
            )?;
            let projection_json = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [&projection_key],
                |row| row.get(0),
            )?;
            Ok(PositionLedgerSnapshot {
                operations,
                observations,
                operation_links,
                projection_json,
            })
        })
        .await
        .unwrap()
}

async fn rebind_participant_to_subject(store: &Store, client_id: &str, subject: &ReviewSubject) {
    let client_key = format!("client:{client_id}");
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT value_json FROM meta WHERE key=?1",
                [&client_key],
                |row| row.get(0),
            )?;
            let mut registration: Value = serde_json::from_str(&raw)?;
            registration["task_id"] = json!(task_id);
            registration["task_revision"] = json!(1);
            registration["attempt_id"] = json!(attempt_id);
            // The destination Attempt has the same manager as its creator, so
            // this keeps the synthetic reassignment a valid live participant scope.
            registration["participation_basis"] = json!({
                "kind":"attempt_owner",
                "assignment_id":null,
                "review_scope":null,
            });
            db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![client_key, model::canonical(&registration)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn proposal_operation_bytes(store: &Store, operation_id: &str) -> (String, String, String) {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT original_request_json,effective_request_json,result_json \
                 FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?)
        })
        .await
        .unwrap()
}

#[derive(Clone, Copy)]
enum ProjectionDamage {
    RehashedCanonicalRequestField,
    Scope,
    ProposalOperationId,
    RecordedManagerReadGrant,
}

async fn damage_projection(store: &Store, concilium_id: &str, damage: ProjectionDamage) -> String {
    let key = format!("{CONCILIUM_PROJECTION_PREFIX}{concilium_id}");
    store
        .run(move |db| {
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            let mut record: Value = serde_json::from_str(&raw)?;
            match damage {
                ProjectionDamage::RehashedCanonicalRequestField => {
                    record["proposal"]["decision_question"] =
                        json!("rewritten but self-consistently rehashed question");
                    record["proposal"]["request_source"]["request"]["decision_question"] =
                        record["proposal"]["decision_question"].clone();
                    let source_bytes = model::canonical(&record["proposal"]["request_source"])?;
                    record["proposal"]["digest"] =
                        json!(format!("sha256:{}", model::digest(source_bytes.as_bytes())));
                }
                ProjectionDamage::Scope => {
                    let changed_attempt = json!("attempt-valid-but-divergent");
                    record["scope"]["attempt_id"] = changed_attempt.clone();
                    record["proposal"]["proposer_scope"]["attempt_id"] = changed_attempt.clone();
                    record["proposal"]["request_source"]["proposer"]["scope"]["attempt_id"] =
                        changed_attempt;
                    let source_bytes = model::canonical(&record["proposal"]["request_source"])?;
                    record["proposal"]["digest"] =
                        json!(format!("sha256:{}", model::digest(source_bytes.as_bytes())));
                }
                ProjectionDamage::ProposalOperationId => {
                    let forged_id = json!("operation-valid-but-unlinked");
                    record["proposal_operation_id"] = forged_id.clone();
                    record["proposal"]["operation_id"] = forged_id;
                }
                ProjectionDamage::RecordedManagerReadGrant => {
                    record["manager_id"] = json!("unrelated-manager");
                }
            }
            db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, model::canonical(&record)?],
            )?;
            Ok(raw)
        })
        .await
        .unwrap()
}

async fn restore_projection(store: &Store, concilium_id: &str, raw: String) {
    let key = format!("{CONCILIUM_PROJECTION_PREFIX}{concilium_id}");
    store
        .run(move |db| {
            db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, raw],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn assert_projection_digest_matches_source(store: &Store, concilium_id: &str) {
    let key = format!("{CONCILIUM_PROJECTION_PREFIX}{concilium_id}");
    let raw = store
        .run(move |db| {
            Ok(
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get::<_, String>(0)
                })?,
            )
        })
        .await
        .unwrap();
    let record: Value = serde_json::from_str(&raw).unwrap();
    let source_bytes = model::canonical(&record["proposal"]["request_source"]).unwrap();
    let expected_digest = format!("sha256:{}", model::digest(source_bytes.as_bytes()));
    assert_eq!(record["proposal"]["digest"], expected_digest);
}

async fn assert_direct_get_and_preview_reject(
    store: &Store,
    reader: Principal,
    concilium_id: &str,
    proposal_operation_id: &str,
) {
    let direct_get = store
        .call(
            reader.clone(),
            "concilium.get".into(),
            json!({"concilium_id":concilium_id,"limit":50}),
        )
        .await
        .unwrap_err();
    assert_eq!(direct_get.code, "CONCILIUM_CORRUPT");
    let preview = store
        .call(
            reader,
            "concilium.preview".into(),
            json!({"proposal_operation_id":proposal_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(preview.code, "CONCILIUM_CORRUPT");
}

#[tokio::test]
async fn store_concilium_list_damaged_index_advances_cursor_and_reports_gap() {
    let (owner, directory, subject, _, participants) =
        start_concilium_fixture("concilium-list-damaged-index", &["concilium-list-reader"]).await;
    let participant_ids = [participants[0].client_id.as_str()];
    let first = propose(
        &owner.store,
        participants[0].clone(),
        &subject,
        &participant_ids,
        "concilium-list-first-request",
        "List cursor fixture first question",
        json!([]),
    )
    .await;
    let second = propose(
        &owner.store,
        participants[0].clone(),
        &subject,
        &participant_ids,
        "concilium-list-second-request",
        "List cursor fixture second question",
        json!([]),
    )
    .await;
    let first_id = first["concilium_id"].as_str().unwrap().to_owned();
    let second_id = second["concilium_id"].as_str().unwrap().to_owned();
    let damaged_id = first_id.min(second_id.clone());
    let healthy_id = if damaged_id == second_id {
        first["concilium_id"].as_str().unwrap().to_owned()
    } else {
        second_id
    };
    let index_key = scope_index_key(&subject.task_id, &subject.attempt_id, &damaged_id);
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![
                    index_key,
                    model::canonical(&json!({"concilium_id":"different-id"}))?
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let reader = principal("review-gm", Role::Manager);
    let first_page = owner
        .store
        .call(
            reader.clone(),
            "concilium.list".into(),
            json!({"task_id":subject.task_id,"attempt_id":subject.attempt_id,"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(first_page["items"], json!([]));
    assert_eq!(first_page["next_after_concilium_id"], damaged_id);
    assert_eq!(first_page["coverage"], "partial");
    assert_eq!(
        first_page["gaps"],
        json!([{"kind":"concilium_record","count":1}])
    );

    let resumed = owner
        .store
        .call(
            reader,
            "concilium.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "limit":1,
                "after_concilium_id":damaged_id,
            }),
        )
        .await
        .unwrap();
    assert_eq!(resumed["items"].as_array().unwrap().len(), 1);
    assert_eq!(resumed["items"][0]["concilium_id"], healthy_id);
    assert_eq!(resumed["next_after_concilium_id"], Value::Null);
    assert_eq!(resumed["coverage"], "complete");
    assert_eq!(resumed["gaps"], json!([]));

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_concilium_manager_read_propagates_missing_after_history_and_gm_decoder_damage() {
    let (owner, directory, subject, _, participants) = start_concilium_fixture(
        "concilium-manager-read-gm-damage",
        &["concilium-history-author"],
    )
    .await;
    let participant_ids = [participants[0].client_id.as_str()];
    let proposal = propose(
        &owner.store,
        participants[0].clone(),
        &subject,
        &participant_ids,
        "concilium-manager-read-history-request",
        "Manager read history propagation question",
        json!([]),
    )
    .await;
    let list_request = json!({
        "task_id":subject.task_id,
        "attempt_id":subject.attempt_id,
        "limit":8,
    });
    let prior_history = owner
        .store
        .call(
            principal("review-gm", Role::Manager),
            "concilium.list".into(),
            list_request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(prior_history["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        prior_history["items"][0]["proposal_operation_id"],
        proposal["operation_id"]
    );

    let handover = owner
        .store
        .call(
            principal("review-gm", Role::Manager),
            "gm.handover".into(),
            json!({
                "client_request_id":"concilium-history-gm-handover",
                "client_id":"unrelated-manager",
            }),
        )
        .await
        .unwrap();
    assert!(handover["gm_epoch"].as_i64().unwrap() > 1);

    owner
        .store
        .run(|db| {
            db.execute("DELETE FROM meta WHERE key='gm'", [])?;
            Ok(())
        })
        .await
        .unwrap();
    let missing = owner
        .store
        .call(
            principal("review-gm", Role::Manager),
            "concilium.list".into(),
            list_request.clone(),
        )
        .await
        .unwrap_err();
    assert_eq!(missing.code, "GM_DESIGNATION_MISSING");

    owner
        .store
        .run(|db| {
            let damaged_gm = model::canonical(&json!({
                "client_id":"unrelated-manager",
                "epoch":"not-an-integer",
            }))?;
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES('gm',?1) \
                 ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                [damaged_gm],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let damaged = owner
        .store
        .call(
            principal("review-gm", Role::Manager),
            "concilium.list".into(),
            list_request,
        )
        .await
        .unwrap_err();
    assert_eq!(damaged.code, "GM_DESIGNATION_DAMAGED");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_concilium_sealed_round_same_request_reuses_operation_without_new_operation_link_or_observation()
 {
    let (owner, directory, subject, manager, participants) = start_concilium_fixture(
        "concilium-sealed-exact-replay",
        &["concilium-replay-author"],
    )
    .await;
    let participant = participants[0].clone();
    let participant_ids = [participant.client_id.as_str()];
    let proposal = propose(
        &owner.store,
        participant.clone(),
        &subject,
        &participant_ids,
        "concilium-exact-replay-proposal-request",
        "Sealed round exact replay question",
        json!([]),
    )
    .await;
    let opened = preview_and_open(
        &owner.store,
        manager.clone(),
        &proposal,
        "concilium-exact-replay-open-request",
    )
    .await;
    let concilium_id = opened["concilium_id"].as_str().unwrap().to_owned();
    let projection = manager_projection(&owner.store, manager, &concilium_id).await;
    let slot = participant_slot(&projection, &participant.client_id).clone();
    let request = position_request(
        "concilium-exact-position-request",
        &concilium_id,
        &slot,
        "Record the exact participant position.",
    );
    let caller_id = participant.client_id.clone();
    let first = owner
        .store
        .call(
            participant.clone(),
            "concilium.position.submit".into(),
            request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(first["status"], "round_1_ready");
    let before_replay =
        position_ledger_snapshot(&owner.store, &participant.client_id, &concilium_id).await;

    let replay = owner
        .store
        .call(participant, "concilium.position.submit".into(), request)
        .await
        .unwrap();
    let after_replay = position_ledger_snapshot(&owner.store, &caller_id, &concilium_id).await;
    assert_eq!(replay, first);
    assert_eq!(after_replay, before_replay);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_concilium_distinct_request_id_after_sealed_round_rejects_without_projection_write_or_concilium_link()
 {
    let (owner, directory, subject, manager, participants) =
        start_concilium_fixture("concilium-sealed-fresh-id", &["concilium-fresh-id-author"]).await;
    let participant = participants[0].clone();
    let participant_ids = [participant.client_id.as_str()];
    let proposal = propose(
        &owner.store,
        participant.clone(),
        &subject,
        &participant_ids,
        "concilium-fresh-id-proposal-request",
        "Sealed round fresh request question",
        json!([]),
    )
    .await;
    let opened = preview_and_open(
        &owner.store,
        manager.clone(),
        &proposal,
        "concilium-fresh-id-open-request",
    )
    .await;
    let concilium_id = opened["concilium_id"].as_str().unwrap().to_owned();
    let projection = manager_projection(&owner.store, manager, &concilium_id).await;
    let slot = participant_slot(&projection, &participant.client_id).clone();
    let first = owner
        .store
        .call(
            participant.clone(),
            "concilium.position.submit".into(),
            position_request(
                "concilium-fresh-id-first-position",
                &concilium_id,
                &slot,
                "Record one sealed position.",
            ),
        )
        .await
        .unwrap();
    assert_eq!(first["status"], "round_1_ready");
    let before =
        position_ledger_snapshot(&owner.store, &participant.client_id, &concilium_id).await;

    let rejected = owner
        .store
        .call(
            participant.clone(),
            "concilium.position.submit".into(),
            position_request(
                "concilium-fresh-id-distinct-position",
                &concilium_id,
                &slot,
                "Record one sealed position.",
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "CONFLICT");
    let after = position_ledger_snapshot(&owner.store, &participant.client_id, &concilium_id).await;
    assert_eq!(after.operations, before.operations + 1);
    assert_eq!(after.observations, before.observations + 1);
    assert_eq!(after.operation_links, before.operation_links);
    assert_eq!(after.projection_json, before.projection_json);
    let request_id = "concilium-fresh-id-distinct-position".to_owned();
    let caller_id = participant.client_id.clone();
    let (rejected_operation_id, rejected_state, has_concilium_link) = owner
        .store
        .run(move |db| {
            let (operation_id, state) = db.query_row(
                "SELECT operation_id,state FROM operations \
                 WHERE caller_id=?1 AND client_request_id=?2 AND method='concilium.position.submit'",
                params![caller_id, request_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )?;
            let link_key = format!("{CONCILIUM_OPERATION_PREFIX}{operation_id}");
            let has_link: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM meta WHERE key=?1)",
                [link_key],
                |row| row.get(0),
            )?;
            Ok((operation_id, state, has_link))
        })
        .await
        .unwrap();
    assert_eq!(rejected_state, "rejected");
    assert!(
        !has_concilium_link,
        "the rejected Operation has no Concilium link"
    );
    assert_ne!(rejected_operation_id, first["operation_id"]);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_operation_get_after_participant_rebind_returns_only_own_bounded_receipt() {
    let (owner, directory, subject, _, participants) = start_concilium_fixture(
        "concilium-operation-after-rebind",
        &["concilium-operation-rebound-reader"],
    )
    .await;
    let participant = participants[0].clone();
    let participant_ids = [participant.client_id.as_str()];
    let proposal = propose(
        &owner.store,
        participant.clone(),
        &subject,
        &participant_ids,
        "concilium-operation-rebind-proposal-request",
        "Operation receipt after rebind question",
        json!([]),
    )
    .await;
    let operation_id = proposal["operation_id"].as_str().unwrap().to_owned();
    let other_subject = seed_subject(
        &owner.store,
        "concilium-operation-rebind-destination",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    rebind_participant_to_subject(&owner.store, &participant.client_id, &other_subject).await;

    let receipt = owner
        .store
        .call(
            participant,
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(receipt["operation_id"], operation_id);
    assert_eq!(receipt["method"], "concilium.propose");
    assert_eq!(receipt["state"], "settled");
    assert_eq!(receipt["result_status"], "not_projected");
    assert!(receipt.get("result").is_none());
    assert_eq!(
        receipt["client_request_id"],
        "concilium-operation-rebind-proposal-request"
    );
    assert!(receipt.get("diagnostic").is_none());
    assert!(receipt.get("original_request_json").is_none());
    assert!(receipt.get("effective_request_json").is_none());
    assert!(receipt.get("caller_id").is_none());

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_concilium_get_after_rebind_does_not_expose_foreign_projection() {
    let (owner, directory, subject, manager, participants) = start_concilium_fixture(
        "concilium-get-after-rebind",
        &["concilium-rebound-reader", "concilium-foreign-reader"],
    )
    .await;
    let reader = participants[0].clone();
    let peer = participants[1].clone();
    let participant_ids = [reader.client_id.as_str(), peer.client_id.as_str()];
    let proposal = propose(
        &owner.store,
        reader.clone(),
        &subject,
        &participant_ids,
        "concilium-rebind-projection-proposal-request",
        "Retained participant redaction after rebind question",
        json!([]),
    )
    .await;
    let opened = preview_and_open(
        &owner.store,
        manager.clone(),
        &proposal,
        "concilium-rebind-projection-open-request",
    )
    .await;
    let concilium_id = opened["concilium_id"].as_str().unwrap().to_owned();
    let manager_view = manager_projection(&owner.store, manager, &concilium_id).await;
    let reader_slot = participant_slot(&manager_view, &reader.client_id).clone();
    let peer_slot = participant_slot(&manager_view, &peer.client_id).clone();
    owner
        .store
        .call(
            reader.clone(),
            "concilium.position.submit".into(),
            position_request(
                "concilium-rebind-reader-position",
                &concilium_id,
                &reader_slot,
                "Reader's retained position.",
            ),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            peer,
            "concilium.position.submit".into(),
            position_request(
                "concilium-rebind-peer-position",
                &concilium_id,
                &peer_slot,
                "Peer's private retained position.",
            ),
        )
        .await
        .unwrap();

    let other_subject = seed_subject(
        &owner.store,
        "concilium-get-rebind-destination",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    rebind_participant_to_subject(&owner.store, &reader.client_id, &other_subject).await;
    let retained = owner
        .store
        .call(
            reader.clone(),
            "concilium.get".into(),
            json!({"concilium_id":concilium_id,"limit":50}),
        )
        .await
        .unwrap();
    assert_eq!(retained["latest_positions"].as_array().unwrap().len(), 1);
    assert_eq!(
        retained["my_slots"].as_array().unwrap().len(),
        1,
        "the retained actor still sees only its own slot after reassignment"
    );
    let own_slot = participant_slot(&retained, &reader.client_id);
    assert_eq!(
        own_slot["position"]["required_change"],
        "Reader's retained position."
    );
    let hidden_peer_slot = participant_slot(&retained, "concilium-foreign-reader");
    for private_field in [
        "position",
        "packet",
        "source_result_ref",
        "participant_scope",
        "participation_basis",
        "registration_fingerprint",
    ] {
        assert!(
            hidden_peer_slot.get(private_field).is_none(),
            "foreign slot field {private_field} is omitted from the participant projection"
        );
    }
    assert!(retained["proposal"].get("request_source").is_none());
    assert!(
        retained["proposal"]["participants"][1]
            .get("scope")
            .is_none()
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_position_submit_after_rebind_remains_denied_by_exact_live_slot_gate() {
    let (owner, directory, subject, manager, participants) = start_concilium_fixture(
        "concilium-position-after-rebind",
        &["concilium-position-rebound-writer"],
    )
    .await;
    let participant = participants[0].clone();
    let participant_ids = [participant.client_id.as_str()];
    let proposal = propose(
        &owner.store,
        participant.clone(),
        &subject,
        &participant_ids,
        "concilium-position-rebind-proposal-request",
        "Position write remains exact after rebind question",
        json!([]),
    )
    .await;
    let opened = preview_and_open(
        &owner.store,
        manager.clone(),
        &proposal,
        "concilium-position-rebind-open-request",
    )
    .await;
    let concilium_id = opened["concilium_id"].as_str().unwrap().to_owned();
    let projection = manager_projection(&owner.store, manager, &concilium_id).await;
    let slot = participant_slot(&projection, &participant.client_id).clone();
    let other_subject = seed_subject(
        &owner.store,
        "concilium-position-rebind-destination",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    rebind_participant_to_subject(&owner.store, &participant.client_id, &other_subject).await;

    let rejected = owner
        .store
        .call(
            participant,
            "concilium.position.submit".into(),
            position_request(
                "concilium-position-after-rebind-request",
                &concilium_id,
                &slot,
                "A rebound participant cannot write the former slot.",
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "STALE_PARTICIPANT");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_concilium_artifact_evidence_rejects_missing_or_stale_task_revision() {
    let (owner, directory, subject, _, participants) = start_concilium_fixture(
        "concilium-artifact-task-revision",
        &["concilium-artifact-author"],
    )
    .await;
    let missing_revision_id = "artifact-concilium-missing-task-revision".to_owned();
    let stale_revision_id = "artifact-concilium-stale-task-revision".to_owned();
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            for (artifact_id, metadata) in [
                (
                    missing_revision_id,
                    json!({"task_id":task_id.clone(),"attempt_id":attempt_id.clone()}),
                ),
                (
                    stale_revision_id,
                    json!({"task_id":task_id,"attempt_id":attempt_id,"task_revision":2}),
                ),
            ] {
                db.execute(
                    "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) \
                     VALUES(?1,?2,'source_snapshot',1,?3,1,?4)",
                    params![
                        artifact_id,
                        format!("artifacts/{artifact_id}.bin"),
                        model::digest(b"fixture artifact bytes"),
                        model::canonical(&metadata)?,
                    ],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();

    let participant = participants[0].clone();
    let participant_ids = [participant.client_id.as_str()];
    for (request_id, artifact_id) in [
        (
            "concilium-artifact-missing-revision-request",
            "artifact-concilium-missing-task-revision",
        ),
        (
            "concilium-artifact-stale-revision-request",
            "artifact-concilium-stale-task-revision",
        ),
    ] {
        let error = owner
            .store
            .call(
                participant.clone(),
                "concilium.propose".into(),
                proposal_request(
                    &subject,
                    &participant_ids,
                    request_id,
                    request_id,
                    json!([format!("artifact:{artifact_id}")]),
                ),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "EVIDENCE_SCOPE_MISMATCH");
    }
    let admitted_proposals: i64 = owner
        .store
        .run({
            let caller = participant.client_id.clone();
            move |db| {
                Ok(db.query_row(
                    "SELECT count(*) FROM operations WHERE caller_id=?1 AND method='concilium.propose'",
                    [&caller],
                    |row| row.get(0),
                )?)
            }
        })
        .await
        .unwrap();
    assert_eq!(admitted_proposals, 0);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_concilium_recorded_sponsor_read_survives_attempt_owner_handover_and_unbound_manager_is_denied()
 {
    let (owner, directory, subject, sponsor, participants) = start_concilium_fixture(
        "concilium-recorded-sponsor-after-handover",
        &["concilium-handover-proposer"],
    )
    .await;
    let proposer = participants[0].clone();
    let proposer_id = proposer.client_id.clone();
    let participant_ids = [proposer_id.as_str()];
    let proposal = propose(
        &owner.store,
        proposer,
        &subject,
        &participant_ids,
        "concilium-recorded-sponsor-proposal-request",
        "Retained sponsor read after Attempt owner handover",
        json!([]),
    )
    .await;
    let concilium_id = proposal["concilium_id"].as_str().unwrap().to_owned();
    let proposal_operation_id = proposal["operation_id"].as_str().unwrap().to_owned();

    let attempt_id = subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE attempts SET owner_id=?2 WHERE attempt_id=?1",
                params![attempt_id, "review-owner-v1"],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let attempt_id = subject.attempt_id.clone();
    let current_attempt_owner = owner
        .store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT owner_id FROM attempts WHERE attempt_id=?1",
                [&attempt_id],
                |row| row.get::<_, String>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(current_attempt_owner, "review-owner-v1");

    let retained_sponsor_view = owner
        .store
        .call(
            sponsor.clone(),
            "concilium.get".into(),
            json!({"concilium_id":concilium_id,"limit":50}),
        )
        .await
        .unwrap();
    assert_eq!(retained_sponsor_view["concilium_id"], concilium_id);
    let retained_sponsor_preview = owner
        .store
        .call(
            sponsor,
            "concilium.preview".into(),
            json!({"proposal_operation_id":proposal_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(retained_sponsor_preview.code, "FORBIDDEN");

    let unbound_manager = principal("unrelated-manager", Role::Manager);
    let denied_get = owner
        .store
        .call(
            unbound_manager.clone(),
            "concilium.get".into(),
            json!({"concilium_id":concilium_id,"limit":50}),
        )
        .await
        .unwrap_err();
    assert_eq!(denied_get.code, "FORBIDDEN");
    let denied_preview = owner
        .store
        .call(
            unbound_manager,
            "concilium.preview".into(),
            json!({"proposal_operation_id":proposal_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(denied_preview.code, "FORBIDDEN");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn store_concilium_retained_projection_rejects_request_field_scope_and_operation_link_divergence()
 {
    let (owner, directory, subject, manager, participants) = start_concilium_fixture(
        "concilium-projection-source-binding",
        &["concilium-projection-author"],
    )
    .await;
    let participant = participants[0].clone();
    let participant_id = participant.client_id.clone();
    let participant_ids = [participant_id.as_str()];
    let proposal = propose(
        &owner.store,
        participant,
        &subject,
        &participant_ids,
        "concilium-projection-source-proposal-request",
        "Projection source binding question",
        json!([]),
    )
    .await;
    let proposal_operation_id = proposal["operation_id"].as_str().unwrap().to_owned();
    let concilium_id = proposal["concilium_id"].as_str().unwrap().to_owned();
    let original_operation_bytes =
        proposal_operation_bytes(&owner.store, &proposal_operation_id).await;
    let sponsor_projection = manager_projection(&owner.store, manager.clone(), &concilium_id).await;
    assert_eq!(sponsor_projection["concilium_id"], concilium_id);
    let sponsor_preview = owner
        .store
        .call(
            manager.clone(),
            "concilium.preview".into(),
            json!({"proposal_operation_id":proposal_operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(
        sponsor_preview["proposal_operation_id"],
        proposal["operation_id"]
    );
    let unrelated_manager = principal("unrelated-manager", Role::Manager);
    let unbound_baseline = owner
        .store
        .call(
            unrelated_manager.clone(),
            "concilium.get".into(),
            json!({"concilium_id":concilium_id,"limit":50}),
        )
        .await
        .unwrap_err();
    assert_eq!(unbound_baseline.code, "FORBIDDEN");
    let unbound_preview = owner
        .store
        .call(
            unrelated_manager.clone(),
            "concilium.preview".into(),
            json!({"proposal_operation_id":proposal_operation_id}),
        )
        .await
        .unwrap_err();
    assert_eq!(unbound_preview.code, "FORBIDDEN");

    for damage in [
        ProjectionDamage::RehashedCanonicalRequestField,
        ProjectionDamage::Scope,
        ProjectionDamage::ProposalOperationId,
        ProjectionDamage::RecordedManagerReadGrant,
    ] {
        let original_projection = damage_projection(&owner.store, &concilium_id, damage).await;
        assert_projection_digest_matches_source(&owner.store, &concilium_id).await;
        let reader = if matches!(damage, ProjectionDamage::RecordedManagerReadGrant) {
            unrelated_manager.clone()
        } else {
            manager.clone()
        };
        assert_direct_get_and_preview_reject(
            &owner.store,
            reader,
            &concilium_id,
            &proposal_operation_id,
        )
        .await;
        restore_projection(&owner.store, &concilium_id, original_projection).await;
    }

    assert_eq!(
        proposal_operation_bytes(&owner.store, &proposal_operation_id).await,
        original_operation_bytes,
        "projection rewrites leave the originating Operation request, receipt, and effective bytes untouched"
    );
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
