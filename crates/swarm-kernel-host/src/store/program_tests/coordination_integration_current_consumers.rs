//! Public Store regressions for exact integration-cell identity and coverage.

use super::*;
use rusqlite::params;
use serde_json::{Value, json};

const PRODUCER_ASSIGNMENT_A: &str = "integration-producer-a";
const PRODUCER_ASSIGNMENT_B: &str = "integration-producer-b";

async fn start_integration_fixture(
    label: &str,
) -> (StoreOwner, PathBuf, ReviewSubject, Credential) {
    let (owner, directory, credential) = start_store(label).await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        label,
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    owner.store.reconcile_watches_once().await.unwrap();
    owner.store.reconcile_watches_once().await.unwrap();
    (owner, directory, subject, credential)
}

async fn erase_legacy_coordination_indexes(store: &Store) {
    store
        .run(|db| {
            db.execute(
                "DELETE FROM meta WHERE key>=?1 AND key<?2",
                params![
                    crate::coordination::participant_task_namespace_prefix(),
                    "coordination:participant-task;",
                ],
            )?;
            db.execute(
                "DELETE FROM meta WHERE key>=?1 AND key<?2",
                params![
                    crate::coordination::global_relevance_namespace_prefix(),
                    "coordination:relevance-global;",
                ],
            )?;
            db.execute(
                "DELETE FROM meta WHERE key=?1",
                [crate::coordination::LEGACY_INDEX_BACKFILL_KEY],
            )?;
            Ok(())
        })
        .await
        .unwrap();
}

async fn reopen_integration_store(
    directory: &std::path::Path,
    credential: Credential,
) -> StoreOwner {
    let root = DataRoot::acquire(directory).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    StoreOwner::start(root, Arc::new(config), credential)
        .await
        .unwrap()
}

async fn register_integration_participant(
    owner: &StoreOwner,
    subject: &ReviewSubject,
    client_id: &str,
    participation_basis: Value,
) -> Principal {
    register_integration_participant_as(
        owner,
        "review-owner-v2",
        subject,
        client_id,
        participation_basis,
    )
    .await
}

async fn register_integration_participant_as(
    owner: &StoreOwner,
    manager_id: &str,
    subject: &ReviewSubject,
    client_id: &str,
    participation_basis: Value,
) -> Principal {
    let token = format!("{}{}", model::new_id(), model::new_id());
    owner
        .store
        .call(
            principal(manager_id, Role::Manager),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":format!("register-{client_id}"),
                "client_id":client_id,
                "token_hash":model::digest(token.as_bytes()),
                "task_id":subject.task_id,
                "task_revision":1,
                "attempt_id":subject.attempt_id,
                "participation_basis":participation_basis,
            }),
        )
        .await
        .unwrap();
    owner
        .store
        .authenticate(Credential {
            client_id: client_id.to_owned(),
            token,
        })
        .await
        .unwrap()
}

async fn damage_retained_consult_result(
    store: &Store,
    operation_id: &str,
    subject: &ReviewSubject,
    invalid_json: &str,
) {
    let operation_id = operation_id.to_owned();
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    let invalid_json = invalid_json.to_owned();
    store
        .run(move |db| {
            // The fixture has already created and settled a real consult. Bypass
            // its retained-JSON CHECK only for this deliberate corruption.
            db.execute_batch("PRAGMA ignore_check_constraints=ON")?;
            let changed = db.execute(
                "UPDATE operations SET result_json=?1 WHERE operation_id=?2 AND task_id=?3 AND attempt_id=?4",
                params![invalid_json, operation_id, task_id, attempt_id],
            )?;
            db.execute_batch("PRAGMA ignore_check_constraints=OFF")?;
            assert_eq!(changed, 1);
            Ok(())
        })
        .await
        .unwrap();
}

async fn publish_producer_contract_card(
    owner: &StoreOwner,
    participant: &Principal,
    contract_key: &str,
    request_id: &str,
) {
    owner
        .store
        .call(
            participant.clone(),
            "coordination.contract_card.publish".into(),
            json!({
                "client_request_id":request_id,
                "contract_key":contract_key,
                "fields":{
                    "role":"producer",
                    "paths":[format!("src/{contract_key}.rs")],
                    "interfaces":[format!("producer contract {contract_key}")],
                },
            }),
        )
        .await
        .unwrap();
}

async fn publish_consumer_contract_card(
    owner: &StoreOwner,
    participant: &Principal,
    contract_key: &str,
    request_id: &str,
) {
    owner
        .store
        .call(
            participant.clone(),
            "coordination.contract_card.publish".into(),
            json!({
                "client_request_id":request_id,
                "contract_key":contract_key,
                "fields":{
                    "role":"consumer",
                    "paths":[format!("src/{contract_key}.rs")],
                    "interfaces":[format!("producer contract {contract_key}")],
                },
            }),
        )
        .await
        .unwrap();
}

async fn sync_offer(
    owner: &StoreOwner,
    participant: &Principal,
    contract_key: &str,
    request_id: &str,
    assumptions: Vec<String>,
) -> Value {
    owner
        .store
        .call(
            participant.clone(),
            "coordination.sync_integration".into(),
            json!({
                "client_request_id":request_id,
                "contract_key":contract_key,
                "offer":{
                    "readiness":"implementation_ready",
                    "will_be_available_at":{"path":format!("src/{contract_key}.rs")},
                    "candidate_ref":null,
                    "assumptions":assumptions,
                },
            }),
        )
        .await
        .unwrap()
}

async fn sync_requirement(
    owner: &StoreOwner,
    participant: &Principal,
    contract_key: &str,
    request_id: &str,
) -> Value {
    owner
        .store
        .call(
            participant.clone(),
            "coordination.sync_integration".into(),
            json!({
                "client_request_id":request_id,
                "contract_key":contract_key,
                "requirement":{
                    "consumer_path":format!("src/{contract_key}.rs"),
                    "consumer_symbol":null,
                    "required_dimensions":{"version":"v1"},
                    "must_be_ready_before":"integration-test",
                    "assumptions":[],
                },
            }),
        )
        .await
        .unwrap()
}

async fn seed_two_live_producer_assignments(owner: &StoreOwner, subject: &ReviewSubject) {
    let attempt_id = subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            let producers = json!([
                {
                    "assignment_id":PRODUCER_ASSIGNMENT_A,
                    "native_session_id":"integration-session-a",
                    "disposition":"admitted",
                },
                {
                    "assignment_id":PRODUCER_ASSIGNMENT_B,
                    "native_session_id":"integration-session-b",
                    "disposition":"admitted",
                },
            ]);
            let updated = db.execute(
                "UPDATE attempts SET producers_json=?2 WHERE attempt_id=?1",
                params![attempt_id, model::canonical(&producers)?],
            )?;
            assert_eq!(updated, 1);
            Ok(())
        })
        .await
        .unwrap();
}

async fn change_participation_basis_assignment(owner: &StoreOwner, participant: &Principal) {
    let key = format!("client:{}", participant.client_id);
    owner
        .store
        .run(move |db| {
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            let mut registration: Value = serde_json::from_str(&raw)?;
            assert_eq!(registration["participation_basis"]["kind"], "producer_ref");
            assert_eq!(
                registration["participation_basis"]["assignment_id"],
                PRODUCER_ASSIGNMENT_A
            );
            registration["participation_basis"]["assignment_id"] = json!(PRODUCER_ASSIGNMENT_B);
            let updated = db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, model::canonical(&registration)?],
            )?;
            assert_eq!(updated, 1);
            Ok(())
        })
        .await
        .unwrap();
}

fn ack_request(cell: &Value, subject: &ReviewSubject, request_id: &str) -> Value {
    json!({
        "client_request_id":request_id,
        "cell_id":cell["cell_id"],
        "expected_state_revision":cell["state_revision"],
        "expected_material_digest":cell["material_digest"],
        "expected_membership_digest":cell["membership_digest"],
        "task_id":subject.task_id,
        "task_revision":1,
        "attempt_id":subject.attempt_id,
        "decision":"accept",
    })
}

fn agreement_get_request(cell_id: &str, subject: &ReviewSubject, version: Option<&Value>) -> Value {
    let mut request = json!({
        "cell_id":cell_id,
        "task_id":subject.task_id,
        "task_revision":1,
        "attempt_id":subject.attempt_id,
        "limit":10,
    });
    if let Some(version) = version {
        request["state_revision"] = version["state_revision"].clone();
        request["material_digest"] = version["material_digest"].clone();
    }
    request
}

async fn start_assumptions_fixture(
    label: &str,
    participant_count: usize,
) -> (StoreOwner, PathBuf, Vec<Principal>, String) {
    let (owner, directory, subject, _) = start_integration_fixture(label).await;
    let contract_key = format!("integration-assumptions-{label}");
    let mut participants = Vec::with_capacity(participant_count);
    for index in 0..participant_count {
        let client_id = format!("integration-{label}-producer-{index}");
        let participant = register_integration_participant(
            &owner,
            &subject,
            &client_id,
            json!({"kind":"attempt_owner"}),
        )
        .await;
        let request_id = format!("publish-{client_id}");
        publish_producer_contract_card(&owner, &participant, &contract_key, &request_id).await;
        participants.push(participant);
    }
    (owner, directory, participants, contract_key)
}

fn assumption_batch(start: usize, count: usize) -> Vec<String> {
    (start..start + count)
        .map(|index| format!("assumption-{index:03}"))
        .collect()
}

#[tokio::test]
async fn participation_basis_change_invalidates_cell_material_identity() {
    let (owner, directory, subject, _) =
        start_integration_fixture("integration-basis-identity").await;
    seed_two_live_producer_assignments(&owner, &subject).await;
    let participant = register_integration_participant(
        &owner,
        &subject,
        "integration-basis-producer",
        json!({"kind":"producer_ref","assignment_id":PRODUCER_ASSIGNMENT_A}),
    )
    .await;
    let contract_key = "integration-basis-identity-contract";
    publish_producer_contract_card(
        &owner,
        &participant,
        contract_key,
        "publish-integration-basis-contract",
    )
    .await;

    let first = sync_offer(
        &owner,
        &participant,
        contract_key,
        "sync-integration-basis-first",
        vec!["same offer".to_owned()],
    )
    .await;
    let unchanged = sync_offer(
        &owner,
        &participant,
        contract_key,
        "sync-integration-basis-unchanged",
        vec!["same offer".to_owned()],
    )
    .await;
    assert_eq!(
        unchanged["cell"]["state_revision"],
        first["cell"]["state_revision"]
    );
    assert_eq!(
        unchanged["cell"]["material_digest"],
        first["cell"]["material_digest"]
    );
    assert_eq!(unchanged["coalesced"], true);
    let first_ack = owner
        .store
        .call(
            participant.clone(),
            "coordination.integration.ack".into(),
            ack_request(
                &first["cell"],
                &subject,
                "ack-integration-basis-before-change",
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        first_ack["material_digest"],
        first["cell"]["material_digest"]
    );

    change_participation_basis_assignment(&owner, &participant).await;
    let changed = sync_offer(
        &owner,
        &participant,
        contract_key,
        "sync-integration-basis-changed",
        vec!["same offer".to_owned()],
    )
    .await;
    assert_ne!(changed["cell"]["cell_id"], first["cell"]["cell_id"]);
    assert_ne!(
        changed["cell"]["membership_digest"],
        first["cell"]["membership_digest"]
    );
    assert_ne!(
        changed["cell"]["material_digest"],
        first["cell"]["material_digest"]
    );
    assert_eq!(
        changed["cell"]["state_revision"].as_i64(),
        first["cell"]["state_revision"]
            .as_i64()
            .map(|revision| revision + 1),
    );
    let public_cell = changed["cell"].to_string();
    assert!(!public_cell.contains(PRODUCER_ASSIGNMENT_A));
    assert!(!public_cell.contains(PRODUCER_ASSIGNMENT_B));
    assert_eq!(
        changed["cell"]["facts"]["member_basis_set"][0]["participation_basis_kind"],
        "producer_ref"
    );
    assert!(
        changed["cell"]["facts"]["member_basis_set"][0]
            .get("participation_basis")
            .is_none()
    );

    let stale_ack = owner
        .store
        .call(
            participant.clone(),
            "coordination.integration.ack".into(),
            ack_request(&first["cell"], &subject, "ack-integration-basis-stale"),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_ack.code, "STALE_REVISION");
    let current_before_ack = owner
        .store
        .call(
            participant.clone(),
            "coordination.agreement.get".into(),
            agreement_get_request(changed["cell"]["cell_id"].as_str().unwrap(), &subject, None),
        )
        .await
        .unwrap();
    assert_eq!(current_before_ack["current"], true);
    assert!(
        current_before_ack["positions"]["items"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    let historical = owner
        .store
        .call(
            participant.clone(),
            "coordination.agreement.get".into(),
            agreement_get_request(
                first["cell"]["cell_id"].as_str().unwrap(),
                &subject,
                Some(&first["cell"]),
            ),
        )
        .await
        .unwrap();
    assert_eq!(historical["current"], false);
    assert_eq!(
        historical["positions"]["items"].as_array().unwrap().len(),
        1
    );
    let current_ack = owner
        .store
        .call(
            participant.clone(),
            "coordination.integration.ack".into(),
            ack_request(&changed["cell"], &subject, "ack-integration-basis-current"),
        )
        .await
        .unwrap();
    assert_eq!(
        current_ack["state_revision"],
        changed["cell"]["state_revision"]
    );
    assert_eq!(
        current_ack["material_digest"],
        changed["cell"]["material_digest"]
    );
    let current_after_ack = owner
        .store
        .call(
            participant,
            "coordination.agreement.get".into(),
            agreement_get_request(changed["cell"]["cell_id"].as_str().unwrap(), &subject, None),
        )
        .await
        .unwrap();
    assert_eq!(
        current_after_ack["positions"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn scoped_manager_attention_isolates_and_surfaces_corrupt_consults() {
    let (owner, directory, subject, _) =
        start_integration_fixture("issue99-scoped-consult-attention-corruption").await;
    let foreign_subject = seed_subject(
        &owner.store,
        "issue99-scoped-consult-attention-foreign",
        "unrelated-manager",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    attach_ready_repair_binding(&owner.store, &subject).await;
    let caller = register_integration_participant(
        &owner,
        &subject,
        "issue99-scoped-consult-attention-caller",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let foreign_caller = register_integration_participant_as(
        &owner,
        "unrelated-manager",
        &foreign_subject,
        "issue99-scoped-consult-attention-foreign-caller",
        json!({"kind":"attempt_owner"}),
    )
    .await;

    let request = |request_id: &str, target: &str| {
        json!({
            "client_request_id":request_id,
            "target":{"contract_key":target},
            "field":"interfaces",
            "question_kind":"contract_shape",
            "question":"Who owns this exact integration contract?",
            "why_needed":"The missing owner blocks this Task.",
            "expected_answer":"one_fact",
            "blocking":true,
            "reply_deadline_ms":model::now_ms().unwrap()+60_000,
            "evidence_refs":[],
        })
    };
    let own_consult = owner
        .store
        .call(
            caller,
            "coordination.consult".into(),
            request(
                "issue99-scoped-consult-attention-own",
                "issue99-scoped-consult-attention-own-selector",
            ),
        )
        .await
        .unwrap();
    assert_eq!(own_consult["status"], "unowned");
    let foreign_consult = owner
        .store
        .call(
            foreign_caller,
            "coordination.consult".into(),
            request(
                "issue99-scoped-consult-attention-foreign",
                "issue99-scoped-consult-attention-foreign-selector",
            ),
        )
        .await
        .unwrap();
    assert_eq!(foreign_consult["status"], "unowned");

    let foreign_operation_id = foreign_consult["manager_attention"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let damaged_foreign_json = "{\"private\":\"DO_NOT_LEAK_FOREIGN_CONSULT_BYTES\"";
    damage_retained_consult_result(
        &owner.store,
        &foreign_operation_id,
        &foreign_subject,
        damaged_foreign_json,
    )
    .await;

    let manager = principal("review-owner-v2", Role::Manager);
    let inspect = owner
        .store
        .call(
            manager.clone(),
            "swarm.agent.inspect".into(),
            json!({"attempt_id":subject.attempt_id}),
        )
        .await
        .unwrap();
    let scoped_attention = &inspect["runtime"]["attention"]["binding_manager_attention"];
    let scoped_items = scoped_attention["items"].as_array().unwrap();
    assert!(scoped_items.iter().any(|item| {
        item["kind"] == "unowned_blocking_consult"
            && item["address"]["operation_id"] == own_consult["manager_attention"]["operation_id"]
    }));
    assert!(scoped_items.iter().all(|item| {
        item["address"]["operation_id"] != foreign_consult["manager_attention"]["operation_id"]
    }));

    let own_operation_id = own_consult["manager_attention"]["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let damaged_own_json = "{\"private\":\"DO_NOT_LEAK_OWN_CONSULT_BYTES\"";
    damage_retained_consult_result(&owner.store, &own_operation_id, &subject, damaged_own_json)
        .await;

    let damaged_inspect = owner
        .store
        .call(
            manager,
            "swarm.agent.inspect".into(),
            json!({"attempt_id":subject.attempt_id}),
        )
        .await
        .unwrap();
    let damaged_attention = &damaged_inspect["runtime"]["attention"]["binding_manager_attention"];
    let gaps = damaged_attention["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| {
            item["kind"] == "coordination_consult_data_gap"
                && item["address"]["operation_id"]
                    == own_consult["manager_attention"]["operation_id"]
        })
        .collect::<Vec<_>>();
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0]["address"]["task_id"], subject.task_id);
    assert_eq!(gaps[0]["address"]["task_revision"], 1);
    assert_eq!(gaps[0]["address"]["attempt_id"], subject.attempt_id);
    assert_eq!(gaps[0]["address"]["projection_status"], "unknown");
    assert_eq!(gaps[0]["gap"]["status"], "unknown");
    assert_eq!(gaps[0]["gap"]["reason"], "consult_result_json_invalid");
    assert_eq!(gaps[0]["suggested_action"]["method"], "operation.get");
    let projected_json = serde_json::to_string(&damaged_attention).unwrap();
    assert!(!projected_json.contains("DO_NOT_LEAK_OWN_CONSULT_BYTES"));
    assert!(!projected_json.contains("DO_NOT_LEAK_FOREIGN_CONSULT_BYTES"));

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_backfill_rechecks_repaired_hints_and_ignores_inactive_pending_clients() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-backfill-recovery-source").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-backfill-recovery-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let unrelated_subject = seed_subject(
        &owner.store,
        "issue99-backfill-recovery-unrelated",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-backfill-recovery-producer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-backfill-recovery-consumer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let inactive = register_integration_participant(
        &owner,
        &unrelated_subject,
        "issue99-backfill-recovery-disabled",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let pending_client_id = "issue99-backfill-recovery-pending-reviewer";
    let pending_token = format!("{}{}", model::new_id(), model::new_id());
    owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.participant.register".into(),
            json!({
                "client_request_id":"issue99-backfill-recovery-register-pending",
                "client_id":pending_client_id,
                "token_hash":model::digest(pending_token.as_bytes()),
                "task_id":unrelated_subject.task_id,
                "task_revision":1,
                "attempt_id":unrelated_subject.attempt_id,
                "participation_basis":{
                    "kind":"sponsored_reviewer",
                    "review_scope":{
                        "review_assignment_id":null,
                        "task_id":unrelated_subject.task_id,
                        "task_revision":1,
                        "attempt_id":unrelated_subject.attempt_id,
                        "submission_ref":unrelated_subject.submission_ref,
                        "candidate_ref":unrelated_subject.candidate_ref,
                    },
                },
            }),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.participant.disable".into(),
            json!({
                "client_request_id":"issue99-backfill-recovery-disable",
                "client_id":inactive.client_id,
                "expected_grant_revision":1,
            }),
        )
        .await
        .unwrap();

    let contract_key = "issue99-backfill-recovery-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-backfill-recovery-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-backfill-recovery-publish-target",
    )
    .await;

    let source_id = source.client_id.clone();
    let source_task_id = source_subject.task_id.clone();
    let source_attempt_id = source_subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            let key = crate::coordination::participant_task_key(&source_task_id, &source_id);
            let incorrect = json!({
                "client_id":source_id,
                "task_id":source_task_id,
                "task_revision":1,
                "attempt_id":source_attempt_id,
                "grant_revision":99,
            });
            let changed = db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, model::canonical(&incorrect)?],
            )?;
            assert_eq!(changed, 1);
            db.execute(
                "DELETE FROM meta WHERE key=?1",
                [crate::coordination::LEGACY_INDEX_BACKFILL_KEY],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    owner.store.reconcile_watches_once().await.unwrap();
    owner.store.reconcile_watches_once().await.unwrap();
    let partial_status = owner
        .store
        .run(|db| crate::store::coordination::legacy_index_backfill_status(db))
        .await
        .unwrap();
    assert_eq!(partial_status["scan_complete"], true);
    assert_eq!(partial_status["complete"], false);
    assert_eq!(partial_status["anomaly_count"], 1);

    let found = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "complete");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], target.client_id);

    let first_cell = sync_offer(
        &owner,
        &source,
        contract_key,
        "issue99-backfill-recovery-sync-source",
        vec![],
    )
    .await;
    let second_cell = sync_requirement(
        &owner,
        &target,
        contract_key,
        "issue99-backfill-recovery-sync-target",
    )
    .await;
    assert_eq!(
        first_cell["cell"]["cell_id"],
        second_cell["cell"]["cell_id"]
    );

    owner.store.reconcile_watches_once().await.unwrap();
    owner.store.reconcile_watches_once().await.unwrap();
    let recovered_status = owner
        .store
        .run(|db| crate::store::coordination::legacy_index_backfill_status(db))
        .await
        .unwrap();
    assert_eq!(recovered_status["scan_complete"], true);
    assert_eq!(recovered_status["complete"], true);
    assert_eq!(recovered_status["anomaly_count"], 0);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_peer_find_advances_after_filtered_task_and_global_pages() {
    let (owner, directory, subject, _) = start_integration_fixture("issue99-filtered-cursor").await;
    let source = register_integration_participant(
        &owner,
        &subject,
        "zzz-issue99-filtered-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &subject,
        "zzz-issue99-filtered-target",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-filtered-cursor-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-filtered-cursor-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-filtered-cursor-publish-target",
    )
    .await;

    let task_id = subject.task_id.clone();
    let source_id = source.client_id.clone();
    let target_id = target.client_id.clone();
    let attempt_id = subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            let source_key = crate::coordination::participant_task_key(&task_id, &source_id);
            db.execute("DELETE FROM meta WHERE key=?1", [source_key])?;
            let mut insert = db.prepare("INSERT INTO meta(key,value_json) VALUES(?1,?2)")?;
            for index in 0..1_000 {
                let client_id = format!("000-issue99-task-filter-{index:04}");
                let key = crate::coordination::participant_task_key(&task_id, &client_id);
                let value = json!({
                    "client_id":client_id,
                    "task_id":task_id,
                    "task_revision":1,
                    "attempt_id":attempt_id,
                    "grant_revision":1,
                });
                insert.execute(params![key, model::canonical(&value)?])?;
            }
            drop(insert);
            let target_key = crate::coordination::participant_task_key(&task_id, &target_id);
            let last_filtered_key =
                crate::coordination::participant_task_key(&task_id, "000-issue99-task-filter-0999");
            assert!(last_filtered_key < target_key);
            let prefix = crate::coordination::participant_task_prefix(&task_id);
            let upper = format!("{prefix}g");
            let indexed_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key>=?1 AND key<?2",
                params![prefix, upper],
                |row| row.get(0),
            )?;
            assert_eq!(indexed_count, 1_001);
            Ok(())
        })
        .await
        .unwrap();

    let first_task_page = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"related_task_id":subject.task_id,"limit":10}),
        )
        .await
        .unwrap();
    assert!(first_task_page["items"].as_array().unwrap().is_empty());
    assert_eq!(first_task_page["coverage"], "partial");
    let task_cursor = first_task_page["next_scan_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_task_page = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({
                "related_task_id":subject.task_id,
                "limit":10,
                "after_scan_cursor":task_cursor,
            }),
        )
        .await
        .unwrap();
    assert_eq!(second_task_page["coverage"], "complete");
    assert_eq!(second_task_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(second_task_page["items"][0]["client_id"], target.client_id);

    let source_id = source.client_id.clone();
    let target_id = target.client_id.clone();
    let contract_key_for_seed = contract_key.to_owned();
    owner
        .store
        .run(move |db| {
            let source_key = crate::coordination::global_relevance_key(
                crate::coordination::TermKind::Contract,
                &contract_key_for_seed,
                &source_id,
                "contract",
                &contract_key_for_seed,
            );
            db.execute("DELETE FROM meta WHERE key=?1", [source_key])?;
            let mut insert = db.prepare("INSERT INTO meta(key,value_json) VALUES(?1,?2)")?;
            for index in 0..1_000 {
                let client_id = format!("000-issue99-global-filter-{index:04}");
                let key = crate::coordination::global_relevance_key(
                    crate::coordination::TermKind::Contract,
                    &contract_key_for_seed,
                    &client_id,
                    "contract",
                    &contract_key_for_seed,
                );
                // Invalid index structure still obeys meta's JSON constraint.
                insert.execute(params![key, "{}"])?;
            }
            drop(insert);
            let target_key = crate::coordination::global_relevance_key(
                crate::coordination::TermKind::Contract,
                &contract_key_for_seed,
                &target_id,
                "contract",
                &contract_key_for_seed,
            );
            let last_filtered_key = crate::coordination::global_relevance_key(
                crate::coordination::TermKind::Contract,
                &contract_key_for_seed,
                "000-issue99-global-filter-0999",
                "contract",
                &contract_key_for_seed,
            );
            assert!(last_filtered_key < target_key);
            let prefix = crate::coordination::global_relevance_prefix(
                crate::coordination::TermKind::Contract,
                &contract_key_for_seed,
            );
            let upper = format!("{prefix}g");
            let indexed_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key>=?1 AND key<?2",
                params![prefix, upper],
                |row| row.get(0),
            )?;
            assert_eq!(indexed_count, 1_001);
            Ok(())
        })
        .await
        .unwrap();

    let first_card_page = owner
        .store
        .call(
            source.clone(),
            "coordination.contract_card.list".into(),
            json!({"contract_key":contract_key,"limit":50}),
        )
        .await
        .unwrap();
    assert!(first_card_page["items"].as_array().unwrap().is_empty());
    assert_eq!(first_card_page["coverage"], "partial");
    let first_card_cursor = first_card_page["next_scan_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_card_page = owner
        .store
        .call(
            source.clone(),
            "coordination.contract_card.list".into(),
            json!({
                "contract_key":contract_key,
                "limit":50,
                "after_scan_cursor":first_card_cursor,
            }),
        )
        .await
        .unwrap();
    assert!(second_card_page["items"].as_array().unwrap().is_empty());
    assert_eq!(second_card_page["coverage"], "partial");
    assert_ne!(
        first_card_cursor,
        second_card_page["next_scan_cursor"].as_str().unwrap()
    );

    let first_contract_get = owner
        .store
        .call(
            source.clone(),
            "coordination.contract_card.get".into(),
            json!({"contract_key":contract_key,"limit":50}),
        )
        .await
        .unwrap();
    assert!(first_contract_get["items"].as_array().unwrap().is_empty());
    assert_eq!(first_contract_get["coverage"], "partial");
    let first_contract_cursor = first_contract_get["next_scan_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_contract_get = owner
        .store
        .call(
            source.clone(),
            "coordination.contract_card.get".into(),
            json!({
                "contract_key":contract_key,
                "limit":50,
                "after_scan_cursor":first_contract_cursor,
            }),
        )
        .await
        .unwrap();
    assert!(second_contract_get["items"].as_array().unwrap().is_empty());
    assert_eq!(second_contract_get["coverage"], "partial");
    assert_ne!(
        first_contract_cursor,
        second_contract_get["next_scan_cursor"].as_str().unwrap()
    );

    let first_global_page = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert!(first_global_page["items"].as_array().unwrap().is_empty());
    assert_eq!(first_global_page["coverage"], "partial");
    let global_cursor = first_global_page["next_scan_cursor"]
        .as_str()
        .unwrap()
        .to_owned();
    let second_global_page = owner
        .store
        .call(
            source,
            "coordination.peer.find".into(),
            json!({
                "contract_key":contract_key,
                "limit":10,
                "after_scan_cursor":global_cursor,
            }),
        )
        .await
        .unwrap();
    assert_eq!(second_global_page["coverage"], "complete");
    assert_eq!(second_global_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        second_global_page["items"][0]["client_id"],
        target.client_id
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_exact_contract_lookup_bypasses_unrelated_owner_prefix_cards() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-contract-owner-prefix-source").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-contract-owner-prefix-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-contract-owner-prefix-source-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-contract-owner-prefix-target-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "zzz-issue99-seam";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-contract-owner-prefix-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-contract-owner-prefix-publish-target",
    )
    .await;

    for index in 0..101 {
        publish_producer_contract_card(
            &owner,
            &source,
            &format!("000-issue99-prefix-noise-{index:03}"),
            &format!("issue99-prefix-noise-publish-{index:03}"),
        )
        .await;
    }

    let source_id = source.client_id.clone();
    let contract_key_for_seed = contract_key.to_owned();
    owner
        .store
        .run(move |db| {
            let scope = crate::store::coordination::load_current_scope_for_client(db, &source_id)?;
            let prefix = format!(
                "coordination:card-owner:{}:{}:contract:",
                scope.scope_id,
                crate::coordination::key_component(&source_id),
            );
            let seam_key = format!(
                "{prefix}{}",
                crate::coordination::key_component(&contract_key_for_seed)
            );
            let last_noise_key = format!(
                "{prefix}{}",
                crate::coordination::key_component("000-issue99-prefix-noise-100")
            );
            assert!(last_noise_key < seam_key);
            let upper = format!("{prefix}g");
            let owner_count: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key>=?1 AND key<?2",
                params![prefix, upper],
                |row| row.get(0),
            )?;
            assert_eq!(owner_count, 102);
            Ok(())
        })
        .await
        .unwrap();

    let found = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "complete");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], target.client_id);
    assert_eq!(found["items"][0]["match"]["relation"]["term"], contract_key);

    let sent = owner
        .store
        .call(
            source,
            "coordination.send".into(),
            json!({
                "client_request_id":"issue99-contract-owner-prefix-send",
                "recipient":target.client_id,
                "body":{"fact":"exact current contract beyond owner prefix"},
            }),
        )
        .await
        .unwrap();
    assert_eq!(sent["recipient"], target.client_id);
    let inbox = owner
        .store
        .call(target, "coordination.inbox".into(), json!({"limit":10}))
        .await
        .unwrap();
    assert_eq!(inbox["items"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["items"][0]["operation_id"], sent["operation_id"]);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn agreement_get_marks_stale_cell_not_current() {
    let (owner, directory, subject, _) =
        start_integration_fixture("integration-current-pointer").await;
    let first_participant = register_integration_participant(
        &owner,
        &subject,
        "integration-pointer-producer-one",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "integration-current-pointer-contract";
    publish_producer_contract_card(
        &owner,
        &first_participant,
        contract_key,
        "publish-integration-pointer-one",
    )
    .await;
    let old = sync_offer(
        &owner,
        &first_participant,
        contract_key,
        "sync-integration-pointer-old",
        vec!["old member basis".to_owned()],
    )
    .await;
    let second_participant = register_integration_participant(
        &owner,
        &subject,
        "integration-pointer-producer-two",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    publish_producer_contract_card(
        &owner,
        &second_participant,
        contract_key,
        "publish-integration-pointer-two",
    )
    .await;
    let current = sync_offer(
        &owner,
        &second_participant,
        contract_key,
        "sync-integration-pointer-current",
        vec!["new member basis".to_owned()],
    )
    .await;
    assert_ne!(old["cell"]["cell_id"], current["cell"]["cell_id"]);

    let current_read = owner
        .store
        .call(
            second_participant.clone(),
            "coordination.agreement.get".into(),
            agreement_get_request(current["cell"]["cell_id"].as_str().unwrap(), &subject, None),
        )
        .await
        .unwrap();
    assert_eq!(current_read["current"], true);

    let stale_read = owner
        .store
        .call(
            first_participant.clone(),
            "coordination.agreement.get".into(),
            agreement_get_request(old["cell"]["cell_id"].as_str().unwrap(), &subject, None),
        )
        .await
        .unwrap();
    assert_eq!(stale_read["current"], false);
    assert_eq!(stale_read["classification"]["state"], "stale");
    assert!(
        stale_read["classification"]["reasons"]
            .as_array()
            .unwrap()
            .iter()
            .any(|reason| reason == "retained_cell_revision_is_not_current")
    );

    let historical_read = owner
        .store
        .call(
            first_participant,
            "coordination.agreement.get".into(),
            agreement_get_request(
                old["cell"]["cell_id"].as_str().unwrap(),
                &subject,
                Some(&old["cell"]),
            ),
        )
        .await
        .unwrap();
    assert_eq!(historical_read["current"], false);
    assert_eq!(
        historical_read["cell"]["state_revision"],
        old["cell"]["state_revision"]
    );
    assert_eq!(
        historical_read["cell"]["material_digest"],
        old["cell"]["material_digest"]
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn cell_assumptions_reports_gap_after_forty_items() {
    let (owner, directory, participants, contract_key) =
        start_assumptions_fixture("assumptions-over-limit", 3).await;
    let _first = sync_offer(
        &owner,
        &participants[0],
        &contract_key,
        "sync-assumptions-over-limit-first",
        assumption_batch(0, 20),
    )
    .await;
    let _second = sync_offer(
        &owner,
        &participants[1],
        &contract_key,
        "sync-assumptions-over-limit-second",
        assumption_batch(20, 20),
    )
    .await;
    let result = sync_offer(
        &owner,
        &participants[2],
        &contract_key,
        "sync-assumptions-over-limit-third",
        assumption_batch(40, 1),
    )
    .await;
    let assumptions = result["cell"]["facts"]["assumptions"].as_array().unwrap();
    assert_eq!(assumptions.len(), 40);
    let gap = result["gaps"]
        .as_array()
        .unwrap()
        .iter()
        .find(|gap| gap["kind"] == "cell_assumptions_projection_bound")
        .expect("omitted assumptions have a stable coverage gap");
    assert_eq!(gap["limit"], 40);
    assert_eq!(gap["omitted_count"], 1);
    assert_eq!(result["coverage"], "partial");
    assert_eq!(result["cell"]["facts"]["coverage"], "partial");
    assert_eq!(result["cell"]["state"], "unknown");
    assert!(
        result["cell"]["facts"]["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap["kind"] == "cell_assumptions_projection_bound")
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn cell_assumptions_at_forty_items_has_no_omission_gap() {
    let (owner, directory, participants, contract_key) =
        start_assumptions_fixture("assumptions-at-limit", 2).await;
    let _first = sync_offer(
        &owner,
        &participants[0],
        &contract_key,
        "sync-assumptions-at-limit-first",
        assumption_batch(0, 20),
    )
    .await;
    let result = sync_offer(
        &owner,
        &participants[1],
        &contract_key,
        "sync-assumptions-at-limit-second",
        assumption_batch(20, 20),
    )
    .await;
    let assumptions = result["cell"]["facts"]["assumptions"].as_array().unwrap();
    assert_eq!(assumptions.len(), 40);
    assert!(
        result["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .all(|gap| { gap["kind"] != "cell_assumptions_projection_bound" })
    );
    assert_ne!(result["coverage"], "partial");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn exact_cross_task_peer_delivery_and_cell_use_each_owners_current_context() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-cross-task-source").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-cross-task-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let unrelated_subject = seed_subject(
        &owner.store,
        "issue99-cross-task-unrelated",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-cross-task-producer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-cross-task-consumer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let unrelated = register_integration_participant(
        &owner,
        &unrelated_subject,
        "issue99-cross-task-unrelated",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-cross-task-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-cross-task-publish-producer",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-cross-task-publish-consumer",
    )
    .await;

    let found = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "complete");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], target.client_id);
    assert_eq!(
        found["items"][0]["context"]["task_id"],
        target_subject.task_id
    );
    assert_eq!(
        found["items"][0]["match"]["relation"]["kind"],
        "contract_key"
    );

    let ready_answer = owner
        .store
        .call(
            source.clone(),
            "coordination.consult".into(),
            json!({
                "client_request_id":"issue99-cross-task-ready-consult",
                "target":{"contract_key":contract_key},
                "field":"interfaces",
                "question_kind":"contract_shape",
                "question":"Which interface is current?",
                "why_needed":"Use the retained exact contract card.",
                "expected_answer":"one_fact",
                "blocking":false,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap();
    assert_eq!(ready_answer["status"], "answered_from_card");
    assert_eq!(ready_answer["delivery_created"], false);

    let ask_request = json!({
        "client_request_id":"issue99-cross-task-ask",
        "target":{"contract_key":contract_key},
        "field":"assumptions",
        "question_kind":"assumption_check",
        "question":"Which implementation assumption applies?",
        "why_needed":"The exact contract card has no current assumption value.",
        "expected_answer":"one_fact",
        "blocking":false,
        "reply_deadline_ms":model::now_ms().unwrap()+60_000,
        "evidence_refs":[],
    });
    let asked = owner
        .store
        .call(
            source.clone(),
            "coordination.consult".into(),
            ask_request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(asked["status"], "asked");
    assert_eq!(asked["recipient"], target.client_id);
    assert_eq!(asked["source_context"]["task_id"], source_subject.task_id);
    assert_eq!(asked["target_context"]["task_id"], target_subject.task_id);
    let retried = owner
        .store
        .call(source.clone(), "coordination.consult".into(), ask_request)
        .await
        .unwrap();
    assert_eq!(retried["operation_id"], asked["operation_id"]);
    assert_eq!(retried["delivery_id"], asked["delivery_id"]);

    let inbox = owner
        .store
        .call(
            target.clone(),
            "coordination.inbox".into(),
            json!({"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(inbox["task_id"], target_subject.task_id);
    assert_eq!(inbox["items"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["items"][0]["operation_id"], asked["operation_id"]);
    assert_eq!(
        inbox["items"][0]["body"]["schema"],
        "eliot.coordination.consult.v1"
    );

    let unrelated_send = owner
        .store
        .call(
            source.clone(),
            "coordination.send".into(),
            json!({
                "client_request_id":"issue99-cross-task-no-relation",
                "recipient":unrelated.client_id,
                "body":{"fact":"should not be delivered"},
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(unrelated_send.code, "FORBIDDEN");
    let first_cell = sync_offer(
        &owner,
        &source,
        contract_key,
        "issue99-cross-task-sync-producer",
        vec![],
    )
    .await;
    let second_cell = sync_requirement(
        &owner,
        &target,
        contract_key,
        "issue99-cross-task-sync-consumer",
    )
    .await;
    assert_eq!(
        first_cell["cell"]["cell_id"],
        second_cell["cell"]["cell_id"]
    );
    let task_revision_set = second_cell["cell"]["facts"]["task_revision_set"]
        .as_array()
        .unwrap();
    assert_eq!(task_revision_set.len(), 2);
    assert!(
        task_revision_set
            .iter()
            .any(|item| item["task_id"] == source_subject.task_id)
    );
    assert!(
        task_revision_set
            .iter()
            .any(|item| item["task_id"] == target_subject.task_id)
    );
    assert_eq!(
        second_cell["cell"]["facts"]["member_basis_set"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    owner
        .store
        .call(
            source.clone(),
            "coordination.integration.ack".into(),
            ack_request(
                &second_cell["cell"],
                &source_subject,
                "issue99-cross-task-ack-producer",
            ),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            target.clone(),
            "coordination.integration.ack".into(),
            ack_request(
                &second_cell["cell"],
                &target_subject,
                "issue99-cross-task-ack-consumer",
            ),
        )
        .await
        .unwrap();
    let agreed_cell = owner
        .store
        .call(
            target.clone(),
            "coordination.agreement.get".into(),
            agreement_get_request(
                second_cell["cell"]["cell_id"].as_str().unwrap(),
                &target_subject,
                None,
            ),
        )
        .await
        .unwrap();
    assert_eq!(agreed_cell["current"], true);
    assert_eq!(
        agreed_cell["positions"]["items"].as_array().unwrap().len(),
        2
    );
    let foreign_read = owner
        .store
        .call(
            unrelated.clone(),
            "coordination.agreement.get".into(),
            agreement_get_request(
                second_cell["cell"]["cell_id"].as_str().unwrap(),
                &unrelated_subject,
                None,
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign_read.code, "NOT_FOUND");

    // Model a new current assignment for the same recipient. The old delivery
    // remains at the original target tuple and does not follow the actor.
    let target_client_id = target.client_id.clone();
    let old_task_id = target_subject.task_id.clone();
    let new_task_id = unrelated_subject.task_id.clone();
    let new_attempt_id = unrelated_subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            let key = format!("client:{target_client_id}");
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            let mut registration: Value = serde_json::from_str(&raw)?;
            registration["task_id"] = json!(new_task_id);
            registration["task_revision"] = json!(1);
            registration["attempt_id"] = json!(new_attempt_id);
            registration["grant_revision"] = json!(2);
            db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, model::canonical(&registration)?],
            )?;
            db.execute(
                "DELETE FROM meta WHERE key=?1",
                [crate::coordination::participant_task_key(
                    &old_task_id,
                    &target_client_id,
                )],
            )?;
            db.execute(
                "INSERT OR REPLACE INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    crate::coordination::participant_task_key(
                        registration["task_id"].as_str().unwrap_or_default(),
                        &target_client_id,
                    ),
                    model::canonical(&json!({
                        "client_id":target_client_id,
                        "task_id":registration["task_id"],
                        "task_revision":registration["task_revision"],
                        "attempt_id":registration["attempt_id"],
                        "grant_revision":registration["grant_revision"],
                    }))?,
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let moved_inbox = owner
        .store
        .call(target, "coordination.inbox".into(), json!({"limit":10}))
        .await
        .unwrap();
    assert_eq!(moved_inbox["task_id"], unrelated_subject.task_id);
    assert!(moved_inbox["items"].as_array().unwrap().is_empty());

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn bounded_legacy_index_backfill_recovers_peers_and_cell_members_after_reopen() {
    let (owner, directory, source_subject, credential) =
        start_integration_fixture("issue99-legacy-backfill-source").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-legacy-backfill-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-legacy-backfill-source-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let same_task_peer = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-legacy-backfill-same-task-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-legacy-backfill-target-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-legacy-backfill-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-legacy-backfill-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-legacy-backfill-publish-target",
    )
    .await;
    let first_cell = sync_offer(
        &owner,
        &source,
        contract_key,
        "issue99-legacy-backfill-sync-source",
        vec![],
    )
    .await;
    let second_cell = sync_requirement(
        &owner,
        &target,
        contract_key,
        "issue99-legacy-backfill-sync-target",
    )
    .await;
    assert_eq!(
        first_cell["cell"]["cell_id"],
        second_cell["cell"]["cell_id"]
    );
    assert_eq!(
        second_cell["cell"]["facts"]["member_basis_set"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    erase_legacy_coordination_indexes(&owner.store).await;
    owner.close().await.unwrap();
    let owner = reopen_integration_store(&directory, credential).await;
    let initial_status = owner
        .store
        .run(|db| crate::store::coordination::legacy_index_backfill_status(db))
        .await
        .unwrap();
    assert_eq!(initial_status["complete"], false);

    let before_grant_page = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"related_task_id":source_subject.task_id,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(before_grant_page["coverage"], "partial");
    assert!(before_grant_page["items"].as_array().unwrap().is_empty());

    owner.store.reconcile_watches_once().await.unwrap();
    let pending_status = owner
        .store
        .run(|db| crate::store::coordination::legacy_index_backfill_status(db))
        .await
        .unwrap();
    assert_eq!(pending_status["phase"], "cards");
    assert_eq!(pending_status["complete"], false);
    let task_peers = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"related_task_id":source_subject.task_id,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(task_peers["coverage"], "partial");
    assert_eq!(task_peers["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        task_peers["items"][0]["client_id"],
        same_task_peer.client_id
    );

    let pending_consult = owner
        .store
        .call(
            source.clone(),
            "coordination.consult".into(),
            json!({
                "client_request_id":"issue99-legacy-backfill-pending-consult",
                "target":{"contract_key":contract_key},
                "field":"interfaces",
                "question_kind":"contract_shape",
                "question":"Which interface is current?",
                "why_needed":"Resolve the exact current card owner.",
                "expected_answer":"one_fact",
                "blocking":false,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap();
    assert_eq!(pending_consult["status"], "unknown_coverage");
    assert_eq!(pending_consult["delivery_created"], false);

    owner.store.reconcile_watches_once().await.unwrap();
    let complete_status = owner
        .store
        .run(|db| crate::store::coordination::legacy_index_backfill_status(db))
        .await
        .unwrap();
    assert_eq!(complete_status["complete"], true);
    let found = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "complete");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], target.client_id);

    let source_id = source.client_id.clone();
    let target_id = target.client_id.clone();
    let contract_key = contract_key.to_owned();
    let cell_relation = owner
        .store
        .run(move |db| {
            let source_scope =
                crate::store::coordination::load_current_scope_for_client(db, &source_id)?;
            let target_scope =
                crate::store::coordination::load_current_scope_for_client(db, &target_id)?;
            crate::store::integration::current_peer_cell_relation(
                db,
                &crate::store::coordination::scope_context(&source_scope),
                &crate::store::coordination::scope_context(&target_scope),
                Some((crate::coordination::TermKind::Contract, contract_key)),
            )
        })
        .await
        .unwrap();
    assert_eq!(cell_relation.unwrap()["kind"], "active_integration_cell");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn legacy_index_migration_keeps_wrong_actor_and_stale_card_coverage_partial() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-legacy-index-corruption").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-legacy-index-corruption-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-legacy-index-corruption-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let same_task_peer = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-legacy-index-corruption-same-task",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-legacy-index-corruption-target-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-legacy-index-corruption-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-legacy-index-corruption-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-legacy-index-corruption-publish-target",
    )
    .await;
    erase_legacy_coordination_indexes(&owner.store).await;

    let same_task_id = same_task_peer.client_id.clone();
    let target_id = target.client_id.clone();
    let source_task_id = source_subject.task_id.clone();
    let source_attempt_id = source_subject.attempt_id.clone();
    let target_task_id = target_subject.task_id.clone();
    let target_attempt_id = target_subject.attempt_id.clone();
    let contract_key_for_seed = contract_key.to_owned();
    owner
        .store
        .run(move |db| {
            let task_key =
                crate::coordination::participant_task_key(&source_task_id, &same_task_id);
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    task_key,
                    model::canonical(&json!({
                        "client_id":"oldActor",
                        "task_id":&source_task_id,
                        "task_revision":1,
                        "attempt_id":&source_attempt_id,
                        "grant_revision":1,
                    }))?,
                ],
            )?;
            let stale_actor_task_key =
                crate::coordination::participant_task_key(&source_task_id, "oldActor");
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    stale_actor_task_key,
                    model::canonical(&json!({
                        "client_id":"oldActor",
                        "task_id":&source_task_id,
                        "task_revision":1,
                        "attempt_id":&source_attempt_id,
                        "grant_revision":1,
                    }))?,
                ],
            )?;
            let wrong_card_revision_key = crate::coordination::global_relevance_key(
                crate::coordination::TermKind::Contract,
                &contract_key_for_seed,
                &target_id,
                "contract",
                &contract_key_for_seed,
            );
            let target_scope_id =
                crate::coordination::scope_id(&target_task_id, 1, &target_attempt_id)?;
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    wrong_card_revision_key,
                    model::canonical(&json!({
                        "client_id":&target_id,
                        "scope_id":&target_scope_id,
                        "card_kind":"contract",
                        "identity":&contract_key_for_seed,
                        "term_kind":"contract",
                        "term":&contract_key_for_seed,
                        "card_revision":999,
                    }))?,
                ],
            )?;
            let stale_actor_card_key = crate::coordination::global_relevance_key(
                crate::coordination::TermKind::Contract,
                &contract_key_for_seed,
                "oldActor",
                "contract",
                &contract_key_for_seed,
            );
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    stale_actor_card_key,
                    model::canonical(&json!({
                        "client_id":"oldActor",
                        "scope_id":"stale-scope",
                        "card_kind":"contract",
                        "identity":&contract_key_for_seed,
                        "term_kind":"contract",
                        "term":&contract_key_for_seed,
                        "card_revision":1,
                    }))?,
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let stale_before_repair = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(stale_before_repair["coverage"], "partial");
    assert!(stale_before_repair["items"].as_array().unwrap().is_empty());
    assert!(
        stale_before_repair["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["client_id"] != "oldActor")
    );

    owner.store.reconcile_watches_once().await.unwrap();
    owner.store.reconcile_watches_once().await.unwrap();
    let status = owner
        .store
        .run(|db| crate::store::coordination::legacy_index_backfill_status(db))
        .await
        .unwrap();
    assert_eq!(status["scan_complete"], true);
    assert_eq!(status["complete"], false);
    assert_eq!(status["coverage"], "partial");

    let task_peers = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"related_task_id":source_subject.task_id,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(task_peers["coverage"], "partial");
    assert_eq!(task_peers["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        task_peers["items"][0]["client_id"],
        same_task_peer.client_id
    );
    assert!(
        task_peers["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["client_id"] != "oldActor")
    );

    let target_id = target.client_id.clone();
    let found = owner
        .store
        .call(
            source,
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "partial");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], target_id);
    assert!(
        found["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["client_id"] != "oldActor")
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn legacy_index_backfill_propagates_derived_write_failure() {
    let (owner, directory, subject, _) =
        start_integration_fixture("issue99-legacy-index-write-failure").await;
    let participant = register_integration_participant(
        &owner,
        &subject,
        "issue99-legacy-index-write-failure-participant",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    erase_legacy_coordination_indexes(&owner.store).await;
    owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER fail_legacy_coordination_index_insert \
                 BEFORE INSERT ON meta \
                 WHEN NEW.key GLOB 'coordination:participant-task:*' \
                 BEGIN SELECT RAISE(ABORT, 'fixture persistence failure'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let error = owner.store.reconcile_watches_once().await.unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    assert_ne!(participant.client_id, "oldActor");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_negative_no_relation_send_forbidden() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-negative-norelation").await;
    let unrelated_subject = seed_subject(
        &owner.store,
        "issue99-negative-norelation-other",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-negative-norelation-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let unrelated = register_integration_participant(
        &owner,
        &unrelated_subject,
        "issue99-negative-norelation-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-negative-norelation-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-negative-norelation-publish",
    )
    .await;
    let denied = owner
        .store
        .call(
            source.clone(),
            "coordination.send".into(),
            json!({
                "client_request_id":"issue99-negative-norelation-send",
                "recipient":unrelated.client_id,
                "body":{"fact":"no relation"},
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.code, "FORBIDDEN");
    let found = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert!(
        found["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["client_id"] != unrelated.client_id)
    );
    let unrelated_inbox = owner
        .store
        .call(
            unrelated.clone(),
            "coordination.inbox".into(),
            json!({"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(unrelated_inbox["task_id"], unrelated_subject.task_id);
    assert!(unrelated_inbox["items"].as_array().unwrap().is_empty());
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_negative_forged_revision_ack_stale() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-negative-forged").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-negative-forged-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-negative-forged-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-negative-forged-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-negative-forged-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-negative-forged-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-negative-forged-publish-target",
    )
    .await;
    let first_cell = sync_offer(
        &owner,
        &source,
        contract_key,
        "issue99-negative-forged-sync-source",
        vec![],
    )
    .await;
    let second_cell = sync_requirement(
        &owner,
        &target,
        contract_key,
        "issue99-negative-forged-sync-target",
    )
    .await;
    assert_eq!(
        first_cell["cell"]["cell_id"],
        second_cell["cell"]["cell_id"]
    );
    let mut forged = ack_request(
        &second_cell["cell"],
        &source_subject,
        "issue99-negative-forged-ack",
    );
    forged["expected_state_revision"] =
        json!(second_cell["cell"]["state_revision"].as_i64().unwrap_or(0) + 99);
    let denied = owner
        .store
        .call(
            source.clone(),
            "coordination.integration.ack".into(),
            forged,
        )
        .await
        .unwrap_err();
    assert_eq!(denied.code, "STALE_REVISION");
    owner
        .store
        .call(
            source.clone(),
            "coordination.integration.ack".into(),
            ack_request(
                &second_cell["cell"],
                &source_subject,
                "issue99-negative-forged-ack-source",
            ),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            target.clone(),
            "coordination.integration.ack".into(),
            ack_request(
                &second_cell["cell"],
                &target_subject,
                "issue99-negative-forged-ack-target",
            ),
        )
        .await
        .unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_negative_revoked_grant_send_stale() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-negative-revoked").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-negative-revoked-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-negative-revoked-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-negative-revoked-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-negative-revoked-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-negative-revoked-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-negative-revoked-publish-target",
    )
    .await;
    let found = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "complete");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], target.client_id);
    owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.participant.disable".into(),
            json!({
                "client_request_id":"issue99-negative-revoked-disable",
                "client_id":target.client_id,
                "expected_grant_revision":1,
            }),
        )
        .await
        .unwrap();
    let denied = owner
        .store
        .call(
            source.clone(),
            "coordination.send".into(),
            json!({
                "client_request_id":"issue99-negative-revoked-send",
                "recipient":target.client_id,
                "body":{"fact":"revoked grant"},
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(denied.code, "STALE_PARTICIPANT");
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_negative_retry_consult_no_second_delivery() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-negative-retry").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-negative-retry-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-negative-retry-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-negative-retry-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-negative-retry-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-negative-retry-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-negative-retry-publish-target",
    )
    .await;
    let ask_request = json!({
        "client_request_id":"issue99-negative-retry-ask",
        "target":{"contract_key":contract_key},
        "field":"assumptions",
        "question_kind":"assumption_check",
        "question":"Which implementation assumption applies?",
        "why_needed":"The exact contract card has no current assumption value.",
        "expected_answer":"one_fact",
        "blocking":false,
        "reply_deadline_ms":model::now_ms().unwrap()+60_000,
        "evidence_refs":[],
    });
    let asked = owner
        .store
        .call(
            source.clone(),
            "coordination.consult".into(),
            ask_request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(asked["status"], "asked");
    assert_eq!(asked["source_context"]["task_id"], source_subject.task_id);
    assert_eq!(asked["target_context"]["task_id"], target_subject.task_id);
    let retried = owner
        .store
        .call(source.clone(), "coordination.consult".into(), ask_request)
        .await
        .unwrap();
    assert_eq!(retried["operation_id"], asked["operation_id"]);
    assert_eq!(retried["delivery_id"], asked["delivery_id"]);
    let inbox = owner
        .store
        .call(
            target.clone(),
            "coordination.inbox".into(),
            json!({"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(inbox["task_id"], target_subject.task_id);
    assert_eq!(inbox["items"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["items"][0]["operation_id"], asked["operation_id"]);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_negative_old_delivery_not_rerouted() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-negative-reroute").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-negative-reroute-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let new_home_subject = seed_subject(
        &owner.store,
        "issue99-negative-reroute-new-home",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-negative-reroute-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-negative-reroute-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-negative-reroute-contract";
    publish_producer_contract_card(
        &owner,
        &source,
        contract_key,
        "issue99-negative-reroute-publish-source",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &target,
        contract_key,
        "issue99-negative-reroute-publish-target",
    )
    .await;
    let ask_request = json!({
        "client_request_id":"issue99-negative-reroute-ask",
        "target":{"contract_key":contract_key},
        "field":"assumptions",
        "question_kind":"assumption_check",
        "question":"Which implementation assumption applies?",
        "why_needed":"The exact contract card has no current assumption value.",
        "expected_answer":"one_fact",
        "blocking":false,
        "reply_deadline_ms":model::now_ms().unwrap()+60_000,
        "evidence_refs":[],
    });
    let asked = owner
        .store
        .call(source.clone(), "coordination.consult".into(), ask_request)
        .await
        .unwrap();
    assert_eq!(asked["target_context"]["task_id"], target_subject.task_id);
    let before_move = owner
        .store
        .call(
            target.clone(),
            "coordination.inbox".into(),
            json!({"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(before_move["task_id"], target_subject.task_id);
    assert_eq!(before_move["items"].as_array().unwrap().len(), 1);
    let target_client_id = target.client_id.clone();
    let old_task_id = target_subject.task_id.clone();
    let new_task_id = new_home_subject.task_id.clone();
    let new_attempt_id = new_home_subject.attempt_id.clone();
    owner
        .store
        .run(move |db| {
            let key = format!("client:{target_client_id}");
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            let mut registration: Value = serde_json::from_str(&raw)?;
            registration["task_id"] = json!(new_task_id);
            registration["task_revision"] = json!(1);
            registration["attempt_id"] = json!(new_attempt_id);
            registration["grant_revision"] = json!(2);
            db.execute(
                "UPDATE meta SET value_json=?2 WHERE key=?1",
                params![key, model::canonical(&registration)?],
            )?;
            db.execute(
                "DELETE FROM meta WHERE key=?1",
                [crate::coordination::participant_task_key(
                    &old_task_id,
                    &target_client_id,
                )],
            )?;
            db.execute(
                "INSERT OR REPLACE INTO meta(key,value_json) VALUES(?1,?2)",
                params![
                    crate::coordination::participant_task_key(
                        registration["task_id"].as_str().unwrap_or_default(),
                        &target_client_id,
                    ),
                    model::canonical(&json!({
                        "client_id":target_client_id,
                        "task_id":registration["task_id"],
                        "task_revision":registration["task_revision"],
                        "attempt_id":registration["attempt_id"],
                        "grant_revision":registration["grant_revision"],
                    }))?,
                ],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let moved_inbox = owner
        .store
        .call(target, "coordination.inbox".into(), json!({"limit":10}))
        .await
        .unwrap();
    assert_eq!(moved_inbox["task_id"], new_home_subject.task_id);
    assert!(moved_inbox["items"].as_array().unwrap().is_empty());
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn publish_carrier_contract_card(
    owner: &StoreOwner,
    participant: &Principal,
    contract_key: &str,
    request_id: &str,
) {
    owner
        .store
        .call(
            participant.clone(),
            "coordination.contract_card.publish".into(),
            json!({
                "client_request_id":request_id,
                "contract_key":contract_key,
                "fields":{
                    "role":"carrier",
                    "paths":[format!("src/{contract_key}.rs")],
                    "interfaces":[format!("carrier contract {contract_key}")],
                },
            }),
        )
        .await
        .unwrap();
}

async fn publish_work_term_card(
    owner: &StoreOwner,
    participant: &Principal,
    path: &str,
    symbol: &str,
    interface: &str,
    request_id: &str,
) {
    owner
        .store
        .call(
            participant.clone(),
            "coordination.work_card.publish".into(),
            json!({
                "client_request_id":request_id,
                "fields":{
                    "paths":[path],
                    "symbols":[symbol],
                    "interfaces":[interface],
                },
            }),
        )
        .await
        .unwrap();
}

async fn propose_accept_shared_scope(
    owner: &StoreOwner,
    participant: &Principal,
    subject: &ReviewSubject,
    label: &str,
    path: &str,
    symbol: &str,
    interface: &str,
) {
    let scope = owner
        .store
        .call(
            participant.clone(),
            "code.scope.propose".into(),
            json!({
                "client_request_id":format!("{label}-propose"),
                "task_id":subject.task_id.clone(),
                "task_revision":1,
                "attempt_id":subject.attempt_id.clone(),
                "mode":"exclusive_edit",
                "paths":[path],
                "symbols":[symbol],
                "interfaces":[interface],
                "baseline_candidate_ref":"advisory-public-baseline",
                "reason":format!("exact shared seam {label}"),
            }),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "code.scope.accept".into(),
            json!({
                "client_request_id":format!("{label}-accept"),
                "scope_intent_id":scope["scope_intent_id"],
                "expected_state_revision":scope["state_revision"],
                "proposal_digest":scope["digest"],
                "reason":format!("accept exact shared seam {label}"),
            }),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn issue99_reviewer_cross_task_via_contract_uses_each_owners_current_context() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-reviewer-xtask").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-reviewer-xtask-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let manager = principal("review-owner-v2", Role::Manager);
    let (reviewer, _) = assign_sponsored_reviewer(
        &owner.store,
        manager,
        &source_subject,
        "issue99-reviewer-xtask-reviewer",
        "issue99-reviewer-xtask-token",
    )
    .await;
    let consumer = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-reviewer-xtask-consumer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-reviewer-xtask-contract";
    publish_producer_contract_card(
        &owner,
        &reviewer,
        contract_key,
        "issue99-reviewer-xtask-publish-reviewer",
    )
    .await;
    publish_consumer_contract_card(
        &owner,
        &consumer,
        contract_key,
        "issue99-reviewer-xtask-publish-consumer",
    )
    .await;
    let found = owner
        .store
        .call(
            reviewer.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "complete");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], consumer.client_id);
    assert_eq!(
        found["items"][0]["context"]["task_id"],
        target_subject.task_id
    );
    assert_eq!(
        found["items"][0]["match"]["relation"]["kind"],
        "contract_key"
    );
    let asked = owner
        .store
        .call(
            reviewer.clone(),
            "coordination.consult".into(),
            json!({
                "client_request_id":"issue99-reviewer-xtask-ask",
                "target":{"contract_key":contract_key},
                "field":"assumptions",
                "question_kind":"assumption_check",
                "question":"Which implementation assumption applies?",
                "why_needed":"The exact contract card has no current assumption value.",
                "expected_answer":"one_fact",
                "blocking":false,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap();
    assert_eq!(asked["status"], "asked");
    assert_eq!(asked["recipient"], consumer.client_id);
    assert_eq!(asked["source_context"]["task_id"], source_subject.task_id);
    assert_eq!(asked["target_context"]["task_id"], target_subject.task_id);
    let inbox = owner
        .store
        .call(
            consumer.clone(),
            "coordination.inbox".into(),
            json!({"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(inbox["task_id"], target_subject.task_id);
    assert_eq!(inbox["items"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["items"][0]["operation_id"], asked["operation_id"]);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_carrier_role_cross_task_cell_uses_each_owners_current_context() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-carrier-xtask").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-carrier-xtask-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let producer = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-carrier-xtask-producer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let carrier = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-carrier-xtask-carrier",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let contract_key = "issue99-carrier-xtask-contract";
    publish_producer_contract_card(
        &owner,
        &producer,
        contract_key,
        "issue99-carrier-xtask-publish-producer",
    )
    .await;
    publish_carrier_contract_card(
        &owner,
        &carrier,
        contract_key,
        "issue99-carrier-xtask-publish-carrier",
    )
    .await;
    let found = owner
        .store
        .call(
            producer.clone(),
            "coordination.peer.find".into(),
            json!({"contract_key":contract_key,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["coverage"], "complete");
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], carrier.client_id);
    assert_eq!(
        found["items"][0]["match"]["relation"]["kind"],
        "contract_key"
    );
    let first_cell = sync_offer(
        &owner,
        &producer,
        contract_key,
        "issue99-carrier-xtask-sync-producer",
        vec![],
    )
    .await;
    let second_cell = sync_offer(
        &owner,
        &carrier,
        contract_key,
        "issue99-carrier-xtask-sync-carrier",
        vec![],
    )
    .await;
    assert_eq!(
        first_cell["cell"]["cell_id"],
        second_cell["cell"]["cell_id"]
    );
    let task_revision_set = second_cell["cell"]["facts"]["task_revision_set"]
        .as_array()
        .unwrap();
    assert_eq!(task_revision_set.len(), 2);
    assert!(
        task_revision_set
            .iter()
            .any(|item| item["task_id"] == source_subject.task_id)
    );
    assert!(
        task_revision_set
            .iter()
            .any(|item| item["task_id"] == target_subject.task_id)
    );
    assert_eq!(
        second_cell["cell"]["facts"]["member_basis_set"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let carrier_constraints = second_cell["cell"]["facts"]["carrier_constraints"]
        .as_array()
        .unwrap();
    assert_eq!(carrier_constraints.len(), 1);
    assert_eq!(carrier_constraints[0]["client_id"], carrier.client_id);
    owner
        .store
        .call(
            producer.clone(),
            "coordination.integration.ack".into(),
            ack_request(
                &second_cell["cell"],
                &source_subject,
                "issue99-carrier-xtask-ack-producer",
            ),
        )
        .await
        .unwrap();
    owner
        .store
        .call(
            carrier.clone(),
            "coordination.integration.ack".into(),
            ack_request(
                &second_cell["cell"],
                &target_subject,
                "issue99-carrier-xtask-ack-carrier",
            ),
        )
        .await
        .unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_task_dependency_cross_task_delivery_uses_each_owners_current_context() {
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-task-dependency").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-task-dependency-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source_task_id = source_subject.task_id.clone();
    let dep_task_id = target_subject.task_id.clone();
    owner
        .store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT spec_json FROM tasks WHERE task_id=?1",
                [&source_task_id],
                |row| row.get(0),
            )?;
            let mut spec: Value = serde_json::from_str(&raw)?;
            spec["dependencies"] = json!([{
                "task_id":dep_task_id,
                "required_revision":1,
                "required_phase":"implementation",
            }]);
            db.execute(
                "UPDATE tasks SET spec_json=?2 WHERE task_id=?1",
                params![source_task_id, model::canonical(&spec)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-task-dependency-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-task-dependency-target",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let found = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"related_task_id":target_subject.task_id,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(found["items"].as_array().unwrap().len(), 1);
    assert_eq!(found["items"][0]["client_id"], target.client_id);
    assert_eq!(
        found["items"][0]["context"]["task_id"],
        target_subject.task_id
    );
    assert_eq!(
        found["items"][0]["match"]["relation"]["kind"],
        "task_dependency"
    );
    assert_eq!(
        found["items"][0]["match"]["relation"]["direction"],
        "source_depends_on_target"
    );
    let sent = owner
        .store
        .call(
            source.clone(),
            "coordination.send".into(),
            json!({
                "client_request_id":"issue99-task-dependency-send",
                "recipient":target.client_id,
                "body":{"fact":"dependency delivery"},
            }),
        )
        .await
        .unwrap();
    assert_eq!(sent["recipient"], target.client_id);
    let inbox = owner
        .store
        .call(
            target.clone(),
            "coordination.inbox".into(),
            json!({"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(inbox["task_id"], target_subject.task_id);
    assert_eq!(inbox["items"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["items"][0]["operation_id"], sent["operation_id"]);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn issue99_accepted_scope_path_symbol_interface_cross_task_uses_each_owners_current_context()
{
    let (owner, directory, source_subject, _) =
        start_integration_fixture("issue99-accepted-scope").await;
    let target_subject = seed_subject(
        &owner.store,
        "issue99-accepted-scope-target",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let source = register_integration_participant(
        &owner,
        &source_subject,
        "issue99-accepted-scope-source",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let target = register_integration_participant(
        &owner,
        &target_subject,
        "issue99-accepted-scope-peer",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let path = "src/issue99-shared-seam.rs";
    let symbol = "Issue99SharedSeam";
    let interface = "issue99-shared-seam-v1";
    publish_work_term_card(
        &owner,
        &source,
        path,
        symbol,
        interface,
        "issue99-accepted-scope-publish-source",
    )
    .await;
    publish_work_term_card(
        &owner,
        &target,
        path,
        symbol,
        interface,
        "issue99-accepted-scope-publish-target",
    )
    .await;
    propose_accept_shared_scope(
        &owner,
        &source,
        &source_subject,
        "issue99-accepted-scope-source",
        path,
        symbol,
        interface,
    )
    .await;
    propose_accept_shared_scope(
        &owner,
        &target,
        &target_subject,
        "issue99-accepted-scope-target",
        path,
        symbol,
        interface,
    )
    .await;
    let by_path = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"path":path,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(by_path["items"].as_array().unwrap().len(), 1);
    assert_eq!(by_path["items"][0]["client_id"], target.client_id);
    assert_eq!(
        by_path["items"][0]["context"]["task_id"],
        target_subject.task_id
    );
    assert_eq!(
        by_path["items"][0]["match"]["relation"]["kind"],
        "accepted_scope_overlap"
    );
    assert_eq!(
        by_path["items"][0]["match"]["relation"]["term_kind"],
        "path"
    );
    assert_eq!(by_path["items"][0]["match"]["relation"]["term"], path);
    let by_symbol = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"symbol":symbol,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(by_symbol["items"].as_array().unwrap().len(), 1);
    assert_eq!(by_symbol["items"][0]["client_id"], target.client_id);
    assert_eq!(
        by_symbol["items"][0]["match"]["relation"]["kind"],
        "accepted_scope_overlap"
    );
    assert_eq!(
        by_symbol["items"][0]["match"]["relation"]["term_kind"],
        "symbol"
    );
    let by_interface = owner
        .store
        .call(
            source.clone(),
            "coordination.peer.find".into(),
            json!({"interface":interface,"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(by_interface["items"].as_array().unwrap().len(), 1);
    assert_eq!(by_interface["items"][0]["client_id"], target.client_id);
    assert_eq!(
        by_interface["items"][0]["match"]["relation"]["kind"],
        "accepted_scope_overlap"
    );
    assert_eq!(
        by_interface["items"][0]["match"]["relation"]["term_kind"],
        "interface"
    );
    let sent = owner
        .store
        .call(
            source.clone(),
            "coordination.send".into(),
            json!({
                "client_request_id":"issue99-accepted-scope-send",
                "recipient":target.client_id,
                "body":{"fact":"scope overlap delivery"},
            }),
        )
        .await
        .unwrap();
    assert_eq!(sent["recipient"], target.client_id);
    let inbox = owner
        .store
        .call(
            target.clone(),
            "coordination.inbox".into(),
            json!({"limit":10}),
        )
        .await
        .unwrap();
    assert_eq!(inbox["task_id"], target_subject.task_id);
    assert_eq!(inbox["items"].as_array().unwrap().len(), 1);
    assert_eq!(inbox["items"][0]["operation_id"], sent["operation_id"]);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn unowned_blocking_consult_reaches_current_manager_attention_once() {
    let (owner, directory, subject, _) =
        start_integration_fixture("issue99-unowned-blocking-attention").await;
    let unrelated_subject = seed_subject(
        &owner.store,
        "issue99-unowned-blocking-attention-unrelated",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let caller = register_integration_participant(
        &owner,
        &subject,
        "issue99-unowned-blocking-attention-caller",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let retired = register_integration_participant(
        &owner,
        &unrelated_subject,
        "issue99-unowned-blocking-attention-retired",
        json!({"kind":"attempt_owner"}),
    )
    .await;
    let manager = principal("review-owner-v2", Role::Manager);
    let request = json!({
        "client_request_id":"issue99-unowned-blocking-attention-consult",
        "target":{"contract_key":"issue99-unowned-blocking-attention-selector"},
        "field":"interfaces",
        "question_kind":"contract_shape",
        "question":"Who owns this exact integration contract?",
        "why_needed":"The missing owner blocks this Task.",
        "expected_answer":"one_fact",
        "blocking":true,
        "reply_deadline_ms":model::now_ms().unwrap()+60_000,
        "evidence_refs":[],
    });
    let first = owner
        .store
        .call(
            caller.clone(),
            "coordination.consult".into(),
            request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(first["status"], "unowned");
    assert_eq!(first["manager_attention"]["created"], true);
    assert_eq!(first["task_id"], subject.task_id);
    assert_eq!(first["task_revision"], 1);
    assert_eq!(first["attempt_id"], subject.attempt_id);

    let report = owner
        .store
        .call(
            manager.clone(),
            "report.attention".into(),
            json!({"after":0,"limit":200}),
        )
        .await
        .unwrap();
    let attention = report["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["kind"] == "unowned_blocking_consult")
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(attention.len(), 1);
    let item = &attention[0];
    assert_eq!(
        item["address"]["operation_id"],
        first["manager_attention"]["operation_id"]
    );
    assert_eq!(item["address"]["requester_id"], caller.client_id);
    assert_eq!(item["address"]["task_id"], subject.task_id);
    assert_eq!(item["address"]["task_revision"], first["task_revision"]);
    assert_eq!(item["address"]["attempt_id"], subject.attempt_id);
    assert_eq!(
        item["address"]["selector"],
        json!({"contract_key":"issue99-unowned-blocking-attention-selector"})
    );
    assert_eq!(item["address"]["field"], "interfaces");

    let retry = owner
        .store
        .call(caller.clone(), "coordination.consult".into(), request)
        .await
        .unwrap();
    assert_eq!(
        retry["manager_attention"]["operation_id"],
        first["manager_attention"]["operation_id"]
    );
    let retry_report = owner
        .store
        .call(
            manager.clone(),
            "report.attention".into(),
            json!({"after":0,"limit":200}),
        )
        .await
        .unwrap();
    assert_eq!(
        retry_report["items"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|item| item["kind"] == "unowned_blocking_consult")
            .count(),
        1
    );

    let nonblocking = owner
        .store
        .call(
            caller.clone(),
            "coordination.consult".into(),
            json!({
                "client_request_id":"issue99-unowned-nonblocking-attention-consult",
                "target":{"contract_key":"issue99-unowned-nonblocking-attention-nonblocking"},
                "field":"interfaces",
                "question_kind":"contract_shape",
                "question":"Who owns this optional integration contract?",
                "why_needed":"Record this nonblocking lookup.",
                "expected_answer":"one_fact",
                "blocking":false,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap();
    assert_eq!(nonblocking["status"], "unowned");
    assert!(nonblocking["manager_attention"].is_null());

    let unrelated_task_ids = vec![unrelated_subject.task_id.clone()];
    let unrelated_scope_report = owner
        .store
        .run(move |db| {
            super::super::capacity::attention_report_for_scope(db, 200, 0, &unrelated_task_ids)
        })
        .await
        .unwrap();
    assert!(
        unrelated_scope_report["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| item["kind"] != "unowned_blocking_consult")
    );

    owner
        .store
        .call(
            manager.clone(),
            "coordination.participant.disable".into(),
            json!({
                "client_request_id":"issue99-unowned-blocking-attention-disable",
                "client_id":retired.client_id,
                "expected_grant_revision":1,
            }),
        )
        .await
        .unwrap();
    let retired_consult = owner
        .store
        .call(
            retired,
            "coordination.consult".into(),
            json!({
                "client_request_id":"issue99-retired-unowned-blocking-attention-consult",
                "target":{"contract_key":"issue99-retired-unowned-blocking-attention-selector"},
                "field":"interfaces",
                "question_kind":"contract_shape",
                "question":"Who owns this retired participant's selector?",
                "why_needed":"This actor has been disabled.",
                "expected_answer":"one_fact",
                "blocking":true,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(retired_consult.code, "UNAUTHORIZED");
    let final_report = owner
        .store
        .call(
            manager,
            "report.attention".into(),
            json!({"after":0,"limit":200}),
        )
        .await
        .unwrap();
    let final_items = final_report["items"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|item| item["kind"] == "unowned_blocking_consult")
        .collect::<Vec<_>>();
    assert_eq!(final_items.len(), 1);
    assert_eq!(
        final_items[0]["address"]["operation_id"],
        first["manager_attention"]["operation_id"]
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
