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
    let token = format!("{}{}", model::new_id(), model::new_id());
    owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
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
