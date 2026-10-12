//! Public Store regressions for exact integration-cell identity and coverage.

use super::*;
use rusqlite::params;
use serde_json::{Value, json};

const PRODUCER_ASSIGNMENT_A: &str = "integration-producer-a";
const PRODUCER_ASSIGNMENT_B: &str = "integration-producer-b";

async fn start_integration_fixture(label: &str) -> (StoreOwner, PathBuf, ReviewSubject) {
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
    (owner, directory, subject)
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
    let (owner, directory, subject) = start_integration_fixture(label).await;
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
    let (owner, directory, subject) = start_integration_fixture("integration-basis-identity").await;
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
    assert_eq!(changed["cell"]["cell_id"], first["cell"]["cell_id"]);
    assert_eq!(
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
    let (owner, directory, subject) =
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
