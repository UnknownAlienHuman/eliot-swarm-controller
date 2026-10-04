//! One public Store path through correction, fresh review, and exact acceptance.
use super::*;
use crate::artifacts::ArtifactRecord;
use rusqlite::params;
use serde_json::json;

async fn register_source_candidate(
    store: &Store,
    task_id: &str,
    attempt_id: &str,
    source: &str,
) -> String {
    let source_digest = model::digest(source.as_bytes());
    let source_length = source.len();
    let manifest = json!({
        "schema_version":1,
        "files":[{
            "path":"src/lib.rs",
            "content":source,
            "content_sha256":source_digest,
            "byte_length":source_length,
        }],
    });
    let bytes = model::canonical(&manifest).unwrap().into_bytes();
    let artifact_id = format!("source-{}", model::digest(&bytes));
    let metadata = json!({
        "task_id":task_id,
        "attempt_id":attempt_id,
        "task_revision":1,
        "coverage":"complete",
    });
    let record = ArtifactRecord {
        kind: "source_snapshot".into(),
        artifact_id: artifact_id.clone(),
        relative_path: format!("artifacts/{artifact_id}.bin"),
        byte_length: u64::try_from(bytes.len()).unwrap(),
        content_digest: model::digest(&bytes),
        metadata: metadata.clone(),
    };
    let publish_record = record.clone();
    let publish_bytes = bytes.clone();
    store
        .file_io(move |files| files.publish(&publish_record, &publish_bytes))
        .await
        .unwrap();

    let db_id = record.artifact_id;
    let db_path = record.relative_path;
    let db_length = i64::try_from(record.byte_length).unwrap();
    let db_digest = record.content_digest;
    let db_metadata = model::canonical(&metadata).unwrap();
    let created_at = model::now_ms().unwrap();
    store
        .run(move |db| {
            db.execute(
                "INSERT INTO artifacts(artifact_id,relative_path,kind,byte_length,content_digest,created_at_ms,metadata_json) \
                 VALUES(?1,?2,'source_snapshot',?3,?4,?5,?6)",
                params![db_id, db_path, db_length, db_digest, created_at, db_metadata],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    artifact_id
}

async fn submit_candidate(
    store: &Store,
    owner: &Principal,
    task_id: &str,
    attempt_id: &str,
    candidate_ref: &str,
    expected_submission_ref: Option<&str>,
    request_id: &str,
) -> ReviewSubject {
    let receipt = store
        .call(
            owner.clone(),
            "task.submit".into(),
            json!({
                "client_request_id":request_id,
                "attempt_id":attempt_id,
                "expected_revision":1,
                "expected_submission_ref":expected_submission_ref,
                "candidate_ref":candidate_ref,
                "summary":"Apply the exact retained candidate for review.",
                "claims":[],
            }),
        )
        .await
        .unwrap();
    let operation_id = receipt["operation_id"].as_str().unwrap().to_owned();
    let operation = store
        .call(
            owner.clone(),
            "operation.get".into(),
            json!({"operation_id":operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(operation["method"], "task.submit");
    assert_eq!(operation["state"], "settled");
    assert_eq!(operation["result"]["outcome"], "applied");
    assert_eq!(operation["result"]["attempt_id"], attempt_id);
    assert_eq!(operation["result"]["candidate_ref"], candidate_ref);

    let submission_ref = operation["result"]["submission_ref"]
        .as_str()
        .unwrap()
        .to_owned();
    let submission = store
        .call(
            owner.clone(),
            "task.submission".into(),
            json!({"submission_ref":submission_ref}),
        )
        .await
        .unwrap();
    assert_eq!(submission["task_id"], task_id);
    assert_eq!(submission["attempt_id"], attempt_id);
    assert_eq!(submission["candidate_ref"], candidate_ref);
    assert_eq!(
        submission["previous_submission_ref"],
        json!(expected_submission_ref)
    );
    ReviewSubject {
        task_id: task_id.to_owned(),
        attempt_id: attempt_id.to_owned(),
        submission_ref,
        candidate_ref: candidate_ref.to_owned(),
    }
}

fn acceptance_request(subject: &ReviewSubject, request_id: &str) -> serde_json::Value {
    json!({
        "client_request_id":request_id,
        "attempt_id":subject.attempt_id,
        "expected_revision":1,
        "submission_ref":subject.submission_ref,
        "candidate_ref":subject.candidate_ref,
        "expected_feedback_observation_id":0,
        "reason":"Accept this exact candidate after its assigned review passed.",
        "reviews":[{
            "requirement_id":"R1",
            "rationale":"The reviewed candidate retains the required evidence.",
            "evidence":[subject.candidate_ref],
        }],
        "check_ids":[],
    })
}

#[tokio::test]
async fn same_attempt_return_repair_fresh_review_and_acceptance_bind_to_candidate_b() {
    let (owner, directory, bootstrap) = start_store("o7-same-attempt-cycle").await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    seed_clients(&owner.store).await;
    owner
        .store
        .run(|db| super::super::set_meta(db, "execution_mode", &json!({"new_work":"enabled"})))
        .await
        .unwrap();

    let task_owner = principal("review-owner-v2", Role::Manager);
    let current_gm = principal("review-gm", Role::Manager);
    let created = owner
        .store
        .call(
            operator,
            "task.create".into(),
            json!({
                "client_request_id":"create-o7-correction-cycle",
                "project_id":"fixture",
                "spec":{
                    "objective":"Correct one reviewed candidate and accept only its successor.",
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Candidate B contains the retained implementation evidence."}],
                    "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                    "acceptance":{"required_check_profiles":[]},
                },
            }),
        )
        .await
        .unwrap();
    assert_eq!(created["created"], true);
    let task_id = created["task_id"].as_str().unwrap().to_owned();
    let claimed = owner
        .store
        .call(
            task_owner.clone(),
            "task.claim".into(),
            json!({
                "client_request_id":"claim-o7-correction-cycle",
                "task_id":task_id,
                "expected_revision":1,
                "owner_id":task_owner.client_id,
            }),
        )
        .await
        .unwrap();
    let attempt_id = claimed["attempt_id"].as_str().unwrap().to_owned();

    // Candidate bytes are fixture inputs; both submission documents and their
    // applied Operation/readback are produced by the real Store task.submit path.
    let candidate_a = register_source_candidate(
        &owner.store,
        &task_id,
        &attempt_id,
        "Candidate A is missing the required implementation evidence.",
    )
    .await;
    let submission_a = submit_candidate(
        &owner.store,
        &task_owner,
        &task_id,
        &attempt_id,
        &candidate_a,
        None,
        "submit-o7-candidate-a",
    )
    .await;
    let (reviewer_a, assignment_a) = assign_sponsored_reviewer(
        &owner.store,
        task_owner.clone(),
        &submission_a,
        "o7-candidate-a-reviewer",
        "o7-candidate-a-reviewer-token",
    )
    .await;
    let result_a = owner
        .store
        .call(
            reviewer_a,
            "review.submit".into(),
            review_result_request(&submission_a, &assignment_a, "review-o7-candidate-a"),
        )
        .await
        .unwrap();
    assert_eq!(result_a["verdict"], "changes_requested");
    assert_eq!(result_a["task_transition"], "none");
    assert_eq!(result_a["acceptance_changed"], false);
    assert_eq!(result_a["publication_started"], false);

    let returned = owner
        .store
        .call(
            task_owner.clone(),
            "task.request_changes".into(),
            json!({
                "client_request_id":"return-o7-candidate-a",
                "attempt_id":attempt_id,
                "expected_revision":1,
                "submission_ref":submission_a.submission_ref,
                "candidate_ref":candidate_a,
                "finding_id":"missing-r1-evidence",
                "reason":"Add the required retained implementation evidence.",
                "requirement_ids":["R1"],
                "evidence":["evidence://review/r1"],
            }),
        )
        .await
        .unwrap();
    assert_eq!(returned["applied"], true);
    assert_eq!(returned["status"], "needs_correction");
    assert_eq!(
        returned["review_provenance"]["review_assignment_id"],
        assignment_a
    );
    let after_return = owner
        .store
        .call(
            task_owner.clone(),
            "attempt.get".into(),
            json!({"attempt_id":attempt_id}),
        )
        .await
        .unwrap();
    assert_eq!(after_return["attempt_id"], attempt_id);
    assert_eq!(after_return["state"], "needs_correction");
    let task_after_return = owner
        .store
        .call(
            task_owner.clone(),
            "task.get".into(),
            json!({"task_id":task_id}),
        )
        .await
        .unwrap();
    assert_eq!(task_after_return["state"], "open");
    assert!(task_after_return["accepted_operation_id"].is_null());

    let candidate_b = register_source_candidate(
        &owner.store,
        &task_id,
        &attempt_id,
        "Candidate B adds the required retained implementation evidence.",
    )
    .await;
    let submission_b = submit_candidate(
        &owner.store,
        &task_owner,
        &task_id,
        &attempt_id,
        &candidate_b,
        Some(&submission_a.submission_ref),
        "submit-o7-candidate-b",
    )
    .await;
    assert_eq!(submission_b.task_id, submission_a.task_id);
    assert_eq!(submission_b.attempt_id, submission_a.attempt_id);
    assert_ne!(submission_b.submission_ref, submission_a.submission_ref);
    assert_ne!(submission_b.candidate_ref, submission_a.candidate_ref);

    let (reviewer_b, assignment_b) = assign_sponsored_reviewer(
        &owner.store,
        task_owner.clone(),
        &submission_b,
        "o7-candidate-b-reviewer",
        "o7-candidate-b-reviewer-token",
    )
    .await;
    assert_ne!(assignment_b, assignment_a);
    let result_b = owner
        .store
        .call(
            reviewer_b,
            "review.submit".into(),
            passing_review_result_request(&submission_b, &assignment_b, "review-o7-candidate-b"),
        )
        .await
        .unwrap();
    assert_eq!(result_b["verdict"], "pass");
    assert_eq!(result_b["coverage"], "complete");
    assert_eq!(result_b["submission_ref"], submission_b.submission_ref);
    assert_eq!(result_b["candidate_ref"], candidate_b);

    let old_review = owner
        .store
        .call(
            task_owner.clone(),
            "review.get".into(),
            json!({"review_assignment_id":assignment_a}),
        )
        .await
        .unwrap();
    assert_eq!(old_review["result"]["verdict"], "changes_requested");
    assert_eq!(
        old_review["assignment"]["identity"]["candidate_ref"],
        candidate_a
    );
    assert_eq!(old_review["current_candidate"], false);
    let new_review = owner
        .store
        .call(
            task_owner.clone(),
            "review.get".into(),
            json!({"review_assignment_id":assignment_b}),
        )
        .await
        .unwrap();
    assert_eq!(new_review["result"]["verdict"], "pass");
    assert_eq!(
        new_review["assignment"]["identity"]["submission_ref"],
        submission_b.submission_ref
    );
    assert_eq!(
        new_review["assignment"]["identity"]["candidate_ref"],
        candidate_b
    );
    assert_eq!(new_review["current_candidate"], true);

    let stale_acceptance = owner
        .store
        .call(
            current_gm.clone(),
            "task.accept".into(),
            acceptance_request(&submission_a, "accept-stale-o7-candidate-a"),
        )
        .await
        .unwrap_err();
    assert_eq!(stale_acceptance.code, "STALE_SUBMISSION");
    let before_acceptance = owner
        .store
        .call(
            current_gm.clone(),
            "task.get".into(),
            json!({"task_id":task_id}),
        )
        .await
        .unwrap();
    assert_eq!(before_acceptance["state"], "open");
    assert!(before_acceptance["accepted_operation_id"].is_null());

    let acceptance_receipt = owner
        .store
        .call(
            current_gm.clone(),
            "task.accept".into(),
            acceptance_request(&submission_b, "accept-o7-candidate-b"),
        )
        .await
        .unwrap();
    let acceptance_id = acceptance_receipt["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let acceptance = owner
        .store
        .call(
            current_gm.clone(),
            "operation.get".into(),
            json!({"operation_id":acceptance_id}),
        )
        .await
        .unwrap();
    assert_eq!(acceptance["method"], "task.accept");
    assert_eq!(acceptance["state"], "settled");
    assert_eq!(acceptance["result"]["outcome"], "applied");
    assert_eq!(acceptance["result"]["attempt_id"], attempt_id);
    assert_eq!(
        acceptance["result"]["submission_ref"],
        submission_b.submission_ref
    );
    assert_eq!(acceptance["result"]["candidate_ref"], candidate_b);
    let accepted_task = owner
        .store
        .call(current_gm, "task.get".into(), json!({"task_id":task_id}))
        .await
        .unwrap();
    assert_eq!(accepted_task["accepted_operation_id"], acceptance_id);
    assert_eq!(accepted_task["accepted_attempt_id"], attempt_id);
    assert_eq!(accepted_task["accepted_candidate_ref"], candidate_b);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
