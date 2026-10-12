//! Public Store consumer fixtures for the frozen submission disposition repair.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};

async fn assigned_changes_requested(
    label: &str,
    suffix: &str,
    reviewer_id: &str,
    reviewer_token: &str,
) -> (
    StoreOwner,
    PathBuf,
    ReviewSubject,
    Principal,
    String,
    String,
) {
    let (owner, directory, _) = start_store(label).await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        suffix,
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let manager = principal("review-owner-v2", Role::Manager);
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        manager.clone(),
        &subject,
        reviewer_id,
        reviewer_token,
    )
    .await;
    let review = owner
        .store
        .call(
            reviewer,
            "review.submit".into(),
            review_result_request(
                &subject,
                &assignment_id,
                &format!("review-result-{suffix}"),
            ),
        )
        .await
        .unwrap();
    assert_eq!(review["verdict"], "changes_requested");
    let result_operation_id = review["operation_id"].as_str().unwrap().to_owned();
    (
        owner,
        directory,
        subject,
        manager,
        assignment_id,
        result_operation_id,
    )
}

async fn operation_snapshot(store: &Store, operation_id: &str) -> (String, String, String) {
    let operation_id = operation_id.to_owned();
    store
        .run(move |db| {
            db.query_row(
                "SELECT original_request_json,effective_request_json,result_json \
                 FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .map_err(Into::into)
        })
        .await
        .unwrap()
}

async fn assert_retained_disposition_mismatch(
    field: &str,
    replacement: Value,
    suffix: &str,
) {
    let reviewer_id = format!("assigned-reviewer-disposition-{suffix}");
    let reviewer_token = format!("reviewer-disposition-token-{suffix}");
    let label = format!("disposition-mismatch-{suffix}");
    let (owner, directory, subject, manager, assignment_id, result_operation_id) =
        assigned_changes_requested(
            &label,
            &label,
            &reviewer_id,
            &reviewer_token,
        )
        .await;

    let initial = owner
        .store
        .call(
            manager.clone(),
            "task.request_changes".into(),
            request_changes_v2(
                &subject,
                &assignment_id,
                &result_operation_id,
                &format!("initial-disposition-{suffix}"),
                &["missing-r1-evidence"],
            ),
        )
        .await
        .unwrap();
    assert_eq!(initial["applied"], true);

    let disposition_key = format!("disposition:{assignment_id}");
    let field_for_update = field.to_owned();
    let changed_disposition = owner
        .store
        .run({
            let replacement = replacement.clone();
            let disposition_key = disposition_key.clone();
            move |db| {
                let raw: String = db.query_row(
                    "SELECT payload_json FROM observations \
                     WHERE source_stream_id='controller:review' \
                       AND source_event_key=?1 AND kind='review.disposition'",
                    [&disposition_key],
                    |row| row.get(0),
                )?;
                let mut disposition: Value = serde_json::from_str(&raw)?;
                disposition[field_for_update.as_str()] = replacement;
                db.execute(
                    "UPDATE observations SET payload_json=?2 \
                     WHERE source_stream_id='controller:review' \
                       AND source_event_key=?1 AND kind='review.disposition'",
                    params![disposition_key, model::canonical(&disposition)?],
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    assert_eq!(changed_disposition, 1, "retained {field} fixture row");

    // A fresh request ID bypasses the immutable outer receipt and reaches
    // the public consumer's semantic comparison of the retained disposition.
    let rejected = owner
        .store
        .call(
            manager,
            "task.request_changes".into(),
            request_changes_v2(
                &subject,
                &assignment_id,
                &result_operation_id,
                &format!("fresh-disposition-{suffix}"),
                &["missing-r1-evidence"],
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(rejected.code, "REVIEW_DISPOSITION_CONFLICT", "retained {field}");

    let deliveries = owner
        .store
        .run({
            let task_id = subject.task_id.clone();
            move |db| {
                db.query_row(
                    "SELECT COUNT(*) FROM observations WHERE kind='task.feedback' \
                     AND json_extract(payload_json,'$.task_id')=?1",
                    [&task_id],
                    |row| row.get::<_, i64>(0),
                )
                .map_err(Into::into)
            }
        })
        .await
        .unwrap();
    assert_eq!(deliveries, 1, "rejected replay must not create a delivery");

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn task_request_changes_rejects_retained_review_disposition_reason_or_evidence_mismatch() {
    assert_retained_disposition_mismatch(
        "reason",
        json!("tampered retained reason with otherwise valid schema"),
        "reason",
    )
    .await;
    assert_retained_disposition_mismatch(
        "evidence_refs",
        json!(["evidence://tampered-retained-reference"]),
        "evidence",
    )
    .await;
}

#[tokio::test]
async fn task_request_changes_coalesced_fresh_request_keeps_new_operation_and_original_message_link() {
    let (owner, directory, subject, manager, assignment_id, result_operation_id) =
        assigned_changes_requested(
            "fresh-coalesced-operation",
            "fresh-coalesced-operation",
            "assigned-reviewer-fresh-coalesced",
            "reviewer-fresh-coalesced-token",
        )
        .await;

    let original = owner
        .store
        .call(
            manager.clone(),
            "task.request_changes".into(),
            request_changes_v2(
                &subject,
                &assignment_id,
                &result_operation_id,
                "first-coalesced-delivery",
                &["missing-r1-evidence"],
            ),
        )
        .await
        .unwrap();
    let original_operation_id = original["operation_id"].as_str().unwrap().to_owned();
    let original_message_id = original["message_id"].as_str().unwrap().to_owned();
    assert_eq!(original_message_id, original_operation_id);
    let original_snapshot = operation_snapshot(&owner.store, &original_operation_id).await;

    let current = owner
        .store
        .call(
            manager.clone(),
            "task.request_changes".into(),
            request_changes_v2(
                &subject,
                &assignment_id,
                &result_operation_id,
                "fresh-coalesced-request",
                &["missing-r1-evidence"],
            ),
        )
        .await
        .unwrap();
    let current_operation_id = current["operation_id"].as_str().unwrap().to_owned();
    assert_ne!(current_operation_id, original_operation_id);
    assert_eq!(current["coalesced"], true);
    assert_eq!(current["coalesced_from_operation_id"], original_operation_id);
    assert_eq!(current["message_id"], original_message_id);

    let receipt = owner
        .store
        .call(
            manager,
            "operation.get".into(),
            json!({"operation_id":current_operation_id}),
        )
        .await
        .unwrap();
    assert_eq!(receipt["operation_id"], current_operation_id);
    assert_eq!(receipt["result"]["operation_id"], current_operation_id);
    assert_eq!(receipt["result"]["message_id"], original_message_id);
    assert_eq!(
        receipt["result"]["coalesced_from_operation_id"],
        original_operation_id
    );

    let current_snapshot = operation_snapshot(&owner.store, &current_operation_id).await;
    let current_result: Value = serde_json::from_str(&current_snapshot.2).unwrap();
    assert_eq!(current_result["operation_id"], current_operation_id);
    assert_eq!(current_result["message_id"], original_message_id);
    assert_eq!(
        current_result["coalesced_from_operation_id"],
        original_operation_id
    );
    assert_eq!(
        operation_snapshot(&owner.store, &original_operation_id).await,
        original_snapshot,
        "coalescing cannot rewrite the original Operation receipt"
    );

    let (delivery_count, disposition_feedback_id) = owner
        .store
        .run({
            let task_id = subject.task_id.clone();
            let disposition_key = format!("disposition:{assignment_id}");
            move |db| {
                let delivery_count: i64 = db.query_row(
                    "SELECT COUNT(*) FROM observations WHERE kind='task.feedback' \
                     AND json_extract(payload_json,'$.task_id')=?1",
                    [&task_id],
                    |row| row.get(0),
                )?;
                let disposition_feedback_id: String = db.query_row(
                    "SELECT json_extract(payload_json,'$.task_feedback_operation_id') \
                     FROM observations WHERE source_stream_id='controller:review' \
                       AND source_event_key=?1 AND kind='review.disposition'",
                    [&disposition_key],
                    |row| row.get(0),
                )?;
                Ok((delivery_count, disposition_feedback_id))
            }
        })
        .await
        .unwrap();
    assert_eq!(delivery_count, 1);
    assert_eq!(disposition_feedback_id, original_operation_id);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn make_seeded_submission_target_valid_for_recovery(
    store: &Store,
    subject: &ReviewSubject,
    suffix: &str,
) -> (String, usize) {
    let attempt_id = subject.attempt_id.clone();
    let operation_id = format!("submit-op-{suffix}");
    let request_id = format!("submit-request-{suffix}");
    let changed = store
        .run({
            let operation_id = operation_id.clone();
            let request_id = request_id.clone();
            move |db| {
                let attempt = tasks::get_attempt(db, &attempt_id)?;
                let request = json!({
                    "client_request_id":request_id,
                    "attempt_id":attempt_id,
                    "expected_revision":1,
                    "expected_submission_ref":null,
                    "candidate_ref":attempt["candidate_ref"],
                    "summary":"fixture submission",
                    "claims":[]
                });
                let input = crate::submission::SubmitRequest::parse(&request)?;
                let spec: crate::model::TaskSpec =
                    serde_json::from_value(attempt["task_snapshot"]["spec"].clone())?;
                let claims = input.normalized_claims(&spec)?;
                let candidate_metadata_raw: String = db.query_row(
                    "SELECT metadata_json FROM artifacts WHERE artifact_id=?1",
                    [&input.candidate_ref],
                    |row| row.get(0),
                )?;
                let mut candidate_metadata: Value =
                    serde_json::from_str(&candidate_metadata_raw)?;
                candidate_metadata["task_id"] = attempt["task_id"].clone();
                db.execute(
                    "UPDATE artifacts SET metadata_json=?2 WHERE artifact_id=?1",
                    params![input.candidate_ref, model::canonical(&candidate_metadata)?],
                )?;
                let (candidate_kind, candidate_length, candidate_digest): (String, i64, String) =
                    db.query_row(
                        "SELECT kind,byte_length,content_digest FROM artifacts WHERE artifact_id=?1",
                        [&input.candidate_ref],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
                    )?;
                let caller_id: String = db.query_row(
                    "SELECT caller_id FROM operations WHERE operation_id=?1 AND method='task.submit'",
                    [&operation_id],
                    |row| row.get(0),
                )?;
                let document = json!({
                    "schema_version":1,
                    "operation_id":operation_id,
                    "task_id":attempt["task_id"],
                    "attempt_id":input.attempt_id,
                    "task_revision":input.expected_revision,
                    "phase":spec.phase,
                    "owner_id":attempt["owner_id"],
                    "submitted_by":caller_id,
                    "previous_submission_ref":input.expected_submission_ref,
                    "candidate_ref":input.candidate_ref,
                    "candidate_sha256":candidate_digest,
                    "candidate_kind":candidate_kind,
                    "candidate_byte_length":u64::try_from(candidate_length).unwrap(),
                    "summary":input.summary,
                    "claims":claims,
                    "claim_counts":crate::submission::claim_counts(&claims),
                    "evidence_level":"submitter_report",
                    "source_checkout_verified":false
                });
                let changed = db.execute(
                    "UPDATE operations SET original_request_json=?2,effective_request_json=?3 \
                     WHERE operation_id=?1 AND method='task.submit' AND client_request_id=?4",
                    params![
                        operation_id,
                        model::canonical(&request)?,
                        model::canonical(&json!({"submission_document":document}))?,
                        request_id,
                    ],
                )?;
                Ok(changed)
            }
        })
        .await
        .unwrap();
    (operation_id, changed)
}

#[tokio::test]
async fn task_submit_recover_same_settled_request_replays_historical_result_but_fresh_request_rechecks_authority() {
    let (owner, directory, bootstrap) = start_store("settled-recovery-replay").await;
    seed_clients(&owner.store).await;
    let suffix = "settled-recovery-replay";
    let subject = seed_subject(
        &owner.store,
        suffix,
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let (target_operation_id, changed) =
        make_seeded_submission_target_valid_for_recovery(&owner.store, &subject, suffix).await;
    assert_eq!(changed, 1, "prepare exact retained settled submission");

    let former_gm = principal("review-gm", Role::Manager);
    let exact_request = json!({
        "client_request_id":"settled-recovery-exact-request",
        "operation_id":target_operation_id
    });
    let first = owner
        .store
        .call(
            former_gm.clone(),
            "task.submit.recover".into(),
            exact_request.clone(),
        )
        .await
        .unwrap();
    assert_eq!(first["outcome"], "already_settled");
    assert_eq!(first["target_operation_id"], target_operation_id);
    assert_eq!(first["target_result"]["outcome"], "applied");

    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    owner
        .store
        .call(
            operator,
            "gm.handover".into(),
            json!({
                "client_request_id":"settled-recovery-transfer-gm",
                "client_id":"review-owner-v2"
            }),
        )
        .await
        .unwrap();

    let historical_replay = owner
        .store
        .call(
            former_gm.clone(),
            "task.submit.recover".into(),
            exact_request,
        )
        .await
        .unwrap();
    assert_eq!(historical_replay, first);

    let fresh_former_request = owner
        .store
        .call(
            former_gm,
            "task.submit.recover".into(),
            json!({
                "client_request_id":"settled-recovery-fresh-retired-gm",
                "operation_id":target_operation_id
            }),
        )
        .await
        .unwrap_err();
    assert_eq!(fresh_former_request.code, "FORBIDDEN");

    let current_gm = principal("review-owner-v2", Role::Manager);
    let fresh_current_request = owner
        .store
        .call(
            current_gm,
            "task.submit.recover".into(),
            json!({
                "client_request_id":"settled-recovery-fresh-current-gm",
                "operation_id":target_operation_id
            }),
        )
        .await
        .unwrap();
    assert_eq!(fresh_current_request["outcome"], "already_settled");
    assert_eq!(fresh_current_request["target_operation_id"], target_operation_id);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
