//! One real Store path for exact, passive submission-review notices.
use super::*;

#[tokio::test]
async fn exact_submission_review_watch_recovers_one_inbox_notice_without_work() {
    let (mut owner, directory, bootstrap) = start_store("submission-reviewed-watch").await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "submission-reviewed-watch",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let manager = principal("review-owner-v2", Role::Manager);
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        &owner.store,
        manager,
        &subject,
        "submission-watch-reviewer",
        "submission-watch-reviewer-token",
    )
    .await;
    let address = json!({
        "submission_ref":subject.submission_ref.clone(),
        "candidate_ref":subject.candidate_ref.clone(),
    });
    let expires_at_ms = model::now_ms().unwrap() + 60_000;

    let denied = owner
        .store
        .call(
            principal("unrelated-manager", Role::Manager),
            "coordination.watch.create".into(),
            json!({
                "client_request_id":"foreign-submission-review-watch",
                "task_id":subject.task_id.clone(),
                "task_revision":1,
                "attempt_id":subject.attempt_id.clone(),
                "watch_kind":"submission_reviewed",
                "address":address.clone(),
                "expires_at_ms":expires_at_ms,
                "delivery":"mailbox_header",
                "one_shot":true,
            }),
        )
        .await;
    assert!(denied.is_err());

    let first = owner
        .store
        .call(
            reviewer.clone(),
            "coordination.watch.create".into(),
            json!({
                "client_request_id":"submission-review-watch-first",
                "watch_kind":"submission_reviewed",
                "address":address.clone(),
                "expires_at_ms":expires_at_ms,
                "delivery":"mailbox_header",
                "one_shot":true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(first["state"], "active");
    assert_eq!(first["target_state_at_create"]["state"], "awaiting_review");

    let result = owner
        .store
        .call(
            reviewer.clone(),
            "review.submit".into(),
            review_result_request(&subject, &assignment_id, "submission-watch-review-result"),
        )
        .await
        .unwrap();
    assert_eq!(result["verdict"], "changes_requested");
    assert_eq!(result["task_transition"], "none");

    owner.close().await.unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let mut config = Config::default();
    config.storage.data_dir = root.path.clone();
    owner = StoreOwner::start(root, Arc::new(config), bootstrap)
        .await
        .unwrap();

    // The shared reconcile tick recovers the retained review fact after Store restart.
    let reconcile = owner.store.reconcile_watches_once().await.unwrap();
    assert_eq!(reconcile["matched"], 1);
    assert_eq!(reconcile["model_wake"], false);
    assert_eq!(reconcile["native_work_queued"], false);

    let watch_page = owner
        .store
        .call(
            reviewer.clone(),
            "coordination.watch.list".into(),
            json!({
                "limit":20,
            }),
        )
        .await
        .unwrap();
    assert_eq!(watch_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(watch_page["items"][0]["state"], "matched");
    assert_eq!(
        watch_page["items"][0]["notification"]["facts"]["verdict"],
        "changes_requested"
    );
    assert_eq!(
        watch_page["items"][0]["notification"]["address"]["submission_ref"],
        subject.submission_ref
    );

    let repeated = owner
        .store
        .call(
            reviewer.clone(),
            "coordination.watch.create".into(),
            json!({
                "client_request_id":"submission-review-watch-repeat",
                "watch_kind":"submission_reviewed",
                "address":address.clone(),
                "expires_at_ms":expires_at_ms,
                "delivery":"mailbox_header",
                "one_shot":true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(repeated["watch_id"], first["watch_id"]);
    assert_eq!(repeated["coalesced"], true);

    let inbox = owner
        .store
        .call(reviewer, "coordination.inbox".into(), json!({"limit":20}))
        .await
        .unwrap();
    let notices = inbox["watch_notifications"]["items"].as_array().unwrap();
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0]["watch_id"], first["watch_id"]);
    assert_eq!(notices[0]["facts"]["review_assignment_id"], assignment_id);

    let effect_counts = owner
        .store
        .run(|db| {
            let open_operations: i64 = db.query_row(
                "SELECT COUNT(*) FROM operations WHERE state IN ('queued','sending','native_accepted','outcome_unknown')",
                [],
                |row| row.get(0),
            )?;
            let review_results: i64 = db.query_row(
                "SELECT COUNT(*) FROM observations WHERE source_stream_id='controller:review' AND kind='review.result'",
                [],
                |row| row.get(0),
            )?;
            Ok((open_operations, review_results))
        })
        .await
        .unwrap();
    assert_eq!(effect_counts, (0, 1));

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn exact_historical_submission_review_notifies_owner_manager_after_release() {
    let (owner, directory, bootstrap) = start_store("historical-submission-reviewed-watch").await;
    seed_clients(&owner.store).await;
    let operator = owner.store.authenticate(bootstrap).await.unwrap();
    assert_eq!(operator.role, Role::Operator);
    let subject = seed_subject(
        &owner.store,
        "historical-submission-reviewed-watch",
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
        "historical-watch-reviewer",
        "historical-watch-reviewer-token",
    )
    .await;
    let address = json!({
        "submission_ref":subject.submission_ref.clone(),
        "candidate_ref":subject.candidate_ref.clone(),
    });
    let expires_at_ms = model::now_ms().unwrap() + 60_000;

    let watch = owner
        .store
        .call(
            manager.clone(),
            "coordination.watch.create".into(),
            json!({
                "client_request_id":"historical-review-watch-manager",
                "task_id":subject.task_id.clone(),
                "task_revision":1,
                "attempt_id":subject.attempt_id.clone(),
                "watch_kind":"submission_reviewed",
                "address":address,
                "expires_at_ms":expires_at_ms,
                "delivery":"mailbox_header",
                "one_shot":true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(watch["state"], "active");
    assert_eq!(watch["target_state_at_create"]["state"], "awaiting_review");

    let released = owner
        .store
        .call(
            manager.clone(),
            "attempt.release".into(),
            json!({
                "client_request_id":"release-before-late-review",
                "attempt_id":subject.attempt_id.clone(),
                "outcome":"superseded",
                "reason":"The owner released this Attempt before its assigned review result arrived.",
                "assignment_closed":true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(released["released"], true);
    assert_eq!(released["outcome"], "superseded");

    let revised = owner
        .store
        .call(
            operator,
            "task.revise".into(),
            json!({
                "client_request_id":"revise-after-watch-release",
                "task_id":subject.task_id.clone(),
                "expected_revision":1,
                "spec":{
                    "objective":"Keep the released review subject historical.",
                    "phase":"implementation",
                    "requirements":[{"id":"R1","statement":"Preserve the exact retained review subject"}],
                    "owner_policy_id":crate::policy::OWNER_POLICY_V2_ID,
                },
            }),
        )
        .await
        .unwrap();
    assert_eq!(revised["revision"], 2);

    let result = owner
        .store
        .call(
            reviewer,
            "review.submit".into(),
            review_result_request(&subject, &assignment_id, "late-review-after-watch-release"),
        )
        .await
        .unwrap();
    assert_eq!(result["applicability"], "historical_candidate");
    assert_eq!(result["task_transition"], "none");

    let reconcile = owner.store.reconcile_watches_once().await.unwrap();
    assert_eq!(reconcile["matched"], 1);
    assert_eq!(reconcile["model_wake"], false);
    assert_eq!(reconcile["native_work_queued"], false);

    // The owner Manager remains authorized for the retained exact Attempt and
    // can read the notice after release even though it is no longer current.
    let page = owner
        .store
        .call(
            manager,
            "coordination.watch.list".into(),
            json!({
                "task_id":subject.task_id.clone(),
                "task_revision":1,
                "attempt_id":subject.attempt_id.clone(),
                "limit":20,
            }),
        )
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["items"][0]["watch_id"], watch["watch_id"]);
    assert_eq!(page["items"][0]["state"], "matched");
    assert_eq!(
        page["items"][0]["notification"]["facts"]["review_assignment_id"],
        assignment_id
    );
    assert_eq!(
        page["items"][0]["notification"]["facts"]["verdict"],
        "changes_requested"
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
