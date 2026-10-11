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

#[tokio::test]
async fn coordination_inbox_resumes_damaged_window_and_preserves_byte_excluded_delivery() {
    const MESSAGE_COUNT: usize = 45;
    const DAMAGED_WINDOW: usize = 36;

    let (owner, directory, _bootstrap) = start_store("inbox-bounded-recovery").await;
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "inbox-bounded-recovery",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let manager = principal("review-owner-v2", Role::Manager);
    let sender_id = "inbox-bounded-sender";
    let recipient_id = "inbox-bounded-recipient";
    let sender_token = format!("{}{}", model::new_id(), model::new_id());
    let recipient_token = format!("{}{}", model::new_id(), model::new_id());

    for (index, client_id, token) in [
        ("sender", sender_id, sender_token.as_str()),
        ("recipient", recipient_id, recipient_token.as_str()),
    ] {
        owner
            .store
            .call(
                manager.clone(),
                "coordination.participant.register".into(),
                json!({
                    "client_request_id":format!("register-inbox-{index}"),
                    "client_id":client_id,
                    "token_hash":model::digest(token.as_bytes()),
                    "task_id":subject.task_id.clone(),
                    "task_revision":1,
                    "attempt_id":subject.attempt_id.clone(),
                    "participation_basis":{"kind":"attempt_owner"},
                }),
            )
            .await
            .unwrap();
    }
    let sender = owner
        .store
        .authenticate(Credential {
            client_id: sender_id.to_owned(),
            token: sender_token,
        })
        .await
        .unwrap();
    let recipient = owner
        .store
        .authenticate(Credential {
            client_id: recipient_id.to_owned(),
            token: recipient_token,
        })
        .await
        .unwrap();

    let mut operation_ids = Vec::with_capacity(MESSAGE_COUNT);
    for index in 0..MESSAGE_COUNT {
        let sent = owner
            .store
            .call(
                sender.clone(),
                "coordination.send".into(),
                json!({
                    "client_request_id":format!("inbox-message-{index}"),
                    "recipient":recipient_id,
                    "body":{"payload":"x".repeat(7_000)},
                }),
            )
            .await
            .unwrap();
        operation_ids.push(sent["operation_id"].as_str().unwrap().to_owned());
    }

    let scope_id = crate::coordination::scope_id(&subject.task_id, 1, &subject.attempt_id).unwrap();
    let prefix = crate::coordination::mailbox_prefix(&scope_id, recipient_id);
    let upper = format!("{prefix}z");
    let query_prefix = prefix.clone();
    let index_rows = owner
        .store
        .run(move |db| {
            let mut statement = db
                .prepare("SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key")?;
            let rows = statement
                .query_map(rusqlite::params![query_prefix, upper], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
        .unwrap();
    assert_eq!(index_rows.len(), MESSAGE_COUNT);

    // Corrupt valid JSON index payloads and remove the final row's derived
    // read-position reference. The canonical scoped key remains available as
    // an independently checked resume point.
    let boundary_operation_id = serde_json::from_str::<Value>(&index_rows[DAMAGED_WINDOW - 1].1)
        .unwrap()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let marker = owner
        .store
        .run({
            let damaged_rows = index_rows[..DAMAGED_WINDOW].to_vec();
            let boundary_operation_id = boundary_operation_id.clone();
            move |db| {
                let marker = db.query_row(
                    "SELECT observation_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms \
                     FROM observations WHERE source_stream_id='controller:read-position:operation' \
                       AND source_event_key=?1",
                    [&boundary_operation_id],
                    |row| {
                        Ok((
                            row.get::<_, i64>(0)?,
                            row.get::<_, String>(1)?,
                            row.get::<_, Option<String>>(2)?,
                            row.get::<_, String>(3)?,
                            row.get::<_, String>(4)?,
                            row.get::<_, i64>(5)?,
                        ))
                    },
                )?;
                let damaged_json = model::canonical(&json!({"damaged":true}))?;
                for (key, _) in damaged_rows {
                    db.execute(
                        "UPDATE meta SET value_json=?2 WHERE key=?1",
                        rusqlite::params![key, damaged_json],
                    )?;
                }
                let deleted = db.execute(
                    "DELETE FROM observations WHERE source_stream_id='controller:read-position:operation' \
                       AND source_event_key=?1",
                    [&boundary_operation_id],
                )?;
                assert_eq!(deleted, 1);
                Ok(marker)
            }
        })
        .await
        .unwrap();

    // The resume request reloads the participant's current registration and
    // exact Task/Attempt scope before resolving either cursor form.
    let recipient_meta_key = format!("client:{recipient_id}");
    let recipient_registration = owner
        .store
        .run({
            let recipient_meta_key = recipient_meta_key.clone();
            move |db| {
                let mut registration = meta(db, &recipient_meta_key)?.unwrap();
                let prior = registration.clone();
                registration["disabled"] = json!(true);
                set_meta(db, &recipient_meta_key, &registration)?;
                Ok(prior)
            }
        })
        .await
        .unwrap();
    let denied_resume = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1,"after_key":index_rows[DAMAGED_WINDOW - 1].0.clone()}),
        )
        .await;
    assert!(denied_resume.is_err());
    owner
        .store
        .run({
            let recipient_meta_key = recipient_meta_key.clone();
            move |db| set_meta(db, &recipient_meta_key, &recipient_registration)
        })
        .await
        .unwrap();

    let empty_gap = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert!(empty_gap["items"].as_array().unwrap().is_empty());
    assert_eq!(empty_gap["coverage"], "partial");
    assert_eq!(empty_gap["next_after"], Value::Null);
    assert_eq!(
        empty_gap["next_after_key"],
        index_rows[DAMAGED_WINDOW - 1].0
    );
    assert!(
        empty_gap["gaps"].as_array().unwrap().iter().any(|gap| {
            gap["kind"] == "damaged_mailbox_indexes" && gap["count"] == DAMAGED_WINDOW
        })
    );
    assert!(
        empty_gap["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap["kind"] == "inbox_scan_bound")
    );

    let resumed = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1,"after_key":empty_gap["next_after_key"]}),
        )
        .await
        .unwrap();
    assert_eq!(
        resumed["items"][0]["operation_id"],
        operation_ids[DAMAGED_WINDOW]
    );

    owner
        .store
        .run({
            let original_rows = index_rows[..DAMAGED_WINDOW].to_vec();
            let marker = marker.clone();
            move |db| {
                for (key, value_json) in original_rows {
                    db.execute(
                        "UPDATE meta SET value_json=?2 WHERE key=?1",
                        rusqlite::params![key, value_json],
                    )?;
                }
                db.execute(
                    "INSERT INTO observations(observation_id,source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
                     VALUES(?1,'controller:read-position:operation',?2,?3,?4,?5,?6)",
                    rusqlite::params![marker.0, marker.1, marker.2, marker.3, marker.4, marker.5],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();

    let byte_page = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":50}),
        )
        .await
        .unwrap();
    let byte_items = byte_page["items"].as_array().unwrap();
    assert!(!byte_items.is_empty());
    assert!(byte_items.len() < MESSAGE_COUNT);
    let serialized_items_bytes = model::canonical(&byte_page["items"]).unwrap().len();
    assert!(serialized_items_bytes <= super::super::projection::MAX_SERIALIZED_BYTES);
    assert_eq!(
        byte_page["next_after"],
        byte_items.last().unwrap()["operation_id"]
    );
    assert!(
        byte_page["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|gap| gap["kind"] == "inbox_byte_budget")
    );
    let excluded_index = byte_items.len();
    let next_page = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":50,"after_operation_id":byte_page["next_after"]}),
        )
        .await
        .unwrap();
    assert_eq!(
        next_page["items"][0]["operation_id"],
        operation_ids[excluded_index]
    );

    // A broken authoritative Operation receipt remains a hard caller error,
    // even though the surrounding mailbox index is otherwise valid.
    let first_operation = operation_ids[0].clone();
    let original_result = owner
        .store
        .run({
            let first_operation = first_operation.clone();
            move |db| {
                let original: String = db.query_row(
                    "SELECT result_json FROM operations WHERE operation_id=?1",
                    [&first_operation],
                    |row| row.get(0),
                )?;
                db.execute(
                    "UPDATE operations SET result_json='{}' WHERE operation_id=?1",
                    [&first_operation],
                )?;
                Ok(original)
            }
        })
        .await
        .unwrap();
    let body_failure = owner
        .store
        .call(recipient, "coordination.inbox".into(), json!({"limit":1}))
        .await;
    assert!(body_failure.is_err());
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE operations SET result_json=?2 WHERE operation_id=?1",
                rusqlite::params![first_operation, original_result],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
