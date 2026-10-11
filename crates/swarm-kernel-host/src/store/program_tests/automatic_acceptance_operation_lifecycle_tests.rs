//! Connected automatic-acceptance lifecycle coverage through the real review-result consumer.
//!
//! The fixture creates one assigned passing review and calls
//! consume_review_result_for_entry directly. It then replays that same consumer
//! against retained lifecycle variants for the one semantic task.accept request.

use super::*;

async fn submit_canonical_passing_review(
    store: &Store,
    manager: Principal,
    subject: &ReviewSubject,
    suffix: &str,
    request_id: &str,
) -> (Principal, String, String) {
    let (reviewer, assignment_id) = assign_sponsored_reviewer(
        store,
        manager,
        subject,
        &format!("{suffix}-reviewer"),
        &format!("{suffix}-token"),
    )
    .await;
    let result = store
        .call(
            reviewer.clone(),
            "review.submit".into(),
            passing_review_result_request(subject, &assignment_id, request_id),
        )
        .await
        .unwrap();
    let result_operation_id = result["operation_id"]
        .as_str()
        .expect("review.submit retains an Operation id")
        .to_owned();
    (reviewer, assignment_id, result_operation_id)
}

async fn consume_configured_review_result(
    store: &Store,
    assignment_id: &str,
    result_operation_id: &str,
) -> Result<Value> {
    let assignment_id = assignment_id.to_owned();
    let result_operation_id = result_operation_id.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let entry = crate::automation::config::load_entry(
                &tx,
                "review-gm",
                "fixture",
                "cross-owner-acceptance",
            )?
            .expect("configured automatic-acceptance entry remains retained");
            let projection = crate::store::automation_acceptance::consume_review_result_for_entry(
                &tx,
                &entry,
                &assignment_id,
                &result_operation_id,
                model::now_ms()?,
            )?;
            tx.commit()?;
            Ok(projection)
        })
        .await
}

async fn acceptance_rows(
    store: &Store,
    subject: &ReviewSubject,
) -> Vec<(String, String, String, String, String, Option<String>)> {
    let task_id = subject.task_id.clone();
    store
        .run(move |db| {
            let mut statement = db.prepare(
                "SELECT operation_id,state,client_request_id,original_request_json,                         effective_request_json,result_json                  FROM operations WHERE method='task.accept' AND task_id=?1                  ORDER BY operation_id",
            )?;
            let rows = statement
                .query_map([&task_id], |row| {
                    Ok((
                        row.get(0)?,
                        row.get(1)?,
                        row.get(2)?,
                        row.get(3)?,
                        row.get(4)?,
                        row.get(5)?,
                    ))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;
            Ok(rows)
        })
        .await
        .unwrap()
}

async fn subject_states(store: &Store, subject: &ReviewSubject) -> (String, String) {
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();
    store
        .run(move |db| {
            let states = db.query_row(
                "SELECT t.state,a.state FROM tasks t JOIN attempts a ON a.task_id=t.task_id \
                 WHERE t.task_id=?1 AND a.attempt_id=?2",
                params![task_id, attempt_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )?;
            Ok(states)
        })
        .await
        .unwrap()
}

async fn retained_disposition_state(store: &Store, key: &str) -> Value {
    let key = key.to_owned();
    store
        .run(move |db| {
            crate::automation::config::read_record(db, &key, "fixture review disposition")?
                .ok_or_else(|| Error::invalid("fixture review disposition disappeared"))
        })
        .await
        .unwrap()
}

/// Replace only the retained Operation lifecycle and receipt fixture. The
/// semantic original request and on-behalf linkage remain the actual consumer
/// writes; the Task row is never reset or rewritten.
async fn rewrite_retained_acceptance_lifecycle(
    store: &Store,
    operation_id: &str,
    state: &str,
    receipt: Value,
) {
    let operation_id = operation_id.to_owned();
    let state = state.to_owned();
    store
        .run(move |db| {
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            let raw_effective: String = tx.query_row(
                "SELECT effective_request_json FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| row.get(0),
            )?;
            let effective: Value = serde_json::from_str(&raw_effective)?;
            let now_ms = model::now_ms()?;
            let terminal = matches!(state.as_str(), "settled" | "rejected" | "cancelled");
            let sent = matches!(state.as_str(), "sending" | "native_accepted" | "outcome_unknown" | "settled");
            let result_json = model::canonical(&receipt)?;
            let effective_json = model::canonical(&effective)?;
            tx.execute(
                "UPDATE operations SET state=?2,result_json=?3,settled_at_ms=?4,                         updated_at_ms=?5,effective_request_json=?6,sent_at_ms=?7                  WHERE operation_id=?1",
                params![
                    operation_id,
                    state,
                    result_json,
                    terminal.then_some(now_ms),
                    now_ms,
                    effective_json,
                    sent.then_some(now_ms)
                ],
            )?;
            tx.commit()?;
            Ok(())
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn actual_review_consumer_reuses_retained_acceptance_lifecycle_receipt() {
    let (owner, directory, bootstrap_credential) =
        start_store("acceptance-lifecycle-consumer").await;
    let operator = owner
        .store
        .authenticate(bootstrap_credential)
        .await
        .unwrap();
    seed_clients(&owner.store).await;
    let subject = seed_subject(
        &owner.store,
        "acceptance-lifecycle-consumer",
        "review-owner-v2",
        crate::policy::OWNER_POLICY_V2_ID,
        false,
    )
    .await;
    let candidate_ref = subject.candidate_ref.clone();
    let submission_ref = subject.submission_ref.clone();
    let (candidate, submission, submission_bytes) = owner
        .store
        .run(move |db| {
            Ok((
                results::get(db, &candidate_ref)?,
                results::get(db, &submission_ref)?,
                model::canonical(&submissions::document(db, &submission_ref)?)?.into_bytes(),
            ))
        })
        .await
        .unwrap();
    owner
        .store
        .file_io(move |files| {
            files.publish(&candidate, b"explicit source snapshot fixture")?;
            files.publish(&submission, &submission_bytes)?;
            Ok(())
        })
        .await
        .unwrap();
    configure_acceptance_fixture(&owner.store, "review-gm", std::slice::from_ref(&subject)).await;

    // The submitted Attempt owner sponsors the independent review; the current
    // GM consumes it through the configured cross-owner acceptance entry.
    let manager = principal("review-owner-v2", Role::Manager);
    let (_reviewer, assignment_id, result_operation_id) = submit_canonical_passing_review(
        &owner.store,
        manager,
        &subject,
        "acceptance-lifecycle-consumer",
        "acceptance-lifecycle-consumer-pass",
    )
    .await;

    assert_eq!(
        subject_states(&owner.store, &subject).await,
        ("open".to_owned(), "submitted".to_owned()),
        "the assigned passing review must not accept the Task before the consumer runs"
    );
    let first =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap();
    assert_eq!(first["status"], "acceptance_reserved", "{first}");
    assert_eq!(first["coalesced"], false);
    assert_eq!(first["review_assignment_id"], assignment_id);
    assert_eq!(first["review_result_operation_id"], result_operation_id);
    assert_eq!(first["task_id"], subject.task_id);
    assert_eq!(first["receipt"]["state"], "queued");
    assert_eq!(first["receipt"]["task_accepted"], false);
    assert_eq!(
        subject_states(&owner.store, &subject).await,
        ("open".to_owned(), "submitted".to_owned()),
        "the first direct consumer call must only reserve acceptance"
    );

    // Use the actual projection's retained Operation and semantic request ID.
    let operation_id = first["operation_id"]
        .as_str()
        .expect("acceptance projection names its retained Operation")
        .to_owned();
    assert_eq!(first["acceptance_operation_id"], operation_id);
    let semantic_request_id = first["semantic_request_id"]
        .as_str()
        .expect("acceptance projection names its semantic request")
        .to_owned();
    let rows = acceptance_rows(&owner.store, &subject).await;
    assert_eq!(
        rows.len(),
        1,
        "the first consumer call reserves one Operation"
    );
    let (
        retained_id,
        retained_state,
        retained_request_id,
        original_json,
        effective_json,
        result_json,
    ) = rows.into_iter().next().unwrap();
    assert_eq!(retained_id, operation_id);
    assert_eq!(retained_state, "queued");
    assert_eq!(retained_request_id, semantic_request_id);
    let original_request: Value = serde_json::from_str(&original_json).unwrap();
    let effective: Value = serde_json::from_str(&effective_json).unwrap();
    assert_eq!(original_request["client_request_id"], semantic_request_id);
    assert_eq!(effective["request"], original_request);
    let expected_feedback_cursor = original_request["expected_feedback_observation_id"]
        .as_i64()
        .expect("the retained semantic request records its feedback cursor");
    let on_behalf_link = effective["automation_on_behalf"].clone();
    let review_result_id = result_operation_id.clone();
    let review_event_key = format!("result:{assignment_id}");
    let (disposition_key, activation_state, review_observation_id) = owner
        .store
        .run(move |db| {
            let key: String = db.query_row(
                "SELECT key FROM meta WHERE key LIKE 'automation:v1:review-disposition:state:%' \
                 AND json_extract(value_json,'$.record.owner_manager_id')='review-gm' \
                 AND json_extract(value_json,'$.record.project_id')='fixture' \
                 AND json_extract(value_json,'$.record.automation_id')='cross-owner-acceptance'",
                [],
                |row| row.get(0),
            )?;
            let state = crate::automation::config::read_record(
                db,
                &key,
                "fixture review disposition activation",
            )?
            .expect("the configured fixture retains its activation state");
            let observation_id: i64 = db.query_row(
                "SELECT observation_id FROM observations WHERE source_stream_id='controller:review' \
                 AND kind='review.result' AND source_event_key=?1 AND operation_id=?2",
                params![review_event_key, review_result_id],
                |row| row.get(0),
            )?;
            Ok((key, state, observation_id))
        })
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(result_json.as_deref().unwrap()).unwrap(),
        first["receipt"]
    );

    let make_receipt = |state: &str, outcome: &str, task_accepted: bool| {
        json!({
            "operation_id":operation_id,
            "acceptance_operation_id":operation_id,
            "state":state,
            "outcome":outcome,
            "task_accepted":task_accepted,
            "coalesced":false
        })
    };
    let foreign_operation_id = format!("foreign-{operation_id}");
    let cases = vec![
        (
            "queued-pending",
            "queued",
            make_receipt("queued", "pending", false),
            "acceptance_in_flight",
            false,
            false,
        ),
        (
            "sending-pending",
            "sending",
            make_receipt("sending", "pending", false),
            "acceptance_in_flight",
            true,
            false,
        ),
        (
            "native-accepted-pending",
            "native_accepted",
            make_receipt("native_accepted", "pending", false),
            "acceptance_in_flight",
            true,
            false,
        ),
        (
            "cancelled",
            "cancelled",
            make_receipt("cancelled", "cancelled_before_effect", false),
            "acceptance_cancelled",
            false,
            true,
        ),
        (
            "rejected",
            "rejected",
            json!({"code":"FORBIDDEN","message":"retained admission was rejected"}),
            "acceptance_rejected",
            false,
            true,
        ),
        (
            "outcome-unknown",
            "outcome_unknown",
            make_receipt("outcome_unknown", "unknown", false),
            "acceptance_readback_required",
            true,
            false,
        ),
        (
            "settled-failed-verification",
            "settled",
            json!({
                "operation_id":operation_id,
                "outcome":"failed",
                "task_accepted":false,
                "error":{"code":"ACCEPTANCE_EVIDENCE_CHANGED","message":"evidence changed during byte verification"}
            }),
            "acceptance_not_applied",
            true,
            true,
        ),
    ];
    let mut settled_nonapplied = None;
    let mut pending_disposition = None;

    for (case, state, receipt, expected_status, expected_started, terminal_not_applied) in cases {
        let receipt = if state == "cancelled" {
            rewrite_retained_acceptance_lifecycle(
                &owner.store,
                &operation_id,
                "queued",
                first["receipt"].clone(),
            )
            .await;
            owner
                .store
                .call(
                    operator.clone(),
                    "operation.cancel".into(),
                    json!({
                        "client_request_id":"cancel-exact-automatic-acceptance",
                        "operation_id":operation_id,
                        "reason":"stop queued acceptance before verification"
                    }),
                )
                .await
                .unwrap();
            let target = operation_id.clone();
            owner
                .store
                .run(move |db| {
                    Ok(super::super::operations::get_operation(db, &target)?["result"].clone())
                })
                .await
                .unwrap()
        } else {
            rewrite_retained_acceptance_lifecycle(
                &owner.store,
                &operation_id,
                state,
                receipt.clone(),
            )
            .await;
            receipt
        };

        // Same manager entry, assignment, result Operation, semantic request,
        // and retained task.accept Operation are sent through the real consumer.
        let replay =
            consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
                .await
                .unwrap();
        assert_eq!(replay["operation_id"], operation_id, "{case}");
        assert_eq!(replay["semantic_request_id"], semantic_request_id, "{case}");
        assert_eq!(replay["review_assignment_id"], assignment_id, "{case}");
        assert_eq!(
            replay["review_result_operation_id"], result_operation_id,
            "{case}"
        );
        assert_eq!(replay["receipt"]["status"], expected_status, "{case}");
        assert_eq!(replay["receipt"]["operation_state"], state, "{case}");
        assert_eq!(
            replay["receipt"]["acceptance_started"], expected_started,
            "{case}"
        );
        assert_eq!(replay["receipt"]["receipt"], receipt, "{case}");
        if terminal_not_applied {
            assert_eq!(replay["receipt"]["terminal_not_applied"], true, "{case}");
        }
        if state == "outcome_unknown" {
            assert_eq!(replay["receipt"]["readback_required"], true);
        }

        let rows = acceptance_rows(&owner.store, &subject).await;
        assert_eq!(rows.len(), 1, "retry created another Operation: {case}");
        let (
            retained_id,
            retained_state,
            retained_request_id,
            raw_original,
            raw_effective,
            raw_result,
        ) = rows.into_iter().next().unwrap();
        assert_eq!(retained_id, operation_id, "{case}");
        assert_eq!(retained_state, state, "{case}");
        assert_eq!(retained_request_id, semantic_request_id, "{case}");
        assert_eq!(
            raw_original, original_json,
            "semantic request changed: {case}"
        );
        let retained_effective: Value = serde_json::from_str(&raw_effective).unwrap();
        assert_eq!(retained_effective["request"], original_request, "{case}");
        assert_eq!(
            retained_effective["automation_on_behalf"], on_behalf_link,
            "{case}"
        );
        assert_eq!(
            retained_effective["request"]["expected_feedback_observation_id"],
            expected_feedback_cursor,
            "feedback cursor changed: {case}"
        );
        assert_eq!(
            retained_effective["receipt"]["value"], first["receipt"],
            "the admission receipt must remain unchanged: {case}"
        );
        assert_eq!(
            serde_json::from_str::<Value>(raw_result.as_deref().unwrap()).unwrap(),
            receipt,
            "{case}"
        );
        assert_eq!(
            subject_states(&owner.store, &subject).await,
            ("open".to_owned(), "submitted".to_owned()),
            "retained lifecycle must not accept the Task: {case}"
        );
        if case == "settled-failed-verification" {
            settled_nonapplied = Some(replay.clone());
        }
        if case == "sending-pending" || terminal_not_applied {
            // Replay the same retained event from its actual activation cut.
            // Only this fixture's cursor is restored; Task, Operation, review,
            // and semantic request provenance remain untouched.
            let key = disposition_key.clone();
            let state = activation_state.clone();
            owner
                .store
                .run(move |db| crate::automation::config::write_record(db, &key, &state))
                .await
                .unwrap();
            let reconciled = owner
                .store
                .reconcile_review_dispositions_once()
                .await
                .unwrap();
            assert_eq!(reconciled["review_results"]["quarantined"], 0, "{case}");
            let state = retained_disposition_state(&owner.store, &disposition_key).await;
            assert!(
                state["cursor"].as_i64().unwrap() >= review_observation_id,
                "{case}: retained={state}; reconciled={reconciled}"
            );
            let summary = state["recent"]
                .as_array()
                .unwrap()
                .iter()
                .rev()
                .find(|item| item["observation_id"] == review_observation_id)
                .expect("the real reconciler persists the exact review result summary");
            assert_eq!(summary["operation_id"], operation_id, "{case}");
            assert_eq!(summary["acceptance_receipt"], replay["receipt"], "{case}");
            if terminal_not_applied {
                assert_eq!(summary["status"], "skipped", "{case}");
                assert_eq!(summary["effect_status"], expected_status, "{case}");
                assert_eq!(summary["code"], expected_status, "{case}");
                assert_eq!(summary["terminal_not_applied"], true, "{case}");
                assert_eq!(summary["task_accepted"], false, "{case}");
                assert!(state["pending"].as_array().unwrap().is_empty(), "{case}");
            } else {
                assert_eq!(state["pending"].as_array().unwrap().len(), 1);
                assert_eq!(state["pending"][0]["observation_id"], review_observation_id);
                pending_disposition = Some(state);
            }
            assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);
            assert_eq!(
                subject_states(&owner.store, &subject).await,
                ("open".to_owned(), "submitted".to_owned()),
                "public reconciliation must not accept a non-applied Task: {case}"
            );
        }
    }

    // Resume the durable pending record after the same Operation has retained
    // its failed terminal result. Make its retry due without a wall-clock wait.
    let mut pending_state = pending_disposition.expect("sending was durably queued for recheck");
    pending_state["pending"][0]["next_retry_at_ms"] = json!(0);
    let key = disposition_key.clone();
    owner
        .store
        .run(move |db| crate::automation::config::write_record(db, &key, &pending_state))
        .await
        .unwrap();
    owner
        .store
        .reconcile_review_dispositions_once()
        .await
        .unwrap();
    let resumed = retained_disposition_state(&owner.store, &disposition_key).await;
    assert!(resumed["pending"].as_array().unwrap().is_empty());
    assert!(resumed["cursor"].as_i64().unwrap() >= review_observation_id);
    let resumed_summary = resumed["recent"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|item| item["observation_id"] == review_observation_id)
        .unwrap();
    assert_eq!(resumed_summary["status"], "skipped");
    assert_eq!(resumed_summary["effect_status"], "acceptance_not_applied");
    assert_eq!(resumed_summary["task_accepted"], false);
    assert_eq!(
        resumed_summary["acceptance_receipt"]["operation_id"],
        operation_id
    );
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);

    // No current automatic task.accept producer path was found that yields a
    // rejected-after-dispatch row, so construct this retained corruption
    // boundary directly. Dispatch evidence is not proof of a pre-effect
    // rejection: the real consumer must fail closed and retain the one slot.
    rewrite_retained_acceptance_lifecycle(
        &owner.store,
        &operation_id,
        "rejected",
        json!({
            "operation_id":operation_id,
            "outcome":"rejected",
            "code":"DISPATCHED_REJECTION",
            "message":"rejection after dispatch is not a no-effect proof"
        }),
    )
    .await;
    let dispatched_operation_id = operation_id.clone();
    owner
        .store
        .run(move |db| {
            let changed = db.execute(
                "UPDATE operations SET sent_at_ms=settled_at_ms WHERE operation_id=?1 AND state='rejected' AND settled_at_ms IS NOT NULL",
                [&dispatched_operation_id],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "acceptance dispatch-evidence fixture did not update its retained Operation",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
    let dispatch_error =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap_err();
    assert_eq!(dispatch_error.code, "AUTOMATION_OPERATION_CORRUPT");
    let rejected_operation_id = operation_id.clone();
    let dispatched_row = owner
        .store
        .run(move |db| {
            let row = db.query_row(
                "SELECT state,sent_at_ms,settled_at_ms FROM operations WHERE operation_id=?1",
                [&rejected_operation_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<i64>>(1)?,
                        row.get::<_, Option<i64>>(2)?,
                    ))
                },
            )?;
            Ok(row)
        })
        .await
        .unwrap();
    assert_eq!(dispatched_row.0, "rejected");
    assert!(dispatched_row.1.is_some());
    assert_eq!(dispatched_row.1, dispatched_row.2);
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);
    assert_eq!(
        subject_states(&owner.store, &subject).await,
        ("open".into(), "submitted".into())
    );

    // The same public Store consumer must fail closed when the real retained
    // Operation loader sees corrupt lifecycle or result columns. Each probe
    // keeps the original semantic slot and never creates a replacement.
    rewrite_retained_acceptance_lifecycle(
        &owner.store,
        &operation_id,
        "queued",
        first["receipt"].clone(),
    )
    .await;
    let unknown_state_operation_id = operation_id.clone();
    owner
        .store
        .run(move |db| {
            db.execute_batch("PRAGMA ignore_check_constraints=ON;")?;
            let update = db.execute(
                "UPDATE operations SET state='unknown_future_state' WHERE operation_id=?1",
                [&unknown_state_operation_id],
            );
            let reset = db.execute_batch("PRAGMA ignore_check_constraints=OFF;");
            let changed = update?;
            reset?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "unknown-state fixture did not update its retained Operation",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
    let unknown_state_error =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap_err();
    assert_eq!(unknown_state_error.code, "AUTOMATION_OPERATION_CORRUPT");
    let corrupt_rows = acceptance_rows(&owner.store, &subject).await;
    let key = disposition_key.clone();
    let state = activation_state.clone();
    owner
        .store
        .run(move |db| crate::automation::config::write_record(db, &key, &state))
        .await
        .unwrap();
    let quarantined = owner
        .store
        .reconcile_review_dispositions_once()
        .await
        .unwrap();
    assert_eq!(quarantined["review_results"]["quarantined"], 1);
    let state = retained_disposition_state(&owner.store, &disposition_key).await;
    assert!(state["cursor"].as_i64().unwrap() >= review_observation_id);
    assert!(state["pending"].as_array().unwrap().is_empty());
    let summary = state["recent"]
        .as_array()
        .unwrap()
        .iter()
        .rev()
        .find(|item| item["observation_id"] == review_observation_id)
        .unwrap();
    assert_eq!(summary["status"], "quarantined");
    assert_eq!(summary["code"], "AUTOMATION_OPERATION_CORRUPT");
    let quarantine_count: i64 = owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key LIKE 'automation:v1:review-disposition:quarantine:%'",
                [],
                |row| row.get(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(quarantine_count, 1);
    assert_eq!(acceptance_rows(&owner.store, &subject).await, corrupt_rows);
    assert_eq!(
        subject_states(&owner.store, &subject).await,
        ("open".to_owned(), "submitted".to_owned())
    );
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);

    rewrite_retained_acceptance_lifecycle(
        &owner.store,
        &operation_id,
        "queued",
        first["receipt"].clone(),
    )
    .await;
    let malformed_result_operation_id = operation_id.clone();
    owner
        .store
        .run(move |db| {
            db.execute_batch("PRAGMA ignore_check_constraints=ON;")?;
            let update = db.execute(
                "UPDATE operations SET result_json=?2 WHERE operation_id=?1",
                params![malformed_result_operation_id, "not-json"],
            );
            let reset = db.execute_batch("PRAGMA ignore_check_constraints=OFF;");
            let changed = update?;
            reset?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "malformed-result fixture did not update its retained Operation",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
    let malformed_result_error =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap_err();
    assert_eq!(malformed_result_error.code, "AUTOMATION_OPERATION_CORRUPT");
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);

    // JSON null parses successfully but cannot serve as a terminal acceptance
    // result. SQL NULL is separately missing evidence and must also fail closed.
    let null_result_operation_id = operation_id.clone();
    owner
        .store
        .run(move |db| {
            let now_ms = model::now_ms()?;
            let changed = db.execute(
                "UPDATE operations SET state='settled',result_json=?2,settled_at_ms=?3,updated_at_ms=?3 WHERE operation_id=?1",
                params![null_result_operation_id, "null", now_ms],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "JSON-null fixture did not update its retained Operation",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
    let null_result_error =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap_err();
    assert_eq!(null_result_error.code, "AUTOMATION_OPERATION_CORRUPT");
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);

    let sql_null_result_operation_id = operation_id.clone();
    owner
        .store
        .run(move |db| {
            let now_ms = model::now_ms()?;
            db.execute_batch("PRAGMA ignore_check_constraints=ON;")?;
            let update = db.execute(
                "UPDATE operations SET result_json=NULL,settled_at_ms=?2,updated_at_ms=?2 WHERE operation_id=?1 AND state='settled'",
                params![sql_null_result_operation_id, now_ms],
            );
            let reset = db.execute_batch("PRAGMA ignore_check_constraints=OFF;");
            let changed = update?;
            reset?;
            if changed != 1 {
                return Err(crate::error::Error::conflict(
                    "SQL-NULL fixture did not update its retained Operation",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
    let sql_null_result_error =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap_err();
    assert_eq!(sql_null_result_error.code, "AUTOMATION_OPERATION_CORRUPT");
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);
    assert_eq!(
        subject_states(&owner.store, &subject).await,
        ("open".into(), "submitted".into())
    );

    // Restore the actual queued admission before exercising the normal worker.
    rewrite_retained_acceptance_lifecycle(
        &owner.store,
        &operation_id,
        "queued",
        first["receipt"].clone(),
    )
    .await;

    // A terminal result for another Operation is corruption, never an ordinary
    // non-applied disposition or permission to mint a replacement request.
    rewrite_retained_acceptance_lifecycle(
        &owner.store,
        &operation_id,
        "settled",
        json!({
            "operation_id":foreign_operation_id,
            "acceptance_operation_id":foreign_operation_id,
            "outcome":"applied","task_accepted":true
        }),
    )
    .await;
    let error =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap_err();
    assert_eq!(error.code, "AUTOMATION_OPERATION_CORRUPT");
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);

    // Execute the actual Store verification worker. It must replace the queued
    // admission result with the applied decision without rewriting admission.
    rewrite_retained_acceptance_lifecycle(
        &owner.store,
        &operation_id,
        "queued",
        first["receipt"].clone(),
    )
    .await;
    owner
        .store
        .verify_automation_acceptances_once()
        .await
        .unwrap();
    assert_eq!(
        subject_states(&owner.store, &subject).await,
        ("accepted".into(), "accepted".into())
    );
    let replay =
        consume_configured_review_result(&owner.store, &assignment_id, &result_operation_id)
            .await
            .unwrap();
    assert_eq!(replay["status"], "acceptance_applied", "{replay}");
    assert_eq!(replay["operation_id"], operation_id);
    assert_eq!(replay["acceptance_operation_id"], operation_id);
    assert_eq!(replay["task_accepted"], true);
    assert_eq!(replay["receipt"]["receipt"]["outcome"], "applied");
    let settled_nonapplied =
        settled_nonapplied.expect("the same Store consumer observed a settled non-applied result");
    assert_eq!(settled_nonapplied["receipt"]["operation_state"], "settled");
    assert_eq!(replay["receipt"]["operation_state"], "settled");
    assert_eq!(
        settled_nonapplied["receipt"]["receipt"]["outcome"],
        "failed"
    );
    assert_eq!(replay["receipt"]["receipt"]["outcome"], "applied");
    assert_eq!(
        settled_nonapplied["receipt"]["status"],
        "acceptance_not_applied"
    );
    assert_eq!(replay["receipt"]["status"], "acceptance_applied");
    assert_eq!(settled_nonapplied["receipt"]["task_accepted"], false);
    assert_eq!(replay["receipt"]["task_accepted"], true);
    assert_eq!(acceptance_rows(&owner.store, &subject).await.len(), 1);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
