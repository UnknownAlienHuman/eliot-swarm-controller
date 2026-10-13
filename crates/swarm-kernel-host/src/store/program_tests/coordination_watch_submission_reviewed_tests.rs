//! One real Store path for exact, passive submission-review notices.
use super::*;

// Project one damaged read through SQLite without weakening main.meta's STRICT
// type or JSON constraints. The public reader still performs its real typed
// column read and JSON decode; every other retained row remains unchanged.
fn install_meta_read_fault(
    db: &rusqlite::Connection,
    key: &str,
    value: rusqlite::types::Value,
) -> crate::error::Result<()> {
    db.execute_batch(
        "CREATE TEMP TABLE coordination_meta_read_fault(key TEXT PRIMARY KEY, value_override);
         CREATE TEMP VIEW meta AS
         SELECT original.key,
                CASE WHEN fault.key IS NOT NULL THEN fault.value_override
                     ELSE original.value_json END AS value_json
         FROM main.meta AS original
         LEFT JOIN temp.coordination_meta_read_fault AS fault ON fault.key=original.key;",
    )?;
    db.execute(
        "INSERT INTO temp.coordination_meta_read_fault(key,value_override) VALUES(?1,?2)",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

fn clear_meta_read_fault(db: &rusqlite::Connection) -> crate::error::Result<()> {
    db.execute_batch("DROP VIEW temp.meta; DROP TABLE temp.coordination_meta_read_fault;")?;
    Ok(())
}

async fn start_inbox_participants(
    label: &str,
) -> (StoreOwner, PathBuf, ReviewSubject, Principal, Principal) {
    let (owner, directory, _bootstrap) = start_store(label).await;
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
    let mut participants = Vec::new();
    for role in ["sender", "recipient"] {
        let client_id = format!("{label}-{role}");
        let token = format!("{}{}", model::new_id(), model::new_id());
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
        participants.push(
            owner
                .store
                .authenticate(Credential { client_id, token })
                .await
                .unwrap(),
        );
    }
    let recipient = participants.pop().unwrap();
    let sender = participants.pop().unwrap();
    (owner, directory, subject, sender, recipient)
}

async fn create_sent_operation_watch(
    owner: &StoreOwner,
    creator: &Principal,
    recipient_id: &str,
    request_id: &str,
) -> Value {
    let sent = owner
        .store
        .call(
            creator.clone(),
            "coordination.send".into(),
            json!({
                "client_request_id":format!("{request_id}-send"),
                "recipient":recipient_id,
                "body":{"watch_target":request_id},
            }),
        )
        .await
        .unwrap();
    let operation_id = sent["operation_id"].as_str().unwrap();
    owner
        .store
        .call(
            creator.clone(),
            "coordination.watch.create".into(),
            json!({
                "client_request_id":format!("{request_id}-watch"),
                "watch_kind":"operation_terminal",
                "address":{"operation_id":operation_id},
                "expires_at_ms":model::now_ms().unwrap()+60_000,
                "delivery":"mailbox_header",
                "one_shot":true,
            }),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn coordination_watch_list_keeps_healthy_sibling_and_resumes_after_damaged_own_watch() {
    let (owner, directory, _, sender, recipient) =
        start_inbox_participants("watch-list-damaged-sibling").await;
    let first = create_sent_operation_watch(
        &owner,
        &sender,
        &recipient.client_id,
        "watch-list-damaged-first",
    )
    .await;
    let second = create_sent_operation_watch(
        &owner,
        &sender,
        &recipient.client_id,
        "watch-list-damaged-second",
    )
    .await;
    let watch_ids = vec![
        first["watch_id"].as_str().unwrap().to_owned(),
        second["watch_id"].as_str().unwrap().to_owned(),
    ];
    let (damaged_watch_id, healthy_watch_id) = owner
        .store
        .run(move |db| {
            let mut rows = Vec::new();
            for watch_id in watch_ids {
                let record_key = format!(
                    "coordination:watch:v1:record:{}",
                    crate::coordination::key_component(&watch_id)
                );
                let raw: String = db.query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [&record_key],
                    |row| row.get(0),
                )?;
                let record: Value = serde_json::from_str(&raw)?;
                rows.push((
                    model::text(&record, "list_index_key")?.to_owned(),
                    watch_id,
                    record_key,
                ));
            }
            rows.sort_by(|left, right| left.0.cmp(&right.0));
            let damaged_watch_id = rows[0].1.clone();
            let healthy_watch_id = rows[1].1.clone();
            let record_key = rows[0].2.clone();
            // Malformed retained JSON must become a row-local gap, not a failed list.
            install_meta_read_fault(db, &record_key, rusqlite::types::Value::Text("{".into()))?;
            Ok((damaged_watch_id, healthy_watch_id))
        })
        .await
        .unwrap();

    let damaged_page = owner
        .store
        .call(
            sender.clone(),
            "coordination.watch.list".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(damaged_page["items"], json!([]));
    assert_eq!(damaged_page["coverage"], "partial");
    assert_eq!(damaged_page["next_after_watch_id"], damaged_watch_id);
    assert_eq!(
        damaged_page["gaps"],
        json!([
            {"kind":"watch_list_entries_damaged","count":1},
            {"kind":"watch_list_window_truncated"},
        ])
    );

    let resumed_page = owner
        .store
        .call(
            sender,
            "coordination.watch.list".into(),
            json!({"limit":1,"after_watch_id":damaged_watch_id}),
        )
        .await
        .unwrap();
    assert_eq!(resumed_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(resumed_page["items"][0]["watch_id"], healthy_watch_id);
    assert_eq!(resumed_page["coverage"], "complete");
    assert_eq!(resumed_page["gaps"], json!([]));

    owner
        .store
        .run(move |db| clear_meta_read_fault(db))
        .await
        .unwrap();

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

async fn register_coordination_test_participant(
    owner: &StoreOwner,
    subject: &ReviewSubject,
    client_id: &str,
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
                "participation_basis":{"kind":"attempt_owner"},
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

async fn publish_matching_work_card(
    owner: &StoreOwner,
    participant: &Principal,
    request_id: &str,
    path: &str,
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
                    "interfaces":["A retained owner fact for the fixture."],
                },
            }),
        )
        .await
        .unwrap();
}

async fn publish_matching_contract_card(
    owner: &StoreOwner,
    participant: &Principal,
    request_id: &str,
    contract_key: &str,
    path: &str,
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
                    "paths":[path],
                    "interfaces":["A retained contract fact for the fixture."],
                },
            }),
        )
        .await
        .unwrap();
}

fn scope_probe_request(request: &Value, phase: &str) -> Value {
    let mut probe = request.clone();
    if let Some(request_id) = probe
        .get("client_request_id")
        .and_then(Value::as_str)
        .map(str::to_owned)
    {
        probe["client_request_id"] = json!(format!("{request_id}-{phase}"));
    }
    probe
}

async fn assert_candidate_scope_error_classification(
    owner: &StoreOwner,
    caller: &Principal,
    candidate_id: &str,
    method: &str,
    request: &Value,
) {
    let registration_key = format!("client:{candidate_id}");
    let key = registration_key.clone();
    let original_registration = owner
        .store
        .run(move |db| {
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            Ok(raw)
        })
        .await
        .unwrap();

    let key = registration_key.clone();
    owner
        .store
        .run(move |db| {
            let deleted = db.execute("DELETE FROM meta WHERE key=?1", [&key])?;
            assert_eq!(deleted, 1);
            Ok(())
        })
        .await
        .unwrap();
    let stale = owner
        .store
        .call(
            caller.clone(),
            method.to_owned(),
            scope_probe_request(request, "missing-owner"),
        )
        .await
        .unwrap();
    assert_eq!(stale["coverage"], "partial", "{method}: {stale}");
    assert!(
        stale["gaps"]
            .as_array()
            .is_some_and(|gaps| gaps.iter().any(|gap| gap["count"] == 1)),
        "{method} did not count the missing indexed owner as one stale gap: {stale}"
    );
    if method == "coordination.consult" {
        assert_eq!(stale["status"], "unknown_coverage");
        assert_eq!(stale["delivery_created"], false);
    }

    let key = registration_key.clone();
    let original = original_registration.clone();
    owner
        .store
        .run(move |db| {
            db.execute(
                "INSERT INTO meta(key,value_json) VALUES(?1,?2) \
                 ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",
                rusqlite::params![key, original],
            )?;
            Ok(())
        })
        .await
        .unwrap();

    let key = registration_key.clone();
    owner
        .store
        .run(move |db| {
            install_meta_read_fault(db, &key, rusqlite::types::Value::Blob(vec![0x80]))?;
            Ok(())
        })
        .await
        .unwrap();
    let storage_error = owner
        .store
        .call(
            caller.clone(),
            method.to_owned(),
            scope_probe_request(request, "sqlite-conversion-error"),
        )
        .await
        .unwrap_err();
    assert_eq!(
        storage_error.code, "STORE_ERROR",
        "{method}: {storage_error}"
    );

    assert!(
        storage_error.message.contains("column type"),
        "{method}: {storage_error}"
    );
    assert!(
        storage_error.message.contains("value_json"),
        "{method}: {storage_error}"
    );

    let key = registration_key.clone();
    owner
        .store
        .run(move |db| {
            db.execute(
                "UPDATE temp.coordination_meta_read_fault SET value_override=?2 WHERE key=?1",
                rusqlite::params![key, "{"],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let decode_error = owner
        .store
        .call(
            caller.clone(),
            method.to_owned(),
            scope_probe_request(request, "scope-decode-error"),
        )
        .await
        .unwrap_err();
    assert_eq!(
        decode_error.code, "INVALID_PARAMS",
        "{method}: {decode_error}"
    );

    owner
        .store
        .run(move |db| clear_meta_read_fault(db))
        .await
        .unwrap();
}

async fn open_coordination_fixture_thread(
    owner: &StoreOwner,
    subject: &ReviewSubject,
    participant: &Principal,
    suffix: &str,
) -> Value {
    owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.open".into(),
            json!({
                "client_request_id":format!("thread-index-{suffix}"),
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "topic_kind":"coordination_fixture",
                "subject":format!("Thread index fixture {suffix}"),
                "participants":[{
                    "client_id":participant.client_id,
                    "reason":"Exercise the public task-indexed Thread read.",
                }],
                "reasonability":{
                    "blocking_fact":"A retained fixture row must be listed.",
                    "decision_needed":"Prove the exact pagination boundary.",
                    "why_coordination_is_needed":"The public page must reflect indexed state.",
                    "expected_output":"A bounded Thread list page.",
                    "close_condition":"The retained index is exhausted.",
                },
                "delivery_mode":"mailbox_only",
            }),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn coordination_consult_single_remote_owner_two_matching_field_cards_routes_one_addressed_ask()
 {
    let (owner, directory, _, sender, remote_owner) =
        start_inbox_participants("consult-one-owner-two-facts").await;
    let target_path = "src/consult-one-owner-two-facts.rs";
    publish_matching_work_card(
        &owner,
        &remote_owner,
        "consult-one-owner-work-card",
        target_path,
    )
    .await;
    publish_matching_contract_card(
        &owner,
        &remote_owner,
        "consult-one-owner-contract-card",
        "consult-one-owner-contract",
        target_path,
    )
    .await;

    let consulted = owner
        .store
        .call(
            sender,
            "coordination.consult".into(),
            json!({
                "client_request_id":"consult-one-owner-two-facts-ask",
                "target":{"path":target_path},
                "field":"interfaces",
                "question_kind":"assumption_check",
                "question":"Which assumption applies?",
                "why_needed":"The two retained cards belong to one exact owner.",
                "expected_answer":"one_fact",
                "blocking":false,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap();
    assert_eq!(consulted["status"], "asked");
    assert_eq!(consulted["owner_state"], "exact_owner");
    assert_eq!(consulted["recipient"], remote_owner.client_id);
    assert_eq!(consulted["ask"]["recipient"], remote_owner.client_id);
    assert_eq!(consulted["delivery_created"], true);

    let inbox = owner
        .store
        .call(
            remote_owner,
            "coordination.inbox".into(),
            json!({"limit":50}),
        )
        .await
        .unwrap();
    let items = inbox["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["operation_id"], consulted["operation_id"]);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_consult_multiple_distinct_field_owners_reports_unique_candidate_count_and_never_sends()
 {
    let (owner, directory, subject, sender, first_owner) =
        start_inbox_participants("consult-two-distinct-owners").await;
    let second_owner = register_coordination_test_participant(
        &owner,
        &subject,
        "consult-two-distinct-owners-second",
    )
    .await;
    let target_path = "src/consult-two-distinct-owners.rs";
    publish_matching_work_card(
        &owner,
        &first_owner,
        "consult-two-owner-first-card",
        target_path,
    )
    .await;
    publish_matching_work_card(
        &owner,
        &second_owner,
        "consult-two-owner-second-card",
        target_path,
    )
    .await;

    let consulted = owner
        .store
        .call(
            sender,
            "coordination.consult".into(),
            json!({
                "client_request_id":"consult-two-distinct-owners-ask",
                "target":{"path":target_path},
                "field":"interfaces",
                "question_kind":"assumption_check",
                "question":"Which owner has the authoritative assumption?",
                "why_needed":"Both distinct owners publish an exact matching field.",
                "expected_answer":"one_fact",
                "blocking":false,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap();
    assert_eq!(consulted["status"], "multiple_candidates");
    assert_eq!(consulted["owner_state"], "multiple_candidates");
    assert_eq!(consulted["candidate_count"], 2);
    assert_eq!(
        consulted["candidates"]["items"].as_array().unwrap().len(),
        2
    );
    assert_eq!(consulted["delivery_created"], false);
    for candidate in [&first_owner, &second_owner] {
        assert!(
            consulted["candidates"]["items"]
                .as_array()
                .unwrap()
                .iter()
                .any(|item| item["client_id"].as_str() == Some(candidate.client_id.as_str()))
        );
        let inbox = owner
            .store
            .call(
                candidate.clone(),
                "coordination.inbox".into(),
                json!({"limit":50}),
            )
            .await
            .unwrap();
        assert!(inbox["items"].as_array().unwrap().is_empty());
    }

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_card_list_stale_missing_owner_counts_stale_but_sqlite_and_scope_decode_errors_propagate()
 {
    let (owner, directory, _, sender, remote_owner) =
        start_inbox_participants("card-list-owner-errors").await;
    let target_path = "src/card-list-owner-errors.rs";
    publish_matching_work_card(
        &owner,
        &remote_owner,
        "card-list-owner-errors-card",
        target_path,
    )
    .await;
    assert_candidate_scope_error_classification(
        &owner,
        &sender,
        &remote_owner.client_id,
        "coordination.work_card.list",
        &json!({"path":target_path}),
    )
    .await;
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_contract_cards_for_term_stale_missing_owner_counts_stale_but_sqlite_and_scope_decode_errors_propagate()
 {
    let (owner, directory, _, sender, remote_owner) =
        start_inbox_participants("contract-card-list-owner-errors").await;
    let contract_key = "contract-card-list-owner-errors";
    publish_matching_contract_card(
        &owner,
        &remote_owner,
        "contract-card-list-owner-errors-card",
        contract_key,
        "src/contract-card-list-owner-errors.rs",
    )
    .await;
    assert_candidate_scope_error_classification(
        &owner,
        &sender,
        &remote_owner.client_id,
        "coordination.contract_card.list",
        &json!({"contract_key":contract_key}),
    )
    .await;
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_peer_find_stale_missing_owner_counts_stale_but_sqlite_and_scope_decode_errors_propagate()
 {
    let (owner, directory, _, sender, remote_owner) =
        start_inbox_participants("peer-find-owner-errors").await;
    let target_path = "src/peer-find-owner-errors.rs";
    publish_matching_work_card(
        &owner,
        &remote_owner,
        "peer-find-owner-errors-card",
        target_path,
    )
    .await;
    assert_candidate_scope_error_classification(
        &owner,
        &sender,
        &remote_owner.client_id,
        "coordination.peer.find",
        &json!({"path":target_path}),
    )
    .await;
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_consult_matching_cards_stale_missing_owner_counts_stale_but_sqlite_and_scope_decode_errors_propagate()
 {
    let (owner, directory, _, sender, remote_owner) =
        start_inbox_participants("consult-owner-errors").await;
    let target_path = "src/consult-owner-errors.rs";
    publish_matching_work_card(
        &owner,
        &remote_owner,
        "consult-owner-errors-card",
        target_path,
    )
    .await;
    assert_candidate_scope_error_classification(
        &owner,
        &sender,
        &remote_owner.client_id,
        "coordination.consult",
        &json!({
            "client_request_id":"consult-owner-errors-probe",
            "target":{"path":target_path},
            "field":"interfaces",
            "question_kind":"assumption_check",
            "question":"Which assumption applies?",
            "why_needed":"Verify current owner error classification.",
            "expected_answer":"one_fact",
            "blocking":false,
            "reply_deadline_ms":model::now_ms().unwrap()+60_000,
            "evidence_refs":[],
        }),
    )
    .await;
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_inbox_live_watch_notifications_honor_limits_1_and_50() {
    const NOTICE_COUNT: usize = 51;
    let (owner, directory, _, sender, recipient) =
        start_inbox_participants("inbox-live-watch-limits").await;
    for index in 0..NOTICE_COUNT {
        create_sent_operation_watch(
            &owner,
            &sender,
            &recipient.client_id,
            &format!("inbox-live-watch-limit-{index}"),
        )
        .await;
    }
    let mut matched = 0usize;
    for _ in 0..4 {
        let reconciled = owner.store.reconcile_watches_once().await.unwrap();
        matched += reconciled["matched"].as_u64().unwrap_or_default() as usize;
        if matched == NOTICE_COUNT {
            break;
        }
    }
    assert_eq!(matched, NOTICE_COUNT);

    let one = owner
        .store
        .call(
            sender.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(
        one["watch_notifications"]["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let fifty = owner
        .store
        .call(sender, "coordination.inbox".into(), json!({"limit":50}))
        .await
        .unwrap();
    assert_eq!(
        fifty["watch_notifications"]["items"]
            .as_array()
            .unwrap()
            .len(),
        50
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_thread_list_exactly_exhausted_index_reports_has_more_false() {
    let (owner, directory, subject, sender, _) =
        start_inbox_participants("thread-index-exhausted").await;
    open_coordination_fixture_thread(&owner, &subject, &sender, "only-row").await;
    let page = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "limit":1,
            }),
        )
        .await
        .unwrap();
    assert_eq!(page["items"].as_array().unwrap().len(), 1);
    assert_eq!(page["has_more"], false);
    assert_eq!(page["coverage"], "complete_for_filter");
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_thread_list_one_index_row_after_returned_cursor_reports_has_more_true() {
    let (owner, directory, subject, sender, _) =
        start_inbox_participants("thread-index-next-row").await;
    open_coordination_fixture_thread(&owner, &subject, &sender, "first-row").await;
    open_coordination_fixture_thread(&owner, &subject, &sender, "second-row").await;
    let first_page = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "limit":1,
            }),
        )
        .await
        .unwrap();
    assert_eq!(first_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(first_page["has_more"], true);
    let first_thread_id = first_page["next_cursor"].as_str().unwrap().to_owned();
    let second_page = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "limit":1,
                "after_thread_id":first_thread_id,
            }),
        )
        .await
        .unwrap();
    assert_eq!(second_page["items"].as_array().unwrap().len(), 1);
    assert_ne!(second_page["items"][0]["thread_id"], first_thread_id);
    assert_eq!(second_page["has_more"], false);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_thread_list_exact_scan_bound_requires_a_row_after_bound_before_has_more_true()
{
    const THREAD_COUNT: usize = 12;
    let (owner, directory, subject, sender, _) =
        start_inbox_participants("thread-index-scan-bound").await;
    for index in 0..THREAD_COUNT {
        open_coordination_fixture_thread(&owner, &subject, &sender, &format!("scan-bound-{index}"))
            .await;
    }
    let initial = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "limit":20,
            }),
        )
        .await
        .unwrap();
    let indexed_threads = initial["items"].as_array().unwrap();
    assert_eq!(indexed_threads.len(), THREAD_COUNT);
    assert_eq!(initial["has_more"], false);

    // With limit 1, the source scans 9 index rows. Cursor after item 3 leaves
    // exactly 9 retained rows; cursor after item 2 leaves one real row beyond
    // that same scan bound.
    let exact_bound_cursor = indexed_threads[2]["thread_id"].as_str().unwrap();
    let exact_bound = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "topic_kind":"fixture-topic-with-no-matches",
                "limit":1,
                "after_thread_id":exact_bound_cursor,
            }),
        )
        .await
        .unwrap();
    assert!(exact_bound["items"].as_array().unwrap().is_empty());
    assert_eq!(exact_bound["has_more"], false);

    let beyond_bound_cursor = indexed_threads[1]["thread_id"].as_str().unwrap();
    let beyond_bound = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "topic_kind":"fixture-topic-with-no-matches",
                "limit":1,
                "after_thread_id":beyond_bound_cursor,
            }),
        )
        .await
        .unwrap();
    assert!(beyond_bound["items"].as_array().unwrap().is_empty());
    assert_eq!(beyond_bound["has_more"], true);
    let last_scanned_thread = beyond_bound["next_cursor"].as_str().unwrap().to_owned();
    let proven_later_row = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "coordination.thread.list".into(),
            json!({
                "task_id":subject.task_id,
                "attempt_id":subject.attempt_id,
                "limit":1,
                "after_thread_id":last_scanned_thread,
            }),
        )
        .await
        .unwrap();
    assert_eq!(proven_later_row["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        proven_later_row["items"][0]["thread_id"],
        indexed_threads[11]["thread_id"]
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_watch_malformed_list_keys_resume_with_scoped_scan_cursor() {
    let (owner, directory, _, sender, recipient) =
        start_inbox_participants("watch-bad-key-cursor").await;
    let healthy = create_sent_operation_watch(
        &owner,
        &sender,
        &recipient.client_id,
        "healthy-after-key-damage",
    )
    .await;
    let healthy_id = healthy["watch_id"].as_str().unwrap().to_owned();
    let watch_id = healthy_id.clone();
    let prefix = owner
        .store
        .run(move |db| {
            let key = format!(
                "coordination:watch:v1:record:{}",
                crate::coordination::key_component(&watch_id)
            );
            let raw: String =
                db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| {
                    row.get(0)
                })?;
            let record: Value = serde_json::from_str(&raw)?;
            let list_key = model::text(&record, "list_index_key")?;
            let (without_id, _) = list_key.rsplit_once(':').unwrap();
            let (prefix, _) = without_id.rsplit_once(':').unwrap();
            Ok(format!("{prefix}:"))
        })
        .await
        .unwrap();
    for suffix in [
        "0000000000000000000x:61",
        "00000000000000000000zz",
        "00000000000000000000:zz",
        "00000000000000000000:20",
    ] {
        let bad_key = format!("{prefix}{suffix}");
        let key = bad_key.clone();
        owner
            .store
            .run(move |db| {
                db.execute("INSERT INTO meta(key,value_json) VALUES(?1,'{}')", [&key])?;
                Ok(())
            })
            .await
            .unwrap();
        let first = owner
            .store
            .call(
                sender.clone(),
                "coordination.watch.list".into(),
                json!({"limit":1}),
            )
            .await
            .unwrap();
        assert_eq!(first["items"], json!([]));
        assert_eq!(first["coverage"], "partial");
        assert_eq!(
            first["gaps"][0],
            json!({"kind":"watch_list_entries_damaged","count":1})
        );
        assert!(first["next_after_watch_id"].is_null());
        let cursor = first["next_scan_cursor"].as_str().unwrap();
        let resumed = owner
            .store
            .call(
                sender.clone(),
                "coordination.watch.list".into(),
                json!({"limit":1,"after_scan_cursor":cursor}),
            )
            .await
            .unwrap();
        assert_eq!(resumed["items"][0]["watch_id"], healthy_id);
        assert_eq!(resumed["coverage"], "complete");
        let foreign = owner
            .store
            .call(
                recipient.clone(),
                "coordination.watch.list".into(),
                json!({"limit":1,"after_scan_cursor":cursor}),
            )
            .await
            .unwrap_err();
        assert_eq!(foreign.code, "INVALID_PARAMS");
        let mixed = owner
            .store
            .call(
                sender.clone(),
                "coordination.watch.list".into(),
                json!({"limit":1,"after_watch_id":healthy_id,"after_scan_cursor":cursor}),
            )
            .await
            .unwrap_err();
        assert_eq!(mixed.code, "INVALID_PARAMS");
        owner
            .store
            .run(move |db| {
                db.execute("DELETE FROM meta WHERE key=?1", [&bad_key])?;
                Ok(())
            })
            .await
            .unwrap();
    }
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_thread_unprojectable_scan_window_reaches_later_authorized_thread() {
    let (owner, directory, subject, sender, _) =
        start_inbox_participants("thread-missing-scan-cursor").await;
    for index in 0..10 {
        open_coordination_fixture_thread(
            &owner,
            &subject,
            &sender,
            &format!("missing-window-{index}"),
        )
        .await;
    }
    let manager = principal("review-owner-v2", Role::Manager);
    let initial = owner
        .store
        .call(
            manager.clone(),
            "coordination.thread.list".into(),
            json!({"task_id":subject.task_id,"attempt_id":subject.attempt_id,"limit":20}),
        )
        .await
        .unwrap();
    let items = initial["items"].as_array().unwrap();
    assert_eq!(items.len(), 10);
    let later_id = items[9]["thread_id"].as_str().unwrap().to_owned();
    let missing_ids = items
        .iter()
        .take(9)
        .map(|item| item["thread_id"].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    owner
        .store
        .run(move |db| {
            for id in missing_ids {
                assert_eq!(
                    db.execute(
                        "DELETE FROM meta WHERE key=?1",
                        [format!("coordination:thread:{id}")]
                    )?,
                    1
                );
            }
            Ok(())
        })
        .await
        .unwrap();
    let first = owner
        .store
        .call(
            manager.clone(),
            "coordination.thread.list".into(),
            json!({"task_id":subject.task_id,"attempt_id":subject.attempt_id,"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(first["items"], json!([]));
    assert_eq!(first["has_more"], true);
    assert!(first["next_cursor"].is_null());
    let cursor = first["next_scan_cursor"].as_str().unwrap();
    let resumed = owner.store.call(manager.clone(), "coordination.thread.list".into(), json!({"task_id":subject.task_id,"attempt_id":subject.attempt_id,"limit":1,"after_scan_cursor":cursor})).await.unwrap();
    assert_eq!(resumed["items"][0]["thread_id"], later_id);
    assert_eq!(resumed["has_more"], false);
    let foreign = owner.store.call(sender, "coordination.thread.list".into(), json!({"task_id":subject.task_id,"attempt_id":subject.attempt_id,"limit":1,"after_scan_cursor":cursor})).await.unwrap_err();
    assert_eq!(foreign.code, "INVALID_PARAMS");
    let mixed = owner.store.call(manager, "coordination.thread.list".into(), json!({"task_id":subject.task_id,"attempt_id":subject.attempt_id,"limit":1,"after_thread_id":later_id,"after_scan_cursor":cursor})).await.unwrap_err();
    assert_eq!(mixed.code, "INVALID_PARAMS");
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_watch_reconcile_counts_non_active_index_as_stale() {
    let (owner, directory, _, sender, recipient) =
        start_inbox_participants("watch-reconcile-inactive-index").await;
    let watch = create_sent_operation_watch(
        &owner,
        &sender,
        &recipient.client_id,
        "watch-reconcile-inactive-index",
    )
    .await;
    let watch_id = watch["watch_id"].as_str().unwrap().to_owned();
    owner
        .store
        .run({
            let watch_id = watch_id.clone();
            move |db| {
                let record_key = format!(
                    "coordination:watch:v1:record:{}",
                    crate::coordination::key_component(&watch_id)
                );
                let raw: String = db.query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [&record_key],
                    |row| row.get(0),
                )?;
                let mut record: Value = serde_json::from_str(&raw)?;
                record["state"] = json!("matched");
                db.execute(
                    "UPDATE meta SET value_json=?2 WHERE key=?1",
                    rusqlite::params![record_key, model::canonical(&record)?],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();

    let receipt = owner.store.reconcile_watches_once().await.unwrap();
    assert_eq!(receipt["processed"], 1);
    assert_eq!(receipt["matched"], 0);
    assert_eq!(receipt["expired"], 0);
    assert_eq!(receipt["stale"], 1);
    assert_eq!(
        receipt["processed"].as_u64().unwrap(),
        receipt["matched"].as_u64().unwrap()
            + receipt["expired"].as_u64().unwrap()
            + receipt["stale"].as_u64().unwrap()
    );

    let active_key = format!(
        "coordination:watch:v1:active:{}",
        crate::coordination::key_component(&watch_id)
    );
    let active_index_count = owner
        .store
        .run(move |db| {
            let count: i64 = db.query_row(
                "SELECT COUNT(*) FROM meta WHERE key=?1",
                [&active_key],
                |row| row.get(0),
            )?;
            Ok(count)
        })
        .await
        .unwrap();
    assert_eq!(active_index_count, 0);

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_inbox_reports_authority_filter_and_window_truncation_together() {
    let (owner, directory, subject, sender, recipient) =
        start_inbox_participants("watch-inbox-two-gaps").await;
    for request_id in ["watch-inbox-two-gaps-first", "watch-inbox-two-gaps-second"] {
        create_sent_operation_watch(&owner, &sender, &recipient.client_id, request_id).await;
    }
    let reconcile = owner.store.reconcile_watches_once().await.unwrap();
    assert_eq!(reconcile["processed"], 2);
    assert_eq!(reconcile["matched"], 2);
    assert_eq!(reconcile["expired"], 0);
    assert_eq!(reconcile["stale"], 0);

    let released = owner
        .store
        .call(
            principal("review-owner-v2", Role::Manager),
            "attempt.release".into(),
            json!({
                "client_request_id":"watch-inbox-two-gaps-release",
                "attempt_id":subject.attempt_id,
                "outcome":"superseded",
                "reason":"Make the retained notices transition-only.",
                "assignment_closed":true,
            }),
        )
        .await
        .unwrap();
    assert_eq!(released["released"], true);

    let inbox = owner
        .store
        .call(sender, "coordination.inbox".into(), json!({"limit":1}))
        .await
        .unwrap();
    assert_eq!(inbox["items"], json!([]));
    let watch_notifications = &inbox["watch_notifications"];
    assert_eq!(watch_notifications["items"], json!([]));
    assert_eq!(watch_notifications["coverage"], "partial");
    assert_eq!(
        watch_notifications["gaps"],
        json!([
            {"kind":"watch_notice_current_authority_filtered","count":1},
            {"kind":"watch_notice_window_truncated","retained_inventory":"coordination.watch.list"},
            {"kind":"current_scope_unavailable","projection":"watch_headers_only"},
        ])
    );

    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_inbox_resumes_server_issued_overlong_retained_key() {
    let (owner, directory, subject, sender, recipient) =
        start_inbox_participants("inbox-overlong-cursor").await;
    let sent = owner
        .store
        .call(
            sender,
            "coordination.send".into(),
            json!({
                "client_request_id":"overlong-cursor-healthy-send",
                "recipient":recipient.client_id,
                "body":{"healthy":true},
            }),
        )
        .await
        .unwrap();
    let scope_id = crate::coordination::scope_id(&subject.task_id, 1, &subject.attempt_id).unwrap();
    let prefix = crate::coordination::mailbox_prefix(&scope_id, &recipient.client_id);
    let malformed_keys = (0..36)
        .map(|index| format!("{prefix}{index:020}:{}", "g".repeat(600)))
        .collect::<Vec<_>>();
    owner
        .store
        .run({
            let malformed_keys = malformed_keys.clone();
            move |db| {
                for key in malformed_keys {
                    db.execute(
                        "INSERT INTO meta(key,value_json) VALUES(?1,'{\"damaged\":true}')",
                        [&key],
                    )?;
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    let gap = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(gap["items"], json!([]));
    assert_eq!(gap["coverage"], "partial");
    assert_eq!(gap["next_after_key"], malformed_keys[35]);
    assert!(gap["next_after_key"].as_str().unwrap().len() > prefix.len() + 536);
    let resumed = owner
        .store
        .call(
            recipient,
            "coordination.inbox".into(),
            json!({"limit":1,"after_key":gap["next_after_key"]}),
        )
        .await
        .unwrap();
    assert_eq!(resumed["items"].as_array().unwrap().len(), 1);
    assert_eq!(resumed["items"][0]["operation_id"], sent["operation_id"]);
    assert_eq!(resumed["coverage"], "complete");
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_inbox_rejects_consult_body_mismatch_before_damaged_index() {
    let (owner, directory, subject, sender, recipient) =
        start_inbox_participants("inbox-consult-integrity").await;
    owner
        .store
        .call(
            recipient.clone(),
            "coordination.work_card.publish".into(),
            json!({"client_request_id":"consult-owner-card","fields":{"paths":["consult-owner.rs"]}}),
        )
        .await
        .unwrap();
    let consulted = owner
        .store
        .call(
            sender,
            "coordination.consult".into(),
            json!({
                "client_request_id":"consult-integrity-ask",
                "target":{"path":"consult-owner.rs"},
                "field":"assumptions",
                "question_kind":"assumption_check",
                "question":"What assumption applies?",
                "why_needed":"Verify one retained consultation.",
                "expected_answer":"one_fact",
                "blocking":false,
                "reply_deadline_ms":model::now_ms().unwrap()+60_000,
                "evidence_refs":[],
            }),
        )
        .await
        .unwrap();
    assert_eq!(consulted["delivery_created"], true);
    let operation_id = consulted["operation_id"].as_str().unwrap().to_owned();
    let healthy = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(healthy["items"][0]["operation_id"], operation_id);
    let scope_id = crate::coordination::scope_id(&subject.task_id, 1, &subject.attempt_id).unwrap();
    let prefix = crate::coordination::mailbox_prefix(&scope_id, &recipient.client_id);
    owner
        .store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT result_json FROM operations WHERE operation_id=?1",
                [&operation_id],
                |row| row.get(0),
            )?;
            let mut result: Value = serde_json::from_str(&raw)?;
            result["ask_id"] = json!("altered-authoritative-ask");
            db.execute(
                "UPDATE operations SET result_json=?2 WHERE operation_id=?1",
                rusqlite::params![operation_id, model::canonical(&result)?],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    for damaged_index in [false, true] {
        if damaged_index {
            owner
                .store
                .run({
                    let prefix = prefix.clone();
                    move |db| {
                        db.execute(
                            "UPDATE meta SET value_json='{\"damaged\":true}' WHERE key>=?1 AND key<?2",
                            rusqlite::params![prefix, format!("{prefix}z")],
                        )?;
                        Ok(())
                    }
                })
                .await
                .unwrap();
        }
        let error = owner
            .store
            .call(
                recipient.clone(),
                "coordination.inbox".into(),
                json!({"limit":1}),
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.code, "INBOX_OPERATION_BODY_DAMAGED",
            "damaged index: {damaged_index}"
        );
    }
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

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

// Admit real deliveries, then retain reverse-timestamp legacy locators so
// the earliest canonical delivery is deliberately last in the legacy scan.
async fn retain_reversed_legacy_inbox(
    store: &Store,
    subject: &ReviewSubject,
    sender: &Principal,
    recipient: &Principal,
    legacy_count: usize,
) -> (Vec<String>, Vec<(String, String)>) {
    let mut ids = Vec::with_capacity(legacy_count + 1);
    for index in 0..=legacy_count {
        let sent = store
            .call(
                sender.clone(),
                "coordination.send".into(),
                json!({"client_request_id":format!("reverse-legacy-{index}"),
                       "recipient":recipient.client_id,"body":{"seq":index}}),
            )
            .await
            .unwrap();
        ids.push(sent["operation_id"].as_str().unwrap().to_owned());
    }
    let scope = crate::coordination::scope_id(&subject.task_id, 1, &subject.attempt_id).unwrap();
    let prefix = crate::coordination::mailbox_prefix(&scope, &recipient.client_id);
    let rows = store
        .run({
            let ids = ids.clone();
            let recipient_id = recipient.client_id.clone();
            move |db| {
                let tx = db.transaction()?;
                let mut rows = Vec::with_capacity(legacy_count);
                for (index, id) in ids.iter().take(legacy_count).enumerate() {
                    let position: i64 = tx.query_row(
                        "SELECT observation_id FROM observations WHERE \
                         source_stream_id='controller:read-position:operation' AND source_event_key=?1",
                        [id], |row| row.get(0),
                    )?;
                    let v2 = format!("{prefix}v2:{position:020}:{}", crate::coordination::key_component(id));
                    let mut record = meta(&tx, &v2)?.unwrap();
                    let stamp = 10_000 + (legacy_count - index) as i64;
                    tx.execute("UPDATE operations SET created_at_ms=?1 WHERE operation_id=?2", params![stamp,id])?;
                    tx.execute(
                        "UPDATE observations SET recorded_at_ms=?1 WHERE \
                         source_stream_id='controller:read-position:operation' AND source_event_key=?2",
                        params![stamp,id],
                    )?;
                    record["created_at_ms"] = json!(stamp);
                    record.as_object_mut().unwrap().remove("read_position");
                    let legacy = crate::coordination::mailbox_key(&scope, &recipient_id, stamp, id);
                    set_meta(&tx, &legacy, &record)?;
                    tx.execute("DELETE FROM meta WHERE key=?1", [&v2])?;
                    rows.push((legacy, model::canonical(&record)?));
                }
                tx.commit()?;
                Ok(rows)
            }
        })
        .await
        .unwrap();
    (ids, rows)
}

#[tokio::test]
async fn coordination_inbox_withholds_items_until_more_than_64_legacy_rows_are_normalized() {
    let (owner, directory, subject, sender, recipient) =
        start_inbox_participants("inbox-normalization-bound").await;
    let (ids, _) =
        retain_reversed_legacy_inbox(&owner.store, &subject, &sender, &recipient, 66).await;
    let prefix = crate::coordination::mailbox_prefix(
        &crate::coordination::scope_id(&subject.task_id, 1, &subject.attempt_id).unwrap(),
        &recipient.client_id,
    );
    let checkpoint_key = format!(
        "coordination:mailbox-normalization:{}",
        crate::coordination::key_component(&prefix)
    );
    let pending = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(pending["items"], json!([]));
    assert_eq!(pending["coverage"], "partial");
    assert_eq!(
        pending["availability"],
        "legacy_order_normalization_pending"
    );
    assert_eq!(pending["gaps"][0]["kind"], "inbox_normalization_pending");
    assert_eq!(pending["next_after"], Value::Null);
    assert_eq!(pending["next_after_key"], Value::Null);
    owner
        .store
        .run({
            let prefix = prefix.clone();
            let checkpoint_key = checkpoint_key.clone();
            move |db| {
                let count: i64 = db.query_row(
                    "SELECT count(*) FROM meta WHERE key>=?1 AND key<?2",
                    params![prefix, format!("{prefix}v")],
                    |row| row.get(0),
                )?;
                assert_eq!(count, 2);
                assert!(meta(db, &checkpoint_key)?.is_some());
                Ok(())
            }
        })
        .await
        .unwrap();
    let first = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(first["items"][0]["operation_id"], ids[0]);
    assert_eq!(first["next_after"], ids[0]);
    let rest = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":50,"after_operation_id":ids[0]}),
        )
        .await
        .unwrap();
    let got: Vec<_> = rest["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["operation_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        got,
        ids[1..51].iter().map(String::as_str).collect::<Vec<_>>()
    );
    let tail = owner
        .store
        .call(
            recipient,
            "coordination.inbox".into(),
            json!({"limit":50,"after_operation_id":rest["next_after"]}),
        )
        .await
        .unwrap();
    let got: Vec<_> = tail["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["operation_id"].as_str().unwrap())
        .collect();
    assert_eq!(
        got,
        ids[51..].iter().map(String::as_str).collect::<Vec<_>>()
    );
    assert_eq!(tail["coverage"], "complete");
    owner
        .store
        .run(move |db| {
            let complete = meta(db, &checkpoint_key)?.unwrap();
            assert_eq!(complete["state"], "complete");
            assert_eq!(complete["damaged_count"], 0);
            Ok(())
        })
        .await
        .unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_inbox_normalization_advances_past_damaged_legacy_prefix() {
    let (owner, directory, subject, sender, recipient) =
        start_inbox_participants("inbox-normalization-damage").await;
    let (ids, _) =
        retain_reversed_legacy_inbox(&owner.store, &subject, &sender, &recipient, 6).await;
    let prefix = crate::coordination::mailbox_prefix(
        &crate::coordination::scope_id(&subject.task_id, 1, &subject.attempt_id).unwrap(),
        &recipient.client_id,
    );
    let damaged: Vec<_> = (0..70)
        .map(|index| format!("{prefix}!damage:{index:03}"))
        .collect();
    owner
        .store
        .run({
            let damaged = damaged.clone();
            move |db| {
                for key in &damaged {
                    set_meta(db, key, &json!({"damaged":true}))?;
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    let pending = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(pending["items"], json!([]));
    assert_eq!(pending["gaps"][0]["after_index_key"], damaged[63]);
    let gap = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(gap["items"], json!([]));
    assert_eq!(gap["next_after_key"], damaged[35]);
    assert!(
        gap["gaps"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["kind"] == "damaged_mailbox_indexes" && entry["count"] == 36)
    );
    let checkpoint_key = format!(
        "coordination:mailbox-normalization:{}",
        crate::coordination::key_component(&prefix)
    );
    let completed: (String, i64) = owner
        .store
        .run({
            let checkpoint_key = checkpoint_key.clone();
            move |db| {
                let raw: String = db.query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [&checkpoint_key],
                    |row| row.get(0),
                )?;
                let record: Value = serde_json::from_str(&raw)?;
                assert_eq!(record["state"], "complete");
                assert_eq!(record["damaged_count"], 70);
                let changes = db.query_row("SELECT total_changes()", [], |row| row.get(0))?;
                Ok((raw, changes))
            }
        })
        .await
        .unwrap();
    // Retained poison stays a source gap, never a reason to restart the sweep.
    let first = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1,"after_key":gap["next_after_key"]}),
        )
        .await
        .unwrap();
    assert_eq!(first["items"][0]["operation_id"], ids[0]);
    let mut after = first["next_after"].clone();
    for expected in ids.iter().skip(1) {
        let page = owner
            .store
            .call(
                recipient.clone(),
                "coordination.inbox".into(),
                json!({"limit":1,"after_operation_id":after}),
            )
            .await
            .unwrap();
        assert_eq!(page["items"].as_array().unwrap().len(), 1);
        assert_eq!(
            page["items"][0]["operation_id"].as_str(),
            Some(expected.as_str())
        );
        assert_eq!(page["coverage"], "partial");
        assert!(
            page["gaps"]
                .as_array()
                .unwrap()
                .iter()
                .any(
                    |entry| entry["kind"] == "legacy_mailbox_normalization_damage"
                        && entry["count"] == 70
                )
        );
        assert!(
            !page["gaps"]
                .as_array()
                .unwrap()
                .iter()
                .any(|entry| entry["kind"] == "inbox_normalization_pending")
        );
        after = page["next_after"].clone();
    }
    let empty = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1,"after_operation_id":after}),
        )
        .await
        .unwrap();
    assert_eq!(empty["items"], json!([]));
    assert_eq!(empty["coverage"], "partial");
    owner
        .store
        .run({
            let checkpoint_key = checkpoint_key.clone();
            move |db| {
                let raw: String = db.query_row(
                    "SELECT value_json FROM meta WHERE key=?1",
                    [&checkpoint_key],
                    |row| row.get(0),
                )?;
                let changes: i64 = db.query_row("SELECT total_changes()", [], |row| row.get(0))?;
                assert_eq!((raw, changes), completed);
                Ok(())
            }
        })
        .await
        .unwrap();
    // Neither same-byte legacy updates nor new ordinary v2 deliveries rearm.
    owner
        .store
        .run({
            let key = damaged[0].clone();
            let checkpoint_key = checkpoint_key.clone();
            move |db| {
                db.execute("UPDATE meta SET value_json=value_json WHERE key=?1", [&key])?;
                assert_eq!(meta(db, &checkpoint_key)?.unwrap()["state"], "complete");
                Ok(())
            }
        })
        .await
        .unwrap();
    let sent = owner.store.call(sender, "coordination.send".into(), json!({"client_request_id":"v2-after-poison-completion","recipient":recipient.client_id,"body":{"fresh":true}})).await.unwrap();
    let fresh = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1,"after_operation_id":ids.last().unwrap()}),
        )
        .await
        .unwrap();
    assert_eq!(fresh["items"][0]["operation_id"], sent["operation_id"]);
    // A meaningful scoped legacy change does rearm exactly once.
    owner
        .store
        .run({
            let key = damaged[0].clone();
            let checkpoint_key = checkpoint_key.clone();
            move |db| {
                set_meta(db, &key, &json!({"damaged":"changed"}))?;
                assert!(meta(db, &checkpoint_key)?.is_none());
                Ok(())
            }
        })
        .await
        .unwrap();
    let rearmed = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1,"after_operation_id":ids[0]}),
        )
        .await
        .unwrap();
    assert_eq!(rearmed["items"], json!([]));
    assert_eq!(rearmed["gaps"][0]["kind"], "inbox_normalization_pending");
    let complete = owner
        .store
        .call(
            recipient,
            "coordination.inbox".into(),
            json!({"limit":1,"after_operation_id":ids[0]}),
        )
        .await
        .unwrap();
    assert_eq!(complete["items"][0]["operation_id"], ids[1]);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn coordination_inbox_normalization_delete_failure_rolls_back_locator_batch() {
    let (owner, directory, subject, sender, recipient) =
        start_inbox_participants("inbox-normalization-rollback").await;
    let (ids, legacy) =
        retain_reversed_legacy_inbox(&owner.store, &subject, &sender, &recipient, 66).await;
    // Reverse timestamp order visits index 65 first and fails deleting 64:
    // both the preceding successful replacement and the failing one roll back.
    owner
        .store
        .run({
            let fail_key = legacy[64].0.clone();
            move |db| {
                db.execute_batch(
                    "CREATE TEMP TABLE normalization_delete_fault(key TEXT PRIMARY KEY);
                CREATE TEMP TRIGGER abort_mailbox_legacy_delete BEFORE DELETE ON main.meta
                WHEN OLD.key IN (SELECT key FROM normalization_delete_fault)
                BEGIN SELECT RAISE(ABORT,'injected legacy delete failure'); END;",
                )?;
                db.execute(
                    "INSERT INTO normalization_delete_fault(key) VALUES(?1)",
                    [&fail_key],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();
    assert!(
        owner
            .store
            .call(
                recipient.clone(),
                "coordination.inbox".into(),
                json!({"limit":1})
            )
            .await
            .is_err()
    );
    owner.store.run(move |db| {
        for (key, raw) in legacy {
            let retained: String = db.query_row("SELECT value_json FROM meta WHERE key=?1", [&key], |row| row.get(0))?;
            assert_eq!(retained, raw);
        }
        let checkpoint_count: i64 = db.query_row("SELECT count(*) FROM meta WHERE key LIKE 'coordination:mailbox-normalization:%'", [], |row| row.get(0))?;
        assert_eq!(checkpoint_count, 0);
        for id in ids.iter().take(66) {
            let position: i64 = db.query_row("SELECT observation_id FROM observations WHERE source_stream_id='controller:read-position:operation' AND source_event_key=?1", [id], |row| row.get(0))?;
            let suffix = format!("%v2:{position:020}:{}", crate::coordination::key_component(id));
            let count: i64 = db.query_row("SELECT count(*) FROM meta WHERE key LIKE ?1", [suffix], |row| row.get(0))?;
            assert_eq!(count, 0);
        }
        db.execute_batch("DROP TRIGGER temp.abort_mailbox_legacy_delete; DROP TABLE temp.normalization_delete_fault;")?;
        Ok(())
    }).await.unwrap();
    let pending = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit":1}),
        )
        .await
        .unwrap();
    assert_eq!(pending["items"], json!([]));
    let recovered = owner
        .store
        .call(recipient, "coordination.inbox".into(), json!({"limit":1}))
        .await
        .unwrap();
    assert_eq!(recovered["items"][0]["body"]["seq"], 0);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn mailbox_mixed_legacy_v2_canonical_order_with_bounded_normalization() {
    // PR34: one retained read-position sequence orders every valid mailbox
    // delivery. Legacy timestamp locators sort before all v2 locators and
    // same-stamp legacy locators sort by Operation ID, so a mixed namespace
    // cannot page canonically until valid legacy locators are retired into
    // v2 positions. New writes stay v2-only; each inbox read retires a
    // bounded window while damaged rows stay explicit for after_key resume.
    const MESSAGE_COUNT: usize = 8;

    let (owner, directory, subject, sender, recipient) =
        start_inbox_participants("mailbox-mixed-canonical").await;
    let sender_id = sender.client_id.clone();
    let recipient_id = recipient.client_id.clone();
    let task_id = subject.task_id.clone();
    let attempt_id = subject.attempt_id.clone();

    let mut operation_ids = Vec::with_capacity(MESSAGE_COUNT);
    for index in 0..MESSAGE_COUNT {
        let sent = owner
            .store
            .call(
                sender.clone(),
                "coordination.send".into(),
                json!({
                    "client_request_id": format!("mixed-canonical-{index}"),
                    "recipient": recipient_id,
                    "body": {"seq": index},
                }),
            )
            .await
            .unwrap();
        operation_ids.push(sent["operation_id"].as_str().unwrap().to_owned());
    }

    let scope_id = crate::coordination::scope_id(&subject.task_id, 1, &subject.attempt_id).unwrap();
    let prefix = crate::coordination::mailbox_prefix(&scope_id, &recipient_id);
    let legacy_upper = format!("{prefix}v");
    let v2_lower = format!("{prefix}v2:");
    let v2_upper = format!("{prefix}v3");
    let malformed_key = format!("{prefix}!malformed");

    // Read back every admitted stamp and v2 locator before crafting history.
    let (admitted, index_rows): (Vec<(String, i64)>, Vec<(String, String)>) = owner
        .store
        .run({
            let operation_ids = operation_ids.clone();
            let prefix = prefix.clone();
            move |db| {
                let mailbox_upper = format!("{prefix}z");
                let mut admitted = Vec::with_capacity(operation_ids.len());
                for operation_id in &operation_ids {
                    let created_at: i64 = db.query_row(
                        "SELECT created_at_ms FROM operations WHERE operation_id=?1",
                        [operation_id],
                        |row| row.get(0),
                    )?;
                    admitted.push((operation_id.clone(), created_at));
                }
                let mut statement = db.prepare(
                    "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key",
                )?;
                let index_rows: Vec<(String, String)> = statement
                    .query_map(rusqlite::params![prefix, mailbox_upper], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<std::result::Result<_, _>>()?;
                Ok((admitted, index_rows))
            }
        })
        .await
        .unwrap();
    assert_eq!(admitted.len(), MESSAGE_COUNT);
    let mut created_of = std::collections::HashMap::new();
    for (operation_id, created_at) in &admitted {
        created_of.insert(operation_id.clone(), *created_at);
    }
    let mut v2key_of = std::collections::HashMap::new();
    for (key, raw) in &index_rows {
        let index: Value = serde_json::from_str(raw).unwrap();
        if let Some(operation_id) = index["operation_id"].as_str() {
            v2key_of.insert(operation_id.to_owned(), key.clone());
        }
    }
    assert_eq!(v2key_of.len(), MESSAGE_COUNT);
    let hexes: Vec<String> = operation_ids
        .iter()
        .map(|operation_id| {
            v2key_of[operation_id.as_str()]
                .rsplit(':')
                .next()
                .unwrap_or_default()
                .to_owned()
        })
        .collect();
    // Find a same-stamp pair whose Operation-ID tiebreak provably disagrees
    // with canonical position order; fall back deterministically otherwise.
    let mut pair = (1usize, 2usize);
    for i in 0..MESSAGE_COUNT {
        for j in (i + 1)..MESSAGE_COUNT {
            if hexes[i] > hexes[j] {
                pair = (i, j);
            }
        }
    }
    let (pair_a, pair_b) = pair;
    let mut rest: Vec<usize> = (0..MESSAGE_COUNT)
        .filter(|index| *index != pair_a && *index != pair_b)
        .collect();
    let duplicate_index = rest.remove(0);
    let missing_index = rest.remove(0);
    let interleave_a = rest.remove(0);
    let interleave_b = rest.remove(0);
    let stamp_a = created_of[operation_ids[pair_a].as_str()];
    let badhex_key = format!("{prefix}{stamp_a:020}:zzzz");
    let trailing_key = format!("{prefix}v9:trailing");
    let legacy_index = |operation_id: &str, stamp: i64| {
        json!({
            "operation_id": operation_id,
            "sender": sender_id.as_str(),
            "recipient": recipient_id.as_str(),
            "task_id": task_id.as_str(),
            "task_revision": 1,
            "attempt_id": attempt_id.as_str(),
            "created_at_ms": stamp,
        })
    };
    let mut retire_keys = Vec::new();
    let mut legacy_rows: Vec<(String, Value)> = Vec::new();
    for index in [pair_a, pair_b, interleave_a, interleave_b] {
        let stamp = if index == pair_a || index == pair_b {
            stamp_a
        } else {
            created_of[operation_ids[index].as_str()]
        };
        retire_keys.push(v2key_of[operation_ids[index].as_str()].clone());
        legacy_rows.push((
            crate::coordination::mailbox_key(
                &scope_id,
                &recipient_id,
                stamp,
                &operation_ids[index],
            ),
            legacy_index(operation_ids[index].as_str(), stamp),
        ));
    }
    let duplicate_id = operation_ids[duplicate_index].clone();
    let duplicate_stamp = created_of[duplicate_id.as_str()];
    let duplicate_legacy = (
        crate::coordination::mailbox_key(&scope_id, &recipient_id, duplicate_stamp, &duplicate_id),
        legacy_index(duplicate_id.as_str(), duplicate_stamp),
    );
    let missing_id = operation_ids[missing_index].clone();
    let malformed_rows: Vec<(String, Value)> = vec![
        (malformed_key.clone(), json!({"damaged": true})),
        (badhex_key.clone(), json!({"damaged": true})),
        (trailing_key.clone(), json!({"damaged": true})),
    ];
    // Craft history: same-stamp legacy pair, interleaved legacy locators, one
    // dual-locator delivery, one missing read position, malformed rows.
    owner
        .store
        .run({
            let pair_b_id = operation_ids[pair_b].clone();
            move |db| {
                db.execute(
                    "UPDATE operations SET created_at_ms=?1 WHERE operation_id=?2",
                    rusqlite::params![stamp_a, pair_b_id],
                )?;
                db.execute(
                    "UPDATE observations SET recorded_at_ms=?1 \
                     WHERE source_stream_id='controller:read-position:operation' \
                       AND source_event_key=?2",
                    rusqlite::params![stamp_a, pair_b_id],
                )?;
                for key in &retire_keys {
                    assert_eq!(db.execute("DELETE FROM meta WHERE key=?1", [key])?, 1);
                }
                for (key, index) in &legacy_rows {
                    set_meta(db, key, index)?;
                }
                set_meta(db, &duplicate_legacy.0, &duplicate_legacy.1)?;
                let deleted = db.execute(
                    "DELETE FROM observations \
                     WHERE source_stream_id='controller:read-position:operation' \
                       AND source_event_key=?1",
                    [&missing_id],
                )?;
                assert_eq!(deleted, 1);
                for (key, index) in &malformed_rows {
                    set_meta(db, key, index)?;
                }
                Ok(())
            }
        })
        .await
        .unwrap();
    // Authoritative Operation and message bytes snapshotted after crafting;
    // bounded normalization must never rewrite them.
    let snapshot = owner
        .store
        .run({
            let operation_ids = operation_ids.clone();
            move |db| {
                let mut snapshot = Vec::with_capacity(operation_ids.len());
                for operation_id in &operation_ids {
                    let row: (String, String, i64, String) = db.query_row(
                        "SELECT original_request_json,result_json,created_at_ms,state \
                         FROM operations WHERE operation_id=?1",
                        [operation_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )?;
                    snapshot.push((operation_id.clone(), row));
                }
                Ok(snapshot)
            }
        })
        .await
        .unwrap();
    let want: Vec<String> = operation_ids
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != missing_index)
        .map(|(_, operation_id)| operation_id.clone())
        .collect();
    // requirements 1 + 2: interleaved and same-stamp legacy deliveries page
    // once in canonical read-position order, never timestamp/Operation-ID order.
    let full = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit": 50}),
        )
        .await
        .unwrap();
    let items = full["items"].as_array().unwrap();
    let got: Vec<String> = items
        .iter()
        .map(|item| item["operation_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(got, want);
    // requirement 6: the retired dual-locator delivery is never duplicated.
    assert_eq!(
        got.iter()
            .filter(|operation_id| operation_id.as_str() == duplicate_id.as_str())
            .count(),
        1
    );
    // requirements 4 + 7: malformed and missing-position rows stay explicit
    // partial-coverage damage with a resumable trailing key cursor.
    assert_eq!(full["coverage"], "partial");
    let gaps = full["gaps"].as_array().unwrap();
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0]["kind"], json!("damaged_mailbox_indexes"));
    assert_eq!(gaps[0]["count"], json!(4));
    assert_eq!(full["next_after"], Value::Null);
    assert_eq!(full["next_after_key"], trailing_key);
    // every valid legacy locator retired; only damaged rows keep old keys.
    let legacy_keys: Vec<String> = owner
        .store
        .run({
            let prefix = prefix.clone();
            let legacy_upper = legacy_upper.clone();
            move |db| {
                let mut statement =
                    db.prepare("SELECT key FROM meta WHERE key>=?1 AND key<?2 ORDER BY key")?;
                let rows: Vec<String> = statement
                    .query_map(rusqlite::params![prefix, legacy_upper], |row| row.get(0))?
                    .collect::<std::result::Result<_, _>>()?;
                Ok(rows)
            }
        })
        .await
        .unwrap();
    assert_eq!(legacy_keys, vec![malformed_key.clone(), badhex_key.clone()]);
    let v2_count: i64 = owner
        .store
        .run({
            let v2_lower = v2_lower.clone();
            let v2_upper = v2_upper.clone();
            move |db| {
                Ok(db.query_row(
                    "SELECT COUNT(*) FROM meta WHERE key>=?1 AND key<?2",
                    rusqlite::params![v2_lower, v2_upper],
                    |row| row.get(0),
                )?)
            }
        })
        .await
        .unwrap();
    assert_eq!(v2_count, MESSAGE_COUNT as i64);
    // requirement 3 + 5: after_operation_id resumes across the retired
    // legacy/v2 boundary and item budgets preserve the first excluded item.
    let first = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit": 2}),
        )
        .await
        .unwrap();
    let first_items = first["items"].as_array().unwrap();
    assert_eq!(first_items.len(), 2);
    assert_eq!(first_items[0]["operation_id"], want[0]);
    assert_eq!(first_items[1]["operation_id"], want[1]);
    let second = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit": 2, "after_operation_id": first_items[1]["operation_id"]}),
        )
        .await
        .unwrap();
    let second_items = second["items"].as_array().unwrap();
    assert_eq!(second_items.len(), 2);
    assert_eq!(second_items[0]["operation_id"], want[2]);
    assert_eq!(second_items[1]["operation_id"], want[3]);
    // requirement 4: the damaged trailing key resumes without consuming.
    let resumed = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit": 50, "after_key": full["next_after_key"]}),
        )
        .await
        .unwrap();
    assert!(resumed["items"].as_array().unwrap().is_empty());
    assert_eq!(resumed["coverage"], "partial");
    assert_eq!(
        resumed["gaps"][0]["kind"],
        "legacy_mailbox_normalization_damage"
    );
    assert_eq!(resumed["gaps"][0]["count"], 2);
    // requirement 8: repeated bounded migration/page calls are idempotent.
    let again = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit": 50}),
        )
        .await
        .unwrap();
    assert_eq!(again["items"], full["items"]);
    assert_eq!(again["next_after"], full["next_after"]);
    assert_eq!(again["next_after_key"], full["next_after_key"]);
    // requirement 6, explicit half: a legacy twin beside a damaged v2 twin
    // stays explicit damage without duplicating the message ...
    let target = want[5].clone();
    let found: Option<(String, String)> = owner
        .store
        .run({
            let target = target.clone();
            move |db| {
                let mut statement = db.prepare(
                    "SELECT key,value_json FROM meta WHERE key>=?1 AND key<?2 ORDER BY key",
                )?;
                let rows: Vec<(String, String)> = statement
                    .query_map(rusqlite::params![v2_lower, v2_upper], |row| {
                        Ok((row.get(0)?, row.get(1)?))
                    })?
                    .collect::<std::result::Result<_, _>>()?;
                for (key, raw) in rows {
                    let index: Value = serde_json::from_str(&raw).unwrap();
                    if index["operation_id"].as_str() == Some(target.as_str()) {
                        return Ok(Some((key, raw)));
                    }
                }
                Ok(None)
            }
        })
        .await
        .unwrap();
    let (target_key, target_json) = found.unwrap();
    owner
        .store
        .run({
            let target = target.clone();
            let target_key = target_key.clone();
            move |db| {
                let stamp: i64 = db.query_row(
                    "SELECT created_at_ms FROM operations WHERE operation_id=?1",
                    [&target],
                    |row| row.get(0),
                )?;
                let twin =
                    crate::coordination::mailbox_key(&scope_id, &recipient_id, stamp, &target);
                let damaged = model::canonical(&json!({"damaged": true}))?;
                db.execute(
                    "UPDATE meta SET value_json=?1 WHERE key=?2",
                    rusqlite::params![damaged, target_key],
                )?;
                set_meta(
                    db,
                    &twin,
                    &json!({
                        "operation_id": target.as_str(),
                        "sender": sender_id,
                        "recipient": recipient_id,
                        "task_id": task_id,
                        "task_revision": 1,
                        "attempt_id": attempt_id,
                        "created_at_ms": stamp,
                    }),
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();
    let conflicted = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit": 50}),
        )
        .await
        .unwrap();
    let conflicted_items = conflicted["items"].as_array().unwrap();
    assert_eq!(conflicted_items.len(), MESSAGE_COUNT - 2);
    assert!(
        !conflicted_items
            .iter()
            .any(|item| item["operation_id"] == target)
    );
    assert_eq!(conflicted["coverage"], "partial");
    // ... and resolves onto the repaired twin without duplicating.
    owner
        .store
        .run({
            let prefix = prefix.clone();
            move |db| {
                db.execute(
                    "UPDATE meta SET value_json=?1 WHERE key=?2",
                    rusqlite::params![target_json, target_key],
                )?;
                // Evidence/twin repair explicitly invalidates only its scoped
                // scheduling hint; it does not rewrite Operation authority.
                db.execute(
                    "DELETE FROM meta WHERE key=?1",
                    [format!(
                        "coordination:mailbox-normalization:{}",
                        crate::coordination::key_component(&prefix)
                    )],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();
    let resolved = owner
        .store
        .call(
            recipient.clone(),
            "coordination.inbox".into(),
            json!({"limit": 50}),
        )
        .await
        .unwrap();
    let resolved_items = resolved["items"].as_array().unwrap();
    let resolved_got: Vec<String> = resolved_items
        .iter()
        .map(|item| item["operation_id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(resolved_got, want);
    assert_eq!(resolved["next_after_key"], trailing_key);
    let legacy_final: Vec<String> = owner
        .store
        .run({
            move |db| {
                let mut statement =
                    db.prepare("SELECT key FROM meta WHERE key>=?1 AND key<?2 ORDER BY key")?;
                let rows: Vec<String> = statement
                    .query_map(rusqlite::params![prefix, legacy_upper], |row| row.get(0))?
                    .collect::<std::result::Result<_, _>>()?;
                Ok(rows)
            }
        })
        .await
        .unwrap();
    assert_eq!(legacy_final, vec![malformed_key, badhex_key]);
    let snapshot_final = owner
        .store
        .run({
            move |db| {
                let mut snapshot = Vec::with_capacity(operation_ids.len());
                for operation_id in &operation_ids {
                    let row: (String, String, i64, String) = db.query_row(
                        "SELECT original_request_json,result_json,created_at_ms,state \
                         FROM operations WHERE operation_id=?1",
                        [operation_id],
                        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
                    )?;
                    snapshot.push((operation_id.clone(), row));
                }
                Ok(snapshot)
            }
        })
        .await
        .unwrap();
    assert_eq!(snapshot_final, snapshot);
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
