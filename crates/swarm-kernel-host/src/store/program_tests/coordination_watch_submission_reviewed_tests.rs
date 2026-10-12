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
