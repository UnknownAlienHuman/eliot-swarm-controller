use super::*;
use crate::{
    config::Config,
    platform::{DataRoot, bootstrap_credential},
};
use std::sync::Arc;

async fn start() -> (StoreOwner, Principal) {
    let directory = std::env::temp_dir().join(format!("eliot-mailbox-test-{}", model::new_id()));
    std::fs::create_dir_all(&directory).unwrap();
    let root = DataRoot::acquire(&directory).unwrap();
    let credential = bootstrap_credential(&root.path).unwrap();
    let mut cfg = Config::default();
    cfg.storage.data_dir = directory;
    let owner = StoreOwner::start(root, Arc::new(cfg), credential.clone())
        .await
        .unwrap();
    let p = owner.store.authenticate(credential).await.unwrap();
    (owner, p)
}

async fn register(store: &Store, operator: &Principal, client_id: &str) -> Principal {
    register_with_role(store, operator, client_id, "manager").await
}

async fn register_with_role(
    store: &Store,
    operator: &Principal,
    client_id: &str,
    role: &str,
) -> Principal {
    let token = format!("token-for-{client_id}");
    let registered = write(
        store,
        operator,
        "client.register",
        json!({"client_id":client_id,"role":role,"token_hash":model::digest(token.as_bytes())}),
    )
    .await
    .unwrap();
    assert_eq!(registered["client_id"], client_id);
    store
        .authenticate(Credential {
            client_id: client_id.into(),
            token,
        })
        .await
        .unwrap()
}

async fn write(store: &Store, p: &Principal, method: &str, mut input: Value) -> Result<Value> {
    input["client_request_id"] = json!(model::new_id());
    store.call(p.clone(), method.into(), input).await
}

async fn read(store: &Store, p: &Principal, method: &str, input: Value) -> Value {
    store.call(p.clone(), method.into(), input).await.unwrap()
}

#[tokio::test]
async fn report_delta_head_pages_past_invisible_operations_without_cross_scope_disclosure() {
    const FOREIGN_EVENTS: usize = 205;

    let (owner, operator) = start().await;
    let authorized =
        register_with_role(&owner.store, &operator, "head-authorized", "observer").await;
    let empty = register_with_role(&owner.store, &operator, "head-empty", "observer").await;
    let visible_operation_id = model::new_id();
    let foreign_operation_ids = (0..FOREIGN_EVENTS)
        .map(|_| model::new_id())
        .collect::<Vec<_>>();
    let inserted_foreign_operation_ids = foreign_operation_ids.clone();
    let visible_id = visible_operation_id.clone();
    let visible_caller = authorized.client_id.clone();
    let (visible_cursor, latest_foreign_cursor) = owner
        .store
        .run(move |db| {
            let visible_result = json!({"operation_id":visible_id,"fixture":"authorized"});
            insert_legacy_operation(
                db,
                &visible_id,
                &visible_caller,
                "head-visible-request",
                "fixture.subscription_event",
                &json!({}),
                &visible_result,
            )?;
            let visible_payload = json!({"operation_id":visible_id,"fixture":"authorized"});
            db.execute(
                "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
                 VALUES('fixture:subscription-head',?1,?1,'fixture.visible_event',?2,1000)",
                params![visible_id, model::canonical(&visible_payload)?],
            )?;
            let visible_cursor = db.last_insert_rowid();

            let mut latest_foreign_cursor = 0;
            for (index, operation_id) in inserted_foreign_operation_ids.iter().enumerate() {
                let result = json!({"operation_id":operation_id,"fixture":"foreign"});
                insert_legacy_operation(
                    db,
                    operation_id,
                    "unrelated-manager",
                    &format!("head-foreign-request-{index}"),
                    "fixture.subscription_event",
                    &json!({}),
                    &result,
                )?;
                let payload = json!({
                    "operation_id":operation_id,
                    "secret":"foreign subscription payload"
                });
                db.execute(
                    "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) \
                     VALUES('fixture:subscription-head',?1,?1,'fixture.foreign_event',?2,1000)",
                    params![operation_id, model::canonical(&payload)?],
                )?;
                latest_foreign_cursor = db.last_insert_rowid();
            }
            Ok((visible_cursor, latest_foreign_cursor))
        })
        .await
        .unwrap();

    let first = read(
        &owner.store,
        &authorized,
        "report.delta",
        json!({"head":true}),
    )
    .await;
    assert_eq!(first["admission"]["state"], "continuation");
    assert!(first["cursor"].is_null());
    let first_wire = serde_json::to_string(&first).unwrap();
    assert!(!first_wire.contains("fixture.foreign_event"));
    assert!(!first_wire.contains("foreign subscription payload"));
    assert!(
        foreign_operation_ids
            .iter()
            .all(|operation_id| !first_wire.contains(operation_id))
    );

    let second = read(
        &owner.store,
        &authorized,
        "report.delta",
        json!({
            "head":true,
            "head_continuation":first["admission"]["continuation"]
        }),
    )
    .await;
    assert_eq!(second["admission"]["state"], "established");
    assert_eq!(second["admission"]["empty"], false);
    assert_eq!(second["cursor"], json!(visible_cursor));

    let cross_scope = read(
        &owner.store,
        &authorized,
        "report.delta",
        json!({"after":visible_cursor,"limit":200}),
    )
    .await;
    assert_eq!(cross_scope["items"], json!([]));
    let cross_scope_wire = serde_json::to_string(&cross_scope).unwrap();
    assert!(!cross_scope_wire.contains("fixture.foreign_event"));
    assert!(!cross_scope_wire.contains("foreign subscription payload"));
    assert!(
        foreign_operation_ids
            .iter()
            .all(|operation_id| !cross_scope_wire.contains(operation_id))
    );

    let empty_first = read(&owner.store, &empty, "report.delta", json!({"head":true})).await;
    assert_eq!(empty_first["admission"]["state"], "continuation");
    assert!(empty_first["cursor"].is_null());
    let empty_second = read(
        &owner.store,
        &empty,
        "report.delta",
        json!({
            "head":true,
            "head_continuation":empty_first["admission"]["continuation"]
        }),
    )
    .await;
    assert_eq!(empty_second["admission"]["state"], "established");
    assert_eq!(empty_second["admission"]["empty"], true);
    assert_eq!(empty_second["cursor"], json!(latest_foreign_cursor));
    owner.close().await.unwrap();
}

#[tokio::test]
async fn send_records_the_full_communication_identity() {
    let (owner, operator) = start().await;
    let alice = register(&owner.store, &operator, "alice").await;
    let bob = register(&owner.store, &operator, "bob").await;
    let sent = write(
        &owner.store,
        &alice,
        "message.send",
        json!({"recipient":"bob","text":"hello","reply_deadline_ms":4102444800000i64}),
    )
    .await
    .unwrap();
    // The historical alias is retained, and the delivery has its own identity.
    assert_eq!(sent["message_id"], sent["operation_id"]);
    let delivery_id = sent["delivery_id"].as_str().unwrap();
    assert_ne!(delivery_id, sent["operation_id"].as_str().unwrap());
    uuid::Uuid::parse_str(delivery_id).unwrap();
    assert_eq!(
        sent["payload_digest"],
        model::message_payload_digest("alice", "bob", "hello").unwrap()
    );
    assert_eq!(
        sent["actor"],
        json!({"client_id":"alice","role":"manager","generation":Value::Null})
    );
    assert_eq!(sent["source_scope"]["client_id"], "alice");
    assert_eq!(sent["source_scope"]["binding_id"], Value::Null);
    assert_eq!(sent["target_scope"]["client_id"], "bob");
    // Deadlines the caller did not supply are explicit nulls, never invented.
    assert_eq!(sent["admission_deadline_ms"], Value::Null);
    assert_eq!(sent["delivery_deadline_ms"], Value::Null);
    assert_eq!(sent["reply_deadline_ms"], json!(4102444800000i64));
    assert_eq!(sent["reply_to"], Value::Null);
    assert_eq!(sent["cancellation"], Value::Null);
    // The durable mailbox projection carries the same record.
    let mail = read(&owner.store, &bob, "message.read", json!({})).await;
    assert_eq!(mail["items"][0]["payload"]["delivery_id"], delivery_id);
    assert_eq!(
        mail["items"][0]["payload"]["payload_digest"],
        sent["payload_digest"]
    );
    // A deadline that is not a positive epoch-ms integer is rejected.
    let err = write(
        &owner.store,
        &alice,
        "message.send",
        json!({"recipient":"bob","text":"x","delivery_deadline_ms":0}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "INVALID_PARAMS");
    owner.close().await.unwrap();
}

#[tokio::test]
async fn report_delta_scopes_mail_to_its_recipient_and_paginates_visible_rows() {
    let (owner, operator) = start().await;
    let sender = register(&owner.store, &operator, "sender").await;
    let recipient = register_with_role(&owner.store, &operator, "recipient", "observer").await;
    let other = register_with_role(&owner.store, &operator, "other", "observer").await;

    // Start after setup observations so each page below contains only the
    // deliberately interleaved directed deliveries.
    let checkpoint = read(
        &owner.store,
        &recipient,
        "report.delta",
        json!({"limit":200}),
    )
    .await["next_cursor"]
        .as_i64()
        .unwrap();

    let hidden_before = write(
        &owner.store,
        &sender,
        "message.send",
        json!({"recipient":"other","text":"private to other before"}),
    )
    .await
    .unwrap();
    let own_first = write(
        &owner.store,
        &sender,
        "message.send",
        json!({"recipient":"recipient","text":"first for recipient"}),
    )
    .await
    .unwrap();
    let hidden_middle = write(
        &owner.store,
        &sender,
        "message.send",
        json!({"recipient":"other","text":"private to other between"}),
    )
    .await
    .unwrap();
    let own_second = write(
        &owner.store,
        &sender,
        "message.send",
        json!({"recipient":"recipient","text":"second for recipient"}),
    )
    .await
    .unwrap();
    let hidden_tail = write(
        &owner.store,
        &sender,
        "message.send",
        json!({"recipient":"other","text":"private to other after"}),
    )
    .await
    .unwrap();
    let cancellation = write(
        &owner.store,
        &sender,
        "message.cancel",
        json!({
            "delivery_id":hidden_before["delivery_id"],
            "payload_digest":hidden_before["payload_digest"],
            "reason":"private cancellation reason"
        }),
    )
    .await
    .unwrap();

    // Make the relevant Operation order deterministic while keeping all
    // fixture events inside the settled-time bound. Registrations precede
    // the deliberately interleaved directed sends.
    let ordered_ids = [
        &hidden_before,
        &own_first,
        &hidden_middle,
        &own_second,
        &hidden_tail,
        &cancellation,
    ]
    .into_iter()
    .map(|operation| operation["operation_id"].as_str().unwrap().to_owned())
    .collect::<Vec<_>>();
    owner
        .store
        .run(move |db| {
            let latest: i64 = db.query_row(
                "SELECT COALESCE(MAX(updated_at_ms),0) FROM operations",
                [],
                |row| row.get(0),
            )?;
            let base = latest - 100;
            db.execute(
                "UPDATE operations SET created_at_ms=?1 WHERE method='client.register'",
                [base],
            )?;
            for (index, operation_id) in ordered_ids.iter().enumerate() {
                db.execute(
                    "UPDATE operations SET created_at_ms=?2 WHERE operation_id=?1",
                    params![operation_id, base + index as i64 + 1],
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();

    // The send caller and recipient can recover the immutable receipt, while
    // another client gets the same NOT_FOUND as for a nonexistent operation.
    let own_operation = read(
        &owner.store,
        &recipient,
        "operation.get",
        json!({"operation_id":own_first["operation_id"]}),
    )
    .await;
    assert_eq!(
        own_operation["result"]["operation_id"],
        own_first["operation_id"]
    );
    assert_eq!(
        own_operation["result"]["delivery_id"],
        own_first["delivery_id"]
    );
    assert_eq!(
        own_operation["result"]["payload_digest"],
        own_first["payload_digest"]
    );
    assert!(own_operation["result"].get("text").is_none());
    let mailbox = read(&owner.store, &recipient, "message.read", json!({})).await;
    let own_message = mailbox["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["payload"]["delivery_id"] == own_first["delivery_id"])
        .expect("the recipient mailbox contains the exact addressed delivery");
    assert_eq!(own_message["payload"]["text"], "first for recipient");
    let sender_operation = read(
        &owner.store,
        &sender,
        "operation.get",
        json!({"operation_id":own_first["operation_id"]}),
    )
    .await;
    assert_eq!(sender_operation, own_operation);
    let foreign_send = owner
        .store
        .call(
            recipient.clone(),
            "operation.get".into(),
            json!({"operation_id":hidden_before["operation_id"]}),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign_send.code, "NOT_FOUND");

    // Operation pagination filters before applying LIMIT, so a hidden row
    // between setup and the recipient's send does not shorten or shift page 1.
    let first_operation_page = read(
        &owner.store,
        &recipient,
        "operation.list",
        json!({"state":"settled","limit":1}),
    )
    .await;
    assert_eq!(first_operation_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        first_operation_page["items"][0]["operation_id"],
        own_first["operation_id"]
    );
    assert_eq!(first_operation_page["has_newer"], true);
    let second_operation_page = read(
        &owner.store,
        &recipient,
        "operation.list",
        json!({"state":"settled","after":first_operation_page["next_after"],"limit":1}),
    )
    .await;
    assert_eq!(second_operation_page["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        second_operation_page["items"][0]["operation_id"],
        own_second["operation_id"]
    );
    assert_ne!(
        second_operation_page["items"][0]["operation_id"],
        first_operation_page["items"][0]["operation_id"]
    );
    assert_eq!(second_operation_page["has_newer"], false);
    let operation_pages_json = json!([&first_operation_page, &second_operation_page]).to_string();
    for hidden in [&hidden_before, &hidden_middle, &hidden_tail] {
        assert!(!operation_pages_json.contains(hidden["operation_id"].as_str().unwrap()));
        assert!(!operation_pages_json.contains(hidden["delivery_id"].as_str().unwrap()));
        assert!(!operation_pages_json.contains(hidden["payload_digest"].as_str().unwrap()));
    }
    assert!(!operation_pages_json.contains(cancellation["operation_id"].as_str().unwrap()));
    assert!(!operation_pages_json.contains("private cancellation reason"));
    let recipient_operations = read(
        &owner.store,
        &recipient,
        "operation.list",
        json!({"limit":200}),
    )
    .await;
    let recipient_operations_json = recipient_operations.to_string();
    for hidden in [&hidden_before, &hidden_middle, &hidden_tail] {
        assert!(!recipient_operations_json.contains(hidden["operation_id"].as_str().unwrap()));
        assert!(!recipient_operations_json.contains(hidden["delivery_id"].as_str().unwrap()));
    }
    assert!(!recipient_operations_json.contains(cancellation["operation_id"].as_str().unwrap()));
    assert!(!recipient_operations_json.contains("private cancellation reason"));
    assert!(!recipient_operations_json.contains(hidden_before["payload_digest"].as_str().unwrap()));
    let other_operations = read(&owner.store, &other, "operation.list", json!({"limit":200})).await;
    let other_operations_json = other_operations.to_string();
    assert!(other_operations_json.contains(hidden_before["operation_id"].as_str().unwrap()));
    assert!(other_operations_json.contains(cancellation["operation_id"].as_str().unwrap()));
    assert!(!other_operations_json.contains("private cancellation reason"));
    assert!(other_operations_json.contains(hidden_before["payload_digest"].as_str().unwrap()));

    let foreign_cancel = owner
        .store
        .call(
            recipient.clone(),
            "operation.get".into(),
            json!({"operation_id":cancellation["operation_id"]}),
        )
        .await
        .unwrap_err();
    assert_eq!(foreign_cancel.code, "NOT_FOUND");
    let recipient_cancel = read(
        &owner.store,
        &other,
        "operation.get",
        json!({"operation_id":cancellation["operation_id"]}),
    )
    .await;
    assert_eq!(
        recipient_cancel["result"]["operation_id"],
        cancellation["operation_id"]
    );
    assert!(recipient_cancel["result"].get("reason").is_none());
    assert!(recipient_cancel["result"].get("cancellation").is_none());
    let sender_cancel = read(
        &owner.store,
        &sender,
        "operation.get",
        json!({"operation_id":cancellation["operation_id"]}),
    )
    .await;
    assert_eq!(sender_cancel, recipient_cancel);

    let operator_receipt = read(
        &owner.store,
        &operator,
        "operation.get",
        json!({"operation_id":hidden_before["operation_id"]}),
    )
    .await;
    assert_eq!(
        operator_receipt["result"]["operation_id"],
        hidden_before["operation_id"]
    );
    assert_eq!(
        operator_receipt["result"]["delivery_id"],
        hidden_before["delivery_id"]
    );
    assert_eq!(
        operator_receipt["result"]["payload_digest"],
        hidden_before["payload_digest"]
    );
    assert!(operator_receipt["result"].get("text").is_none());

    let first = read(
        &owner.store,
        &recipient,
        "report.delta",
        json!({"after":checkpoint,"limit":1}),
    )
    .await;
    assert_eq!(first["items"].as_array().unwrap().len(), 1);
    assert_eq!(first["items"][0]["kind"], "message.send");
    assert_eq!(first["items"][0]["payload"]["text"], "first for recipient");
    assert_eq!(first["items"][0]["payload"]["recipient"], "recipient");
    assert_eq!(
        first["items"][0]["payload"]["delivery_id"],
        own_first["delivery_id"]
    );
    assert_eq!(first["projection"]["has_newer"], true);
    let first_cursor = first["next_cursor"].as_i64().unwrap();

    let second = read(
        &owner.store,
        &recipient,
        "report.delta",
        json!({"after":first_cursor,"limit":1}),
    )
    .await;
    assert_eq!(second["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        second["items"][0]["payload"]["text"],
        "second for recipient"
    );
    assert_eq!(
        second["items"][0]["payload"]["delivery_id"],
        own_second["delivery_id"]
    );
    // A hidden-only tail does not leak through the pagination projection.
    assert_eq!(second["projection"]["has_newer"], false);

    let recipient_pages = json!([first, second]);
    let recipient_json = recipient_pages.to_string();
    for hidden in [&hidden_before, &hidden_middle, &hidden_tail] {
        assert!(!recipient_json.contains(hidden["delivery_id"].as_str().unwrap()));
    }
    assert!(!recipient_json.contains("private to other"));
    assert!(!recipient_json.contains("private cancellation reason"));
    assert!(!recipient_json.contains(cancellation["operation_id"].as_str().unwrap()));
    assert!(!recipient_json.contains(hidden_before["payload_digest"].as_str().unwrap()));

    let other_page = read(
        &owner.store,
        &other,
        "report.delta",
        json!({"after":checkpoint,"limit":200}),
    )
    .await;
    let other_items = other_page["items"].as_array().unwrap();
    assert_eq!(other_items.len(), 4);
    assert_eq!(
        other_items
            .iter()
            .filter(|item| item["kind"] == "message.send")
            .count(),
        3
    );
    assert_eq!(
        other_items
            .iter()
            .filter(|item| item["kind"] == "message.cancel")
            .count(),
        1
    );
    assert!(other_items.iter().any(|item| {
        item["payload"]["delivery_id"] == hidden_before["delivery_id"]
            && item["payload"]["text"] == "private to other before"
    }));
    assert!(other_items.iter().any(|item| {
        item["payload"]["delivery_id"] == hidden_middle["delivery_id"]
            && item["payload"]["text"] == "private to other between"
    }));
    assert!(other_items.iter().any(|item| {
        item["payload"]["delivery_id"] == hidden_tail["delivery_id"]
            && item["payload"]["text"] == "private to other after"
    }));
    assert!(other_items.iter().any(|item| {
        item["kind"] == "message.cancel"
            && item["operation_id"] == cancellation["operation_id"]
            && item["payload"].get("reason").is_none()
            && item["payload"]["cancellation"]["payload_digest"] == hidden_before["payload_digest"]
    }));

    let other_mail = read(
        &owner.store,
        &other,
        "message.read",
        json!({"after":checkpoint,"limit":200}),
    )
    .await;
    let mailbox_items = other_mail["items"].as_array().unwrap();
    assert_eq!(mailbox_items.len(), 3);
    assert!(
        mailbox_items
            .iter()
            .all(|item| item["kind"] == "message.send")
    );
    assert!(
        !other_mail
            .to_string()
            .contains("private cancellation reason")
    );

    let sender_timeline = read(
        &owner.store,
        &sender,
        "report.delta",
        json!({"after":checkpoint,"limit":200}),
    )
    .await;
    assert_eq!(sender_timeline["items"].as_array().unwrap().len(), 6);

    let operator_timeline = read(
        &owner.store,
        &operator,
        "report.delta",
        json!({"after":checkpoint,"limit":200}),
    )
    .await;
    assert_eq!(operator_timeline["items"].as_array().unwrap().len(), 6);
    owner.close().await.unwrap();
}

#[tokio::test]
async fn malformed_legacy_cancel_links_fail_closed_for_recipients() {
    let (owner, operator) = start().await;
    let sender = register(&owner.store, &operator, "legacy-sender").await;
    let recipient =
        register_with_role(&owner.store, &operator, "legacy-recipient", "observer").await;
    let checkpoint = read(
        &owner.store,
        &recipient,
        "report.delta",
        json!({"limit":200}),
    )
    .await["next_cursor"]
        .as_i64()
        .unwrap();

    let cases = [
        // A legacy source has a delivery ID and recipient but no digest, so
        // a cancel's claimed digest cannot establish the original identity.
        ("missing-digest", true, true, false),
        // A well-formed-looking cancellation reference without any source is
        // never sufficient to infer who the original recipient was.
        ("orphan", false, false, false),
        // Even a complete pair is ambiguous if legacy rows duplicate it.
        ("duplicate", true, false, true),
    ];
    let mut cancel_refs = Vec::new();
    for (label, has_source, omit_source_digest, duplicate_source) in cases {
        let delivery_id = model::new_id();
        let digest = model::digest(format!("payload-{label}").as_bytes());
        let cancel_id = model::new_id();
        let source_id = model::new_id();
        let duplicate_id = model::new_id();
        let label = label.to_owned();
        let sender_id = sender.client_id.clone();
        let result_digest = (!omit_source_digest).then_some(digest.clone());
        let cancel_ref = (cancel_id.clone(), delivery_id.clone(), digest.clone());
        owner
            .store
            .run(move |db| {
                if has_source {
                    let mut source_result = json!({
                        "operation_id":source_id.clone(),
                        "sender":sender_id.clone(),
                        "recipient":"legacy-recipient",
                        "delivery_id":delivery_id.clone(),
                        "text":format!("legacy source {label}")
                    });
                    if let Some(source_digest) = result_digest {
                        source_result["payload_digest"] = json!(source_digest);
                    }
                    let source_request = json!({"client_request_id":format!("source-{label}")});
                    insert_legacy_operation(
                        db,
                        &source_id,
                        &sender_id,
                        &format!("source-{label}"),
                        "message.send",
                        &source_request,
                        &source_result,
                    )?;
                }
                if duplicate_source {
                    let duplicate_result = json!({
                        "operation_id":duplicate_id.clone(),
                        "sender":sender_id.clone(),
                        "recipient":"legacy-recipient",
                        "delivery_id":delivery_id.clone(),
                        "payload_digest":digest.clone(),
                        "text":format!("duplicate legacy source {label}")
                    });
                    let duplicate_request = json!({"client_request_id":format!("duplicate-{label}")});
                    insert_legacy_operation(
                        db,
                        &duplicate_id,
                        &sender_id,
                        &format!("duplicate-{label}"),
                        "message.send",
                        &duplicate_request,
                        &duplicate_result,
                    )?;
                }
                let cancel_result = json!({
                    "operation_id":cancel_id.clone(),
                    "cancellation":{
                        "delivery_id":delivery_id.clone(),
                        "payload_digest":digest.clone()
                    },
                    "reason":format!("legacy cancel diagnostic {label}")
                });
                let cancel_request = json!({
                    "client_request_id":format!("cancel-{label}"),
                    "delivery_id":delivery_id.clone(),
                    "payload_digest":digest.clone()
                });
                insert_legacy_operation(
                    db,
                    &cancel_id,
                    &sender_id,
                    &format!("cancel-{label}"),
                    "message.cancel",
                    &cancel_request,
                    &cancel_result,
                )?;
                let cancel_payload = json!({
                    "delivery_id":delivery_id,
                    "payload_digest":digest,
                    "recipient":"legacy-recipient",
                    "reason":format!("legacy cancel diagnostic {label}"),
                    "cancellation":cancel_result["cancellation"]
                });
                db.execute(
                    "INSERT INTO observations(source_stream_id,source_event_key,operation_id,kind,payload_json,recorded_at_ms) VALUES('legacy-cancel-test',?1,?1,'message.cancel',?2,1000)",
                    params![cancel_id, model::canonical(&cancel_payload)?],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        cancel_refs.push(cancel_ref);
    }

    let recipient_events = read(
        &owner.store,
        &recipient,
        "report.delta",
        json!({"after":checkpoint,"limit":200}),
    )
    .await;
    let recipient_events_json = recipient_events.to_string();
    for (cancel_id, delivery_id, digest) in &cancel_refs {
        let error = owner
            .store
            .call(
                recipient.clone(),
                "operation.get".into(),
                json!({"operation_id":cancel_id}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "NOT_FOUND");
        assert!(!recipient_events_json.contains(cancel_id));
        assert!(!recipient_events_json.contains(delivery_id));
        assert!(!recipient_events_json.contains(digest));
    }
    assert!(!recipient_events_json.contains("legacy cancel diagnostic"));

    // The malformed recipient linkage does not erase the sender's own
    // immutable receipts or the bootstrap operator's global diagnostics.
    for principal in [&sender, &operator] {
        let events = read(
            &owner.store,
            principal,
            "report.delta",
            json!({"after":checkpoint,"limit":200}),
        )
        .await;
        let events_json = events.to_string();
        for (cancel_id, delivery_id, digest) in &cancel_refs {
            let receipt = read(
                &owner.store,
                principal,
                "operation.get",
                json!({"operation_id":cancel_id}),
            )
            .await;
            assert_eq!(receipt["result"]["operation_id"], cancel_id.as_str());
            assert!(receipt["result"].get("cancellation").is_none());
            assert!(receipt["result"].get("reason").is_none());
            assert!(events_json.contains(cancel_id));
            assert!(events_json.contains(delivery_id));
            assert!(events_json.contains(digest));
        }
        assert!(!events_json.contains("legacy cancel diagnostic"));
    }
    owner.close().await.unwrap();
}

fn insert_legacy_operation(
    db: &rusqlite::Connection,
    operation_id: &str,
    caller_id: &str,
    client_request_id: &str,
    method: &str,
    request: &Value,
    result: &Value,
) -> Result<()> {
    let request = model::canonical(request)?;
    let effective = model::canonical(&json!({"receipt":{"ok":true,"value":result}}))?;
    let result = model::canonical(result)?;
    db.execute(
        "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,?2,?3,?4,?5,?6,'settled',?7,1000,1000,1000,1000)",
        params![operation_id, caller_id, client_request_id, method, request, effective, result],
    )?;
    Ok(())
}

#[tokio::test]
async fn reply_addresses_the_original_delivery_by_identity_and_digest() {
    let (owner, operator) = start().await;
    let alice = register(&owner.store, &operator, "alice").await;
    let bob = register(&owner.store, &operator, "bob").await;
    let carol = register(&owner.store, &operator, "carol").await;
    let sent = write(
        &owner.store,
        &alice,
        "message.send",
        json!({"recipient":"bob","text":"question"}),
    )
    .await
    .unwrap();
    let delivery_id = sent["delivery_id"].as_str().unwrap();
    let digest = sent["payload_digest"].as_str().unwrap();
    // Addressed by delivery_id, with the digest claim verified.
    let reply = write(
        &owner.store,
        &bob,
        "message.send",
        json!({"recipient":"alice","text":"answer","in_reply_to":delivery_id,"in_reply_to_digest":digest}),
    )
    .await
    .unwrap();
    assert_eq!(
        reply["reply_to"],
        json!({"delivery_id":delivery_id,"payload_digest":digest})
    );
    // Addressed by the historical operation id, the reference still names the
    // original delivery and its recorded digest.
    let reply = write(
        &owner.store,
        &bob,
        "message.send",
        json!({"recipient":"alice","text":"answer two","in_reply_to":sent["operation_id"]}),
    )
    .await
    .unwrap();
    assert_eq!(
        reply["reply_to"],
        json!({"delivery_id":delivery_id,"payload_digest":digest})
    );
    // A digest claim that does not match the recorded digest is rejected.
    let err = write(
        &owner.store,
        &bob,
        "message.send",
        json!({"recipient":"alice","text":"answer","in_reply_to":delivery_id,"in_reply_to_digest":"0".repeat(64)}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "DIGEST_MISMATCH");
    // A third party cannot reply into a delivery it was not part of.
    let err = write(
        &owner.store,
        &carol,
        "message.send",
        json!({"recipient":"alice","text":"intrusion","in_reply_to":delivery_id}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "INVALID_PARAMS");
    owner.close().await.unwrap();
}

#[tokio::test]
async fn legacy_delivery_reads_back_with_explicit_unknowns() {
    let (owner, operator) = start().await;
    let alice = register(&owner.store, &operator, "alice").await;
    let bob = register(&owner.store, &operator, "bob").await;
    // A settled message.send record exactly as written before delivery
    // identity existed: no delivery_id, no digest, no scopes, no deadlines.
    let legacy_id = model::new_id();
    let legacy_result = json!({"operation_id":legacy_id,"message_id":legacy_id,"sender":"alice","recipient":"bob","text":"written before delivery identity","in_reply_to":Value::Null,"delivery":"durable_mailbox_only"});
    let request = json!({"client_request_id":"legacy-request","recipient":"bob","text":"written before delivery identity"});
    let mut effective = request.clone();
    effective["receipt"] = json!({"ok":true,"value":legacy_result});
    let original = model::canonical(&request).unwrap();
    let effective = model::canonical(&effective).unwrap();
    let result = model::canonical(&legacy_result).unwrap();
    let inserted_id = legacy_id.clone();
    owner
        .store
        .run(move |db| {
            db.execute(
                "INSERT INTO operations(operation_id,caller_id,client_request_id,method,original_request_json,effective_request_json,state,result_json,due_at_ms,settled_at_ms,created_at_ms,updated_at_ms) VALUES(?1,'alice','legacy-request','message.send',?2,?3,'settled',?4,1000,1000,1000,1000)",
                params![inserted_id, original, effective, result],
            )?;
            Ok(())
        })
        .await
        .unwrap();
    // Projection is unchanged: no identity is invented for the legacy record.
    let got = read(
        &owner.store,
        &operator,
        "operation.get",
        json!({"operation_id":legacy_id}),
    )
    .await;
    assert!(got["result"].get("delivery_id").is_none());
    assert!(got["result"].get("payload_digest").is_none());
    assert!(got["result"].get("reply_to").is_none());
    // It stays addressable by its historical operation id, and the reply's
    // reference projects explicit unknowns instead of fabricated identity.
    let reply = write(
        &owner.store,
        &bob,
        "message.send",
        json!({"recipient":"alice","text":"ack","in_reply_to":legacy_id}),
    )
    .await
    .unwrap();
    assert_eq!(
        reply["reply_to"],
        json!({"delivery_id":Value::Null,"payload_digest":Value::Null})
    );
    // A digest claim against a legacy delivery cannot be verified.
    let err = write(
        &owner.store,
        &bob,
        "message.send",
        json!({"recipient":"alice","text":"ack","in_reply_to":legacy_id,"in_reply_to_digest":"a".repeat(64)}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "DIGEST_UNVERIFIABLE");
    let _ = alice;
    owner.close().await.unwrap();
}

#[tokio::test]
async fn cancel_references_the_delivery_and_rewrites_nothing() {
    let (owner, operator) = start().await;
    let alice = register(&owner.store, &operator, "alice").await;
    let bob = register(&owner.store, &operator, "bob").await;
    let sent = write(
        &owner.store,
        &alice,
        "message.send",
        json!({"recipient":"bob","text":"please do the thing"}),
    )
    .await
    .unwrap();
    let delivery_id = sent["delivery_id"].as_str().unwrap();
    let digest = sent["payload_digest"].as_str().unwrap();
    // Free text that mentions a delivery never cancels it: the real
    // cancellation below still succeeds afterwards.
    write(
        &owner.store,
        &bob,
        "message.send",
        json!({"recipient":"alice","text":format!("cancel delivery {delivery_id} now")}),
    )
    .await
    .unwrap();
    // A digest that does not match the recorded one cancels nothing.
    let err = write(
        &owner.store,
        &alice,
        "message.cancel",
        json!({"delivery_id":delivery_id,"payload_digest":"0".repeat(64)}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "DIGEST_MISMATCH");
    // Only the original sender can cancel the delivery.
    let err = write(
        &owner.store,
        &bob,
        "message.cancel",
        json!({"delivery_id":delivery_id,"payload_digest":digest}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "FORBIDDEN");
    // An unknown delivery identity is not found.
    let err = write(
        &owner.store,
        &alice,
        "message.cancel",
        json!({"delivery_id":model::new_id(),"payload_digest":digest}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "NOT_FOUND");
    // The sender cancels by the delivery identity plus its recorded digest.
    let cancelled = write(
        &owner.store,
        &alice,
        "message.cancel",
        json!({"delivery_id":delivery_id,"payload_digest":digest,"reason":"no longer needed"}),
    )
    .await
    .unwrap();
    assert_eq!(
        cancelled["cancellation"],
        json!({"delivery_id":delivery_id,"payload_digest":digest})
    );
    assert_eq!(cancelled["cancelled_by"]["client_id"], "alice");
    assert_eq!(cancelled["original_record_changed"], false);
    // The original delivery record is evidence: it is not rewritten and its
    // operation does not change state.
    let got = read(
        &owner.store,
        &operator,
        "operation.get",
        json!({"operation_id":sent["operation_id"]}),
    )
    .await;
    assert_eq!(got["state"], "settled");
    assert_eq!(got["result"]["delivery_id"], delivery_id);
    assert_eq!(got["result"]["cancellation"], Value::Null);
    // A second cancellation of the same delivery conflicts.
    let err = write(
        &owner.store,
        &alice,
        "message.cancel",
        json!({"delivery_id":delivery_id,"payload_digest":digest}),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code, "CONFLICT");
    owner.close().await.unwrap();
}
