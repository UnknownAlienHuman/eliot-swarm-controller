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
    let token = format!("token-for-{client_id}");
    let registered = write(
        store,
        operator,
        "client.register",
        json!({"client_id":client_id,"role":"manager","token_hash":model::digest(token.as_bytes())}),
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
