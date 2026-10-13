//! Held-disposition projection and acknowledgement for quarantined CheckRuns.
//!
//! Fixtures admit through the same Store task/attempt/check path as the
//! pending-isolation suite; SQL writes are limited to explicit
//! retained-state fault injection. The new `check.acknowledge_held` method,
//! the quarantine-first `check.get` read and the `operation.get` held
//! evidence are all exercised through public `Store::call`.
use super::*;
use rusqlite::params;
use serde_json::{Value, json};
async fn admit_held(fixture: &Fixture, project: &str, content: &[u8]) -> Value {
    let (task_id, attempt_id) = create_attempt(fixture, project, None).await;
    let candidate = source_snapshot(fixture, &task_id, &attempt_id, content).await;
    admit_check(fixture, &attempt_id, &candidate, "v1").await
}
async fn mark_sending(store: &Store, check_id: &str) {
    let id = check_id.to_owned();
    store
        .run(move |db| {
            let changed = db.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=(SELECT operation_id FROM check_runs WHERE check_id=?1)",
                params![id, model::now_ms()?],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::new(
                    "TEST_STATE",
                    "admitted CheckRun has no Operation to select",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
}
async fn damage_spec_json(store: &Store, check_id: &str) {
    let id = check_id.to_owned();
    let damaged = "{\"token\": \"held-fixture-invalid-json\", \"candidate\":".to_owned();
    store
        .run(move |db| {
            db.execute_batch("PRAGMA ignore_check_constraints=ON")?;
            let injected = db.execute(
                "UPDATE check_runs SET spec_json=?2 WHERE check_id=?1",
                params![id, damaged],
            );
            db.execute_batch("PRAGMA ignore_check_constraints=OFF")?;
            if injected? != 1 {
                return Err(crate::error::Error::new(
                    "TEST_STATE",
                    "admitted CheckRun disappeared before fault injection",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
}
async fn damage_profile_shape(store: &Store, check_id: &str) {
    let id = check_id.to_owned();
    store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT spec_json FROM check_runs WHERE check_id=?1",
                [&id],
                |row| row.get(0),
            )?;
            let mut spec: Value = serde_json::from_str(&raw).unwrap();
            spec["profile"] = json!("not-an-object");
            let changed = db.execute(
                "UPDATE check_runs SET spec_json=?2 WHERE check_id=?1",
                params![id, model::canonical(&spec)?],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::new(
                    "TEST_STATE",
                    "admitted CheckRun disappeared before fault injection",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
}
async fn pending_page(store: &Store, root: std::path::PathBuf) -> Result<Vec<Work>> {
    store.run(move |db| pending(db, root)).await
}
async fn spec_digest(store: &Store, check_id: &str) -> String {
    let id = check_id.to_owned();
    let raw: String = store
        .run(move |db| {
            db.query_row(
                "SELECT spec_json FROM check_runs WHERE check_id=?1",
                [id],
                |row| row.get(0),
            )
        })
        .await
        .unwrap();
    model::digest(raw.as_bytes())
}
async fn operation_id_of(store: &Store, check_id: &str) -> String {
    let id = check_id.to_owned();
    store
        .run(move |db| {
            db.query_row(
                "SELECT operation_id FROM check_runs WHERE check_id=?1",
                [id],
                |row| row.get(0),
            )
        })
        .await
        .unwrap()
}
async fn write(store: &Store, principal: &Principal, method: &str, mut value: Value) -> Value {
    value["client_request_id"] = json!(model::new_id());
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}
async fn write_with_id(store: &Store, principal: &Principal, method: &str, request_id: &str, mut value: Value) -> Value {
    value["client_request_id"] = json!(request_id);
    store
        .call(principal.clone(), method.to_owned(), value)
        .await
        .unwrap()
}
async fn read_check(store: &Store, principal: &Principal, check_id: &str) -> Value {
    store
        .call(principal.clone(), "check.get".to_owned(), json!({"check_id": check_id}))
        .await
        .unwrap()
}
async fn wrong_actor(fixture: &Fixture) -> Principal {
    let token = format!("held-wrong-actor-{}", model::new_id());
    write(
        &fixture.owner.store,
        &fixture.principal,
        "client.register",
        json!({
            "client_id": "held-wrong-actor",
            "role": "manager",
            "token_hash": model::digest(token.as_bytes()),
        }),
    )
    .await;
    fixture
        .owner
        .store
        .authenticate(crate::model::Credential {
            client_id: "held-wrong-actor".to_owned(),
            token,
        })
        .await
        .unwrap()
}
#[tokio::test]
async fn held_check_get_reports_invalid_json_quarantine() {
    let fixture = fixture().await;
    let admitted = admit_held(&fixture, "held-invalid-json", b"held invalid json source").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &check_id).await;
    damage_spec_json(&fixture.owner.store, &check_id).await;
    let selected = pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
    assert!(selected.is_empty(), "damaged subject yields no Work");
    let readback = read_check(&fixture.owner.store, &fixture.principal, &check_id).await;
    assert_eq!(readback["check_id"], json!(&check_id));
    assert_eq!(readback["state"], json!("queued"));
    assert_eq!(readback["reconciliation"]["disposition"], json!("quarantined"));
    assert_eq!(readback["reconciliation"]["code"], json!("CHECK_SPEC_SUBJECT_CORRUPT"));
    assert_eq!(readback["reconciliation"]["field"], json!("spec"));
    assert_eq!(readback["reconciliation"]["reason"], json!("invalid_json"));
    assert_eq!(readback["reconciliation"]["held"], json!(false));
    assert!(readback.get("profile_id").is_none(), "sanitized view exposes no spec fields");
    assert_eq!(readback["reconciliation"]["source_digest"], json!(spec_digest(&fixture.owner.store, &check_id).await));
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
#[tokio::test]
async fn held_check_get_reports_malformed_field_quarantine() {
    let fixture = fixture().await;
    let admitted = admit_held(&fixture, "held-bad-profile", b"held bad profile source").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &check_id).await;
    damage_profile_shape(&fixture.owner.store, &check_id).await;
    let selected = pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
    assert!(selected.is_empty(), "damaged subject yields no Work");
    let readback = read_check(&fixture.owner.store, &fixture.principal, &check_id).await;
    assert_eq!(readback["reconciliation"]["disposition"], json!("quarantined"));
    assert_eq!(readback["reconciliation"]["field"], json!("profile"));
    assert_eq!(readback["reconciliation"]["reason"], json!("invalid_subject_shape"));
    assert_eq!(readback["reconciliation"]["held"], json!(false));
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
#[tokio::test]
async fn held_poison_leaves_active_page_capacity() {
    let fixture = fixture().await;
    let mut poisoned = Vec::new();
    for index in 0..16 {
        let admitted = admit_held(&fixture, &format!("held-poison-{index}"), b"held poison source").await;
        let check_id = admitted["check_id"].as_str().unwrap().to_owned();
        mark_sending(&fixture.owner.store, &check_id).await;
        damage_spec_json(&fixture.owner.store, &check_id).await;
        poisoned.push(check_id);
    }
    let healthy = admit_held(&fixture, "held-healthy", b"held healthy source").await;
    let healthy_id = healthy["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &healthy_id).await;
    let mut seen = std::collections::BTreeSet::new();
    for _ in 0..4 {
        let page = pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
        seen.extend(page.into_iter().map(|work| work.check_id));
    }
    assert_eq!(seen.len(), 1, "only the healthy row is ever selected");
    assert!(seen.contains(&healthy_id), "the healthy row is reachable past sixteen poisons");
    let quarantined: i64 = fixture
        .owner
        .store
        .run(|db| {
            db.query_row(
                "SELECT count(*) FROM meta WHERE key LIKE 'checks:pending:v1:quarantine:%'",
                [],
                |row| row.get(0),
            )
        })
        .await
        .unwrap();
    assert_eq!(quarantined, 16, "every poison retains exact evidence");
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
#[tokio::test]
async fn acknowledge_held_cas_matrix() {
    let fixture = fixture().await;
    let operator = fixture.principal.clone();
    let admitted = admit_held(&fixture, "held-ack", b"held ack source").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &check_id).await;
    damage_spec_json(&fixture.owner.store, &check_id).await;
    pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
    let digest = spec_digest(&fixture.owner.store, &check_id).await;
    let cid = check_id.as_str();
    let expected = digest.as_str();
    let first = write_with_id(
        &fixture.owner.store,
        &operator,
        "check.acknowledge_held",
        "held-ack-1",
        json!({"check_id": cid, "expected_source_digest": expected}),
    )
    .await;
    assert_eq!(first["acknowledged"], json!(true));
    assert_eq!(first["held"], json!(true));
    assert_eq!(first["state"], json!("queued"));
    assert_eq!(first["source_digest"], json!(expected));
    let ack_operation = first["operation_id"].as_str().unwrap().to_owned();
    let reread = write_with_id(
        &fixture.owner.store,
        &operator,
        "check.acknowledge_held",
        "held-ack-1",
        json!({"check_id": cid, "expected_source_digest": expected}),
    )
    .await;
    assert_eq!(reread["operation_id"], json!(&ack_operation), "lost reply returns the retained receipt");
    let readback = read_check(&fixture.owner.store, &operator, &check_id).await;
    assert_eq!(readback["reconciliation"]["held"], json!(true));
    assert_eq!(readback["state"], json!("queued"), "acknowledgement keeps the run nonterminal");
    let mismatch = fixture
        .owner
        .store
        .call(operator.clone(), "check.acknowledge_held".to_owned(), json!({"client_request_id": model::new_id(), "check_id": cid, "expected_source_digest": "0".repeat(64)}))
        .await
        .unwrap_err();
    assert_eq!(mismatch.code, "CHECK_HELD_DIGEST_MISMATCH");
    let stranger = wrong_actor(&fixture).await;
    let denied = fixture
        .owner
        .store
        .call(stranger, "check.acknowledge_held".to_owned(), json!({"client_request_id": model::new_id(), "check_id": cid, "expected_source_digest": expected}))
        .await
        .unwrap_err();
    assert_eq!(denied.code, "FORBIDDEN");
    let fresh = admit_held(&fixture, "held-ack-plain", b"held plain source").await;
    let plain_id = fresh["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &plain_id).await;
    let plain_digest = spec_digest(&fixture.owner.store, &plain_id).await;
    let absent = fixture
        .owner
        .store
        .call(operator.clone(), "check.acknowledge_held".to_owned(), json!({"client_request_id": model::new_id(), "check_id": plain_id, "expected_source_digest": plain_digest}))
        .await
        .unwrap_err();
    assert_eq!(absent.code, "CHECK_HELD_NOT_QUARANTINED");
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
#[tokio::test]
async fn acknowledge_held_changed_source_conflicts() {
    let fixture = fixture().await;
    let operator = fixture.principal.clone();
    let admitted = admit_held(&fixture, "held-changed", b"held changed source").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &check_id).await;
    damage_spec_json(&fixture.owner.store, &check_id).await;
    pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
    let first_digest = spec_digest(&fixture.owner.store, &check_id).await;
    damage_profile_shape(&fixture.owner.store, &check_id).await;
    let changed = pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap_err();
    assert_eq!(changed.code, "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT");
    let conflicted = fixture
        .owner
        .store
        .call(operator.clone(), "check.acknowledge_held".to_owned(), json!({"client_request_id": model::new_id(), "check_id": check_id, "expected_source_digest": first_digest}))
        .await
        .unwrap_err();
    assert_eq!(conflicted.code, "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT");
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
#[tokio::test]
async fn acknowledge_held_rejects_live_child() {
    let fixture = fixture().await;
    let operator = fixture.principal.clone();
    let admitted = admit_held(&fixture, "held-live", b"held live source").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &check_id).await;
    let works = pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
    let work = works.into_iter().find(|item| item.check_id == check_id).expect("healthy check is selected");
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--list")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    fixture.owner.store.retain_test_live_child(work, child).await;
    let digest = spec_digest(&fixture.owner.store, &check_id).await;
    let uncertain = fixture
        .owner
        .store
        .call(operator, "check.acknowledge_held".to_owned(), json!({"client_request_id": model::new_id(), "check_id": check_id, "expected_source_digest": digest}))
        .await
        .unwrap_err();
    assert_eq!(uncertain.code, "CHECK_HELD_LIVE_UNCERTAIN");
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
#[tokio::test]
async fn reopen_held_never_relaunches() {
    let fixture = fixture().await;
    let operator = fixture.principal.clone();
    let admitted = admit_held(&fixture, "held-reopen", b"held reopen source").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &check_id).await;
    damage_spec_json(&fixture.owner.store, &check_id).await;
    pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
    let digest = spec_digest(&fixture.owner.store, &check_id).await;
    write(&fixture.owner.store, &operator, "check.acknowledge_held", json!({"check_id": &check_id, "expected_source_digest": &digest})).await;
    let directory = fixture.directory.clone();
    let config = fixture.config.clone();
    fixture.owner.close().await.unwrap();
    let root = crate::platform::DataRoot::acquire(&directory).unwrap();
    let credential = crate::platform::bootstrap_credential(&directory).unwrap();
    let reopened = crate::store::StoreOwner::start(root, config, credential.clone()).await.unwrap();
    let successor = reopened.store.authenticate(credential).await.unwrap();
    let readback = read_check(&reopened.store, &successor, &check_id).await;
    assert_eq!(readback["reconciliation"]["disposition"], json!("quarantined"));
    assert_eq!(readback["reconciliation"]["held"], json!(true));
    assert_eq!(reopened.store.check_child_custody_count().await, 0, "reopen retains no child custody");
    let reselected = pending_page(&reopened.store, directory.clone()).await.unwrap();
    assert!(reselected.is_empty(), "held rows never re-enter the active page");
    reopened.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
#[tokio::test]
async fn operation_get_reports_held_disposition() {
    let fixture = fixture().await;
    let operator = fixture.principal.clone();
    let admitted = admit_held(&fixture, "held-operation", b"held operation source").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_sending(&fixture.owner.store, &check_id).await;
    damage_spec_json(&fixture.owner.store, &check_id).await;
    pending_page(&fixture.owner.store, fixture.directory.clone()).await.unwrap();
    let operation_id = operation_id_of(&fixture.owner.store, &check_id).await;
    let projection = fixture
        .owner
        .store
        .call(operator.clone(), "operation.get".to_owned(), json!({"operation_id": &operation_id}))
        .await
        .unwrap();
    assert_eq!(projection["method"], json!("check.run"));
    assert_eq!(projection["reconciliation"]["disposition"], json!("quarantined"));
    assert_eq!(projection["reconciliation"]["code"], json!("CHECK_SPEC_SUBJECT_CORRUPT"));
    assert_eq!(projection["reconciliation"]["held"], json!(false));
    let digest = spec_digest(&fixture.owner.store, &check_id).await;
    write(&fixture.owner.store, &operator, "check.acknowledge_held", json!({"check_id": check_id, "expected_source_digest": digest})).await;
    let reread = fixture
        .owner
        .store
        .call(operator, "operation.get".to_owned(), json!({"operation_id": &operation_id}))
        .await
        .unwrap();
    assert_eq!(reread["reconciliation"]["held"], json!(true));
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
