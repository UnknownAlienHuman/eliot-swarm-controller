//! Private REG23-P2-04 consumer tests over publicly admitted CheckRuns.
//!
//! The fixtures use Store task/attempt/check admission and the real pending
//! consumer. SQL writes are limited to explicit retained-state fault injection;
//! no CheckRun process or native command is launched by these cases.

use super::*;
use rusqlite::params;
use serde_json::{Value, json};
use std::collections::BTreeSet;

async fn admit_pending(
    fixture: &Fixture,
    project_id: &str,
    content: &[u8],
    revision: &str,
) -> Value {
    let (task_id, attempt_id) = create_attempt(fixture, project_id, None).await;
    let candidate = source_snapshot(fixture, &task_id, &attempt_id, content).await;
    let admitted = admit_check(fixture, &attempt_id, &candidate, revision).await;
    assert_eq!(admitted["state"], "queued");
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    mark_selected(&fixture.owner.store, &check_id).await;
    admitted
}

/// Test-only transition to the exact consumer-eligible sending state. The
/// Operation and CheckRun themselves came from the public check.run admission.
async fn mark_selected(store: &Store, check_id: &str) {
    let id = check_id.to_owned();
    store
        .run(move |db| {
            let now = model::now_ms()?;
            let changed = db.execute(
                "UPDATE operations SET state='sending',sent_at_ms=?2,updated_at_ms=?2 WHERE operation_id=(SELECT operation_id FROM check_runs WHERE check_id=?1)",
                params![id, now],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::new(
                    "TEST_STATE",
                    "publicly admitted CheckRun has no Operation to select",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
}

async fn damage_profile(store: &Store, check_id: &str) -> String {
    let id = check_id.to_owned();
    store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT spec_json FROM check_runs WHERE check_id=?1",
                [&id],
                |row| row.get(0),
            )?;
            let mut spec: Value = serde_json::from_str(&raw)?;
            assert!(
                spec["candidate"].is_object(),
                "public admission retains the candidate artifact"
            );
            let candidate = spec["candidate"].clone();
            assert!(
                spec["profile"].is_object(),
                "fixture admission keeps a full profile"
            );
            spec["profile"] = json!("not-an-object");
            assert_eq!(
                spec["candidate"], candidate,
                "profile damage leaves the admitted candidate unchanged"
            );
            let damaged = model::canonical(&spec)?;
            let changed = db.execute(
                "UPDATE check_runs SET spec_json=?2 WHERE check_id=?1",
                params![id, damaged],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::new(
                    "TEST_STATE",
                    "publicly admitted CheckRun disappeared before fault injection",
                ));
            }
            Ok(damaged)
        })
        .await
        .unwrap()
}

/// Test-only retained-state fault injection: replace the exact spec bytes with
/// truncated invalid JSON while keeping every Operation/Attempt/Task fact.
async fn damage_spec_invalid_json(store: &Store, check_id: &str) -> String {
    let id = check_id.to_owned();
    store
        .run(move |db| {
            let damaged = "{\"token\": \"reg23-invalid-json\", \"candidate\":".to_owned();
            let changed = db.execute(
                "UPDATE check_runs SET spec_json=?2 WHERE check_id=?1",
                params![id, damaged],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::new(
                    "TEST_STATE",
                    "publicly admitted CheckRun disappeared before fault injection",
                ));
            }
            Ok(damaged)
        })
        .await
        .unwrap()
}

async fn retained_quarantine(store: &Store, check_id: &str) -> Value {
    let key = quarantine_key(check_id);
    store
        .run(move |db| meta(db, &key))
        .await
        .unwrap()
        .unwrap_or(Value::Null)
}

async fn write_quarantine(store: &Store, check_id: &str, value: Value) {
    let key = quarantine_key(check_id);
    store
        .run(move |db| set_meta(db, &key, &value))
        .await
        .unwrap();
}

async fn spec_bytes(store: &Store, check_id: &str) -> String {
    let id = check_id.to_owned();
    store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT spec_json FROM check_runs WHERE check_id=?1",
                [id],
                |row| row.get(0),
            )?;
            Ok(raw)
        })
        .await
        .unwrap()
}
async fn retained_domain_snapshot(store: &Store, check_id: &str) -> Value {
    let id = check_id.to_owned();
    let raw = store
        .run(move |db| {
            let raw: String = db.query_row(
                "SELECT json_object('check',json_object('operation_id',c.operation_id,'attempt_id',c.attempt_id,'candidate_ref',c.candidate_ref,'cache_key',c.cache_key,'resource_key',c.resource_key,'spec_json',c.spec_json,'state',c.state,'process_identity_json',c.process_identity_json,'resource_claimed_at_ms',c.resource_claimed_at_ms,'resource_released_at_ms',c.resource_released_at_ms,'exit_code',c.exit_code),'operation',json_object('caller_id',o.caller_id,'client_request_id',o.client_request_id,'method',o.method,'original_request_json',o.original_request_json,'effective_request_json',o.effective_request_json,'task_id',o.task_id,'attempt_id',o.attempt_id,'binding_id',o.binding_id,'binding_generation',o.binding_generation,'state',o.state),'attempt',json_object('attempt_id',a.attempt_id,'task_id',a.task_id,'task_revision',a.task_revision,'task_snapshot_json',a.task_snapshot_json,'owner_id',a.owner_id,'start_owner',a.start_owner,'start_operation_id',a.start_operation_id,'binding_id',a.binding_id,'binding_generation',a.binding_generation),'task',json_object('task_id',t.task_id,'project_id',t.project_id,'revision',t.revision)) FROM check_runs c JOIN operations o ON o.operation_id=c.operation_id JOIN attempts a ON a.attempt_id=c.attempt_id JOIN tasks t ON t.task_id=a.task_id WHERE c.check_id=?1",
                [id],
                |row| row.get(0),
            )?;
            Ok(raw)
        })
        .await
        .unwrap();
    serde_json::from_str(&raw).unwrap()
}

async fn pending_page(store: &Store, root: std::path::PathBuf) -> Result<Vec<Work>> {
    store.run(move |db| pending(db, root)).await
}

async fn assert_pending_error_code(fixture: &Fixture, expected_code: &str) {
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, expected_code);
}

/// A valid-JSON profile fault is isolated, while its publicly admitted healthy
/// neighbor remains real Work and every retained Operation/Attempt fact stays
/// unchanged.
#[tokio::test]
async fn damaged_pending_spec_does_not_block_healthy_neighbor() {
    let fixture = fixture().await;
    let damaged = admit_pending(
        &fixture,
        "reg23-damaged-neighbor",
        b"damaged subject source",
        "v1",
    )
    .await;
    let healthy = admit_pending(
        &fixture,
        "reg23-healthy-neighbor",
        b"healthy subject source",
        "v2",
    )
    .await;
    let damaged_id = damaged["check_id"].as_str().unwrap().to_owned();
    let healthy_id = healthy["check_id"].as_str().unwrap().to_owned();
    let damaged_spec = damage_profile(&fixture.owner.store, &damaged_id).await;
    let before = retained_domain_snapshot(&fixture.owner.store, &damaged_id).await;

    let page = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();

    assert_eq!(page.len(), 1);
    assert_eq!(page[0].check_id, healthy_id);
    assert_eq!(
        page[0].operation_id,
        healthy["operation_id"].as_str().unwrap()
    );
    assert_eq!(page[0].profile.profile_revision, "v2");
    assert!(!page.iter().any(|work| work.check_id == damaged_id));
    assert_eq!(
        retained_domain_snapshot(&fixture.owner.store, &damaged_id).await,
        before,
        "quarantine must leave CheckRun, Operation, Attempt, and Task facts unchanged"
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &damaged_id).await,
        damaged_spec
    );

    let evidence = retained_quarantine(&fixture.owner.store, &damaged_id).await;
    assert_eq!(evidence["schema_version"], 1);
    assert_eq!(evidence["identity"]["check_id"], damaged_id);
    assert_eq!(
        evidence["identity"]["operation_id"],
        damaged["operation_id"].as_str().unwrap()
    );
    assert_eq!(evidence["code"], "CHECK_SPEC_SUBJECT_CORRUPT");
    assert_eq!(evidence["field"], "profile");
    assert_eq!(evidence["reason"], "invalid_subject_shape");
    assert_eq!(
        evidence["identity"]["check_spec_sha256"],
        model::digest(damaged_spec.as_bytes())
    );
    assert!(
        evidence["identity"]["operation_original_request_sha256"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
    assert!(
        evidence["identity"]["attempt_task_snapshot_sha256"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
    assert!(evidence["first_seen_at_ms"].as_i64().is_some());
    assert!(evidence["last_seen_at_ms"].as_i64().is_some());
    assert_eq!(evidence["occurrences"], 1);
    assert_eq!(fixture.owner.store.check_child_custody_count().await, 0);

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

#[tokio::test]
async fn repeated_consumer_call_preserves_bounded_disposition() {
    let fixture = fixture().await;
    let admitted = admit_pending(&fixture, "reg23-repeat", b"repeat subject source", "v1").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    let damaged_spec = damage_profile(&fixture.owner.store, &check_id).await;

    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let first = retained_quarantine(&fixture.owner.store, &check_id).await;
    assert_eq!(first["occurrences"], 1);

    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let second = retained_quarantine(&fixture.owner.store, &check_id).await;
    assert_eq!(
        second, first,
        "unchanged quarantine is stable across polls: polling alone must not rewrite occurrences or last_seen"
    );

    let saturated = {
        let mut saturated = second.clone();
        saturated["occurrences"] = json!(u32::MAX);
        saturated
    };
    write_quarantine(&fixture.owner.store, &check_id, saturated.clone()).await;
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let after = retained_quarantine(&fixture.owner.store, &check_id).await;
    assert_eq!(
        after, saturated,
        "stable re-observation keeps the saturating bound without rewriting identity"
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &check_id).await,
        damaged_spec
    );

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// The durable cursor and exact damaged-source identity survive a real Store
/// close and reopen through StoreOwner::start.
#[tokio::test]
async fn durable_disposition_survives_an_actual_store_restart() {
    let fixture = fixture().await;
    let admitted = admit_pending(&fixture, "reg23-restart", b"restart subject source", "v1").await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    let post_damage_spec = damage_profile(&fixture.owner.store, &check_id).await;
    assert_eq!(
        spec_bytes(&fixture.owner.store, &check_id).await,
        post_damage_spec,
        "restart expectation is captured from the actual retained post-damage bytes"
    );

    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let before = retained_quarantine(&fixture.owner.store, &check_id).await;
    let cursor = fixture
        .owner
        .store
        .run(|db| meta(db, PENDING_CURSOR_KEY))
        .await
        .unwrap();
    assert_eq!(cursor, Some(json!(check_id)));

    fixture.owner.close().await.unwrap();
    let credential = crate::platform::bootstrap_credential(&fixture.directory).unwrap();
    let root = crate::platform::DataRoot::acquire(&fixture.directory).unwrap();
    let reopened =
        crate::store::StoreOwner::start(root, fixture.config.clone(), credential.clone())
            .await
            .unwrap();
    let reopened_store = reopened.store.clone();
    reopened_store.authenticate(credential).await.unwrap();

    assert_eq!(
        retained_quarantine(&reopened_store, &check_id).await,
        before
    );
    assert_eq!(
        spec_bytes(&reopened_store, &check_id).await,
        post_damage_spec
    );
    assert!(
        pending_page(&reopened_store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let after = retained_quarantine(&reopened_store, &check_id).await;
    assert_eq!(
        after, before,
        "restart recognizes retained quarantine without rewriting occurrences or last_seen"
    );
    assert_eq!(
        reopened_store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        Some(json!(check_id)),
        "restart preserves the durable cursor without a rewrite"
    );
    assert_eq!(
        spec_bytes(&reopened_store, &check_id).await,
        post_damage_spec
    );

    reopened.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// Unchanged deterministic damage stays read-only across polls: polling alone
/// must not rewrite occurrences/last_seen and must not hit the writer, even
/// with an observation-write failure trigger installed. New quarantine/cursor
/// hard-failure rollback stays covered by the late-write and
/// quarantine-insert fixtures.
#[tokio::test]
async fn unchanged_quarantine_poll_stays_read_only() {
    let fixture = fixture().await;
    let admitted = admit_pending(
        &fixture,
        "reg23-seen-write-failure",
        b"seen write failure source",
        "v1",
    )
    .await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    let damaged_spec = damage_profile(&fixture.owner.store, &check_id).await;
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let before = retained_quarantine(&fixture.owner.store, &check_id).await;
    let cursor_before = fixture
        .owner
        .store
        .run(|db| meta(db, PENDING_CURSOR_KEY))
        .await
        .unwrap();
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER reg23_fail_seen_update BEFORE UPDATE ON meta WHEN substr(NEW.key,1,length('checks:pending:v1:quarantine:'))='checks:pending:v1:quarantine:' BEGIN SELECT RAISE(ABORT,'injected seen write failure'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty(),
        "unchanged poll must stay read-only and not hit the observation-write trigger"
    );
    assert_eq!(
        retained_quarantine(&fixture.owner.store, &check_id).await,
        before
    );
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        cursor_before
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &check_id).await,
        damaged_spec
    );
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_seen_update")?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let after = retained_quarantine(&fixture.owner.store, &check_id).await;
    assert_eq!(
        after, before,
        "polling alone must not rewrite occurrences or last_seen"
    );
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// A cursor-write failure after multiple subject dispositions rolls back the
/// whole Immediate page, including all quarantine rows and the cursor.
#[tokio::test]
async fn late_write_failure_rolls_back_the_whole_page() {
    let fixture = fixture().await;
    let first = admit_pending(
        &fixture,
        "reg23-rollback-first",
        b"rollback first source",
        "v1",
    )
    .await;
    let second = admit_pending(
        &fixture,
        "reg23-rollback-second",
        b"rollback second source",
        "v1",
    )
    .await;
    let first_id = first["check_id"].as_str().unwrap().to_owned();
    let second_id = second["check_id"].as_str().unwrap().to_owned();
    let first_spec = damage_profile(&fixture.owner.store, &first_id).await;
    let second_spec = damage_profile(&fixture.owner.store, &second_id).await;
    let before_first = retained_domain_snapshot(&fixture.owner.store, &first_id).await;
    let before_second = retained_domain_snapshot(&fixture.owner.store, &second_id).await;

    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER reg23_fail_cursor_insert BEFORE INSERT ON meta WHEN NEW.key='checks:pending:v1:cursor' BEGIN SELECT RAISE(ABORT,'injected cursor write failure'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_cursor_insert;")?;
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(
        retained_quarantine(&fixture.owner.store, &first_id).await,
        Value::Null
    );
    assert_eq!(
        retained_quarantine(&fixture.owner.store, &second_id).await,
        Value::Null
    );
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        retained_domain_snapshot(&fixture.owner.store, &first_id).await,
        before_first
    );
    assert_eq!(
        retained_domain_snapshot(&fixture.owner.store, &second_id).await,
        before_second
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &first_id).await,
        first_spec
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &second_id).await,
        second_spec
    );

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

#[tokio::test]
async fn malformed_global_pending_cursor_is_hard() {
    let fixture = fixture().await;
    let admitted = admit_pending(
        &fixture,
        "reg23-global-cursor",
        b"cursor subject source",
        "v1",
    )
    .await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    damage_profile(&fixture.owner.store, &check_id).await;

    for broken in [
        json!(""),
        json!("bad\ncursor"),
        json!(12),
        Value::Null,
        json!({"check_id":check_id}),
    ] {
        fixture
            .owner
            .store
            .run({
                let value = broken.clone();
                move |db| set_meta(db, PENDING_CURSOR_KEY, &value)
            })
            .await
            .unwrap();
        let error = pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap_err();
        assert_eq!(error.code, "CHECK_PENDING_CURSOR_CORRUPT");
        assert_eq!(
            retained_quarantine(&fixture.owner.store, &check_id).await,
            Value::Null
        );
    }

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

#[tokio::test]
async fn quarantine_store_write_failure_at_subject_boundary_stays_hard() {
    let fixture = fixture().await;
    let damaged = admit_pending(
        &fixture,
        "reg23-write-failure-damaged",
        b"write failure damaged source",
        "v1",
    )
    .await;
    let healthy = admit_pending(
        &fixture,
        "reg23-write-failure-healthy",
        b"write failure healthy source",
        "v1",
    )
    .await;
    let damaged_id = damaged["check_id"].as_str().unwrap().to_owned();
    let healthy_id = healthy["check_id"].as_str().unwrap().to_owned();
    damage_profile(&fixture.owner.store, &damaged_id).await;
    let before = retained_domain_snapshot(&fixture.owner.store, &damaged_id).await;
    let healthy_spec = spec_bytes(&fixture.owner.store, &healthy_id).await;

    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER reg23_fail_quarantine_insert BEFORE INSERT ON meta WHEN substr(NEW.key,1,length('checks:pending:v1:quarantine:'))='checks:pending:v1:quarantine:' BEGIN SELECT RAISE(ABORT,'injected quarantine write failure'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_quarantine_insert;")?;
            Ok(())
        })
        .await
        .unwrap();

    assert_eq!(
        retained_quarantine(&fixture.owner.store, &damaged_id).await,
        Value::Null
    );
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        retained_domain_snapshot(&fixture.owner.store, &damaged_id).await,
        before
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &healthy_id).await,
        healthy_spec
    );
    assert_eq!(fixture.owner.store.check_child_custody_count().await, 0);

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

#[tokio::test]
async fn durable_disposition_rejects_identity_and_shape_mutations() {
    let fixture = fixture().await;
    let admitted = admit_pending(
        &fixture,
        "reg23-identity-mutation",
        b"identity mutation source",
        "v1",
    )
    .await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    let operation_id = admitted["operation_id"].as_str().unwrap().to_owned();
    damage_profile(&fixture.owner.store, &check_id).await;
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let evidence = retained_quarantine(&fixture.owner.store, &check_id).await;
    let original = retained_domain_snapshot(&fixture.owner.store, &check_id).await;
    let original_request = original["operation"]["original_request_json"]
        .as_str()
        .unwrap()
        .to_owned();
    let attempt_id = original["attempt"]["attempt_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let owner_id = original["attempt"]["owner_id"].as_str().unwrap().to_owned();

    fixture
        .owner
        .store
        .run({
            let operation_id = operation_id.clone();
            move |db| {
                db.execute(
                    "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
                    params![operation_id, "{\"mutated\":true}"],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT");
    fixture
        .owner
        .store
        .run({
            let operation_id = operation_id.clone();
            move |db| {
                db.execute(
                    "UPDATE operations SET original_request_json=?2 WHERE operation_id=?1",
                    params![operation_id, original_request],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();

    fixture
        .owner
        .store
        .run({
            let attempt_id = attempt_id.clone();
            move |db| {
                db.execute(
                    "UPDATE attempts SET owner_id='mutated-owner' WHERE attempt_id=?1",
                    [attempt_id],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT");
    fixture
        .owner
        .store
        .run({
            let attempt_id = attempt_id.clone();
            move |db| {
                db.execute(
                    "UPDATE attempts SET owner_id=?2 WHERE attempt_id=?1",
                    params![attempt_id, owner_id],
                )?;
                Ok(())
            }
        })
        .await
        .unwrap();

    let mut changed_identity = evidence.clone();
    changed_identity["identity"]["attempt_owner_id"] = json!("other-owner");
    write_quarantine(&fixture.owner.store, &check_id, changed_identity).await;
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT");
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut changed_operation_binding = evidence.clone();
    changed_operation_binding["identity"]["operation_binding_id"] = json!("changed-binding");
    changed_operation_binding["identity"]["operation_binding_generation"] = json!(2);
    write_quarantine(&fixture.owner.store, &check_id, changed_operation_binding).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut changed_attempt_binding = evidence.clone();
    changed_attempt_binding["identity"]["attempt_binding_id"] = json!("changed-binding");
    changed_attempt_binding["identity"]["attempt_binding_generation"] = json!(2);
    write_quarantine(&fixture.owner.store, &check_id, changed_attempt_binding).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_IDENTITY_CONFLICT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut missing_binding_key = evidence.clone();
    assert!(
        missing_binding_key["identity"]
            .as_object_mut()
            .unwrap()
            .remove("operation_binding_id")
            .is_some()
    );
    write_quarantine(&fixture.owner.store, &check_id, missing_binding_key).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    // The triggering identity has a real nullable value here. An explicit
    // JSON null is present and must continue to match the live identity.
    assert!(evidence["identity"]["operation_binding_id"].is_null());
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .expect("explicit null remains a valid present nullable value")
            .is_empty()
    );

    for key in [
        "operation_task_id",
        "operation_attempt_id",
        "operation_binding_generation",
        "attempt_start_operation_id",
        "attempt_binding_id",
        "attempt_binding_generation",
    ] {
        let mut missing_key = evidence.clone();
        assert!(
            missing_key["identity"]
                .as_object_mut()
                .unwrap()
                .remove(key)
                .is_some(),
            "fixture includes nullable identity key {key}"
        );
        write_quarantine(&fixture.owner.store, &check_id, missing_key).await;
        assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
        write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;
    }

    let mut unknown_field = evidence.clone();
    unknown_field["unrecognized"] = json!(true);
    write_quarantine(&fixture.owner.store, &check_id, unknown_field).await;
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, "CHECK_PENDING_QUARANTINE_CORRUPT");
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut invalid_schema = evidence.clone();
    invalid_schema["schema_version"] = json!(2);
    write_quarantine(&fixture.owner.store, &check_id, invalid_schema).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut invalid_field = evidence.clone();
    invalid_field["field"] = json!("unknown_field");
    write_quarantine(&fixture.owner.store, &check_id, invalid_field).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut invalid_reason = evidence.clone();
    invalid_reason["reason"] = json!("unknown_reason");
    write_quarantine(&fixture.owner.store, &check_id, invalid_reason).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut mismatched_field_reason = evidence.clone();
    mismatched_field_reason["field"] = json!("cancel_request");
    mismatched_field_reason["reason"] = json!("missing_required_field");
    write_quarantine(&fixture.owner.store, &check_id, mismatched_field_reason).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut negative_timestamp = evidence.clone();
    negative_timestamp["first_seen_at_ms"] = json!(-1);
    write_quarantine(&fixture.owner.store, &check_id, negative_timestamp).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut reversed_timestamps = evidence.clone();
    reversed_timestamps["first_seen_at_ms"] = json!(10);
    reversed_timestamps["last_seen_at_ms"] = json!(9);
    write_quarantine(&fixture.owner.store, &check_id, reversed_timestamps).await;
    assert_pending_error_code(&fixture, "CHECK_PENDING_QUARANTINE_CORRUPT").await;
    write_quarantine(&fixture.owner.store, &check_id, evidence.clone()).await;

    let mut invalid_counter = evidence.clone();
    invalid_counter["occurrences"] = json!(0);
    write_quarantine(&fixture.owner.store, &check_id, invalid_counter).await;
    let error = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap_err();
    assert_eq!(error.code, "CHECK_PENDING_QUARANTINE_CORRUPT");

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

#[tokio::test]
async fn bounded_cursor_progress_includes_later_rows_and_new_admissions() {
    let fixture = fixture().await;
    let mut expected = BTreeSet::new();
    for index in 0..18 {
        let project_id = format!("reg23-page-{index}");
        let content = format!("bounded pending source {index}");
        let admitted = admit_pending(&fixture, &project_id, content.as_bytes(), "v1").await;
        expected.insert(admitted["check_id"].as_str().unwrap().to_owned());
    }

    let first = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert_eq!(first.len(), 16);
    let first_ids: BTreeSet<String> = first.into_iter().map(|work| work.check_id).collect();
    let second = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert_eq!(second.len(), 16);
    let mut visited = first_ids;
    visited.extend(second.into_iter().map(|work| work.check_id));
    assert_eq!(visited, expected, "both bounded pages cover later rows");

    let new_admission = admit_pending(
        &fixture,
        "reg23-page-added-later",
        b"late pending admission",
        "v1",
    )
    .await;
    let new_id = new_admission["check_id"].as_str().unwrap().to_owned();
    let third = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    let fourth = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert!(
        third.iter().any(|work| work.check_id == new_id)
            || fourth.iter().any(|work| work.check_id == new_id),
        "a new ID before or after the cursor is visited by bounded wraparound"
    );

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// Invalid JSON in one subject is isolated: the healthy neighbor still
/// returns as real Work, the damaged subject retains one bounded quarantine
/// record bound to its exact raw digest, and repeat polls stay stable without
/// rewriting occurrences or the cursor.
#[tokio::test]
async fn invalid_json_pending_spec_does_not_block_healthy_neighbor() {
    let fixture = fixture().await;
    let damaged = admit_pending(
        &fixture,
        "reg23-invalid-json-neighbor",
        b"invalid json subject source",
        "v1",
    )
    .await;
    let healthy = admit_pending(
        &fixture,
        "reg23-invalid-json-healthy",
        b"invalid json healthy source",
        "v2",
    )
    .await;
    let damaged_id = damaged["check_id"].as_str().unwrap().to_owned();
    let healthy_id = healthy["check_id"].as_str().unwrap().to_owned();
    let damaged_spec = damage_spec_invalid_json(&fixture.owner.store, &damaged_id).await;
    assert!(
        serde_json::from_str::<Value>(&damaged_spec).is_err(),
        "fault injection must retain invalid JSON bytes"
    );
    let before = retained_domain_snapshot(&fixture.owner.store, &damaged_id).await;

    let page = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].check_id, healthy_id);
    assert_eq!(
        page[0].operation_id,
        healthy["operation_id"].as_str().unwrap()
    );
    assert_eq!(page[0].profile.profile_revision, "v2");
    assert!(!page.iter().any(|work| work.check_id == damaged_id));
    assert_eq!(
        retained_domain_snapshot(&fixture.owner.store, &damaged_id).await,
        before,
        "quarantine must leave CheckRun, Operation, Attempt, and Task facts unchanged"
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &damaged_id).await,
        damaged_spec
    );

    let evidence = retained_quarantine(&fixture.owner.store, &damaged_id).await;
    assert_eq!(evidence["schema_version"], 1);
    assert_eq!(evidence["identity"]["check_id"], damaged_id);
    assert_eq!(
        evidence["identity"]["operation_id"],
        damaged["operation_id"].as_str().unwrap()
    );
    assert_eq!(evidence["code"], "CHECK_SPEC_SUBJECT_CORRUPT");
    assert_eq!(evidence["field"], "spec");
    assert_eq!(evidence["reason"], "invalid_json");
    assert_eq!(
        evidence["identity"]["check_spec_sha256"],
        model::digest(damaged_spec.as_bytes())
    );
    assert!(
        evidence["identity"]["operation_original_request_sha256"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
    assert!(
        evidence["identity"]["attempt_task_snapshot_sha256"]
            .as_str()
            .is_some_and(|value| value.len() == 64)
    );
    assert!(evidence["first_seen_at_ms"].as_i64().is_some());
    assert!(evidence["last_seen_at_ms"].as_i64().is_some());
    assert_eq!(evidence["occurrences"], 1);
    assert_eq!(fixture.owner.store.check_child_custody_count().await, 0);

    let second = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert!(
        second.iter().any(|work| work.check_id == healthy_id),
        "healthy later subject still progresses while invalid-JSON neighbor stays quarantined"
    );
    assert!(!second.iter().any(|work| work.check_id == damaged_id));
    assert_eq!(
        retained_quarantine(&fixture.owner.store, &damaged_id).await,
        evidence,
        "repeat poll must not rewrite invalid-JSON quarantine occurrences or last_seen"
    );
    assert_eq!(
        spec_bytes(&fixture.owner.store, &damaged_id).await,
        damaged_spec
    );

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// Empty Store with no retained cursor stays read-only: repeated pending
/// scans return no Work, retain no cursor, and do not hit the SQLite writer.
#[tokio::test]
async fn empty_pending_scan_stays_read_only() {
    let fixture = fixture().await;
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        None
    );
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER reg23_fail_empty_insert BEFORE INSERT ON meta BEGIN SELECT RAISE(ABORT,'injected empty insert'); END; CREATE TRIGGER reg23_fail_empty_update BEFORE UPDATE ON meta BEGIN SELECT RAISE(ABORT,'injected empty update'); END; CREATE TRIGGER reg23_fail_empty_delete BEFORE DELETE ON meta BEGIN SELECT RAISE(ABORT,'injected empty delete'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty(),
        "empty scan with no cursor must not attempt any meta write"
    );
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "DROP TRIGGER reg23_fail_empty_insert; DROP TRIGGER reg23_fail_empty_update; DROP TRIGGER reg23_fail_empty_delete;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        None
    );
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// A stale cursor with no subjects is removed once; the next empty scan is
/// read-only and does not attempt a second delete.
#[tokio::test]
async fn stale_cursor_is_removed_once_then_read_only() {
    let fixture = fixture().await;
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    fixture
        .owner
        .store
        .run(|db| set_meta(db, PENDING_CURSOR_KEY, &json!("stale-cursor-check-id")))
        .await
        .unwrap();
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        None,
        "stale cursor is deleted once"
    );
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER reg23_fail_stale_delete BEFORE DELETE ON meta BEGIN SELECT RAISE(ABORT,'injected stale second delete'); END;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty(),
        "second empty scan must be read-only and not attempt another delete"
    );
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_stale_delete")?;
            Ok(())
        })
        .await
        .unwrap();
    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
