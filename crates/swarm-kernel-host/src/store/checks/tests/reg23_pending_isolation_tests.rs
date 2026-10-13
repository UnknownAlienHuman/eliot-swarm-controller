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
            // Corruption injection intentionally bypasses the admission-time
            // JSON CHECK on this test connection only, then restores it before
            // the real pending consumer observes the retained damaged bytes.
            db.execute_batch("PRAGMA ignore_check_constraints=ON")?;
            let injected = db.execute(
                "UPDATE check_runs SET spec_json=?2 WHERE check_id=?1",
                params![id, damaged],
            );
            db.execute_batch("PRAGMA ignore_check_constraints=OFF")?;
            let changed = injected?;
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
/// Bounded selector progress moves only through completed subjects: bare
/// selection without any stable disposition re-selects the same page, so
/// unexamined Work is never marked visited. Real dispositions recorded
/// through the startup reconciliation seam still cover all rows across
/// bounded pages, and a late admission waits at most one exact cycle turn.
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
    let first_ids: BTreeSet<String> = first.iter().map(|work| work.check_id.clone()).collect();
    // Bare re-selection without any stable disposition returns the same
    // page and leaves the cursor untouched.
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        None
    );
    let reselect = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert_eq!(
        reselect
            .iter()
            .map(|work| &work.check_id)
            .collect::<BTreeSet<_>>(),
        first_ids.iter().collect::<BTreeSet<_>>(),
        "selection without a stable disposition must not advance"
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

    // Real terminal dispositions recorded through actual reconciliation
    // complete the whole first page and move the cursor to its last row.
    for check_id in &first_ids {
        mark_preflight_terminal(&fixture.owner.store, check_id).await;
    }
    for check_id in expected.difference(&first_ids) {
        mark_preflight_terminal(&fixture.owner.store, check_id).await;
    }
    fixture.owner.store.reconcile_checks_once().await.unwrap();
    let completed: BTreeSet<String> = first_ids.clone();
    for check_id in &completed {
        assert_eq!(
            check_state(&fixture.owner.store, check_id).await["state"],
            "error",
            "first-page row must reach its terminal disposition"
        );
    }

    // A late admission waits at most one exact cycle turn: it stays out of
    // the active page while pre-cut rows remain, then enters with a fresh
    // cut once the cycle drains.
    let new_admission = admit_pending(
        &fixture,
        "reg23-page-added-later",
        b"late pending admission",
        "v1",
    )
    .await;
    let new_id = new_admission["check_id"].as_str().unwrap().to_owned();
    mark_preflight_terminal(&fixture.owner.store, &new_id).await;
    fixture.owner.store.reconcile_checks_once().await.unwrap();
    // A late admission either joins the open cycle (visited immediately)
    // or waits exactly one turnover (post-cut); both converge on the next
    // pass, so the row is terminal after at most two more passes.
    fixture.owner.store.reconcile_checks_once().await.unwrap();
    assert_eq!(
        check_state(&fixture.owner.store, &new_id).await["state"],
        "error",
        "a new ID is visited within one exact cycle turn"
    );
    let terminal = fixture
        .owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT count(*) FROM check_runs WHERE state='error'",
                [],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(
        terminal, 19,
        "every admitted row reaches terminal exactly once"
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

/// Empty pending scans never reserve the SQLite writer: they succeed while an
/// unrelated external connection holds an IMMEDIATE writer reservation on the
/// same swarm.db. An IMMEDIATE pending reader would block on that RESERVED
/// lock and deadlock this fixture because release happens only after pending
/// returns; a Deferred read-only snapshot returns promptly.
#[tokio::test]
async fn empty_pending_scan_succeeds_under_external_immediate_lock() {
    let fixture = fixture().await;
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let db_path = fixture.directory.join("swarm.db");
    let db = rusqlite::Connection::open(&db_path).expect("open external Store database");
    db.execute_batch("BEGIN IMMEDIATE")
        .expect("reserve external IMMEDIATE writer");
    let observed = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        pending_page(&fixture.owner.store, fixture.directory.clone()),
    )
    .await;
    // Release even when the regression times out, so the negative cannot
    // strand a writer-lock thread or leave the Store owner blocked.
    db.execute_batch("ROLLBACK")
        .expect("release external writer");
    drop(db);
    assert!(
        observed
            .expect("empty pending read must finish while the writer is reserved")
            .unwrap()
            .is_empty(),
        "empty Deferred reader must not block on an external IMMEDIATE reservation"
    );
    assert_eq!(
        fixture
            .owner
            .store
            .run(|db| meta(db, PENDING_CURSOR_KEY))
            .await
            .unwrap(),
        None,
        "empty scan under external lock must not retain a cursor"
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
        // A stale cursor must still be canonical to stay a valid ordering
        // boundary: noncanonical values are hard integrity errors (see the
        // canonical cursor fixture), so this names a canonical UUID that was
        // never admitted.
        .run(|db| set_meta(db, PENDING_CURSOR_KEY, &json!(model::new_id())))
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

/// Test-only retained-state fault injection: record a genuine terminal
/// preflight failure for real reconciliation to consume. The error is an
/// exact retained CheckRun fact (not a fabricated outcome): the startup
/// and supervisor seams terminalize it through worker::failure and the
/// existing finish gates without spawning any worker.
async fn mark_preflight_terminal(store: &Store, check_id: &str) {
    let id = check_id.to_owned();
    let failure = model::canonical(&json!({
        "code": "CHECK_FIXTURE_PREFLIGHT",
        "message": "fixture terminal path for completed-subject cursor",
    }))
    .unwrap();
    store
        .run(move |db| {
            let changed = db.execute(
                "UPDATE check_runs SET spec_json=json_set(spec_json,'$.preflight_error',json(?2)) WHERE check_id=?1",
                params![id, failure],
            )?;
            if changed != 1 {
                return Err(crate::error::Error::new(
                    "TEST_STATE",
                    "publicly admitted CheckRun disappeared before fault injection",
                ));
            }
            Ok(())
        })
        .await
        .unwrap();
}

async fn check_state(store: &Store, check_id: &str) -> Value {
    let id = check_id.to_owned();
    store
        .run(move |db| describe(db, &json!({ "check_id": id })))
        .await
        .unwrap()
}

async fn retained_cursor(store: &Store) -> Option<Value> {
    store.run(|db| meta(db, PENDING_CURSOR_KEY)).await.unwrap()
}

async fn retained_cycle_cut(store: &Store) -> Option<String> {
    store
        .run(|db| Ok(read_pending_cycle(db)?.map(|cycle| cycle.cut)))
        .await
        .unwrap()
}

/// The cursor advances only through subjects that reached a stable,
/// truthfully retained disposition. Seventeen pending CheckRuns with a hard
/// Store failure injected while real reconciliation handles row 2 leave the
/// cursor at row 1, never at the end of the selected page; selection alone
/// before the first completed subject leaves the prior cursor untouched;
/// after the fault is removed row 2 is next eligible and every later row
/// reaches its terminal disposition exactly once.
#[tokio::test]
async fn pending_cursor_advances_only_through_completed_subjects() {
    let fixture = fixture().await;
    let mut admitted = Vec::new();
    for index in 0..17 {
        let project_id = format!("reg23-cursor-{index}");
        let content = format!("completed-subject cursor source {index}");
        admitted.push(admit_pending(&fixture, &project_id, content.as_bytes(), "v1").await);
    }
    for receipt in &admitted {
        mark_preflight_terminal(&fixture.owner.store, receipt["check_id"].as_str().unwrap()).await;
    }
    let root = fixture.directory.clone();
    let first = fixture
        .owner
        .store
        .run(move |db| pending_selection(db, root))
        .await
        .unwrap();
    assert_eq!(first.work.len(), 16);
    let ordered: Vec<String> = first
        .work
        .iter()
        .map(|work| work.check_id.clone())
        .collect();
    let mut lexical = ordered.clone();
    lexical.sort();
    assert_eq!(ordered, lexical, "the active page stays in lexical order");
    let row1 = ordered[0].clone();
    let row2 = ordered[1].clone();
    let late: Vec<String> = admitted
        .iter()
        .map(|receipt| receipt["check_id"].as_str().unwrap().to_owned())
        .filter(|id| *id != row1 && ordered.iter().all(|seen| seen != id))
        .collect();
    assert_eq!(
        late.len(),
        1,
        "one admitted row stays outside the first page"
    );
    let row17 = late[0].clone();
    assert_eq!(
        retained_cycle_cut(&fixture.owner.store).await,
        None,
        "selection alone persists no cycle cut"
    );

    // Failure before the first completed subject leaves the prior cursor
    // (absent here) unchanged.
    let first_trigger = row1.clone();
    fixture
        .owner
        .store
        .run(move |db| {
            db.execute_batch(&format!(
                "CREATE TRIGGER reg23_fail_first_finish BEFORE UPDATE ON check_runs WHEN NEW.check_id='{first_trigger}' BEGIN SELECT RAISE(ABORT,'injected first-row finish failure'); END;"
            ))?;
            Ok(())
        })
        .await
        .unwrap();
    let error = fixture
        .owner
        .store
        .reconcile_checks_once()
        .await
        .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_first_finish")?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        retained_cursor(&fixture.owner.store).await,
        None,
        "failure before the first completed subject leaves the prior cursor unchanged"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &row1).await["state"],
        "queued",
        "the rolled-back row stays eligible"
    );

    // A hard failure while real reconciliation handles row 2 leaves the
    // cursor at row 1, not at the end of the selected page.
    let second_trigger = row2.clone();
    fixture
        .owner
        .store
        .run(move |db| {
            db.execute_batch(&format!(
                "CREATE TRIGGER reg23_fail_second_finish BEFORE UPDATE ON check_runs WHEN NEW.check_id='{second_trigger}' BEGIN SELECT RAISE(ABORT,'injected second-row finish failure'); END;"
            ))?;
            Ok(())
        })
        .await
        .unwrap();
    let error = fixture
        .owner
        .store
        .reconcile_checks_once()
        .await
        .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_second_finish")?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        retained_cursor(&fixture.owner.store).await,
        Some(json!(row1)),
        "the cursor ends at row 1, not at the end of the selected page"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &row1).await["state"],
        "error",
        "row 1 reached its terminal disposition"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &row2).await["state"],
        "queued",
        "the failed row stays eligible"
    );
    assert_eq!(
        retained_cycle_cut(&fixture.owner.store).await,
        Some(row17.clone()),
        "the cycle cut is captured at the admitted maximum"
    );

    // Removing the fault makes row 2 next eligible; every later row then
    // reaches its terminal disposition exactly once, and reaching the cut
    // closes the cycle.
    let head = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert_eq!(
        head[0].check_id, row2,
        "row 2 stays next eligible after recovery"
    );
    for _ in 0..2 {
        fixture.owner.store.reconcile_checks_once().await.unwrap();
    }
    for receipt in &admitted {
        let check_id = receipt["check_id"].as_str().unwrap();
        assert_eq!(
            check_state(&fixture.owner.store, check_id).await["state"],
            "error",
            "no later row is skipped"
        );
    }
    assert_eq!(
        retained_cursor(&fixture.owner.store).await,
        Some(json!(row17)),
        "recovery converges on the last completed subject"
    );
    assert_eq!(
        retained_cycle_cut(&fixture.owner.store).await,
        None,
        "reaching the cut closes the cycle"
    );
    // Convergence and the next empty-page cleanup are separate boundaries.
    fixture.owner.store.reconcile_checks_once().await.unwrap();
    assert!(retained_cursor(&fixture.owner.store).await.is_none());
    assert!(retained_cycle_cut(&fixture.owner.store).await.is_none());
    let terminal = fixture
        .owner
        .store
        .run(|db| {
            Ok(db.query_row(
                "SELECT count(*) FROM check_runs WHERE state='error'",
                [],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(terminal, 17, "no later row is duplicated");

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// The pending cursor is a closed canonical UUIDv4 coordinate: retained
/// values such as "zzzz", truncated or noncanonical UUIDs, or the wrong
/// UUID version are hard integrity errors with no Work processing and no
/// metadata writes, while a canonical completed CheckRun keeps working as
/// an ordering boundary.
#[tokio::test]
async fn canonical_pending_cursor_is_closed_uuidv4() {
    let fixture = fixture().await;
    let admitted = admit_pending(
        &fixture,
        "reg23-canonical-cursor",
        b"canonical cursor source",
        "v1",
    )
    .await;
    let check_id = admitted["check_id"].as_str().unwrap().to_owned();
    damage_profile(&fixture.owner.store, &check_id).await;
    for broken in [
        "zzzz",
        "not-a-uuid",
        "550e8400-e29b-11d3-a456-426614174000",
        "550E8400-E29B-41D4-A716-446655440000",
        "6ba7b8109dad11d180b400c04fd430c",
        "",
    ] {
        let value = broken.to_owned();
        fixture
            .owner
            .store
            .run(move |db| set_meta(db, PENDING_CURSOR_KEY, &json!(value)))
            .await
            .unwrap();
        let error = pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap_err();
        assert_eq!(error.code, "CHECK_PENDING_CURSOR_CORRUPT");
        assert_eq!(
            retained_quarantine(&fixture.owner.store, &check_id).await,
            Value::Null,
            "a corrupt cursor processes no Work"
        );
    }
    // A canonical but dangling cursor is accepted as an ordering
    // coordinate; the damaged row is then quarantined exactly once.
    let dangling = model::new_id();
    fixture
        .owner
        .store
        .run(move |db| set_meta(db, PENDING_CURSOR_KEY, &json!(dangling)))
        .await
        .unwrap();
    assert!(
        pending_page(&fixture.owner.store, fixture.directory.clone())
            .await
            .unwrap()
            .is_empty()
    );
    let evidence = retained_quarantine(&fixture.owner.store, &check_id).await;
    assert_eq!(evidence["field"], "profile");
    assert_eq!(evidence["reason"], "invalid_subject_shape");

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// A canonical cursor that names a completed CheckRun stays a valid
/// ordering boundary, and reopening the Store preserves the in-progress
/// cycle: recovery resumes after the last completed subject, never after
/// the last selected one.
#[tokio::test]
async fn completed_cursor_and_cycle_survive_reopen_after_hard_failure() {
    let fixture = fixture().await;
    let done_a = admit_pending(&fixture, "reg23-reopen-a", b"reopen source a", "v1").await;
    let done_b = admit_pending(&fixture, "reg23-reopen-b", b"reopen source b", "v1").await;
    let done_ids = vec![
        done_a["check_id"].as_str().unwrap().to_owned(),
        done_b["check_id"].as_str().unwrap().to_owned(),
    ];
    for id in &done_ids {
        mark_preflight_terminal(&fixture.owner.store, id).await;
    }
    fixture.owner.store.reconcile_checks_once().await.unwrap();
    let mut done_lexical = done_ids.clone();
    done_lexical.sort();
    assert_eq!(
        retained_cursor(&fixture.owner.store).await,
        Some(json!(done_lexical[1].clone())),
        "both rows complete through the corrected seam"
    );
    // Explicitly naming the completed row keeps working as a boundary for
    // later admissions.
    let boundary = done_lexical[1].clone();
    fixture
        .owner
        .store
        .run(move |db| set_meta(db, PENDING_CURSOR_KEY, &json!(boundary)))
        .await
        .unwrap();
    let late = admit_pending(&fixture, "reg23-reopen-late", b"reopen late", "v1").await;
    let late_id = late["check_id"].as_str().unwrap().to_owned();
    let page = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert!(
        page.iter().any(|work| work.check_id == late_id),
        "a completed cursor never hides later admissions"
    );
    // Put every newly admitted UUIDv4 in one lexical suffix for the fault
    // contour; the earlier completed-row boundary is covered above.
    fixture
        .owner
        .store
        .run(|db| {
            set_meta(
                db,
                PENDING_CURSOR_KEY,
                &json!("00000000-0000-4000-8000-000000000000"),
            )
        })
        .await
        .unwrap();
    // Stall one row of the next page with a hard failure: the rows before
    // it complete, and the cursor plus the captured cycle stay exactly
    // there across reopen.
    let fourth = admit_pending(&fixture, "reg23-reopen-d", b"reopen source d", "v1").await;
    let fifth = admit_pending(&fixture, "reg23-reopen-e", b"reopen source e", "v1").await;
    let mut pending_ids = vec![
        late_id.clone(),
        fourth["check_id"].as_str().unwrap().to_owned(),
        fifth["check_id"].as_str().unwrap().to_owned(),
    ];
    for id in &pending_ids {
        mark_preflight_terminal(&fixture.owner.store, id).await;
    }
    pending_ids.sort();
    let stalled = pending_ids[1].clone();
    let stalled_trigger = stalled.clone();
    fixture
        .owner
        .store
        .run(move |db| {
            db.execute_batch(&format!(
                "CREATE TRIGGER reg23_fail_middle_finish BEFORE UPDATE ON check_runs WHEN NEW.check_id='{stalled_trigger}' BEGIN SELECT RAISE(ABORT,'injected middle-row finish failure'); END;"
            ))?;
            Ok(())
        })
        .await
        .unwrap();
    let error = fixture
        .owner
        .store
        .reconcile_checks_once()
        .await
        .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    // The first row of the page completed (terminal or examined no-op);
    // the stalled middle row stays eligible.
    let cursor_before = retained_cursor(&fixture.owner.store).await;
    assert_eq!(cursor_before, Some(json!(pending_ids[0].clone())));
    let cycle_before = retained_cycle_cut(&fixture.owner.store).await;
    assert!(cycle_before.is_some(), "the in-progress cycle is durable");
    assert_eq!(
        check_state(&fixture.owner.store, &stalled).await["state"],
        "queued",
        "the stalled row stays eligible"
    );

    fixture.owner.close().await.unwrap();
    let credential = crate::platform::bootstrap_credential(&fixture.directory).unwrap();
    let root = crate::platform::DataRoot::acquire(&fixture.directory).unwrap();
    let reopened =
        crate::store::StoreOwner::start(root, fixture.config.clone(), credential.clone())
            .await
            .unwrap();
    let reopened_store = reopened.store.clone();
    reopened_store.authenticate(credential).await.unwrap();
    assert_eq!(retained_cursor(&reopened_store).await, cursor_before);
    assert_eq!(retained_cycle_cut(&reopened_store).await, cycle_before);
    // Recovery resumes after the last completed subject: the stalled row
    // heads the next page, not the rows already completed before it.
    let head = pending_page(&reopened_store, fixture.directory.clone())
        .await
        .unwrap();
    assert_eq!(head[0].check_id, stalled);
    reopened
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_middle_finish")?;
            Ok(())
        })
        .await
        .unwrap();
    reopened.store.reconcile_checks_once().await.unwrap();
    reopened.store.reconcile_checks_once().await.unwrap();
    for id in pending_ids.iter().chain(done_ids.iter()) {
        assert_eq!(check_state(&reopened_store, id).await["state"], "error");
    }

    reopened.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// The bounded cycle revisits a failed older subject within a deterministic
/// number of passes while later admissions keep arriving on either lexical
/// side: post-cut admissions stay out of the active page, and the reserved
/// wrap prefix keeps moving, so the failed row is reprocessed every pass
/// until it recovers instead of starving behind a replenished suffix.
#[tokio::test]
async fn pending_cycle_revisits_failed_subject_under_continuous_admissions() {
    let fixture = fixture().await;
    let mut initial = Vec::new();
    for index in 0..8 {
        let project_id = format!("reg23-cycle-{index}");
        let content = format!("cycle fairness source {index}");
        initial.push(admit_pending(&fixture, &project_id, content.as_bytes(), "v1").await);
    }
    for receipt in &initial {
        mark_preflight_terminal(&fixture.owner.store, receipt["check_id"].as_str().unwrap()).await;
    }
    let head = pending_page(&fixture.owner.store, fixture.directory.clone())
        .await
        .unwrap();
    assert_eq!(head.len(), 8);
    let mut ordered: Vec<String> = head.iter().map(|work| work.check_id.clone()).collect();
    ordered.sort();
    let failed = ordered[2].clone();
    let failed_trigger = failed.clone();
    fixture
        .owner
        .store
        .run(move |db| {
            db.execute_batch(&format!(
                "CREATE TRIGGER reg23_fail_cycle_row BEFORE UPDATE ON check_runs WHEN NEW.check_id='{failed_trigger}' BEGIN SELECT RAISE(ABORT,'injected cycle-row finish failure'); END;"
            ))?;
            Ok(())
        })
        .await
        .unwrap();
    let error = fixture
        .owner
        .store
        .reconcile_checks_once()
        .await
        .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    assert_eq!(
        retained_cursor(&fixture.owner.store).await,
        Some(json!(ordered[1].clone())),
        "the cursor stops at the last completed subject before the failure"
    );
    // Three rounds of later admissions: every pass still reaches the
    // failed row (each pass ends in its hard failure) and the cursor never
    // moves past it.
    for round in 0..3 {
        for index in 0..3 {
            let flood_project = format!("reg23-cycle-flood-{round}-{index}");
            let flood_content = format!("cycle flood {round} {index}");
            admit_pending(&fixture, &flood_project, flood_content.as_bytes(), "v1").await;
        }
        let error = fixture
            .owner
            .store
            .reconcile_checks_once()
            .await
            .unwrap_err();
        assert_eq!(error.code, "STORE_ERROR");
        assert_ne!(
            retained_cursor(&fixture.owner.store).await,
            Some(json!(&failed)),
            "continuous admissions never push the cursor past the failed row"
        );
    }
    // Recovery converges: the failed row terminalizes through the same
    // real gates, and every initial row reaches terminal exactly once.
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_cycle_row")?;
            Ok(())
        })
        .await
        .unwrap();
    for _ in 0..4 {
        fixture.owner.store.reconcile_checks_once().await.unwrap();
    }
    assert_eq!(
        check_state(&fixture.owner.store, &failed).await["state"],
        "error",
        "the failed row converges after recovery"
    );
    for receipt in &initial {
        assert_eq!(
            check_state(&fixture.owner.store, receipt["check_id"].as_str().unwrap()).await["state"],
            "error"
        );
    }

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// The supervisor consumes the same corrected seam: a hard failure stops
/// the pass with the cursor at the last completed subject, a handled
/// subject-local launch defect advances only after its incident is retained
/// (authorizing no execution and releasing no custody), and recovery
/// converges with a single retained incident and no second worker.
#[tokio::test]
async fn supervisor_hard_failure_keeps_last_completed_cursor_and_converges_on_retry() {
    let fixture = fixture().await;
    let admitted_a = admit_pending(&fixture, "reg23-sup-a", b"supervisor source a", "v1").await;
    let admitted_b = admit_pending(&fixture, "reg23-sup-b", b"supervisor source b", "v1").await;
    let admitted_c = admit_pending(&fixture, "reg23-sup-c", b"supervisor source c", "v1").await;
    let row_a = admitted_a["check_id"].as_str().unwrap().to_owned();
    let row_b = admitted_b["check_id"].as_str().unwrap().to_owned();
    let row_c = admitted_c["check_id"].as_str().unwrap().to_owned();
    // Assign test roles to the actual publicly minted identities in page
    // order. Never rewrite CheckRun IDs or their retained receipt backlinks.
    let mut lexical = vec![row_a, row_b, row_c];
    lexical.sort();
    let row_a = lexical[0].clone();
    let row_b = lexical[1].clone();
    let row_c = lexical[2].clone();
    mark_preflight_terminal(&fixture.owner.store, &row_a).await;
    mark_preflight_terminal(&fixture.owner.store, &row_b).await;
    // A launch receipt the supervisor can read but never accept: valid
    // JSON with the wrong shape fails closed as a subject-local incident
    // instead of authorizing execution.
    let c_dir = worker::directory(&fixture.directory, &row_c).unwrap();
    std::fs::create_dir_all(&c_dir).unwrap();
    std::fs::write(c_dir.join("launch.json"), b"{\"version\":2}").unwrap();
    let launch_bytes = std::fs::read(c_dir.join("launch.json")).unwrap();
    // Order the fault and the incident row by page position: the trigger
    // hits the middle row so the first row must complete first.
    assert_eq!(lexical.len(), 3);
    let fault_trigger = lexical[1].clone();
    fixture
        .owner
        .store
        .run(move |db| {
            db.execute_batch(&format!(
                "CREATE TRIGGER reg23_fail_sup_finish BEFORE UPDATE ON check_runs WHEN NEW.check_id='{fault_trigger}' BEGIN SELECT RAISE(ABORT,'injected supervisor finish failure'); END;"
            ))?;
            Ok(())
        })
        .await
        .unwrap();
    let (_stop_tx, stopping) = tokio::sync::watch::channel(false);
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        fixture.owner.store.clone().supervise_checks(stopping),
    )
    .await
    .expect("supervisor must return the injected hard failure")
    .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch("DROP TRIGGER reg23_fail_sup_finish")?;
            Ok(())
        })
        .await
        .unwrap();
    assert_eq!(
        retained_cursor(&fixture.owner.store).await,
        Some(json!(lexical[0].clone())),
        "the supervisor stops with the cursor at the last completed subject"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &lexical[0]).await["state"],
        "error"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &lexical[1]).await["state"],
        "queued",
        "the faulted row stays eligible"
    );
    // Recovery converges through the supervisor: the middle row finishes,
    // the launch-defective row is held by incident without execution, and
    // the run stops cleanly with no owned child.
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let supervising = fixture.owner.store.clone().supervise_checks(stop_rx);
    tokio::pin!(supervising);
    let held = lexical[2].clone();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(20), async {
        loop {
            tokio::select! {
                outcome = &mut supervising => {
                    break outcome;
                }
                _ = tokio::time::sleep(std::time::Duration::from_millis(50)) => {
                    if retained_cursor(&fixture.owner.store).await == Some(json!(&held)) {
                        stop_tx.send_replace(true);
                    }
                }
            }
        }
    })
    .await
    .expect("supervisor must converge on the held row");
    outcome.unwrap();
    assert_eq!(
        check_state(&fixture.owner.store, &lexical[1]).await["state"],
        "error",
        "the faulted row finishes on retry"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &lexical[2]).await["state"],
        "queued",
        "the held row stays nonterminal without execution"
    );
    let held_incident_key = format!("check:{}:CONFLICT", lexical[2]);
    let incidents = fixture
        .owner
        .store
        .run(move |db| {
            Ok(db.query_row(
                "SELECT count(*) FROM incidents WHERE dedup_key=?1",
                params![held_incident_key],
                |row| row.get::<_, i64>(0),
            )?)
        })
        .await
        .unwrap();
    assert_eq!(incidents, 1, "the held row retains exactly one incident");
    assert_eq!(
        std::fs::read(c_dir.join("launch.json")).unwrap(),
        launch_bytes,
        "the defective receipt is only observed, never repaired"
    );
    for check_id in [lexical[0].as_str(), lexical[1].as_str()] {
        let dir = worker::directory(&fixture.directory, check_id).unwrap();
        for receipt in ["launch.json", "worker.json", "go.json"] {
            assert!(
                !dir.join(receipt).exists(),
                "terminalized rows never authorize a worker"
            );
        }
    }
    for receipt in ["worker.json", "go.json", "completion.json"] {
        assert!(
            !c_dir.join(receipt).exists(),
            "the held row never authorizes execution"
        );
    }
    assert_eq!(
        fixture.owner.store.check_child_custody_count().await,
        0,
        "no child custody is retained"
    );

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}

/// A cursor-write failure after a stable subject never erases CheckRun
/// truth and never starts a second worker: the terminal disposition is
/// already durable, and retry converges by exact readback of the same
/// subject without republishing its completion.
#[tokio::test]
async fn pending_cursor_write_failure_preserves_truth_and_converges_on_retry() {
    let fixture = fixture().await;
    let admitted_a =
        admit_pending(&fixture, "reg23-cursor-a", b"cursor write source a", "v1").await;
    let admitted_b =
        admit_pending(&fixture, "reg23-cursor-b", b"cursor write source b", "v1").await;
    let mut ordered_ids = [
        admitted_a["check_id"].as_str().unwrap().to_owned(),
        admitted_b["check_id"].as_str().unwrap().to_owned(),
    ];
    ordered_ids.sort();
    let row_a = ordered_ids[0].clone();
    let row_b = ordered_ids[1].clone();
    mark_preflight_terminal(&fixture.owner.store, &row_a).await;
    mark_preflight_terminal(&fixture.owner.store, &row_b).await;
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "CREATE TRIGGER reg23_fail_cursor_insert BEFORE INSERT ON meta WHEN NEW.key='checks:pending:v1:cursor' BEGIN SELECT RAISE(ABORT,'injected cursor write failure'); END; CREATE TRIGGER reg23_fail_cursor_update BEFORE UPDATE ON meta WHEN NEW.key='checks:pending:v1:cursor' BEGIN SELECT RAISE(ABORT,'injected cursor write failure'); END;"
            )?;
            Ok(())
        })
        .await
        .unwrap();
    let error = fixture
        .owner
        .store
        .reconcile_checks_once()
        .await
        .unwrap_err();
    assert_eq!(error.code, "STORE_ERROR");
    let first = row_a.clone();
    let dir_a = worker::directory(&fixture.directory, &first).unwrap();
    let completion_before = std::fs::read(dir_a.join("completion.json")).unwrap();
    assert_eq!(
        check_state(&fixture.owner.store, &row_a).await["state"],
        "error",
        "the terminal disposition stays durable without its cursor"
    );
    assert_eq!(retained_cursor(&fixture.owner.store).await, None);
    assert_eq!(retained_cycle_cut(&fixture.owner.store).await, None);
    assert_eq!(
        check_state(&fixture.owner.store, &row_b).await["state"],
        "queued",
        "the unexamined row stays eligible"
    );
    fixture
        .owner
        .store
        .run(|db| {
            db.execute_batch(
                "DROP TRIGGER reg23_fail_cursor_insert; DROP TRIGGER reg23_fail_cursor_update;",
            )?;
            Ok(())
        })
        .await
        .unwrap();
    fixture.owner.store.reconcile_checks_once().await.unwrap();
    assert_eq!(
        std::fs::read(dir_a.join("completion.json")).unwrap(),
        completion_before,
        "retry converges by exact readback without republishing"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &row_a).await["state"],
        "error"
    );
    assert_eq!(
        check_state(&fixture.owner.store, &row_b).await["state"],
        "error",
        "the remaining row finishes on retry"
    );
    assert_eq!(
        fixture.owner.store.check_child_custody_count().await,
        0,
        "retry starts no second worker"
    );

    fixture.owner.close().await.unwrap();
    std::fs::remove_dir_all(fixture.directory).unwrap();
}
