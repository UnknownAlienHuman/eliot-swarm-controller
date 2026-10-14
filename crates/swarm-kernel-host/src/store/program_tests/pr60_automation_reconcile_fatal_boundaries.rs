use super::*;
use crate::{
    automation::config,
    error::Error,
    store::automation_reconcile::{self, DomainErrorDisposition},
};
use rusqlite::Connection;
use serde_json::json;

#[test]
fn pr60_automation_reconcile_fatal_sqlite_write_rolls_back_domain() {
    let mut db = Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE boundary_writes(subject TEXT NOT NULL);
         CREATE TRIGGER reject_second_subject
         BEFORE INSERT ON boundary_writes
         WHEN NEW.subject='fault'
         BEGIN SELECT RAISE(ABORT, 'injected fatal sqlite write'); END;",
    )
    .unwrap();

    let error = super::super::reconcile_automation_domain(
        &mut db,
        "fatal_fixture",
        |tx| {
            tx.execute(
                "INSERT INTO boundary_writes(subject) VALUES('before-fault')",
                [],
            )?;
            tx.execute("INSERT INTO boundary_writes(subject) VALUES('fault')", [])?;
            Ok(json!({"status":"processed"}))
        },
        DomainErrorDisposition::Fatal,
    )
    .expect_err("an unknown SQLite write error must stay fatal");

    assert_eq!(error.code, "STORE_ERROR");
    let retained_writes: i64 = db
        .query_row("SELECT count(*) FROM boundary_writes", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        retained_writes, 0,
        "the failed domain transaction rolls back"
    );
}

#[test]
fn pr60_automation_reconcile_savepoint_uncertainty_retains_both_causes() {
    let mut db = Connection::open_in_memory().unwrap();
    let tx = db.transaction().unwrap();

    let error = automation_reconcile::with_subject_savepoint::<()>(
        &tx,
        || {
            // End the transaction after the subject savepoint opens, forcing
            // rollback-to-savepoint to fail as an infrastructure boundary.
            tx.execute_batch("ROLLBACK")?;
            Err(Error::new(
                "PR60_UNKNOWN_SUBJECT_FAILURE",
                "injected unknown subject failure",
            ))
        },
        |_| None,
    )
    .expect_err("savepoint uncertainty must abort the enclosing domain");

    assert_eq!(error.code, "AUTOMATION_SUBJECT_SAVEPOINT_UNCERTAIN");
    assert!(
        error
            .secondary_codes
            .contains(&"PR60_UNKNOWN_SUBJECT_FAILURE".to_owned())
    );
    assert!(error.secondary_codes.contains(&"STORE_ERROR".to_owned()));
}

#[test]
fn pr60_automation_reconcile_quarantine_digest_counters_and_namespaces_are_bound() {
    let mut db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY, value_json TEXT NOT NULL);")
        .unwrap();

    let source_key = "automation:v1:entry:fixture";
    let raw = " {\"schema_version\":1,\"id\":\"fixture\"}\t";
    let evidence = automation_reconcile::automation_entry_evidence(source_key, raw);
    let source_key_digest = crate::model::digest(source_key.as_bytes());
    let expected_digest = crate::model::digest(raw.as_bytes());
    let expected_pointer = format!("meta/key-sha256:{source_key_digest}");
    assert_eq!(
        evidence.subject_identity,
        format!("meta-key-sha256:{source_key_digest}")
    );
    assert_eq!(
        evidence.source_pointer.as_deref(),
        Some(expected_pointer.as_str())
    );
    assert_eq!(
        evidence.source_digest.as_deref(),
        Some(expected_digest.as_str())
    );

    let prefixes = [
        "automation:v1:quarantine:dispatch:",
        "automation:v1:quarantine:work-dispatch:",
        "automation:v1:publication:quarantine:",
        "automation:v1:goal-progression:quarantine:",
        "automation:v1:github-projection:quarantine:",
        "automation:v1:cron:quarantine:",
        "automation:v1:review-disposition:quarantine:",
    ];
    let keys = prefixes
        .iter()
        .map(|prefix| automation_reconcile::quarantine_record_key(prefix, &evidence).unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        keys.len(),
        prefixes.len(),
        "each domain key stays namespaced"
    );

    let changed = automation_reconcile::automation_entry_evidence(source_key, &format!("{raw}\n"));
    assert_ne!(
        automation_reconcile::quarantine_record_key(prefixes[2], &evidence).unwrap(),
        automation_reconcile::quarantine_record_key(prefixes[2], &changed).unwrap(),
        "a changed raw source digest gets a different quarantine identity",
    );

    let record_key = automation_reconcile::quarantine_record_key(prefixes[2], &evidence).unwrap();
    let tx = db.transaction().unwrap();
    let first = automation_reconcile::persist_quarantine(
        &tx,
        &record_key,
        "AUTOMATION_RECORD_CORRUPT",
        evidence.clone(),
        10,
    )
    .unwrap();
    let second = automation_reconcile::persist_quarantine(
        &tx,
        &record_key,
        "AUTOMATION_RECORD_CORRUPT",
        evidence.clone(),
        20,
    )
    .unwrap();
    assert_eq!(first.first_seen_at_ms, 10);
    assert_eq!(second.first_seen_at_ms, 10);
    assert_eq!(second.last_seen_at_ms, 20);
    assert_eq!(second.occurrences, 2);

    config::write_record(&tx, &record_key, &json!({"malformed":true})).unwrap();
    let malformed = automation_reconcile::persist_quarantine(
        &tx,
        &record_key,
        "AUTOMATION_RECORD_CORRUPT",
        evidence,
        30,
    )
    .expect_err("malformed retained quarantine metadata must stay hard damage");
    assert_eq!(malformed.code, "AUTOMATION_QUARANTINE_RECORD_CORRUPT");
    tx.rollback().unwrap();
}

#[tokio::test]
async fn pr60_automation_reconcile_public_store_read_failure_is_fatal() {
    let (owner, directory, _) = super::start_store("pr60-fatal-read").await;
    owner
        .store
        .run_fixture(|db| {
            db.execute_batch("ALTER TABLE meta RENAME TO pr60_saved_meta;")?;
            Ok(())
        })
        .await
        .unwrap();

    let error = owner
        .store
        .reconcile_automations_once()
        .await
        .expect_err("a SQLite read failure must not produce success or degraded output");
    assert_eq!(error.code, "STORE_ERROR");

    owner
        .store
        .run_fixture(|db| {
            db.execute_batch("ALTER TABLE pr60_saved_meta RENAME TO meta;")?;
            Ok(())
        })
        .await
        .unwrap();
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn pr60_automation_reconcile_foreign_domain_cursors_are_rejected() {
    let (owner, directory, _) = super::start_store("pr60-foreign-cursors").await;
    owner
        .store
        .run_fixture(|db| {
            for (cursor_key, foreign_pointer) in [
                (
                    "automation:v1:dispatch_global_cursor",
                    "automation:v1:github-projection:global-cursor",
                ),
                (
                    "automation:v1:work-dispatch_global_cursor",
                    "automation:v1:publication:global-cursor",
                ),
                (
                    "automation:v1:publication:global-cursor",
                    "automation:v1:goal-progression_global_cursor",
                ),
                (
                    "automation:v1:goal-progression_global_cursor",
                    "automation:v1:work-dispatch_global_cursor",
                ),
                (
                    "automation:v1:github-projection:global-cursor",
                    "automation:v1:dispatch_global_cursor",
                ),
                (
                    "automation:v1:review-disposition:global-cursor",
                    "automation:v1:github-projection:global-cursor",
                ),
            ] {
                set_meta(
                    db,
                    cursor_key,
                    &json!({"schema_version":1,"last_entry_key":foreign_pointer}),
                )?;
            }
            Ok(())
        })
        .await
        .unwrap();

    let pass = owner
        .store
        .reconcile_automations_once()
        .await
        .expect("each domain reports its own cursor damage and keeps the aggregate truthful");
    for (domain, code) in [
        ("review_dispatch", "AUTOMATION_CURSOR_CORRUPT"),
        ("work_dispatch", "AUTOMATION_WORK_CURSOR_CORRUPT"),
        ("publication", "AUTOMATION_PUBLICATION_CURSOR_CORRUPT"),
        ("goal_progression", "AUTOMATION_GOAL_CURSOR_CORRUPT"),
        (
            "github_projection",
            "AUTOMATION_GITHUB_PROJECTION_CURSOR_CORRUPT",
        ),
    ] {
        assert_eq!(pass[domain]["status"], "degraded", "{domain}: {pass}");
        assert_eq!(pass[domain]["code"], code, "{domain}: {pass}");
        assert_eq!(pass[domain]["cursor_advanced"], false, "{domain}: {pass}");
    }

    let review_disposition = owner
        .store
        .reconcile_review_dispositions_once()
        .await
        .expect("review disposition rejects the foreign domain cursor without consuming it");
    assert_eq!(review_disposition["review_results"]["status"], "degraded");
    assert_eq!(
        review_disposition["review_results"]["code"],
        "AUTOMATION_REVIEW_DISPOSITION_CURSOR_CORRUPT"
    );
    assert_eq!(
        review_disposition["review_results"]["cursor_advanced"],
        false
    );
    owner.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
