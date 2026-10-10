use super::*;
use crate::automation::actions::AutomationStep;
use rusqlite::{Connection, OptionalExtension, params};

const OWNER: &str = "cron-candidate-owner";
const PROJECT: &str = "cron-candidate-project";
const AUTOMATION_ID: &str = "candidate-check";
const NOW_MS: i64 = 120_000;

fn memory_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch(
        "CREATE TABLE meta(key TEXT PRIMARY KEY, value_json TEXT NOT NULL);
         CREATE TABLE attempts(attempt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL);",
    )
    .unwrap();
    db
}

fn pending_candidate_db() -> (Connection, String, String, String, i64) {
    let db = memory_db();
    let calendar = scheduler::calendar::CalendarDefinition {
        expression: "0 * * * * *".to_owned(),
        timezone: "UTC".to_owned(),
        anchor_ms: 0,
    };
    let mut entry = AutomationEntry::new(OWNER, PROJECT, AUTOMATION_ID, 0);
    entry.enabled = true;
    entry.steps = vec![AutomationStep::CheckRun];
    entry.cron = Some(CronSettings {
        calendar: calendar.clone(),
        action: scheduler::ScheduleAction::CheckRun {
            attempt_id: "missing-cron-attempt".to_owned(),
            expected_task_revision: 1,
            candidate_ref: "source-candidate".to_owned(),
            profile_id: "strict".to_owned(),
            profile_revision: "v1".to_owned(),
        },
    });
    config::validate_entry(&entry).unwrap();
    config::write_record(
        &db,
        &config::entry_key(OWNER, PROJECT, AUTOMATION_ID).unwrap(),
        &entry.value().unwrap(),
    )
    .unwrap();
    set_meta(
        &db,
        &format!("client:{OWNER}"),
        &json!({"role":"manager","disabled":false}),
    )
    .unwrap();
    set_meta(&db, "execution_mode", &json!({"new_work":"enabled"})).unwrap();

    let (origin_manager_id, logical_id) = logical_id(&db, &entry).unwrap();
    let generation = scheduler::calendar::generation_digest(&calendar).unwrap();
    let due_at_ms = scheduler::calendar::latest_due(&calendar, NOW_MS, None)
        .unwrap()
        .unwrap()
        .due_at_ms;
    let index_key = due_key(0, &logical_id).unwrap();
    let index = DueIndex {
        schema_version: SCHEMA_VERSION,
        logical_id: logical_id.clone(),
        origin_manager_id: origin_manager_id.clone(),
        current_owner_manager_id: OWNER.to_owned(),
        project_id: PROJECT.to_owned(),
        automation_id: AUTOMATION_ID.to_owned(),
        generation: generation.clone(),
        wake_at_ms: 0,
    };
    let state = GenerationState {
        schema_version: SCHEMA_VERSION,
        logical_id: logical_id.clone(),
        origin_manager_id,
        project_id: PROJECT.to_owned(),
        automation_id: AUTOMATION_ID.to_owned(),
        generation: generation.clone(),
        activation_cut_ms: 0,
        include_existing: true,
        last_considered_due_ms: None,
        last_occurrence_id: None,
        last_operation_id: None,
        last_operation_state: None,
        last_receipt: None,
        indexed_due_at_ms: Some(0),
        updated_at_ms: 0,
    };
    set_meta(
        &db,
        &active_key(&logical_id),
        &json!(ActiveGeneration {
            schema_version: SCHEMA_VERSION,
            generation: generation.clone(),
        }),
    )
    .unwrap();
    write_state(&db, &state).unwrap();
    set_meta(&db, &index_key, &json!(index)).unwrap();
    (db, index_key, logical_id, generation, due_at_ms)
}

#[test]
fn real_candidate_with_missing_attempt_remains_held_and_due() {
    let (mut db, original_index_key, logical_id, generation, due_at_ms) = pending_candidate_db();
    let tx = db.transaction().unwrap();
    let batch = prepare_batch(&tx, 1, NOW_MS).unwrap();
    assert!(batch.candidates.is_empty());
    assert_eq!(batch.next_due_at_ms, Some(retry_wake(NOW_MS)));
    tx.commit().unwrap();

    let held: HeldIndex = read_json(&db, &held_key(&logical_id), "held cron index")
        .unwrap()
        .unwrap();
    assert_eq!(held.first_due_at_ms, due_at_ms);
    let state = load_state(&db, &logical_id, &generation).unwrap().unwrap();
    assert_eq!(state.indexed_due_at_ms, Some(retry_wake(NOW_MS)));
    assert!(meta(&db, &original_index_key).unwrap().is_none());
    let retry_index = meta(&db, &due_key(retry_wake(NOW_MS), &logical_id).unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(retry_index["wake_at_ms"], retry_wake(NOW_MS));
}

#[test]
fn quarantine_retains_digest_of_exact_whitespace_source_row() {
    let mut db = memory_db();
    let index_key = due_key(0, "quarantine-subject").unwrap();
    let source_value = json!({
        "schema_version":SCHEMA_VERSION,
        "logical_id":"quarantine-subject",
        "origin_manager_id":OWNER,
        "current_owner_manager_id":OWNER,
        "project_id":PROJECT,
        "automation_id":AUTOMATION_ID,
        "generation":"fixture-generation",
        "wake_at_ms":0,
    });
    let source_raw = format!(" \n{}\t", serde_json::to_string(&source_value).unwrap());
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
        params![index_key, source_raw],
    )
    .unwrap();

    let tx = db.transaction().unwrap();
    let disposition = classify_cron_candidate(
        &tx,
        &index_key,
        Error::new("AUTOMATION_RECORD_CORRUPT", "fixture corruption"),
        10,
    )
    .unwrap();
    let SubjectDisposition::Quarantined { code, evidence } = disposition else {
        panic!("recognized retained-row corruption must be quarantined");
    };
    assert_eq!(code, "AUTOMATION_RECORD_CORRUPT");
    assert_eq!(
        evidence.source_digest,
        Some(model::digest(source_raw.as_bytes()))
    );
    assert!(exact_meta_value(&tx, &index_key).unwrap().is_none());
    tx.commit().unwrap();

    let record_key: String = db
        .query_row(
            "SELECT key FROM meta WHERE key LIKE ?1",
            [format!("{CRON_QUARANTINE_PREFIX}%")],
            |row| row.get(0),
        )
        .unwrap();
    let record = config::read_record(&db, &record_key, "cron quarantine")
        .unwrap()
        .unwrap();
    assert_eq!(record["code"], "AUTOMATION_RECORD_CORRUPT");
    let expected_evidence = cron_subject_evidence(&index_key, &source_raw);
    assert_eq!(
        record["evidence"]["subject_identity"],
        expected_evidence.subject_identity
    );
    assert_eq!(
        record["evidence"]["source_pointer"],
        expected_evidence.source_pointer.unwrap()
    );
    assert_eq!(
        record["evidence"]["source_digest"],
        model::digest(source_raw.as_bytes())
    );
    assert_eq!(record["occurrences"], 1);
    let source_after: Option<String> = db
        .query_row(
            "SELECT value_json FROM meta WHERE key=?1",
            [&index_key],
            |row| row.get(0),
        )
        .optional()
        .unwrap();
    assert!(source_after.is_none());
}

#[test]
fn quarantine_cas_rejects_whitespace_only_source_mutation() {
    let mut db = memory_db();
    let index_key = due_key(0, "cas-subject").unwrap();
    let source_raw = r#"{"schema_version":1,"logical_id":"cas-subject","origin_manager_id":"cron-candidate-owner","current_owner_manager_id":"cron-candidate-owner","project_id":"cron-candidate-project","automation_id":"candidate-check","generation":"fixture-generation","wake_at_ms":0}"#;
    let changed_raw = r#"{ "schema_version" : 1, "logical_id" : "cas-subject", "origin_manager_id" : "cron-candidate-owner", "current_owner_manager_id" : "cron-candidate-owner", "project_id" : "cron-candidate-project", "automation_id" : "candidate-check", "generation" : "fixture-generation", "wake_at_ms" : 0 }"#;
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
        params![index_key, source_raw],
    )
    .unwrap();
    db.execute_batch(&format!(
        "CREATE TRIGGER mutate_cron_subject AFTER INSERT ON meta
         WHEN NEW.key LIKE '{CRON_QUARANTINE_PREFIX}%'
         BEGIN
             UPDATE meta SET value_json='{changed_raw}' WHERE key='{index_key}';
         END;"
    ))
    .unwrap();

    let tx = db.transaction().unwrap();
    let error = match classify_cron_candidate(
        &tx,
        &index_key,
        Error::new("AUTOMATION_RECORD_CORRUPT", "fixture corruption"),
        10,
    ) {
        Err(error) => error,
        Ok(_) => panic!("a byte-changed source row must fail compare-and-swap"),
    };
    assert_eq!(error.code, "AUTOMATION_CRON_SUBJECT_CHANGED");
    assert_eq!(
        exact_meta_value(&tx, &index_key).unwrap().as_deref(),
        Some(changed_raw)
    );
    let retained_during_tx: i64 = tx
        .query_row(
            "SELECT count(*) FROM meta WHERE key LIKE ?1",
            [format!("{CRON_QUARANTINE_PREFIX}%")],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_during_tx, 1);
    tx.rollback().unwrap();

    assert_eq!(
        exact_meta_value(&db, &index_key).unwrap().as_deref(),
        Some(source_raw)
    );
    let retained_after_rollback: i64 = db
        .query_row(
            "SELECT count(*) FROM meta WHERE key LIKE ?1",
            [format!("{CRON_QUARANTINE_PREFIX}%")],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(retained_after_rollback, 0);
}

#[test]
fn unknown_and_non_candidate_errors_remain_fatal() {
    let mut db = memory_db();
    let index_key = due_key(0, "unknown-subject").unwrap();
    let source_raw = "{}";
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
        params![index_key, source_raw],
    )
    .unwrap();
    let tx = db.transaction().unwrap();
    for code in [
        "SQLITE_BUSY",
        "NOT_FOUND",
        "CHECK_SOURCE_REQUIRED",
        "AUTOMATION_LINK_CORRUPT",
        "AUTOMATION_FUTURE_ERROR",
    ] {
        let error =
            match classify_cron_candidate(&tx, &index_key, Error::new(code, "fixture error"), 10) {
                Err(error) => error,
                Ok(_) => {
                    panic!("{code} must remain fatal outside the exact candidate producer set")
                }
            };
        assert_eq!(error.code, code);
        assert_eq!(
            exact_meta_value(&tx, &index_key).unwrap().as_deref(),
            Some(source_raw)
        );
    }
    for code in ["AUTOMATION_ACTION_UNAVAILABLE", "AUTOMATION_RECORD_CORRUPT"] {
        let error = classify_cron_candidate(
            &tx,
            &index_key,
            Error::new(code, "candidate error with storage failure")
                .with_secondary_code("SQLITE_BUSY"),
            10,
        )
        .err()
        .expect("secondary infrastructure failures must remain fatal");
        assert_eq!(error.code, code);
        assert_eq!(error.secondary_codes, vec!["SQLITE_BUSY"]);
        assert_eq!(
            exact_meta_value(&tx, &index_key).unwrap().as_deref(),
            Some(source_raw)
        );
    }
    let quarantine_count: i64 = tx
        .query_row(
            "SELECT count(*) FROM meta WHERE key LIKE ?1",
            [format!("{CRON_QUARANTINE_PREFIX}%")],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(quarantine_count, 0);
    tx.rollback().unwrap();
}
