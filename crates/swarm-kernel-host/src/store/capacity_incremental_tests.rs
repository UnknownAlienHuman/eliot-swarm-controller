use super::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::json;

fn fixture_db() -> Connection {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE meta(key TEXT PRIMARY KEY NOT NULL, value_json TEXT NOT NULL);")
        .unwrap();
    db
}

fn fixture_scope() -> Value {
    scope_facts(
        &json!({
            "alias":"oc",
            "runtime":"opencode_v2",
            "native_options":{
                "service_id":"svc-a",
                "model":{"providerID":"provider-a"}
            }
        }),
        Some("native-scope-a"),
        "binding-1",
    )
}

fn fixture_evidence(entry_id: &str) -> ResourceEvidence {
    ResourceEvidence {
        entry_id: entry_id.to_owned(),
        kind: ResourceEntryKind::Operation,
        operation_id: Some(entry_id.to_owned()),
        attempt_id: None,
        assignment_id: None,
        method: Some("agent.send".to_owned()),
        task_id: None,
        binding_id: "binding-1".to_owned(),
        binding_generation: 1,
        admitted_at_ms: 100,
        phase: DerivedPhase::Reserved,
        execution_identity: None,
        execution_start_ref: Value::Null,
        release_reason: None,
        unknown_since_ms: None,
    }
}

fn fixture_ledger(scope: &Value, ids: impl IntoIterator<Item = String>) -> CapacityLedger {
    let entries = ids
        .into_iter()
        .map(|entry_id| {
            let evidence = fixture_evidence(&entry_id);
            (entry_id, entry_from_evidence(&evidence, 100))
        })
        .collect();
    CapacityLedger {
        schema_version: CAPACITY_LEDGER_SCHEMA_VERSION,
        scope: scope.clone(),
        entries,
        updated_at_ms: 100,
    }
}

fn seed_incremental(db: &Connection, ledger: &CapacityLedger) {
    let target = ledger.entries.keys().next().unwrap();
    assert_eq!(
        store_ledger(db, ledger, &LedgerStorage::Missing, target).unwrap(),
        LedgerStoreOutcome::Stored
    );
}

fn sync_fixture(db: &Connection, scope: &Value, entry_id: &str, now: i64) -> Result<()> {
    sync_evidence(
        db,
        scope,
        EvidenceDerivation {
            evidence: Some(fixture_evidence(entry_id)),
            damage: None,
        },
        now,
    )
}

fn raw_meta(db: &Connection, key: &str) -> Option<String> {
    db.query_row("SELECT value_json FROM meta WHERE key=?1", [key], |row| {
        row.get(0)
    })
    .optional()
    .unwrap()
}

fn entry_row_count(db: &Connection, scope_key: &str) -> i64 {
    let prefix = capacity_entry_scope_prefix(scope_key);
    db.query_row(
        "SELECT COUNT(*) FROM meta WHERE substr(key,1,length(?1))=?1",
        [prefix],
        |row| row.get(0),
    )
    .unwrap()
}

fn attempt_evidence(entry_id: &str, attempt_id: &str) -> ResourceEvidence {
    let mut evidence = fixture_evidence(entry_id);
    evidence.attempt_id = Some(attempt_id.to_owned());
    evidence.unknown_since_ms = Some(100);
    evidence
}

fn oversized_active_evidence(entry_id: &str, attempt_id: &str) -> ResourceEvidence {
    let mut evidence = attempt_evidence(entry_id, attempt_id);
    evidence.phase = DerivedPhase::Active;
    evidence.execution_identity = Some(ExecutionIdentity {
        operation_id: Some(entry_id.to_owned()),
        native_session_id: Some("native-session-a".to_owned()),
        native_input_id: Some("native-input-a".to_owned()),
        native_run_id: Some("native-run-a".to_owned()),
        source_observation_id: None,
        source_stream_id: None,
        source_event_key: None,
        admission_event_ref: None,
        delivery_event_ref: None,
        execution_start_event_ref: None,
        terminal_event_ref: None,
        terminal_disposition: None,
    });
    evidence.execution_start_ref = json!({"padding":"x".repeat(MAX_CAPACITY_LEDGER_BYTES)});
    evidence
}

fn seed_attempt_ledger(db: &Connection, scope: &Value, legacy: bool) {
    let evidence = attempt_evidence("op-a", "att-a");
    let ledger = CapacityLedger {
        schema_version: CAPACITY_LEDGER_SCHEMA_VERSION,
        scope: scope.clone(),
        entries: BTreeMap::from([("op-a".to_owned(), entry_from_evidence(&evidence, 100))]),
        updated_at_ms: 100,
    };
    if legacy {
        set_meta(
            db,
            &ledger_key(scope["scope_key"].as_str().unwrap()),
            &serde_json::to_value(ledger).unwrap(),
        )
        .unwrap();
    } else {
        seed_incremental(db, &ledger);
    }
}

fn seed_native_unknown(db: &Connection, operation_id: &str) -> String {
    db.execute_batch(
        "CREATE TABLE operations(operation_id TEXT PRIMARY KEY, state TEXT NOT NULL, \
         native_refs_json TEXT NOT NULL);",
    )
    .unwrap();
    let raw = model::canonical(&json!({"native_input_id":"native-input-a"})).unwrap();
    db.execute(
        "INSERT INTO operations(operation_id,state,native_refs_json) \
         VALUES(?1,'outcome_unknown',?2)",
        params![operation_id, raw],
    )
    .unwrap();
    raw
}

fn native_snapshot(db: &Connection, operation_id: &str) -> (String, String) {
    db.query_row(
        "SELECT state,native_refs_json FROM operations WHERE operation_id=?1",
        [operation_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .unwrap()
}

fn assert_attempt_limit_unknown(db: &Connection, scope: &Value, attempt_id: &str, code: &str) {
    let accounting = attempt_accounting(db, scope, attempt_id).unwrap();
    assert_eq!(accounting["status"], "ledger_damaged");
    assert_eq!(accounting["damage"]["code"], code);
    for field in ["reserved", "active", "outcome_unknown", "count"] {
        assert!(accounting["attempt_entries"][field].is_null());
    }
    assert!(accounting["ledger_updated_at_ms"].is_null());
    let scope_key = scope["scope_key"].as_str().unwrap();
    assert!(all_ledgers(db).unwrap().iter().any(|row| {
        matches!(row, LedgerRow::Damaged(key, damage) if key.as_str() == scope_key && damage.code == code)
    }));
}

fn retained_ledger_rows(db: &Connection) -> Vec<(String, String)> {
    let mut stmt = db
        .prepare(
            "SELECT key,value_json FROM meta \
             WHERE key GLOB 'capacity:*' OR key GLOB 'capacity-entry:*' ORDER BY key",
        )
        .unwrap();
    stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn entry_keys_keep_scope_and_identity_separators_unambiguous() {
    let first = capacity_entry_key("runtime:service", "producer:attempt:assignment");
    let second = capacity_entry_key("runtime", "service:producer:attempt:assignment");
    assert_ne!(first, second);
    let prefix = capacity_entry_scope_prefix("runtime:service");
    assert_eq!(
        decode_key_component(first.strip_prefix(&prefix).unwrap()),
        Some("producer:attempt:assignment".to_owned())
    );
}

#[test]
fn legacy_merge_migrates_atomically_and_retains_untouched_entry() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let legacy = fixture_ledger(&scope, ["op-a".to_owned(), "op-b".to_owned()]);
    let untouched_value = serde_json::to_value(&legacy.entries["op-b"]).unwrap();
    let untouched_json = model::canonical(&untouched_value).unwrap();
    let legacy_value = serde_json::to_value(&legacy).unwrap();
    set_meta(&db, &ledger_key(scope_key), &legacy_value).unwrap();

    sync_fixture(&db, &scope, "op-a", 200).unwrap();

    let header_raw = raw_meta(&db, &ledger_key(scope_key)).unwrap();
    let header: Value = serde_json::from_str(&header_raw).unwrap();
    assert_eq!(header["storage_version"], CAPACITY_LEDGER_STORAGE_VERSION);
    assert_eq!(header["entry_count"], 2);
    assert!(header.get("entries").is_none());
    assert_eq!(
        raw_meta(&db, &capacity_entry_key(scope_key, "op-b")).as_deref(),
        Some(untouched_json.as_str())
    );
    let touched: Value =
        serde_json::from_str(&raw_meta(&db, &capacity_entry_key(scope_key, "op-a")).unwrap())
            .unwrap();
    assert_eq!(touched["last_synced_at_ms"], 200);
    assert_eq!(entry_row_count(&db, scope_key), 2);
    let LedgerLoad::Valid(loaded) = load_ledger(&db, &scope).unwrap() else {
        panic!("migrated ledger should reconstruct");
    };
    assert_eq!(loaded.ledger.entries.len(), 2);
    assert!(matches!(loaded.storage, LedgerStorage::Incremental { .. }));
}

#[test]
fn failed_legacy_header_write_rolls_back_migrated_entry_rows() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let legacy = fixture_ledger(&scope, ["op-a".to_owned(), "op-b".to_owned()]);
    let header_key = ledger_key(scope_key);
    set_meta(&db, &header_key, &serde_json::to_value(&legacy).unwrap()).unwrap();
    let old_legacy = raw_meta(&db, &header_key).unwrap();
    db.execute_batch(&format!(
        "CREATE TEMP TRIGGER fail_legacy_capacity_header \
         BEFORE UPDATE ON meta WHEN OLD.key='{header_key}' \
         BEGIN SELECT RAISE(ABORT,'fixture legacy header failure'); END;"
    ))
    .unwrap();

    assert!(sync_fixture(&db, &scope, "op-a", 200).is_err());

    assert_eq!(raw_meta(&db, &header_key).unwrap(), old_legacy);
    assert_eq!(entry_row_count(&db, scope_key), 0);
}

#[test]
fn incremental_merge_updates_only_the_touched_entry_and_header() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let ledger = fixture_ledger(&scope, ["op-a".to_owned(), "op-b".to_owned()]);
    seed_incremental(&db, &ledger);
    let header_key = ledger_key(scope_key);
    let touched_key = capacity_entry_key(scope_key, "op-a");
    let untouched_key = capacity_entry_key(scope_key, "op-b");
    let old_header = raw_meta(&db, &header_key).unwrap();
    let old_touched = raw_meta(&db, &touched_key).unwrap();
    let old_untouched = raw_meta(&db, &untouched_key).unwrap();

    sync_fixture(&db, &scope, "op-a", 200).unwrap();

    assert_ne!(raw_meta(&db, &header_key).unwrap(), old_header);
    assert_ne!(raw_meta(&db, &touched_key).unwrap(), old_touched);
    assert_eq!(raw_meta(&db, &untouched_key).unwrap(), old_untouched);
    assert_eq!(entry_row_count(&db, scope_key), 2);
}

#[test]
fn duplicate_sync_keeps_one_entry_row_and_stable_payload() {
    let db = fixture_db();
    let scope = fixture_scope();

    sync_fixture(&db, &scope, "op-a", 200).unwrap();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let entry_key = capacity_entry_key(scope_key, "op-a");
    let header_key = ledger_key(scope_key);
    let first_entry = raw_meta(&db, &entry_key).unwrap();
    let first_header = raw_meta(&db, &header_key).unwrap();

    sync_fixture(&db, &scope, "op-a", 200).unwrap();

    assert_eq!(raw_meta(&db, &entry_key).unwrap(), first_entry);
    assert_eq!(raw_meta(&db, &header_key).unwrap(), first_header);
    assert_eq!(entry_row_count(&db, scope_key), 1);
}

#[test]
fn failed_header_write_rolls_back_the_touched_entry_update() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let ledger = fixture_ledger(&scope, ["op-a".to_owned(), "op-b".to_owned()]);
    seed_incremental(&db, &ledger);
    let header_key = ledger_key(scope_key);
    let touched_key = capacity_entry_key(scope_key, "op-a");
    let old_header = raw_meta(&db, &header_key).unwrap();
    let old_touched = raw_meta(&db, &touched_key).unwrap();
    db.execute_batch(&format!(
        "CREATE TEMP TRIGGER fail_capacity_header \
         BEFORE UPDATE ON meta WHEN OLD.key='{header_key}' \
         BEGIN SELECT RAISE(ABORT,'fixture header write failure'); END;"
    ))
    .unwrap();

    assert!(sync_fixture(&db, &scope, "op-a", 200).is_err());

    assert_eq!(raw_meta(&db, &header_key).unwrap(), old_header);
    assert_eq!(raw_meta(&db, &touched_key).unwrap(), old_touched);
}

#[test]
fn oversized_legacy_blob_is_damaged_and_never_migrated() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let ledger = fixture_ledger(&scope, ["op-a".to_owned()]);
    let mut raw = model::canonical(&serde_json::to_value(&ledger).unwrap()).unwrap();
    assert!(raw.len() < MAX_CAPACITY_LEDGER_BYTES);
    raw.push_str(&" ".repeat(MAX_CAPACITY_LEDGER_BYTES + 1 - raw.len()));
    let key = ledger_key(scope_key);
    db.execute(
        "INSERT INTO meta(key,value_json) VALUES(?1,?2)",
        params![key, raw],
    )
    .unwrap();
    let old_raw = raw_meta(&db, &key).unwrap();

    assert!(matches!(
        load_ledger(&db, &scope).unwrap(),
        LedgerLoad::Damaged(_)
    ));
    sync_fixture(&db, &scope, "op-a", 200).unwrap();

    assert_eq!(raw_meta(&db, &key).unwrap(), old_raw);
    assert_eq!(entry_row_count(&db, scope_key), 0);
}

#[test]
fn byte_and_entry_budgets_reject_writes_without_partial_rows() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let evidence = fixture_evidence("op-large");
    let mut large_entry = entry_from_evidence(&evidence, 100);
    large_entry.execution_start_ref = json!({
        "padding":"x".repeat(MAX_CAPACITY_LEDGER_BYTES)
    });
    let oversized = CapacityLedger {
        schema_version: CAPACITY_LEDGER_SCHEMA_VERSION,
        scope: scope.clone(),
        entries: BTreeMap::from([("op-large".to_owned(), large_entry)]),
        updated_at_ms: 100,
    };
    assert!(validate_ledger(&oversized, scope_key, Some(&scope)));
    assert_eq!(
        store_ledger(&db, &oversized, &LedgerStorage::Missing, "op-large").unwrap(),
        LedgerStoreOutcome::LimitRefused(CapacityLedgerLimit::Bytes)
    );
    assert!(raw_meta(&db, &ledger_key(scope_key)).is_none());
    assert_eq!(entry_row_count(&db, scope_key), 0);

    let ids = (0..=MAX_CAPACITY_LEDGER_ENTRIES)
        .map(|index| format!("op-{index}"))
        .collect::<Vec<_>>();
    let too_many = fixture_ledger(&scope, ids);
    assert_eq!(
        store_ledger(&db, &too_many, &LedgerStorage::Missing, "op-0").unwrap(),
        LedgerStoreOutcome::LimitRefused(CapacityLedgerLimit::Entries)
    );
    assert!(raw_meta(&db, &ledger_key(scope_key)).is_none());
    assert_eq!(entry_row_count(&db, scope_key), 0);
}

#[test]
fn missing_and_corrupt_incremental_rows_remain_damaged_on_sync() {
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();

    let missing_db = fixture_db();
    let two_entries = fixture_ledger(&scope, ["op-a".to_owned(), "op-b".to_owned()]);
    seed_incremental(&missing_db, &two_entries);
    let header_key = ledger_key(scope_key);
    let header_before = raw_meta(&missing_db, &header_key).unwrap();
    missing_db
        .execute(
            "DELETE FROM meta WHERE key=?1",
            [capacity_entry_key(scope_key, "op-a")],
        )
        .unwrap();
    assert!(matches!(
        load_ledger(&missing_db, &scope).unwrap(),
        LedgerLoad::Damaged(_)
    ));
    sync_fixture(&missing_db, &scope, "op-a", 200).unwrap();
    assert_eq!(raw_meta(&missing_db, &header_key).unwrap(), header_before);
    assert!(raw_meta(&missing_db, &capacity_entry_key(scope_key, "op-a")).is_none());

    let corrupt_db = fixture_db();
    let one_entry = fixture_ledger(&scope, ["op-a".to_owned()]);
    seed_incremental(&corrupt_db, &one_entry);
    let corrupt_key = capacity_entry_key(scope_key, "op-a");
    corrupt_db
        .execute(
            "UPDATE meta SET value_json='{' WHERE key=?1",
            [&corrupt_key],
        )
        .unwrap();
    assert!(matches!(
        load_ledger(&corrupt_db, &scope).unwrap(),
        LedgerLoad::Damaged(_)
    ));
    sync_fixture(&corrupt_db, &scope, "op-a", 200).unwrap();
    assert_eq!(raw_meta(&corrupt_db, &corrupt_key).as_deref(), Some("{"));
}

#[test]
fn orphan_entry_scope_is_reported_as_global_damage() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    let evidence = fixture_evidence("op-a");
    let entry = entry_from_evidence(&evidence, 100);
    set_meta(
        &db,
        &capacity_entry_key(scope_key, "op-a"),
        &serde_json::to_value(entry).unwrap(),
    )
    .unwrap();

    assert!(matches!(
        load_ledger(&db, &scope).unwrap(),
        LedgerLoad::Damaged(_)
    ));
    let rows = all_ledgers(&db).unwrap();
    assert!(matches!(
        rows.as_slice(),
        [LedgerRow::Damaged(scope, damage)]
            if scope.as_str() == "unknown:damaged-ledger-key"
                && damage.code == "ledger_entry_scope_orphaned"
    ));
}

#[test]
fn byte_limit_refusal_makes_attempt_counts_unknown_and_preserves_native_facts() {
    for legacy in [false, true] {
        let db = fixture_db();
        let scope = fixture_scope();
        let scope_key = scope["scope_key"].as_str().unwrap();
        seed_attempt_ledger(&db, &scope, legacy);
        seed_native_unknown(&db, "op-a");
        let before = attempt_accounting(&db, &scope, "att-a").unwrap();
        assert_eq!(before["status"], "recorded_attempt_entries");
        assert_eq!(before["attempt_entries"]["count"], 1);
        let retained_before = retained_ledger_rows(&db);
        let native_refs = model::canonical(&json!({
            "native_input_id":"native-input-a",
            "native_run_id":"native-run-a"
        }))
        .unwrap();
        db.execute_batch("BEGIN").unwrap();
        db.execute(
            "UPDATE operations SET native_refs_json=?1 WHERE operation_id='op-a'",
            [&native_refs],
        )
        .unwrap();
        sync_evidence(
            &db,
            &scope,
            EvidenceDerivation {
                evidence: Some(oversized_active_evidence("op-a", "att-a")),
                damage: None,
            },
            200,
        )
        .unwrap();
        db.execute_batch("COMMIT").unwrap();

        assert_eq!(retained_ledger_rows(&db), retained_before);
        assert_eq!(
            native_snapshot(&db, "op-a"),
            ("outcome_unknown".to_owned(), native_refs)
        );
        assert_attempt_limit_unknown(&db, &scope, "att-a", "ledger_byte_limit_refused");
        let refusal_key = capacity_limit_refusal_key(scope_key);
        let first_refusal = raw_meta(&db, &refusal_key).unwrap();
        assert!(first_refusal.len() <= MAX_CAPACITY_LIMIT_REFUSAL_BYTES);
        sync_fixture(&db, &scope, "op-a", 300).unwrap();
        assert_eq!(raw_meta(&db, &refusal_key).unwrap(), first_refusal);
        assert_eq!(retained_ledger_rows(&db), retained_before);
        assert_attempt_limit_unknown(&db, &scope, "att-a", "ledger_byte_limit_refused");
    }
}

#[test]
fn entry_limit_refusal_makes_new_and_retained_attempt_counts_unknown() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    for index in 0..MAX_CAPACITY_LEDGER_ENTRIES {
        let id = index.to_string();
        let mut compact = json!({
            "entry_id":id.as_str(),
            "kind":"operation",
            "operation_id":id.as_str(),
            "method":"agent.send",
            "binding_id":"b",
            "binding_generation":1,
            "phase":"reserved",
            "admitted_at_ms":100,
            "execution_start_ref":null,
            "last_synced_at_ms":100
        });
        if index == 0 {
            compact["attempt_id"] = json!("att-retained");
        }
        set_meta(&db, &capacity_entry_key(scope_key, &id), &compact).unwrap();
    }
    let header = CapacityLedgerHeader {
        storage_version: CAPACITY_LEDGER_STORAGE_VERSION,
        scope: scope.clone(),
        entry_count: MAX_CAPACITY_LEDGER_ENTRIES,
        updated_at_ms: 100,
    };
    set_meta(
        &db,
        &ledger_key(scope_key),
        &serde_json::to_value(header).unwrap(),
    )
    .unwrap();
    let LedgerLoad::Valid(loaded) = load_ledger(&db, &scope).unwrap() else {
        panic!("compact fixture must fit the unchanged byte and entry bounds");
    };
    assert_eq!(loaded.ledger.entries.len(), MAX_CAPACITY_LEDGER_ENTRIES);
    assert_eq!(
        attempt_accounting(&db, &scope, "att-new").unwrap()["attempt_entries"]["count"],
        0
    );
    let retained_before = retained_ledger_rows(&db);
    let native_refs = seed_native_unknown(&db, "op-refused");
    db.execute_batch("BEGIN").unwrap();
    sync_evidence(
        &db,
        &scope,
        EvidenceDerivation {
            evidence: Some(attempt_evidence("op-refused", "att-new")),
            damage: None,
        },
        200,
    )
    .unwrap();
    db.execute_batch("COMMIT").unwrap();

    assert_eq!(
        entry_row_count(&db, scope_key) as usize,
        MAX_CAPACITY_LEDGER_ENTRIES
    );
    assert_eq!(retained_ledger_rows(&db), retained_before);
    assert_eq!(
        native_snapshot(&db, "op-refused"),
        ("outcome_unknown".to_owned(), native_refs)
    );
    assert_attempt_limit_unknown(&db, &scope, "att-new", "ledger_entry_limit_refused");
    assert_attempt_limit_unknown(&db, &scope, "att-retained", "ledger_entry_limit_refused");
}

#[test]
fn refusal_persistence_failure_rolls_back_without_requeuing_unknown_operation() {
    let db = fixture_db();
    let scope = fixture_scope();
    let scope_key = scope["scope_key"].as_str().unwrap();
    seed_attempt_ledger(&db, &scope, false);
    let native_before = seed_native_unknown(&db, "op-a");
    let retained_before = retained_ledger_rows(&db);
    let refusal_key = capacity_limit_refusal_key(scope_key);
    db.execute_batch(&format!(
        "CREATE TEMP TRIGGER fail_capacity_limit_refusal \
         BEFORE INSERT ON meta WHEN NEW.key='{refusal_key}' \
         BEGIN SELECT RAISE(ABORT,'fixture refusal persistence failure'); END;"
    ))
    .unwrap();
    db.execute_batch("BEGIN").unwrap();
    db.execute(
        "UPDATE operations SET native_refs_json='{\"native_run_id\":\"new-run\"}' \
         WHERE operation_id='op-a'",
        [],
    )
    .unwrap();
    assert!(
        sync_evidence(
            &db,
            &scope,
            EvidenceDerivation {
                evidence: Some(oversized_active_evidence("op-a", "att-a")),
                damage: None,
            },
            200,
        )
        .is_err()
    );
    assert!(!db.is_autocommit());
    assert!(raw_meta(&db, &refusal_key).is_none());
    assert_eq!(retained_ledger_rows(&db), retained_before);
    db.execute_batch("ROLLBACK").unwrap();

    assert_eq!(
        native_snapshot(&db, "op-a"),
        ("outcome_unknown".to_owned(), native_before)
    );
    assert_eq!(retained_ledger_rows(&db), retained_before);
    assert_eq!(
        attempt_accounting(&db, &scope, "att-a").unwrap()["attempt_entries"]["outcome_unknown"],
        1
    );
}

#[test]
fn refusal_without_a_ledger_is_visible_to_scope_and_attempt_projections() {
    let db = fixture_db();
    let scope = fixture_scope();
    sync_evidence(
        &db,
        &scope,
        EvidenceDerivation {
            evidence: Some(oversized_active_evidence("op-a", "att-a")),
            damage: None,
        },
        200,
    )
    .unwrap();
    assert!(retained_ledger_rows(&db).is_empty());
    assert_attempt_limit_unknown(&db, &scope, "att-a", "ledger_byte_limit_refused");
}
