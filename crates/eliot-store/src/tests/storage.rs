use super::*;

#[test]
fn source_contention_does_not_fault_writer() {
    let path = std::env::temp_dir().join(format!("eliot-source-lock-{}.sqlite", Ulid::new()));
    let mut store = Store::open(&path).unwrap();
    store
        .connection
        .busy_timeout(std::time::Duration::ZERO)
        .unwrap();
    // Rollback journal mode also lets a concurrent reader block COMMIT.
    store
        .connection
        .pragma_update(None, "journal_mode", "DELETE")
        .unwrap();
    let mut external = rusqlite::Connection::open(&path).unwrap();
    let now = i64::try_from(Ulid::new().timestamp_ms()).unwrap();
    let agent = ready_agent(&mut store, "source-contention", now);
    let batch = SourceBatch {
        batch_id: "usage".to_owned(),
        agent_id: agent.clone(),
        generation: 1,
        source: "stream".to_owned(),
        events: vec![NativeEvent {
            ts: None,
            kind: "session.usage".to_owned(),
            native_id: "usage".to_owned(),
            data: json!({"usage": {"context_used_tokens": 10}}),
        }],
        cursor: None,
    };
    let gap = SourceGap {
        gap_id: "rejected-usage".to_owned(),
        agent_id: agent.clone(),
        generation: 1,
        source: "stream".to_owned(),
        families: vec!["session".to_owned()],
        reason: "events.put rejected: busy".to_owned(),
        from: None,
        to: None,
    };
    let health = eliot_core::SourceHealthUpdate::new(&agent, 1, "stream", "stale");
    for behavior in [
        rusqlite::TransactionBehavior::Immediate,
        rusqlite::TransactionBehavior::Deferred,
    ] {
        let lock = external.transaction_with_behavior(behavior).unwrap();
        // An immediate transaction owns the writer lock. A deferred
        // transaction takes a shared lock on its first read.
        lock.query_row("SELECT count(*) FROM events", [], |row| {
            row.get::<_, i64>(0)
        })
        .unwrap();
        assert_eq!(
            store.put_batch(batch.clone(), now + 1).unwrap_err().code,
            "busy"
        );
        assert_eq!(
            store
                .record_rejected_batch_gap(gap.clone(), (&agent, 1), None, now + 2)
                .unwrap_err()
                .code,
            "busy"
        );
        assert_eq!(
            store
                .set_source_health(health.clone(), now + 3)
                .unwrap_err()
                .code,
            "busy"
        );
        assert!(!store.is_write_faulted());
        assert!(store.get_session_usage(&agent, 1).unwrap().is_none());
        lock.rollback().unwrap();
    }

    let change = store
        .record_rejected_batch_gap(gap, (&agent, 1), None, now + 4)
        .unwrap();
    assert_eq!(change.events[0].kind, "source.gap");
    assert_eq!(change.events[1].kind, "source.health");
    assert_eq!(change.events[1].data["health"], "stale");
    assert_eq!(
        store.put_batch(batch, now + 5).unwrap().events[0].kind,
        "session.usage"
    );
    assert!(!store.is_write_faulted());
    drop(external);
    drop(store);
    fs::remove_file(path).unwrap();
}

#[test]
fn txn_stats_count_every_committed_transaction() {
    let before = Store::open(Path::new(":memory:"))
        .expect("open store")
        .txn_stats();
    let mut store = Store::open(Path::new(":memory:")).expect("open store");
    let now = i64::try_from(Ulid::new().timestamp_ms()).expect("timestamp fits i64");
    store
        .record_write(
            &Ulid::new().to_string(),
            "owner",
            "test.method",
            &json!({}),
            &json!({}),
            now,
        )
        .expect("record write");
    let after = store.txn_stats();
    assert!(
        after.commits > before.commits,
        "one write commits at least one transaction: {before:?} -> {after:?}"
    );
}

#[cfg(unix)]
#[test]
fn new_store_database_files_are_current_user_only() {
    use std::os::unix::fs::PermissionsExt;

    // The database file itself carries an explicit 0600 creation mode, so it
    // is private under any ambient umask. WAL/SHM/journal siblings inherit
    // the database file's permissions, unadulterated by the umask (SQLite
    // `src/os_unix.c`: "We want those files to have *exactly* the same
    // permissions as their original database, unadulterated by the umask.").
    let path = std::env::temp_dir().join(format!("eliot-perms-{}.sqlite", Ulid::new()));
    let store = Store::open(&path).expect("open new Store");
    drop(store);
    let mode = fs::metadata(&path)
        .expect("new Store database exists")
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "new Store database is 0600");
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        let _ = fs::remove_file(PathBuf::from(&candidate));
    }
    let _ = fs::remove_file(&path);
}

#[cfg(unix)]
#[test]
fn memory_sentinel_creates_no_literal_file() {
    // `:memory:` is the SQLite sentinel, not a filesystem path: opening it
    // must not create a literal file (the Unix pre-creation path bypasses it).
    let sentinel = std::env::current_dir().expect("cwd").join(":memory:");
    let _ = fs::remove_file(&sentinel);
    let store = Store::open(Path::new(":memory:")).expect("open :memory:");
    drop(store);
    assert!(
        !sentinel.exists(),
        "`:memory:` must not create a literal file"
    );
    let _ = fs::remove_file(&sentinel);
}

#[test]
fn open_store_runs_patched_wal_sqlite() {
    // RUSTSEC-2026-0459: bundled SQLite before 3.54.0 can return stale
    // process memory when a database and its sibling WAL disagree on page
    // size. The Store always opens its private DataDir database with WAL,
    // so it must run on the patched bundled SQLite and apply WAL at open.
    // Exposure still needs a less-trusted party to supply the DB and WAL
    // pair; the daemon only opens its own DataDir file.
    assert!(
        rusqlite::version_number() >= 3_054_000,
        "bundled SQLite predates the RUSTSEC-2026-0459 fix"
    );
    let path = std::env::temp_dir().join(format!("eliot-wal-{}", Ulid::new()));
    let store = Store::open(&path).expect("open new Store");
    let journal_mode: String = store
        .connection
        .query_row("PRAGMA journal_mode", (), |row| row.get(0))
        .expect("read journal_mode");
    assert_eq!(
        journal_mode.to_lowercase(),
        "wal",
        "Store must apply WAL journal mode at open"
    );
    drop(store);
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut candidate = path.as_os_str().to_os_string();
        candidate.push(suffix);
        let _ = fs::remove_file(PathBuf::from(&candidate));
    }
    let _ = fs::remove_file(&path);
}
