use super::*;

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
    assert_eq!(
        after.commit_errors, before.commit_errors,
        "the write must not fail its commit"
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
