//! Authoritative daemon diagnostics, owned by run(), never by its CLI starter.
//! Records are optional diagnostics, not semantic outcomes or replay authority.

use crate::platform::{
    DataDir,
    data_dir::{DaemonLock, open_private_log_file, rotate_private_log_file},
};
use eliot_core::{
    Error, MAX_STDERR_DIAGNOSTIC_BYTES, Result, SecretPolicy, ViewHealthOutcome,
    bound_stderr_diagnostics,
};
use eliot_plugin::sync::LockExt;
use serde_json::{Map, Value, json};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};

const LOG_BYTES: u64 = 1024 * 1024;
const RECORD_BYTES: usize = 32 * 1024;
static LOG: Mutex<Option<LogFile>> = Mutex::new(None);

/// Holding this guard keeps the daemon's log open; drop closes it before the
/// enclosing single-owner lock is released. No reader/caller retains the file.
pub(crate) struct DaemonLog {
    _owner: (),
}

pub(crate) fn init(data: &DataDir, lock: &DaemonLock) -> Result<DaemonLog> {
    let mut current = LOG.lock_recover();
    if current.is_some() {
        return Err(Error::new(
            "daemon_log_busy",
            "this process already owns a daemon log",
        ));
    }
    *current = Some(LogFile::open(data, lock)?);
    Ok(DaemonLog { _owner: () })
}

impl Drop for DaemonLog {
    fn drop(&mut self) {
        let mut current = LOG.lock_recover();
        if let Some(mut log) = current.take()
            && let Some(file) = log.file.as_mut()
        {
            let _ = file.flush();
        }
    }
}

/// Best effort: neither encoding nor filesystem failure can change an external
/// operation's code, result, acknowledgement or retry behavior. Error.message,
/// requests and inherited environment are deliberately never written here.
pub(crate) fn record(
    policy: &SecretPolicy,
    code: &str,
    ids: &[(&str, &str)],
    error: Option<&Error>,
) {
    write_record(policy, code, ids, None, error)
}

/// Typed nonnegative counts for one concrete diagnostic command. Counts are
/// encoded as JSON numbers under "counts", distinct from string ids, and are
/// never derived from arbitrary caller text.
pub(crate) struct Counts {
    pub(crate) events: u64,
    pub(crate) pruned_through: u64,
    pub(crate) commands: u64,
    pub(crate) writes: u64,
    pub(crate) mail: u64,
}

/// One bounded Muse view-health log record: the cumulative per-agent counters
/// after the increment plus the per-event load context. The reason is a fixed
/// `noneReason` label (never free text); every other field is a typed number
/// or null. Best effort like every other daemon-log write: encoding or
/// filesystem failure never changes the caller's operation.
pub(crate) struct ViewHealthRecord {
    pub(crate) unavailable: u64,
    pub(crate) readback_recovered: u64,
    pub(crate) settled_unknown: u64,
    pub(crate) live_muse_hosts: Option<i64>,
    pub(crate) turn_age_ms: Option<i64>,
    pub(crate) harness_rss_bytes: Option<u64>,
    pub(crate) reason_label: &'static str,
}

pub(crate) fn record_view_health(
    policy: &SecretPolicy,
    outcome: ViewHealthOutcome,
    agent_id: &str,
    generation: i64,
    source: &str,
    record: &ViewHealthRecord,
) {
    let Some(bytes) = encode_view_health(policy, outcome, agent_id, generation, source, record)
    else {
        return;
    };
    let mut current = LOG.lock_recover();
    if let Some(log) = current.as_mut()
        && log.write(&bytes).is_err()
    {
        log.file = None;
        eprintln!("error: daemon_log_unavailable");
    }
}

pub(crate) fn record_counts(
    policy: &SecretPolicy,
    code: &str,
    counts: Counts,
    error: Option<&Error>,
) {
    write_record(policy, code, &[], Some(counts), error)
}

fn write_record(
    policy: &SecretPolicy,
    code: &str,
    ids: &[(&str, &str)],
    counts: Option<Counts>,
    error: Option<&Error>,
) {
    let Some(bytes) = encode_record(policy, code, ids, counts, error) else {
        return;
    };
    let mut current = LOG.lock_recover();
    if let Some(log) = current.as_mut()
        && log.write(&bytes).is_err()
    {
        // Disable this sink without exposing the I/O message or affecting any
        // caller. No fallback file is opened in an unvalidated location.
        log.file = None;
        eprintln!("error: daemon_log_unavailable");
    }
}

fn encode_view_health(
    policy: &SecretPolicy,
    outcome: ViewHealthOutcome,
    agent_id: &str,
    generation: i64,
    source: &str,
    record: &ViewHealthRecord,
) -> Option<Vec<u8>> {
    let value = json!({
        "ts": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
        "code": outcome.log_code(),
        "ids": {
            "agent_id": bounded_text(policy, agent_id, 256),
            "generation": generation,
            "source": bounded_text(policy, source, 64),
        },
        "reason": bounded_text(policy, record.reason_label, 64),
        "view_health": {
            "unavailable": record.unavailable,
            "readback_recovered": record.readback_recovered,
            "settled_unknown": record.settled_unknown,
            "live_muse_hosts": record.live_muse_hosts,
            "turn_age_ms": record.turn_age_ms,
            "harness_rss_bytes": record.harness_rss_bytes,
        },
    });
    let mut bytes = serde_json::to_vec(&value).ok()?;
    if bytes.len() >= RECORD_BYTES {
        return None;
    }
    bytes.push(b'\n');
    Some(bytes)
}

fn bounded_text(policy: &SecretPolicy, text: &str, limit: usize) -> String {
    let safe = policy.redact_text(text);
    let mut end = safe.len().min(limit);
    while !safe.is_char_boundary(end) {
        end -= 1;
    }
    safe[..end].to_owned()
}

fn encode_record(
    policy: &SecretPolicy,
    code: &str,
    ids: &[(&str, &str)],
    counts: Option<Counts>,
    error: Option<&Error>,
) -> Option<Vec<u8>> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let mut value = json!({"ts": ts, "code": bounded_text(policy, code, 64)});
    let mut context = Map::new();
    for &(key, text) in ids.iter().take(8) {
        if matches!(
            key,
            "agent"
                | "agent_id"
                | "generation"
                | "command"
                | "command_id"
                | "turn_id"
                | "request_id"
                | "source"
                | "batch_id"
                | "event_kind"
                | "validation"
                | "boot_id"
                | "pid"
                | "method"
        ) {
            context.insert(
                key.to_owned(),
                Value::String(bounded_text(policy, text, 256)),
            );
        }
    }
    if !context.is_empty() {
        value["ids"] = Value::Object(context);
    }
    if let Some(counts) = counts {
        value["counts"] = json!({
            "events": counts.events,
            "pruned_through": counts.pruned_through,
            "commands": counts.commands,
            "writes": counts.writes,
            "mail": counts.mail,
        });
    }
    if let Some(error) = error {
        value["error_code"] = json!(bounded_text(policy, &error.code, 64));
        let mut diagnostics = error.diagnostics.clone();
        for diagnostic in &mut diagnostics {
            diagnostic.snapshot.text = policy.redact_text(&diagnostic.snapshot.text);
        }
        let remaining = RECORD_BYTES.saturating_sub(serde_json::to_vec(&value).ok()?.len() + 32);
        bound_stderr_diagnostics(&mut diagnostics, remaining.min(MAX_STDERR_DIAGNOSTIC_BYTES));
        if !diagnostics.is_empty() {
            value["diagnostics"] = serde_json::to_value(diagnostics).ok()?;
        }
    }
    let mut bytes = serde_json::to_vec(&value).ok()?;
    if bytes.len() >= RECORD_BYTES {
        return None;
    }
    bytes.push(b'\n');
    Some(bytes)
}

struct LogFile {
    path: PathBuf,
    backup: PathBuf,
    file: Option<File>,
    size: u64,
}

impl LogFile {
    fn open(data: &DataDir, lock: &DaemonLock) -> Result<Self> {
        let directory = data.daemon_log_directory(lock)?;
        let path = directory.join("daemon.log");
        let backup = directory.join("daemon.log.1");
        match fs::symlink_metadata(&backup) {
            Ok(_) => {
                let mut file = open_private_log_file(&backup)?;
                cap_existing(&mut file)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(log_io()),
        }
        let mut file = open_private_log_file(&path)?;
        let size = cap_existing(&mut file)?;
        Ok(Self {
            path,
            backup,
            file: Some(file),
            size,
        })
    }

    fn write(&mut self, bytes: &[u8]) -> Result<()> {
        if self.file.is_none() {
            return Ok(());
        }
        if self.size + bytes.len() as u64 > LOG_BYTES {
            // Close before replacement: Windows must not retain the old writer.
            self.file.take();
            self.file = Some(rotate_private_log_file(&self.path, &self.backup)?);
            self.size = 0;
        }
        let Some(file) = self.file.as_mut() else {
            return Ok(());
        };
        file.write_all(bytes).map_err(|_| log_io())?;
        self.size += bytes.len() as u64;
        Ok(())
    }
}

fn cap_existing(file: &mut File) -> Result<u64> {
    let size = file.metadata().map_err(|_| log_io())?.len();
    if size <= LOG_BYTES {
        return Ok(size);
    }
    file.seek(SeekFrom::Start(size - LOG_BYTES))
        .map_err(|_| log_io())?;
    let mut tail = Vec::with_capacity(LOG_BYTES as usize);
    Read::take(&mut *file, LOG_BYTES)
        .read_to_end(&mut tail)
        .map_err(|_| log_io())?;
    // An oversized preexisting file may start inside a record. Keep only whole
    // trailing lines, within the same fixed file budget.
    if let Some(newline) = tail.iter().position(|byte| *byte == b'\n') {
        tail.drain(..=newline);
    } else {
        tail.clear();
    }
    file.set_len(0).map_err(|_| log_io())?;
    file.seek(SeekFrom::Start(0)).map_err(|_| log_io())?;
    file.write_all(&tail).map_err(|_| log_io())?;
    Ok(tail.len() as u64)
}

fn log_io() -> Error {
    Error::new("daemon_log_io", "private daemon log I/O failed")
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_core::{MAX_STDERR_BYTES, StderrDiagnostic, StderrSnapshot, StderrSource};

    struct FreshRoot(PathBuf);
    impl FreshRoot {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("eliot daemon log Проба {}", ulid::Ulid::new())))
        }
    }
    impl Drop for FreshRoot {
        fn drop(&mut self) {
            if self.0.parent() == Some(std::env::temp_dir().as_path()) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
    }

    #[test]
    fn rotation_keeps_one_private_backup_and_redacts_both_sources() {
        let root = FreshRoot::new();
        let data = DataDir::open(&root.0).unwrap();
        let lock = data.acquire_daemon_lock().unwrap();
        let policy = SecretPolicy::new([data.owner_token(), "log-test-secret"]);
        let mut log = LogFile::open(&data, &lock).unwrap();
        let mut error = Error::new("unknown", "request text must never enter this log");
        for source in [StderrSource::Plugin, StderrSource::Harness] {
            error = error.with_stderr(
                source,
                StderrSnapshot {
                    text: format!(
                        "log-test-secret {} {}",
                        data.owner_token(),
                        "a".repeat(4096)
                    ),
                    truncated: false,
                    eof: true,
                    read_error: false,
                },
            );
        }
        let bytes = encode_record(
            &policy,
            "plugin.failed",
            &[("agent", "a1")],
            None,
            Some(&error),
        )
        .unwrap();
        for _ in 0..300 {
            log.write(&bytes).unwrap();
        }
        drop(log);
        for name in ["daemon.log", "daemon.log.1"] {
            let path = data.path().join("logs").join(name);
            assert!(fs::metadata(&path).unwrap().len() <= LOG_BYTES);
            let contents = fs::read_to_string(&path).unwrap();
            assert!(
                !contents.contains(data.owner_token()),
                "owner credential leaked"
            );
            assert!(
                !contents.contains("log-test-secret"),
                "known credential leaked"
            );
            assert!(!contents.contains("request text must never enter this log"));
            let first: Value = serde_json::from_str(contents.lines().next().unwrap()).unwrap();
            assert_eq!(first["diagnostics"].as_array().unwrap().len(), 2);
            drop(open_private_log_file(&path).unwrap()); // owner/reparse/DACL validation
        }
        assert_eq!(fs::read_dir(data.path().join("logs")).unwrap().count(), 2);
    }

    #[test]
    fn controls_and_short_secret_expansion_fit_after_redaction() {
        let policy = SecretPolicy::new(["!"]);
        let mut error = Error::new("unknown", "primary message remains unchanged");
        error.diagnostics = [StderrSource::Plugin, StderrSource::Harness]
            .into_iter()
            .map(|source| StderrDiagnostic {
                source,
                snapshot: StderrSnapshot {
                    text: "!\0".repeat(MAX_STDERR_BYTES / 2),
                    truncated: false,
                    eof: true,
                    read_error: false,
                },
            })
            .collect();
        let bytes = encode_record(&policy, "operation.unknown", &[], None, Some(&error)).unwrap();
        assert!(bytes.len() <= RECORD_BYTES);
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let diagnostics = &value["diagnostics"];
        assert!(serde_json::to_vec(diagnostics).unwrap().len() <= MAX_STDERR_DIAGNOSTIC_BYTES);
        assert_eq!(diagnostics.as_array().unwrap().len(), 2);
        for diagnostic in diagnostics.as_array().unwrap() {
            assert!(diagnostic["snapshot"]["truncated"].as_bool().unwrap());
            assert!(
                !diagnostic["snapshot"]["text"]
                    .as_str()
                    .unwrap()
                    .contains('!')
            );
        }
        assert_eq!(error.code, "unknown");
        assert_eq!(error.message, "primary message remains unchanged");
    }

    #[test]
    fn existing_non_file_is_not_replaced_or_cleaned_up() {
        let root = FreshRoot::new();
        let data = DataDir::open(&root.0).unwrap();
        let lock = data.acquire_daemon_lock().unwrap();
        let directory = data.daemon_log_directory(&lock).unwrap();
        let collision = directory.join("daemon.log");
        fs::create_dir(&collision).unwrap();
        fs::write(collision.join("keep"), "untouched").unwrap();
        assert!(LogFile::open(&data, &lock).is_err());
        assert_eq!(
            fs::read_to_string(collision.join("keep")).unwrap(),
            "untouched"
        );
        assert!(!directory.join("daemon.log.1").exists());
    }

    #[test]
    fn rpc_error_records_keep_the_calling_method() {
        let policy = SecretPolicy::default();
        let error = Error::new(
            "stale_generation",
            "Agent generation has no live plugin owner",
        );
        let bytes = encode_record(
            &policy,
            "rpc.error",
            &[("method", "daemon.status")],
            None,
            Some(&error),
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["ids"]["method"], json!("daemon.status"));
        assert_eq!(value["error_code"], json!("stale_generation"));
        // Unknown id keys are still dropped.
        let bytes = encode_record(
            &policy,
            "rpc.error",
            &[("request", "must-not-appear")],
            None,
            Some(&error),
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("ids").is_none());
    }

    #[test]
    fn view_health_records_carry_bounded_labels_and_typed_context() {
        let policy = SecretPolicy::new(["log-test-secret"]);
        for outcome in [
            ViewHealthOutcome::Unavailable,
            ViewHealthOutcome::ReadbackRecovered,
            ViewHealthOutcome::SettledUnknown,
        ] {
            let bytes = encode_view_health(
                &policy,
                outcome,
                "a1",
                2,
                "stream",
                &ViewHealthRecord {
                    unavailable: 3,
                    readback_recovered: 2,
                    settled_unknown: 1,
                    live_muse_hosts: Some(9),
                    turn_age_ms: Some(42_000),
                    harness_rss_bytes: Some(400 * 1024 * 1024),
                    reason_label: "projectionUnavailable",
                },
            )
            .expect("encode view-health record");
            assert!(bytes.len() < RECORD_BYTES);
            assert_eq!(bytes.last(), Some(&b'\n'));
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["code"], json!(outcome.log_code()));
            assert!(value["code"].as_str().unwrap().starts_with("muse."));
            assert_eq!(value["ids"]["agent_id"], json!("a1"));
            assert_eq!(value["ids"]["generation"], json!(2));
            assert_eq!(value["ids"]["source"], json!("stream"));
            assert_eq!(value["reason"], json!("projectionUnavailable"));
            assert_eq!(value["view_health"]["unavailable"], json!(3));
            assert_eq!(value["view_health"]["readback_recovered"], json!(2));
            assert_eq!(value["view_health"]["settled_unknown"], json!(1));
            assert_eq!(value["view_health"]["live_muse_hosts"], json!(9));
            assert_eq!(value["view_health"]["turn_age_ms"], json!(42_000));
            assert_eq!(
                value["view_health"]["harness_rss_bytes"],
                json!(400 * 1024 * 1024)
            );
        }
        // Unknown contexts stay null; secrets in ids are still redacted.
        let bytes = encode_view_health(
            &policy,
            ViewHealthOutcome::Unavailable,
            "a1log-test-secret",
            1,
            "stream",
            &ViewHealthRecord {
                unavailable: 1,
                readback_recovered: 0,
                settled_unknown: 0,
                live_muse_hosts: None,
                turn_age_ms: None,
                harness_rss_bytes: None,
                reason_label: "unknown",
            },
        )
        .expect("encode degraded view-health record");
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(!value.to_string().contains("log-test-secret"));
        assert!(value["view_health"]["live_muse_hosts"].is_null());
        assert!(value["view_health"]["turn_age_ms"].is_null());
        assert!(value["view_health"]["harness_rss_bytes"].is_null());
    }

    #[test]
    fn oversized_owned_files_are_capped_before_the_next_record() {
        let root = FreshRoot::new();
        let data = DataDir::open(&root.0).unwrap();
        let lock = data.acquire_daemon_lock().unwrap();
        let directory = data.daemon_log_directory(&lock).unwrap();
        for name in ["daemon.log", "daemon.log.1"] {
            let mut file = open_private_log_file(&directory.join(name)).unwrap();
            file.write_all(&b"old record\n".repeat(LOG_BYTES as usize / 4))
                .unwrap();
        }
        let mut log = LogFile::open(&data, &lock).unwrap();
        let record =
            encode_record(&SecretPolicy::default(), "daemon.starting", &[], None, None).unwrap();
        log.write(&record).unwrap();
        drop(log);
        for name in ["daemon.log", "daemon.log.1"] {
            assert!(fs::metadata(directory.join(name)).unwrap().len() <= LOG_BYTES);
        }
    }
}
