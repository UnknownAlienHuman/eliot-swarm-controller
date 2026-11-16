//! Authoritative daemon diagnostics, owned by run(), never by its CLI starter.
//! Records are optional diagnostics, not semantic outcomes or replay authority.
//!
//! Two modes, both bounded with rotation. Standard mode records lifecycle,
//! failures (with origin, `where`, code and classified evidence), crash
//! markers, capacity refusals and daemon retention. Debug mode additionally
//! records every RPC with timing, every plugin frame as metadata only
//! (method, direction, size, latency, turn and command IDs; never content),
//! every process start and exit with numeric metadata, and Store transaction
//! timings. Secrets, prompts and model output never enter either mode.
//! `swarm why` works in standard mode; debug only adds detail.

use crate::platform::{
    DataDir,
    data_dir::{DaemonLock, open_private_log_file, rotate_private_log_file},
};
use eliot_core::{
    Error, MAX_STDERR_DIAGNOSTIC_BYTES, Result, SecretPolicy, bound_stderr_diagnostics,
};
use eliot_plugin::sync::LockExt;
use serde_json::{Map, Value, json};
use std::{
    fs::{self, File},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{
        Mutex,
        atomic::{AtomicU8, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

const LOG_BYTES: u64 = 1024 * 1024;
const RECORD_BYTES: usize = 32 * 1024;
pub(crate) const CAPACITY_REFUSAL_CODE: &str = "command.refused";
static LOG: Mutex<Option<LogFile>> = Mutex::new(None);
static LEVEL: AtomicU8 = AtomicU8::new(LogLevel::Standard as u8);

/// Daemon log verbosity. Standard is the working mode; debug is the
/// everything mode for chasing a live failure. Hot-reloadable through
/// `settings.save` (`[daemon] log_level`); never a restart gate on its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LogLevel {
    Standard = 0,
    Debug = 1,
}

impl LogLevel {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        match value {
            "standard" => Ok(Self::Standard),
            "debug" => Ok(Self::Debug),
            _ => Err(Error::new(
                "profile_error",
                "config.toml: daemon.log_level must be standard or debug",
            )),
        }
    }
}

/// Current verbosity for this process. The daemon sets it at startup from
/// `[daemon] log_level` and refreshes it on every `settings.save`.
pub(crate) fn set_log_level(level: LogLevel) {
    LEVEL.store(level as u8, Ordering::Relaxed);
}

pub(crate) fn is_debug() -> bool {
    LEVEL.load(Ordering::Relaxed) == LogLevel::Debug as u8
}

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
/// operation's code, result, acknowledgement or retry behavior. Request
/// parameters, prompts, model output, environment values and raw native
/// frames are deliberately never written here. A record failure is
/// diagnostic loss only: it cannot change command settlement, source
/// acknowledgement or process cleanup.
pub(crate) fn record(
    policy: &SecretPolicy,
    code: &str,
    ids: &[(&str, &str)],
    error: Option<&Error>,
) {
    write_record(policy, code, ids, None, None, error)
}

/// Debug-only record: typed nonnegative numeric fields next to bounded
/// string ids. Numbers are encoded as JSON numbers; callers pass no
/// free-form text here, so secrets cannot enter through this path. Dropped
/// unless the daemon runs at debug log level.
pub(crate) fn record_debug(
    policy: &SecretPolicy,
    code: &str,
    ids: &[(&str, &str)],
    fields: &[(&str, u64)],
) {
    if !is_debug() {
        return;
    }
    let mut detail = Map::new();
    for (key, value) in fields.iter().take(8) {
        if key.len() <= 64
            && key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        {
            detail.insert((*key).to_owned(), Value::from(*value));
        }
    }
    write_record(policy, code, ids, None, Some(detail), None)
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
    pub(crate) digests: u64,
}

pub(crate) fn record_counts(
    policy: &SecretPolicy,
    code: &str,
    counts: Counts,
    error: Option<&Error>,
) {
    write_record(policy, code, &[], Some(counts), None, error)
}

fn write_record(
    policy: &SecretPolicy,
    code: &str,
    ids: &[(&str, &str)],
    counts: Option<Counts>,
    detail: Option<Map<String, Value>>,
    error: Option<&Error>,
) {
    let Some(bytes) = encode_record(policy, code, ids, counts, detail, error) else {
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
    detail: Option<Map<String, Value>>,
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
            "digests": counts.digests,
        });
    }
    if let Some(detail) = detail
        && !detail.is_empty()
    {
        value["detail"] = Value::Object(detail);
    }
    if let Some(error) = error {
        value["error_code"] = json!(bounded_text(policy, &error.code, 64));
        if let Some(where_) = error.failure_where.as_deref() {
            value["where"] = json!(bounded_text(policy, where_, 128));
        }
        value["origin"] = json!(error.origin_or_unknown().as_str());
        if let Some(evidence) = error.origin_evidence.as_deref() {
            value["origin_evidence"] = json!(bounded_text(policy, evidence, 512));
        }
        let mut diagnostics = (*error.diagnostics).clone();
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

/// Exact-ID capacity preflight evidence when no command was admitted. These
/// optional rotated records are diagnostics only, never command/replay state.
pub(crate) fn capacity_refusal(
    data_root: &std::path::Path,
    id: &str,
    policy: &SecretPolicy,
) -> Option<eliot_core::api::DiagnoseStep> {
    if ulid::Ulid::from_string(id).is_err() {
        return None;
    }
    for name in ["daemon.log", "daemon.log.1"] {
        let path = data_root.join("logs").join(name);
        if crate::platform::data_dir::ensure_private_file(&path).is_err() {
            continue;
        }
        let Ok(file) = File::open(path) else {
            continue;
        };
        let mut bytes = Vec::new();
        if file.take(LOG_BYTES).read_to_end(&mut bytes).is_err() {
            continue;
        }
        for line in bytes
            .rsplit(|byte| *byte == b'\n')
            .filter(|line| line.len() <= RECORD_BYTES)
        {
            let Ok(value) = serde_json::from_slice::<Value>(line) else {
                continue;
            };
            if value["code"] != CAPACITY_REFUSAL_CODE
                || value["ids"]["command"] != id
                || value["error_code"] != "capacity_unavailable"
                || value["origin"] != "swarm"
            {
                continue;
            }
            return Some(eliot_core::api::DiagnoseStep {
                kind: "admission".to_owned(),
                code: Some("capacity_unavailable".to_owned()),
                message: Some("capacity refused before command admission".to_owned()),
                failure_where: value["where"]
                    .as_str()
                    .map(|text| bounded_text(policy, text, 128)),
                origin: Some("swarm".to_owned()),
                origin_evidence: value["origin_evidence"]
                    .as_str()
                    .map(|text| bounded_text(policy, text, 512)),
                exit_code: None,
                ntstatus: None,
                at: value["ts"].as_i64().filter(|at| *at >= 0),
            });
        }
    }
    None
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

    #[test]
    fn capacity_refusal_is_exact_id_and_excludes_free_text() {
        let root = FreshRoot::new();
        let data = DataDir::open(&root.0).unwrap();
        let lock = data.acquire_daemon_lock().unwrap();
        let mut log = LogFile::open(&data, &lock).unwrap();
        let id = ulid::Ulid::new().to_string();
        let error = Error::new(
            "capacity_unavailable",
            "unclassified input must not be logged",
        )
        .with_where("store.admission.capacity")
        .with_origin(
            eliot_core::FailureOrigin::Swarm,
            "configured local capacity bound is reserved",
        );
        let policy = SecretPolicy::default();
        log.write(
            &encode_record(
                &policy,
                "command.refused",
                &[("command", &id)],
                None,
                None,
                Some(&error),
            )
            .unwrap(),
        )
        .unwrap();
        let step = capacity_refusal(data.path(), &id, &policy).unwrap();
        assert_eq!(step.origin.as_deref(), Some("swarm"));
        assert_eq!(
            step.failure_where.as_deref(),
            Some("store.admission.capacity")
        );
        assert!(
            !serde_json::to_string(&step)
                .unwrap()
                .contains("unclassified input")
        );
        assert!(capacity_refusal(data.path(), &ulid::Ulid::new().to_string(), &policy).is_none());
    }

    #[test]
    fn standard_record_excludes_free_text_message() {
        let error = Error::new("failed", "arbitrary user content")
            .with_where("plugin.start")
            .with_origin(eliot_core::FailureOrigin::Harness, "native refusal");
        let bytes = encode_record(
            &SecretPolicy::default(),
            "command.failed",
            &[],
            None,
            None,
            Some(&error),
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("error_message").is_none());
        assert!(
            !String::from_utf8(bytes)
                .unwrap()
                .contains("arbitrary user content")
        );
        assert_eq!(value["origin"], "harness");
    }

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
        let mut error = Error::new(
            "unknown",
            "request carrying log-test-secret must never enter this log",
        );
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
            assert!(!contents.contains("request carrying"));
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
            .collect::<Vec<_>>()
            .into();
        let bytes =
            encode_record(&policy, "operation.unknown", &[], None, None, Some(&error)).unwrap();
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
        assert_eq!(error.message.as_str(), "primary message remains unchanged");
    }

    #[cfg(unix)]
    #[test]
    fn unix_private_log_keeps_ambient_umask_for_outside_files() {
        use std::os::unix::fs::PermissionsExt;

        // The daemon sets no process-wide umask: private files carry explicit
        // 0600 modes while outside files keep the ambient mask.
        let _mask = crate::platform::unix::hold_umask(0o022);
        let root = FreshRoot::new();
        let data = DataDir::open(&root.0).unwrap();
        let lock = data.acquire_daemon_lock().unwrap();
        let log = LogFile::open(&data, &lock).unwrap();
        drop(log);
        let mode = fs::metadata(data.path().join("logs").join("daemon.log"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600, "daemon log is current-user-only");
        let outside = std::env::temp_dir().join(format!("eliot-ambient-{}", ulid::Ulid::new()));
        fs::write(&outside, b"ambient").unwrap();
        let outside_mode = fs::metadata(&outside).unwrap().permissions().mode() & 0o777;
        let _ = fs::remove_file(&outside);
        assert_eq!(
            outside_mode, 0o644,
            "files outside the data dir keep the ambient umask"
        );
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
            None,
            Some(&error),
        )
        .unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("ids").is_none());
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
        let record = encode_record(
            &SecretPolicy::default(),
            "daemon.starting",
            &[],
            None,
            None,
            None,
        )
        .unwrap();
        log.write(&record).unwrap();
        drop(log);
        for name in ["daemon.log", "daemon.log.1"] {
            assert!(fs::metadata(directory.join(name)).unwrap().len() <= LOG_BYTES);
        }
    }
}
