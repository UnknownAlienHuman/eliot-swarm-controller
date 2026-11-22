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
    pub(crate) processes: u64,
    pub(crate) pages_reclaimed: u64,
    pub(crate) freelist_pages: u64,
    pub(crate) auto_vacuum_mode: u64,
    pub(crate) wal_frames: u64,
    pub(crate) checkpointed_frames: u64,
    pub(crate) checkpoint_busy: u64,
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
                | "worker"
                | "restart_count"
                | "attempt"
                | "retry_after_ms"
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
            "processes": counts.processes,
            "pages_reclaimed": counts.pages_reclaimed,
            "freelist_pages": counts.freelist_pages,
            "auto_vacuum_mode": counts.auto_vacuum_mode,
            "wal_frames": counts.wal_frames,
            "checkpointed_frames": counts.checkpointed_frames,
            "checkpoint_busy": counts.checkpoint_busy,
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
