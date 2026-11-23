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
pub(crate) const ADMISSION_REFUSAL_CODE: &str = "command.refused";
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

pub(crate) fn record_liveness_stuck(policy: &SecretPolicy, subject: &str, reason: &str) {
    let detail = Map::from_iter([
        (
            "subject".to_owned(),
            json!(bounded_text(policy, subject, 256)),
        ),
        (
            "reason".to_owned(),
            json!(bounded_text(policy, reason, 128)),
        ),
    ]);
    write_record(
        policy,
        "daemon.liveness_stuck",
        &[],
        None,
        Some(detail),
        None,
    );
}

pub(crate) fn record_repeated_rule_failure(
    policy: &SecretPolicy,
    principal: &str,
    rule: &str,
    code: &str,
    message: &str,
    count: u32,
) {
    let detail = Map::from_iter([
        (
            "principal".to_owned(),
            json!(bounded_text(policy, principal, 256)),
        ),
        ("rule".to_owned(), json!(bounded_text(policy, rule, 256))),
        (
            "failure_code".to_owned(),
            json!(bounded_text(policy, code, 64)),
        ),
        (
            "message".to_owned(),
            json!(bounded_text(policy, message, 512)),
        ),
        ("count".to_owned(), json!(count)),
    ]);
    write_record(policy, "repeated", &[], None, Some(detail), None);
}

pub(crate) struct RepeatedGoalFailure<'a> {
    pub(crate) goal_id: &'a str,
    pub(crate) principal: &'a str,
    pub(crate) agent: &'a str,
    pub(crate) generation: i64,
    pub(crate) failure_class: &'a str,
    pub(crate) code: &'a str,
    pub(crate) message: &'a str,
}

pub(crate) fn record_repeated_goal_failure(
    policy: &SecretPolicy,
    failure: RepeatedGoalFailure<'_>,
    count: u32,
) {
    let detail = Map::from_iter([
        (
            "goal_id".to_owned(),
            json!(bounded_text(policy, failure.goal_id, 256)),
        ),
        (
            "principal".to_owned(),
            json!(bounded_text(policy, failure.principal, 256)),
        ),
        (
            "agent".to_owned(),
            json!(bounded_text(policy, failure.agent, 64)),
        ),
        ("generation".to_owned(), json!(failure.generation)),
        (
            "failure_class".to_owned(),
            json!(bounded_text(policy, failure.failure_class, 64)),
        ),
        (
            "failure_code".to_owned(),
            json!(bounded_text(policy, failure.code, 64)),
        ),
        (
            "message".to_owned(),
            json!(bounded_text(policy, failure.message, 512)),
        ),
        ("count".to_owned(), json!(count)),
    ]);
    write_record(policy, "repeated", &[], None, Some(detail), None);
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
    let detail = debug_detail(fields);
    write_record(policy, code, ids, None, Some(detail), None)
}

/// Debug-only failure record. Keeps the same bounded numeric timing detail as
/// `record_debug` while retaining the safe, typed failure explanation.
pub(crate) fn record_debug_error(
    policy: &SecretPolicy,
    code: &str,
    ids: &[(&str, &str)],
    fields: &[(&str, u64)],
    error: &Error,
) {
    if !is_debug() {
        return;
    }
    write_record(
        policy,
        code,
        ids,
        None,
        Some(debug_detail(fields)),
        Some(error),
    )
}

fn debug_detail(fields: &[(&str, u64)]) -> Map<String, Value> {
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
    detail
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

pub(crate) struct CommandStormMetadata {
    pub(crate) connection_count: u64,
    pub(crate) connections_overflow: bool,
}

pub(crate) fn record_command_storm(
    policy: &SecretPolicy,
    ids: &[(&str, &str)],
    attempts: u64,
    refused: u64,
    window_ms: u64,
    metadata: CommandStormMetadata,
) {
    let detail = Map::from_iter([
        ("attempts".to_owned(), Value::from(attempts)),
        (
            "accepted".to_owned(),
            Value::from(attempts.saturating_sub(refused)),
        ),
        ("refused".to_owned(), Value::from(refused)),
        ("window_ms".to_owned(), Value::from(window_ms)),
        (
            "connection_count".to_owned(),
            Value::from(metadata.connection_count),
        ),
        (
            "connections_overflow".to_owned(),
            Value::from(metadata.connections_overflow),
        ),
    ]);
    write_record(
        policy,
        "daemon.command_storm",
        ids,
        None,
        Some(detail),
        None,
    );
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
                | "principal"
                | "producer"
                | "operation"
                | "path"
                | "project"
                | "rule"
                | "boot_id"
                | "pid"
                | "method"
                | "error_code"
                | "version"
                | "executable_directory"
                | "git_sha"
                | "executable_sha256"
                | "skipped"
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
    let fallback_error_code = context
        .remove("error_code")
        .and_then(|value| value.as_str().map(str::to_owned));
    let failure_record = error.is_some() || is_failure_record(code);
    if failure_record {
        context
            .entry("principal".to_owned())
            .or_insert_with(|| Value::String("system".to_owned()));
        if context.get("principal").and_then(Value::as_str) == Some("system") {
            context
                .entry("producer".to_owned())
                .or_insert_with(|| Value::String(bounded_text(policy, code, 128)));
        }
        if !context.contains_key("method") && !context.contains_key("command") {
            context.insert(
                "method".to_owned(),
                Value::String(bounded_text(policy, code, 128)),
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
        value["cause"] = json!(bounded_text(policy, &error.message, 1024));
        value["error_code"] = json!(bounded_text(policy, &error.code, 64));
        let where_ = error.failure_where.as_deref().unwrap_or(code);
        value["where"] = json!(bounded_text(policy, where_, 128));
        if let Some(details) = error.details.as_deref() {
            value["invalid_param"] = json!({
                "field": bounded_text(policy, &details.field, 128),
                "expected": bounded_text(policy, &details.expected.to_string(), 256),
            });
        }
        value["origin"] = json!(error.origin_or_unknown().as_str());
        if let Some(evidence) = error.origin_evidence.as_deref() {
            value["origin_evidence"] = json!(bounded_text(policy, evidence, 512));
        }
        if let Some(exit_code) = error.exit_code.as_deref() {
            value["exit_code"] = json!(exit_code);
        }
        if let Some(ntstatus) = error.ntstatus.as_deref() {
            value["ntstatus"] = json!(bounded_text(policy, ntstatus, 16));
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
    } else if failure_record {
        value["where"] = json!(bounded_text(policy, code, 128));
        value["cause"] = json!("no safe failure detail was available at the logging callsite");
        value["error_code"] = json!(
            fallback_error_code
                .as_deref()
                .map(|code| bounded_text(policy, code, 64))
                .unwrap_or_else(|| "unknown".to_owned())
        );
    }
    let mut bytes = serde_json::to_vec(&value).ok()?;
    if bytes.len() >= RECORD_BYTES {
        return None;
    }
    bytes.push(b'\n');
    Some(bytes)
}

fn is_failure_record(code: &str) -> bool {
    let event = code.rsplit('.').next().unwrap_or(code);
    event == "error"
        || event == "failed"
        || event == "refused"
        || event.ends_with("_error")
        || event.ends_with("_failed")
        || event.ends_with("_rejected")
        || event.ends_with("_unavailable")
        || event.ends_with("_exhausted")
        || event.ends_with("_unexpectedly")
}

/// Exact-ID admission preflight evidence (capacity, a missing profile
/// executable) when no command was admitted. These optional rotated records
/// are diagnostics only, never command/replay state.
pub(crate) fn admission_refusal(
    data_root: &std::path::Path,
    id: &str,
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
            if value["code"] != ADMISSION_REFUSAL_CODE || value["ids"]["command"] != id {
                continue;
            }
            let Some(refusal) = value["error_code"].as_str().and_then(|code| {
                eliot_core::AdmissionRefusal::of_parts(
                    code,
                    value["where"].as_str(),
                    value["origin"]
                        .as_str()
                        .and_then(eliot_core::FailureOrigin::parse),
                )
            }) else {
                continue;
            };
            let (code, where_, origin, evidence) = refusal.parts();
            return Some(eliot_core::api::DiagnoseStep {
                kind: "admission".to_owned(),
                code: Some(code.to_owned()),
                message: Some("refused before command admission".to_owned()),
                failure_where: Some(where_.to_owned()),
                origin: Some(origin.as_str().to_owned()),
                origin_evidence: Some(evidence.to_owned()),
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
