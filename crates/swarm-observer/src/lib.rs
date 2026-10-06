//! Optional local observer with metadata-only defaults and redacted text opt-in.
//!
//! The observer has no global subscriber, host service, model, provider, or
//! business ledger. The host recorder is opt-in and lazy; local file following
//! polls only while its explicit CLI command is active. Authenticated Store
//! readback is explicit and one-shot, so disconnects never replay or
//! acknowledge work.

pub mod follow;
pub mod host;
pub mod host_image_receipt;
pub mod live_config;
pub mod metrics_cli;
pub mod process_metrics;
pub use live_config::LiveConfigSource;

use crate::live_config::{Kind as DiagnosticKind, LiveSettings, Severity as DiagnosticSeverity};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use swarm_client::{Client, IpcConfig};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};
use swarm_process::private_permissions;

pub const MAX_DIAGNOSTIC_BYTES: usize = swarm_telemetry::MAX_RECORD_BYTES;
// The host's existing report handlers accept pages of at most 200 records.
pub const MAX_READBACK_LIMIT: u64 = 200;
const MAX_SEGMENT_BYTES: u64 = 1_073_741_824;
const MAX_RETENTION_BYTES: u64 = 8_589_934_592;
const MAX_RETENTION_DAYS: u64 = 3650;

/// Exact wire shapes emitted by `swarm-telemetry` schemas 1 through 4.
/// Schema 4 adds bounded redacted text. Unknown fields are rejected so the
/// observer cannot silently claim newer coverage; schemas 1 and 2 remain
/// readable without schema-3 correlation identities.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticRecord {
    pub schema_version: u8,
    pub sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub occurred_at_unix_ms: Option<u64>,
    pub severity: String,
    pub kind: String,
    pub phase: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub link_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binding_id: Option<String>,
    /// Exact retained Store generation for binding-scoped module records.
    /// Missing remains valid for schema-1 records and non-binding schema-2 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_boot_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<String>,
    /// Bounded, Atlas-redacted producer text. This field is accepted only in
    /// schema 4 and is absent from metadata-only schema 1 through 3 records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redacted_text: Option<String>,
}

pub fn decode_line(line: &[u8]) -> Result<DiagnosticRecord> {
    if line.len() > MAX_DIAGNOSTIC_BYTES {
        return Err(Error::new(
            "OBSERVER_RECORD_TOO_LARGE",
            "diagnostic line exceeds the bounded envelope",
        ));
    }
    let record: DiagnosticRecord = serde_json::from_slice(line).map_err(|_| {
        Error::new(
            "OBSERVER_RECORD_INVALID",
            "diagnostic line is not the supported metadata schema",
        )
    })?;
    if !matches!(record.schema_version, 1 | 2 | 3 | 4) {
        return Err(Error::new(
            "OBSERVER_SCHEMA_UNSUPPORTED",
            "diagnostic schema version is unsupported",
        ));
    }
    let has_schema3_identity = record.event_id.is_some()
        || record.component.is_some()
        || record.module_id.is_some()
        || record.artifact_id.is_some()
        || record.artifact_version.is_some()
        || record.build_id.is_some()
        || record.task_id.is_some()
        || record.attempt_id.is_some();
    if record.schema_version == 1 && (record.binding_generation.is_some() || has_schema3_identity) {
        return Err(Error::new(
            "OBSERVER_SCHEMA_UNSUPPORTED",
            "diagnostic schema-1 record contains a newer-schema field",
        ));
    }
    if record.schema_version == 2 && has_schema3_identity {
        return Err(Error::new(
            "OBSERVER_SCHEMA_UNSUPPORTED",
            "diagnostic schema-2 record contains a schema-3 field",
        ));
    }
    if record.schema_version <= 3 && record.redacted_text.is_some() {
        return Err(Error::new(
            "OBSERVER_SCHEMA_UNSUPPORTED",
            "metadata diagnostic schema contains a newer content field",
        ));
    }
    if record.schema_version == 4
        && record
            .redacted_text
            .as_ref()
            .is_none_or(|text| text.len() > swarm_telemetry::MAX_REDACTED_TEXT_BYTES)
    {
        return Err(Error::new(
            "OBSERVER_RECORD_INVALID",
            "schema-4 diagnostic text is missing or exceeds its bound",
        ));
    }
    validate_record(&record)?;
    Ok(record)
}

fn validate_record(record: &DiagnosticRecord) -> Result<()> {
    if record.binding_generation.is_some_and(|generation| {
        generation == 0 || generation > i64::MAX as u64 || record.binding_id.is_none()
    }) {
        return Err(Error::new(
            "OBSERVER_RECORD_INVALID",
            "diagnostic binding generation is outside the Store range",
        ));
    }
    if !matches!(
        record.severity.as_str(),
        "error" | "warn" | "info" | "debug" | "trace"
    ) || !matches!(
        record.kind.as_str(),
        "client_disconnected"
            | "store_operation_failed"
            | "module_started"
            | "module_stopped"
            | "agent_delivery_failed"
            | "recorder_failure"
    ) || !matches!(
        record.phase.as_str(),
        "admission"
            | "commit"
            | "readback"
            | "store_disconnect"
            | "module_start"
            | "module_exit"
            | "agent_delivery"
            | "recorder_write"
    ) {
        return Err(Error::new(
            "OBSERVER_RECORD_INVALID",
            "diagnostic vocabulary is unsupported",
        ));
    }
    if let Some(code) = &record.code
        && !matches!(
            code.as_str(),
            "disconnect_persistence_failed"
                | "store_operation_failed"
                | "module_start_failed"
                | "native_exit_observed"
                | "agent_delivery_failed"
                | "recorder_write_failed"
        )
    {
        return Err(Error::new(
            "OBSERVER_RECORD_INVALID",
            "diagnostic code is unsupported",
        ));
    }
    for value in [
        record.client_id.as_deref(),
        record.link_id.as_deref(),
        record.binding_id.as_deref(),
        record.operation_id.as_deref(),
        record.module_boot_id.as_deref(),
    ]
    .into_iter()
    .flatten()
    {
        if value.is_empty()
            || value.len() > 128
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return Err(Error::new(
                "OBSERVER_RECORD_INVALID",
                "diagnostic identity is outside the bounded metadata vocabulary",
            ));
        }
    }
    for (value, max_bytes, punctuation) in [
        (record.event_id.as_deref(), 256, &b"-_.:"[..]),
        (record.module_id.as_deref(), 128, &b"._:-"[..]),
        (record.artifact_id.as_deref(), 128, &b"._:-"[..]),
        (record.artifact_version.as_deref(), 128, &b".+_-"[..]),
        (record.build_id.as_deref(), 128, &b"._:-+"[..]),
        (record.task_id.as_deref(), 128, &b"-_.:"[..]),
        (record.attempt_id.as_deref(), 128, &b"-_.:"[..]),
    ] {
        if let Some(value) = value
            && !valid_metadata_identity(value, max_bytes, punctuation)
        {
            return Err(Error::new(
                "OBSERVER_RECORD_INVALID",
                "diagnostic correlation identity is outside the bounded metadata vocabulary",
            ));
        }
    }
    if record.schema_version == 2
        && matches!(record.kind.as_str(), "module_started" | "module_stopped")
        && (record.binding_id.is_none()
            || record.binding_generation.is_none()
            || record.module_boot_id.is_none())
    {
        return Err(Error::new(
            "OBSERVER_RECORD_INVALID",
            "schema-2 module diagnostics require exact binding, generation, and boot identities",
        ));
    }
    if record.schema_version == 3 {
        let is_module = matches!(record.kind.as_str(), "module_started" | "module_stopped");
        if !is_module
            || record.component.as_deref() != Some("module_supervisor")
            || record.event_id.is_none()
            || record.module_id.is_none()
            || record.artifact_id.is_none()
            || record.artifact_version.is_none()
            || record.binding_id.is_none()
            || record.binding_generation.is_none()
            || record.module_boot_id.is_none()
            || (record.attempt_id.is_some() && record.task_id.is_none())
            || ((record.task_id.is_some() || record.attempt_id.is_some())
                && record.operation_id.is_none())
        {
            return Err(Error::new(
                "OBSERVER_RECORD_INVALID",
                "schema-3 module diagnostics require exact event, module, artifact, binding, and boot identities",
            ));
        }
    }
    if record.schema_version == 4 && has_schema3_identity {
        let is_module = matches!(record.kind.as_str(), "module_started" | "module_stopped");
        if !is_module
            || record.component.as_deref() != Some("module_supervisor")
            || record.event_id.is_none()
            || record.module_id.is_none()
            || record.artifact_id.is_none()
            || record.artifact_version.is_none()
            || record.binding_id.is_none()
            || record.binding_generation.is_none()
            || record.module_boot_id.is_none()
            || (record.attempt_id.is_some() && record.task_id.is_none())
            || ((record.task_id.is_some() || record.attempt_id.is_some())
                && record.operation_id.is_none())
        {
            return Err(Error::new(
                "OBSERVER_RECORD_INVALID",
                "schema-4 correlated diagnostics require exact module identities",
            ));
        }
    }
    Ok(())
}

fn valid_metadata_identity(value: &str, max_bytes: usize, punctuation: &[u8]) -> bool {
    !value.is_empty()
        && value.len() <= max_bytes
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || punctuation.contains(&byte))
}

#[derive(Clone, Debug)]
pub struct RecorderConfig {
    pub directory: PathBuf,
    pub queue_records: usize,
    pub queue_bytes: usize,
    pub max_record_bytes: usize,
    pub segment_bytes: u64,
    pub retention_bytes: u64,
    pub retention_days: u64,
}

impl RecorderConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.directory.is_absolute()
            || self.queue_records == 0
            || self.queue_records > swarm_telemetry::MAX_QUEUE_RECORDS
            || self.queue_bytes == 0
            || self.queue_bytes > swarm_telemetry::MAX_QUEUE_BYTES
            || self.max_record_bytes == 0
            || self.max_record_bytes > MAX_DIAGNOSTIC_BYTES
            || self.queue_bytes < self.max_record_bytes
            || self.segment_bytes < self.max_record_bytes as u64
            || self.segment_bytes > MAX_SEGMENT_BYTES
            || self.retention_bytes < self.segment_bytes
            || self.retention_bytes > MAX_RETENTION_BYTES
            || self.retention_days == 0
            || self.retention_days > MAX_RETENTION_DAYS
        {
            return Err(Error::new(
                "OBSERVER_CONFIG_INVALID",
                "observer bounds or directory are invalid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct RecorderStats {
    pub accepted_records: u64,
    pub written_records: u64,
    pub written_bytes: u64,
    pub dropped_records: u64,
    pub dropped_bytes: u64,
    pub durability_unknown_records: u64,
    pub durability_unknown_bytes: u64,
    pub sink_failures: u64,
    pub pending_records: u64,
    pub pending_bytes: u64,
    /// Records deliberately omitted by the active level/category filter.
    pub filtered_records: u64,
    pub filtered_bytes: u64,
    /// Highest fully validated live config version applied by this recorder.
    pub live_config_version: u64,
    /// Failed bounded reads/validations of the optional live config file.
    pub live_config_reload_failures: u64,
    /// Closed local error code from the latest failed live config reload.
    pub live_config_last_error_code: Option<&'static str>,
}

struct Counters {
    accepted_records: AtomicU64,
    written_records: AtomicU64,
    written_bytes: AtomicU64,
    dropped_records: AtomicU64,
    dropped_bytes: AtomicU64,
    durability_unknown_records: AtomicU64,
    durability_unknown_bytes: AtomicU64,
    sink_failures: AtomicU64,
    pending_records: AtomicU64,
    pending_bytes: AtomicU64,
    filtered_records: AtomicU64,
    filtered_bytes: AtomicU64,
    live_config_version: AtomicU64,
    live_config_reload_failures: AtomicU64,
    live_config_last_error: AtomicU64,
    failed: AtomicBool,
}

impl Default for Counters {
    fn default() -> Self {
        Self {
            accepted_records: AtomicU64::new(0),
            written_records: AtomicU64::new(0),
            written_bytes: AtomicU64::new(0),
            dropped_records: AtomicU64::new(0),
            dropped_bytes: AtomicU64::new(0),
            durability_unknown_records: AtomicU64::new(0),
            durability_unknown_bytes: AtomicU64::new(0),
            sink_failures: AtomicU64::new(0),
            pending_records: AtomicU64::new(0),
            pending_bytes: AtomicU64::new(0),
            filtered_records: AtomicU64::new(0),
            filtered_bytes: AtomicU64::new(0),
            live_config_version: AtomicU64::new(0),
            live_config_reload_failures: AtomicU64::new(0),
            live_config_last_error: AtomicU64::new(0),
            failed: AtomicBool::new(false),
        }
    }
}

struct QueuedRecord {
    bytes: Vec<u8>,
    // Sidecar identities keep the existing DiagnosticRecord wire schemas
    // unchanged while allowing the writer to select a scoped policy.
    severity: DiagnosticSeverity,
    kind: DiagnosticKind,
    module_id: Option<String>,
    client_id: Option<String>,
    operation_id: Option<String>,
    manager_policy: Option<swarm_telemetry::ScopedPolicyOverride>,
    contains_redacted_text: bool,
}

enum Command {
    Append(QueuedRecord),
    Shutdown,
}

/// Explicitly started bounded recorder. `start` owns the only file-I/O worker;
/// `disabled` owns no worker and is safe for ordinary agent startup.
pub struct Recorder {
    sender: Option<SyncSender<Command>>,
    counters: Arc<Counters>,
    config: Option<RecorderConfig>,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Recorder {
    pub fn disabled() -> Self {
        Self {
            sender: None,
            counters: Arc::new(Counters::default()),
            config: None,
            join: Mutex::new(None),
        }
    }

    pub fn start(config: RecorderConfig) -> Result<Self> {
        Self::start_with_live_config(config, None)
    }

    pub(crate) fn start_with_live_config(
        config: RecorderConfig,
        live_config: Option<LiveConfigSource>,
    ) -> Result<Self> {
        config.validate()?;
        if let Some(source) = &live_config {
            source.validate()?;
        }
        fs::create_dir_all(&config.directory).map_err(|_| {
            Error::new(
                "OBSERVER_PATH_UNAVAILABLE",
                "observer directory is unavailable",
            )
        })?;
        ensure_private_directory(&config.directory)?;
        let counters = Arc::new(Counters::default());
        let (sender, receiver) = mpsc::sync_channel(config.queue_records);
        let worker_config = config.clone();
        let worker_counters = Arc::clone(&counters);
        let join = thread::Builder::new()
            .name("swarm-observer-recorder".into())
            .spawn(move || recorder_loop(receiver, worker_config, worker_counters, live_config))
            .map_err(|_| {
                Error::new(
                    "OBSERVER_START_FAILED",
                    "observer recorder worker could not start",
                )
            })?;
        Ok(Self {
            sender: Some(sender),
            counters,
            config: Some(config),
            join: Mutex::new(Some(join)),
        })
    }

    pub fn append(&self, record: DiagnosticRecord) -> Result<()> {
        self.append_with_manager_policy(record, None)
    }

    fn append_with_manager_policy(
        &self,
        record: DiagnosticRecord,
        manager_policy: Option<swarm_telemetry::ScopedPolicyOverride>,
    ) -> Result<()> {
        if let Err(error) = validate_record(&record) {
            self.drop_record(0);
            return Err(error);
        }
        let Some(severity) = DiagnosticSeverity::from_record(&record.severity) else {
            self.drop_record(0);
            return Err(Error::new(
                "OBSERVER_RECORD_INVALID",
                "diagnostic severity is outside the supported vocabulary",
            ));
        };
        let Some(kind) = DiagnosticKind::from_record(&record.kind) else {
            self.drop_record(0);
            return Err(Error::new(
                "OBSERVER_RECORD_INVALID",
                "diagnostic category is outside the supported vocabulary",
            ));
        };
        let Some(sender) = &self.sender else {
            return Err(Error::new(
                "OBSERVER_DISABLED",
                "observer recorder is disabled",
            ));
        };
        let mut bytes = serde_json::to_vec(&record)?;
        bytes.push(b'\n');
        let size = bytes.len();
        let Some(config) = &self.config else {
            return Err(Error::new(
                "OBSERVER_DISABLED",
                "observer recorder is disabled",
            ));
        };
        if size > config.max_record_bytes {
            self.drop_record(size as u64);
            return Err(Error::new(
                "OBSERVER_RECORD_TOO_LARGE",
                "diagnostic record exceeds configured bound",
            ));
        }
        if self.counters.failed.load(Ordering::Acquire) {
            self.drop_record(size as u64);
            return Err(Error::new(
                "OBSERVER_UNAVAILABLE",
                "observer recorder sink failed",
            ));
        }
        if !reserve(
            &self.counters.pending_records,
            config.queue_records as u64,
            1,
        ) {
            self.drop_record(size as u64);
            return Err(Error::new(
                "OBSERVER_QUEUE_FULL",
                "observer queue is full; loss is visible in counters",
            ));
        }
        if !reserve(
            &self.counters.pending_bytes,
            config.queue_bytes as u64,
            size as u64,
        ) {
            self.counters.pending_records.fetch_sub(1, Ordering::AcqRel);
            self.drop_record(size as u64);
            return Err(Error::new(
                "OBSERVER_QUEUE_FULL",
                "observer byte budget is full; loss is visible in counters",
            ));
        }
        match sender.try_send(Command::Append(QueuedRecord {
            bytes,
            severity,
            kind,
            module_id: record.module_id.clone(),
            client_id: record.client_id.clone(),
            operation_id: record.operation_id.clone(),
            manager_policy,
            contains_redacted_text: record.redacted_text.is_some(),
        })) {
            Ok(()) => {
                self.counters
                    .accepted_records
                    .fetch_add(1, Ordering::AcqRel);
                Ok(())
            }
            Err(TrySendError::Full(Command::Append(record))) => {
                release_pending(&self.counters, record.bytes.len() as u64);
                self.drop_record(record.bytes.len() as u64);
                Err(Error::new(
                    "OBSERVER_QUEUE_FULL",
                    "observer recorder rejected the bounded record",
                ))
            }
            Err(TrySendError::Disconnected(Command::Append(record))) => {
                release_pending(&self.counters, record.bytes.len() as u64);
                self.drop_record(record.bytes.len() as u64);
                mark_sink_failed(&self.counters);
                Err(Error::new(
                    "OBSERVER_UNAVAILABLE",
                    "observer recorder worker disconnected",
                ))
            }
            Err(TrySendError::Full(Command::Shutdown))
            | Err(TrySendError::Disconnected(Command::Shutdown)) => {
                release_pending(&self.counters, size as u64);
                self.drop_record(size as u64);
                mark_sink_failed(&self.counters);
                Err(Error::new(
                    "OBSERVER_UNAVAILABLE",
                    "observer recorder command channel failed",
                ))
            }
        }
    }

    /// Account for one overlong input line that the bounded reader discarded
    /// without retaining its payload in memory.
    pub fn record_dropped_input(&self, bytes: u64) {
        self.drop_record(bytes);
    }

    pub fn append_line(&self, line: &[u8]) -> Result<()> {
        self.append_line_with_manager_policy(line, None)
    }

    pub(crate) fn append_line_with_manager_policy(
        &self,
        line: &[u8],
        manager_policy: Option<swarm_telemetry::ScopedPolicyOverride>,
    ) -> Result<()> {
        let record = match decode_line(line) {
            Ok(record) => record,
            Err(error) => {
                self.drop_record(line.len() as u64);
                return Err(error);
            }
        };
        self.append_with_manager_policy(record, manager_policy)
    }

    pub fn stats(&self) -> RecorderStats {
        RecorderStats {
            accepted_records: self.counters.accepted_records.load(Ordering::Acquire),
            written_records: self.counters.written_records.load(Ordering::Acquire),
            written_bytes: self.counters.written_bytes.load(Ordering::Acquire),
            dropped_records: self.counters.dropped_records.load(Ordering::Acquire),
            dropped_bytes: self.counters.dropped_bytes.load(Ordering::Acquire),
            durability_unknown_records: self
                .counters
                .durability_unknown_records
                .load(Ordering::Acquire),
            durability_unknown_bytes: self
                .counters
                .durability_unknown_bytes
                .load(Ordering::Acquire),
            sink_failures: self.counters.sink_failures.load(Ordering::Acquire),
            pending_records: self.counters.pending_records.load(Ordering::Acquire),
            pending_bytes: self.counters.pending_bytes.load(Ordering::Acquire),
            filtered_records: self.counters.filtered_records.load(Ordering::Acquire),
            filtered_bytes: self.counters.filtered_bytes.load(Ordering::Acquire),
            live_config_version: self.counters.live_config_version.load(Ordering::Acquire),
            live_config_reload_failures: self
                .counters
                .live_config_reload_failures
                .load(Ordering::Acquire),
            live_config_last_error_code: live_config_error_code(
                self.counters.live_config_last_error.load(Ordering::Acquire),
            ),
        }
    }

    pub fn shutdown(self) -> Result<RecorderStats> {
        let (stats, result) = self.shutdown_with_status();
        result.map(|()| stats)
    }

    /// Return loss counters even when the writer or final sync failed. The
    /// ordinary `shutdown` API remains convenient for callers that only need
    /// success/failure.
    pub fn shutdown_with_status(self) -> (RecorderStats, Result<()>) {
        self.shutdown_with_timeout_option(None)
    }

    /// Drain and join the recorder within `timeout`. A timed-out worker is
    /// detached without being killed; pending counters remain visible and the
    /// returned error explicitly says that clean shutdown was not established.
    pub fn shutdown_with_timeout(self, timeout: Duration) -> (RecorderStats, Result<()>) {
        self.shutdown_with_timeout_option(Some(timeout))
    }

    fn shutdown_with_timeout_option(
        self,
        timeout: Option<Duration>,
    ) -> (RecorderStats, Result<()>) {
        let deadline = timeout.map(|timeout| {
            Instant::now()
                .checked_add(timeout)
                .unwrap_or_else(Instant::now)
        });
        self.shutdown_with_deadline(deadline)
    }

    fn shutdown_with_deadline(self, deadline: Option<Instant>) -> (RecorderStats, Result<()>) {
        let mut shutdown_error = None;
        if let Some(sender) = &self.sender {
            if let Some(deadline) = deadline {
                let mut command = Command::Shutdown;
                loop {
                    if Instant::now() >= deadline {
                        shutdown_error = Some(Error::new(
                            "OBSERVER_SHUTDOWN_TIMEOUT",
                            "recorder did not accept shutdown before the deadline",
                        ));
                        break;
                    }
                    match sender.try_send(command) {
                        Ok(()) => break,
                        Err(TrySendError::Full(returned)) => {
                            command = returned;
                            let remaining = deadline.saturating_duration_since(Instant::now());
                            if remaining.is_zero() {
                                shutdown_error = Some(Error::new(
                                    "OBSERVER_SHUTDOWN_TIMEOUT",
                                    "recorder did not accept shutdown before the deadline",
                                ));
                                break;
                            }
                            thread::sleep(remaining.min(Duration::from_millis(2)));
                        }
                        Err(TrySendError::Disconnected(_)) => {
                            shutdown_error = Some(Error::new(
                                "OBSERVER_SHUTDOWN_FAILED",
                                "recorder worker disconnected before shutdown",
                            ));
                            break;
                        }
                    }
                }
            } else if sender.send(Command::Shutdown).is_err() {
                shutdown_error = Some(Error::new(
                    "OBSERVER_SHUTDOWN_FAILED",
                    "recorder worker did not accept shutdown",
                ));
            }
        }
        let join = match self.join.lock() {
            Ok(mut join) => join.take(),
            Err(_) => {
                mark_sink_failed(&self.counters);
                shutdown_error = Some(Error::new(
                    "OBSERVER_SHUTDOWN_FAILED",
                    "recorder worker state could not be read",
                ));
                None
            }
        };
        if let Some(join) = join {
            let joined = if let Some(deadline) = deadline {
                while !join.is_finished() && Instant::now() < deadline {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    thread::sleep(remaining.min(Duration::from_millis(2)));
                }
                if join.is_finished() {
                    join.join().is_ok()
                } else {
                    false
                }
            } else {
                join.join().is_ok()
            };
            if !joined {
                if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                    shutdown_error = Some(Error::new(
                        "OBSERVER_SHUTDOWN_TIMEOUT",
                        "recorder worker did not finish before the deadline",
                    ));
                } else {
                    mark_sink_failed(&self.counters);
                    record_durability_unknown(
                        &self.counters,
                        self.counters.written_records.load(Ordering::Acquire),
                        self.counters.written_bytes.load(Ordering::Acquire),
                    );
                    account_abandoned_pending(&self.counters);
                    shutdown_error = Some(Error::new(
                        "OBSERVER_SHUTDOWN_FAILED",
                        "recorder worker did not finish cleanly",
                    ));
                }
            }
        }
        if self.counters.failed.load(Ordering::Acquire) && shutdown_error.is_none() {
            shutdown_error = Some(Error::new(
                "OBSERVER_SHUTDOWN_FAILED",
                "recorder reported a sink failure",
            ));
        }
        if self.counters.pending_records.load(Ordering::Acquire) > 0 {
            mark_sink_failed(&self.counters);
            account_abandoned_pending(&self.counters);
            shutdown_error = Some(Error::new(
                "OBSERVER_SHUTDOWN_FAILED",
                "recorder left accepted records pending",
            ));
        }
        let stats = self.stats();
        (stats, shutdown_error.map_or(Ok(()), Err))
    }

    fn drop_record(&self, bytes: u64) {
        self.counters.dropped_records.fetch_add(1, Ordering::AcqRel);
        self.counters
            .dropped_bytes
            .fetch_add(bytes, Ordering::AcqRel);
    }
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|_| {
        Error::new(
            "OBSERVER_PATH_INVALID",
            "observer directory cannot be inspected",
        )
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "OBSERVER_PATH_INVALID",
            "observer directory must be a regular directory",
        ));
    }
    private_permissions(path, true).map_err(|_| {
        Error::new(
            "OBSERVER_PERMISSION_FAILED",
            "observer directory permissions could not be restricted",
        )
    })?;
    Ok(())
}

fn reserve(counter: &AtomicU64, limit: u64, amount: u64) -> bool {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        if amount > limit.saturating_sub(current) {
            return false;
        }
        match counter.compare_exchange(
            current,
            current + amount,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return true,
            Err(next) => current = next,
        }
    }
}

fn release_pending(counters: &Counters, bytes: u64) {
    counters.pending_records.fetch_sub(1, Ordering::AcqRel);
    counters.pending_bytes.fetch_sub(bytes, Ordering::AcqRel);
}

fn record_drop(counters: &Counters, bytes: u64) {
    counters.dropped_records.fetch_add(1, Ordering::AcqRel);
    counters.dropped_bytes.fetch_add(bytes, Ordering::AcqRel);
}

fn record_durability_unknown(counters: &Counters, records: u64, bytes: u64) {
    counters
        .durability_unknown_records
        .fetch_add(records, Ordering::AcqRel);
    counters
        .durability_unknown_bytes
        .fetch_add(bytes, Ordering::AcqRel);
}

fn mark_sink_failed(counters: &Counters) {
    if !counters.failed.swap(true, Ordering::AcqRel) {
        counters.sink_failures.fetch_add(1, Ordering::AcqRel);
    }
}

fn account_abandoned_pending(counters: &Counters) {
    let records = counters.pending_records.swap(0, Ordering::AcqRel);
    let bytes = counters.pending_bytes.swap(0, Ordering::AcqRel);
    counters
        .dropped_records
        .fetch_add(records, Ordering::AcqRel);
    counters.dropped_bytes.fetch_add(bytes, Ordering::AcqRel);
}

fn reject_queued(receiver: &mpsc::Receiver<Command>, counters: &Counters) {
    while let Ok(command) = receiver.recv() {
        match command {
            Command::Append(record) => {
                let size = record.bytes.len() as u64;
                release_pending(counters, size);
                record_drop(counters, size);
            }
            Command::Shutdown => break,
        }
    }
}

fn recorder_loop(
    receiver: mpsc::Receiver<Command>,
    mut config: RecorderConfig,
    counters: Arc<Counters>,
    live_config: Option<LiveConfigSource>,
) {
    let mut segment_index = match next_segment_index(&config.directory) {
        Ok(index) => index,
        Err(_) => {
            mark_sink_failed(&counters);
            reject_queued(&receiver, &counters);
            return;
        }
    };
    let mut file = match open_segment(&config.directory, segment_index) {
        Ok(file) => file,
        Err(_) => {
            mark_sink_failed(&counters);
            reject_queued(&receiver, &counters);
            return;
        }
    };
    let mut segment_bytes = match file.metadata() {
        Ok(metadata) => metadata.len(),
        Err(_) => {
            mark_sink_failed(&counters);
            reject_queued(&receiver, &counters);
            return;
        }
    };
    let mut live_settings = LiveSettings::initial(config.retention_bytes, config.retention_days);
    let mut next_live_poll = Instant::now();
    if let Some(source) = live_config.as_ref() {
        reload_live_settings(source, &mut config, &mut live_settings, &counters);
        next_live_poll = Instant::now() + Duration::from_secs(1);
    }
    if apply_retention(&config, Some(segment_index)).is_err() {
        mark_sink_failed(&counters);
        reject_queued(&receiver, &counters);
        return;
    }
    let mut segment_written_records = 0_u64;
    let mut segment_written_bytes = 0_u64;
    loop {
        if let Some(source) = live_config.as_ref()
            && Instant::now() >= next_live_poll
        {
            reload_live_settings(source, &mut config, &mut live_settings, &counters);
            next_live_poll = Instant::now() + Duration::from_secs(1);
        }
        let command = if live_config.is_some() {
            let timeout = next_live_poll.saturating_duration_since(Instant::now());
            match receiver.recv_timeout(timeout) {
                Ok(command) => command,
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match receiver.recv() {
                Ok(command) => command,
                Err(_) => break,
            }
        };
        match command {
            Command::Shutdown => break,
            Command::Append(record) => {
                let size = record.bytes.len() as u64;
                if !live_settings.allows_record(
                    now_unix_ms(),
                    record.severity,
                    record.kind,
                    record.module_id.as_deref(),
                    record.client_id.as_deref(),
                    record.operation_id.as_deref(),
                    record.manager_policy,
                    record.contains_redacted_text,
                ) {
                    release_pending(&counters, size);
                    record_filtered(&counters, size);
                    continue;
                }
                if counters.failed.load(Ordering::Acquire) {
                    release_pending(&counters, size);
                    record_drop(&counters, size);
                    continue;
                }
                let result = (|| -> std::io::Result<()> {
                    if segment_bytes > 0
                        && segment_bytes.saturating_add(size) > config.segment_bytes
                    {
                        file.sync_all()?;
                        segment_written_records = 0;
                        segment_written_bytes = 0;
                        let next_segment_index = segment_index.checked_add(1).ok_or_else(|| {
                            std::io::Error::other("observer segment index exhausted")
                        })?;
                        let next_file = open_segment(&config.directory, next_segment_index)?;
                        file = next_file;
                        segment_index = next_segment_index;
                        segment_bytes = file.metadata()?.len();
                        apply_retention(&config, Some(segment_index))?;
                    }
                    file.write_all(&record.bytes)?;
                    file.flush()?;
                    segment_bytes = segment_bytes.saturating_add(size);
                    Ok(())
                })();
                release_pending(&counters, size);
                match result {
                    Ok(()) => {
                        counters.written_records.fetch_add(1, Ordering::AcqRel);
                        counters.written_bytes.fetch_add(size, Ordering::AcqRel);
                        segment_written_records = segment_written_records.saturating_add(1);
                        segment_written_bytes = segment_written_bytes.saturating_add(size);
                    }
                    Err(_) => {
                        mark_sink_failed(&counters);
                        record_drop(&counters, size);
                    }
                };
            }
        }
    }
    let final_sync_ok = file.sync_all().is_ok();
    if !final_sync_ok {
        mark_sink_failed(&counters);
        record_durability_unknown(&counters, segment_written_records, segment_written_bytes);
    }
    drop(file);
    if let Some(source) = live_config.as_ref() {
        // Shutdown is an existing retention pass too; sample once before it
        // so a recently published operator snapshot can take effect here.
        reload_live_settings(source, &mut config, &mut live_settings, &counters);
    }
    let protected_index = (!final_sync_ok).then_some(segment_index);
    if apply_retention(&config, protected_index).is_err() {
        mark_sink_failed(&counters);
    }
}

fn reload_live_settings(
    source: &LiveConfigSource,
    config: &mut RecorderConfig,
    current: &mut LiveSettings,
    counters: &Counters,
) {
    match source.load_update(current, config.segment_bytes) {
        Ok(Some(next)) => {
            let version = next.config_version;
            config.retention_bytes = next.retention_bytes;
            config.retention_days = next.retention_days;
            *current = next;
            source.publish_text_settings(current);
            counters.live_config_last_error.store(0, Ordering::Release);
            counters
                .live_config_version
                .store(version, Ordering::Release);
        }
        Ok(None) => {
            counters.live_config_last_error.store(0, Ordering::Release);
        }
        Err(error) => {
            saturating_add(&counters.live_config_reload_failures, 1);
            counters
                .live_config_last_error
                .store(live_config_error_number(&error.code), Ordering::Release);
        }
    }
}

fn live_config_error_number(code: &str) -> u64 {
    match code {
        "OBSERVER_LIVE_CONFIG_UNAVAILABLE" => 1,
        "OBSERVER_LIVE_CONFIG_SCOPE_MISMATCH" => 2,
        "OBSERVER_LIVE_CONFIG_VERSION_REJECTED" => 3,
        "OBSERVER_LIVE_CONFIG_INVALID" => 4,
        "OBSERVER_CONTENT_UNSUPPORTED" => 5,
        _ => 4,
    }
}

fn live_config_error_code(value: u64) -> Option<&'static str> {
    match value {
        1 => Some("OBSERVER_LIVE_CONFIG_UNAVAILABLE"),
        2 => Some("OBSERVER_LIVE_CONFIG_SCOPE_MISMATCH"),
        3 => Some("OBSERVER_LIVE_CONFIG_VERSION_REJECTED"),
        4 => Some("OBSERVER_LIVE_CONFIG_INVALID"),
        5 => Some("OBSERVER_CONTENT_UNSUPPORTED"),
        _ => None,
    }
}

fn saturating_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
        Some(value.saturating_add(amount))
    });
}

fn record_filtered(counters: &Counters, bytes: u64) {
    saturating_add(&counters.filtered_records, 1);
    saturating_add(&counters.filtered_bytes, bytes);
}

fn next_segment_index(directory: &Path) -> std::io::Result<u64> {
    let mut maximum = 0;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if let Some(index) = entry.file_name().to_str().and_then(|name| {
            name.strip_prefix("diagnostics-")?
                .strip_suffix(".jsonl")?
                .parse()
                .ok()
        }) {
            maximum = maximum.max(index);
        }
    }
    Ok(maximum)
}

fn open_segment(directory: &Path, index: u64) -> std::io::Result<File> {
    let path = directory.join(format!("diagnostics-{index:020}.jsonl"));
    match fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.is_file() || metadata.file_type().is_symlink() => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "observer segment is not a regular file",
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let file = OpenOptions::new().create(true).append(true).open(&path)?;
    private_permissions(&path, false).map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "observer segment ACL could not be restricted",
        )
    })?;
    Ok(file)
}

fn apply_retention(config: &RecorderConfig, active_index: Option<u64>) -> std::io::Result<()> {
    let now = SystemTime::now();
    let mut files = Vec::new();
    for entry in fs::read_dir(&config.directory)? {
        let entry = entry?;
        let file_name = entry.file_name();
        let Some(name) = file_name.to_str().map(str::to_owned) else {
            continue;
        };
        if let Some(index) = name
            .strip_prefix("diagnostics-")
            .and_then(|name| name.strip_suffix(".jsonl"))
            .and_then(|index| index.parse::<u64>().ok())
        {
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file() || metadata.file_type().is_symlink() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "observer segment is not a regular file",
                ));
            }
            files.push((index, entry, metadata));
        }
    }
    files.sort_by_key(|(index, _, _)| *index);
    let mut total = files.iter().fold(0_u64, |total, (_, _, metadata)| {
        total.saturating_add(metadata.len())
    });
    let max_age = Duration::from_secs(config.retention_days.saturating_mul(86_400));
    for (index, entry, metadata) in files {
        if Some(index) == active_index {
            continue;
        }
        let old = now.duration_since(metadata.modified()?).unwrap_or_default() >= max_age;
        if old || total > config.retention_bytes {
            fs::remove_file(entry.path())?;
            total = total.saturating_sub(metadata.len());
        }
    }
    if total > config.retention_bytes {
        return Err(std::io::Error::other(
            "observer active segment exceeds retention capacity",
        ));
    }
    Ok(())
}

/// Three separately completed authenticated report reads. These projections
/// are sampled sequentially and do not claim one atomic Store snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct ReadbackSnapshot {
    pub after: u64,
    pub limit: u64,
    pub content_mode: &'static str,
    pub consistency: &'static str,
    pub read_completed_at_unix_ms: ProjectionReadTimes,
    pub delta: ReportPage<TimelineItemMetadata>,
    pub attention: ReportPage<AttentionItemMetadata>,
    pub capacity: ReportPage<CapacityItemMetadata>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectionReadTimes {
    pub delta: u64,
    pub attention: u64,
    pub capacity: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ReportPage<T> {
    pub item_count: u64,
    pub next_cursor: Option<u64>,
    pub next_after: Option<u64>,
    pub total_items: Option<u64>,
    pub generated_at_ms: Option<i64>,
    pub projection: ProjectionMetadata,
    pub items: Vec<T>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProjectionMetadata {
    pub source_kind: &'static str,
    pub coverage_complete: bool,
    pub has_older: bool,
    pub has_newer: bool,
    pub gap_reason: Option<String>,
    pub gap_count: u64,
    pub serialized_byte_length: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineItemMetadata {
    pub cursor: u64,
    pub kind: String,
    pub recorded_at_ms: i64,
    pub operation_id: Option<String>,
    pub payload_present: bool,
    pub gap_present: bool,
    /// Exact bounded loss metadata from the authorized Store page. No source
    /// payload is copied into the observer snapshot.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gap: Option<TimelineGapMetadata>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimelineGapMetadata {
    pub reason: &'static str,
    pub item_serialized_bytes: u64,
    pub payload_serialized_bytes: u64,
    pub max_single_item_bytes: u64,
    pub payload_digest: String,
    pub detached_reference: TimelineDetachedReference,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimelineDetachedReference {
    Observation {
        observation_id: u64,
    },
    Artifact {
        artifact_id: String,
        range_reads: &'static str,
        observation_id: u64,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct AttentionItemMetadata {
    pub kind: String,
    pub scope_key: Option<String>,
    pub binding_id: Option<String>,
    pub generation: Option<i64>,
    pub manager_actionable: Option<bool>,
    pub source: Option<AttentionSourceMetadata>,
    /// Closed, identity-preserving view of a Manager-visible supervisor
    /// failure. The raw Store address and diagnostic payload are never copied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub module_supervisor: Option<ModuleSupervisorFailureMetadata>,
    pub gap_present: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct ModuleSupervisorFailureMetadata {
    pub component: &'static str,
    pub event_id: String,
    pub module_id: String,
    pub artifact_id: String,
    pub artifact_version: String,
    pub build_id: Option<String>,
    pub binding_id: String,
    pub generation: i64,
    pub boot_id: Option<String>,
    pub phase: String,
    pub effect_certainty: String,
    pub certainty_scope: &'static str,
    pub stage: Option<String>,
    pub error_code: Option<String>,
    pub unknown_operation_ids: Vec<String>,
    pub unknown_operation_count: u64,
    pub unknown_operation_ids_truncated: bool,
    pub retry_authorized: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct AttentionSourceMetadata {
    pub kind: String,
    pub observed_at_ms: Option<i64>,
    pub stale: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapacityItemMetadata {
    pub scope_key: Option<String>,
    pub counts: CapacityCounts,
    pub roster: Option<String>,
    pub capacity_available: Option<bool>,
    pub capacity_reason: Option<String>,
    pub new_work_enabled: Option<bool>,
    pub binding_count: Option<u64>,
    pub ledger_updated_at_ms: Option<i64>,
    pub gap_present: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct CapacityCounts {
    pub reserved: u64,
    pub active: u64,
    pub unknown_outcomes: u64,
    pub desired_writers: u64,
    pub effective_writers: u64,
    pub pending_admissions: u64,
    pub commands_in_flight: u64,
    pub released_entries: u64,
}

pub async fn readback_snapshot(
    root: &Path,
    credential: &Credential,
    config: &IpcConfig,
    after: u64,
    limit: u64,
) -> Result<ReadbackSnapshot> {
    if limit == 0 || limit > MAX_READBACK_LIMIT || after > i64::MAX as u64 {
        return Err(Error::new(
            "OBSERVER_READBACK_INVALID",
            "report cursor or page limit is outside the supported range",
        ));
    }
    let mut client = Client::connect(root, credential, config)
        .await
        .map_err(|_| {
            Error::new(
                "OBSERVER_READBACK_FAILED",
                "authenticated report readback failed",
            )
        })?;
    let delta = client
        .request("report.delta", json!({"after":after,"limit":limit}))
        .await
        .map_err(|_| {
            Error::new(
                "OBSERVER_READBACK_FAILED",
                "authenticated report readback failed",
            )
        })?;
    let delta_at = read_completed_at_unix_ms();
    let delta = project_report_page(
        delta,
        limit,
        "observation_timeline",
        "next_cursor",
        project_timeline_item,
    )?;
    let attention = client
        .request("report.attention", json!({"after":after,"limit":limit}))
        .await
        .map_err(|_| {
            Error::new(
                "OBSERVER_READBACK_FAILED",
                "authenticated report readback failed",
            )
        })?;
    let attention_at = read_completed_at_unix_ms();
    let attention = project_report_page(
        attention,
        limit,
        "attention_projection",
        "next_after",
        project_attention_item,
    )?;
    let capacity = client
        .request("report.capacity", json!({"after":after,"limit":limit}))
        .await
        .map_err(|_| {
            Error::new(
                "OBSERVER_READBACK_FAILED",
                "authenticated report readback failed",
            )
        })?;
    let capacity_at = read_completed_at_unix_ms();
    let capacity = project_report_page(
        capacity,
        limit,
        "capacity_accounting",
        "next_after",
        project_capacity_item,
    )?;
    Ok(ReadbackSnapshot {
        after,
        limit,
        content_mode: "metadata_only",
        consistency: "independent_authenticated_reads",
        read_completed_at_unix_ms: ProjectionReadTimes {
            delta: delta_at,
            attention: attention_at,
            capacity: capacity_at,
        },
        delta,
        attention,
        capacity,
    })
}

fn schema_error() -> Error {
    Error::new(
        "OBSERVER_SCHEMA_UNSUPPORTED",
        "authenticated report response is outside the closed metadata schema",
    )
}

fn required_bool(value: &Value, key: &str) -> Result<bool> {
    value
        .get(key)
        .and_then(Value::as_bool)
        .ok_or_else(schema_error)
}

fn optional_bool(value: &Value, key: &str) -> Result<Option<bool>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_bool().map(Some).ok_or_else(schema_error),
    }
}

fn optional_i64(value: &Value, key: &str) -> Result<Option<i64>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_i64().map(Some).ok_or_else(schema_error),
    }
}

fn required_u64(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(schema_error)
}

fn optional_u64(value: &Value, key: &str) -> Result<Option<u64>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(value) => value.as_u64().map(Some).ok_or_else(schema_error),
    }
}

fn optional_token(value: &Value, key: &str) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(token)) => checked_token(token).map(Some),
        Some(_) => Err(schema_error()),
    }
}

fn required_token(value: &Value, key: &str) -> Result<String> {
    match value.get(key) {
        Some(Value::String(token)) => checked_token(token),
        _ => Err(schema_error()),
    }
}

fn checked_token(token: &str) -> Result<String> {
    if token.is_empty()
        || token.len() > 128
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(schema_error());
    }
    Ok(token.to_owned())
}

fn checked_identity(value: &Value, key: &str, max_len: usize, extra: &[u8]) -> Result<String> {
    let identity = value
        .get(key)
        .and_then(Value::as_str)
        .filter(|identity| {
            !identity.is_empty()
                && identity.len() <= max_len
                && identity
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || extra.contains(&byte))
        })
        .ok_or_else(schema_error)?;
    Ok(identity.to_owned())
}

fn optional_identity(
    value: &Value,
    key: &str,
    max_len: usize,
    extra: &[u8],
) -> Result<Option<String>> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(identity))
            if !identity.is_empty()
                && identity.len() <= max_len
                && identity
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || extra.contains(&byte)) =>
        {
            Ok(Some(identity.clone()))
        }
        _ => Err(schema_error()),
    }
}

fn project_module_supervisor_failure(
    item: &Value,
    binding_id: &str,
    generation: i64,
) -> Result<ModuleSupervisorFailureMetadata> {
    const TOKEN_EXTRA: &[u8] = b"-_.:";
    const ATOM_EXTRA: &[u8] = b"._:-";
    const BUILD_EXTRA: &[u8] = b"._:-+";
    const VERSION_EXTRA: &[u8] = b".+_-";
    const MAX_OPERATION_IDS: usize = 256;
    const MAX_OPERATION_COUNT: u64 = 1_000_000;

    let address = item
        .get("address")
        .and_then(Value::as_object)
        .map(|_| &item["address"])
        .ok_or_else(schema_error)?;
    let projected_binding_id = checked_identity(address, "binding_id", 128, TOKEN_EXTRA)?;
    let projected_generation = address
        .get("generation")
        .and_then(Value::as_i64)
        .filter(|value| *value > 0)
        .ok_or_else(schema_error)?;
    if projected_binding_id != binding_id || projected_generation != generation {
        return Err(schema_error());
    }

    let phase = required_token(address, "phase")?;
    if !matches!(
        phase.as_str(),
        "waiting_for_demand"
            | "waiting_for_kernel"
            | "starting"
            | "ready"
            | "restart_backoff"
            | "exited"
            | "exited_proven"
            | "owner_retained"
            | "identity_unknown"
            | "completed"
            | "isolated"
    ) {
        return Err(schema_error());
    }
    let effect_certainty = required_token(address, "effect_certainty")?;
    if !matches!(effect_certainty.as_str(), "unknown" | "not_started")
        || address["certainty_scope"] != "latest_helper_attempt_only"
        || required_bool(address, "retry_authorized")?
    {
        return Err(schema_error());
    }

    let stage = optional_token(address, "stage")?;
    if stage.as_deref().is_some_and(|stage| {
        !matches!(
            stage,
            "resolve_refs" | "validate_launch" | "spawn" | "worker" | "owner" | "store" | "journal"
        )
    }) {
        return Err(schema_error());
    }
    let error_code = optional_identity(address, "error_code", 128, b"_")?;
    if error_code.as_deref().is_some_and(|code| {
        !code
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
    }) || (stage.is_some() && error_code.is_none())
        || (effect_certainty == "not_started"
            && !stage
                .as_deref()
                .is_some_and(|stage| matches!(stage, "resolve_refs" | "validate_launch")))
    {
        return Err(schema_error());
    }

    let ids = address
        .get("unknown_operation_ids")
        .and_then(Value::as_array)
        .filter(|ids| ids.len() <= MAX_OPERATION_IDS)
        .ok_or_else(schema_error)?;
    let mut unknown_operation_ids = Vec::with_capacity(ids.len());
    for id in ids {
        let id = id.as_str().ok_or_else(schema_error)?;
        unknown_operation_ids.push(checked_token(id)?);
    }
    if unknown_operation_ids
        .windows(2)
        .any(|pair| pair[0] >= pair[1])
    {
        return Err(schema_error());
    }
    let unknown_operation_count = required_u64(address, "unknown_operation_count")?;
    let unknown_operation_ids_truncated =
        required_bool(address, "unknown_operation_ids_truncated")?;
    if unknown_operation_count > MAX_OPERATION_COUNT
        || unknown_operation_count < unknown_operation_ids.len() as u64
        || unknown_operation_ids_truncated
            != (unknown_operation_count > unknown_operation_ids.len() as u64)
    {
        return Err(schema_error());
    }

    Ok(ModuleSupervisorFailureMetadata {
        component: "module_supervisor",
        event_id: checked_identity(address, "event_id", 256, TOKEN_EXTRA)?,
        module_id: checked_identity(address, "module_id", 128, ATOM_EXTRA)?,
        artifact_id: checked_identity(address, "artifact_id", 128, ATOM_EXTRA)?,
        artifact_version: checked_identity(address, "artifact_version", 128, VERSION_EXTRA)?,
        build_id: optional_identity(address, "build_id", 128, BUILD_EXTRA)?,
        binding_id: projected_binding_id,
        generation: projected_generation,
        boot_id: optional_identity(address, "boot_id", 128, TOKEN_EXTRA)?,
        phase,
        effect_certainty,
        certainty_scope: "latest_helper_attempt_only",
        stage,
        error_code,
        unknown_operation_ids,
        unknown_operation_count,
        unknown_operation_ids_truncated,
        retry_authorized: false,
    })
}

fn project_report_page<T>(
    raw: Value,
    limit: u64,
    expected_source: &'static str,
    cursor_name: &'static str,
    project_item: fn(&Value) -> Result<T>,
) -> Result<ReportPage<T>> {
    let projection = raw.get("projection").ok_or_else(schema_error)?;
    let source_kind = required_token(projection, "source_kind")?;
    if source_kind != expected_source {
        return Err(schema_error());
    }
    let projection = ProjectionMetadata {
        source_kind: expected_source,
        coverage_complete: required_bool(projection, "coverage_complete")?,
        has_older: required_bool(projection, "has_older")?,
        has_newer: required_bool(projection, "has_newer")?,
        gap_reason: optional_token(projection, "gap_reason")?,
        gap_count: required_u64(projection, "gap_count")?,
        serialized_byte_length: required_u64(projection, "serialized_byte_length")?,
    };
    let raw_items = raw
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(schema_error)?;
    if raw_items.len() as u64 > limit {
        return Err(schema_error());
    }
    let items = raw_items
        .iter()
        .map(project_item)
        .collect::<Result<Vec<_>>>()?;
    let next = optional_u64(&raw, cursor_name)?;
    if next.is_none() {
        return Err(schema_error());
    }
    let (next_cursor, next_after) = if cursor_name == "next_cursor" {
        (next, None)
    } else {
        (None, next)
    };
    Ok(ReportPage {
        item_count: items.len() as u64,
        next_cursor,
        next_after,
        total_items: optional_u64(&raw, "total_items")?,
        generated_at_ms: optional_i64(&raw, "generated_at_ms")?,
        projection,
        items,
    })
}

fn project_timeline_item(item: &Value) -> Result<TimelineItemMetadata> {
    let cursor = required_u64(item, "cursor")?;
    let recorded_at_ms = item
        .get("recorded_at_ms")
        .and_then(Value::as_i64)
        .ok_or_else(schema_error)?;
    let item_object = item.as_object().ok_or_else(schema_error)?;
    let gap = match item.get("gap") {
        None => None,
        Some(gap) => Some(project_timeline_gap(gap, cursor)?),
    };
    Ok(TimelineItemMetadata {
        cursor,
        kind: required_token(item, "kind")?,
        recorded_at_ms,
        operation_id: optional_token(item, "operation_id")?,
        payload_present: item_object.contains_key("payload"),
        gap_present: gap.is_some(),
        gap,
    })
}

fn project_timeline_gap(gap: &Value, cursor: u64) -> Result<TimelineGapMetadata> {
    let reason = match required_token(gap, "reason")?.as_str() {
        "item_exceeds_single_item_bytes" => "item_exceeds_single_item_bytes",
        "item_exceeds_page_byte_budget" => "item_exceeds_page_byte_budget",
        _ => return Err(schema_error()),
    };
    let item_serialized_bytes = required_u64(gap, "item_serialized_bytes")?;
    let payload_serialized_bytes = required_u64(gap, "payload_serialized_bytes")?;
    let max_single_item_bytes = required_u64(gap, "max_single_item_bytes")?;
    if item_serialized_bytes == 0
        || payload_serialized_bytes == 0
        || payload_serialized_bytes > item_serialized_bytes
        || max_single_item_bytes == 0
        || (reason == "item_exceeds_single_item_bytes"
            && item_serialized_bytes <= max_single_item_bytes)
        || (reason == "item_exceeds_page_byte_budget"
            && item_serialized_bytes > max_single_item_bytes)
    {
        return Err(schema_error());
    }

    let payload_digest = required_token(gap, "payload_digest")?;
    if payload_digest.len() != 64
        || !payload_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(schema_error());
    }
    let detached_reference = gap
        .get("detached_reference")
        .filter(|value| value.is_object())
        .ok_or_else(schema_error)?;
    let observation_id = required_u64(detached_reference, "observation_id")?;
    if observation_id != cursor {
        return Err(schema_error());
    }
    let detached_reference = match required_token(detached_reference, "kind")?.as_str() {
        "observation" if gap.get("artifact_ref").is_none() => {
            TimelineDetachedReference::Observation { observation_id }
        }
        "artifact" => {
            let artifact_id = required_token(detached_reference, "artifact_id")?;
            if gap.get("artifact_ref").and_then(Value::as_str) != Some(artifact_id.as_str())
                || required_token(detached_reference, "range_reads")? != "artifact.read"
            {
                return Err(schema_error());
            }
            TimelineDetachedReference::Artifact {
                artifact_id,
                range_reads: "artifact.read",
                observation_id,
            }
        }
        _ => return Err(schema_error()),
    };
    Ok(TimelineGapMetadata {
        reason,
        item_serialized_bytes,
        payload_serialized_bytes,
        max_single_item_bytes,
        payload_digest,
        detached_reference,
    })
}

fn project_attention_item(item: &Value) -> Result<AttentionItemMetadata> {
    let kind = required_token(item, "kind")?;
    let source = match item.get("source") {
        None | Some(Value::Null) => None,
        Some(source) => Some(AttentionSourceMetadata {
            kind: required_token(source, "kind")?,
            observed_at_ms: optional_i64(source, "observed_at_ms")?,
            stale: required_bool(source, "stale")?,
        }),
    };
    let generation = optional_i64(item, "generation")?;
    if generation.is_some_and(|generation| generation < 0) {
        return Err(schema_error());
    }
    let binding_id = optional_token(item, "binding_id")?;
    let module_supervisor = if kind == "module_supervisor_failure" {
        let source = source.as_ref().ok_or_else(schema_error)?;
        if source.kind != "module_supervisor" {
            return Err(schema_error());
        }
        let binding_id = binding_id.as_deref().ok_or_else(schema_error)?;
        let generation = generation
            .filter(|generation| *generation > 0)
            .ok_or_else(schema_error)?;
        Some(project_module_supervisor_failure(
            item, binding_id, generation,
        )?)
    } else {
        None
    };
    Ok(AttentionItemMetadata {
        kind,
        scope_key: optional_token(item, "scope_key")?,
        binding_id,
        generation,
        manager_actionable: optional_bool(item, "manager_actionable")?,
        source,
        module_supervisor,
        gap_present: item.get("gap").is_some(),
    })
}

fn project_capacity_item(item: &Value) -> Result<CapacityItemMetadata> {
    let counts = item.get("counts").ok_or_else(schema_error)?;
    let bindings = match item.get("bindings") {
        None | Some(Value::Null) => None,
        Some(Value::Array(bindings)) => Some(bindings.len() as u64),
        Some(_) => return Err(schema_error()),
    };
    Ok(CapacityItemMetadata {
        scope_key: item
            .get("scope")
            .map(|scope| optional_token(scope, "scope_key"))
            .transpose()?
            .flatten(),
        counts: CapacityCounts {
            reserved: required_u64(counts, "reserved")?,
            active: required_u64(counts, "active")?,
            unknown_outcomes: required_u64(counts, "unknown_outcomes")?,
            desired_writers: required_u64(counts, "desired_writers")?,
            effective_writers: required_u64(counts, "effective_writers")?,
            pending_admissions: required_u64(counts, "pending_admissions")?,
            commands_in_flight: required_u64(counts, "commands_in_flight")?,
            released_entries: required_u64(counts, "released_entries")?,
        },
        roster: optional_token(item, "roster")?,
        capacity_available: optional_bool(item, "capacity_available")?,
        capacity_reason: optional_token(item, "capacity_reason")?,
        new_work_enabled: optional_bool(item, "new_work_enabled")?,
        binding_count: bindings,
        ledger_updated_at_ms: optional_i64(item, "ledger_updated_at_ms")?,
        gap_present: item.get("gap").is_some(),
    })
}

fn read_completed_at_unix_ms() -> u64 {
    now_unix_ms()
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
