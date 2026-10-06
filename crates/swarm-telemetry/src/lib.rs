//! Bounded diagnostic producer with opt-in Atlas-redacted text.
//!
//! Constructing a [`Producer`] starts no worker. The first enabled emission
//! lazily starts one stderr writer. Emission validates and serializes only a
//! closed record schema, accounts record and byte capacity, and uses
//! `SyncSender::try_send`; it never waits for writer I/O. Business facts and
//! Store acknowledgements must not be routed through this diagnostic channel.

use serde::Serialize;
use std::{
    io::{self, Write},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const STATE_UNSTARTED: u8 = 0;
const STATE_STARTING: u8 = 1;
const STATE_READY: u8 = 2;
const STATE_FAILED: u8 = 3;

/// Initial operational bounds. These constrain diagnostic buffering only;
/// they do not limit agents, model calls, Store work, or durable Operations.
pub const DEFAULT_QUEUE_RECORDS: usize = 256;
pub const DEFAULT_QUEUE_BYTES: usize = 1_048_576;
pub const DEFAULT_MAX_RECORD_BYTES: usize = 16_384;
pub const MAX_QUEUE_RECORDS: usize = 65_536;
pub const MAX_QUEUE_BYTES: usize = 67_108_864;
pub const MAX_RECORD_BYTES: usize = 65_536;
/// Maximum raw text accepted by the optional content path before redaction.
/// Oversized text is rejected before any wire serialization.
pub const MAX_TEXT_INPUT_BYTES: usize = 16_384;
/// Maximum redacted text retained in one observer content record.
pub const MAX_REDACTED_TEXT_BYTES: usize = 8_192;
const MAX_ID_BYTES: usize = 128;

/// Host-provided pure redactor. A missing result suppresses optional content;
/// the bounded metadata record remains available.
pub type TextRedactor = fn(&str) -> Option<String>;
/// A bounded scope check for the current capture policy. It is evaluated
/// before redaction, serialization, or queue admission.
pub type TextCapturePolicy =
    Arc<dyn Fn(Severity, Kind, Option<&str>, Option<&str>, Option<&str>) -> bool + Send + Sync>;

/// Validated local-recorder configuration. The optional producer is local to
/// its caller; this crate installs no global subscriber or mandatory backend.
#[derive(Clone, Debug)]
pub struct Config {
    enabled: bool,
    queue_records: usize,
    queue_bytes: usize,
    max_record_bytes: usize,
    text_redactor: Option<TextRedactor>,
    text_capture_policy: Option<TextCapturePolicy>,
}

impl Config {
    /// Return a valid config with explicit default recorder bounds.
    pub fn metadata_stderr() -> Self {
        Self::default()
    }

    /// Disable diagnostics without starting a writer or counting intentional
    /// suppression as a loss.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Set operational queue and record bounds. The resulting config remains
    /// opt-in at the Producer construction site and uses stderr as its sink.
    pub fn with_limits(
        queue_records: usize,
        queue_bytes: usize,
        max_record_bytes: usize,
    ) -> Result<Self, ConfigError> {
        if queue_records == 0 {
            return Err(ConfigError::ZeroRecordCapacity);
        }
        if queue_bytes == 0 {
            return Err(ConfigError::ZeroByteCapacity);
        }
        if max_record_bytes == 0 {
            return Err(ConfigError::ZeroRecordLimit);
        }
        if queue_records > MAX_QUEUE_RECORDS {
            return Err(ConfigError::RecordCapacityTooLarge);
        }
        if queue_bytes > MAX_QUEUE_BYTES {
            return Err(ConfigError::ByteCapacityTooLarge);
        }
        if max_record_bytes > MAX_RECORD_BYTES {
            return Err(ConfigError::RecordLimitTooLarge);
        }
        if max_record_bytes > queue_bytes {
            return Err(ConfigError::RecordLimitExceedsQueue);
        }
        Ok(Self {
            enabled: true,
            queue_records,
            queue_bytes,
            max_record_bytes,
            text_redactor: None,
            text_capture_policy: None,
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Install the pure redactor used by the optional content path. The
    /// default remains metadata-only; callers must opt into a redactor before
    /// a text field can reach the observer line.
    pub fn with_text_redactor(mut self, redactor: TextRedactor) -> Self {
        self.text_redactor = Some(redactor);
        self
    }

    /// Install a live scope policy. Without a policy, the producer remains
    /// metadata-only even when a record carries optional text.
    pub fn with_text_capture_policy(mut self, policy: TextCapturePolicy) -> Self {
        self.text_capture_policy = Some(policy);
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            queue_records: DEFAULT_QUEUE_RECORDS,
            queue_bytes: DEFAULT_QUEUE_BYTES,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
            text_redactor: None,
            text_capture_policy: None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    ZeroRecordCapacity,
    ZeroByteCapacity,
    ZeroRecordLimit,
    RecordCapacityTooLarge,
    ByteCapacityTooLarge,
    RecordLimitTooLarge,
    RecordLimitExceedsQueue,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::ZeroRecordCapacity => "queue_records must be greater than zero",
            Self::ZeroByteCapacity => "queue_bytes must be greater than zero",
            Self::ZeroRecordLimit => "max_record_bytes must be greater than zero",
            Self::RecordCapacityTooLarge => "queue_records exceeds the operational ceiling",
            Self::ByteCapacityTooLarge => "queue_bytes exceeds the operational ceiling",
            Self::RecordLimitTooLarge => "max_record_bytes exceeds the operational ceiling",
            Self::RecordLimitExceedsQueue => "max_record_bytes must fit within queue_bytes",
        };
        f.write_str(message)
    }
}

impl std::error::Error for ConfigError {}

/// Closed severity vocabulary. `off` is a producer setting, not a record.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

/// Closed diagnostic event vocabulary for current producer callsites.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    ClientDisconnected,
    StoreOperationFailed,
    ModuleStarted,
    ModuleStopped,
    AgentDeliveryFailed,
    RecorderFailure,
}

/// Finite producer components associated with correlated records.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Component {
    ModuleSupervisor,
}

/// Closed lifecycle phase vocabulary.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Admission,
    Commit,
    Readback,
    StoreDisconnect,
    ModuleStart,
    ModuleExit,
    AgentDelivery,
    RecorderWrite,
}

/// Safe static code values. Raw `Error` strings and source locations are not
/// accepted by the producer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Code {
    DisconnectPersistenceFailed,
    StoreOperationFailed,
    ModuleStartFailed,
    NativeExitObserved,
    AgentDeliveryFailed,
    RecorderWriteFailed,
}

/// An opaque identifier accepted only in known identity fields. IDs are
/// bounded and restricted to a conservative token alphabet. Arbitrary message,
/// native body, credential, and path fields do not exist in the public schema.
#[derive(Clone, Debug)]
struct KnownId(String);

impl KnownId {
    fn from_known(value: &str) -> Option<Self> {
        if value.is_empty()
            || value.len() > MAX_ID_BYTES
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':')
            })
        {
            return None;
        }
        Some(Self(value.to_owned()))
    }
}

/// Additional bounded metadata identities used only by the correlated
/// supervisor diagnostic shape. Their accepted alphabets mirror the already
/// validated Store observation fields.
#[derive(Clone, Debug)]
struct KnownMetadata(String);

impl KnownMetadata {
    fn from_event_id(value: &str) -> Option<Self> {
        Self::from_bounded(value, 256, b"-_.:")
    }

    fn from_atom(value: &str) -> Option<Self> {
        Self::from_bounded(value, MAX_ID_BYTES, b"._:-")
    }

    fn from_build_id(value: &str) -> Option<Self> {
        Self::from_bounded(value, MAX_ID_BYTES, b"._:-+")
    }

    fn from_version(value: &str) -> Option<Self> {
        Self::from_bounded(value, MAX_ID_BYTES, b".+_-")
    }

    fn from_bounded(value: &str, max_bytes: usize, punctuation: &[u8]) -> Option<Self> {
        if value.is_empty()
            || value.len() > max_bytes
            || !value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || punctuation.contains(&byte))
        {
            return None;
        }
        Some(Self(value.to_owned()))
    }
}

/// Input record. Optional identifiers are included only when the caller has
/// the corresponding known value; no values are synthesized for missing IDs.
#[derive(Clone, Debug)]
pub struct Record {
    severity: Severity,
    kind: Kind,
    phase: Phase,
    component: Option<Component>,
    code: Option<Code>,
    client_id: Option<KnownId>,
    link_id: Option<KnownId>,
    binding_id: Option<KnownId>,
    binding_generation: Option<u64>,
    operation_id: Option<KnownId>,
    module_boot_id: Option<KnownId>,
    event_id: Option<KnownMetadata>,
    module_id: Option<KnownMetadata>,
    artifact_id: Option<KnownMetadata>,
    artifact_version: Option<KnownMetadata>,
    build_id: Option<KnownMetadata>,
    task_id: Option<KnownId>,
    attempt_id: Option<KnownId>,
    text: Option<String>,
}

impl Record {
    pub fn new(severity: Severity, kind: Kind, phase: Phase) -> Self {
        Self {
            severity,
            kind,
            phase,
            component: None,
            code: None,
            client_id: None,
            link_id: None,
            binding_id: None,
            binding_generation: None,
            operation_id: None,
            module_boot_id: None,
            event_id: None,
            module_id: None,
            artifact_id: None,
            artifact_version: None,
            build_id: None,
            task_id: None,
            attempt_id: None,
            text: None,
        }
    }

    pub fn with_code(mut self, value: Option<Code>) -> Self {
        self.code = value;
        self
    }
    pub fn with_component(mut self, value: Option<Component>) -> Self {
        self.component = value;
        self
    }
    pub fn with_client_id(mut self, value: Option<&str>) -> Self {
        self.client_id = value.and_then(KnownId::from_known);
        self
    }
    pub fn with_link_id(mut self, value: Option<&str>) -> Self {
        self.link_id = value.and_then(KnownId::from_known);
        self
    }
    pub fn with_binding_id(mut self, value: Option<&str>) -> Self {
        self.binding_id = value.and_then(KnownId::from_known);
        self
    }
    pub fn with_binding_generation(mut self, value: Option<u64>) -> Self {
        self.binding_generation =
            value.filter(|generation| *generation > 0 && *generation <= i64::MAX as u64);
        self
    }
    pub fn with_operation_id(mut self, value: Option<&str>) -> Self {
        self.operation_id = value.and_then(KnownId::from_known);
        self
    }
    pub fn with_module_boot_id(mut self, value: Option<&str>) -> Self {
        self.module_boot_id = value.and_then(KnownId::from_known);
        self
    }
    pub fn with_event_id(mut self, value: Option<&str>) -> Self {
        self.event_id = value.and_then(KnownMetadata::from_event_id);
        self
    }
    pub fn with_module_id(mut self, value: Option<&str>) -> Self {
        self.module_id = value.and_then(KnownMetadata::from_atom);
        self
    }
    pub fn with_artifact_id(mut self, value: Option<&str>) -> Self {
        self.artifact_id = value.and_then(KnownMetadata::from_atom);
        self
    }
    pub fn with_artifact_version(mut self, value: Option<&str>) -> Self {
        self.artifact_version = value.and_then(KnownMetadata::from_version);
        self
    }
    pub fn with_build_id(mut self, value: Option<&str>) -> Self {
        self.build_id = value.and_then(KnownMetadata::from_build_id);
        self
    }
    pub fn with_task_id(mut self, value: Option<&str>) -> Self {
        self.task_id = value.and_then(KnownId::from_known);
        self
    }
    pub fn with_attempt_id(mut self, value: Option<&str>) -> Self {
        self.attempt_id = value.and_then(KnownId::from_known);
        self
    }

    /// Attach optional producer text. Oversized inputs are not copied, and the
    /// value is serialized only after the configured redactor succeeds.
    pub fn with_text(mut self, value: Option<&str>) -> Self {
        self.text = value
            .filter(|text| text.len() <= MAX_TEXT_INPUT_BYTES)
            .map(str::to_owned);
        self
    }

    fn has_extended_correlation(&self) -> bool {
        self.event_id.is_some()
            && self.component.is_some()
            && self.module_id.is_some()
            && self.artifact_id.is_some()
            && self.artifact_version.is_some()
            && self.binding_id.is_some()
            && self.binding_generation.is_some()
            && self.module_boot_id.is_some()
            && (self.task_id.is_none() && self.attempt_id.is_none()
                || self.operation_id.is_some()
                    && (self.attempt_id.is_none() || self.task_id.is_some()))
    }
}

#[derive(Serialize)]
struct WireRecord<'a> {
    // Schema 2 adds the optional retained binding generation. Schema 3 is
    // emitted only for records with the additional bounded correlation fields.
    schema_version: u8,
    // Identifies emission attempts, including dropped ones. Concurrent sends
    // can reach the writer in another order; this is not a journal cursor.
    sequence: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    occurred_at_unix_ms: Option<u64>,
    severity: Severity,
    kind: Kind,
    phase: Phase,
    #[serde(skip_serializing_if = "Option::is_none")]
    component: Option<Component>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<Code>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    link_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binding_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binding_generation: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    module_boot_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    event_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    module_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    artifact_version: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    build_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    task_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    attempt_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    redacted_text: Option<&'a str>,
}

struct Queued {
    bytes: Vec<u8>,
    observer_bytes: Option<Vec<u8>>,
}

impl Queued {
    fn pending_bytes(&self) -> u64 {
        self.bytes.len() as u64
            + self
                .observer_bytes
                .as_ref()
                .map_or(0, |bytes| bytes.len() as u64)
    }
}

#[derive(Default)]
struct Counters {
    enqueued_records: AtomicU64,
    written_records: AtomicU64,
    written_bytes: AtomicU64,
    dropped_records: AtomicU64,
    dropped_bytes: AtomicU64,
    pending_records: AtomicU64,
    pending_bytes: AtomicU64,
    sink_failures: AtomicU64,
    startup_failures: AtomicU64,
    observer_panics: AtomicU64,
    sink_failed: AtomicBool,
    idle_mutex: Mutex<()>,
    idle: Condvar,
}

struct Inner {
    config: Config,
    line_observer: Option<LineObserver>,
    scoped_filters: Mutex<Vec<ScopedFilter>>,
    sender: OnceLock<SyncSender<Queued>>,
    state: Arc<AtomicU8>,
    next_sequence: AtomicU64,
    counters: Arc<Counters>,
}

/// A second best-effort consumer for the already-serialized closed metadata
/// record. Implementations must use a nonblocking bounded handoff. The host
/// recorder is called on this crate's diagnostic writer thread, never on a
/// Store/kernel caller.
pub type LineObserver = Arc<dyn Fn(&[u8]) + Send + Sync + 'static>;

/// A closed metadata-only level used by the authenticated Store logging
/// control. The scope is matched before serialization, so a diagnostic filter
/// cannot change durable Operations or journal admission.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl FilterLevel {
    pub fn allows(self, severity: Severity) -> bool {
        match self {
            Self::Off => false,
            Self::Error => matches!(severity, Severity::Error),
            Self::Warn => matches!(severity, Severity::Error | Severity::Warn),
            Self::Info => matches!(severity, Severity::Error | Severity::Warn | Severity::Info),
            Self::Debug => matches!(
                severity,
                Severity::Error | Severity::Warn | Severity::Info | Severity::Debug
            ),
            Self::Trace => true,
        }
    }
}

/// Exact metadata identities attached to one Manager-owned diagnostic scope.
/// Task/Attempt are optional because taskless client, Operation, binding, and
/// module events are valid diagnostic sources. When present, Task/Attempt are
/// the current owner context proven by the Store; optional selectors narrow
/// that context and never widen it to another principal or task.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FilterScope {
    client_id: String,
    task_id: Option<String>,
    attempt_id: Option<String>,
    operation_id: Option<String>,
    binding_id: Option<String>,
    binding_generation: Option<u64>,
    module_id: Option<String>,
}

impl FilterScope {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_id: &str,
        task_id: Option<&str>,
        attempt_id: Option<&str>,
        operation_id: Option<&str>,
        binding_id: Option<&str>,
        binding_generation: Option<u64>,
        module_id: Option<&str>,
    ) -> Option<Self> {
        let known = |value: &str| KnownId::from_known(value).map(|id| id.0);
        let client_id = known(client_id)?;
        if task_id.is_some() != attempt_id.is_some() {
            return None;
        }
        let task_id = task_id.and_then(known);
        let attempt_id = attempt_id.and_then(known);
        let operation_id = operation_id.and_then(known);
        let binding_id = binding_id.and_then(known);
        let module_id = module_id.and_then(KnownMetadata::from_atom).map(|id| id.0);
        if binding_id.is_some() != binding_generation.is_some()
            || binding_generation.is_some_and(|generation| generation == 0)
        {
            return None;
        }
        Some(Self {
            client_id,
            task_id,
            attempt_id,
            operation_id,
            binding_id,
            binding_generation,
            module_id,
        })
    }

    fn matches(&self, record: &Record) -> bool {
        record
            .client_id
            .as_ref()
            .is_some_and(|id| id.0.as_str() == self.client_id.as_str())
            && self.task_id.as_deref().is_none_or(|expected| {
                record
                    .task_id
                    .as_ref()
                    .is_some_and(|id| id.0.as_str() == expected)
            })
            && self.attempt_id.as_deref().is_none_or(|expected| {
                record
                    .attempt_id
                    .as_ref()
                    .is_some_and(|id| id.0.as_str() == expected)
            })
            && self.operation_id.as_deref().is_none_or(|expected| {
                record
                    .operation_id
                    .as_ref()
                    .is_some_and(|id| id.0.as_str() == expected)
            })
            && self.binding_id.as_deref().is_none_or(|expected| {
                record
                    .binding_id
                    .as_ref()
                    .is_some_and(|id| id.0.as_str() == expected)
                    && self.binding_generation == record.binding_generation
            })
            && self.module_id.as_deref().is_none_or(|expected| {
                record
                    .module_id
                    .as_ref()
                    .is_some_and(|id| id.0.as_str() == expected)
            })
    }

    fn specificity(&self) -> (bool, bool, bool, bool) {
        (
            self.operation_id.is_some(),
            self.module_id.is_some(),
            self.binding_id.is_some(),
            self.task_id.is_some(),
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ScopedFilter {
    pub scope: FilterScope,
    pub level: FilterLevel,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilterError {
    TooManyScopes,
}

const MAX_SCOPED_FILTERS: usize = 64;

/// Cloneable, per-owner producer with no global state or mandatory install.
#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

impl Producer {
    /// Construct without starting a worker. The first enabled emission starts
    /// one blocking stderr writer on a dedicated thread.
    pub fn new(config: Config) -> Self {
        Self::with_line_observer(config, None)
    }

    /// Construct a producer that also offers each admitted closed metadata line
    /// to an optional observer. The existing stderr sink and its accounting
    /// remain independent; an observer panic is counted and cannot disable it.
    pub fn with_line_observer(config: Config, line_observer: Option<LineObserver>) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
                line_observer,
                scoped_filters: Mutex::new(Vec::new()),
                sender: OnceLock::new(),
                state: Arc::new(AtomicU8::new(STATE_UNSTARTED)),
                next_sequence: AtomicU64::new(0),
                counters: Arc::new(Counters::default()),
            }),
        }
    }

    /// Emit one bounded metadata record without waiting for the recorder.
    /// Disabled output is an intentional suppression and is excluded from loss
    /// counters. Rejected, oversized, and sink-failed records count as dropped.
    pub fn emit(&self, record: Record) -> EmitResult {
        if !self.inner.config.enabled {
            return EmitResult::Disabled;
        }
        if !self.allows(&record) {
            return EmitResult::Disabled;
        }
        let sequence = next_sequence(&self.inner.next_sequence);
        let include_correlation = record.has_extended_correlation();
        let text_capture_enabled = if self.inner.line_observer.is_some()
            && record.text.is_some()
            && let Some(policy) = self.inner.config.text_capture_policy.as_ref()
        {
            catch_unwind(AssertUnwindSafe(|| {
                policy(
                    record.severity,
                    record.kind,
                    record.module_id.as_ref().map(|value| value.0.as_str()),
                    record.client_id.as_ref().map(|value| value.0.as_str()),
                    record.operation_id.as_ref().map(|value| value.0.as_str()),
                )
            }))
            .unwrap_or(false)
        } else {
            false
        };
        let redacted_text = if text_capture_enabled {
            record.text.as_deref().and_then(|raw_text| {
                if raw_text.len() > MAX_TEXT_INPUT_BYTES {
                    return None;
                }
                let redactor = self.inner.config.text_redactor?;
                catch_unwind(AssertUnwindSafe(|| redactor(raw_text)))
                    .ok()
                    .flatten()
                    .filter(|text| text.len() <= MAX_REDACTED_TEXT_BYTES)
            })
        } else {
            None
        };
        let schema_version = if include_correlation { 3 } else { 2 };
        let mut wire = WireRecord {
            schema_version,
            sequence,
            occurred_at_unix_ms: unix_time_ms(),
            severity: record.severity,
            kind: record.kind,
            phase: record.phase,
            component: if include_correlation {
                record.component
            } else {
                None
            },
            code: record.code,
            client_id: record.client_id.as_ref().map(|id| id.0.as_str()),
            link_id: record.link_id.as_ref().map(|id| id.0.as_str()),
            binding_id: record.binding_id.as_ref().map(|id| id.0.as_str()),
            binding_generation: record.binding_generation,
            operation_id: record.operation_id.as_ref().map(|id| id.0.as_str()),
            module_boot_id: record.module_boot_id.as_ref().map(|id| id.0.as_str()),
            event_id: if include_correlation {
                record.event_id.as_ref().map(|id| id.0.as_str())
            } else {
                None
            },
            module_id: if include_correlation {
                record.module_id.as_ref().map(|id| id.0.as_str())
            } else {
                None
            },
            artifact_id: if include_correlation {
                record.artifact_id.as_ref().map(|id| id.0.as_str())
            } else {
                None
            },
            artifact_version: if include_correlation {
                record.artifact_version.as_ref().map(|id| id.0.as_str())
            } else {
                None
            },
            build_id: if include_correlation {
                record.build_id.as_ref().map(|id| id.0.as_str())
            } else {
                None
            },
            task_id: if include_correlation {
                record.task_id.as_ref().map(|id| id.0.as_str())
            } else {
                None
            },
            attempt_id: if include_correlation {
                record.attempt_id.as_ref().map(|id| id.0.as_str())
            } else {
                None
            },
            redacted_text: None,
        };
        let mut bytes = match serde_json::to_vec(&wire) {
            Ok(bytes) => bytes,
            Err(_) => {
                record_drop(&self.inner.counters, 1, 0);
                return EmitResult::Dropped(DropReason::SerializationFailure);
            }
        };
        let line_bytes = bytes.len().saturating_add(1);
        if line_bytes > self.inner.config.max_record_bytes {
            record_drop(&self.inner.counters, 1, line_bytes as u64);
            return EmitResult::Dropped(DropReason::RecordTooLarge);
        }
        bytes.push(b'\n');
        let byte_count = bytes.len() as u64;

        // The stderr line stays metadata-only. The observer receives the
        // schema-4 variant only after the live scope policy has opted in and
        // Atlas has redacted the bounded input. Raw text is never serialized.
        let observer_bytes = if let Some(redacted_text) = redacted_text.as_deref() {
            wire.schema_version = 4;
            wire.redacted_text = Some(redacted_text);
            let mut observer_bytes = match serde_json::to_vec(&wire) {
                Ok(bytes) => bytes,
                Err(_) => {
                    record_drop(&self.inner.counters, 1, byte_count);
                    return EmitResult::Dropped(DropReason::SerializationFailure);
                }
            };
            let observer_line_bytes = observer_bytes.len().saturating_add(1);
            if observer_line_bytes > self.inner.config.max_record_bytes {
                None
            } else {
                observer_bytes.push(b'\n');
                Some(observer_bytes)
            }
        } else {
            None
        };
        let queued_bytes = (bytes.len() as u64).saturating_add(
            observer_bytes
                .as_ref()
                .map_or(0, |bytes| bytes.len() as u64),
        );
        if bytes.len() > self.inner.config.max_record_bytes
            || observer_bytes
                .as_ref()
                .is_some_and(|bytes| bytes.len() > self.inner.config.max_record_bytes)
        {
            record_drop(&self.inner.counters, 1, byte_count);
            return EmitResult::Dropped(DropReason::RecordTooLarge);
        }

        if self.inner.counters.sink_failed.load(Ordering::Acquire) {
            record_drop(&self.inner.counters, 1, queued_bytes);
            return EmitResult::Dropped(DropReason::RecorderUnavailable);
        }
        let sender = match self.ensure_writer() {
            Ok(sender) => sender,
            Err(reason) => {
                record_drop(&self.inner.counters, 1, queued_bytes);
                return EmitResult::Dropped(reason);
            }
        };

        if !reserve(
            &self.inner.counters.pending_records,
            self.inner.config.queue_records as u64,
            1,
        ) {
            record_drop(&self.inner.counters, 1, queued_bytes);
            return EmitResult::Dropped(DropReason::QueueFull);
        }
        if !reserve(
            &self.inner.counters.pending_bytes,
            self.inner.config.queue_bytes as u64,
            queued_bytes,
        ) {
            atomic_sub(&self.inner.counters.pending_records, 1);
            record_drop(&self.inner.counters, 1, queued_bytes);
            return EmitResult::Dropped(DropReason::QueueFull);
        }
        let queued = Queued {
            bytes,
            observer_bytes,
        };
        match sender.try_send(queued) {
            Ok(()) => {
                atomic_add(&self.inner.counters.enqueued_records, 1);
                EmitResult::Queued
            }
            Err(TrySendError::Full(queued)) => {
                finish_pending(&self.inner.counters, queued_bytes);
                record_drop(&self.inner.counters, 1, queued_bytes);
                drop(queued);
                EmitResult::Dropped(DropReason::QueueFull)
            }
            Err(TrySendError::Disconnected(queued)) => {
                finish_pending(&self.inner.counters, queued_bytes);
                mark_failed(&self.inner.counters, &self.inner.state);
                record_drop(&self.inner.counters, 1, queued_bytes);
                drop(queued);
                EmitResult::Dropped(DropReason::RecorderUnavailable)
            }
        }
    }

    /// Read a best-effort atomic counter snapshot. Fields are monotonic counters
    /// plus current in-flight bounds; snapshots are not a transactional cut.
    pub fn stats(&self) -> Stats {
        let state = self.inner.state.load(Ordering::Acquire);
        Stats {
            enabled: self.inner.config.enabled,
            sink_state: if !self.inner.config.enabled {
                SinkState::Disabled
            } else {
                match state {
                    STATE_UNSTARTED => SinkState::NotStarted,
                    STATE_STARTING => SinkState::Starting,
                    STATE_READY => SinkState::Ready,
                    _ => SinkState::Failed,
                }
            },
            enqueued_records: load(&self.inner.counters.enqueued_records),
            written_records: load(&self.inner.counters.written_records),
            written_bytes: load(&self.inner.counters.written_bytes),
            dropped_records: load(&self.inner.counters.dropped_records),
            dropped_bytes: load(&self.inner.counters.dropped_bytes),
            pending_records: load(&self.inner.counters.pending_records),
            pending_bytes: load(&self.inner.counters.pending_bytes),
            sink_failures: load(&self.inner.counters.sink_failures),
            startup_failures: load(&self.inner.counters.startup_failures),
            observer_panics: load(&self.inner.counters.observer_panics),
        }
    }

    /// Atomically replace the bounded set of exact Store-owned scopes. The
    /// replacement is in-memory only and therefore safe to call after the
    /// durable meta transaction commits or while a recorder is idle.
    pub fn replace_scoped_filters(&self, filters: Vec<ScopedFilter>) -> Result<(), FilterError> {
        if filters.len() > MAX_SCOPED_FILTERS {
            return Err(FilterError::TooManyScopes);
        }
        let mut current = self
            .inner
            .scoped_filters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *current = filters;
        Ok(())
    }

    pub fn scoped_filter_count(&self) -> usize {
        self.inner
            .scoped_filters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    fn allows(&self, record: &Record) -> bool {
        let filters = self
            .inner
            .scoped_filters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        filters
            .iter()
            .filter(|filter| filter.scope.matches(record))
            .max_by_key(|filter| filter.scope.specificity())
            .map_or(true, |filter| filter.level.allows(record.severity))
    }

    /// Wait for currently admitted diagnostics and line-observer callbacks to
    /// finish. Call only after all event producers have stopped; this is not a
    /// barrier against a concurrent future `emit`.
    pub fn wait_for_idle(&self, timeout: Duration) -> bool {
        let Some(deadline) = Instant::now().checked_add(timeout) else {
            return false;
        };
        let mut guard = self
            .inner
            .counters
            .idle_mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        while load(&self.inner.counters.pending_records) != 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            let waited = self.inner.counters.idle.wait_timeout(guard, remaining);
            let (next_guard, timeout_result) =
                waited.unwrap_or_else(std::sync::PoisonError::into_inner);
            guard = next_guard;
            if timeout_result.timed_out() && load(&self.inner.counters.pending_records) != 0 {
                return false;
            }
        }
        true
    }

    fn ensure_writer(&self) -> Result<&SyncSender<Queued>, DropReason> {
        match self.inner.state.load(Ordering::Acquire) {
            STATE_READY => {
                return self
                    .inner
                    .sender
                    .get()
                    .ok_or(DropReason::RecorderUnavailable);
            }
            STATE_STARTING => return Err(DropReason::RecorderStarting),
            STATE_FAILED => return Err(DropReason::RecorderUnavailable),
            _ => {}
        }
        if self
            .inner
            .state
            .compare_exchange(
                STATE_UNSTARTED,
                STATE_STARTING,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return match self.inner.state.load(Ordering::Acquire) {
                STATE_READY => self
                    .inner
                    .sender
                    .get()
                    .ok_or(DropReason::RecorderUnavailable),
                STATE_FAILED => Err(DropReason::RecorderUnavailable),
                _ => Err(DropReason::RecorderStarting),
            };
        }

        let (sender, receiver) = sync_channel(self.inner.config.queue_records);
        let counters = Arc::clone(&self.inner.counters);
        let state = Arc::clone(&self.inner.state);
        let line_observer = self.inner.line_observer.clone();
        let spawned = thread::Builder::new()
            .name("swarm-telemetry-stderr".to_owned())
            .spawn(move || {
                let mut active_bytes = None;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    writer_loop(
                        &receiver,
                        &counters,
                        &state,
                        line_observer,
                        &mut active_bytes,
                    )
                }));
                if result.is_err() {
                    mark_failed(&counters, &state);
                    if let Some(bytes) = active_bytes.take() {
                        finish_pending(&counters, bytes);
                        record_drop(&counters, 1, bytes);
                    }
                    // Keep the failed worker parked on recv and account every
                    // admitted record until producers are dropped. No retry timer.
                    while let Ok(item) = receiver.recv() {
                        let bytes = item.pending_bytes();
                        finish_pending(&counters, bytes);
                        record_drop(&counters, 1, bytes);
                    }
                }
            });
        if spawned.is_err() {
            atomic_add(&self.inner.counters.startup_failures, 1);
            mark_failed(&self.inner.counters, &self.inner.state);
            return Err(DropReason::RecorderUnavailable);
        }
        if self.inner.sender.set(sender).is_err() {
            mark_failed(&self.inner.counters, &self.inner.state);
            return Err(DropReason::RecorderUnavailable);
        }
        if self
            .inner
            .state
            .compare_exchange(
                STATE_STARTING,
                STATE_READY,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(DropReason::RecorderUnavailable);
        }
        self.inner
            .sender
            .get()
            .ok_or(DropReason::RecorderUnavailable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    QueueFull,
    RecordTooLarge,
    RecorderStarting,
    RecorderUnavailable,
    SerializationFailure,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EmitResult {
    Queued,
    Disabled,
    Dropped(DropReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkState {
    Disabled,
    NotStarted,
    Starting,
    Ready,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Stats {
    pub enabled: bool,
    pub sink_state: SinkState,
    pub enqueued_records: u64,
    pub written_records: u64,
    pub written_bytes: u64,
    pub dropped_records: u64,
    pub dropped_bytes: u64,
    pub pending_records: u64,
    pub pending_bytes: u64,
    pub sink_failures: u64,
    pub startup_failures: u64,
    /// Observer callbacks that panicked. Recorder queue/sink losses are
    /// counted by the observer itself and reported separately.
    pub observer_panics: u64,
}

fn writer_loop(
    receiver: &Receiver<Queued>,
    counters: &Counters,
    state: &AtomicU8,
    line_observer: Option<LineObserver>,
    active_bytes: &mut Option<u64>,
) {
    let stderr = io::stderr();
    let mut sink_failed = false;
    while let Ok(item) = receiver.recv() {
        let byte_count = item.bytes.len() as u64;
        let pending_byte_count = item.pending_bytes();
        *active_bytes = Some(pending_byte_count);
        if let Some(observer) = &line_observer
            && catch_unwind(AssertUnwindSafe(|| {
                observer(item.observer_bytes.as_deref().unwrap_or(&item.bytes))
            }))
            .is_err()
        {
            atomic_add(&counters.observer_panics, 1);
        }
        if sink_failed {
            finish_pending(counters, pending_byte_count);
            record_drop(counters, 1, pending_byte_count);
            *active_bytes = None;
            continue;
        }
        let write_result = {
            let mut sink = stderr.lock();
            sink.write_all(&item.bytes).and_then(|_| sink.flush())
        };
        match write_result {
            Ok(()) => {
                atomic_add(&counters.written_records, 1);
                atomic_add(&counters.written_bytes, byte_count);
                finish_pending(counters, pending_byte_count);
            }
            Err(_) => {
                mark_failed(counters, state);
                sink_failed = true;
                finish_pending(counters, pending_byte_count);
                record_drop(counters, 1, pending_byte_count);
            }
        }
        *active_bytes = None;
    }
}

fn mark_failed(counters: &Counters, state: &AtomicU8) {
    state.store(STATE_FAILED, Ordering::Release);
    if !counters.sink_failed.swap(true, Ordering::AcqRel) {
        atomic_add(&counters.sink_failures, 1);
    }
}

fn record_drop(counters: &Counters, records: u64, bytes: u64) {
    atomic_add(&counters.dropped_records, records);
    atomic_add(&counters.dropped_bytes, bytes);
}

fn finish_pending(counters: &Counters, bytes: u64) {
    let _guard = counters
        .idle_mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    atomic_sub(&counters.pending_records, 1);
    atomic_sub(&counters.pending_bytes, bytes);
    if load(&counters.pending_records) == 0 {
        counters.idle.notify_all();
    }
}

fn reserve(counter: &AtomicU64, capacity: u64, amount: u64) -> bool {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        if amount > capacity.saturating_sub(current) {
            return false;
        }
        match counter.compare_exchange_weak(
            current,
            current + amount,
            Ordering::AcqRel,
            Ordering::Relaxed,
        ) {
            Ok(_) => return true,
            Err(actual) => current = actual,
        }
    }
}

fn atomic_add(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_add(amount))
    });
}

fn atomic_sub(counter: &AtomicU64, amount: u64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
        Some(value.saturating_sub(amount))
    });
}

fn load(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}

fn next_sequence(counter: &AtomicU64) -> u64 {
    let previous = counter
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        })
        .unwrap_or_else(|value| value);
    previous.saturating_add(1)
}

fn unix_time_ms() -> Option<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(elapsed.as_millis()).ok()
}
