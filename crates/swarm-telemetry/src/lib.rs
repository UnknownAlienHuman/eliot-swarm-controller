//! Bounded, metadata-only diagnostic producer.
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
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
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
const MAX_ID_BYTES: usize = 128;

/// Validated local-recorder configuration. The optional producer is local to
/// its caller; this crate installs no global subscriber or mandatory backend.
#[derive(Clone, Debug)]
pub struct Config {
    enabled: bool,
    queue_records: usize,
    queue_bytes: usize,
    max_record_bytes: usize,
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
        })
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: true,
            queue_records: DEFAULT_QUEUE_RECORDS,
            queue_bytes: DEFAULT_QUEUE_BYTES,
            max_record_bytes: DEFAULT_MAX_RECORD_BYTES,
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

/// Input record. Optional identifiers are included only when the caller has
/// the corresponding known value; no values are synthesized for missing IDs.
#[derive(Clone, Debug)]
pub struct Record {
    severity: Severity,
    kind: Kind,
    phase: Phase,
    code: Option<Code>,
    client_id: Option<KnownId>,
    link_id: Option<KnownId>,
    binding_id: Option<KnownId>,
    operation_id: Option<KnownId>,
    module_boot_id: Option<KnownId>,
}

impl Record {
    pub fn new(severity: Severity, kind: Kind, phase: Phase) -> Self {
        Self {
            severity,
            kind,
            phase,
            code: None,
            client_id: None,
            link_id: None,
            binding_id: None,
            operation_id: None,
            module_boot_id: None,
        }
    }

    pub fn with_code(mut self, value: Option<Code>) -> Self {
        self.code = value;
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
    pub fn with_operation_id(mut self, value: Option<&str>) -> Self {
        self.operation_id = value.and_then(KnownId::from_known);
        self
    }
    pub fn with_module_boot_id(mut self, value: Option<&str>) -> Self {
        self.module_boot_id = value.and_then(KnownId::from_known);
        self
    }
}

#[derive(Serialize)]
struct WireRecord<'a> {
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
    code: Option<Code>,
    #[serde(skip_serializing_if = "Option::is_none")]
    client_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    link_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    binding_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    module_boot_id: Option<&'a str>,
}

struct Queued {
    bytes: Vec<u8>,
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
    sink_failed: AtomicBool,
}

struct Inner {
    config: Config,
    sender: OnceLock<SyncSender<Queued>>,
    state: Arc<AtomicU8>,
    next_sequence: AtomicU64,
    counters: Arc<Counters>,
}

/// Cloneable, per-owner producer with no global state or mandatory install.
#[derive(Clone)]
pub struct Producer {
    inner: Arc<Inner>,
}

impl Producer {
    /// Construct without starting a worker. The first enabled emission starts
    /// one blocking stderr writer on a dedicated thread.
    pub fn new(config: Config) -> Self {
        Self {
            inner: Arc::new(Inner {
                config,
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
        let sequence = next_sequence(&self.inner.next_sequence);
        let wire = WireRecord {
            schema_version: 1,
            sequence,
            occurred_at_unix_ms: unix_time_ms(),
            severity: record.severity,
            kind: record.kind,
            phase: record.phase,
            code: record.code,
            client_id: record.client_id.as_ref().map(|id| id.0.as_str()),
            link_id: record.link_id.as_ref().map(|id| id.0.as_str()),
            binding_id: record.binding_id.as_ref().map(|id| id.0.as_str()),
            operation_id: record.operation_id.as_ref().map(|id| id.0.as_str()),
            module_boot_id: record.module_boot_id.as_ref().map(|id| id.0.as_str()),
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

        if self.inner.counters.sink_failed.load(Ordering::Acquire) {
            record_drop(&self.inner.counters, 1, byte_count);
            return EmitResult::Dropped(DropReason::RecorderUnavailable);
        }
        let sender = match self.ensure_writer() {
            Ok(sender) => sender,
            Err(reason) => {
                record_drop(&self.inner.counters, 1, byte_count);
                return EmitResult::Dropped(reason);
            }
        };

        if !reserve(
            &self.inner.counters.pending_records,
            self.inner.config.queue_records as u64,
            1,
        ) {
            record_drop(&self.inner.counters, 1, byte_count);
            return EmitResult::Dropped(DropReason::QueueFull);
        }
        if !reserve(
            &self.inner.counters.pending_bytes,
            self.inner.config.queue_bytes as u64,
            byte_count,
        ) {
            atomic_sub(&self.inner.counters.pending_records, 1);
            record_drop(&self.inner.counters, 1, byte_count);
            return EmitResult::Dropped(DropReason::QueueFull);
        }
        let queued = Queued { bytes };
        match sender.try_send(queued) {
            Ok(()) => {
                atomic_add(&self.inner.counters.enqueued_records, 1);
                EmitResult::Queued
            }
            Err(TrySendError::Full(queued)) => {
                finish_pending(&self.inner.counters, byte_count);
                record_drop(&self.inner.counters, 1, byte_count);
                drop(queued);
                EmitResult::Dropped(DropReason::QueueFull)
            }
            Err(TrySendError::Disconnected(queued)) => {
                finish_pending(&self.inner.counters, byte_count);
                mark_failed(&self.inner.counters, &self.inner.state);
                record_drop(&self.inner.counters, 1, byte_count);
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
        }
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
        let spawned = thread::Builder::new()
            .name("swarm-telemetry-stderr".to_owned())
            .spawn(move || {
                let mut active_bytes = None;
                let result = catch_unwind(AssertUnwindSafe(|| {
                    writer_loop(&receiver, &counters, &state, &mut active_bytes)
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
                        let bytes = item.bytes.len() as u64;
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
}

fn writer_loop(
    receiver: &Receiver<Queued>,
    counters: &Counters,
    state: &AtomicU8,
    active_bytes: &mut Option<u64>,
) {
    let stderr = io::stderr();
    let mut sink_failed = false;
    while let Ok(item) = receiver.recv() {
        let byte_count = item.bytes.len() as u64;
        *active_bytes = Some(byte_count);
        if sink_failed {
            finish_pending(counters, byte_count);
            record_drop(counters, 1, byte_count);
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
                finish_pending(counters, byte_count);
            }
            Err(_) => {
                mark_failed(counters, state);
                sink_failed = true;
                finish_pending(counters, byte_count);
                record_drop(counters, 1, byte_count);
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
    atomic_sub(&counters.pending_records, 1);
    atomic_sub(&counters.pending_bytes, bytes);
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
