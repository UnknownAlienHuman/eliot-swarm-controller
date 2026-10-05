//! Explicit, bounded resource samples for already selected process identities.
//!
//! This module does not enumerate processes or start a worker. Call
//! `sample_explicit_tick` only from an enabled observer snapshot/follow tick,
//! with a bounded list of identities already selected by the trusted caller.
//! The output contains no image path, image hash, owner record, arguments,
//! environment, or error text.

use serde::Serialize;
use serde_json::Value;
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(windows)]
use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

#[cfg(windows)]
use swarm_process::{
    module_child_belongs_to_owner, process_birth_identity, process_image_identity,
};

#[cfg(windows)]
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_ACCESS_DENIED, ERROR_INVALID_PARAMETER, FILETIME, GetLastError, HANDLE,
        WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    System::{
        ProcessStatus::{K32GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS},
        Threading::{
            ALL_PROCESSOR_GROUPS, GetActiveProcessorCount, GetProcessTimes, OpenProcess,
            PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE, WaitForSingleObject,
        },
    },
};

pub const MAX_SELECTED_PROCESSES: usize = 8;

/// This is a caller-selected classification, not an OS ownership claim.
/// Child classifications are emitted only after the existing process package
/// confirms membership in the supplied module owner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessRole {
    Host,
    Helper,
    Adapter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotStatus {
    Disabled,
    Empty,
    Observed,
    Partial,
    Unavailable,
}

/// Closed, non-sensitive error categories. Native error text and codes are
/// deliberately not returned to the observer.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnavailableReason {
    UnsupportedPlatform,
    InvalidObservation,
    ProcessExited,
    AccessDenied,
    IdentityUnavailable,
    IdentityChanged,
    MembershipUnavailable,
    NotOwned,
    ProcessQueryFailed,
    SelectionLimitExceeded,
    DuplicateProcess,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessResourceSample {
    pub role: ProcessRole,
    pub status: SampleStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    /// The observed process birth time distinguishes PID reuse without
    /// disclosing an executable path or hash.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub process_birth_filetime: Option<u64>,
    /// Windows working-set bytes (resident set, including shared mapped pages).
    /// Do not sum the same shared native process once per hosted session.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resident_bytes: Option<u64>,
    /// Cumulative user + kernel process CPU time in 100-nanosecond units.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cumulative_cpu_100ns: Option<u64>,
    /// Present only after two successful samples of the same exact process
    /// identity with a monotonic interval and known logical processor count.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_percent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cpu_interval_ns: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logical_processor_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<UnavailableReason>,
}

impl ProcessResourceSample {
    /// Preserve an identity/query-construction failure as an explicit
    /// unavailable sample. Missing values stay absent; callers must not replace
    /// them with zero readings.
    pub fn unavailable(
        role: ProcessRole,
        pid: Option<u32>,
        process_birth_filetime: Option<u64>,
        reason: UnavailableReason,
    ) -> Self {
        Self {
            role,
            status: SampleStatus::Unavailable,
            pid,
            process_birth_filetime,
            resident_bytes: None,
            cumulative_cpu_100ns: None,
            cpu_percent: None,
            cpu_interval_ns: None,
            logical_processor_count: None,
            sampled_at_unix_ms: unix_ms(),
            unavailable_reason: Some(reason),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleStatus {
    Observed,
    Unavailable,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProcessMetricsSnapshot {
    pub status: SnapshotStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sampled_at_unix_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unavailable_reason: Option<UnavailableReason>,
    pub samples: Vec<ProcessResourceSample>,
}

#[derive(Clone, Debug)]
pub struct ObservedProcess {
    role: ProcessRole,
    pid: u32,
    #[cfg(windows)]
    identity: ProcessIdentity,
    #[cfg(windows)]
    identity_record: Value,
    #[cfg(windows)]
    owner_record: Option<Value>,
}

#[cfg(windows)]
#[derive(Clone, Debug)]
struct ProcessIdentity {
    pid: u32,
    creation_filetime: u64,
    image_path: String,
    image_sha256: String,
}

impl ObservedProcess {
    /// Build a host selection from the exact `process_image_identity` record
    /// already observed by the host/supervisor. The record is re-read and its
    /// birth identity is cross-checked before the selection is retained.
    pub fn host(image_identity: &Value) -> std::result::Result<Self, UnavailableReason> {
        #[cfg(windows)]
        {
            let identity = parse_image_identity(image_identity)?;
            verify_process_identity(&identity, image_identity)?;
            Ok(Self {
                role: ProcessRole::Host,
                pid: identity.pid,
                identity,
                identity_record: image_identity.clone(),
                owner_record: None,
            })
        }
        #[cfg(not(windows))]
        {
            let _ = image_identity;
            Err(UnavailableReason::UnsupportedPlatform)
        }
    }

    /// Build a helper/adapter selection only from an exact live image receipt
    /// that the existing module-membership API confirms belongs to the exact
    /// manager-owned module process group. The role is descriptive metadata;
    /// membership is independently checked on every sample.
    pub fn module_child(
        role: ProcessRole,
        owner_record: &Value,
        image_identity: &Value,
    ) -> std::result::Result<Self, UnavailableReason> {
        #[cfg(windows)]
        {
            if role == ProcessRole::Host || bounded_json_len(owner_record) > 64 * 1024 {
                return Err(UnavailableReason::InvalidObservation);
            }
            let identity = parse_image_identity(image_identity)?;
            let belongs = module_child_belongs_to_owner(owner_record, image_identity)
                .map_err(membership_error)?;
            if !belongs {
                return Err(UnavailableReason::NotOwned);
            }
            Ok(Self {
                role,
                pid: identity.pid,
                identity,
                identity_record: image_identity.clone(),
                owner_record: Some(owner_record.clone()),
            })
        }
        #[cfg(not(windows))]
        {
            let _ = (role, owner_record, image_identity);
            Err(UnavailableReason::UnsupportedPlatform)
        }
    }
}

#[derive(Default)]
pub struct ProcessMetricsSampler {
    #[cfg(windows)]
    baselines: HashMap<u32, CpuBaseline>,
}

impl ProcessMetricsSampler {
    /// Sample only the explicitly supplied identities for this enabled tick.
    /// This method has no timer, thread, process enumeration, or persistent
    /// state beyond a bounded CPU baseline for the current selection. The
    /// caller must check its enable gate before constructing ObservedProcess,
    /// because those constructors revalidate process identity.
    pub fn sample_explicit_tick(
        &mut self,
        enabled: bool,
        selected: &[ObservedProcess],
    ) -> ProcessMetricsSnapshot {
        if !enabled {
            #[cfg(windows)]
            self.baselines.clear();
            return ProcessMetricsSnapshot {
                status: SnapshotStatus::Disabled,
                sampled_at_unix_ms: None,
                unavailable_reason: None,
                samples: Vec::new(),
            };
        }
        if selected.is_empty() {
            #[cfg(windows)]
            self.baselines.clear();
            return ProcessMetricsSnapshot {
                status: SnapshotStatus::Empty,
                sampled_at_unix_ms: unix_ms(),
                unavailable_reason: None,
                samples: Vec::new(),
            };
        }
        if selected.len() > MAX_SELECTED_PROCESSES {
            #[cfg(windows)]
            self.baselines.clear();
            return unavailable_snapshot(UnavailableReason::SelectionLimitExceeded);
        }

        let mut seen = std::collections::HashSet::with_capacity(selected.len());
        if selected.iter().any(|subject| !seen.insert(subject.pid)) {
            #[cfg(windows)]
            self.baselines.clear();
            return unavailable_snapshot(UnavailableReason::DuplicateProcess);
        }

        #[cfg(windows)]
        {
            self.sample_windows(selected)
        }
        #[cfg(not(windows))]
        {
            let samples = selected
                .iter()
                .map(|subject| unavailable_sample(subject, UnavailableReason::UnsupportedPlatform))
                .collect();
            ProcessMetricsSnapshot {
                status: SnapshotStatus::Unavailable,
                sampled_at_unix_ms: unix_ms(),
                unavailable_reason: Some(UnavailableReason::UnsupportedPlatform),
                samples,
            }
        }
    }

    #[cfg(windows)]
    fn sample_windows(&mut self, selected: &[ObservedProcess]) -> ProcessMetricsSnapshot {
        let selected_pids: HashSet<u32> = selected.iter().map(|subject| subject.pid).collect();
        self.baselines.retain(|pid, _| selected_pids.contains(pid));
        let logical_processors = logical_processor_count();
        let mut samples = Vec::with_capacity(selected.len());

        for subject in selected {
            match sample_process(subject) {
                Ok(raw) => {
                    let key = ProcessKey::new(subject);
                    let previous = self.baselines.remove(&subject.pid);
                    let (cpu_percent, cpu_interval_ns) = previous
                        .filter(|baseline| baseline.key == key)
                        .and_then(|baseline| {
                            let elapsed =
                                raw.sampled_at.checked_duration_since(baseline.sampled_at)?;
                            let percent = logical_processors.and_then(|logical_processors| {
                                cpu_percent(
                                    baseline.cumulative_cpu_100ns,
                                    raw.cumulative_cpu_100ns,
                                    elapsed,
                                    logical_processors,
                                )
                            });
                            Some((percent, u64::try_from(elapsed.as_nanos()).ok()))
                        })
                        .unwrap_or((None, None));
                    self.baselines.insert(
                        subject.pid,
                        CpuBaseline {
                            key,
                            sampled_at: raw.sampled_at,
                            cumulative_cpu_100ns: raw.cumulative_cpu_100ns,
                        },
                    );
                    samples.push(ProcessResourceSample {
                        role: subject.role,
                        status: SampleStatus::Observed,
                        pid: Some(subject.pid),
                        process_birth_filetime: Some(subject.identity.creation_filetime),
                        resident_bytes: Some(raw.resident_bytes),
                        cumulative_cpu_100ns: Some(raw.cumulative_cpu_100ns),
                        cpu_percent,
                        cpu_interval_ns,
                        logical_processor_count: logical_processors,
                        sampled_at_unix_ms: unix_ms(),
                        unavailable_reason: None,
                    });
                }
                Err(reason) => {
                    self.baselines.remove(&subject.pid);
                    samples.push(unavailable_sample(subject, reason));
                }
            }
        }

        let observed = samples
            .iter()
            .filter(|sample| sample.status == SampleStatus::Observed)
            .count();
        let status = if observed == samples.len() {
            SnapshotStatus::Observed
        } else if observed == 0 {
            SnapshotStatus::Unavailable
        } else {
            SnapshotStatus::Partial
        };
        ProcessMetricsSnapshot {
            status,
            sampled_at_unix_ms: unix_ms(),
            unavailable_reason: None,
            samples,
        }
    }
}

fn unavailable_snapshot(reason: UnavailableReason) -> ProcessMetricsSnapshot {
    ProcessMetricsSnapshot {
        status: SnapshotStatus::Unavailable,
        sampled_at_unix_ms: unix_ms(),
        unavailable_reason: Some(reason),
        samples: Vec::new(),
    }
}

fn unavailable_sample(
    subject: &ObservedProcess,
    reason: UnavailableReason,
) -> ProcessResourceSample {
    #[cfg(windows)]
    let process_birth_filetime = Some(subject.identity.creation_filetime);
    #[cfg(not(windows))]
    let process_birth_filetime = None;
    ProcessResourceSample::unavailable(
        subject.role,
        Some(subject.pid),
        process_birth_filetime,
        reason,
    )
}

fn unix_ms() -> Option<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|value| value.as_millis().min(u64::MAX as u128) as u64)
}

#[cfg(windows)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ProcessKey {
    role: ProcessRole,
    pid: u32,
    creation_filetime: u64,
    image_path: String,
    image_sha256: String,
}

#[cfg(windows)]
impl ProcessKey {
    fn new(subject: &ObservedProcess) -> Self {
        Self {
            role: subject.role,
            pid: subject.identity.pid,
            creation_filetime: subject.identity.creation_filetime,
            image_path: subject.identity.image_path.clone(),
            image_sha256: subject.identity.image_sha256.clone(),
        }
    }
}

#[cfg(windows)]
struct CpuBaseline {
    key: ProcessKey,
    sampled_at: Instant,
    cumulative_cpu_100ns: u64,
}

#[cfg(windows)]
struct RawSample {
    cumulative_cpu_100ns: u64,
    resident_bytes: u64,
    sampled_at: Instant,
}

#[cfg(windows)]
struct OwnedProcessHandle(HANDLE);

#[cfg(windows)]
impl Drop for OwnedProcessHandle {
    fn drop(&mut self) {
        // SAFETY: this handle is owned by this wrapper and came from OpenProcess.
        unsafe {
            CloseHandle(self.0);
        }
    }
}

#[cfg(windows)]
fn sample_process(subject: &ObservedProcess) -> std::result::Result<RawSample, UnavailableReason> {
    verify_subject(subject)?;
    // SAFETY: request only query and synchronize rights; the returned handle
    // is wrapped immediately and remains open for all process queries below.
    let process = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            subject.pid,
        )
    };
    if process.is_null() {
        return Err(open_process_error());
    }
    let process = OwnedProcessHandle(process);
    ensure_live(process.0)?;
    let (birth_before, kernel_before, user_before) = process_times(process.0)?;
    if birth_before != subject.identity.creation_filetime {
        return Err(UnavailableReason::IdentityChanged);
    }
    let resident_bytes = resident_working_set(process.0)?;
    let (birth_after, kernel_after, user_after) = process_times(process.0)?;
    if birth_after != subject.identity.creation_filetime
        || kernel_after < kernel_before
        || user_after < user_before
    {
        return Err(UnavailableReason::IdentityChanged);
    }
    ensure_live(process.0)?;
    verify_subject(subject)?;
    let cumulative_cpu_100ns = kernel_after
        .checked_add(user_after)
        .ok_or(UnavailableReason::ProcessQueryFailed)?;
    Ok(RawSample {
        cumulative_cpu_100ns,
        resident_bytes,
        sampled_at: Instant::now(),
    })
}

#[cfg(windows)]
fn verify_subject(subject: &ObservedProcess) -> std::result::Result<(), UnavailableReason> {
    if let Some(owner_record) = subject.owner_record.as_ref() {
        // This shared API rechecks the exact image/birth identity and module
        // membership itself, so do not duplicate its executable hash read.
        let belongs = module_child_belongs_to_owner(owner_record, &subject.identity_record)
            .map_err(membership_error)?;
        if !belongs {
            return Err(UnavailableReason::NotOwned);
        }
    } else {
        let current = process_image_identity(subject.pid).map_err(identity_error)?;
        if current != subject.identity_record {
            return Err(UnavailableReason::IdentityChanged);
        }
        let birth = process_birth_identity(subject.pid).map_err(identity_error)?;
        verify_birth_record(
            subject.pid,
            subject.identity.creation_filetime,
            birth.as_ref(),
        )?;
    }
    Ok(())
}

#[cfg(windows)]
fn verify_process_identity(
    identity: &ProcessIdentity,
    identity_record: &Value,
) -> std::result::Result<(), UnavailableReason> {
    let current = process_image_identity(identity.pid).map_err(identity_error)?;
    if current != *identity_record {
        return Err(UnavailableReason::IdentityChanged);
    }
    let birth = process_birth_identity(identity.pid).map_err(identity_error)?;
    verify_birth_record(identity.pid, identity.creation_filetime, birth.as_ref())
}

#[cfg(windows)]
fn verify_birth_record(
    pid: u32,
    expected_creation_filetime: u64,
    record: Option<&Value>,
) -> std::result::Result<(), UnavailableReason> {
    let record = record.ok_or(UnavailableReason::ProcessExited)?;
    if record.get("platform").and_then(Value::as_str) != Some("windows")
        || record.get("pid").and_then(Value::as_u64) != Some(u64::from(pid))
        || record
            .get("creation_filetime")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<u64>().ok())
            != Some(expected_creation_filetime)
    {
        return Err(UnavailableReason::IdentityChanged);
    }
    Ok(())
}

#[cfg(windows)]
fn parse_image_identity(value: &Value) -> std::result::Result<ProcessIdentity, UnavailableReason> {
    let object = value
        .as_object()
        .filter(|object| {
            object.len() == 4
                && ["pid", "creation_filetime", "image_path", "image_sha256"]
                    .iter()
                    .all(|key| object.contains_key(*key))
        })
        .ok_or(UnavailableReason::InvalidObservation)?;
    let pid = object
        .get("pid")
        .and_then(Value::as_u64)
        .filter(|pid| *pid > 0)
        .and_then(|pid| u32::try_from(pid).ok())
        .ok_or(UnavailableReason::InvalidObservation)?;
    let creation_filetime_text = object
        .get("creation_filetime")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty() && value.len() <= 20)
        .ok_or(UnavailableReason::InvalidObservation)?;
    let creation_filetime = creation_filetime_text
        .parse::<u64>()
        .ok()
        .filter(|value| value.to_string() == creation_filetime_text)
        .ok_or(UnavailableReason::InvalidObservation)?;
    let image_path = object
        .get("image_path")
        .and_then(Value::as_str)
        .filter(|value| {
            !value.is_empty() && value.len() <= 32 * 1024 && !value.chars().any(char::is_control)
        })
        .ok_or(UnavailableReason::InvalidObservation)?
        .to_owned();
    let image_sha256 = object
        .get("image_sha256")
        .and_then(Value::as_str)
        .filter(|value| {
            value.strip_prefix("sha256:").is_some_and(|hex| {
                hex.len() == 64
                    && hex
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        })
        .ok_or(UnavailableReason::InvalidObservation)?
        .to_owned();
    Ok(ProcessIdentity {
        pid,
        creation_filetime,
        image_path,
        image_sha256,
    })
}

#[cfg(windows)]
fn bounded_json_len(value: &Value) -> usize {
    serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len())
}

#[cfg(windows)]
fn identity_error(error: swarm_contracts::error::Error) -> UnavailableReason {
    if error.code == "PROCESS_GONE" {
        UnavailableReason::ProcessExited
    } else if error.code == "PROCESS_PLATFORM_UNSUPPORTED" {
        UnavailableReason::UnsupportedPlatform
    } else {
        UnavailableReason::IdentityUnavailable
    }
}

#[cfg(windows)]
fn membership_error(error: swarm_contracts::error::Error) -> UnavailableReason {
    if error.code == "PROCESS_GONE" {
        UnavailableReason::ProcessExited
    } else if error.code == "MODULE_MEMBERSHIP_UNSUPPORTED" {
        UnavailableReason::UnsupportedPlatform
    } else {
        UnavailableReason::MembershipUnavailable
    }
}

#[cfg(windows)]
fn open_process_error() -> UnavailableReason {
    // SAFETY: GetLastError is thread-local and read immediately after OpenProcess failed.
    match unsafe { GetLastError() } {
        ERROR_ACCESS_DENIED => UnavailableReason::AccessDenied,
        ERROR_INVALID_PARAMETER => UnavailableReason::ProcessExited,
        _ => UnavailableReason::IdentityUnavailable,
    }
}

#[cfg(windows)]
fn query_error() -> UnavailableReason {
    // SAFETY: GetLastError is thread-local and read immediately after a Win32 query failed.
    if unsafe { GetLastError() } == ERROR_ACCESS_DENIED {
        UnavailableReason::AccessDenied
    } else {
        UnavailableReason::ProcessQueryFailed
    }
}

#[cfg(windows)]
fn ensure_live(process: HANDLE) -> std::result::Result<(), UnavailableReason> {
    // SAFETY: the owned process handle remains valid for this zero-time wait.
    match unsafe { WaitForSingleObject(process, 0) } {
        WAIT_TIMEOUT => Ok(()),
        WAIT_OBJECT_0 => Err(UnavailableReason::ProcessExited),
        WAIT_FAILED => Err(query_error()),
        _ => Err(UnavailableReason::ProcessQueryFailed),
    }
}

#[cfg(windows)]
fn filetime_value(value: FILETIME) -> u64 {
    (u64::from(value.dwHighDateTime) << 32) | u64::from(value.dwLowDateTime)
}

#[cfg(windows)]
fn process_times(process: HANDLE) -> std::result::Result<(u64, u64, u64), UnavailableReason> {
    // SAFETY: all pointers refer to initialized, writable FILETIME values and
    // the caller holds a valid query handle until this call returns.
    unsafe {
        let mut creation: FILETIME = std::mem::zeroed();
        let mut exit: FILETIME = std::mem::zeroed();
        let mut kernel: FILETIME = std::mem::zeroed();
        let mut user: FILETIME = std::mem::zeroed();
        if GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) == 0 {
            return Err(query_error());
        }
        Ok((
            filetime_value(creation),
            filetime_value(kernel),
            filetime_value(user),
        ))
    }
}

#[cfg(windows)]
fn resident_working_set(process: HANDLE) -> std::result::Result<u64, UnavailableReason> {
    // SAFETY: the initialized structure is writable and the process handle has
    // PROCESS_QUERY_LIMITED_INFORMATION; the API writes its documented size.
    unsafe {
        let size = std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32;
        let mut counters = PROCESS_MEMORY_COUNTERS {
            cb: size,
            ..Default::default()
        };
        if K32GetProcessMemoryInfo(process, &mut counters, size) == 0 {
            return Err(query_error());
        }
        Ok(counters.WorkingSetSize as u64)
    }
}

#[cfg(windows)]
fn logical_processor_count() -> Option<u32> {
    // SAFETY: this API reads the system's active logical processors and has no
    // pointer parameters. Zero means that the count is unavailable.
    let count = unsafe { GetActiveProcessorCount(ALL_PROCESSOR_GROUPS) };
    (count > 0).then_some(count)
}

#[cfg(windows)]
fn cpu_percent(
    previous_cpu_100ns: u64,
    current_cpu_100ns: u64,
    elapsed: std::time::Duration,
    logical_processors: u32,
) -> Option<f64> {
    let delta_100ns = current_cpu_100ns.checked_sub(previous_cpu_100ns)?;
    let elapsed_ns = elapsed.as_nanos();
    if elapsed_ns == 0 || logical_processors == 0 {
        return None;
    }
    let delta_cpu_ns = (delta_100ns as f64) * 100.0;
    let percent = (delta_cpu_ns / elapsed_ns as f64 / f64::from(logical_processors)) * 100.0;
    percent.is_finite().then_some(percent)
}
