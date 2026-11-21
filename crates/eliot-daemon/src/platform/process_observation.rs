use eliot_core::{Error, ProcessIdentity, ProcessSample, ProcessSnapshot, ProcessState, Result};
#[cfg(any(unix, windows))]
use eliot_plugin::sync::LockExt;
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use super::unix::ProcessObserver as PlatformObserver;
#[cfg(windows)]
use super::windows::ProcessObserver as PlatformObserver;

/// Bounds observations, not the number of processes an owner may launch.
pub(crate) const MAX_PROCESS_SAMPLES: usize = 1024;

/// Cap on one argv read: only the program plus its first tokens ever matter
/// downstream, and the full body never persists.
#[cfg(target_os = "linux")]
pub(crate) const MAX_CMDLINE_BYTES: usize = 64 * 1024;

/// Read at most [`MAX_CMDLINE_BYTES`] plus one probe byte, so an oversized
/// body is rejected without a full read. Fails closed to `None` on missing
/// input, read errors and oversized bodies.
#[cfg(target_os = "linux")]
pub(crate) fn read_bounded_cmdline<R: std::io::Read>(reader: R) -> Option<Vec<u8>> {
    use std::io::Read as _;
    let mut limited = reader.take(MAX_CMDLINE_BYTES as u64 + 1);
    let mut body = Vec::new();
    limited.read_to_end(&mut body).ok()?;
    if body.len() > MAX_CMDLINE_BYTES {
        return None;
    }
    Some(body)
}

/// Parsed `/proc/<pid>/stat` identity and counters for one host process.
/// Pure data, so sweep grouping stays unit-testable on every platform; only
/// the `/proc` reader itself is Linux-only.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LinuxProcessStat {
    pub parent: u32,
    pub pgid: i32,
    pub start_ticks: u64,
    pub state: ProcessState,
    pub cpu_time_ns: Option<u64>,
    pub rss_bytes: Option<u64>,
}

/// One host process captured by a shared sampling-sweep enumeration: its pid
/// next to its parsed stat record. The sweep producer owns the single bounded
/// scan; per-owner grouping below is pure so its capping behavior is
/// unit-testable on every platform.
#[cfg(target_os = "linux")]
pub(crate) type SweepEntry = (u32, LinuxProcessStat);

/// Select one owner's members from a single shared enumeration, in
/// enumeration order: skip other groups and the group root, then stop at the
/// per-owner member capacity. Returns the selected members and whether every
/// matching member fit. A truncated selection is incomplete coverage, never
/// proof that omitted processes exited.
#[cfg(target_os = "linux")]
pub(crate) fn select_sweep_members(
    entries: &[SweepEntry],
    group: i32,
    root_pid: u32,
    capacity: usize,
) -> (Vec<SweepEntry>, bool) {
    let mut members = Vec::new();
    for entry in entries {
        if entry.0 == root_pid || entry.1.pgid != group {
            continue;
        }
        if members.len() >= capacity {
            return (members, false);
        }
        members.push(*entry);
    }
    (members, true)
}

/// Read-only observation of one owned family. Clones share identity/role history,
/// but never acquire permission to signal a process or prolong owned execution.
#[derive(Clone)]
pub struct CustodyObserver {
    #[cfg(any(unix, windows))]
    inner: Arc<Mutex<ObservationState>>,
}

#[cfg(any(unix, windows))]
struct ObservationState {
    platform: PlatformObserver,
    owner: Option<String>,
    known: Vec<ProcessSample>,
}

impl CustodyObserver {
    /// Observes known process-group emptiness after recorded root exit on Linux.
    /// Escaped descendants may survive; this is not exact family containment.
    #[cfg(unix)]
    pub fn known_process_group_empty(&self) -> Result<bool> {
        self.inner
            .lock()
            .map_err(|_| Error::new("process_observation", "process observer lock was poisoned"))?
            .platform
            .known_process_group_empty()
    }

    pub(crate) fn record_root_exit(&self, code: Option<i32>) {
        #[cfg(any(unix, windows))]
        self.inner.lock_recover().platform.record_root_exit(code);
        #[cfg(not(any(unix, windows)))]
        let _ = code;
    }

    #[cfg(any(unix, windows))]
    pub(crate) fn new(platform: PlatformObserver) -> Self {
        Self {
            inner: Arc::new(Mutex::new(ObservationState {
                platform,
                owner: None,
                known: Vec::new(),
            })),
        }
    }

    /// Read-only leader check bracketing a shared sweep enumeration. The
    /// caller checks every captured leader before enumerating once, then each
    /// group revalidates after via its own snapshot build.
    #[cfg(target_os = "linux")]
    pub(crate) fn group_is_current(&self) -> bool {
        self.inner.lock_recover().platform.group_is_current()
    }

    /// Build one owner's snapshot from a shared sweep enumeration instead of
    /// scanning the host again. Known-identity refresh and publication keep
    /// the same behavior as [`CustodyObserver::snapshot`].
    #[cfg(target_os = "linux")]
    pub(crate) fn snapshot_with_sweep(
        &self,
        owner: &str,
        now: i64,
        sweep: &super::unix::LinuxProcessSweep,
        leader_before: bool,
    ) -> Result<ProcessSnapshot> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| Error::new("process_observation", "process observer lock was poisoned"))?;
        state.bind_owner(owner)?;
        let snapshot = state
            .platform
            .snapshot_from_sweep(owner, now, sweep, leader_before);
        Ok(state.finish_snapshot(snapshot, now))
    }

    pub fn snapshot(&self, owner: &str, now: i64) -> Result<ProcessSnapshot> {
        #[cfg(any(unix, windows))]
        {
            let mut state = self.inner.lock().map_err(|_| {
                Error::new("process_observation", "process observer lock was poisoned")
            })?;
            state.bind_owner(owner)?;
            let snapshot = state.platform.snapshot(owner, now)?;
            Ok(state.finish_snapshot(snapshot, now))
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (owner, now);
            Err(Error::new(
                "unsupported",
                "process observation is unsupported",
            ))
        }
    }

    /// Registration is a verified role update, never authority supplied by a PID.
    pub fn register_pid(
        &self,
        pid: u32,
        role: &str,
        owner: &str,
        now: i64,
    ) -> Result<ProcessSample> {
        #[cfg(any(unix, windows))]
        {
            if !matches!(role, "plugin" | "harness" | "child") {
                return Err(Error::new("unsupported", "process role is unsupported"));
            }
            let mut state = self.inner.lock().map_err(|_| {
                Error::new("process_observation", "process observer lock was poisoned")
            })?;
            state.bind_owner(owner)?;
            let sample = state.platform.register_pid(pid, role, owner, now)?;
            if let Some(old) = state
                .known
                .iter_mut()
                .find(|old| same_identity(&old.record.identity, &sample.record.identity))
            {
                *old = sample.clone();
            } else {
                if state.known.len() >= MAX_PROCESS_SAMPLES {
                    return Err(Error::new(
                        "busy",
                        "process observation identity bound reached",
                    ));
                }
                state.known.push(sample.clone());
            }
            Ok(sample)
        }
        #[cfg(not(any(unix, windows)))]
        {
            let _ = (pid, role, owner, now);
            Err(Error::new(
                "unsupported",
                "process observation is unsupported",
            ))
        }
    }
}

#[cfg(any(unix, windows))]
impl ObservationState {
    /// Refresh known identities missing from a platform snapshot, restore
    /// their roles, and enforce the per-owner member bound keeping known
    /// identities first. Shared by single scans and sweep builds.
    fn finish_snapshot(&mut self, mut snapshot: ProcessSnapshot, now: i64) -> ProcessSnapshot {
        let enumerated_identities = snapshot
            .samples
            .iter()
            .map(|sample| sample.record.identity.clone())
            .collect::<Vec<_>>();
        for old in &self.known {
            if !snapshot
                .samples
                .iter()
                .any(|sample| same_identity(&sample.record.identity, &old.record.identity))
            {
                let sample = self.platform.refresh(old, now);
                snapshot.samples.push(sample);
            }
        }
        for sample in &mut snapshot.samples {
            if let Some(old) = self
                .known
                .iter()
                .find(|old| same_identity(&old.record.identity, &sample.record.identity))
            {
                sample.record.role.clone_from(&old.record.role);
            }
        }
        if snapshot.samples.len() > MAX_PROCESS_SAMPLES {
            // Keep previously published identities so their exit/uncertainty
            // is not lost when a newly enumerated family exceeds the bound.
            snapshot.samples.sort_by_key(|sample| {
                !self
                    .known
                    .iter()
                    .any(|old| same_identity(&old.record.identity, &sample.record.identity))
            });
            snapshot.samples.truncate(MAX_PROCESS_SAMPLES);
            snapshot.complete = false;
        }
        if snapshot
            .samples
            .iter()
            .any(|sample| sample.record.state == ProcessState::Unknown)
        {
            snapshot.complete = false;
        }
        self.known = snapshot
            .samples
            .iter()
            .filter(|sample| {
                sample.record.state != ProcessState::Exited
                    || enumerated_identities.contains(&sample.record.identity)
            })
            .cloned()
            .collect();
        snapshot
    }

    fn bind_owner(&mut self, owner: &str) -> Result<()> {
        if owner.is_empty() || owner.chars().any(char::is_control) {
            return Err(Error::new("invalid", "process owner is empty or malformed"));
        }
        match self.owner.as_deref() {
            Some(previous) if previous != owner => Err(Error::new(
                "not_allowed",
                "process observer belongs to another owner",
            )),
            Some(_) => Ok(()),
            None => {
                self.owner = Some(owner.to_owned());
                Ok(())
            }
        }
    }
}

/// Bounded program identity for one owned process: the episode category plus
/// how it was observed. The daemon classifies at read time; full command
/// lines never persist.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CommandClass {
    pub category: String,
    pub source: &'static str,
}

pub(crate) fn same_identity(left: &ProcessIdentity, right: &ProcessIdentity) -> bool {
    left.host_boot_id == right.host_boot_id
        && left.pid == right.pid
        && left.start_time == right.start_time
}

pub(crate) fn unknown_sample(old: &ProcessSample, now: i64) -> ProcessSample {
    let mut sample = old.clone();
    sample.record.state = ProcessState::Unknown;
    sample.record.exit_code = None;
    sample.record.updated_at = now;
    sample.cpu_time_ns = None;
    sample.rss_bytes = None;
    sample
}

#[cfg(any(windows, target_os = "linux"))]
pub(crate) fn exited_sample(old: &ProcessSample, now: i64) -> ProcessSample {
    let mut sample = unknown_sample(old, now);
    sample.record.state = ProcessState::Exited;
    sample
}
