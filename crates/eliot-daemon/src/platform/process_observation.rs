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

/// One host process captured by a shared sampling-sweep enumeration. The
/// sweep producer owns the single bounded scan; per-owner grouping below is
/// pure so its capping behavior is unit-testable on every platform.
#[cfg(any(target_os = "linux", test))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SweepEntry {
    pub pid: u32,
    pub pgid: i32,
    pub start_ticks: u64,
    pub state: ProcessState,
    pub cpu_time_ns: Option<u64>,
    pub rss_bytes: Option<u64>,
}

/// Select one owner's members from a single shared enumeration, in
/// enumeration order: skip other groups and the group root, then stop at the
/// per-owner member capacity. Returns the selected members and whether every
/// matching member fit. A truncated selection is incomplete coverage, never
/// proof that omitted processes exited.
#[cfg(any(target_os = "linux", test))]
pub(crate) fn select_sweep_members(
    entries: &[SweepEntry],
    group: i32,
    root_pid: u32,
    capacity: usize,
) -> (Vec<SweepEntry>, bool) {
    let mut members = Vec::new();
    for entry in entries {
        if entry.pid == root_pid || entry.pgid != group {
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

#[cfg(test)]
mod sweep_tests {
    use super::*;

    fn entry(pid: u32, pgid: i32) -> SweepEntry {
        SweepEntry {
            pid,
            pgid,
            start_ticks: u64::from(pid) * 100,
            state: ProcessState::Running,
            cpu_time_ns: Some(u64::from(pid)),
            rss_bytes: None,
        }
    }

    /// One host enumeration serves every owner in a sweep: the enumeration
    /// count per sweep is exactly one regardless of group count.
    struct FakeSweep {
        enumerations: usize,
        entries: Vec<SweepEntry>,
    }

    impl FakeSweep {
        fn enumerate(&mut self) -> Vec<SweepEntry> {
            self.enumerations += 1;
            self.entries.clone()
        }
    }

    #[test]
    fn one_enumeration_serves_every_group_with_per_owner_caps() {
        let mut sweep = FakeSweep {
            enumerations: 0,
            entries: vec![
                entry(10, 10), // group A root
                entry(11, 10),
                entry(12, 10),
                entry(20, 20), // group B root
                entry(21, 20),
                entry(22, 20),
                entry(23, 20),
                entry(30, 30), // unrelated group root
            ],
        };
        let entries = sweep.enumerate();
        let (group_a, a_complete) = select_sweep_members(&entries, 10, 10, MAX_PROCESS_SAMPLES);
        let (group_b, b_complete) = select_sweep_members(&entries, 20, 20, 2);
        let (group_c, c_complete) = select_sweep_members(&entries, 30, 30, MAX_PROCESS_SAMPLES);
        assert_eq!(sweep.enumerations, 1, "one enumeration per sweep");
        assert_eq!(entries.len(), 8, "eight host entries enumerated once");
        assert_eq!(
            group_a.iter().map(|entry| entry.pid).collect::<Vec<_>>(),
            vec![11, 12]
        );
        assert!(a_complete);
        // The per-owner member cap truncates only that owner, in order.
        assert_eq!(
            group_b.iter().map(|entry| entry.pid).collect::<Vec<_>>(),
            vec![21, 22]
        );
        assert!(!b_complete);
        // A lone root contributes no members but stays complete.
        assert!(group_c.is_empty());
        assert!(c_complete);
        // Full identities survive grouping.
        assert_eq!(group_a[0].start_ticks, 1_100);
        assert_eq!(group_a[0].cpu_time_ns, Some(11));
        assert_eq!(group_a[0].state, ProcessState::Running);
    }

    #[test]
    fn zero_capacity_keeps_no_members_and_reports_incomplete() {
        let entries = vec![entry(10, 10), entry(11, 10)];
        let (members, complete) = select_sweep_members(&entries, 10, 10, 0);
        assert!(members.is_empty());
        assert!(!complete);
        let (members, complete) = select_sweep_members(&entries, 99, 10, 0);
        assert!(members.is_empty());
        assert!(complete, "no match means no truncation");
    }
}
