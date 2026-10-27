use eliot_core::{Error, ProcessIdentity, ProcessSample, ProcessSnapshot, ProcessState, Result};
use std::sync::{Arc, Mutex};

#[cfg(unix)]
use super::unix::ProcessObserver as PlatformObserver;
#[cfg(windows)]
use super::windows::ProcessObserver as PlatformObserver;

/// Bounds observations, not the number of processes an owner may launch.
pub(crate) const MAX_PROCESS_SAMPLES: usize = 1024;

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
    pub(crate) fn record_root_exit(&self, code: Option<i32>) {
        #[cfg(any(unix, windows))]
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .platform
            .record_root_exit(code);
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

    pub fn snapshot(&self, owner: &str, now: i64) -> Result<ProcessSnapshot> {
        #[cfg(any(unix, windows))]
        {
            let mut state = self.inner.lock().map_err(|_| {
                Error::new("process_observation", "process observer lock was poisoned")
            })?;
            state.bind_owner(owner)?;
            let mut snapshot = state.platform.snapshot(owner, now)?;
            let enumerated_identities = snapshot
                .samples
                .iter()
                .map(|sample| sample.record.identity.clone())
                .collect::<Vec<_>>();
            for old in &state.known {
                if !snapshot
                    .samples
                    .iter()
                    .any(|sample| same_identity(&sample.record.identity, &old.record.identity))
                {
                    let sample = state.platform.refresh(old, now);
                    snapshot.samples.push(sample);
                }
            }
            for sample in &mut snapshot.samples {
                if let Some(old) = state
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
                    !state
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
            state.known = snapshot
                .samples
                .iter()
                .filter(|sample| {
                    sample.record.state != ProcessState::Exited
                        || enumerated_identities.contains(&sample.record.identity)
                })
                .cloned()
                .collect();
            Ok(snapshot)
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
