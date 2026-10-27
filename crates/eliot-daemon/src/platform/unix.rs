#![allow(unsafe_code)]

use super::process_observation::unknown_sample;
#[cfg(target_os = "linux")]
use super::process_observation::{MAX_PROCESS_SAMPLES, exited_sample};
use eliot_core::{Error, ProcessSample, ProcessSnapshot, Result};
#[cfg(target_os = "linux")]
use eliot_core::{ProcessIdentity, ProcessRecord, ProcessState};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

pub(crate) fn ensure_private_root(path: &Path) -> Result<()> {
    check_path_components(path)?;
    fs::create_dir_all(path).map_err(|error| io_error("create data directory", error))?;
    check_path_components(path)?;

    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("inspect data directory", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "unsafe_data_dir",
            "data directory must be a real directory, not a symlink or file",
        ));
    }
    require_current_owner(&metadata, "data directory")?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
        .map_err(|error| io_error("restrict data directory", error))
}

pub(crate) fn ensure_private_file(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("inspect private file", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::new(
            "unsafe_private_file",
            "private data files must be regular files, not symlinks or devices",
        ));
    }
    require_current_owner(&metadata, "private data file")?;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
        .map_err(|error| io_error("restrict private file", error))
}

pub(crate) fn validate_private_root(path: &Path) -> Result<()> {
    check_path_components(path)?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error("inspect existing data directory", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "unsafe_data_dir",
            "data directory must be a real directory, not a symlink or file",
        ));
    }
    require_current_owner(&metadata, "data directory")?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::new(
            "unsafe_data_dir",
            "data directory grants access to group or other users",
        ));
    }
    Ok(())
}

pub(crate) fn validate_private_file(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error("inspect existing private file", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::new(
            "unsafe_private_file",
            "private data files must be regular files, not symlinks or devices",
        ));
    }
    require_current_owner(&metadata, "private data file")?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(Error::new(
            "unsafe_private_file",
            "private data file grants access to group or other users",
        ));
    }
    Ok(())
}

pub(crate) fn prepare_leaf_command(
    mut command: tokio::process::Command,
) -> Result<tokio::process::Command> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        verify_unprivileged_leaf().map_err(|_| {
            Error::new(
                "git_leaf_fence_unavailable",
                "leaf execution requires verified unprivileged nonroot Linux credentials",
            )
        })?;
        // SAFETY: this callback captures nothing and only uses stack storage
        // and direct credential/limit syscalls. No allocation, locks, or I/O.
        unsafe {
            command.as_std_mut().pre_exec(|| {
                verify_unprivileged_leaf()?;
                // Prevent setuid/file capabilities from restoring privileges on exec.
                if libc::prctl(
                    libc::PR_SET_NO_NEW_PRIVS,
                    1 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                    0 as libc::c_ulong,
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                let limit = libc::rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                if libc::setrlimit(libc::RLIMIT_NPROC, &limit) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                let mut actual = libc::rlimit {
                    rlim_cur: libc::RLIM_INFINITY,
                    rlim_max: libc::RLIM_INFINITY,
                };
                if libc::getrlimit(libc::RLIMIT_NPROC, &mut actual) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if actual.rlim_cur != 0 || actual.rlim_max != 0 {
                    return Err(std::io::Error::from_raw_os_error(libc::EPERM));
                }
                Ok(())
            });
        }
        Ok(command)
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = &mut command;
        Err(Error::new(
            "git_leaf_fence_unavailable",
            "leaf process restrictions are not verified on this Unix platform",
        ))
    }
}

#[cfg(target_os = "linux")]
fn verify_unprivileged_leaf() -> std::io::Result<()> {
    let (mut real, mut effective, mut saved) = (0, 0, 0);
    // SAFETY: all three outputs point to writable uid_t-sized local values.
    if unsafe { libc::getresuid(&mut real, &mut effective, &mut saved) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    if real == 0 || effective == 0 || saved == 0 || real != effective || real != saved {
        return Err(std::io::Error::from_raw_os_error(libc::EPERM));
    }
    // Linux capget v3 uses two 32-bit capability words. Query the calling
    // thread again after fork: capabilities are thread-local, not daemon-global.
    #[repr(C)]
    struct Header {
        version: u32,
        pid: i32,
    }
    #[repr(C)]
    #[derive(Clone, Copy, Default)]
    struct Capabilities {
        effective: u32,
        permitted: u32,
        inheritable: u32,
    }
    let mut header = Header {
        version: 0x2008_0522,
        pid: 0,
    };
    let mut caps = [Capabilities::default(); 2];
    // SAFETY: the v3 header and two correctly sized output words remain live.
    if unsafe {
        libc::syscall(
            libc::SYS_capget,
            &mut header as *mut Header,
            caps.as_mut_ptr(),
        )
    } != 0
    {
        return Err(std::io::Error::last_os_error());
    }
    // CAP_SYS_ADMIN and CAP_SYS_RESOURCE bypass RLIMIT_NPROC. Reject every
    // capability-bearing context, including privileges that could change UID.
    if caps
        .iter()
        .any(|caps| caps.effective != 0 || caps.permitted != 0 || caps.inheritable != 0)
    {
        return Err(std::io::Error::from_raw_os_error(libc::EPERM));
    }
    Ok(())
}

pub(crate) fn effective_uid() -> u32 {
    // SAFETY: geteuid has no pointer arguments or preconditions.
    unsafe { libc::geteuid() }
}

#[cfg(target_os = "linux")]
pub(crate) fn host_boot_id() -> Result<String> {
    let boot_id = fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .map_err(|error| io_error("read host boot identity", error))?;
    let boot_id = boot_id.trim();
    if boot_id.is_empty() || boot_id.chars().any(char::is_control) {
        return Err(Error::new(
            "process_identity",
            "Linux host boot identity is malformed",
        ));
    }
    Ok(boot_id.to_owned())
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn host_boot_id() -> Result<String> {
    // Other Unix targets lack a shared boot UUID API. Keep the approximation
    // visibly qualified instead of presenting it as an exact kernel boot ID.
    let mut uptime = std::mem::MaybeUninit::<libc::timespec>::uninit();
    // SAFETY: uptime points to writable storage for one timespec.
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, uptime.as_mut_ptr()) } != 0 {
        return Err(io_error(
            "read monotonic clock",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: clock_gettime initialized uptime on success.
    let uptime = unsafe { uptime.assume_init() };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| Error::new("process_identity", error.to_string()))?
        .as_secs();
    Ok(format!(
        "unix-boot-estimate:{}",
        now.saturating_sub(uptime.tv_sec as u64)
    ))
}

#[cfg(target_os = "linux")]
pub(crate) fn current_process_birth() -> Result<String> {
    let stat = fs::read_to_string("/proc/self/stat")
        .map_err(|error| io_error("read process birth identity", error))?;
    let close = stat
        .rfind(')')
        .ok_or_else(|| Error::new("process_identity", "Linux process stat record is malformed"))?;
    // The comm field may contain spaces and parentheses. Fields after its final
    // ')' begin with field 3; starttime is field 22 (index 19 in this suffix).
    let start_ticks = stat[close + 1..]
        .split_whitespace()
        .nth(19)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| Error::new("process_identity", "Linux process start time is invalid"))?;
    Ok(format!("linux-start-ticks:{start_ticks}"))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn current_process_birth() -> Result<String> {
    Err(Error::new(
        "process_identity_unsupported",
        "exact Unix process birth identity is currently implemented on Linux",
    ))
}

/// Exact identity captured for a spawned process-group leader.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ProcessGroupIdentity {
    #[cfg(target_os = "linux")]
    pid: u32,
    #[cfg(target_os = "linux")]
    pgid: i32,
    #[cfg(target_os = "linux")]
    start_ticks: u64,
}

#[cfg(target_os = "linux")]
pub(crate) fn capture_process_group_identity(pid: u32) -> Result<ProcessGroupIdentity> {
    let (pgid, start_ticks) = read_linux_process_identity(pid)
        .map_err(|error| unknown_process_identity("cannot read spawned process identity", error))?;
    let expected_pgid = i32::try_from(pid).map_err(|_| {
        Error::new(
            "process_custody_unknown",
            "spawned process ID cannot be represented as a process group",
        )
    })?;
    if pid == 0 || pgid != expected_pgid {
        return Err(Error::new(
            "process_custody_unknown",
            "spawned process is not the leader of its requested process group",
        ));
    }
    Ok(ProcessGroupIdentity {
        pid,
        pgid,
        start_ticks,
    })
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn capture_process_group_identity(_pid: u32) -> Result<ProcessGroupIdentity> {
    Err(Error::new(
        "process_identity_unsupported",
        "exact process-group leader identity is currently implemented on Linux",
    ))
}

pub(crate) fn terminate_process_group(identity: &ProcessGroupIdentity) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        let (pgid, start_ticks) = read_linux_process_identity(identity.pid).map_err(|error| {
            unknown_process_identity("cannot revalidate process-group leader", error)
        })?;
        if pgid != identity.pgid || start_ticks != identity.start_ticks {
            return Err(Error::new(
                "process_custody_unknown",
                "process-group leader PID birth or PGID changed; no signal was sent",
            ));
        }

        // The owner retains the Tokio child until wait succeeds. A reaped leader
        // loses this identity in OwnedChild, so its numeric PGID cannot be reused
        // between this check and the group signal.
        // SAFETY: the negative PGID is the exact group revalidated above.
        if unsafe { libc::kill(-identity.pgid, libc::SIGKILL) } == 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ESRCH) {
            Err(unknown_process_identity(
                "process group disappeared before it could be signalled",
                error,
            ))
        } else {
            Err(io_error("terminate process group", error))
        }
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = identity;
        Err(Error::new(
            "process_custody_unknown",
            "no exact Unix process-group identity is available; no signal was sent",
        ))
    }
}

#[cfg(target_os = "linux")]
fn read_linux_process_identity(pid: u32) -> std::io::Result<(i32, u64)> {
    let stat = read_linux_process_stat(pid)?;
    Ok((stat.pgid, stat.start_ticks))
}

#[cfg(target_os = "linux")]
struct LinuxProcessStat {
    pgid: i32,
    start_ticks: u64,
    state: ProcessState,
    cpu_time_ns: Option<u64>,
    rss_bytes: Option<u64>,
}

#[cfg(target_os = "linux")]
fn read_linux_process_stat(pid: u32) -> std::io::Result<LinuxProcessStat> {
    use std::io::{Error as IoError, ErrorKind};

    if pid == 0 {
        return Err(IoError::new(
            ErrorKind::InvalidInput,
            "PID must be positive",
        ));
    }
    let stat = fs::read_to_string(format!("/proc/{pid}/stat"))?;
    let open = stat
        .find('(')
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "malformed process stat"))?;
    let close = stat
        .rfind(')')
        .filter(|close| *close > open)
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "malformed process stat"))?;
    let reported_pid = stat[..open]
        .trim()
        .parse::<u32>()
        .map_err(|_| IoError::new(ErrorKind::InvalidData, "malformed process PID"))?;
    if reported_pid != pid {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            "process stat PID did not match requested PID",
        ));
    }
    // Fields after the final ')' start at field 3: pgrp is field 5 (index 2)
    // and starttime is field 22 (index 19). The comm field may contain spaces.
    let fields: Vec<_> = stat[close + 1..].split_whitespace().collect();
    let pgid = fields
        .get(2)
        .and_then(|value| value.parse::<i32>().ok())
        .filter(|value| *value > 0)
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "invalid process PGID"))?;
    let start_ticks = fields
        .get(19)
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(|| IoError::new(ErrorKind::InvalidData, "invalid process start time"))?;
    let state = match fields.first().copied() {
        Some("Z" | "X" | "x") => ProcessState::Exited,
        Some(_) => ProcessState::Running,
        None => {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "missing process state",
            ));
        }
    };
    // SAFETY: sysconf only reads the two process-independent numeric limits.
    let ticks_per_second = u64::try_from(unsafe { libc::sysconf(libc::_SC_CLK_TCK) })
        .ok()
        .filter(|n| *n > 0);
    let page_size = u64::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) })
        .ok()
        .filter(|size| *size > 0);
    let cpu_time_ns = fields
        .get(11)
        .and_then(|v| v.parse::<u64>().ok())
        .zip(fields.get(12).and_then(|v| v.parse::<u64>().ok()))
        .and_then(|(user, system)| user.checked_add(system))
        .zip(ticks_per_second)
        .and_then(|(ticks, frequency)| {
            u64::try_from(u128::from(ticks) * 1_000_000_000 / u128::from(frequency)).ok()
        });
    let rss_bytes = fields
        .get(21)
        .and_then(|v| v.parse::<u64>().ok())
        .zip(page_size)
        .and_then(|(pages, size)| pages.checked_mul(size));
    Ok(LinuxProcessStat {
        pgid,
        start_ticks,
        state,
        cpu_time_ns,
        rss_bytes,
    })
}

#[cfg(target_os = "linux")]
pub(crate) struct ProcessObserver {
    group: ProcessGroupIdentity,
    root: ProcessIdentity,
    root_exit: Option<Option<i32>>,
}

#[cfg(target_os = "linux")]
impl ProcessObserver {
    pub(crate) fn new(group: ProcessGroupIdentity) -> Result<Self> {
        let current = read_linux_process_identity(group.pid)
            .map_err(|error| unknown_process_identity("verify observed group leader", error))?;
        if current != (group.pgid, group.start_ticks) {
            return Err(Error::new(
                "process_custody_unknown",
                "owned group leader identity changed",
            ));
        }
        Ok(Self {
            root: ProcessIdentity {
                host_boot_id: host_boot_id()?,
                pid: group.pid,
                start_time: group.start_ticks,
            },
            group,
            root_exit: None,
        })
    }

    fn group_is_current(&self) -> bool {
        read_linux_process_identity(self.group.pid).ok()
            == Some((self.group.pgid, self.group.start_ticks))
    }

    pub(crate) fn record_root_exit(&mut self, code: Option<i32>) {
        self.root_exit = Some(code);
    }

    pub(crate) fn snapshot(&self, owner: &str, now: i64) -> Result<ProcessSnapshot> {
        let root = ProcessSample {
            record: ProcessRecord {
                identity: self.root.clone(),
                owner: owner.to_owned(),
                role: "plugin".to_owned(),
                state: ProcessState::Unknown,
                exit_code: None,
                updated_at: now,
            },
            cpu_time_ns: None,
            rss_bytes: None,
        };
        let mut snapshot = ProcessSnapshot {
            samples: vec![self.refresh(&root, now)],
            complete: false,
        };
        if !self.group_is_current() {
            return Ok(snapshot);
        }
        let entries = fs::read_dir("/proc")
            .map_err(|error| io_error("enumerate owned process group", error))?;
        snapshot.complete = true;
        // /proc is host-wide: both scanned directory entries and returned group
        // members are bounded. This never limits spawning or signals a process.
        for (index, entry) in entries.enumerate() {
            if index >= 65_536 {
                snapshot.complete = false;
                break;
            }
            let entry = match entry {
                Ok(entry) => entry,
                Err(_) => {
                    snapshot.complete = false;
                    continue;
                }
            };
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            if pid == self.root.pid {
                continue;
            }
            match read_linux_process_stat(pid) {
                Ok(stat) if stat.pgid == self.group.pgid => {
                    if snapshot.samples.len() >= MAX_PROCESS_SAMPLES {
                        snapshot.complete = false;
                        break;
                    }
                    snapshot
                        .samples
                        .push(self.sample(pid, stat, "child", owner, now));
                }
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => snapshot.complete = false,
            }
        }
        if !self.group_is_current() {
            // The leader is no longer authority for newly discovered members.
            snapshot.samples.truncate(1);
            snapshot.complete = false;
        }
        Ok(snapshot)
    }

    pub(crate) fn register_pid(
        &self,
        pid: u32,
        role: &str,
        owner: &str,
        now: i64,
    ) -> Result<ProcessSample> {
        if !self.group_is_current() {
            return Err(Error::new(
                "process_custody_unknown",
                "owned group leader cannot be verified",
            ));
        }
        let stat = read_linux_process_stat(pid)
            .map_err(|error| io_error("verify registered process", error))?;
        if stat.pgid != self.group.pgid
            || stat.state != ProcessState::Running
            || !self.group_is_current()
        {
            return Err(Error::new(
                "not_allowed",
                "registered process is not live in the owned process group",
            ));
        }
        let is_root = pid == self.root.pid && stat.start_ticks == self.root.start_time;
        if (role == "plugin") != is_root {
            return Err(Error::new(
                "invalid",
                "only the owned root has the plugin role",
            ));
        }
        Ok(self.sample(pid, stat, role, owner, now))
    }

    pub(crate) fn refresh(&self, old: &ProcessSample, now: i64) -> ProcessSample {
        if old.record.identity == self.root
            && let Some(code) = self.root_exit
        {
            let mut sample = exited_sample(old, now);
            sample.record.exit_code = code;
            return sample;
        }
        match read_linux_process_stat(old.record.identity.pid) {
            Ok(stat) if stat.start_ticks != old.record.identity.start_time => {
                exited_sample(old, now)
            }
            Ok(stat) if stat.pgid == self.group.pgid => self.sample(
                old.record.identity.pid,
                stat,
                &old.record.role,
                &old.record.owner,
                now,
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => exited_sample(old, now),
            _ => unknown_sample(old, now),
        }
    }

    fn sample(
        &self,
        pid: u32,
        stat: LinuxProcessStat,
        role: &str,
        owner: &str,
        now: i64,
    ) -> ProcessSample {
        ProcessSample {
            record: ProcessRecord {
                identity: ProcessIdentity {
                    host_boot_id: self.root.host_boot_id.clone(),
                    pid,
                    start_time: stat.start_ticks,
                },
                owner: owner.to_owned(),
                role: role.to_owned(),
                state: stat.state,
                // Only OwnedChild::wait supplies a known Linux leader code.
                // Missing/filtered proc exit status must not invent a child code.
                exit_code: None,
                updated_at: now,
            },
            cpu_time_ns: stat.cpu_time_ns,
            rss_bytes: stat.rss_bytes,
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub(crate) struct ProcessObserver;

#[cfg(not(target_os = "linux"))]
impl ProcessObserver {
    pub(crate) fn new(_group: ProcessGroupIdentity) -> Result<Self> {
        Err(Error::new(
            "unsupported",
            "process observation requires Linux boot and birth identity",
        ))
    }
    pub(crate) fn snapshot(&self, _owner: &str, _now: i64) -> Result<ProcessSnapshot> {
        Err(Error::new(
            "unsupported",
            "process observation requires Linux boot and birth identity",
        ))
    }
    pub(crate) fn register_pid(
        &self,
        _pid: u32,
        _role: &str,
        _owner: &str,
        _now: i64,
    ) -> Result<ProcessSample> {
        Err(Error::new(
            "unsupported",
            "process observation requires Linux boot and birth identity",
        ))
    }
    pub(crate) fn refresh(&self, old: &ProcessSample, now: i64) -> ProcessSample {
        unknown_sample(old, now)
    }
    pub(crate) fn record_root_exit(&mut self, _code: Option<i32>) {}
}

#[cfg(target_os = "linux")]
fn unknown_process_identity(action: &str, error: std::io::Error) -> Error {
    Error::new(
        "process_custody_unknown",
        format!("{action}: {error}; no signal was sent"),
    )
}

fn require_current_owner(metadata: &fs::Metadata, description: &str) -> Result<()> {
    if metadata.uid() == effective_uid() {
        Ok(())
    } else {
        Err(Error::new(
            "unsafe_data_dir",
            format!("{description} is owned by another user"),
        ))
    }
}

fn check_path_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::RootDir => current.push(component.as_os_str()),
            Component::Normal(name) => current.push(name),
            Component::CurDir => continue,
            Component::ParentDir | Component::Prefix(_) => {
                return Err(Error::new(
                    "unsafe_data_dir",
                    "data directory path contains an unsupported component",
                ));
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::new(
                    "unsafe_data_dir",
                    "data directory path may not pass through a symlink",
                ));
            }
            Ok(metadata) if !metadata.is_dir() => {
                return Err(Error::new(
                    "unsafe_data_dir",
                    "a data directory path component is not a directory",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(io_error("inspect data directory path", error)),
        }
    }
    Ok(())
}

fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{action}: {error}"))
}
