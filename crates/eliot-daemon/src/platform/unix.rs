#![allow(unsafe_code)]

use eliot_core::{Error, Result};
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
    Ok((pgid, start_ticks))
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
