//! Crash-repairable ownership marker for one explicitly selected state directory.
//!
//! The marker is coordination, not process liveness or application authority.
//! Repair is permitted only after acquiring the OS lock and only when an empty
//! or strict-prefix marker is the directory's sole entry.

use crate::permissions::private_permissions;
use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Component, Path},
};
use swarm_contracts::error::Error;

const MAX_MARKER_BYTES: usize = 128;

#[derive(Debug)]
pub enum StateMarkerError {
    Busy,
    ForeignDirectory,
    InvalidMarker,
    System(Error),
}

impl From<std::io::Error> for StateMarkerError {
    fn from(error: std::io::Error) -> Self {
        Self::System(error.into())
    }
}

/// Open, lock and validate one ownership marker. A crash-created empty or
/// strict-prefix marker is repaired only while it remains the sole directory
/// entry. The returned File retains the lock for the caller's owner lifetime.
pub fn acquire_state_marker(
    canonical_directory: &Path,
    file_name: &str,
    expected_marker: &[u8],
) -> std::result::Result<File, StateMarkerError> {
    validate_inputs(canonical_directory, file_name, expected_marker)?;
    let directory = fs::canonicalize(canonical_directory)?;
    if directory != canonical_directory || !fs::metadata(&directory)?.is_dir() {
        return Err(StateMarkerError::System(Error::invalid(
            "state directory must be an existing canonical directory",
        )));
    }

    let marker_name = Path::new(file_name)
        .file_name()
        .ok_or_else(|| StateMarkerError::System(Error::invalid("invalid state marker name")))?;
    let marker_path = directory.join(marker_name);
    let fresh_candidate = match fs::symlink_metadata(&marker_path) {
        Ok(metadata) => {
            validate_marker_metadata(&metadata)?;
            false
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let empty = fs::read_dir(&directory)?.next().transpose()?.is_none();
            if empty {
                true
            } else {
                // A competing fresh owner may have created the marker between
                // the first metadata lookup and the directory scan. Recognize
                // only that exact regular marker; arbitrary entries remain
                // foreign state.
                match fs::symlink_metadata(&marker_path) {
                    Ok(metadata) => {
                        validate_marker_metadata(&metadata)?;
                        false
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Err(StateMarkerError::ForeignDirectory);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(error) => return Err(error.into()),
    };

    let mut file = if fresh_candidate {
        match open_marker(&marker_path, true) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                open_marker(&marker_path, false)?
            }
            Err(error) => return Err(error.into()),
        }
    } else {
        open_marker(&marker_path, false)?
    };

    match file.try_lock() {
        Ok(()) => {}
        Err(std::fs::TryLockError::WouldBlock) => return Err(StateMarkerError::Busy),
        Err(std::fs::TryLockError::Error(error)) => return Err(error.into()),
    }

    // Re-check the named path after the lock. On supported platforms the open
    // also refuses to follow the final symlink/reparse point.
    let metadata = fs::symlink_metadata(&marker_path)?;
    validate_marker_metadata(&metadata)?;

    let stored = read_bounded(&mut file, expected_marker.len() + 1)?;
    let marker_only = directory_contains_only(&directory, marker_name)?;
    let repairable_prefix =
        stored.len() < expected_marker.len() && expected_marker.starts_with(&stored) && marker_only;
    let repaired = if stored == expected_marker {
        false
    } else if repairable_prefix {
        repair_marker(&mut file, expected_marker)?;
        true
    } else {
        return Err(StateMarkerError::InvalidMarker);
    };

    private_permissions(&marker_path, false).map_err(StateMarkerError::System)?;
    if fresh_candidate || repaired || marker_only {
        // Syncing an exact marker-only directory also closes the window where
        // another process wrote the marker but died before syncing the parent.
        sync_directory(&directory)?;
    }
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

fn validate_inputs(
    directory: &Path,
    file_name: &str,
    expected_marker: &[u8],
) -> std::result::Result<(), StateMarkerError> {
    if !directory.is_absolute() {
        return Err(StateMarkerError::System(Error::invalid(
            "state directory must be absolute",
        )));
    }
    let name_path = Path::new(file_name);
    let mut components = name_path.components();
    let valid_name = matches!(
        components.next(),
        Some(Component::Normal(name)) if !name.is_empty() && name_path.as_os_str() == name
    ) && components.next().is_none();
    if !valid_name || expected_marker.is_empty() || expected_marker.len() > MAX_MARKER_BYTES {
        return Err(StateMarkerError::System(Error::invalid(
            "state marker name or bytes are invalid",
        )));
    }
    Ok(())
}

fn open_marker(path: &Path, create_new: bool) -> std::io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).truncate(false);
    if create_new {
        options.create_new(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    options.open(path)
}

fn validate_marker_metadata(metadata: &fs::Metadata) -> std::result::Result<(), StateMarkerError> {
    if metadata.file_type().is_symlink() || is_reparse(metadata) || !metadata.is_file() {
        return Err(StateMarkerError::InvalidMarker);
    }
    Ok(())
}

#[cfg(windows)]
fn is_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_reparse(_metadata: &fs::Metadata) -> bool {
    false
}

fn read_bounded(file: &mut File, limit: usize) -> std::io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::with_capacity(limit);
    file.take(limit as u64).read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn directory_contains_only(directory: &Path, marker_name: &OsStr) -> std::io::Result<bool> {
    let mut entries = fs::read_dir(directory)?;
    let Some(entry) = entries.next().transpose()? else {
        return Ok(false);
    };
    Ok(entry.file_name() == marker_name && entries.next().transpose()?.is_none())
}

fn repair_marker(file: &mut File, expected_marker: &[u8]) -> std::io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.set_len(0)?;
    file.write_all(expected_marker)?;
    file.sync_all()?;
    let readback = read_bounded(file, expected_marker.len() + 1)?;
    if readback != expected_marker {
        return Err(std::io::Error::other(
            "state marker readback differs after repair",
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(directory: &Path) -> std::io::Result<()> {
    File::open(directory)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_directory: &Path) -> std::io::Result<()> {
    Ok(())
}
