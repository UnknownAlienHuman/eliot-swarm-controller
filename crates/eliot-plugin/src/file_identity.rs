//! Identity and final path of the same open file used by an observation reader.

use serde::{Deserialize, Serialize};
use std::{fs::File, io, path::PathBuf};

#[cfg(windows)]
mod windows;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(tag = "platform", rename_all = "snake_case", deny_unknown_fields)]
pub enum FileIdentity {
    Windows { volume: u64, file_id: [u8; 16] },
    Unix { device: u64, inode: u64 },
}

/// Read identity from this handle, never by reopening its path.
/// Identity does not prove that file contents are append-only.
pub fn file_identity(file: &File) -> io::Result<FileIdentity> {
    #[cfg(windows)]
    {
        windows::file_identity(file)
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = file.metadata()?;
        Ok(FileIdentity::Unix {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = file;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "open-file identity is unavailable on this platform",
        ))
    }
}

/// Resolve the held file's final path without reopening a caller-supplied path.
/// The caller still checks containment and handles subsequent rename/replacement.
pub fn file_final_path(file: &File) -> io::Result<PathBuf> {
    #[cfg(windows)]
    {
        windows::file_final_path(file)
    }
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        std::fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = file;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "open-file final path is unavailable on this platform",
        ))
    }
}
