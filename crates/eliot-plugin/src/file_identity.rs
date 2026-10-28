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

#[cfg(all(test, any(windows, target_os = "linux")))]
mod tests {
    use super::{file_final_path, file_identity};
    use std::{
        fs::{self, OpenOptions},
        io::{self, Write},
        path::PathBuf,
        time::{SystemTime, UNIX_EPOCH},
    };

    struct TemporaryDirectory(PathBuf);

    impl Drop for TemporaryDirectory {
        fn drop(&mut self) {
            // Created exclusively by this test under the canonical temp root.
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn held_handle_survives_rename_and_path_replacement() -> io::Result<()> {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(io::Error::other)?
            .as_nanos();
        let directory = std::env::temp_dir().canonicalize()?.join(format!(
            "eliot-file-identity-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir(&directory)?;
        let directory = TemporaryDirectory(directory);
        let original = directory.0.join("original.jsonl");
        let renamed = directory.0.join("renamed-λ-🧪.jsonl");
        let mut held = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&original)?;
        held.write_all(b"held object\n")?;
        let identity = file_identity(&held)?;
        assert_eq!(file_final_path(&held)?, original.canonicalize()?);

        fs::rename(&original, &renamed)?;
        let mut replacement = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&original)?;
        replacement.write_all(b"replacement object\n")?;

        assert_eq!(file_identity(&held)?, identity);
        assert_ne!(file_identity(&replacement)?, identity);
        assert_eq!(file_final_path(&held)?, renamed.canonicalize()?);
        assert_eq!(file_final_path(&replacement)?, original.canonicalize()?);
        Ok(())
    }
}
