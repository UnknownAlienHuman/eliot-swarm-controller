//! Platform IPC, private data-dir and process-custody helpers (L1).
//!
//! Owner: platform slice. Daemon worker and `plugin_host` consume this API.

#[cfg(not(any(windows, unix)))]
use eliot_core::Error;
use eliot_core::Result;
use std::path::Path;

pub mod custody;
pub mod data_dir;
pub mod ipc;
mod process_observation;
pub(crate) mod script_file;
pub(crate) mod worktree_guard;

#[cfg(unix)]
pub(crate) mod unix;
#[cfg(windows)]
pub(crate) mod windows;

pub use custody::{CustodyGuarantee, OwnedChild, RestartCustody, restart_custody};
pub(crate) use data_dir::repair_managed_dir_acl;
pub use data_dir::{DataDir, ForeignDataDirAce};
pub use ipc::{IpcListener, IpcStream};
pub(crate) use process_observation::CommandClass;
pub use process_observation::CustodyObserver;

/// Return free bytes available to the current owner on the filesystem that
/// contains `path`.
pub(crate) fn available_disk_space(path: &Path) -> Result<u64> {
    #[cfg(windows)]
    {
        windows::available_disk_space(path)
    }
    #[cfg(unix)]
    {
        unix::available_disk_space(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "unsupported",
            "free disk space is unavailable on this platform",
        ))
    }
}

/// Whether the owner process recorded beside a private daemon lock is still
/// alive. A reused PID reads as alive, which only keeps a directory.
pub(crate) fn lock_owner_process_alive(pid: u32) -> bool {
    #[cfg(windows)]
    {
        windows::lock_owner_process_alive(pid)
    }
    #[cfg(not(windows))]
    {
        unix::lock_owner_process_alive(pid)
    }
}

pub(crate) fn is_reparse_point(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

        metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        let _ = metadata;
        false
    }
}

/// Bounded live metadata for one inspected process node.
#[derive(Clone, Debug)]
pub(crate) struct ProcessNodeInfo {
    pub(crate) pid: u32,
    pub(crate) parent_pid: Option<u32>,
    pub(crate) image_path: Option<String>,
    pub(crate) command_line: Option<String>,
    pub(crate) argv: Vec<String>,
}

/// Inspect one process's parent PID, image path, and bounded command line/argv
/// for process-tree custody classification. Fails closed to `None` on exited/inaccessible PIDs.
pub(crate) fn inspect_process_node(pid: u32) -> Option<ProcessNodeInfo> {
    #[cfg(windows)]
    {
        windows::inspect_process_node(pid)
    }
    #[cfg(target_os = "linux")]
    {
        unix::inspect_process_node(pid)
    }
    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = pid;
        None
    }
}

#[cfg(windows)]
pub(crate) use windows::{PinnedProcess, pin_process};

#[cfg(not(windows))]
pub(crate) use unix::{PinnedProcess, pin_process};
