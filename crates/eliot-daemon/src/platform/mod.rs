//! Platform IPC, private data-dir and process-custody helpers (L1).
//!
//! Owner: platform slice. Daemon worker and `plugin_host` consume this API.

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
pub use data_dir::DataDir;
pub use ipc::{IpcListener, IpcStream};
pub(crate) use process_observation::CommandClass;
pub use process_observation::CustodyObserver;

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
