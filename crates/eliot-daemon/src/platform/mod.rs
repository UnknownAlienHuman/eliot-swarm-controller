//! Platform IPC, private data-dir and process-custody helpers (L1).
//!
//! Owner: platform slice. Daemon worker and `plugin_host` consume this API.

pub mod custody;
pub mod data_dir;
pub mod ipc;
mod process_observation;

#[cfg(unix)]
pub(crate) mod unix;
#[cfg(windows)]
pub(crate) mod windows;

pub use custody::{CustodyGuarantee, OwnedChild};
pub use data_dir::DataDir;
pub use ipc::{IpcListener, IpcStream};
pub use process_observation::CustodyObserver;

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
