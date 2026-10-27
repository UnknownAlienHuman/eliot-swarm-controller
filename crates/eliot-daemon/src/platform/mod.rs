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
