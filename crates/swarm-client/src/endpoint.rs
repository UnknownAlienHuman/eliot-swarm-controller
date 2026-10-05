#[cfg(windows)]
use sha2::{Digest, Sha256};
use std::path::Path;
#[cfg(not(windows))]
use swarm_contracts::error::Error;
use swarm_contracts::error::Result;

/// Compute the existing local endpoint from a canonical data directory.
///
/// The Windows pipe suffix and Unix socket filename are persisted discovery
/// conventions; changing either would strand installed clients.
pub fn ipc_endpoint(root: &Path) -> Result<String> {
    let canonical = std::fs::canonicalize(root)?;
    #[cfg(windows)]
    {
        let path = canonical.as_os_str().to_string_lossy().to_lowercase();
        let digest = Sha256::digest(path.as_bytes());
        let suffix = digest[..16]
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(format!(r"\\.\pipe\eliot-swarm-{suffix}"))
    }
    #[cfg(unix)]
    {
        let path = canonical.join("control.sock");
        use std::os::unix::ffi::OsStrExt;
        if path.as_os_str().as_bytes().len() > 100 {
            return Err(Error::invalid("data-dir too long for a Unix socket"));
        }
        path.to_str()
            .map(str::to_owned)
            .ok_or_else(|| Error::invalid("data-dir must be UTF-8 for local IPC discovery"))
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = canonical;
        Err(Error::new(
            "HOST_UNAVAILABLE",
            "local IPC is supported only on Windows and Unix",
        ))
    }
}
