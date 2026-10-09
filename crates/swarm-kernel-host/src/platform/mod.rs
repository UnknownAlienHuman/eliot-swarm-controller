pub mod process_group;
use crate::error::{Error, Result};
use crate::model::{Credential, new_id};
use std::fs::File;
use std::path::{Path, PathBuf};
use swarm_process::StateMarkerError;

#[cfg(windows)]
pub mod windows;

/// The lock remains owned by the DB thread until the connection has been dropped.
pub struct DataRoot {
    pub path: PathBuf,
    pub lock: File,
}
impl DataRoot {
    pub fn acquire(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let path = std::fs::canonicalize(path)?;
        const MARKER: &[u8] = b"ELIOT_SWARM_STATE_V1\n";
        let lock = match swarm_process::acquire_state_marker(&path, "host.lock", MARKER) {
            Ok(lock) => lock,
            Err(StateMarkerError::Busy) => {
                return Err(Error::new(
                    "HOST_ALREADY_RUNNING",
                    "state marker lock is already held",
                ));
            }
            Err(StateMarkerError::ForeignDirectory | StateMarkerError::InvalidMarker) => {
                return Err(Error::new(
                    "FOREIGN_STATE_DIRECTORY",
                    "choose an empty directory or an existing Swarm state directory",
                ));
            }
            Err(StateMarkerError::System(error)) => return Err(error.into()),
        };
        // Never chmod/re-ACL an arbitrary existing folder because a caller
        // mistyped --data-dir. Marker validation succeeds before this call.
        private_permissions(&path, true)?;
        Ok(Self { path, lock })
    }
}

/// Only touches the prototype's explicitly selected state or credential paths.
pub fn private_permissions(path: &Path, directory: bool) -> Result<()> {
    swarm_process::private_permissions(path, directory).map_err(Into::into)
}
pub fn write_private_new(path: &Path, data: &[u8]) -> Result<()> {
    swarm_process::write_private_new(path, data).map_err(Into::into)
}
pub fn load_credential(path: &Path) -> Result<Credential> {
    let c: Credential = serde_json::from_slice(&std::fs::read(path)?)?;
    if c.client_id.is_empty() || c.token.len() < 32 {
        return Err(Error::new("AUTH_ERROR", "invalid credential file"));
    }
    Ok(c)
}
pub fn bootstrap_credential(root: &Path) -> Result<Credential> {
    let file = root.join("operator.json");
    if file.try_exists()? {
        return load_credential(&file);
    }
    // Never silently replace the authority of an existing database.
    if root.join("swarm.db").try_exists()? {
        return Err(Error::new(
            "CREDENTIAL_MISSING",
            "restore operator.json for the existing database; it is not regenerated",
        ));
    }
    let credential = Credential {
        client_id: "operator".into(),
        token: format!("{}{}", new_id(), new_id()),
    };
    write_private_new(&file, &serde_json::to_vec_pretty(&credential)?)?;
    Ok(credential)
}
pub fn endpoint(root: &Path) -> Result<String> {
    swarm_client::ipc_endpoint(root).map_err(Into::into)
}
