pub mod process_group;
use crate::error::{Error, Result};
use crate::model::{Credential, new_id};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

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
        let empty = std::fs::read_dir(&path)?.next().transpose()?.is_none();
        let lock_path = path.join("host.lock");
        // Never chmod/re-ACL an arbitrary existing folder because a caller mistyped
        // --data-dir. The marker is coordination, not a malicious-user boundary.
        if !empty && !lock_path.is_file() {
            return Err(Error::new(
                "FOREIGN_STATE_DIRECTORY",
                "choose an empty directory or an existing Swarm state directory",
            ));
        }
        let mut options = OpenOptions::new();
        options.read(true).write(true).truncate(false);
        if empty {
            options.create(true);
        }
        let mut lock = options.open(&lock_path)?;
        lock.try_lock()
            .map_err(|e| Error::new("HOST_ALREADY_RUNNING", e.to_string()))?;
        let mut marker = String::new();
        (&mut lock).take(128).read_to_string(&mut marker)?;
        const MARKER: &str = "ELIOT_SWARM_STATE_V1\n";
        if marker.is_empty() && empty {
            lock.write_all(MARKER.as_bytes())?;
            lock.sync_all()?;
        } else if marker != MARKER {
            return Err(Error::new(
                "FOREIGN_STATE_DIRECTORY",
                "host.lock is not this prototype's ownership marker",
            ));
        }
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
