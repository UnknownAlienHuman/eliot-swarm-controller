use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use crate::error::{Error, Result};
use crate::model::{Credential, new_id};

#[cfg(windows)] pub mod windows;

/// The lock remains owned by the DB thread until the connection has been dropped.
pub struct DataRoot { pub path: PathBuf, pub lock: File }
impl DataRoot {
    pub fn acquire(path: &Path) -> Result<Self> {
        std::fs::create_dir_all(path)?;
        let path = std::fs::canonicalize(path)?;
        private_permissions(&path, true)?;
        let lock = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path.join("host.lock"))?;
        lock.try_lock().map_err(|e| Error::new("HOST_ALREADY_RUNNING", e.to_string()))?;
        Ok(Self { path, lock })
    }
}

/// Only touches the prototype's explicitly selected state or credential paths.
pub fn private_permissions(path: &Path, directory: bool) -> Result<()> {
    #[cfg(unix)] {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(if directory { 0o700 } else { 0o600 }))?;
    }
    #[cfg(windows)] windows::restrict_path(path, directory)?;
    Ok(())
}
pub fn write_private_new(path: &Path, data: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new(); options.write(true).create_new(true);
    #[cfg(unix)] {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut f = options.open(path)?;
    private_permissions(path, false)?;
    f.write_all(data)?; f.sync_all()?;
    Ok(())
}
pub fn load_credential(path: &Path) -> Result<Credential> {
    let c: Credential = serde_json::from_slice(&std::fs::read(path)?)?;
    if c.client_id.is_empty() || c.token.len() < 32 { return Err(Error::new("AUTH_ERROR", "invalid credential file")); }
    Ok(c)
}
pub fn bootstrap_credential(root: &Path) -> Result<Credential> {
    let file = root.join("operator.json");
    if file.try_exists()? { return load_credential(&file); }
    // Never silently replace the authority of an existing database.
    if root.join("swarm.db").try_exists()? {
        return Err(Error::new("CREDENTIAL_MISSING", "restore operator.json for the existing database; it is not regenerated"));
    }
    let credential = Credential { client_id: "operator".into(), token: format!("{}{}", new_id(), new_id()) };
    write_private_new(&file, &serde_json::to_vec_pretty(&credential)?)?;
    Ok(credential)
}
pub fn endpoint(root: &Path) -> Result<String> {
    let canonical = std::fs::canonicalize(root)?;
    #[cfg(windows)] {
        use crate::model::digest;
        Ok(format!(r"\\.\pipe\eliot-swarm-{}", &digest(canonical.as_os_str().to_string_lossy().to_lowercase().as_bytes())[..32]))
    }
    #[cfg(unix)] {
        let path = canonical.join("control.sock");
        use std::os::unix::ffi::OsStrExt;
        if path.as_os_str().as_bytes().len() > 100 { return Err(Error::invalid("data-dir too long for a Unix socket")); }
        path.to_str().map(str::to_owned).ok_or_else(|| Error::invalid("data-dir must be UTF-8 for local IPC discovery"))
    }
}
