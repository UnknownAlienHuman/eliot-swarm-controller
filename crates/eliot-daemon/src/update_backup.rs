//! Own the private temporary Store copy and the rollback/admission boundary.
use crate::platform::data_dir::{
    DaemonInfo, DaemonLock, DataDir, create_new_private_file, replace_file, sync_directory,
    validate_private_file,
};
use eliot_core::{Error, Result};
use eliot_store::Store;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

pub fn update_backup_path(root: &Path, id: &str) -> Result<PathBuf> {
    let ulid: ulid::Ulid = id
        .parse()
        .map_err(|_| Error::new("invalid", "update id must be a ULID"))?;
    if ulid.to_string() != id {
        return Err(Error::new("invalid", "update id must be a canonical ULID"));
    }
    Ok(root.join(format!(".eliot.db.update-{id}.tmp")))
}

pub(crate) fn create(root: &Path, id: &str, store: &Store) -> Result<()> {
    let path = update_backup_path(root, id)?;
    let needed = store.online_backup_bytes()?;
    let available = available_space(root)?;
    if available < needed {
        return Err(Error::new(
            "insufficient_space",
            format!("Store backup needs {needed} bytes; {available} bytes available"),
        ));
    }
    drop(create_new_private_file(&path).map_err(|error| io_error("create Store backup", error))?);
    if let Err(error) = store
        .online_backup(&path)
        .and_then(|()| sync_directory(root))
    {
        let _ = fs::remove_file(&path);
        return Err(error);
    }
    Ok(())
}

fn available_space(path: &Path) -> Result<u64> {
    #[cfg(windows)]
    return crate::platform::windows::available_disk_space(path);
    #[cfg(unix)]
    return crate::platform::unix::available_disk_space(path);
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "unsupported",
            "cannot check Store backup disk space",
        ))
    }
}

/// The replacement holds this lock only while publishing its admission
/// witness. A timed-out launcher uses the same lock before terminating an
/// unadmitted candidate, so publication and termination cannot cross.
pub fn lock_update_publication(root: &Path, id: &str) -> Result<File> {
    let path = update_backup_path(root, id)?;
    validate_private_file(&path)?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| io_error("open Store backup", error))?;
    file.try_lock().map_err(|_| {
        Error::new(
            "operator_busy",
            "update admission publication is in progress",
        )
    })?;
    Ok(file)
}

pub fn restore_update_backup(
    root: &Path,
    id: &str,
    previous: &DaemonInfo,
    lock: &DaemonLock,
) -> Result<()> {
    let data = DataDir::open_existing(root)?;
    data.check_lock(lock)?;
    // daemon.json is written before any boot recovery worker or RPC can admit
    // work. Under the exclusive daemon lock it cannot change during restore.
    if data.read_daemon_info()?.as_ref() != Some(previous) {
        return Err(Error::new(
            "admission_unknown",
            "replacement admission cannot be ruled out; Store backup retained",
        ));
    }
    let backup = update_backup_path(root, id)?;
    validate_private_file(&backup)?;
    let live = root.join("eliot.db");
    existing_store_file(&live)?;
    for name in ["eliot.db-wal", "eliot.db-shm", "eliot.db-journal"] {
        let path = root.join(name);
        if existing_store_file(&path)? {
            fs::remove_file(path)
                .map_err(|error| io_error("remove replacement Store sidecar", error))?;
        }
    }
    replace_file(&backup, &live)?;
    sync_directory(root)
}

fn existing_store_file(path: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(io_error("inspect replacement Store file", error)),
    };
    // SQLite-created files can inherit the already validated private root ACL.
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        if metadata.file_attributes() & 0x400 != 0 {
            return Err(Error::new(
                "unsafe_data_dir",
                "Store file is a reparse point",
            ));
        }
    }
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return Err(Error::new(
            "unsafe_data_dir",
            "Store path is not a regular file",
        ));
    }
    Ok(true)
}

pub fn discard_update_backup(root: &Path, id: &str) -> Result<()> {
    let path = update_backup_path(root, id)?;
    match fs::symlink_metadata(&path) {
        Ok(_) => {
            validate_private_file(&path)?;
            fs::remove_file(path).map_err(|error| io_error("remove Store backup", error))?;
            sync_directory(root)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error("inspect Store backup", error)),
    }
}

fn io_error(context: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{context}: {error}"))
}
