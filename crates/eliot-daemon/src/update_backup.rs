//! Own the private temporary Store copy and the rollback/admission boundary.
use crate::platform::data_dir::{
    DaemonInfo, DaemonLock, DataDir, create_new_private_file, replace_file, sync_directory,
    validate_private_file,
};
use eliot_core::{Error, Result};
use eliot_store::Store;
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

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
    let deadline = Instant::now() + Duration::from_secs(120);
    let path = update_backup_path(root, id)?;
    let needed = store.online_backup_bytes()?;
    let available = crate::platform::available_disk_space(root)?;
    if available < needed {
        return Err(Error::new(
            "insufficient_space",
            format!("Store backup needs {needed} bytes; {available} bytes available"),
        ));
    }
    drop(create_new_private_file(&path).map_err(|error| io_error("create Store backup", error))?);
    if let Err(error) = store.online_backup(&path).and_then(|()| {
        sync_directory(root)?;
        if Instant::now() >= deadline {
            return Err(Error::new(
                "backup_timeout",
                "Store backup and verification exceeded the restart budget",
            ));
        }
        Ok(())
    }) {
        if let Err(cleanup) = discard_update_backup(root, id) {
            return Err(Error::new(
                error.code,
                format!("{}; Store backup cleanup failed: {cleanup}", error.message),
            ));
        }
        return Err(error);
    }
    Ok(())
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
    let mut parked = Vec::new();
    let replacement = (|| {
        for (name, kind) in [
            ("eliot.db-wal", "wal"),
            ("eliot.db-shm", "shm"),
            ("eliot.db-journal", "journal"),
        ] {
            let path = root.join(name);
            let aside = backup.with_extension(format!("tmp.aside-{kind}"));
            require_absent(&aside)?;
            if existing_store_file(&path)? {
                fs::rename(&path, &aside)
                    .map_err(|error| io_error("park replacement Store sidecar", error))?;
                parked.push((path, aside));
            }
        }
        sync_directory(root)?;
        replace_file(&backup, &live).map_err(|error| {
            Error::new(error.code, format!("replace live Store: {}", error.message))
        })
    })();
    if let Err(error) = replacement {
        return Err(restore_sidecars(root, parked, error));
    }
    // After replacement, these sidecars belong to the displaced database.
    // Keep them until the new directory entry is durable.
    sync_directory(root)?;
    for (_, aside) in parked {
        fs::remove_file(&aside).map_err(|error| {
            io_error(
                &format!("remove parked Store sidecar {}", aside.display()),
                error,
            )
        })?;
    }
    sync_directory(root)
}

fn require_absent(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Ok(_) => Err(Error::new(
            "sidecar_exists",
            format!("Store sidecar path already exists: {}", path.display()),
        )),
        Err(error) => Err(io_error("inspect Store sidecar path", error)),
    }
}

fn restore_sidecars(root: &Path, parked: Vec<(PathBuf, PathBuf)>, error: Error) -> Error {
    let mut failures = Vec::new();
    for (path, aside) in parked.into_iter().rev() {
        let restored = require_absent(&path).and_then(|()| {
            fs::rename(&aside, &path).map_err(|error| io_error("restore Store sidecar", error))
        });
        if let Err(failure) = restored {
            failures.push(format!("{} retained: {failure}", aside.display()));
        }
    }
    if let Err(failure) = sync_directory(root) {
        failures.push(failure.to_string());
    }
    if failures.is_empty() {
        error
    } else {
        Error::new(
            error.code,
            format!(
                "{}; could not fully restore sidecars: {}",
                error.message,
                failures.join("; ")
            ),
        )
    }
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
    let mut removed = false;
    for suffix in ["-wal", "-shm", "-journal", ""] {
        let mut file = path.as_os_str().to_os_string();
        file.push(suffix);
        let file = PathBuf::from(file);
        if existing_store_file(&file)? {
            if suffix.is_empty() {
                validate_private_file(&file)?;
            }
            fs::remove_file(file).map_err(|error| io_error("remove Store backup file", error))?;
            removed = true;
        }
    }
    if removed {
        sync_directory(root)?;
    }
    Ok(())
}

fn io_error(context: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{context}: {error}"))
}
