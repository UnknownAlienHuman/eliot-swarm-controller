use eliot_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

const OWNER_TOKEN_FILE: &str = "owner.token";
const DAEMON_LOCK_FILE: &str = "daemon.lock";
const DAEMON_INFO_FILE: &str = "daemon.json";

/// The small local endpoint record consumed by the CLI during daemon discovery.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DaemonInfo {
    pub boot_id: String,
    pub pid: u32,
    pub process_birth: String,
    pub endpoint: String,
    pub diagnostic_version: String,
}

/// Private local state shared by the daemon and its clients.
///
/// Opening the directory validates and tightens its permissions. The daemon lock is
/// acquired separately so clients can read the owner token while the daemon is live.
pub struct DataDir {
    path: PathBuf,
    owner_token: String,
}

/// Holds the OS file lock for one daemon lifetime.
pub struct DaemonLock {
    root: PathBuf,
    _file: File,
}

impl DataDir {
    /// Open or initialize a private data root and its persistent owner token.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = absolute_path(path.as_ref())?;
        ensure_private_root(&path)?;
        let owner_token = load_or_create_owner_token(&path)?;
        Ok(Self { path, owner_token })
    }

    /// Open a previously initialized data root for a client without creating
    /// or opening daemon.lock, the database, or any missing root/token files.
    pub fn open_existing(path: impl AsRef<Path>) -> Result<Self> {
        let path = absolute_path(path.as_ref())?;
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(unavailable("data directory does not exist"));
            }
            Err(error) => return Err(io_error("inspect existing data directory", error)),
            Ok(_) => {}
        }
        validate_private_root(&path)?;
        let owner_token = read_existing_owner_token(&path)?;
        Ok(Self { path, owner_token })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the bearer token from the current-user-only owner.token file.
    pub fn owner_token(&self) -> &str {
        &self.owner_token
    }

    /// Acquire the single-daemon lock. The returned guard releases it on drop.
    pub fn acquire_daemon_lock(&self) -> Result<DaemonLock> {
        let path = self.path.join(DAEMON_LOCK_FILE);
        let file = open_or_create_daemon_lock(&path)?;
        ensure_private_file(&path)?;

        match file.try_lock() {
            Ok(()) => Ok(DaemonLock {
                root: self.path.clone(),
                _file: file,
            }),
            Err(std::fs::TryLockError::WouldBlock) => Err(Error::new(
                "daemon_already_running",
                "another daemon holds this data-directory lock",
            )),
            Err(std::fs::TryLockError::Error(error)) => {
                Err(io_error("lock daemon data directory", error))
            }
        }
    }

    /// Atomically publish this daemon's boot, process-birth and endpoint identity.
    pub fn publish_daemon(
        &self,
        lock: &DaemonLock,
        endpoint: &str,
        diagnostic_version: &str,
    ) -> Result<DaemonInfo> {
        self.check_lock(lock)?;
        validate_text("endpoint", endpoint, 4096)?;
        validate_text("diagnostic_version", diagnostic_version, 128)?;

        #[cfg(any(windows, unix))]
        {
            #[cfg(windows)]
            let (boot_id, process_birth) = (
                super::windows::host_boot_id()?,
                super::windows::current_process_birth()?,
            );
            #[cfg(unix)]
            let (boot_id, process_birth) = (
                super::unix::host_boot_id()?,
                super::unix::current_process_birth()?,
            );

            let info = DaemonInfo {
                boot_id,
                pid: std::process::id(),
                process_birth,
                endpoint: endpoint.to_owned(),
                diagnostic_version: diagnostic_version.to_owned(),
            };
            let bytes = serde_json::to_vec_pretty(&info)
                .map_err(|error| Error::new("daemon_metadata", error.to_string()))?;
            let target = self.path.join(DAEMON_INFO_FILE);
            match fs::symlink_metadata(&target) {
                Ok(_) => ensure_private_file(&target)?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error("inspect daemon metadata", error)),
            }

            let temporary = self
                .path
                .join(format!(".daemon.json.{}.tmp", ulid::Ulid::new()));
            let mut temporary_created = false;
            let result = (|| {
                let mut file = create_new_private_file(&temporary)
                    .map_err(|error| io_error("create daemon metadata", error))?;
                temporary_created = true;
                ensure_private_file(&temporary)?;
                file.write_all(&bytes)
                    .map_err(|error| io_error("write daemon metadata", error))?;
                file.sync_all()
                    .map_err(|error| io_error("sync daemon metadata", error))?;
                drop(file);
                replace_file(&temporary, &target)?;
                ensure_private_file(&target)?;
                sync_directory(&self.path)?;
                Ok(info.clone())
            })();

            if result.is_err() && temporary_created {
                let _ = fs::remove_file(&temporary);
            }
            result
        }

        #[cfg(not(any(windows, unix)))]
        {
            Err(Error::new(
                "platform_unsupported",
                "daemon process identity is supported on Windows and Unix",
            ))
        }
    }

    /// Read daemon.json without taking or changing the daemon lock.
    pub fn read_daemon_info(&self) -> Result<Option<DaemonInfo>> {
        let path = self.path.join(DAEMON_INFO_FILE);
        match fs::symlink_metadata(&path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(io_error("inspect daemon metadata", error)),
            Ok(_) => {}
        }
        validate_private_file(&path)?;
        let bytes = fs::read(&path).map_err(|error| io_error("read daemon metadata", error))?;
        let info: DaemonInfo = serde_json::from_slice(&bytes)
            .map_err(|error| Error::new("daemon_metadata", error.to_string()))?;
        validate_daemon_info(&info)?;
        Ok(Some(info))
    }

    pub(crate) fn check_lock(&self, lock: &DaemonLock) -> Result<()> {
        if lock.root == self.path {
            Ok(())
        } else {
            Err(Error::new(
                "invalid_daemon_lock",
                "daemon lock belongs to another data directory",
            ))
        }
    }

    /// Only the daemon holding this root's lock may create its private log directory.
    pub(crate) fn daemon_log_directory(&self, lock: &DaemonLock) -> Result<PathBuf> {
        self.check_lock(lock)?;
        validate_private_root(&self.path)?;
        let directory = self.path.join("logs");
        ensure_private_root(&directory)?;
        validate_private_root(&directory)?;
        Ok(directory)
    }
}

/// Creation uses the same TokenUser/protected-DACL path as owner.token on Windows.
/// Existing files must already be private; never take ownership or follow a link.
pub(crate) fn open_private_log_file(path: &Path) -> Result<File> {
    let parent = path
        .parent()
        .ok_or_else(|| Error::new("invalid_log_path", "log file has no private parent"))?;
    validate_private_root(parent)?;
    match create_new_private_file(path) {
        Ok(file) => drop(file),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(io_error("create private daemon log", error)),
    }
    validate_private_file(path)?;
    #[cfg(unix)]
    let mut options = private_file_options();
    #[cfg(not(unix))]
    let mut options = OpenOptions::new();
    let mut file = options
        .read(true)
        .write(true)
        .open(path)
        .map_err(|error| io_error("open private daemon log", error))?;
    validate_private_file(path)?;
    file.seek(SeekFrom::End(0))
        .map_err(|error| io_error("seek private daemon log", error))?;
    Ok(file)
}

/// The caller closes its log handle first, including on Windows. Replacement is
/// confined to the same validated private directory and an owned regular backup.
pub(crate) fn rotate_private_log_file(path: &Path, backup: &Path) -> Result<File> {
    let parent = path
        .parent()
        .filter(|parent| Some(*parent) == backup.parent() && path != backup)
        .ok_or_else(|| {
            Error::new(
                "invalid_log_path",
                "log rotation must stay in one directory",
            )
        })?;
    validate_private_root(parent)?;
    validate_private_file(path)?;
    match fs::symlink_metadata(backup) {
        Ok(_) => validate_private_file(backup)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("inspect daemon log backup", error)),
    }
    replace_file(path, backup)?;
    let file = open_private_log_file(path)?;
    sync_directory(parent)?;
    Ok(file)
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| io_error("resolve data directory", error))?
            .join(path)
    };
    if absolute
        .components()
        .any(|component| matches!(component, Component::ParentDir))
    {
        return Err(Error::new(
            "invalid_data_dir",
            "data directory paths may not contain parent traversal",
        ));
    }
    Ok(absolute)
}

fn load_or_create_owner_token(root: &Path) -> Result<String> {
    let path = root.join(OWNER_TOKEN_FILE);
    let token = format!("{}{}", ulid::Ulid::new(), ulid::Ulid::new());
    match create_new_private_file(&path) {
        Ok(mut file) => {
            if let Err(error) = ensure_private_file(&path) {
                let _ = fs::remove_file(&path);
                return Err(error);
            }
            if let Err(error) = file
                .write_all(token.as_bytes())
                .and_then(|()| file.sync_all())
            {
                let _ = fs::remove_file(&path);
                return Err(io_error("initialize owner token", error));
            }
            Ok(token)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            ensure_private_file(&path)?;
            read_existing_owner_token(root)
        }
        Err(error) => Err(io_error("create owner token", error)),
    }
}

fn read_existing_owner_token(root: &Path) -> Result<String> {
    let path = root.join(OWNER_TOKEN_FILE);
    match fs::symlink_metadata(&path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(unavailable("owner.token does not exist"));
        }
        Err(error) => return Err(io_error("inspect owner token", error)),
        Ok(_) => {}
    }
    validate_private_file(&path)?;
    let token = fs::read_to_string(&path).map_err(|error| io_error("read owner token", error))?;
    validate_owner_token(&token)?;
    Ok(token)
}

fn validate_owner_token(token: &str) -> Result<()> {
    if !(32..=128).contains(&token.len())
        || !token
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'.')
    {
        return Err(Error::new(
            "invalid_owner_token",
            "owner.token is malformed",
        ));
    }
    Ok(())
}

fn validate_daemon_info(info: &DaemonInfo) -> Result<()> {
    if info.pid == 0 {
        return Err(Error::new("daemon_metadata", "daemon PID must be positive"));
    }
    validate_text("boot_id", &info.boot_id, 256)?;
    validate_text("process_birth", &info.process_birth, 256)?;
    validate_text("endpoint", &info.endpoint, 4096)?;
    validate_text("diagnostic_version", &info.diagnostic_version, 128)
}

fn validate_text(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty() || value.len() > max_bytes || value.chars().any(char::is_control) {
        return Err(Error::new(
            "invalid_daemon_metadata",
            format!("{field} is empty or outside its supported boundary"),
        ));
    }
    Ok(())
}

#[cfg(unix)]
fn private_file_options() -> OpenOptions {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    options
}

fn create_new_private_file(path: &Path) -> std::io::Result<File> {
    #[cfg(windows)]
    {
        super::windows::create_private_file_new(path)
    }
    #[cfg(unix)]
    {
        let mut options = private_file_options();
        options.write(true).create_new(true);
        options.open(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "private files are unsupported on this platform",
        ))
    }
}

fn open_or_create_daemon_lock(path: &Path) -> Result<File> {
    #[cfg(windows)]
    {
        match create_new_private_file(path) {
            Ok(file) => Ok(file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                ensure_private_file(path)?;
                let file = OpenOptions::new()
                    .read(true)
                    .write(true)
                    .open(path)
                    .map_err(|error| io_error("open daemon lock", error))?;
                ensure_private_file(path)?;
                Ok(file)
            }
            Err(error) => Err(io_error("create daemon lock", error)),
        }
    }
    #[cfg(unix)]
    {
        let mut options = private_file_options();
        options.create(true).read(true).write(true);
        options
            .open(path)
            .map_err(|error| io_error("open daemon lock", error))
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "platform_unsupported",
            "daemon locks are unsupported on this platform",
        ))
    }
}

fn ensure_private_root(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        super::windows::ensure_private_root(path)
    }
    #[cfg(unix)]
    {
        super::unix::ensure_private_root(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "platform_unsupported",
            "private data-directory permissions are unsupported on this platform",
        ))
    }
}

fn ensure_private_file(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        super::windows::ensure_private_file(path)
    }
    #[cfg(unix)]
    {
        super::unix::ensure_private_file(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "platform_unsupported",
            "private file permissions are unsupported on this platform",
        ))
    }
}

fn validate_private_root(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        super::windows::validate_private_root(path)
    }
    #[cfg(unix)]
    {
        super::unix::validate_private_root(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "platform_unsupported",
            "private data-directory validation is unsupported on this platform",
        ))
    }
}

fn validate_private_file(path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        super::windows::validate_private_file(path)
    }
    #[cfg(unix)]
    {
        super::unix::validate_private_file(path)
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Err(Error::new(
            "platform_unsupported",
            "private file validation is unsupported on this platform",
        ))
    }
}

fn replace_file(source: &Path, target: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        super::windows::replace_file(source, target)
    }
    #[cfg(unix)]
    {
        fs::rename(source, target).map_err(|error| io_error("replace daemon metadata", error))
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync data directory", error))
    }
    #[cfg(windows)]
    {
        let _ = path;
        Ok(())
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = path;
        Ok(())
    }
}

fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{action}: {error}"))
}

fn unavailable(message: &str) -> Error {
    Error::new("data_dir_unavailable", message)
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    struct TestRoot {
        base: PathBuf,
        path: PathBuf,
    }

    impl TestRoot {
        fn new() -> Self {
            let base = fs::canonicalize(std::env::temp_dir()).expect("canonical temp directory");
            let path = base.join(format!("eliot-daemon-read-acl-{}", ulid::Ulid::new()));
            fs::create_dir(&path).expect("create private metadata test root");
            Self { base, path }
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            if self.path.parent() == Some(self.base.as_path())
                && self
                    .path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("eliot-daemon-read-acl-"))
            {
                let _ = fs::remove_dir_all(&self.path);
            }
        }
    }

    #[test]
    fn daemon_metadata_read_rejects_and_preserves_unsafe_acl() {
        let root = TestRoot::new();
        let data = DataDir::open(&root.path).expect("initialize private data directory");
        let metadata_path = data.path().join(DAEMON_INFO_FILE);
        let metadata = DaemonInfo {
            boot_id: "test-boot".to_owned(),
            pid: 1,
            process_birth: "test-birth".to_owned(),
            endpoint: "test-endpoint".to_owned(),
            diagnostic_version: "test-version".to_owned(),
        };
        fs::write(
            &metadata_path,
            serde_json::to_vec(&metadata).expect("encode fixture daemon metadata"),
        )
        .expect("write fixture daemon metadata");
        super::super::windows::set_test_untrusted_file_acl(&metadata_path)
            .expect("install deterministic Everyone DACL");

        assert_eq!(
            validate_private_file(&metadata_path)
                .expect_err("fixture DACL must be rejected before read")
                .code,
            "unsafe_data_dir"
        );
        assert_eq!(
            data.read_daemon_info()
                .expect_err("read must not repair an unsafe metadata ACL")
                .code,
            "unsafe_data_dir"
        );
        assert_eq!(
            validate_private_file(&metadata_path)
                .expect_err("read must leave the unsafe DACL in place")
                .code,
            "unsafe_data_dir"
        );
    }
}
