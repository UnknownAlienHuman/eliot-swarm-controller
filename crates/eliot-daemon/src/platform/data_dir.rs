use eliot_core::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Component, Path, PathBuf};

const OWNER_TOKEN_FILE: &str = "owner.token";
const DAEMON_LOCK_FILE: &str = "daemon.lock";
#[cfg(not(windows))]
const OPERATOR_LOCK_FILE: &str = "operator.lock";
const DAEMON_INFO_FILE: &str = "daemon.json";
/// Present from an Owner's `daemon.shutdown` until the next daemon start:
/// the daemon stopped because the Owner asked, not because it failed.
const OWNER_STOP_FILE: &str = "owner-stop";
const MAX_SELECTED_ROOT_BYTES: usize = 32 * 1024;

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
/// Opening an existing directory validates its permissions without changing them;
/// a new directory is created privately. The daemon lock is acquired separately so
/// clients can read the owner token while the daemon is live.
pub struct DataDir {
    path: PathBuf,
    owner_token: Option<String>,
}

/// Holds the OS file lock for one daemon lifetime.
pub struct DaemonLock {
    root: PathBuf,
    _file: File,
}

#[cfg(windows)]
pub type OperatorLock = std::os::windows::io::OwnedHandle;
#[cfg(not(windows))]
pub type OperatorLock = File;

impl DataDir {
    /// Open or initialize a private data root and its persistent owner token.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = absolute_path(path.as_ref())?;
        ensure_private_root(&path).map_err(|error| {
            if error.code == "unsafe_data_dir" {
                unsafe_data_dir_error(&path)
            } else {
                error
            }
        })?;
        let owner_token = load_or_create_owner_token(&path)?;
        Ok(Self {
            path,
            owner_token: Some(owner_token),
        })
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
        validate_private_root(&path).map_err(|error| {
            if error.code == "unsafe_data_dir" {
                unsafe_data_dir_error(&path)
            } else {
                error
            }
        })?;
        Ok(Self {
            path,
            owner_token: None,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read the bearer token from the current-user-only owner.token file.
    pub fn owner_token(&self) -> &str {
        self.owner_token
            .as_deref()
            .expect("owner token is present when opened for daemon")
    }

    /// Read and validate the bearer token from owner.token for a client.
    pub fn read_owner_token(&self) -> Result<String> {
        read_existing_owner_token(&self.path)
    }

    /// Acquire the single-daemon lock. The returned guard releases it on drop.
    pub fn acquire_daemon_lock(&self) -> Result<DaemonLock> {
        let path = self.path.join(DAEMON_LOCK_FILE);
        let file = open_or_create_lock_file(&path)?;
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

    #[cfg(windows)]
    pub(crate) fn acquire_operator_lock(&self, _exclusive: bool) -> Result<OperatorLock> {
        super::windows::acquire_operator_event(&self.path)
    }

    #[cfg(not(windows))]
    pub(crate) fn acquire_operator_lock(&self, exclusive: bool) -> Result<OperatorLock> {
        let path = self.path.join(OPERATOR_LOCK_FILE);
        let file = open_or_create_lock_file(&path)?;
        ensure_private_file(&path)?;
        let acquired = if exclusive {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        match acquired {
            Ok(()) => Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => Err(Error::new(
                "operator_busy",
                "another local operator or daemon startup holds this data directory",
            )),
            Err(std::fs::TryLockError::Error(error)) => {
                Err(io_error("lock local operator operation", error))
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

    /// A starting daemon clears the previous Owner stop.
    pub(crate) fn clear_owner_stop(&self, lock: &DaemonLock) -> Result<()> {
        self.check_lock(lock)?;
        match fs::remove_file(self.path.join(OWNER_STOP_FILE)) {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                Err(io_error("clear owner stop marker", error))
            }
            _ => Ok(()),
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

fn unsafe_data_dir_error(path: &Path) -> Error {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or("Eliot");
    let fresh_path = parent.join(format!("{name}-{}", ulid::Ulid::new()));
    // PowerShell single-quoted literals double embedded apostrophes.
    let command_path = fresh_path.to_string_lossy().replace('\'', "''");
    Error::new(
        "unsafe_data_dir",
        format!(
            "the selected data directory is unsafe and was not changed; create a fresh private directory with `swarm --data-dir '{command_path}' daemon` (the desktop offers the same remedy)"
        ),
    )
}

/// Record, in the serving daemon's validated root, that the Owner asked it to
/// stop (`daemon.shutdown`, drain or force). Written while the daemon still
/// serves, so a client that finds it gone also finds the marker.
pub(crate) fn mark_owner_stop(root: &Path) -> Result<()> {
    replace_file_atomically(root, OWNER_STOP_FILE, b"owner-stop\n", true)
}

/// Whether this root's daemon was last stopped by its Owner and has not
/// started since. A desktop does not restart such a daemon on its own.
pub fn owner_stopped(root: &Path) -> bool {
    DataDir::open_existing(root).is_ok_and(|root| root.path().join(OWNER_STOP_FILE).is_file())
}

/// Persist a user-selected private data root so CLI and desktop launches share it.
/// The pointer lives outside the data root, allowing it to redirect away from an
/// unsafe default without changing that directory.
pub fn persist_selected_root(path: &Path) -> Result<()> {
    persist_selected_root_at(path, &selected_root_pointer_dir()?)
}

fn persist_selected_root_at(path: &Path, pointer_dir: &Path) -> Result<()> {
    let selected = DataDir::open_existing(path)?;
    ensure_private_root(pointer_dir)?;
    validate_private_root(pointer_dir)?;
    let root = selected.path().to_str().ok_or_else(|| {
        Error::new(
            "invalid_data_dir_selection",
            "selected data directory path is not valid UTF-8",
        )
    })?;
    replace_file_atomically(pointer_dir, "selected-data-dir", root.as_bytes(), true)
}

/// Resolve the durable root selection, if the user has made one.
pub fn resolve_selected_root() -> Result<Option<PathBuf>> {
    resolve_selected_root_at(&selected_root_pointer_dir()?)
}

fn resolve_selected_root_at(pointer_dir: &Path) -> Result<Option<PathBuf>> {
    match fs::symlink_metadata(pointer_dir) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("inspect selected-root directory", error)),
        Ok(_) => validate_private_root(pointer_dir)?,
    }
    let pointer = pointer_dir.join("selected-data-dir");
    match fs::symlink_metadata(&pointer) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error("inspect selected-root pointer", error)),
        Ok(_) => {
            validate_private_file(&pointer)?;
            let bytes = read_private_bytes(&pointer, MAX_SELECTED_ROOT_BYTES).map_err(|_| {
                Error::new(
                    "invalid_data_dir_selection",
                    "selected-root pointer is unsafe or exceeds its size limit",
                )
            })?;
            let text = std::str::from_utf8(&bytes).map_err(|_| {
                Error::new(
                    "invalid_data_dir_selection",
                    "selected-root pointer is not UTF-8",
                )
            })?;
            let path = PathBuf::from(text);
            if !path.is_absolute() || text.contains(['\n', '\r', '\0']) {
                return Err(Error::new(
                    "invalid_data_dir_selection",
                    "selected-root pointer is not an absolute path",
                ));
            }
            DataDir::open_existing(&path)?;
            Ok(Some(path))
        }
    }
}

/// Resolve the data root shared by CLI and desktop startup. Explicit arguments
/// and `ELIOT_HOME` take precedence over the durable user selection and default.
pub fn resolve_data_dir(explicit: Option<&Path>) -> Result<PathBuf> {
    let env_home = std::env::var_os("ELIOT_HOME").filter(|value| !value.is_empty());
    resolve_data_dir_with(
        explicit,
        env_home.as_deref(),
        resolve_selected_root,
        default_data_dir,
    )
}

fn resolve_data_dir_with(
    explicit: Option<&Path>,
    env_home: Option<&std::ffi::OsStr>,
    selected: impl FnOnce() -> Result<Option<PathBuf>>,
    default: impl FnOnce() -> Result<PathBuf>,
) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return absolute_path(path);
    }
    if let Some(path) = env_home {
        return absolute_path(Path::new(path));
    }
    if let Some(path) = selected()? {
        return absolute_path(&path);
    }
    default()
}

pub(crate) fn default_data_dir() -> Result<PathBuf> {
    #[cfg(windows)]
    let path = std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .map(|path| path.join("Eliot"));
    #[cfg(unix)]
    let path = {
        let xdg = std::env::var_os("XDG_DATA_HOME").filter(|value| !value.is_empty());
        match xdg.map(PathBuf::from) {
            Some(path) if path.is_absolute() => Some(path.join("eliot")),
            Some(_) => {
                return Err(Error::new(
                    "data_dir_unavailable",
                    "XDG_DATA_HOME must be absolute",
                ));
            }
            None => std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
                .map(|path| path.join(".local").join("share").join("eliot")),
        }
    };
    #[cfg(not(any(windows, unix)))]
    let path: Option<PathBuf> = None;
    path.map(|path| absolute_path(&path))
        .transpose()?
        .ok_or_else(|| {
            unavailable("cannot determine the data directory; set --data-dir or ELIOT_HOME")
        })
}

fn selected_root_pointer_dir() -> Result<PathBuf> {
    #[cfg(windows)]
    let base = std::env::var_os("LOCALAPPDATA")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .ok_or_else(|| Error::new("data_dir_unavailable", "LOCALAPPDATA is not set"))?;
    #[cfg(unix)]
    let base = {
        let configured = std::env::var_os("XDG_CONFIG_HOME").filter(|value| !value.is_empty());
        let home = std::env::var_os("HOME").filter(|value| !value.is_empty());
        match configured.map(PathBuf::from) {
            Some(path) if path.is_absolute() => path,
            Some(_) => {
                return Err(Error::new(
                    "data_dir_unavailable",
                    "XDG_CONFIG_HOME must be absolute",
                ));
            }
            None => PathBuf::from(
                home.ok_or_else(|| Error::new("data_dir_unavailable", "HOME is not set"))?,
            )
            .join(".config"),
        }
    };
    #[cfg(not(any(windows, unix)))]
    let base = return Err(Error::new(
        "platform_unsupported",
        "selected data roots are unsupported on this platform",
    ));
    #[cfg(windows)]
    let dirname = "Eliot-bootstrap";
    #[cfg(unix)]
    let dirname = "eliot-bootstrap";
    Ok(base.join(dirname))
}

/// Create or validate a private data directory before an explicit first use.
///
/// This is the safe remedy for an unsafe default root: callers must pass the
/// user's chosen fresh path. It does not repair, inspect, or copy an unsafe
/// existing root.
pub(crate) fn ensure_private_data_dir(path: impl AsRef<Path>) -> Result<PathBuf> {
    let data_dir = DataDir::open(path)?;
    Ok(data_dir.path().to_path_buf())
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

pub(crate) fn read_existing_owner_token(root: &Path) -> Result<String> {
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

pub(crate) fn create_new_private_file(path: &Path) -> std::io::Result<File> {
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

fn open_or_create_lock_file(path: &Path) -> Result<File> {
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

pub(crate) fn ensure_private_root(path: &Path) -> Result<()> {
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

pub(crate) fn ensure_private_file(path: &Path) -> Result<()> {
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

/// Atomically replace one private file directly under a private data
/// directory (used for `config.toml` profile saves). See
/// [`replace_file_atomically`].
pub(crate) fn replace_private_file(dir: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    replace_file_atomically(dir, name, bytes, true)
}

/// The daemon is the only writer of the private MCP handoff. Scan only while
/// holding its lock and before taking commands, so a live write cannot lose
/// its temporary file. The plugin receives only the published file.
pub(crate) fn remove_abandoned_mcp_temps(data: &DataDir, lock: &DaemonLock) -> Result<()> {
    if lock.root != data.path {
        return Err(Error::new(
            "invalid",
            "daemon lock belongs to another data directory",
        ));
    }
    let plugins = fs::canonicalize(data.path())
        .map_err(|error| io_error("resolve data directory", error))?
        .join("plugins");
    let metadata = match fs::symlink_metadata(&plugins) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(io_error("inspect plugin directory", error)),
    };
    if !metadata.is_dir() || fs::canonicalize(&plugins).ok().as_ref() != Some(&plugins) {
        return Err(Error::new("not_allowed", "plugin directory was redirected"));
    }
    let entries =
        fs::read_dir(&plugins).map_err(|error| io_error("read plugin directory", error))?;
    let prefix = format!(
        ".{}.",
        eliot_plugin::native_cli::ANTIGRAVITY_MCP_CONFIG_FILE
    );
    for entry in entries {
        let entry = entry.map_err(|error| io_error("read plugin instance", error))?;
        let name = entry.file_name();
        if !name.to_str().is_some_and(is_plugin_instance_name)
            || !entry.file_type().is_ok_and(|kind| kind.is_dir())
        {
            continue;
        }
        let instance = plugins.join(&name);
        if fs::canonicalize(&instance).ok().as_ref() != Some(&instance) {
            continue;
        }
        for file in
            fs::read_dir(&instance).map_err(|error| io_error("read plugin instance", error))?
        {
            let file = file.map_err(|error| io_error("read plugin instance entry", error))?;
            let name = file.file_name();
            let is_mcp_temp = name
                .to_str()
                .and_then(|name| name.strip_prefix(&prefix)?.strip_suffix(".tmp"))
                .is_some_and(|id| ulid::Ulid::from_string(id).is_ok());
            if is_mcp_temp && file.file_type().is_ok_and(|kind| kind.is_file()) {
                match fs::remove_file(file.path()) {
                    Ok(()) => (),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
                    Err(error) => {
                        return Err(io_error("remove abandoned MCP temporary file", error));
                    }
                }
            }
        }
    }
    Ok(())
}

fn is_plugin_instance_name(name: &str) -> bool {
    let Some((subject, suffix)) = name.rsplit_once('-') else {
        return false;
    };
    let Some((agent, generation)) = subject.rsplit_once("-g") else {
        return false;
    };
    !agent.is_empty()
        && agent
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_'))
        && generation.bytes().all(|c| c.is_ascii_digit())
        && generation.parse::<i64>().is_ok_and(|n| n > 0)
        && ulid::Ulid::from_string(suffix).is_ok()
}

/// Atomically replace one existing-or-new file directly under `dir`: a new
/// temporary file in the same directory is written and synced, then renamed
/// over the target. With `private`, both files must stay current-user-only
/// (data-directory files); without it the new file takes the directory's
/// inherited permissions (a registered project checkout). On failure before
/// the rename the previous file is untouched and the temporary is removed;
/// a failure after the rename is `unknown`, never "unchanged".
pub(crate) fn replace_file_atomically(
    dir: &Path,
    name: &str,
    bytes: &[u8],
    private: bool,
) -> Result<()> {
    validate_text("file name", name, 255)?;
    if name.contains(['/', '\\', ':']) || name.starts_with('.') {
        return Err(Error::new(
            "invalid",
            "replaced file name must be a plain file name",
        ));
    }
    #[cfg(any(windows, unix))]
    {
        let target = dir.join(name);
        match fs::symlink_metadata(&target) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(Error::new(
                    "not_allowed",
                    "replaced file must not be a link",
                ));
            }
            Ok(_) if private => ensure_private_file(&target)?,
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("inspect replaced file", error)),
        }
        let temporary = dir.join(format!(".{name}.{}.tmp", ulid::Ulid::new()));
        let mut temporary_created = false;
        let result = (|| {
            let mut file = if private {
                create_new_private_file(&temporary)
            } else {
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&temporary)
            }
            .map_err(|error| io_error("create temporary file", error))?;
            temporary_created = true;
            if private {
                ensure_private_file(&temporary)?;
            }
            file.write_all(bytes)
                .map_err(|error| io_error("write temporary file", error))?;
            file.sync_all()
                .map_err(|error| io_error("sync temporary file", error))?;
            drop(file);
            replace_file(&temporary, &target)?;
            let after = if private {
                ensure_private_file(&target)
            } else {
                Ok(())
            };
            after.and_then(|()| sync_directory(dir)).map_err(|error| {
                Error::new(
                    "unknown",
                    format!("{name} was replaced, then {}", error.message),
                )
            })
        })();
        if result.is_err() && temporary_created {
            let _ = fs::remove_file(&temporary);
        }
        result
    }
    #[cfg(not(any(windows, unix)))]
    {
        let _ = (dir, bytes, private);
        Err(Error::new(
            "platform_unsupported",
            "atomic file replacement is unsupported on this platform",
        ))
    }
}

pub(crate) fn validate_private_file(path: &Path) -> Result<()> {
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

/// Small private immutable inference artifacts. Callers supply only daemon-owned
/// names; existing links, nonregular files and oversized content fail closed.
pub(crate) fn read_private_bytes(path: &Path, max: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    validate_private_file(path)?;
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
        options.share_mode(windows_sys::Win32::Storage::FileSystem::FILE_SHARE_READ);
    }
    let file = options
        .open(path)
        .map_err(|e| io_error("open private inference file", e))?;
    let metadata = file
        .metadata()
        .map_err(|e| io_error("inspect private inference file", e))?;
    if !metadata.is_file() || super::is_reparse_point(&metadata) || metadata.len() > max as u64 {
        return Err(Error::new(
            "source_integrity",
            "invalid private inference artifact",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| io_error("read private inference file", e))?;
    if bytes.len() > max {
        return Err(Error::new(
            "source_integrity",
            "private inference artifact exceeds bound",
        ));
    }
    Ok(bytes)
}

pub(crate) fn replace_file(source: &Path, target: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        super::windows::replace_file(source, target)
    }
    #[cfg(unix)]
    {
        fs::rename(source, target).map_err(|error| io_error("replace daemon metadata", error))
    }
}

pub(crate) fn sync_directory(path: &Path) -> Result<()> {
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
