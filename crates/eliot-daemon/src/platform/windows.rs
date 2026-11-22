#![allow(
    unsafe_code,
    reason = "Windows process custody requires Job and process-handle FFI."
)]

use super::process_observation::{
    CommandClass, MAX_PROCESS_SAMPLES, exited_sample, same_identity, unknown_sample,
};
use eliot_core::{
    Error, ProcessIdentity, ProcessRecord, ProcessSample, ProcessSnapshot, ProcessState, Result,
    verbatim_path,
};
use eliot_plugin::sync::LockExt;
use std::ffi::{OsStr, c_void};
use std::fs;
use std::os::windows::ffi::OsStrExt;
use std::os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle};
use std::path::{Component, Path, PathBuf};
use std::ptr::{null, null_mut};
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use windows_sys::Win32::Foundation::{
    CloseHandle, DuplicateHandle, ERROR_ACCESS_DENIED, ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND,
    ERROR_INVALID_PARAMETER, ERROR_MORE_DATA, ERROR_SHARING_VIOLATION, FILETIME, GetLastError,
    HANDLE, HLOCAL, INVALID_HANDLE_VALUE, LocalFree, STILL_ACTIVE, SetLastError, WAIT_OBJECT_0,
    WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW, GetSecurityInfo,
    SE_FILE_OBJECT, SetSecurityInfo,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL_SIZE_INFORMATION, AclSizeInformation,
    CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
    GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetTokenInformation,
    OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    SE_DACL_PROTECTED, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    BY_HANDLE_FILE_INFORMATION, CREATE_NEW, CreateDirectoryW, CreateFileW, FILE_ALL_ACCESS,
    FILE_ATTRIBUTE_DIRECTORY, FILE_ATTRIBUTE_NORMAL, FILE_ATTRIBUTE_REPARSE_POINT,
    FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_FIRST_PIPE_INSTANCE, FILE_FLAG_OPEN_REPARSE_POINT,
    FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FILE_TYPE_DISK,
    GetDiskFreeSpaceExW, GetFileInformationByHandle, GetFileType, MOVEFILE_REPLACE_EXISTING,
    MOVEFILE_WRITE_THROUGH, MoveFileExW, OPEN_EXISTING, READ_CONTROL, WRITE_DAC,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob, JOB_OBJECT_LIMIT_ACTIVE_PROCESS,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_BASIC_PROCESS_ID_LIST,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicProcessIdList,
    JobObjectExtendedLimitInformation, OpenJobObjectW, QueryInformationJobObject,
    SetInformationJobObject, TerminateJobObject,
};
use windows_sys::Win32::System::Pipes::{
    CreateNamedPipeW, GetNamedPipeClientProcessId, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS,
    PIPE_TYPE_BYTE, PIPE_UNLIMITED_INSTANCES, PIPE_WAIT, WaitNamedPipeW,
};
use windows_sys::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows_sys::Win32::System::SystemInformation::{GetSystemTimeAsFileTime, GetTickCount64};
use windows_sys::Win32::System::Threading::{
    CREATE_BREAKAWAY_FROM_JOB, CREATE_NEW_PROCESS_GROUP, CreateEventW, CreateProcessW,
    DETACHED_PROCESS, GetCurrentProcess, GetExitCodeProcess, GetProcessTimes, OpenProcess,
    OpenProcessToken, PROCESS_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION, PROCESS_SYNCHRONIZE,
    PROCESS_TERMINATE, QueryFullProcessImageNameW, STARTUPINFOW, TerminateProcess,
    WaitForSingleObject,
};

/// Availability is only a connect hint: another client may win the instance.
/// This writes no request bytes and keeps the caller's original deadline.
/// https://learn.microsoft.com/windows/win32/api/namedpipeapi/nf-namedpipeapi-waitnamedpipew
pub(super) async fn wait_pipe_available(endpoint: &str, until: tokio::time::Instant) -> Result<()> {
    let endpoint = endpoint.to_owned();
    tokio::task::spawn_blocking(move || {
        let remaining = until.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(Error::new(
                "unavailable",
                "daemon IPC connect deadline elapsed before any request was sent",
            ));
        }
        // Zero selects the server default and MAX waits forever; use neither.
        let milliseconds = remaining.as_millis().clamp(1, u32::MAX as u128 - 1) as u32;
        let wide: Vec<u16> = endpoint.encode_utf16().chain(Some(0)).collect();
        // SAFETY: wide is NUL-terminated and lives through the bounded call.
        if unsafe { WaitNamedPipeW(wide.as_ptr(), milliseconds) } != 0 {
            return Ok(());
        }
        let error = std::io::Error::last_os_error();
        if error.raw_os_error() == Some(windows_sys::Win32::Foundation::ERROR_SEM_TIMEOUT as i32) {
            Err(Error::new(
                "unavailable",
                "daemon IPC connect deadline elapsed before any request was sent",
            ))
        } else {
            Err(io_error("wait for daemon named pipe", error))
        }
    })
    .await
    .map_err(|_| {
        Error::new(
            "unavailable",
            "daemon IPC availability wait failed before any request was sent",
        )
    })?
}

pub(crate) fn start_background_daemon(data_dir: &Path) -> Result<()> {
    let executable =
        std::env::current_exe().map_err(|error| io_error("locate Eliot executable", error))?;
    start_background_daemon_executable(&executable, data_dir)
}

pub(crate) fn acquire_operator_event(data_dir: &Path) -> Result<OwnedHandle> {
    let root = fs::canonicalize(data_dir)
        .map_err(|error| io_error("resolve operator data root", error))?;
    let identity = root.to_string_lossy().to_lowercase();
    let hash = eliot_core::api::inference::sha256_hex(identity.as_bytes());
    let name: Vec<u16> = format!(r"Global\EliotSwarmOperator.{hash}")
        .encode_utf16()
        .chain([0])
        .collect();
    let descriptor = private_security_descriptor(false)?;
    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    };
    // The single successful creator holds the name until it drops the handle.
    // Unlike a lock file in the root, this guard survives directory deletion.
    // SAFETY: both arguments stay live through CreateEventW. Last error is thread local.
    let (handle, error) = unsafe {
        SetLastError(0);
        let handle = CreateEventW(&attributes, 0, 0, name.as_ptr());
        (handle, GetLastError())
    };
    if handle.is_null() {
        return Err(io_error(
            "acquire operator event",
            std::io::Error::from_raw_os_error(error as i32),
        ));
    }
    // SAFETY: CreateEventW returned a uniquely owned non-null handle.
    let handle = unsafe { OwnedHandle::from_raw_handle(handle) };
    if error == ERROR_ALREADY_EXISTS {
        return Err(Error::new(
            "operator_busy",
            "another local operator or daemon startup holds this data directory",
        ));
    }
    Ok(handle)
}

pub(crate) fn start_background_daemon_executable(executable: &Path, data_dir: &Path) -> Result<()> {
    let mut command_line = windows_command_line(&[
        executable.as_os_str(),
        OsStr::new("--data-dir"),
        data_dir.as_os_str(),
        OsStr::new("daemon"),
        OsStr::new("--foreground"),
    ])?;
    let application = wide_path(executable);
    let startup = STARTUPINFOW {
        cb: std::mem::size_of::<STARTUPINFOW>() as u32,
        ..STARTUPINFOW::default()
    };
    let mut process = PROCESS_INFORMATION::default();
    // SAFETY: application and the writable command line are NUL-terminated UTF-16.
    // All structures live through the call. FALSE disables ALL handle inheritance,
    // including otherwise-unused capture-pipe writers held by the CLI. No standard
    // handles are supplied; DETACHED_PROCESS prevents console inheritance/creation.
    // A new process group separates console signaling where a console is attached.
    // BREAKAWAY prevents a caller in a kill-on-close Job from lending that Job's
    // lifetime to the independently owned daemon. A Job that forbids
    // breakaway causes an explicit start failure instead of a short-lived daemon.
    // Null environment/current-directory pointers preserve the caller's values.
    if unsafe {
        CreateProcessW(
            application.as_ptr(),
            command_line.as_mut_ptr(),
            null(),
            null(),
            0,
            DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP | CREATE_BREAKAWAY_FROM_JOB,
            null(),
            null(),
            &startup,
            &mut process,
        )
    } == 0
    {
        return Err(io_error(
            "start background daemon",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: successful CreateProcessW returned these two uniquely owned handles.
    // Closing them neither waits for nor terminates the independent daemon. No PID
    // is retained or adopted as later control authority.
    unsafe {
        CloseHandle(process.hThread);
        CloseHandle(process.hProcess);
    }
    Ok(())
}

pub(crate) fn windows_command_line(arguments: &[&OsStr]) -> Result<Vec<u16>> {
    let mut command_line = Vec::new();
    for argument in arguments {
        if !command_line.is_empty() {
            command_line.push(u16::from(b' '));
        }
        command_line.push(u16::from(b'"'));
        let mut backslashes = 0;
        for unit in argument.encode_wide() {
            if unit == 0 {
                return Err(Error::new("invalid", "daemon argument contains NUL"));
            }
            if unit == u16::from(b'\\') {
                backslashes += 1;
                continue;
            }
            // Windows argv parsing doubles backslashes before a quote, then
            // escapes that quote. Other backslashes are literal.
            let escapes = if unit == u16::from(b'"') {
                backslashes * 2 + 1
            } else {
                backslashes
            };
            command_line.extend(std::iter::repeat_n(u16::from(b'\\'), escapes));
            command_line.push(unit);
            backslashes = 0;
        }
        // Protect trailing backslashes from consuming the closing quote.
        command_line.extend(std::iter::repeat_n(u16::from(b'\\'), backslashes * 2));
        command_line.push(u16::from(b'"'));
    }
    if command_line.len() >= 32_767 {
        return Err(Error::new(
            "invalid",
            "daemon command line exceeds the Windows limit",
        ));
    }
    command_line.push(0);
    Ok(command_line)
}

struct LocalSecurityDescriptor(*mut c_void);

impl Drop for LocalSecurityDescriptor {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: this descriptor was allocated by the matching Windows API.
            unsafe { LocalFree(self.0 as HLOCAL) };
        }
    }
}

fn private_security_descriptor(directory: bool) -> Result<LocalSecurityDescriptor> {
    let user_sid = current_user_sid()?;
    let sid = sid_to_string(&user_sid)?;
    let sddl = if directory {
        format!("O:{sid}D:P(A;OICI;FA;;;{sid})")
    } else {
        format!("O:{sid}D:P(A;;FA;;;{sid})")
    };
    let sddl_wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
    let mut descriptor = null_mut();
    // SAFETY: sddl_wide is NUL-terminated and descriptor is writable storage.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl_wide.as_ptr(),
            1,
            &mut descriptor,
            null_mut(),
        )
    } == 0
    {
        return Err(io_error(
            "build current-user owner and ACL",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(LocalSecurityDescriptor(descriptor))
}

pub(crate) fn create_private_directory(path: &Path) -> Result<bool> {
    let descriptor = private_security_descriptor(true)?;
    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    };
    let path_wide = verbatim_file_path(path);
    // SAFETY: path and security descriptor remain alive through the call.
    if unsafe { CreateDirectoryW(path_wide.as_ptr(), &attributes) } != 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        Ok(false)
    } else {
        Err(io_error("create private data directory", error))
    }
}

pub(crate) fn create_private_file_new(path: &Path) -> std::io::Result<fs::File> {
    let descriptor = private_security_descriptor(false)
        .map_err(|error| std::io::Error::other(error.to_string()))?;
    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor.0,
        bInheritHandle: 0,
    };
    let path_wide = verbatim_file_path(path);
    // SAFETY: path and security descriptor remain alive through the call. CREATE_NEW
    // preserves existing-object validation and never opens or truncates an old file.
    let handle = unsafe {
        CreateFileW(
            path_wide.as_ptr(),
            FILE_ALL_ACCESS,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            &attributes,
            CREATE_NEW,
            FILE_ATTRIBUTE_NORMAL,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: CreateFileW returned a uniquely owned handle for the new file.
    Ok(unsafe { fs::File::from_raw_handle(handle) })
}

fn create_private_directories(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if matches!(
            component,
            Component::Prefix(_) | Component::RootDir | Component::CurDir
        ) {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(Error::new(
                        "unsafe_data_dir",
                        "data directory component is not a real directory",
                    ));
                }
                reject_reparse(&current)?;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let created = create_private_directory(&current)?;
                let metadata = fs::symlink_metadata(&current)
                    .map_err(|error| io_error("inspect created data directory", error))?;
                if metadata.file_type().is_symlink() || !metadata.is_dir() {
                    return Err(Error::new(
                        "unsafe_data_dir",
                        "created data directory component is not a real directory",
                    ));
                }
                reject_reparse(&current)?;
                validate_new_or_raced_directory(&current, created)?;
            }
            Err(error) => return Err(io_error("inspect data directory component", error)),
        }
    }
    Ok(())
}

fn validate_new_or_raced_directory(path: &Path, created: bool) -> Result<()> {
    let context = if created {
        "newly created"
    } else {
        "concurrently existing"
    };
    ensure_current_owner(path, true).map_err(|error| {
        Error::new(
            "unsafe_data_dir",
            format!("{context} data directory failed owner validation: {error}"),
        )
    })?;
    validate_current_user_acl(path, true).map_err(|error| {
        Error::new(
            "unsafe_data_dir",
            format!("{context} data directory failed ACL validation: {error}"),
        )
    })
}

pub(crate) fn ensure_private_root(path: &Path) -> Result<()> {
    check_no_reparse_components(path)?;
    match fs::symlink_metadata(path) {
        Ok(_) => return validate_private_root(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("inspect data directory", error)),
    }
    create_private_directories(path)?;
    check_no_reparse_components(path)?;

    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("inspect data directory", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "unsafe_data_dir",
            "data directory must be a real directory, not a reparse point or file",
        ));
    }
    validate_private_root(path)
}

pub(crate) fn ensure_private_file(path: &Path) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("inspect private file", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::new(
            "unsafe_private_file",
            "private data files must be regular files, not reparse points or devices",
        ));
    }
    reject_reparse(path)?;
    ensure_current_owner(path, false)?;
    set_current_user_acl(path, false)
}

pub(crate) fn validate_private_root(path: &Path) -> Result<()> {
    check_no_reparse_components(path)?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error("inspect existing data directory", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "unsafe_data_dir",
            "data directory must be a real directory, not a reparse point or file",
        ));
    }
    validate_current_user_acl(path, true)
}

pub(crate) fn validate_private_file(path: &Path) -> Result<()> {
    reject_reparse(path)?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| io_error("inspect existing private file", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(Error::new(
            "unsafe_private_file",
            "private data files must be regular files, not reparse points or devices",
        ));
    }
    validate_current_user_acl(path, false)
}

/// How long `replace_file` waits out readers that hold the target open.
const REPLACE_CONTENTION: std::time::Duration = std::time::Duration::from_secs(2);

/// Replace `target` with `source`. Any open handle on the target, even one that
/// shares delete access, makes `MoveFileExW` fail with `ERROR_ACCESS_DENIED`, and
/// CLI clients read `daemon.json` while a restarting daemon republishes it. Those
/// reads are brief, so the local, idempotent rename is retried for a bounded time.
/// This blocks the calling thread for at most `REPLACE_CONTENTION`: once at daemon
/// startup, and on log rotation, whose file only the daemon itself normally opens.
pub(crate) fn replace_file(source: &Path, target: &Path) -> Result<()> {
    let deadline = std::time::Instant::now() + REPLACE_CONTENTION;
    loop {
        let error = match replace_file_once(source, target) {
            Ok(()) => return Ok(()),
            Err(error) => error,
        };
        let contended = matches!(
            error.raw_os_error().map(|code| code as u32),
            Some(ERROR_ACCESS_DENIED | ERROR_SHARING_VIOLATION)
        );
        if !contended || std::time::Instant::now() >= deadline {
            return Err(io_error("replace daemon metadata", error));
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

pub(crate) fn replace_file_once(source: &Path, target: &Path) -> std::io::Result<()> {
    let source_wide = verbatim_file_path(source);
    let target_wide = verbatim_file_path(target);
    // SAFETY: both paths are NUL-terminated UTF-16 strings for this call.
    if unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            target_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub(crate) fn host_boot_id() -> Result<String> {
    static BOOT_MARKER: OnceLock<String> = OnceLock::new();
    Ok(BOOT_MARKER
        .get_or_init(|| {
            // Windows does not expose a public boot UUID. This is the boot FILETIME
            // estimated from wall-clock FILETIME minus monotonic uptime.
            let mut now = std::mem::MaybeUninit::uninit();
            // SAFETY: GetSystemTimeAsFileTime initializes one FILETIME value.
            unsafe { GetSystemTimeAsFileTime(now.as_mut_ptr()) };
            // SAFETY: the call above initialized now.
            let now = unsafe { now.assume_init() };
            let now = (u64::from(now.dwHighDateTime) << 32) | u64::from(now.dwLowDateTime);
            let uptime = unsafe { GetTickCount64() }.saturating_mul(10_000);
            format!(
                "windows-boot-filetime-estimate:{}",
                now.saturating_sub(uptime)
            )
        })
        .clone())
}

pub(crate) fn current_process_birth() -> Result<String> {
    // Source: v1 donor dc6f70aa, process_group.rs process_creation_filetime /
    // process_birth_identity; reduced to the current process's birth timestamp.
    let mut creation = std::mem::MaybeUninit::uninit();
    let mut exit = std::mem::MaybeUninit::uninit();
    let mut kernel = std::mem::MaybeUninit::uninit();
    let mut user = std::mem::MaybeUninit::uninit();
    // SAFETY: GetCurrentProcess is a valid pseudo-handle and each output points
    // to one writable FILETIME. The pseudo-handle needs no close.
    if unsafe {
        GetProcessTimes(
            GetCurrentProcess(),
            creation.as_mut_ptr(),
            exit.as_mut_ptr(),
            kernel.as_mut_ptr(),
            user.as_mut_ptr(),
        )
    } == 0
    {
        return Err(io_error(
            "read current process birth identity",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: GetProcessTimes initialized creation on success.
    let creation = unsafe { creation.assume_init() };
    let birth = (u64::from(creation.dwHighDateTime) << 32) | u64::from(creation.dwLowDateTime);
    Ok(format!("windows-creation-filetime:{birth}"))
}

pub(crate) fn create_pipe_server(
    endpoint: &str,
    first_instance: bool,
) -> Result<tokio::net::windows::named_pipe::NamedPipeServer> {
    if !endpoint.starts_with(r"\\.\pipe\")
        || endpoint.chars().any(char::is_control)
        || endpoint.contains('\0')
    {
        return Err(Error::new(
            "invalid_endpoint",
            "invalid named-pipe endpoint",
        ));
    }
    let descriptor = current_user_pipe_descriptor()?;
    let attributes = SECURITY_ATTRIBUTES {
        nLength: std::mem::size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: descriptor,
        bInheritHandle: 0,
    };
    let endpoint_wide: Vec<u16> = endpoint.encode_utf16().chain(Some(0)).collect();
    let first_instance_flag = if first_instance {
        FILE_FLAG_FIRST_PIPE_INSTANCE
    } else {
        0
    };
    // SAFETY: the pipe name and SECURITY_ATTRIBUTES remain valid through the
    // call; its explicit DACL grants access only to this process's user SID.
    let handle = unsafe {
        CreateNamedPipeW(
            endpoint_wide.as_ptr(),
            windows_sys::Win32::Storage::FileSystem::PIPE_ACCESS_DUPLEX
                | FILE_FLAG_OVERLAPPED
                | first_instance_flag,
            PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
            PIPE_UNLIMITED_INSTANCES,
            64 * 1024,
            64 * 1024,
            0,
            &attributes,
        )
    };
    // SAFETY: the descriptor was allocated by the Windows security API above.
    unsafe { windows_sys::Win32::Foundation::LocalFree(descriptor as HLOCAL) };

    if handle == INVALID_HANDLE_VALUE {
        return Err(io_error(
            "create named-pipe server",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: CreateNamedPipeW returned a fresh overlapped pipe handle. Tokio
    // takes ownership and registers the handle with its I/O driver.
    unsafe {
        tokio::net::windows::named_pipe::NamedPipeServer::from_raw_handle(handle)
            .map_err(|error| io_error("register named-pipe server", error))
    }
}

pub(crate) struct Job {
    handle: HANDLE,
    query: Arc<Mutex<Option<QueryHandle>>>,
    family_empty_confirmed: Arc<AtomicBool>,
}

pub(crate) struct LocalPeer {
    pid: u32,
    process: Option<QueryHandle>,
}

impl LocalPeer {
    pub(crate) fn process_id(&self) -> u32 {
        self.pid
    }

    pub(crate) fn image_path(&self) -> Option<String> {
        self.process
            .as_ref()
            .and_then(|process| process_image_path(process.0))
    }

    pub(crate) fn capture(pipe: &tokio::net::windows::named_pipe::NamedPipeServer) -> Option<Self> {
        let mut pid = 0;
        // SAFETY: the accepted pipe instance is live and pid is a writable DWORD.
        if unsafe { GetNamedPipeClientProcessId(pipe.as_raw_handle(), &mut pid) } == 0 || pid == 0 {
            return None;
        }
        // SAFETY: OpenProcess takes a PID and returns a new noninheritable handle.
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if handle.is_null() {
            // SAFETY: this is the thread's error from the failed OpenProcess.
            return (unsafe { GetLastError() } == ERROR_ACCESS_DENIED)
                .then_some(Self { pid, process: None });
        }
        Some(Self {
            pid,
            process: Some(QueryHandle(handle)),
        })
    }

    pub(crate) fn inspect(self, _owned_agents: bool) -> Result<Self> {
        self.verify()?;
        Ok(self)
    }

    pub(crate) fn verify(&self) -> Result<()> {
        let Some(process) = &self.process else {
            // With a denied process handle, every Job must resolve the
            // kernel-reported client PID through its own member list.
            return Ok(());
        };
        // SAFETY: the process handle has synchronize access and outlives the wait.
        if unsafe { WaitForSingleObject(process.0, 0) } != WAIT_TIMEOUT {
            return Err(Error::new(
                "process_identity",
                "named-pipe client is no longer live",
            ));
        }
        Ok(())
    }
}

pub(crate) struct JobMembership(Arc<Mutex<Option<QueryHandle>>>);

impl JobMembership {
    pub(crate) fn contains(&self, peer: &LocalPeer) -> Result<bool> {
        let query = self.0.lock_recover();
        let job = query.as_ref().ok_or_else(|| {
            Error::new(
                "process_custody_unknown",
                "owned Job query handle is closed",
            )
        })?;
        let Some(process) = &peer.process else {
            let (pids, complete) = job_members(job.0)?;
            if pids.contains(&peer.pid) {
                return Ok(true);
            }
            if !complete {
                return Err(Error::new(
                    "process_custody_unknown",
                    "owned Job member list is incomplete",
                ));
            }
            return Ok(false);
        };
        let mut contained = 0;
        // SAFETY: both handles remain live during this read-only membership query.
        if unsafe { IsProcessInJob(process.0, job.0, &mut contained) } == 0 {
            return Err(io_error(
                "verify owned Job membership",
                std::io::Error::last_os_error(),
            ));
        }
        Ok(contained != 0)
    }
}

// The owned kernel handle may be transferred to another thread; no borrowed
// pointer or thread-affine state is stored in this wrapper.
unsafe impl Send for Job {}

impl Job {
    fn query_handle(&self) -> Result<std::sync::MutexGuard<'_, Option<QueryHandle>>> {
        let mut query = self.query.lock_recover();
        if query.is_none() {
            let mut handle = null_mut();
            // JOB_OBJECT_QUERY (0x0004) is documented by Job Object Security and
            // Access Rights but is not exported by windows-sys 0.61.2.
            // SAFETY: duplicate our private Job with query-only, non-inheritable access.
            if unsafe {
                DuplicateHandle(
                    GetCurrentProcess(),
                    self.handle,
                    GetCurrentProcess(),
                    &mut handle,
                    0x0004,
                    0,
                    0,
                )
            } == 0
            {
                return Err(io_error(
                    "duplicate owned Job query handle",
                    std::io::Error::last_os_error(),
                ));
            }
            *query = Some(QueryHandle(handle));
        }
        Ok(query)
    }

    pub(crate) fn membership(&self) -> Result<JobMembership> {
        drop(self.query_handle()?);
        Ok(JobMembership(Arc::clone(&self.query)))
    }

    pub(crate) fn observer(&self, pid: u32) -> Result<ProcessObserver> {
        let query = self.query_handle()?;
        let handle = query
            .as_ref()
            .ok_or_else(|| {
                Error::new(
                    "process_custody_unknown",
                    "owned Job query handle is closed",
                )
            })?
            .0;
        let boot_id = host_boot_id()?;
        let (sample, contained) = read_job_process(Some(handle), pid, &boot_id, "", "plugin", 0)?;
        if !contained || sample.record.state != ProcessState::Running {
            return Err(Error::new(
                "process_custody_unknown",
                "plugin is no longer live in its owned Job",
            ));
        }
        Ok(ProcessObserver {
            query: Arc::clone(&self.query),
            family_empty_confirmed: Arc::clone(&self.family_empty_confirmed),
            root: sample.record.identity,
            root_exit: None,
        })
    }

    pub(crate) fn is_empty(&self) -> Result<bool> {
        self.family_empty_confirmed.store(false, Ordering::Release);
        let (pids, complete) = job_members(self.handle)?;
        let empty = complete && pids.is_empty();
        self.family_empty_confirmed.store(empty, Ordering::Release);
        Ok(empty)
    }

    /// Source: v1 donor dc6f70aa, process_group.rs. This uses only its Job
    /// Object lifetime/assignment mechanism; process schemas and wrappers stay local.
    pub(crate) fn new_kill_on_close() -> Result<Self> {
        Self::new_with_leaf_limit(false, None)
    }

    /// A kill-on-close Job whose name lets a later daemon prove it closed.
    /// An existing object with that name is never adopted as custody.
    pub(crate) fn new_named_kill_on_close(name: &str) -> Result<Self> {
        Self::new_with_leaf_limit(false, Some(name))
    }

    pub(crate) fn new_leaf() -> Result<Self> {
        Self::new_with_leaf_limit(true, None)
    }

    fn new_with_leaf_limit(leaf: bool, name: Option<&str>) -> Result<Self> {
        let name = name.map(|name| {
            OsStr::new(name)
                .encode_wide()
                .chain(Some(0))
                .collect::<Vec<u16>>()
        });
        // SAFETY: null attributes request default security; the name is either
        // null (private Job) or a NUL-terminated buffer that outlives the call.
        let handle =
            unsafe { CreateJobObjectW(null(), name.as_ref().map_or(null(), |name| name.as_ptr())) };
        if handle.is_null() {
            return Err(io_error(
                "create process Job Object",
                std::io::Error::last_os_error(),
            ));
        }
        // SAFETY: GetLastError reads this thread's error from the call above.
        if name.is_some() && unsafe { GetLastError() } == ERROR_ALREADY_EXISTS {
            // SAFETY: handle was opened above and is not shared.
            unsafe { CloseHandle(handle) };
            return Err(Error::new(
                "process_custody_conflict",
                "a Job Object with this custody name already exists",
            ));
        }

        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if leaf {
            // Microsoft JOBOBJECT_BASIC_LIMIT_INFORMATION: a second process
            // exceeds this Job's limit and cannot be admitted. No breakaway flags.
            limits.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_ACTIVE_PROCESS;
            limits.BasicLimitInformation.ActiveProcessLimit = 1;
        }
        // SAFETY: `limits` has the exact structure and size required by this
        // information class and remains valid for the duration of the call.
        if unsafe {
            SetInformationJobObject(
                handle,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        } == 0
        {
            let error = std::io::Error::last_os_error();
            // SAFETY: handle was created above and is not shared.
            unsafe { CloseHandle(handle) };
            return Err(io_error("configure process Job Object", error));
        }
        Ok(Self {
            handle,
            query: Arc::new(Mutex::new(None)),
            family_empty_confirmed: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn assign_then_resume(
        &self,
        process: HANDLE,
        before_resume: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        self.family_empty_confirmed.store(false, Ordering::Release);
        // The process was created with CREATE_SUSPENDED, so assignment occurs
        // before any child instruction can execute.
        // SAFETY: both handles are live and owned by this launch operation.
        if unsafe { AssignProcessToJobObject(self.handle, process) } == 0 {
            let error = std::io::Error::last_os_error();
            // The suspended process is not in our Job if assignment failed.
            // SAFETY: process is the live handle returned by Tokio's spawn.
            unsafe { TerminateProcess(process, 1) };
            return Err(io_error("assign suspended process to Job Object", error));
        }

        if let Err(error) = before_resume() {
            let _ = self.terminate();
            return Err(error);
        }
        // Tokio exposes the process handle and async stdio, but not the primary
        // thread handle required by ResumeThread. This narrow NT call resumes
        // only after the exact Job membership has been established.
        let status = unsafe { NtResumeProcess(process) };
        if status < 0 {
            // SAFETY: the Job owns the suspended process and kills its members.
            unsafe { TerminateJobObject(self.handle, 1) };
            return Err(Error::new(
                "process_resume_failed",
                format!("NtResumeProcess returned status {status:#x}"),
            ));
        }
        Ok(())
    }

    pub(crate) fn terminate(&self) -> Result<()> {
        // SAFETY: this wrapper owns a live Job handle.
        if unsafe { TerminateJobObject(self.handle, 1) } == 0 {
            Err(io_error(
                "terminate process Job Object",
                std::io::Error::last_os_error(),
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        // A retained observer never retains a Job handle beyond this owner.
        // Explicitly terminate, then close the shared query handle before the
        // original handle: kill-on-close remains effective even if termination fails.
        let _ = self.terminate();
        self.query.lock_recover().take();
        // SAFETY: this object uniquely owns the handle.
        unsafe { CloseHandle(self.handle) };
    }
}

/// Terminate the exact named generation Job, independently of its runtime actor.
/// A removed routing entry does not imply a closed Job: the actor task can
/// still own it while draining or stuck, keeping tool descendants alive
/// after a stop that reports `not_found`. Opening by name never adopts
/// custody: a missing name means the generation already closed and there is
/// nothing to terminate. Returns false only when the name is absent; a live
/// Job must be observed empty within the bounded wait or the outcome is unknown.
pub(crate) fn terminate_named_generation_job(name: &str) -> Result<bool> {
    let wide: Vec<u16> = OsStr::new(name).encode_wide().chain(Some(0)).collect();
    // JOB_OBJECT_TERMINATE (0x0008) plus the query-only rights from
    // Job::observer; neither is exported by windows-sys 0.61.2.
    const TERMINATE_AND_QUERY: u32 = 0x0008 | 0x0004;
    // SAFETY: `wide` is a NUL-terminated name that outlives the call.
    let handle = unsafe { OpenJobObjectW(TERMINATE_AND_QUERY, 0, wide.as_ptr()) };
    if handle.is_null() {
        let error = std::io::Error::last_os_error();
        return if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) {
            Ok(false)
        } else {
            Err(io_error("open recorded custody Job Object", error))
        };
    }
    // SAFETY: handle was opened above and is not shared.
    let terminated = unsafe { TerminateJobObject(handle, 1) } != 0;
    let terminated_error = std::io::Error::last_os_error();
    // Members die asynchronously after termination; confirm emptiness on a
    // bounded wait before reporting, the same way the owned stop path does.
    let mut empty = false;
    if terminated {
        for _ in 0..20 {
            match job_members(handle) {
                Ok((pids, complete)) if complete && pids.is_empty() => {
                    empty = true;
                    break;
                }
                Ok(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
                Err(_) => break,
            }
        }
    }
    // SAFETY: handle was opened above and is not shared.
    unsafe { CloseHandle(handle) };
    if !terminated {
        return Err(io_error(
            "terminate recorded custody Job Object",
            terminated_error,
        ));
    }
    if !empty {
        return Err(Error::new(
            "unknown",
            "terminated generation Job did not confirm empty before its deadline",
        ));
    }
    Ok(true)
}

/// Whether a named kill-on-close custody Job has been closed. The name leaves
/// the namespace when the last handle closes; for this Job that close has
/// already told the kernel to terminate every member (no breakaway), but a
/// member can still be exiting, so callers also check recorded identities.
/// An open Job counts only when it has no member.
pub(crate) fn named_job_closed(name: &str) -> Result<bool> {
    let wide: Vec<u16> = OsStr::new(name).encode_wide().chain(Some(0)).collect();
    // JOB_OBJECT_QUERY (0x0004); see Job::observer.
    // SAFETY: `wide` is a NUL-terminated name that outlives the call.
    let handle = unsafe { OpenJobObjectW(0x0004, 0, wide.as_ptr()) };
    if handle.is_null() {
        let error = std::io::Error::last_os_error();
        return if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) {
            Ok(true)
        } else {
            Err(io_error("open recorded custody Job Object", error))
        };
    }
    let members = job_members(handle);
    // SAFETY: handle was opened above and is not shared.
    unsafe { CloseHandle(handle) };
    let (pids, complete) = members?;
    Ok(complete && pids.is_empty())
}

/// Whether a numeric process id names a process that has not exited yet.
/// Query-only rights are used and the handle is always closed. A reused PID
/// reads as alive, which only keeps a directory: never a deletion proof.
pub(crate) fn lock_owner_process_alive(pid: u32) -> bool {
    // SAFETY: OpenProcess with query-only rights performs no write to the
    // target; the handle is closed on every path below.
    unsafe {
        let process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid);
        if process.is_null() {
            return false;
        }
        let mut code: u32 = 0;
        let ok = GetExitCodeProcess(process, &mut code) != 0;
        CloseHandle(process);
        ok && code == STILL_ACTIVE as u32
    }
}

/// Whether a process recorded by an earlier daemon (exact creation FILETIME)
/// is still running. A missing, exited or reused PID is not that process.
pub(crate) fn recorded_process_alive(pid: u32, start_time: u64) -> Result<bool> {
    match read_job_process(None, pid, "", "", "", 0) {
        Ok((sample, _)) => Ok(sample.record.identity.start_time == start_time
            && sample.record.state == ProcessState::Running),
        Err(error) if error.code == "process_gone" => Ok(false),
        Err(error) => Err(error),
    }
}

pub(crate) fn process_start_time(pid: u32) -> Result<u64> {
    let (sample, _) = read_job_process(None, pid, "", "", "", 0)?;
    if sample.record.state == ProcessState::Exited {
        return Err(Error::new(
            "process_gone",
            "the launched process already exited",
        ));
    }
    Ok(sample.record.identity.start_time)
}

pub(crate) fn recorded_process_argv(pid: u32, start_time: u64) -> Result<Vec<String>> {
    let process = verified_process(pid, start_time, 0)?;
    let command_line = process_command_line(process.0)
        .ok_or_else(|| Error::new("process_identity", "the daemon command line is unavailable"))?;
    Ok(eliot_core::split_windows_command_line(&command_line))
}

pub(crate) fn recorded_process_image(pid: u32, start_time: u64) -> Result<PathBuf> {
    let process = verified_process(pid, start_time, 0)?;
    process_image_path(process.0)
        .map(PathBuf::from)
        .ok_or_else(|| Error::new("process_identity", "the daemon executable is unavailable"))
}

pub(crate) fn terminate_recorded_process(pid: u32, start_time: u64) -> Result<()> {
    if !recorded_process_alive(pid, start_time)? {
        return Ok(());
    }
    let process = match verified_process(pid, start_time, PROCESS_TERMINATE) {
        Ok(process) => process,
        Err(_) if !recorded_process_alive(pid, start_time)? => return Ok(()),
        Err(error) => return Err(error),
    };
    // SAFETY: this handle belongs to the exact candidate process verified above.
    unsafe {
        TerminateProcess(process.0, 1);
        if WaitForSingleObject(process.0, 5000) == WAIT_OBJECT_0 {
            return Ok(());
        }
    }
    Err(Error::new(
        "process_identity",
        "the replacement process did not exit",
    ))
}

fn verified_process(pid: u32, start_time: u64, extra_access: u32) -> Result<QueryHandle> {
    // SAFETY: the query/synchronize handle pins the observed process identity.
    let process = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE | extra_access,
            0,
            pid,
        )
    };
    if process.is_null() {
        return Err(io_error(
            "open daemon process",
            std::io::Error::last_os_error(),
        ));
    }
    let process = QueryHandle(process);
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: the handle stays live and all timestamp outputs are writable.
    let valid = unsafe {
        GetProcessTimes(process.0, &mut creation, &mut exit, &mut kernel, &mut user) != 0
            && WaitForSingleObject(process.0, 0) == WAIT_TIMEOUT
    };
    if !valid || filetime_ticks(creation) != start_time {
        return Err(Error::new(
            "process_identity",
            "the daemon process identity changed",
        ));
    }
    Ok(process)
}
/// ProcessCommandLineInformation class for NtQueryInformationProcess.
const PROCESS_COMMAND_LINE_INFORMATION: u32 = 60;
/// ProcessBasicInformation class for NtQueryInformationProcess (parent pid).
const PROCESS_BASIC_INFORMATION: u32 = 0;
/// Ancestor depth bound for heavy attribution: the harness wrapper, driver
/// and compiler sit within a few levels; anything deeper is not one command.
const HEAVY_ANCESTOR_DEPTH: usize = 8;
/// Cap on one argv read: only the program plus its first tokens ever matter
/// downstream, and the full line never persists.
const MAX_HEAVY_ARGV_TOKENS: usize = 64;

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtQueryInformationProcess(
        ProcessHandle: HANDLE,
        ProcessInformationClass: u32,
        ProcessInformation: *mut c_void,
        ProcessInformationLength: u32,
        ReturnLength: *mut u32,
    ) -> i32;
}

/// `PROCESS_BASIC_INFORMATION` prefix for the parent pid read (Microsoft
/// Learn: NtQueryInformationProcess). Only the inherited pid is used.
#[repr(C)]
#[allow(
    non_snake_case,
    reason = "Windows ABI fields must retain the platform's required spelling."
)]
struct ProcessBasicInformation {
    ExitStatus: i32,
    PebBaseAddress: *mut c_void,
    AffinityMask: usize,
    BasePriority: i32,
    UniqueProcessId: usize,
    InheritedFromUniqueProcessId: usize,
}

/// Bounded program identity for one process: the image basename plus at most
/// one subcommand token, classified at read time so full argv never persists.
///
/// Microsoft Learn: QueryFullProcessImageNameW for the image (documented,
/// same query rights the observer already holds); NtQueryInformationProcess
/// with ProcessCommandLineInformation for the command line (documented entry
/// point). Access-denied, exited-race and unparseable results fail closed to
/// the image-only category or to None.
/// Parent pid for heavy attribution (Microsoft Learn:
/// NtQueryInformationProcess with ProcessBasicInformation). Exited races and
/// denied queries fail closed to None; pid 0 is never a parent.
fn process_parent_pid(process: HANDLE) -> Option<u32> {
    let mut info = ProcessBasicInformation {
        ExitStatus: 0,
        PebBaseAddress: null_mut(),
        AffinityMask: 0,
        BasePriority: 0,
        UniqueProcessId: 0,
        InheritedFromUniqueProcessId: 0,
    };
    let mut returned: u32 = 0;
    // SAFETY: info is caller-owned and sized; the kernel fills the struct.
    let status = unsafe {
        NtQueryInformationProcess(
            process,
            PROCESS_BASIC_INFORMATION,
            &mut info as *mut ProcessBasicInformation as *mut c_void,
            std::mem::size_of::<ProcessBasicInformation>() as u32,
            &mut returned,
        )
    };
    if status != 0 {
        return None;
    }
    let parent = u32::try_from(info.InheritedFromUniqueProcessId).ok()?;
    if parent == 0 {
        return None;
    }
    Some(parent)
}

/// Bounded argv plus parent pid for one process in a single query handle.
/// Empty, overlong or unreadable results fail closed to None; the full line
/// never persists.
fn query_argv_and_parent(process: HANDLE) -> Option<(Vec<String>, Option<u32>)> {
    let cmdline = process_command_line(process)?;
    let mut argv = eliot_core::split_windows_command_line(&cmdline);
    if argv.is_empty() || argv.len() > MAX_HEAVY_ARGV_TOKENS {
        return None;
    }
    if argv.iter().any(|token| token.len() > 32_768) {
        return None;
    }
    argv.truncate(MAX_HEAVY_ARGV_TOKENS);
    Some((argv, process_parent_pid(process)))
}

/// Heavy category for one owned child process: its own argv plus ancestors'
/// argv (nearest first) resolve through harness plumbing (console hosts,
/// the shell wrapper) to the program that actually consumed the resources,
/// via `eliot_core::resolve_heavy_category`. Anything unobservable fails
/// closed to the image-only category; console-host plumbing with no
/// meaningful relative yields None so no episode opens on it.
pub(crate) fn resolve_heavy_category(pid: u32) -> Option<CommandClass> {
    if pid == 0 {
        return None;
    }
    // SAFETY: read-only query rights; the wrapper closes the handle.
    let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
    if process.is_null() {
        return None;
    }
    let process = QueryHandle(process);
    let image = process_image_path(process.0).unwrap_or_default();
    let image_category = eliot_core::classify_heavy_command(&image, &[]);
    if image_category == "unknown" {
        return None;
    }
    let Some(cmdline) = process_command_line(process.0) else {
        return Some(CommandClass {
            category: image_category,
            source: eliot_core::CATEGORY_SOURCE_IMAGE,
        });
    };
    let mut own = eliot_core::split_windows_command_line(&cmdline);
    if own.is_empty() || own.len() > MAX_HEAVY_ARGV_TOKENS {
        return Some(CommandClass {
            category: image_category,
            source: eliot_core::CATEGORY_SOURCE_IMAGE,
        });
    }
    own.truncate(MAX_HEAVY_ARGV_TOKENS);
    let mut ancestors: Vec<Vec<String>> = Vec::new();
    let mut seen: Vec<u32> = vec![pid];
    let mut cursor = process_parent_pid(process.0);
    while let Some(ppid) = cursor {
        if ancestors.len() >= HEAVY_ANCESTOR_DEPTH || seen.contains(&ppid) || ppid == 0 {
            break;
        }
        seen.push(ppid);
        // SAFETY: read-only query rights; the wrapper closes the handle.
        let parent = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, ppid) };
        if parent.is_null() {
            break;
        }
        let parent = QueryHandle(parent);
        match query_argv_and_parent(parent.0) {
            Some((argv, grandparent)) => {
                ancestors.push(argv);
                cursor = grandparent;
            }
            None => break,
        }
    }
    match eliot_core::resolve_heavy_category(&own, &ancestors) {
        Some(category) => Some(CommandClass {
            category,
            source: eliot_core::CATEGORY_SOURCE_CMDLINE,
        }),
        None => {
            let stem = image
                .rsplit(['/', '\\'])
                .next()
                .unwrap_or(&image)
                .trim()
                .to_ascii_lowercase();
            if eliot_core::CONSOLE_HOST_PROGRAMS.contains(&stem.as_str()) {
                return None;
            }
            Some(CommandClass {
                category: image_category,
                source: eliot_core::CATEGORY_SOURCE_IMAGE,
            })
        }
    }
}

fn process_image_path(process: HANDLE) -> Option<String> {
    let mut size: u32 = 32_768;
    let mut name = vec![0u16; size as usize];
    // SAFETY: the buffer holds size UTF-16 units; the kernel writes at most
    // size units and reports the count back through size.
    if unsafe { QueryFullProcessImageNameW(process, 0, name.as_mut_ptr(), &mut size) } == 0 {
        return None;
    }
    String::from_utf16(&name[..size as usize]).ok()
}

fn process_command_line(process: HANDLE) -> Option<String> {
    const BUFFER_BYTES: usize = 65_536;
    let mut buffer = vec![0u8; BUFFER_BYTES];
    let mut returned: u32 = 0;
    // SAFETY: the buffer is caller-owned and sized; the kernel fills a
    // UNICODE_STRING header plus the string bytes inside it.
    let status = unsafe {
        NtQueryInformationProcess(
            process,
            PROCESS_COMMAND_LINE_INFORMATION,
            buffer.as_mut_ptr() as *mut c_void,
            BUFFER_BYTES as u32,
            &mut returned,
        )
    };
    if status != 0 {
        return None;
    }
    let ptr_size = std::mem::size_of::<usize>();
    let header = 4 + ptr_size;
    if (returned as usize) > buffer.len() || (returned as usize) < header + 2 {
        return None;
    }
    let length = u16::from_ne_bytes([buffer[0], buffer[1]]) as usize;
    if length == 0 || !length.is_multiple_of(2) || length > 32_760 {
        return None;
    }
    let mut addr = [0u8; 8];
    if ptr_size == 8 {
        addr.copy_from_slice(&buffer[8..16]);
    } else {
        addr[..4].copy_from_slice(&buffer[4..8]);
    }
    let str_ptr = usize::from_ne_bytes(addr) as *const u16;
    if str_ptr.is_null() {
        return None;
    }
    let base = buffer.as_ptr() as usize;
    let addr = str_ptr as usize;
    // The kernel returns the string inside the caller buffer; anything else
    // is not a command line.
    if addr < base || addr + length > base + buffer.len() {
        return None;
    }
    // SAFETY: bounds validated above; the kernel wrote UTF-16 units there.
    let units = unsafe { std::slice::from_raw_parts(str_ptr, length / 2) };
    String::from_utf16(units)
        .ok()
        .filter(|line| !line.is_empty())
}
struct QueryHandle(HANDLE);

// A query-only kernel handle has no thread-affine state. The observer serializes
// its operations and this wrapper only closes its own handle.
unsafe impl Send for QueryHandle {}

impl Drop for QueryHandle {
    fn drop(&mut self) {
        // SAFETY: this wrapper owns exactly this query/process handle.
        unsafe { CloseHandle(self.0) };
    }
}

pub(crate) struct ProcessObserver {
    query: Arc<Mutex<Option<QueryHandle>>>,
    family_empty_confirmed: Arc<AtomicBool>,
    root: ProcessIdentity,
    root_exit: Option<Option<i32>>,
}

impl ProcessObserver {
    pub(crate) fn record_root_exit(&mut self, code: Option<i32>) {
        self.root_exit = Some(code);
    }

    pub(crate) fn snapshot(&self, owner: &str, now: i64) -> Result<ProcessSnapshot> {
        let query = self.query.lock_recover();
        let job = query.as_ref().map(|handle| handle.0);
        let (pids, mut complete) = match job {
            Some(job) => job_members(job)?,
            // Drop alone proves nothing. The owner must have confirmed a
            // complete empty Job through wait_family_empty before releasing it.
            // Known identities are still refreshed below and by CustodyObserver.
            None => (
                Vec::new(),
                self.family_empty_confirmed.load(Ordering::Acquire),
            ),
        };
        let root = ProcessSample {
            record: ProcessRecord {
                identity: self.root.clone(),
                owner: owner.to_owned(),
                role: "plugin".to_owned(),
                state: ProcessState::Unknown,
                exit_code: None,
                updated_at: now,
            },
            cpu_time_ns: None,
            rss_bytes: None,
        };
        let mut samples = vec![self.refresh_with_job(job, &root, now)];
        for pid in pids {
            if pid == self.root.pid {
                continue;
            }
            if samples.len() >= MAX_PROCESS_SAMPLES {
                complete = false;
                break;
            }
            match read_job_process(job, pid, &self.root.host_boot_id, owner, "child", now) {
                Ok((sample, true)) => samples.push(sample),
                _ => complete = false,
            }
        }
        Ok(ProcessSnapshot { samples, complete })
    }

    pub(crate) fn register_pid(
        &self,
        pid: u32,
        role: &str,
        owner: &str,
        now: i64,
    ) -> Result<ProcessSample> {
        let query = self.query.lock_recover();
        let job = query
            .as_ref()
            .ok_or_else(|| Error::new("not_allowed", "owned Job has been released"))?
            .0;
        let (sample, contained) =
            read_job_process(Some(job), pid, &self.root.host_boot_id, owner, role, now)?;
        if !contained || sample.record.state != ProcessState::Running {
            return Err(Error::new(
                "not_allowed",
                "registered process is not running in the owned Job",
            ));
        }
        if (role == "plugin") != same_identity(&sample.record.identity, &self.root) {
            return Err(Error::new(
                "invalid",
                "only the owned root has the plugin role",
            ));
        }
        Ok(sample)
    }

    pub(crate) fn refresh(&self, old: &ProcessSample, now: i64) -> ProcessSample {
        let query = self.query.lock_recover();
        self.refresh_with_job(query.as_ref().map(|handle| handle.0), old, now)
    }

    fn refresh_with_job(
        &self,
        job: Option<HANDLE>,
        old: &ProcessSample,
        now: i64,
    ) -> ProcessSample {
        if same_identity(&old.record.identity, &self.root)
            && let Some(code) = self.root_exit
        {
            let mut sample = exited_sample(old, now);
            sample.record.exit_code = code;
            return sample;
        }
        match read_job_process(
            job,
            old.record.identity.pid,
            &self.root.host_boot_id,
            &old.record.owner,
            &old.record.role,
            now,
        ) {
            Ok((sample, _)) if !same_identity(&sample.record.identity, &old.record.identity) => {
                exited_sample(old, now)
            }
            Ok((sample, true)) => sample,
            Ok((sample, false)) if sample.record.state == ProcessState::Exited => sample,
            Err(error) if error.code == "process_gone" => exited_sample(old, now),
            _ => unknown_sample(old, now),
        }
    }
}

fn job_members(job: HANDLE) -> Result<(Vec<u32>, bool)> {
    let header = std::mem::offset_of!(JOBOBJECT_BASIC_PROCESS_ID_LIST, ProcessIdList);
    let bytes = header + MAX_PROCESS_SAMPLES * std::mem::size_of::<usize>();
    let mut storage = vec![0usize; bytes.div_ceil(std::mem::size_of::<usize>())];
    // SAFETY: storage is pointer-aligned and sized for the complete bounded list.
    let ok = unsafe {
        QueryInformationJobObject(
            job,
            JobObjectBasicProcessIdList,
            storage.as_mut_ptr().cast(),
            bytes as u32,
            null_mut(),
        )
    };
    if ok == 0 && std::io::Error::last_os_error().raw_os_error() != Some(ERROR_MORE_DATA as i32) {
        return Err(io_error(
            "enumerate owned Job processes",
            std::io::Error::last_os_error(),
        ));
    }
    // SAFETY: even a truncated list has its two DWORD counts in the initialized buffer.
    let info = unsafe { &*storage.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>() };
    let count = info.NumberOfProcessIdsInList as usize;
    if count > MAX_PROCESS_SAMPLES {
        return Err(Error::new(
            "process_observation",
            "Job process list exceeded its buffer",
        ));
    }
    // SAFETY: header is the API layout's pointer-array offset and count was bounded above.
    let ids = unsafe {
        slice::from_raw_parts(
            storage.as_ptr().cast::<u8>().add(header).cast::<usize>(),
            count,
        )
    };
    let pids = ids
        .iter()
        .map(|pid| u32::try_from(*pid))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|_| Error::new("process_identity", "Job returned an invalid process id"))?;
    Ok((
        pids,
        ok != 0 && count == info.NumberOfAssignedProcesses as usize,
    ))
}

fn filetime_ticks(time: FILETIME) -> u64 {
    (u64::from(time.dwHighDateTime) << 32) | u64::from(time.dwLowDateTime)
}

fn read_job_process(
    job: Option<HANDLE>,
    pid: u32,
    boot_id: &str,
    owner: &str,
    role: &str,
    now: i64,
) -> Result<(ProcessSample, bool)> {
    if pid == 0 {
        return Err(Error::new("invalid", "process id must be positive"));
    }
    // Source: dc6f70aa process_group.rs process_birth_identity. Query/synchronize
    // only; the opened process handle pins GetProcessTimes across the observation.
    // SAFETY: no pointer arguments; request only read/synchronization rights.
    let process = unsafe {
        OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
            0,
            pid,
        )
    };
    if process.is_null() {
        let error = std::io::Error::last_os_error();
        return Err(
            if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                Error::new("process_gone", "recorded process no longer exists")
            } else {
                io_error("open process for observation", error)
            },
        );
    }
    let process = QueryHandle(process);
    let mut creation = FILETIME::default();
    let mut exit = FILETIME::default();
    let mut kernel = FILETIME::default();
    let mut user = FILETIME::default();
    // SAFETY: the owned handle stays live and all four FILETIME outputs are writable.
    if unsafe { GetProcessTimes(process.0, &mut creation, &mut exit, &mut kernel, &mut user) } == 0
    {
        return Err(io_error(
            "read process birth and CPU time",
            std::io::Error::last_os_error(),
        ));
    }
    let mut contained = 0;
    if let Some(job) = job {
        // SAFETY: compare against this exact private Job, never NULL/any-job membership.
        if unsafe { IsProcessInJob(process.0, job, &mut contained) } == 0 {
            return Err(io_error(
                "verify owned Job membership",
                std::io::Error::last_os_error(),
            ));
        }
    }
    // SAFETY: a zero-time wait only observes this query/synchronize process handle.
    let state = match unsafe { WaitForSingleObject(process.0, 0) } {
        WAIT_OBJECT_0 => ProcessState::Exited,
        WAIT_TIMEOUT => ProcessState::Running,
        _ => {
            return Err(io_error(
                "observe process liveness",
                std::io::Error::last_os_error(),
            ));
        }
    };
    let mut exit_code = 0;
    // SAFETY: read an exit code only after the process object was signalled.
    let exit_code = if state == ProcessState::Exited
        && unsafe { GetExitCodeProcess(process.0, &mut exit_code) } != 0
    {
        Some(exit_code as i32)
    } else {
        None
    };
    let mut memory = PROCESS_MEMORY_COUNTERS {
        cb: std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        ..Default::default()
    };
    // SAFETY: memory has the exact API size. A denied/unavailable RSS remains None.
    let rss_bytes = if state == ProcessState::Running
        && unsafe {
            GetProcessMemoryInfo(
                process.0,
                &mut memory,
                std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
            )
        } != 0
    {
        u64::try_from(memory.WorkingSetSize).ok()
    } else {
        None
    };
    let cpu_time_ns = filetime_ticks(kernel)
        .checked_add(filetime_ticks(user))
        .and_then(|ticks| ticks.checked_mul(100));
    Ok((
        ProcessSample {
            record: ProcessRecord {
                identity: ProcessIdentity {
                    host_boot_id: boot_id.to_owned(),
                    pid,
                    start_time: filetime_ticks(creation),
                },
                owner: owner.to_owned(),
                role: role.to_owned(),
                state,
                exit_code,
                updated_at: now,
            },
            cpu_time_ns,
            rss_bytes,
        },
        contained != 0,
    ))
}

fn check_no_reparse_components(path: &Path) -> Result<()> {
    for component in path
        .ancestors()
        .filter(|component| !component.as_os_str().is_empty())
    {
        match file_attributes(component) {
            Ok(attributes) if attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 => {
                return Err(Error::new(
                    "unsafe_data_dir",
                    "data directory path may not pass through a reparse point",
                ));
            }
            Ok(_) => {}
            Err(error) if matches!(error.raw_os_error(), Some(2 | 3)) => {
                continue;
            }
            Err(error) => return Err(io_error("inspect data directory path", error)),
        }
    }
    Ok(())
}

fn reject_reparse(path: &Path) -> Result<()> {
    match file_attributes(path).map_err(|error| io_error("inspect private data file", error))? {
        attributes if attributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 => Err(Error::new(
            "unsafe_private_file",
            "private data files may not be reparse points",
        )),
        _ => Ok(()),
    }
}

fn file_attributes(path: &Path) -> std::io::Result<u32> {
    let path_wide = verbatim_file_path(path);
    // SAFETY: path_wide is NUL-terminated UTF-16 for this call.
    let attributes =
        unsafe { windows_sys::Win32::Storage::FileSystem::GetFileAttributesW(path_wide.as_ptr()) };
    if attributes == u32::MAX {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(attributes)
    }
}

fn ensure_current_owner(path: &Path, directory: bool) -> Result<()> {
    let handle = open_security_handle(path, READ_CONTROL, directory)?;
    let mut owner = null_mut();
    let mut descriptor = null_mut();
    // SAFETY: outputs are writable pointers and the requested owner SID is
    // returned inside the allocated security descriptor.
    let status = unsafe {
        GetSecurityInfo(
            handle.0,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            null_mut(),
            null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io_error(
            "read data owner ACL",
            std::io::Error::from_raw_os_error(status as i32),
        ));
    }

    let result = match current_user_sid() {
        // SAFETY: both SID pointers remain valid until descriptor/buffer drop.
        Ok(user_sid)
            if !owner.is_null() && unsafe { EqualSid(owner, sid_pointer(&user_sid)) } != 0 =>
        {
            Ok(())
        }
        Ok(user_sid) => Err(foreign_owner(path, owner, &user_sid)),
        Err(error) => Err(error),
    };
    // SAFETY: GetNamedSecurityInfoW allocated this descriptor for the caller.
    unsafe { windows_sys::Win32::Foundation::LocalFree(descriptor as HLOCAL) };
    result
}

/// Name the object and both SIDs: Windows CI runners intermittently report
/// this mismatch, and the bare message did not say which file or owner.
/// `owner` must point into a live security descriptor.
fn foreign_owner(path: &Path, owner: *mut c_void, user_sid: &[usize]) -> Error {
    let owner = if owner.is_null() {
        "none".to_owned()
    } else {
        sid_text(owner).unwrap_or_else(|_| "unreadable".to_owned())
    };
    let user = sid_to_string(user_sid).unwrap_or_else(|_| "unreadable".to_owned());
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    Error::new(
        "unsafe_data_dir",
        format!(
            "data root or private file is owned by another Windows user \
             ({name}: owner {owner}, current user {user})"
        ),
    )
}

fn validate_current_user_acl(path: &Path, directory: bool) -> Result<()> {
    let handle = open_security_handle(path, READ_CONTROL, directory)?;
    let mut owner = null_mut();
    let mut dacl = null_mut();
    let mut descriptor = null_mut();
    // SAFETY: output pointers are writable; the owner and DACL pointers are
    // backed by the returned security descriptor until it is freed below.
    let status = unsafe {
        GetSecurityInfo(
            handle.0,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        return Err(io_error(
            "read private data ACL",
            std::io::Error::from_raw_os_error(status as i32),
        ));
    }

    let result = (|| {
        let user_sid = current_user_sid()?;
        if owner.is_null()
            // SAFETY: owner and current-user SID remain valid during this closure.
            || unsafe { EqualSid(owner, sid_pointer(&user_sid)) } == 0
        {
            return Err(foreign_owner(path, owner, &user_sid));
        }
        if dacl.is_null() {
            return Err(Error::new(
                "unsafe_data_dir",
                "private data path has no access-control list",
            ));
        }

        let mut control = 0u16;
        let mut revision = 0u32;
        // SAFETY: descriptor is valid and both outputs are writable.
        if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
            || control & SE_DACL_PROTECTED == 0
        {
            return Err(Error::new(
                "unsafe_data_dir",
                "private data ACL is not protected from inherited access",
            ));
        }

        let mut acl_info = ACL_SIZE_INFORMATION::default();
        // SAFETY: dacl is valid and acl_info is writable.
        if unsafe {
            GetAclInformation(
                dacl,
                (&mut acl_info as *mut ACL_SIZE_INFORMATION).cast(),
                std::mem::size_of::<ACL_SIZE_INFORMATION>() as u32,
                AclSizeInformation,
            )
        } == 0
            || acl_info.AceCount != 1
        {
            return Err(Error::new(
                "unsafe_data_dir",
                "private data ACL must contain only the current-user entry",
            ));
        }

        let mut ace_pointer: *mut c_void = null_mut();
        // SAFETY: dacl contains exactly one ACE and ace_pointer is writable.
        if unsafe { GetAce(dacl, 0, &mut ace_pointer) } == 0 || ace_pointer.is_null() {
            return Err(io_error(
                "read private data ACL entry",
                std::io::Error::last_os_error(),
            ));
        }
        // SAFETY: GetAce returned the first ACE in this validated ACL.
        let header = unsafe { *(ace_pointer.cast::<ACE_HEADER>()) };
        let expected_flags = if directory {
            (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8
        } else {
            0
        };
        if header.AceType != 0 || header.AceFlags != expected_flags {
            return Err(Error::new(
                "unsafe_data_dir",
                "private data ACL entry has an unexpected type or inheritance",
            ));
        }

        // SAFETY: the ACE type was checked and GetAce returned a full ACE.
        let ace = unsafe { &*ace_pointer.cast::<ACCESS_ALLOWED_ACE>() };
        // SAFETY: SidStart begins the SID bytes in ACCESS_ALLOWED_ACE.
        let ace_sid = std::ptr::addr_of!(ace.SidStart).cast_mut().cast::<c_void>();
        // SAFETY: both SID pointers are valid while their parent buffers live.
        if ace.Mask != FILE_ALL_ACCESS || unsafe { EqualSid(ace_sid, sid_pointer(&user_sid)) } == 0
        {
            return Err(Error::new(
                "unsafe_data_dir",
                "private data ACL does not grant full access exclusively to the current user",
            ));
        }
        Ok(())
    })();

    // SAFETY: GetNamedSecurityInfoW allocated this descriptor for the caller.
    unsafe { windows_sys::Win32::Foundation::LocalFree(descriptor as HLOCAL) };
    result
}

fn set_current_user_acl(path: &Path, directory: bool) -> Result<()> {
    let user_sid = current_user_sid()?;
    let sid = sid_to_string(&user_sid)?;
    let sddl = if directory {
        format!("D:P(A;OICI;FA;;;{sid})")
    } else {
        format!("D:P(A;;FA;;;{sid})")
    };
    let sddl_wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
    let mut descriptor: *mut c_void = null_mut();
    // SAFETY: sddl_wide is NUL-terminated and descriptor is writable.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl_wide.as_ptr(),
            1,
            &mut descriptor,
            null_mut(),
        )
    } == 0
    {
        return Err(io_error(
            "build current-user ACL",
            std::io::Error::last_os_error(),
        ));
    }

    let mut present = 0;
    let mut defaulted = 0;
    let mut dacl = null_mut();
    // SAFETY: descriptor is valid and output pointers are writable.
    let dacl_ok =
        unsafe { GetSecurityDescriptorDacl(descriptor, &mut present, &mut dacl, &mut defaulted) }
            != 0
            && present != 0
            && !dacl.is_null();
    if !dacl_ok {
        // SAFETY: descriptor was allocated by the conversion API.
        unsafe { windows_sys::Win32::Foundation::LocalFree(descriptor as HLOCAL) };
        return Err(Error::new(
            "platform_acl",
            "Windows did not produce a private data DACL",
        ));
    }

    let handle = open_security_handle(path, READ_CONTROL | WRITE_DAC, directory)?;
    // SAFETY: handle, user SID buffer and DACL remain live for this call. The
    // protected DACL replaces inherited access with a single user ACE.
    let status = unsafe {
        SetSecurityInfo(
            handle.0,
            SE_FILE_OBJECT,
            DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            null_mut(),
            null_mut(),
            dacl,
            null(),
        )
    };
    // SAFETY: descriptor was allocated by the conversion API.
    unsafe { windows_sys::Win32::Foundation::LocalFree(descriptor as HLOCAL) };
    if status != 0 {
        Err(io_error(
            "apply current-user ACL",
            std::io::Error::from_raw_os_error(status as i32),
        ))
    } else {
        Ok(())
    }
}

fn current_user_sid() -> Result<Vec<usize>> {
    let mut token: HANDLE = null_mut();
    // SAFETY: token points to writable storage and the current-process pseudo-
    // handle is valid for OpenProcessToken.
    if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
        return Err(io_error(
            "open current user token",
            std::io::Error::last_os_error(),
        ));
    }

    let mut bytes = 0;
    // The first call obtains the required output size; a false return is expected.
    // SAFETY: null buffer with length zero is the documented size-query form.
    unsafe {
        GetTokenInformation(token, TokenUser, null_mut(), 0, &mut bytes);
    }
    if bytes == 0 {
        let error = std::io::Error::last_os_error();
        // SAFETY: token was opened above and is uniquely owned here.
        unsafe { CloseHandle(token) };
        return Err(io_error("size current user token information", error));
    }

    let words = (bytes as usize).div_ceil(std::mem::size_of::<usize>());
    let mut buffer = vec![0usize; words];
    // SAFETY: buffer is aligned storage large enough for `bytes` and remains
    // valid through GetTokenInformation.
    let ok = unsafe {
        GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            bytes,
            &mut bytes,
        )
    };
    // SAFETY: token was opened above and is uniquely owned here.
    unsafe { CloseHandle(token) };
    if ok == 0 {
        return Err(io_error(
            "read current user token information",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(buffer)
}

fn sid_pointer(buffer: &[usize]) -> *mut c_void {
    // SAFETY: current_user_sid fills the first TOKEN_USER structure in buffer.
    unsafe { (*(buffer.as_ptr().cast::<TOKEN_USER>())).User.Sid }
}

fn sid_to_string(buffer: &[usize]) -> Result<String> {
    sid_text(sid_pointer(buffer))
}

/// `sid` must point to a valid SID that stays live through the call.
fn sid_text(sid: *mut c_void) -> Result<String> {
    let mut text: *mut u16 = null_mut();
    // SAFETY: the caller guarantees `sid` references a live SID.
    if unsafe { ConvertSidToStringSidW(sid, &mut text) } == 0 {
        return Err(io_error("format SID", std::io::Error::last_os_error()));
    }
    let mut length = 0usize;
    // SAFETY: ConvertSidToStringSidW returns a NUL-terminated allocation.
    while unsafe { *text.add(length) } != 0 {
        length += 1;
        if length > 256 {
            // SAFETY: text was allocated by ConvertSidToStringSidW.
            unsafe { windows_sys::Win32::Foundation::LocalFree(text as HLOCAL) };
            return Err(Error::new("platform_acl", "SID is too long"));
        }
    }
    // SAFETY: text points to `length` initialized UTF-16 code units.
    let result = String::from_utf16(unsafe { slice::from_raw_parts(text, length) })
        .map_err(|error| Error::new("platform_acl", error.to_string()));
    // SAFETY: text was allocated by ConvertSidToStringSidW.
    unsafe { windows_sys::Win32::Foundation::LocalFree(text as HLOCAL) };
    result
}

fn current_user_pipe_descriptor() -> Result<*mut c_void> {
    let user_sid = current_user_sid()?;
    let sid = sid_to_string(&user_sid)?;
    let sddl = format!("D:P(A;;GA;;;{sid})");
    let sddl_wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
    let mut descriptor = null_mut();
    // SAFETY: sddl_wide is NUL-terminated and descriptor is writable.
    if unsafe {
        ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl_wide.as_ptr(),
            1,
            &mut descriptor,
            null_mut(),
        )
    } == 0
    {
        Err(io_error(
            "build named-pipe ACL",
            std::io::Error::last_os_error(),
        ))
    } else {
        Ok(descriptor)
    }
}

pub(crate) fn available_disk_space(path: &Path) -> Result<u64> {
    let path = wide_path(path);
    let mut available = 0;
    // SAFETY: the path is NUL-terminated and the one requested output is writable.
    if unsafe { GetDiskFreeSpaceExW(path.as_ptr(), &mut available, null_mut(), null_mut()) } == 0 {
        return Err(io_error(
            "query available disk space",
            std::io::Error::last_os_error(),
        ));
    }
    Ok(available)
}

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn verbatim_file_path(path: &Path) -> Vec<u16> {
    verbatim_path(path)
        .as_os_str()
        .encode_wide()
        .chain(Some(0))
        .collect()
}

/// Stable identity for a watched directory: the volume serial plus file
/// index of the open handle, so a rename-away plus fresh-directory
/// replacement is detectable even though the path never goes absent. `None`
/// when the path cannot be opened (it raced a removal) or is not a
/// directory.
pub(crate) fn directory_identity(path: &Path) -> Option<(u64, u64)> {
    let verbatim = verbatim_file_path(path);
    // SAFETY: verbatim is NUL-terminated and lives through the call; zero
    // access only queries metadata, backup semantics allow opening
    // directories, and the handle is uniquely owned when valid.
    let handle = unsafe {
        CreateFileW(
            verbatim.as_ptr(),
            0,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            null(),
            OPEN_EXISTING,
            FILE_FLAG_BACKUP_SEMANTICS,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return None;
    }
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the handle is valid and info is writable storage for the call.
    let identity = if unsafe { GetFileInformationByHandle(handle, &mut info) } == 0
        || info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY == 0
    {
        None
    } else {
        Some((
            u64::from(info.dwVolumeSerialNumber),
            (u64::from(info.nFileIndexHigh) << 32) | u64::from(info.nFileIndexLow),
        ))
    };
    // SAFETY: the handle came from CreateFileW and closes exactly once here.
    unsafe { CloseHandle(handle) };
    identity
}

/// Uniquely owned handle for a handle-based private-data security call.
struct SecurityHandle(HANDLE);

impl Drop for SecurityHandle {
    fn drop(&mut self) {
        // SAFETY: the handle came from CreateFileW and closes exactly once here.
        unsafe { CloseHandle(self.0) };
    }
}

/// Open a private-data file or directory for a handle-based security call.
///
/// Backup semantics allow opening directories; pass READ_CONTROL to inspect
/// or READ_CONTROL | WRITE_DAC to replace the DACL.
fn open_security_handle(path: &Path, access: u32, directory: bool) -> Result<SecurityHandle> {
    let verbatim = verbatim_file_path(path);
    // SAFETY: verbatim is NUL-terminated and lives through the call; the
    // returned handle is uniquely owned when valid. OPEN_REPARSE_POINT stops
    // the open itself from following a junction or symlink, so a swap between
    // the path check and this open cannot redirect the handle to a target.
    let handle = unsafe {
        CreateFileW(
            verbatim.as_ptr(),
            access,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            null(),
            OPEN_EXISTING,
            FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io_error(
            "open private data path for ACL inspection",
            std::io::Error::last_os_error(),
        ));
    }
    let guard = SecurityHandle(handle);
    validate_security_handle(&guard, directory)?;
    Ok(guard)
}

fn security_kind(directory: bool) -> &'static str {
    if directory {
        "unsafe_data_dir"
    } else {
        "unsafe_private_file"
    }
}

/// Validate the opened handle itself: it must be a disk file or directory of
/// the expected kind and must not be a reparse point. This closes the
/// check-to-open race where a junction swapped in after the path check would
/// otherwise redirect ACL inspection or mutation to its target.
fn validate_security_handle(handle: &SecurityHandle, directory: bool) -> Result<()> {
    // SAFETY: the handle is a valid open file handle owned by the guard.
    let file_type = unsafe { GetFileType(handle.0) };
    if file_type != FILE_TYPE_DISK {
        return Err(Error::new(
            security_kind(directory),
            "private data path is not a file or directory",
        ));
    }
    let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
    // SAFETY: the handle is valid and info is writable storage for the call.
    if unsafe { GetFileInformationByHandle(handle.0, &mut info) } == 0 {
        return Err(io_error(
            "inspect private data handle",
            std::io::Error::last_os_error(),
        ));
    }
    if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(Error::new(
            security_kind(directory),
            "private data path is a reparse point",
        ));
    }
    let is_dir = info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0;
    if is_dir != directory {
        return Err(Error::new(
            security_kind(directory),
            "private data path has the wrong file kind",
        ));
    }
    Ok(())
}

fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{action}: {error}"))
}

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtResumeProcess(process_handle: HANDLE) -> i32;
}
