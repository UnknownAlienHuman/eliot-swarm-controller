#![allow(unsafe_code)]

use super::process_observation::{
    MAX_PROCESS_SAMPLES, exited_sample, same_identity, unknown_sample,
};
use eliot_core::{
    Error, ProcessIdentity, ProcessRecord, ProcessSample, ProcessSnapshot, ProcessState, Result,
};
use std::ffi::{OsStr, c_void};
use std::fs;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::ptr::{null, null_mut};
use std::slice;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use windows_sys::Win32::Foundation::{
    CloseHandle, DuplicateHandle, ERROR_INVALID_PARAMETER, ERROR_MORE_DATA, FILETIME, HANDLE,
    HLOCAL, INVALID_HANDLE_VALUE, WAIT_OBJECT_0, WAIT_TIMEOUT,
};
use windows_sys::Win32::Security::Authorization::{
    ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
    GetNamedSecurityInfoW, SE_FILE_OBJECT, SetNamedSecurityInfoW,
};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACL_SIZE_INFORMATION, AclSizeInformation,
    CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, GetAce, GetAclInformation,
    GetSecurityDescriptorControl, GetSecurityDescriptorDacl, GetTokenInformation,
    OBJECT_INHERIT_ACE, OWNER_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION,
    SE_DACL_PROTECTED, SECURITY_ATTRIBUTES, TOKEN_QUERY, TOKEN_USER, TokenUser,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ALL_ACCESS, FILE_ATTRIBUTE_REPARSE_POINT, FILE_FLAG_FIRST_PIPE_INSTANCE,
    FILE_FLAG_OVERLAPPED, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
};
use windows_sys::Win32::System::JobObjects::{
    AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob, JOB_OBJECT_LIMIT_ACTIVE_PROCESS,
    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_BASIC_PROCESS_ID_LIST,
    JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicProcessIdList,
    JobObjectExtendedLimitInformation, QueryInformationJobObject, SetInformationJobObject,
    TerminateJobObject,
};
use windows_sys::Win32::System::Pipes::{
    CreateNamedPipeW, PIPE_READMODE_BYTE, PIPE_REJECT_REMOTE_CLIENTS, PIPE_TYPE_BYTE,
    PIPE_UNLIMITED_INSTANCES, PIPE_WAIT,
};
use windows_sys::Win32::System::ProcessStatus::{GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS};
use windows_sys::Win32::System::SystemInformation::{GetSystemTimeAsFileTime, GetTickCount64};
use windows_sys::Win32::System::Threading::{
    CREATE_NO_WINDOW, CreateProcessW, GetCurrentProcess, GetExitCodeProcess, GetProcessTimes,
    OpenProcess, OpenProcessToken, PROCESS_INFORMATION, PROCESS_QUERY_LIMITED_INFORMATION,
    PROCESS_SYNCHRONIZE, STARTUPINFOW, TerminateProcess, WaitForSingleObject,
};

pub(crate) fn start_background_daemon(data_dir: &Path) -> Result<()> {
    let executable =
        std::env::current_exe().map_err(|error| io_error("locate Eliot executable", error))?;
    let mut command_line = background_daemon_command_line(&executable, data_dir)?;
    let application = wide_path(&executable);
    let startup = STARTUPINFOW {
        cb: std::mem::size_of::<STARTUPINFOW>() as u32,
        ..STARTUPINFOW::default()
    };
    let mut process = PROCESS_INFORMATION::default();
    // SAFETY: application and the writable command line are NUL-terminated UTF-16.
    // All structures live through the call. FALSE disables ALL handle inheritance,
    // including otherwise-unused capture-pipe writers held by the CLI. No standard
    // handles are supplied; CREATE_NO_WINDOW prevents console inheritance/creation.
    // Null environment/current-directory pointers preserve the caller's values.
    if unsafe {
        CreateProcessW(
            application.as_ptr(),
            command_line.as_mut_ptr(),
            null(),
            null(),
            0,
            CREATE_NO_WINDOW,
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

fn background_daemon_command_line(executable: &Path, data_dir: &Path) -> Result<Vec<u16>> {
    let mut command_line = Vec::new();
    for argument in [
        executable.as_os_str(),
        OsStr::new("--data-dir"),
        data_dir.as_os_str(),
        OsStr::new("daemon"),
        OsStr::new("--foreground"),
    ] {
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

pub(crate) fn ensure_private_root(path: &Path) -> Result<()> {
    check_no_reparse_components(path)?;
    fs::create_dir_all(path).map_err(|error| io_error("create data directory", error))?;
    check_no_reparse_components(path)?;

    let metadata =
        fs::symlink_metadata(path).map_err(|error| io_error("inspect data directory", error))?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(Error::new(
            "unsafe_data_dir",
            "data directory must be a real directory, not a reparse point or file",
        ));
    }
    ensure_current_owner(path)?;
    set_current_user_acl(path, true)
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
    ensure_current_owner(path)?;
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

pub(crate) fn replace_file(source: &Path, target: &Path) -> Result<()> {
    let source_wide = wide_path(source);
    let target_wide = wide_path(target);
    // SAFETY: both paths are NUL-terminated UTF-16 strings for this call.
    if unsafe {
        MoveFileExW(
            source_wide.as_ptr(),
            target_wide.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        Err(io_error(
            "replace daemon metadata",
            std::io::Error::last_os_error(),
        ))
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

// The owned kernel handle may be transferred to another thread; no borrowed
// pointer or thread-affine state is stored in this wrapper.
unsafe impl Send for Job {}

impl Job {
    pub(crate) fn observer(&self, pid: u32) -> Result<ProcessObserver> {
        let mut query = self
            .query
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        Self::new_with_leaf_limit(false)
    }

    pub(crate) fn new_leaf() -> Result<Self> {
        Self::new_with_leaf_limit(true)
    }

    fn new_with_leaf_limit(leaf: bool) -> Result<Self> {
        // SAFETY: null attributes/name request a private unnamed Job Object.
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        if handle.is_null() {
            return Err(io_error(
                "create process Job Object",
                std::io::Error::last_os_error(),
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

    pub(crate) fn assign_then_resume(&self, process: HANDLE) -> Result<()> {
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
        self.query
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        // SAFETY: this object uniquely owns the handle.
        unsafe { CloseHandle(self.handle) };
    }
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
        let query = self
            .query
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        let query = self
            .query
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        let query = self
            .query
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
    let path_wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    // SAFETY: path_wide is NUL-terminated UTF-16 for this call.
    let attributes =
        unsafe { windows_sys::Win32::Storage::FileSystem::GetFileAttributesW(path_wide.as_ptr()) };
    if attributes == u32::MAX {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(attributes)
    }
}

fn ensure_current_owner(path: &Path) -> Result<()> {
    let path_wide = wide_path(path);
    let mut owner = null_mut();
    let mut descriptor = null_mut();
    // SAFETY: outputs are writable pointers and the requested owner SID is
    // returned inside the allocated security descriptor.
    let status = unsafe {
        GetNamedSecurityInfoW(
            path_wide.as_ptr(),
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

    let user_sid = current_user_sid();
    let same_owner = match user_sid {
        Ok(user_sid) if !owner.is_null() => {
            // SAFETY: both SID pointers remain valid until descriptor/buffer drop.
            unsafe { EqualSid(owner, sid_pointer(&user_sid)) != 0 }
        }
        Ok(_) => false,
        Err(error) => {
            // SAFETY: descriptor is owned by this function.
            unsafe { windows_sys::Win32::Foundation::LocalFree(descriptor as HLOCAL) };
            return Err(error);
        }
    };
    // SAFETY: GetNamedSecurityInfoW allocated this descriptor for the caller.
    unsafe { windows_sys::Win32::Foundation::LocalFree(descriptor as HLOCAL) };
    if same_owner {
        Ok(())
    } else {
        Err(Error::new(
            "unsafe_data_dir",
            "data root or private file is owned by another Windows user",
        ))
    }
}

fn validate_current_user_acl(path: &Path, directory: bool) -> Result<()> {
    let path_wide = wide_path(path);
    let mut owner = null_mut();
    let mut dacl = null_mut();
    let mut descriptor = null_mut();
    // SAFETY: output pointers are writable; the owner and DACL pointers are
    // backed by the returned security descriptor until it is freed below.
    let status = unsafe {
        GetNamedSecurityInfoW(
            path_wide.as_ptr(),
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
            return Err(Error::new(
                "unsafe_data_dir",
                "data root or private file is owned by another Windows user",
            ));
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

    let path_wide = wide_path(path);
    // SAFETY: path, user SID buffer and DACL remain live for this call. The
    // protected DACL replaces inherited access with a single user ACE.
    let status = unsafe {
        SetNamedSecurityInfoW(
            path_wide.as_ptr(),
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
    let mut text: *mut u16 = null_mut();
    // SAFETY: the SID pointer references the aligned, live token buffer.
    if unsafe { ConvertSidToStringSidW(sid_pointer(buffer), &mut text) } == 0 {
        return Err(io_error(
            "format current user SID",
            std::io::Error::last_os_error(),
        ));
    }
    let mut length = 0usize;
    // SAFETY: ConvertSidToStringSidW returns a NUL-terminated allocation.
    while unsafe { *text.add(length) } != 0 {
        length += 1;
        if length > 256 {
            // SAFETY: text was allocated by ConvertSidToStringSidW.
            unsafe { windows_sys::Win32::Foundation::LocalFree(text as HLOCAL) };
            return Err(Error::new("platform_acl", "current user SID is too long"));
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

fn wide_path(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(Some(0)).collect()
}

fn io_error(action: &str, error: std::io::Error) -> Error {
    Error::new("platform_io", format!("{action}: {error}"))
}

#[link(name = "ntdll")]
unsafe extern "system" {
    fn NtResumeProcess(process_handle: HANDLE) -> i32;
}
