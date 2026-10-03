//! Windows native-process runner for the configured Git client.
//!
//! A dedicated Job Object is supplied through PROC_THREAD_ATTRIBUTE_JOB_LIST
//! during CreateProcessW, so Git cannot run before it is inside the job. The
//! runner drains anonymous pipes synchronously with PeekNamedPipe/ReadFile and
//! joins the job before returning; descendants cannot retain a pipe and strand
//! a reader thread.

use super::{ForgeConfig, ForgeProject, GitOutput};
use crate::error::{Error, Result};
use sha2::{Digest, Sha256};
use std::{
    cmp::Ordering,
    env,
    ffi::{OsStr, OsString},
    mem::size_of,
    os::windows::{ffi::OsStrExt, process::ExitStatusExt},
    path::Path,
    ptr::{null, null_mut},
    thread,
    time::{Duration, Instant},
};
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_BROKEN_PIPE, FALSE, GENERIC_READ, GetLastError, HANDLE,
        HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation, TRUE, WAIT_FAILED,
        WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    Globalization::{CSTR_EQUAL, CSTR_GREATER_THAN, CSTR_LESS_THAN, CompareStringOrdinal},
    Security::SECURITY_ATTRIBUTES,
    Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
        ReadFile,
    },
    System::{
        JobObjects::{
            CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
            JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
            JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
            QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
        },
        Pipes::{CreatePipe, PeekNamedPipe},
        Threading::{
            CREATE_NO_WINDOW, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
            DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, GetExitCodeProcess,
            InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_HANDLE_LIST,
            PROC_THREAD_ATTRIBUTE_JOB_LIST, PROCESS_INFORMATION, STARTF_USESTDHANDLES,
            STARTUPINFOEXW, UpdateProcThreadAttribute, WaitForSingleObject,
        },
    },
};

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const MAX_DRAIN_PER_POLL: u32 = 64 * 1024;
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(10);
const DROP_CLEANUP_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_WINDOWS_COMMAND_LINE: usize = 32_767;
const MAX_WINDOWS_ENVIRONMENT: usize = 32_767;
const FORCED_EXIT_CODE: u32 = 0xE117_0001;

pub(super) fn run_git(
    config: &ForgeConfig,
    project: &ForgeProject,
    args: &[String],
) -> Result<GitOutput> {
    let mut argv = vec![
        config.git_executable.as_os_str().to_owned(),
        OsString::from("--no-optional-locks"),
        OsString::from("--no-replace-objects"),
        OsString::from("-c"),
        OsString::from("core.fsmonitor=false"),
        OsString::from("-C"),
        project.repository_path.as_os_str().to_owned(),
    ];
    argv.extend(
        args.iter()
            .map(|argument| OsString::from(argument.as_str())),
    );

    let environment = env::vars_os().collect::<Vec<_>>();
    run_process(
        config.git_executable.as_os_str(),
        &argv,
        &project.repository_path,
        environment,
        Duration::from_secs(config.timeout_seconds),
        config.max_output_bytes,
    )
}

fn run_process(
    executable: &OsStr,
    argv: &[OsString],
    current_dir: &Path,
    environment: Vec<(OsString, OsString)>,
    timeout: Duration,
    output_cap: usize,
) -> Result<GitOutput> {
    let executable_wide = wide_nul(executable);
    let current_dir_wide = wide_nul(current_dir.as_os_str());
    let mut command_line = command_line(argv)?;
    let environment = environment_block(environment)?;
    let mut job = Job::new()?;
    let mut attributes = Attributes::new(job.handle)?;
    let security = inheritable_security_attributes();
    let (stdout_reader, stdout_writer) = anonymous_pipe(&security)?;
    let (stderr_reader, stderr_writer) = anonymous_pipe(&security)?;
    let stdin = null_stdin(&security)?;
    clear_inheritance(stdout_reader.handle)?;
    clear_inheritance(stderr_reader.handle)?;
    attributes.allow_handles(&[stdin.handle, stdout_writer.handle, stderr_writer.handle])?;

    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = stdin.handle;
    startup.StartupInfo.hStdOutput = stdout_writer.handle;
    startup.StartupInfo.hStdError = stderr_writer.handle;
    startup.lpAttributeList = attributes.list;

    let mut process_info = PROCESS_INFORMATION::default();
    // SAFETY: all strings and attribute storage remain alive through the call;
    // std handles are explicitly allowlisted and inheritable, and the job is
    // atomically assigned before the process begins executing.
    let created = unsafe {
        CreateProcessW(
            executable_wide.as_ptr(),
            command_line.as_mut_ptr(),
            null(),
            null(),
            TRUE,
            CREATE_NO_WINDOW | CREATE_UNICODE_ENVIRONMENT | EXTENDED_STARTUPINFO_PRESENT,
            environment.as_ptr().cast(),
            current_dir_wide.as_ptr(),
            &startup.StartupInfo,
            &mut process_info,
        )
    };
    if created == FALSE {
        return Err(last_win32("CreateProcessW"));
    }
    job.empty = false;
    let result: Result<GitOutput> = (|| {
        // Only the child owns these pipe ends now. The parent-side read handles
        // remain non-inheritable and are the only handles polled below.
        drop(stdin);
        drop(stdout_writer);
        drop(stderr_writer);
        let process = Handle::from_raw(process_info.hProcess)?;
        let thread_handle = Handle::from_raw(process_info.hThread)?;
        drop(thread_handle);

        let deadline = Instant::now() + timeout;
        let mut stdout = PipeReader::new(stdout_reader, output_cap, false);
        let mut stderr = PipeReader::new(stderr_reader, output_cap, true);
        let mut timed_out = false;
        let exit_code = loop {
            stdout.drain_available()?;
            stderr.drain_available()?;
            match wait(process.handle, 0)? {
                WAIT_OBJECT_0 => break process_exit_code(process.handle)?,
                WAIT_TIMEOUT if Instant::now() < deadline => thread::sleep(POLL_INTERVAL),
                WAIT_TIMEOUT => {
                    timed_out = true;
                    job.terminate_and_join()?;
                    break process_exit_code(process.handle)?;
                }
                _ => return Err(last_win32("WaitForSingleObject(process)")),
            }
        };

        // Git can exit while a helper still owns inherited standard handles. End
        // those helpers on ordinary exit too, then wait until the job is empty.
        job.terminate_remaining_and_join()?;
        stdout.drain_after_job_exit()?;
        stderr.drain_after_job_exit()?;

        let stdout_truncated = stdout.total > stdout.bytes.len() as u64;
        Ok(GitOutput {
            status: std::process::ExitStatus::from_raw(exit_code),
            stdout: stdout.bytes,
            stdout_truncated,
            stderr_digest: stderr.digest_hex(),
            stderr_bytes: stderr.total,
            timed_out,
        })
    })();

    match result {
        Ok(output) => Ok(output),
        Err(error) if job.empty => Err(confirmed_cleanup_error(error)),
        Err(error) => match job.terminate_and_join() {
            Ok(()) => Err(confirmed_cleanup_error(error)),
            Err(cleanup_error) => Err(Error::new(
                "FORGE_GIT_TREE_TERMINATION",
                format!(
                    "post-spawn Git operation failed ({}) and process-tree cleanup is unconfirmed ({})",
                    error.code, cleanup_error.code
                ),
            )),
        },
    }
}

fn confirmed_cleanup_error(error: Error) -> Error {
    if error.code == "FORGE_GIT_TREE_TERMINATION" {
        Error::new(
            "FORGE_GIT_TREE_CLEANUP_DELAYED",
            "Job Object cleanup exceeded its first deadline but a later query confirmed the process tree empty",
        )
    } else {
        error
    }
}

fn inheritable_security_attributes() -> SECURITY_ATTRIBUTES {
    SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: TRUE,
    }
}

fn anonymous_pipe(security: &SECURITY_ATTRIBUTES) -> Result<(Handle, Handle)> {
    let mut reader = null_mut();
    let mut writer = null_mut();
    // SAFETY: both out-pointers target initialized HANDLE locals; the security
    // attributes live for the call and request inheritable pipe handles.
    if unsafe { CreatePipe(&mut reader, &mut writer, security, 0) } == FALSE {
        return Err(last_win32("CreatePipe"));
    }
    Ok((Handle::from_raw(reader)?, Handle::from_raw(writer)?))
}

fn null_stdin(security: &SECURITY_ATTRIBUTES) -> Result<Handle> {
    let nul = wide_nul(OsStr::new("NUL"));
    // SAFETY: NUL is a fixed, NUL-terminated Windows device path; the handle is
    // opened for reading and is inheritable only by the explicitly allowlisted
    // child process.
    let handle = unsafe {
        CreateFileW(
            nul.as_ptr(),
            GENERIC_READ,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            security,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            null_mut(),
        )
    };
    Handle::from_raw(handle)
}

fn clear_inheritance(handle: HANDLE) -> Result<()> {
    // SAFETY: `handle` is an owned pipe read handle created by CreatePipe.
    if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == FALSE {
        return Err(last_win32("SetHandleInformation"));
    }
    Ok(())
}

fn command_line(argv: &[OsString]) -> Result<Vec<u16>> {
    if argv.is_empty() {
        return Err(Error::new("FORGE_GIT_START", "Git argv is empty"));
    }
    let mut result = Vec::new();
    for (index, argument) in argv.iter().enumerate() {
        if index != 0 {
            result.push(b' ' as u16);
        }
        quote_windows_argument(argument, &mut result);
    }
    if result.len() + 1 > MAX_WINDOWS_COMMAND_LINE {
        return Err(Error::new(
            "FORGE_GIT_START",
            "Git command line exceeds the Windows process limit",
        ));
    }
    result.push(0);
    Ok(result)
}

fn quote_windows_argument(argument: &OsStr, output: &mut Vec<u16>) {
    let units = argument.encode_wide().collect::<Vec<_>>();
    let quote = units.is_empty()
        || units
            .iter()
            .any(|unit| matches!(*unit, 0x20 | 0x09) || *unit == b'"' as u16);
    if !quote {
        output.extend(units);
        return;
    }

    output.push(b'"' as u16);
    let mut slashes = 0usize;
    for unit in units {
        if unit == b'\\' as u16 {
            slashes += 1;
        } else if unit == b'"' as u16 {
            output.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2 + 1));
            output.push(unit);
            slashes = 0;
        } else {
            output.extend(std::iter::repeat_n(b'\\' as u16, slashes));
            output.push(unit);
            slashes = 0;
        }
    }
    output.extend(std::iter::repeat_n(b'\\' as u16, slashes * 2));
    output.push(b'"' as u16);
}

fn wide_nul(value: &OsStr) -> Vec<u16> {
    let mut wide = value.encode_wide().collect::<Vec<_>>();
    wide.push(0);
    wide
}

fn environment_block(mut variables: Vec<(OsString, OsString)>) -> Result<Vec<u16>> {
    variables.retain(|(key, _)| !remove_git_environment_key(key));
    deduplicate_environment(&mut variables);
    set_environment(&mut variables, "GIT_TERMINAL_PROMPT", "0");
    set_environment(&mut variables, "GIT_OPTIONAL_LOCKS", "0");
    variables.sort_by(|(left, _), (right, _)| ordinal_ignore_case(left, right));

    let mut block = Vec::new();
    for (key, value) in variables {
        let key = key.encode_wide().collect::<Vec<_>>();
        let value = value.encode_wide().collect::<Vec<_>>();
        if key.contains(&0) || value.contains(&0) || key.is_empty() {
            return Err(Error::new(
                "FORGE_GIT_ENV",
                "Windows environment contains an invalid entry",
            ));
        }
        block.extend(key);
        block.push(b'=' as u16);
        block.extend(value);
        block.push(0);
        if block.len() + 1 > MAX_WINDOWS_ENVIRONMENT {
            return Err(Error::new(
                "FORGE_GIT_ENV",
                "Windows environment block exceeds the process limit",
            ));
        }
    }
    block.push(0);
    if block.len() == 1 {
        block.push(0);
    }
    Ok(block)
}

fn remove_git_environment_key(key: &OsStr) -> bool {
    const EXACT: &[&str] = &[
        "GIT_DIR",
        "GIT_WORK_TREE",
        "GIT_INDEX_FILE",
        "GIT_OBJECT_DIRECTORY",
        "GIT_ALTERNATE_OBJECT_DIRECTORIES",
        "GIT_COMMON_DIR",
        "GIT_NAMESPACE",
        "GIT_SSH",
        "GIT_SSH_COMMAND",
        "GIT_ASKPASS",
        "SSH_ASKPASS",
        "GIT_CONFIG",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_CURL_VERBOSE",
    ];
    let normalized = uppercase_ascii_units(key);
    EXACT
        .iter()
        .any(|name| normalized == name.encode_utf16().collect::<Vec<_>>())
        || normalized.starts_with(&"GIT_CONFIG_KEY_".encode_utf16().collect::<Vec<_>>())
        || normalized.starts_with(&"GIT_CONFIG_VALUE_".encode_utf16().collect::<Vec<_>>())
        || normalized.starts_with(&"GIT_TRACE".encode_utf16().collect::<Vec<_>>())
}

fn uppercase_ascii_units(value: &OsStr) -> Vec<u16> {
    value
        .encode_wide()
        .map(|unit| {
            if (b'a' as u16..=b'z' as u16).contains(&unit) {
                unit - (b'a' as u16 - b'A' as u16)
            } else {
                unit
            }
        })
        .collect()
}

fn deduplicate_environment(variables: &mut Vec<(OsString, OsString)>) {
    let mut unique: Vec<(OsString, OsString)> = Vec::with_capacity(variables.len());
    for (key, value) in variables.drain(..) {
        if let Some(existing) = unique
            .iter_mut()
            .find(|(existing, _)| ordinal_ignore_case(existing, &key) == Ordering::Equal)
        {
            *existing = (key, value);
        } else {
            unique.push((key, value));
        }
    }
    *variables = unique;
}

fn set_environment(variables: &mut Vec<(OsString, OsString)>, name: &str, value: &str) {
    variables.retain(|(key, _)| ordinal_ignore_case(key, OsStr::new(name)) != Ordering::Equal);
    variables.push((OsString::from(name), OsString::from(value)));
}

fn ordinal_ignore_case(left: &OsStr, right: &OsStr) -> Ordering {
    let left = left.encode_wide().collect::<Vec<_>>();
    let right = right.encode_wide().collect::<Vec<_>>();
    // SAFETY: both pointers address UTF-16 buffers of the supplied lengths;
    // the comparison does not retain either pointer.
    match unsafe {
        CompareStringOrdinal(
            left.as_ptr(),
            left.len() as i32,
            right.as_ptr(),
            right.len() as i32,
            TRUE,
        )
    } {
        CSTR_LESS_THAN => Ordering::Less,
        CSTR_EQUAL => Ordering::Equal,
        CSTR_GREATER_THAN => Ordering::Greater,
        _ => left.cmp(&right),
    }
}

fn process_exit_code(process: HANDLE) -> Result<u32> {
    let mut code = 0;
    // SAFETY: `process` is an owned process handle returned by CreateProcessW.
    if unsafe { GetExitCodeProcess(process, &mut code) } == FALSE {
        return Err(last_win32("GetExitCodeProcess"));
    }
    Ok(code)
}

fn wait(handle: HANDLE, milliseconds: u32) -> Result<u32> {
    // SAFETY: `handle` remains valid and owned for the duration of this wait.
    let result = unsafe { WaitForSingleObject(handle, milliseconds) };
    if result == WAIT_FAILED {
        Err(last_win32("WaitForSingleObject"))
    } else {
        Ok(result)
    }
}

fn last_win32(operation: &str) -> Error {
    // SAFETY: GetLastError is thread-local and is read immediately following
    // the failing Windows API call.
    let code = unsafe { GetLastError() };
    Error::new(
        "FORGE_GIT_WIN32",
        format!("{operation} failed with Win32 error {code}"),
    )
}

struct Handle {
    handle: HANDLE,
}

impl Handle {
    fn from_raw(handle: HANDLE) -> Result<Self> {
        if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            Err(last_win32("handle creation"))
        } else {
            Ok(Self { handle })
        }
    }
}

impl Drop for Handle {
    fn drop(&mut self) {
        if !self.handle.is_null() && self.handle != INVALID_HANDLE_VALUE {
            // SAFETY: this wrapper owns the handle and closes it exactly once.
            unsafe { CloseHandle(self.handle) };
        }
    }
}

struct Job {
    handle: HANDLE,
    empty: bool,
}

impl Job {
    fn new() -> Result<Self> {
        // SAFETY: null attributes create an unnamed, non-inheritable Job.
        let handle = unsafe { CreateJobObjectW(null(), null()) };
        let handle = if handle.is_null() || handle == INVALID_HANDLE_VALUE {
            return Err(last_win32("CreateJobObjectW"));
        } else {
            handle
        };
        let job = Self {
            handle,
            empty: true,
        };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        // SAFETY: limits has the exact documented structure and size for this
        // information class.
        let configured = unsafe {
            SetInformationJobObject(
                job.handle,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if configured == FALSE {
            return Err(last_win32("SetInformationJobObject"));
        }
        Ok(job)
    }

    fn terminate_remaining_and_join(&mut self) -> Result<()> {
        if self.active_processes()? == 0 {
            self.empty = true;
            Ok(())
        } else {
            self.terminate_and_join()
        }
    }

    fn terminate_and_join(&mut self) -> Result<()> {
        if self.active_processes()? == 0 {
            self.empty = true;
            return Ok(());
        }
        // SAFETY: this is the dedicated Job Object assigned only to this Git
        // process tree; no host or unrelated process belongs to it.
        let terminated = unsafe { TerminateJobObject(self.handle, FORCED_EXIT_CODE) };
        if terminated == FALSE {
            let termination_error = last_win32("TerminateJobObject");
            // The root can exit between the zero-time wait and this call. In
            // that case the accounting query observes an empty job.
            if self.wait_until_empty(CLEANUP_TIMEOUT).is_ok() {
                return Ok(());
            }
            return Err(termination_error);
        }
        self.wait_until_empty(CLEANUP_TIMEOUT)
    }

    fn active_processes(&self) -> Result<u32> {
        let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
        // SAFETY: `accounting` is the documented output structure for this
        // information class and remains writable for the call.
        if unsafe {
            QueryInformationJobObject(
                self.handle,
                JobObjectBasicAccountingInformation,
                (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                null_mut(),
            )
        } == FALSE
        {
            Err(last_win32("QueryInformationJobObject"))
        } else {
            Ok(accounting.ActiveProcesses)
        }
    }

    fn wait_until_empty(&mut self, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.active_processes()? == 0 {
                self.empty = true;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::new(
                    "FORGE_GIT_TREE_TERMINATION",
                    "Git process tree did not stop before the cleanup deadline",
                ));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }
}

impl Drop for Job {
    fn drop(&mut self) {
        if !self.empty {
            // Drop has no error channel, so use a finite best-effort cleanup;
            // closing the last handle then enforces KILL_ON_JOB_CLOSE.
            let _ = unsafe { TerminateJobObject(self.handle, FORCED_EXIT_CODE) };
            let _ = self.wait_until_empty(DROP_CLEANUP_TIMEOUT);
        }
        // KILL_ON_JOB_CLOSE is a final cleanup guarantee if the explicit
        // termination path encountered a Windows API failure.
        // SAFETY: Job owns the handle and closes it once.
        unsafe { CloseHandle(self.handle) };
    }
}

struct Attributes {
    _storage: Vec<usize>,
    list: *mut core::ffi::c_void,
    _job_handles: Box<[HANDLE; 1]>,
    _inherited_handles: Vec<HANDLE>,
}

impl Attributes {
    fn new(job: HANDLE) -> Result<Self> {
        let mut byte_len = 0usize;
        // SAFETY: documented sizing call; a null list obtains required bytes.
        let _ = unsafe { InitializeProcThreadAttributeList(null_mut(), 2, 0, &mut byte_len) };
        if byte_len == 0 {
            return Err(last_win32("InitializeProcThreadAttributeList(size)"));
        }
        let mut storage = vec![0usize; byte_len.div_ceil(size_of::<usize>())];
        let list = storage.as_mut_ptr().cast();
        // SAFETY: storage is suitably aligned and large enough per the sizing
        // call; two attributes are installed below.
        if unsafe { InitializeProcThreadAttributeList(list, 2, 0, &mut byte_len) } == FALSE {
            return Err(last_win32("InitializeProcThreadAttributeList"));
        }
        let result = Self {
            _storage: storage,
            list,
            _job_handles: Box::new([job]),
            _inherited_handles: Vec::new(),
        };
        // SAFETY: the job handle is valid and has assignment rights; the
        // process is assigned before it can execute.
        if unsafe {
            UpdateProcThreadAttribute(
                result.list,
                0,
                PROC_THREAD_ATTRIBUTE_JOB_LIST as usize,
                result._job_handles.as_ptr().cast(),
                size_of::<HANDLE>(),
                null_mut(),
                null(),
            )
        } == FALSE
        {
            return Err(last_win32("UpdateProcThreadAttribute(job list)"));
        }
        Ok(result)
    }

    fn allow_handles(&mut self, handles: &[HANDLE]) -> Result<()> {
        self._inherited_handles = handles.to_vec();
        if self._inherited_handles.is_empty() {
            return Err(Error::new(
                "FORGE_GIT_HANDLE_LIST",
                "child process handle allowlist cannot be empty",
            ));
        }
        // SAFETY: all handles are live and inheritable; the list storage stays
        // alive until CreateProcessW consumes this attribute list.
        if unsafe {
            UpdateProcThreadAttribute(
                self.list,
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                self._inherited_handles.as_ptr().cast(),
                size_of::<HANDLE>() * self._inherited_handles.len(),
                null_mut(),
                null(),
            )
        } == FALSE
        {
            Err(last_win32("UpdateProcThreadAttribute(handle list)"))
        } else {
            Ok(())
        }
    }
}

impl Drop for Attributes {
    fn drop(&mut self) {
        // SAFETY: a successful InitializeProcThreadAttributeList initialized
        // this list, and `storage` remains allocated until after this call.
        unsafe { DeleteProcThreadAttributeList(self.list) };
    }
}

struct PipeReader {
    handle: Handle,
    cap: usize,
    total: u64,
    bytes: Vec<u8>,
    eof: bool,
    hash_all_bytes: bool,
    hasher: Sha256,
}

impl PipeReader {
    fn new(handle: Handle, cap: usize, hash_all_bytes: bool) -> Self {
        Self {
            handle,
            cap,
            total: 0,
            bytes: Vec::with_capacity(cap.min(8192)),
            eof: false,
            hash_all_bytes,
            hasher: Sha256::new(),
        }
    }

    fn drain_available(&mut self) -> Result<()> {
        if self.eof {
            return Ok(());
        }
        let mut drained_this_poll = 0u32;
        loop {
            let mut available = 0u32;
            // SAFETY: this is a synchronous local anonymous pipe read handle;
            // PeekNamedPipe only reports the bytes available without consuming.
            if unsafe {
                PeekNamedPipe(
                    self.handle.handle,
                    null_mut(),
                    0,
                    null_mut(),
                    &mut available,
                    null_mut(),
                )
            } == FALSE
            {
                if unsafe { GetLastError() } == ERROR_BROKEN_PIPE {
                    self.eof = true;
                    return Ok(());
                }
                return Err(last_win32("PeekNamedPipe"));
            }
            if available == 0 {
                return Ok(());
            }
            if drained_this_poll >= MAX_DRAIN_PER_POLL {
                return Ok(());
            }
            let mut buffer = [0u8; 8192];
            let request = available
                .min(buffer.len() as u32)
                .min(MAX_DRAIN_PER_POLL - drained_this_poll);
            let mut read = 0u32;
            // SAFETY: `buffer` has at least `request` bytes, the pipe is
            // synchronous, and the request does not exceed PeekNamedPipe's
            // immediately available byte count.
            if unsafe {
                ReadFile(
                    self.handle.handle,
                    buffer.as_mut_ptr(),
                    request,
                    &mut read,
                    null_mut(),
                )
            } == FALSE
            {
                if unsafe { GetLastError() } == ERROR_BROKEN_PIPE {
                    self.eof = true;
                    return Ok(());
                }
                return Err(last_win32("ReadFile(pipe)"));
            }
            if read == 0 {
                return Ok(());
            }
            drained_this_poll = drained_this_poll.saturating_add(read);
            self.total = self.total.saturating_add(read as u64);
            if self.hash_all_bytes {
                self.hasher.update(&buffer[..read as usize]);
            }
            let retained = self.cap.saturating_sub(self.bytes.len()).min(read as usize);
            self.bytes.extend_from_slice(&buffer[..retained]);
        }
    }

    fn drain_after_job_exit(&mut self) -> Result<()> {
        loop {
            let previous_total = self.total;
            self.drain_available()?;
            if self.eof || self.total == previous_total {
                return Ok(());
            }
        }
    }

    fn digest_hex(&self) -> String {
        format!("{:x}", self.hasher.clone().finalize())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::{Command, Stdio};

    const CHILD_MODE: &str = "ELIOT_FORGE_DESCENDANT_TEST";

    #[test]
    fn windows_argv_uses_crt_backslash_and_quote_rules() {
        let mut output = Vec::new();
        quote_windows_argument(OsStr::new("plain"), &mut output);
        assert_eq!(String::from_utf16(&output).unwrap(), "plain");

        output.clear();
        quote_windows_argument(OsStr::new("path with space\\"), &mut output);
        assert_eq!(
            String::from_utf16(&output).unwrap(),
            "\"path with space\\\\\""
        );

        output.clear();
        quote_windows_argument(OsStr::new("say\"hello"), &mut output);
        assert_eq!(String::from_utf16(&output).unwrap(), "\"say\\\"hello\"");
    }

    #[test]
    fn windows_environment_is_sanitized_sorted_and_preserves_os_values() {
        use std::os::windows::ffi::OsStringExt;

        let unusual_value = OsString::from_wide(&[b'x' as u16, 0xD800, b'y' as u16]);
        let block = environment_block(vec![
            (OsString::from("zLast"), OsString::from("z")),
            (OsString::from("gIt_CoNfIg_CoUnT"), OsString::from("7")),
            (OsString::from("Ä-test"), unusual_value.clone()),
            (OsString::from("aFirst"), OsString::from("a")),
        ])
        .unwrap();
        let entries = block
            .split(|unit| *unit == 0)
            .filter(|entry| !entry.is_empty())
            .map(<[u16]>::to_vec)
            .collect::<Vec<_>>();
        let names = entries
            .iter()
            .map(|entry| {
                let split = entry.iter().position(|unit| *unit == b'=' as u16).unwrap();
                OsString::from_wide(&entry[..split])
            })
            .collect::<Vec<_>>();
        assert!(
            names
                .iter()
                .any(|name| name.as_os_str() == OsStr::new("GIT_TERMINAL_PROMPT"))
        );
        assert!(
            names
                .iter()
                .any(|name| name.as_os_str() == OsStr::new("GIT_OPTIONAL_LOCKS"))
        );
        assert!(
            !names.iter().any(
                |name| ordinal_ignore_case(name, OsStr::new("GIT_CONFIG_COUNT")) == Ordering::Equal
            )
        );
        assert!(
            names
                .windows(2)
                .all(|pair| ordinal_ignore_case(&pair[0], &pair[1]) != Ordering::Greater)
        );
        let unusual = entries
            .iter()
            .find(|entry| entry.starts_with(&"Ä-test=".encode_utf16().collect::<Vec<_>>()))
            .unwrap();
        let equal = unusual
            .iter()
            .position(|unit| *unit == b'=' as u16)
            .unwrap();
        assert_eq!(OsString::from_wide(&unusual[equal + 1..]), unusual_value);
    }

    #[test]
    fn job_kills_descendant_that_keeps_pipe_handles_after_root_exit() {
        if env::var_os(CHILD_MODE).is_some() {
            // The parent test process exits immediately while this descendant
            // retains inherited stdout/stderr handles. The Job Object must
            // end it before run_process returns.
            let child = Command::new("cmd.exe")
                .args(["/C", "ping -n 30 127.0.0.1"])
                .stdin(Stdio::inherit())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .spawn()
                .unwrap();
            drop(child);
            return;
        }

        let test_exe = env::current_exe().unwrap();
        let test_name =
            "forge::windows::tests::job_kills_descendant_that_keeps_pipe_handles_after_root_exit";
        let argv = vec![
            test_exe.as_os_str().to_owned(),
            OsString::from("--exact"),
            OsString::from(test_name),
            OsString::from("--nocapture"),
        ];
        let mut environment = env::vars_os().collect::<Vec<_>>();
        environment.push((OsString::from(CHILD_MODE), OsString::from("1")));
        let started = Instant::now();
        let output = run_process(
            test_exe.as_os_str(),
            &argv,
            &env::current_dir().unwrap(),
            environment,
            Duration::from_secs(10),
            8192,
        )
        .unwrap();
        assert!(output.status.success());
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
