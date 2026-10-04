//! OS process ownership shared by checks, scripts and independently launched modules.
//! Checks and scripts use kill-on-close; module groups never kill on owner loss.
use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{fs::File, io::Read, path::Path};

const MAX_PROCESS_IMAGE_BYTES: u64 = 512 * 1024 * 1024;

fn image_path_text(path: &Path) -> Result<String> {
    let value = path.to_str().ok_or_else(|| {
        Error::new(
            "PROCESS_IDENTITY",
            "process executable path is not valid Unicode",
        )
    })?;
    if value.is_empty() || value.len() > 32 * 1024 || value.chars().any(char::is_control) {
        return Err(Error::new(
            "PROCESS_IDENTITY",
            "process executable path is outside the supported boundary",
        ));
    }
    Ok(value.to_owned())
}

fn hash_process_image(file: &mut File) -> Result<String> {
    let before = file.metadata()?;
    if !before.is_file() || before.len() == 0 || before.len() > MAX_PROCESS_IMAGE_BYTES {
        return Err(Error::new(
            "PROCESS_IDENTITY",
            "process executable is outside the supported file boundary",
        ));
    }
    let modified_before = before.modified()?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut total = 0_u64;
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        total = total
            .checked_add(read as u64)
            .ok_or_else(|| Error::new("PROCESS_IDENTITY", "process image size overflow"))?;
        if total > MAX_PROCESS_IMAGE_BYTES {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "process executable exceeds the supported hash boundary",
            ));
        }
        hasher.update(&buffer[..read]);
    }
    let after = file.metadata()?;
    if total != before.len() || after.len() != before.len() || after.modified()? != modified_before
    {
        return Err(Error::new(
            "PROCESS_IDENTITY",
            "process executable changed during bounded hashing",
        ));
    }
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

#[cfg(windows)]
mod os {
    use super::*;
    use std::{mem::size_of, ptr};
    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND, ERROR_INVALID_PARAMETER,
            ERROR_MORE_DATA, FILETIME, GetLastError, HANDLE, WAIT_FAILED, WAIT_OBJECT_0,
            WAIT_TIMEOUT,
        },
        System::{
            JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOBOBJECT_BASIC_ACCOUNTING_INFORMATION,
                JOBOBJECT_BASIC_PROCESS_ID_LIST, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JobObjectBasicAccountingInformation, JobObjectBasicProcessIdList,
                JobObjectExtendedLimitInformation, OpenJobObjectW, QueryInformationJobObject,
                SetInformationJobObject,
            },
            Threading::{
                GetCurrentProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
                PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, QueryFullProcessImageNameW,
                TerminateProcess, WaitForSingleObject,
            },
        },
    };
    // Win32 documented job-specific access mask (not exposed by the enabled bindings).
    // https://learn.microsoft.com/windows/win32/procthread/job-object-security-and-access-rights
    const JOB_QUERY_ACCESS: u32 = 0x0004;
    // TerminateProcess is asynchronous. A short wait resolves the common race
    // where a repeated termination request reaches a process already exiting.
    const TERMINATION_WAIT_MS: u32 = 250;

    fn wait_process_signaled(handle: HANDLE, timeout_ms: u32) -> Result<bool> {
        // SAFETY: callers keep the opened process handle alive throughout the wait.
        match unsafe { WaitForSingleObject(handle, timeout_ms) } {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            WAIT_FAILED => Err(std::io::Error::last_os_error().into()),
            status => Err(Error::new(
                "PROCESS_WAIT",
                format!("unexpected process wait status {status:#x}"),
            )),
        }
    }

    fn process_creation_filetime(handle: HANDLE) -> Result<u64> {
        // SAFETY: the caller keeps a valid process handle open for this query.
        unsafe {
            let mut creation: FILETIME = std::mem::zeroed();
            let mut exit: FILETIME = std::mem::zeroed();
            let mut kernel: FILETIME = std::mem::zeroed();
            let mut user: FILETIME = std::mem::zeroed();
            if GetProcessTimes(handle, &mut creation, &mut exit, &mut kernel, &mut user) == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(((creation.dwHighDateTime as u64) << 32) | creation.dwLowDateTime as u64)
        }
    }

    fn validate_process_birth(handle: HANDLE, expected: u64) -> Result<()> {
        if wait_process_signaled(handle, 0)? || process_creation_filetime(handle)? != expected {
            return Err(Error::new(
                "PROCESS_GONE",
                "process exited or its pinned birth identity changed",
            ));
        }
        Ok(())
    }

    fn process_image_path(handle: HANDLE) -> Result<std::path::PathBuf> {
        use std::{ffi::OsString, os::windows::ffi::OsStringExt};
        let mut buffer = vec![0_u16; 32 * 1024];
        let mut size = u32::try_from(buffer.len())
            .map_err(|_| Error::new("PROCESS_IDENTITY", "image path buffer overflow"))?;
        // SAFETY: `buffer` is writable for `size` UTF-16 code units and the
        // process handle is held by the caller for the complete query.
        if unsafe { QueryFullProcessImageNameW(handle, 0, buffer.as_mut_ptr(), &mut size) } == 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let size = usize::try_from(size)
            .map_err(|_| Error::new("PROCESS_IDENTITY", "invalid image path length"))?;
        if size == 0 || size > buffer.len() {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "process image path length is outside the supported boundary",
            ));
        }
        Ok(std::path::PathBuf::from(OsString::from_wide(
            &buffer[..size],
        )))
    }

    pub fn process_image_identity(pid: u32) -> Result<Value> {
        use std::fs;
        // SAFETY: access is limited to querying process information and waiting;
        // the handle pins the process object against PID reuse through hashing.
        unsafe {
            let process = OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            );
            if process.is_null() {
                return Err(std::io::Error::last_os_error().into());
            }
            let result = (|| -> Result<Value> {
                if wait_process_signaled(process, 0)? {
                    return Err(Error::new("PROCESS_GONE", "process is no longer live"));
                }
                let birth = process_creation_filetime(process)?;
                let reported_path = process_image_path(process)?;
                let canonical_path = fs::canonicalize(&reported_path)?;
                let mut image = File::open(&canonical_path)?;
                let image_sha256 = hash_process_image(&mut image)?;
                validate_process_birth(process, birth)?;
                let path_after = fs::canonicalize(process_image_path(process)?)?;
                if !canonical_path
                    .to_string_lossy()
                    .eq_ignore_ascii_case(&path_after.to_string_lossy())
                {
                    return Err(Error::new(
                        "PROCESS_IDENTITY",
                        "process executable path changed during bounded hashing",
                    ));
                }
                let image_path = image_path_text(&canonical_path)?;
                Ok(json!({
                    "pid":pid,
                    "creation_filetime":birth.to_string(),
                    "image_path":image_path,
                    "image_sha256":image_sha256
                }))
            })();
            CloseHandle(process);
            result
        }
    }

    /// Return a pinned live process birth identity, or `None` only when the
    /// process is proven exited/absent. Access and query failures stay errors.
    pub fn process_birth_identity(pid: u32) -> Result<Option<Value>> {
        if pid == 0 {
            return Err(Error::invalid("invalid process PID"));
        }
        // SAFETY: the handle is opened only for query/synchronize rights and is
        // kept alive through both the exit check and immutable creation-time read.
        unsafe {
            let process = OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            );
            if process.is_null() {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                    return Ok(None);
                }
                return Err(error.into());
            }
            let result = (|| -> Result<Option<Value>> {
                if wait_process_signaled(process, 0)? {
                    return Ok(None);
                }
                let birth = process_creation_filetime(process)?;
                if wait_process_signaled(process, 0)? {
                    return Ok(None);
                }
                Ok(Some(json!({
                    "platform":"windows",
                    "pid":pid,
                    "creation_filetime":birth.to_string()
                })))
            })();
            CloseHandle(process);
            result
        }
    }

    pub struct Group {
        job: HANDLE,
        pub identity: Value,
    }
    impl Group {
        pub fn enter(token: &str) -> Result<Self> {
            Self::enter_owned(token, false)
        }
        /// Scripts share the transient Check Job lifecycle and its named-token
        /// recovery path; the purpose records the actual invocation owner.
        pub fn enter_script(token: &str) -> Result<Self> {
            let mut group = Self::enter(token)?;
            group.identity["purpose"] = json!("script");
            Ok(group)
        }
        pub fn enter_module(token: &str) -> Result<Self> {
            Self::enter_owned(token, true)
        }
        fn enter_owned(token: &str, module: bool) -> Result<Self> {
            // SAFETY: structures are initialized and handles are local to this worker.
            unsafe {
                let purpose = if module { "Module" } else { "Check" };
                let name = format!("Global\\EliotSwarm{purpose}-{token}");
                let wide: Vec<u16> = name.encode_utf16().chain(Some(0)).collect();
                let job = CreateJobObjectW(ptr::null(), wide.as_ptr());
                if job.is_null() {
                    return Err(std::io::Error::last_os_error().into());
                }
                if GetLastError() == ERROR_ALREADY_EXISTS {
                    CloseHandle(job);
                    return Err(Error::new(
                        "CHECK_OWNER_EXISTS",
                        "refusing to adopt an existing Job",
                    ));
                }
                let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
                info.BasicLimitInformation.LimitFlags = if module {
                    0
                } else {
                    JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                };
                if SetInformationJobObject(
                    job,
                    JobObjectExtendedLimitInformation,
                    (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                ) == 0
                    || AssignProcessToJobObject(job, GetCurrentProcess()) == 0
                {
                    let err = std::io::Error::last_os_error();
                    CloseHandle(job);
                    return Err(err.into());
                }
                let mut creation: FILETIME = std::mem::zeroed();
                let mut exit: FILETIME = std::mem::zeroed();
                let mut kernel: FILETIME = std::mem::zeroed();
                let mut user: FILETIME = std::mem::zeroed();
                if GetProcessTimes(
                    GetCurrentProcess(),
                    &mut creation,
                    &mut exit,
                    &mut kernel,
                    &mut user,
                ) == 0
                {
                    // No command has started. Clear kill-on-close before returning.
                    info.BasicLimitInformation.LimitFlags = 0;
                    SetInformationJobObject(
                        job,
                        JobObjectExtendedLimitInformation,
                        (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                        size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                    );
                    CloseHandle(job);
                    return Err(std::io::Error::last_os_error().into());
                }
                Ok(Self {
                    job,
                    identity: json!({"pid":std::process::id(),"creation_filetime":((creation.dwHighDateTime as u64)<<32)|creation.dwLowDateTime as u64,"scope":"windows_job","purpose":if module {"module"} else {"check"},"job_name":name,"disposition_source":"job_accounting"}),
                })
            }
        }
        pub fn children_empty(&self) -> Result<bool> {
            // Job accounting can retain exited PIDs briefly, so inspect the
            // exact Job's current PID inventory and pin each candidate before
            // deciding whether a live descendant remains.
            for pid in members(self.job)? {
                if pid == std::process::id() {
                    continue;
                }
                // SAFETY: the process handle pins identity while its signal state
                // and membership in this exact owned Job are checked.
                unsafe {
                    let handle = OpenProcess(
                        PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                        0,
                        pid,
                    );
                    if handle.is_null() {
                        let error = std::io::Error::last_os_error();
                        if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                            continue;
                        }
                        return Err(error.into());
                    }
                    let signaled = match wait_process_signaled(handle, 0) {
                        Ok(signaled) => signaled,
                        Err(error) => {
                            CloseHandle(handle);
                            return Err(error);
                        }
                    };
                    if signaled {
                        CloseHandle(handle);
                        continue;
                    }
                    let mut owned = 0;
                    if IsProcessInJob(handle, self.job, &mut owned) == 0 {
                        let error = std::io::Error::last_os_error();
                        CloseHandle(handle);
                        return Err(error.into());
                    }
                    CloseHandle(handle);
                    if owned != 0 {
                        return Ok(false);
                    }
                }
            }
            Ok(true)
        }
        /// Terminate only current members of this worker's Job, excluding the worker.
        /// Each process handle is checked against this exact Job, avoiding PID reuse.
        pub fn cancel_children(&self) -> Result<u64> {
            let mut sent = 0;
            for pid in members(self.job)? {
                if pid == std::process::id() {
                    continue;
                }
                // SAFETY: the owned handle pins identity through membership and termination.
                unsafe {
                    let handle = OpenProcess(
                        PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | PROCESS_SYNCHRONIZE,
                        0,
                        pid,
                    );
                    if handle.is_null() {
                        let error = std::io::Error::last_os_error();
                        if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                            continue;
                        }
                        return Err(error.into());
                    }
                    let result = (|| -> Result<()> {
                        if wait_process_signaled(handle, 0)? {
                            return Ok(());
                        }
                        let mut owned = 0;
                        if IsProcessInJob(handle, self.job, &mut owned) == 0 {
                            return Err(std::io::Error::last_os_error().into());
                        }
                        if owned != 0 {
                            if TerminateProcess(handle, 1) == 0 {
                                let error = std::io::Error::last_os_error();
                                if !wait_process_signaled(handle, TERMINATION_WAIT_MS)? {
                                    return Err(error.into());
                                }
                            } else {
                                sent += 1;
                                // A successful request is not proof of exit, but
                                // waiting here reduces repeated termination races.
                                let _ = wait_process_signaled(handle, TERMINATION_WAIT_MS)?;
                            }
                        }
                        Ok(())
                    })();
                    CloseHandle(handle);
                    result?;
                }
            }
            Ok(sent) // Termination requests are not resource-release evidence.
        }
        pub fn disarm(&self) -> Result<()> {
            if !self.children_empty()? {
                return Err(Error::new(
                    "CHECK_DESCENDANTS_ACTIVE",
                    "check job is not empty",
                ));
            }
            unsafe {
                let info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
                if SetInformationJobObject(
                    self.job,
                    JobObjectExtendedLimitInformation,
                    (&info as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                    size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                ) == 0
                {
                    return Err(std::io::Error::last_os_error().into());
                }
            }
            Ok(())
        }
    }
    fn members(job: HANDLE) -> Result<Vec<u32>> {
        let mut capacity = 64usize;
        loop {
            let bytes = 8usize
                .checked_add(
                    capacity
                        .checked_mul(size_of::<usize>())
                        .ok_or_else(|| Error::invalid("Job inventory overflow"))?,
                )
                .ok_or_else(|| Error::invalid("Job inventory overflow"))?;
            let size =
                u32::try_from(bytes).map_err(|_| Error::invalid("Job inventory too large"))?;
            let mut buffer = vec![0usize; bytes.div_ceil(size_of::<usize>())];
            // SAFETY: pointer-aligned storage includes the header and full variable array.
            unsafe {
                let ok = QueryInformationJobObject(
                    job,
                    JobObjectBasicProcessIdList,
                    buffer.as_mut_ptr().cast(),
                    size,
                    ptr::null_mut(),
                );
                let error = if ok == 0 {
                    Some(std::io::Error::last_os_error())
                } else {
                    None
                };
                let list = &*buffer.as_ptr().cast::<JOBOBJECT_BASIC_PROCESS_ID_LIST>();
                if let Some(error) = error.as_ref()
                    && error.raw_os_error() != Some(ERROR_MORE_DATA as i32)
                {
                    return Err(Error::new("JOB_INVENTORY", error.to_string()));
                }
                if error.is_some() || list.NumberOfAssignedProcesses > list.NumberOfProcessIdsInList
                {
                    capacity = capacity
                        .checked_mul(2)
                        .ok_or_else(|| Error::invalid("Job inventory overflow"))?
                        .max(list.NumberOfAssignedProcesses as usize);
                    continue;
                }
                let count = list.NumberOfProcessIdsInList as usize;
                if count > capacity {
                    return Err(Error::invalid("invalid Job process count"));
                }
                let start = buffer.as_ptr().cast::<u8>().add(8).cast::<usize>();
                return std::slice::from_raw_parts(start, count)
                    .iter()
                    .map(|pid| {
                        u32::try_from(*pid).map_err(|_| Error::invalid("invalid Job process ID"))
                    })
                    .collect();
            }
        }
    }
    pub fn departed_empty(identity: &Value, token: &str) -> Result<bool> {
        let purpose = if identity["purpose"] == "module" {
            "Module"
        } else {
            "Check"
        };
        let expected = format!("Global\\EliotSwarm{purpose}-{token}");
        if identity["scope"] != "windows_job" || identity["job_name"] != expected {
            return Err(Error::new(
                "CHECK_RECOVERY_UNSUPPORTED",
                "a named check Job is required; legacy unnamed Jobs are not guessed",
            ));
        }
        let pid = u32::try_from(model::positive(identity, "pid")?)
            .map_err(|_| Error::invalid("invalid worker PID"))?;
        let birth = identity["creation_filetime"]
            .as_u64()
            .ok_or_else(|| Error::invalid("worker creation time missing"))?;
        // SAFETY: opened process/Job handles are query-only and always closed.
        unsafe {
            let process = OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            );
            if !process.is_null() {
                let result = (|| -> Result<bool> {
                    let mut creation: FILETIME = std::mem::zeroed();
                    let mut exit: FILETIME = std::mem::zeroed();
                    let mut kernel: FILETIME = std::mem::zeroed();
                    let mut user: FILETIME = std::mem::zeroed();
                    if GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user)
                        == 0
                    {
                        return Err(std::io::Error::last_os_error().into());
                    }
                    let actual =
                        ((creation.dwHighDateTime as u64) << 32) | creation.dwLowDateTime as u64;
                    Ok(actual == birth && WaitForSingleObject(process, 0) != WAIT_OBJECT_0)
                })();
                CloseHandle(process);
                if result? {
                    return Ok(false);
                }
            } else {
                let error = std::io::Error::last_os_error();
                if error.raw_os_error() != Some(ERROR_INVALID_PARAMETER as i32) {
                    return Err(error.into());
                }
            }
            let name: Vec<u16> = expected.encode_utf16().chain(Some(0)).collect();
            let job = OpenJobObjectW(JOB_QUERY_ACCESS, 0, name.as_ptr());
            if job.is_null() {
                let error = std::io::Error::last_os_error();
                return if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) {
                    Ok(true)
                } else {
                    Err(error.into())
                };
            }
            let mut count: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = std::mem::zeroed();
            let ok = QueryInformationJobObject(
                job,
                JobObjectBasicAccountingInformation,
                (&mut count as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                ptr::null_mut(),
            );
            let error = if ok == 0 {
                Some(std::io::Error::last_os_error())
            } else {
                None
            };
            CloseHandle(job);
            if let Some(error) = error {
                return Err(error.into());
            }
            Ok(count.ActiveProcesses == 0)
        }
    }
    pub fn spawned_identity(pid: u32) -> Result<Value> {
        // Host-side evidence for a worker it just created, captured before the
        // worker can publish its own identity. A bare PID is never enough: the
        // creation time pins this exact process instance against PID reuse.
        unsafe {
            let process = OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            );
            if process.is_null() {
                let error = std::io::Error::last_os_error();
                return if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                    Err(Error::new(
                        "PROCESS_GONE",
                        "spawned process exited before its launch identity was captured",
                    ))
                } else {
                    Err(error.into())
                };
            }
            let result = (|| -> Result<Value> {
                let mut creation: FILETIME = std::mem::zeroed();
                let mut exit: FILETIME = std::mem::zeroed();
                let mut kernel: FILETIME = std::mem::zeroed();
                let mut user: FILETIME = std::mem::zeroed();
                if GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) == 0 {
                    return Err(std::io::Error::last_os_error().into());
                }
                if WaitForSingleObject(process, 0) == WAIT_OBJECT_0 {
                    return Err(Error::new(
                        "PROCESS_GONE",
                        "spawned process exited before its launch identity was captured",
                    ));
                }
                Ok(
                    json!({"pid":pid,"creation_filetime":((creation.dwHighDateTime as u64)<<32)|creation.dwLowDateTime as u64,"scope":"launcher_spawned_process","purpose":"check"}),
                )
            })();
            CloseHandle(process);
            result
        }
    }
    pub fn spawned_departed(identity: &Value, token: &str) -> Result<bool> {
        if identity["scope"] != "launcher_spawned_process" || identity["purpose"] != "check" {
            return Err(Error::invalid(
                "not a launcher-spawned check process record",
            ));
        }
        // Before publishing worker.json the worker spawns nothing: its only
        // possible descendants enter the token-named Job when it creates its
        // group, and the tool spawns only after identity publication. Reuse the
        // exact named-Job accounting with the launch record's pid/birth.
        let shim = json!({"pid":identity["pid"],"creation_filetime":identity["creation_filetime"],"scope":"windows_job","purpose":"check","job_name":format!("Global\\EliotSwarmCheck-{token}"),"disposition_source":"job_accounting"});
        departed_empty(&shim, token)
    }
    impl Drop for Group {
        fn drop(&mut self) {
            unsafe {
                CloseHandle(self.job);
            }
        }
    }
}
#[cfg(target_os = "linux")]
mod os {
    use super::*;
    use std::fs;
    pub struct Group {
        pgid: i32,
        pub identity: Value,
    }
    fn stat(pid: u32) -> Result<(char, i32, String)> {
        let s = fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::new("PROCESS_GONE", "process exited during inventory")
            } else {
                e.into()
            }
        })?;
        let tail = s
            .rsplit_once(')')
            .ok_or_else(|| Error::new("PROCESS_IDENTITY", "invalid proc stat"))?
            .1;
        let fields: Vec<_> = tail.split_whitespace().collect();
        if fields.len() < 20 {
            return Err(Error::new("PROCESS_IDENTITY", "incomplete proc stat"));
        }
        Ok((
            fields[0].chars().next().unwrap_or('?'),
            fields[2]
                .parse()
                .map_err(|_| Error::new("PROCESS_IDENTITY", "invalid process group"))?,
            fields[19].to_string(),
        ))
    }
    fn live(pid: u32, state: char) -> Result<bool> {
        if !matches!(state, 'Z' | 'X') {
            return Ok(true);
        }
        // The main thread can be a zombie while another thread still writes. A
        // process pidfd becomes readable only after the last thread terminates.
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0u32) };
        if raw < 0 {
            let e = std::io::Error::last_os_error();
            return if e.raw_os_error() == Some(libc::ESRCH) {
                Ok(false)
            } else {
                Err(e.into())
            };
        }
        let fd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
        let mut poll = libc::pollfd {
            fd: fd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialized pollfd, no retained pointer, zero-duration poll.
        if unsafe { libc::poll(&mut poll, 1, 0) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if poll.revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "cannot observe process pidfd",
            ));
        }
        Ok(poll.revents & (libc::POLLIN | libc::POLLHUP) == 0)
    }

    pub fn process_image_identity(pid: u32) -> Result<Value> {
        use std::os::unix::fs::MetadataExt;
        let proc_exe = std::path::PathBuf::from(format!("/proc/{pid}/exe"));
        let boot_before = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        let boot_before = boot_before.trim();
        if boot_before.is_empty() {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "kernel boot identity is unavailable",
            ));
        }
        let (state_before, _, start_ticks) = stat(pid)?;
        if !live(pid, state_before)? {
            return Err(Error::new("PROCESS_GONE", "process is no longer live"));
        }
        // /proc/<pid>/exe is a kernel-provided reference to the executable
        // backing this live process; do not resolve a configured or caller path.
        let reported_path = fs::read_link(&proc_exe)?;
        if !reported_path.is_absolute() {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "kernel executable path is not absolute",
            ));
        }
        let mut image = File::open(&proc_exe).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::new("PROCESS_GONE", "process executable disappeared")
            } else {
                error.into()
            }
        })?;
        let image_before = image.metadata()?;
        let (state_open, _, start_open) = stat(pid)?;
        let boot_open = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        if start_open != start_ticks || boot_open.trim() != boot_before || !live(pid, state_open)? {
            return Err(Error::new(
                "PROCESS_GONE",
                "process birth changed while opening its executable",
            ));
        }
        let image_sha256 = hash_process_image(&mut image)?;
        let image_after = image.metadata()?;
        let current_image = File::open(&proc_exe).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::new("PROCESS_GONE", "process executable disappeared")
            } else {
                error.into()
            }
        })?;
        let current_image_metadata = current_image.metadata()?;
        let current_path = fs::read_link(&proc_exe)?;
        let (state_after, _, start_after) = stat(pid)?;
        let boot_after = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        if start_after != start_ticks
            || boot_after.trim() != boot_before
            || !live(pid, state_after)?
            || current_path != reported_path
            || image_before.dev() != image_after.dev()
            || image_before.ino() != image_after.ino()
            || image_before.dev() != current_image_metadata.dev()
            || image_before.ino() != current_image_metadata.ino()
        {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "process birth or executable image changed during bounded hashing",
            ));
        }
        Ok(json!({
            "pid":pid,
            "start_ticks":start_ticks,
            "boot_id":boot_before,
            "image_path":image_path_text(&reported_path)?,
            "image_sha256":image_sha256
        }))
    }

    /// Use a pidfd to pin this exact process incarnation while checking its
    /// birth tuple. A changed/reused PID is returned as its new tuple so callers
    /// can compare it with retained birth authority; uncertain reads fail closed.
    pub fn process_birth_identity(pid: u32) -> Result<Option<Value>> {
        use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
        if pid == 0 {
            return Err(Error::invalid("invalid process PID"));
        }
        // SAFETY: pidfd_open returns a new owned descriptor on success.
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0u32) };
        if raw < 0 {
            let error = std::io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(None)
            } else {
                Err(error.into())
            };
        }
        // SAFETY: `raw` is the descriptor just returned by pidfd_open.
        let pidfd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
        let exited = || -> Result<bool> {
            let mut poll = libc::pollfd {
                fd: pidfd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            // SAFETY: one initialized pollfd and a zero-duration poll.
            if unsafe { libc::poll(&mut poll, 1, 0) } < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if poll.revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
                return Err(Error::new(
                    "PROCESS_IDENTITY",
                    "cannot observe pinned process birth",
                ));
            }
            Ok(poll.revents & (libc::POLLIN | libc::POLLHUP) != 0)
        };
        if exited()? {
            return Ok(None);
        }
        let boot_before = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        let boot_before = boot_before.trim().to_owned();
        if boot_before.is_empty() {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "kernel boot identity is unavailable",
            ));
        }
        let (_, _, start_before) = match stat(pid) {
            Ok(value) => value,
            Err(error) if error.code == "PROCESS_GONE" => return Ok(None),
            Err(error) => return Err(error),
        };
        if exited()? {
            return Ok(None);
        }
        let boot_after = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        let (_, _, start_after) = match stat(pid) {
            Ok(value) => value,
            Err(error) if error.code == "PROCESS_GONE" => return Ok(None),
            Err(error) => return Err(error),
        };
        if exited()? {
            return Ok(None);
        }
        if boot_after.trim() != boot_before || start_after != start_before {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "process birth changed during pinned identity read",
            ));
        }
        Ok(Some(json!({
            "platform":"linux",
            "pid":pid,
            "boot_id":boot_before,
            "start_ticks":start_before
        })))
    }
    impl Group {
        pub fn enter(token: &str) -> Result<Self> {
            Self::enter_owned(token, false)
        }
        pub fn enter_script(token: &str) -> Result<Self> {
            let mut group = Self::enter(token)?;
            group.identity["purpose"] = json!("script");
            Ok(group)
        }
        pub fn enter_module(token: &str) -> Result<Self> {
            Self::enter_owned(token, true)
        }
        fn enter_owned(_token: &str, module: bool) -> Result<Self> {
            // SAFETY: make only this worker a group leader before launching tools.
            if unsafe { libc::setpgid(0, 0) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let pid = std::process::id();
            let (_, pgid, start) = stat(pid)?;
            let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
            Ok(Self {
                pgid,
                identity: json!({"pid":pid,"pgid":pgid,"start_ticks":start,"boot_id":boot.trim(),"scope":"linux_process_group","purpose":if module {"module"} else {"check"},"disposition_source":"proc_group_members"}),
            })
        }
        pub fn children_empty(&self) -> Result<bool> {
            for e in fs::read_dir("/proc")? {
                let e = e?;
                let Some(pid) = e.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
                    continue;
                };
                if pid == std::process::id() {
                    continue;
                }
                match stat(pid) {
                    Ok((state, group, _)) if group == self.pgid && live(pid, state)? => {
                        return Ok(false);
                    }
                    Ok(_) => {}
                    Err(e) if e.code == "PROCESS_GONE" => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(true)
        }
        pub fn cancel_children(&self) -> Result<u64> {
            use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
            let mut sent = 0;
            for e in fs::read_dir("/proc")? {
                let e = e?;
                let Some(pid) = e.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
                    continue;
                };
                if pid == std::process::id() {
                    continue;
                }
                let (state, pgid, start) = match stat(pid) {
                    Ok(s) => s,
                    Err(e) if e.code == "PROCESS_GONE" => continue,
                    Err(e) => return Err(e),
                };
                if pgid != self.pgid || !live(pid, state)? {
                    continue;
                }
                // SAFETY: pidfd pins the selected process; recheck group/birth after open.
                let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0u32) };
                if raw < 0 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() == Some(libc::ESRCH) {
                        continue;
                    }
                    return Err(error.into());
                }
                let fd = unsafe { OwnedFd::from_raw_fd(raw as i32) };
                match stat(pid) {
                    Ok((s, g, birth)) if g == self.pgid && birth == start && live(pid, s)? => {}
                    Ok(_) => continue,
                    Err(e) if e.code == "PROCESS_GONE" => continue,
                    Err(e) => return Err(e),
                }
                let rc = unsafe {
                    libc::syscall(
                        libc::SYS_pidfd_send_signal,
                        fd.as_raw_fd(),
                        libc::SIGKILL,
                        std::ptr::null::<libc::siginfo_t>(),
                        0u32,
                    )
                };
                if rc != 0 {
                    let error = std::io::Error::last_os_error();
                    if error.raw_os_error() != Some(libc::ESRCH) {
                        return Err(error.into());
                    }
                } else {
                    sent += 1;
                }
            }
            Ok(sent)
        }
        pub fn disarm(&self) -> Result<()> {
            if self.children_empty()? {
                Ok(())
            } else {
                Err(Error::new(
                    "CHECK_DESCENDANTS_ACTIVE",
                    "check process group is not empty",
                ))
            }
        }
    }
    pub fn departed_empty(identity: &Value, _token: &str) -> Result<bool> {
        if identity["scope"] != "linux_process_group" {
            return Err(Error::invalid("not a Linux check process group"));
        }
        let pid = u32::try_from(model::positive(identity, "pid")?)
            .map_err(|_| Error::invalid("invalid worker PID"))?;
        let group = i32::try_from(model::positive(identity, "pgid")?)
            .map_err(|_| Error::invalid("invalid check PGID"))?;
        if i64::from(pid) != i64::from(group) {
            return Err(Error::invalid("check worker was not its group leader"));
        }
        let boot = model::text(identity, "boot_id")?;
        let birth = model::text(identity, "start_ticks")?;
        if fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim() != boot {
            return Ok(true);
        }
        match stat(pid) {
            Ok((s, g, start)) if g == group && start == birth && live(pid, s)? => {
                return Ok(false);
            }
            Ok(_) => {}
            Err(e) if e.code == "PROCESS_GONE" => {}
            Err(e) => return Err(e),
        }
        // A reused numeric group may conservatively retain the resource; never signal it.
        for e in fs::read_dir("/proc")? {
            let e = e?;
            let Some(pid) = e.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            match stat(pid) {
                Ok((s, g, _)) if g == group && live(pid, s)? => return Ok(false),
                Ok(_) => {}
                Err(e) if e.code == "PROCESS_GONE" => {}
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
    pub fn spawned_identity(pid: u32) -> Result<Value> {
        // Host-side evidence for a worker it just created, captured before the
        // worker can publish its own identity. The boot ID and start ticks pin
        // this exact process instance against PID reuse.
        let (state, pgid, start) = stat(pid)?;
        if !live(pid, state)? {
            return Err(Error::new(
                "PROCESS_GONE",
                "spawned process exited before its launch identity was captured",
            ));
        }
        let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        Ok(
            json!({"pid":pid,"spawned_pgid":pgid,"start_ticks":start,"boot_id":boot.trim(),"scope":"launcher_spawned_process","purpose":"check"}),
        )
    }
    pub fn spawned_departed(identity: &Value, _token: &str) -> Result<bool> {
        if identity["scope"] != "launcher_spawned_process" || identity["purpose"] != "check" {
            return Err(Error::invalid(
                "not a launcher-spawned check process record",
            ));
        }
        let pid = u32::try_from(model::positive(identity, "pid")?)
            .map_err(|_| Error::invalid("invalid spawned PID"))?;
        if let Some(boot) = identity["boot_id"].as_str()
            && fs::read_to_string("/proc/sys/kernel/random/boot_id")?.trim() != boot
        {
            return Ok(true);
        }
        let birth = identity["start_ticks"].as_str();
        match stat(pid) {
            Ok((state, _, start)) => {
                if birth == Some(start.as_str()) {
                    if live(pid, state)? {
                        return Ok(false);
                    }
                } else if birth.is_some() {
                    // The PID was recycled: the recorded process is gone. Fall
                    // through to the group scan before calling it departed.
                } else if live(pid, state)? {
                    // Without a recorded birth, a live process at this PID
                    // cannot be excluded as the spawned worker.
                    return Err(Error::new(
                        "CHECK_RECOVERY_UNSUPPORTED",
                        "spawned process birth was not recorded; departure is unprovable while its PID is occupied",
                    ));
                }
            }
            Err(e) if e.code == "PROCESS_GONE" => {}
            Err(e) => return Err(e),
        }
        // Before publishing worker.json the worker spawns nothing, and it can
        // only create descendants after entering its own group (pgid == its
        // PID). Any live member of that prospective group retains the launch.
        let group = i32::try_from(pid).map_err(|_| Error::invalid("invalid spawned PID"))?;
        for e in fs::read_dir("/proc")? {
            let e = e?;
            let Some(member) = e.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            match stat(member) {
                Ok((s, g, _)) if g == group && live(member, s)? => return Ok(false),
                Ok(_) => {}
                Err(e) if e.code == "PROCESS_GONE" => {}
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    }
}
#[cfg(not(any(windows, target_os = "linux")))]
mod os {
    use super::*;
    pub struct Group {
        pub identity: Value,
    }
    impl Group {
        pub fn enter_script(token: &str) -> Result<Self> {
            Self::enter(token)
        }
        pub fn enter_module(token: &str) -> Result<Self> {
            Self::enter(token)
        }
        pub fn enter(_token: &str) -> Result<Self> {
            Err(Error::new(
                "CHECK_PLATFORM_UNSUPPORTED",
                "check process disposition is implemented for Windows and Linux",
            ))
        }
        pub fn children_empty(&self) -> Result<bool> {
            Ok(false)
        }
        pub fn cancel_children(&self) -> Result<u64> {
            Err(Error::new(
                "CHECK_PLATFORM_UNSUPPORTED",
                "no safe process cancellation",
            ))
        }
        pub fn disarm(&self) -> Result<()> {
            Err(Error::new(
                "CHECK_PLATFORM_UNSUPPORTED",
                "no process-group evidence",
            ))
        }
    }
    pub fn departed_empty(_identity: &Value, _token: &str) -> Result<bool> {
        Err(Error::new(
            "CHECK_PLATFORM_UNSUPPORTED",
            "no process disposition",
        ))
    }
    pub fn spawned_identity(_pid: u32) -> Result<Value> {
        Err(Error::new(
            "CHECK_PLATFORM_UNSUPPORTED",
            "no spawned process identity",
        ))
    }
    pub fn spawned_departed(_identity: &Value, _token: &str) -> Result<bool> {
        Err(Error::new(
            "CHECK_PLATFORM_UNSUPPORTED",
            "no spawned process disposition",
        ))
    }
    pub fn process_image_identity(_pid: u32) -> Result<Value> {
        Err(Error::new(
            "PROCESS_PLATFORM_UNSUPPORTED",
            "process image identity is implemented for Windows and Linux",
        ))
    }
    pub fn process_birth_identity(_pid: u32) -> Result<Option<Value>> {
        Err(Error::new(
            "PROCESS_PLATFORM_UNSUPPORTED",
            "process birth identity is implemented for Windows and Linux",
        ))
    }
}
pub use os::{
    Group, departed_empty, process_birth_identity, process_image_identity, spawned_departed,
    spawned_identity,
};
