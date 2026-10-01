//! A transient check worker owns its process group before starting any tool.
//! Native agent processes are never added to this group.
use crate::{
    error::{Error, Result},
    model,
};
use serde_json::{Value, json};

#[cfg(windows)]
mod os {
    use super::*;
    use std::{mem::size_of, ptr};
    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, ERROR_ALREADY_EXISTS, ERROR_FILE_NOT_FOUND, ERROR_INVALID_PARAMETER,
            ERROR_MORE_DATA, FILETIME, GetLastError, HANDLE, WAIT_OBJECT_0,
        },
        System::{
            JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, IsProcessInJob,
                JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE, JOB_OBJECT_QUERY,
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_BASIC_PROCESS_ID_LIST,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectBasicAccountingInformation,
                JobObjectBasicProcessIdList, JobObjectExtendedLimitInformation, OpenJobObjectW,
                QueryInformationJobObject, SetInformationJobObject,
            },
            Threading::{
                GetCurrentProcess, GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
                PROCESS_SYNCHRONIZE, PROCESS_TERMINATE, TerminateProcess, WaitForSingleObject,
            },
        },
    };
    pub struct Group {
        job: HANDLE,
        pub identity: Value,
    }
    impl Group {
        pub fn enter(token: &str) -> Result<Self> {
            // SAFETY: structures are initialized and handles are local to this worker.
            unsafe {
                let name = format!("Global\\EliotSwarmCheck-{token}");
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
                info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
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
                    identity: json!({"pid":std::process::id(),"creation_filetime":((creation.dwHighDateTime as u64)<<32)|creation.dwLowDateTime as u64,"scope":"windows_job","job_name":name,"disposition_source":"job_accounting"}),
                })
            }
        }
        pub fn children_empty(&self) -> Result<bool> {
            // SAFETY: the Job is held throughout the accounting query. No notification
            // or parent PID is substituted for the actual active process count.
            unsafe {
                let mut count: JOBOBJECT_BASIC_ACCOUNTING_INFORMATION = std::mem::zeroed();
                if QueryInformationJobObject(
                    self.job,
                    JobObjectBasicAccountingInformation,
                    (&mut count as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    ptr::null_mut(),
                ) == 0
                {
                    return Err(std::io::Error::last_os_error().into());
                }
                Ok(count.ActiveProcesses == 1) // This worker is the sole remaining member.
            }
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
                        if WaitForSingleObject(handle, 0) == WAIT_OBJECT_0 {
                            return Ok(());
                        }
                        let mut owned = 0;
                        if IsProcessInJob(handle, self.job, &mut owned) == 0 {
                            return Err(std::io::Error::last_os_error().into());
                        }
                        if owned != 0 {
                            if TerminateProcess(handle, 1) == 0 {
                                let error = std::io::Error::last_os_error();
                                if WaitForSingleObject(handle, 0) != WAIT_OBJECT_0 {
                                    return Err(error.into());
                                }
                            } else {
                                sent += 1;
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
        let expected = format!("Global\\EliotSwarmCheck-{token}");
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
            let job = OpenJobObjectW(JOB_OBJECT_QUERY, 0, name.as_ptr());
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
    impl Group {
        pub fn enter(_token: &str) -> Result<Self> {
            // SAFETY: make only this worker a group leader before launching tools.
            if unsafe { libc::setpgid(0, 0) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let pid = std::process::id();
            let (_, pgid, start) = stat(pid)?;
            let boot = fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
            Ok(Self {
                pgid,
                identity: json!({"pid":pid,"pgid":pgid,"start_ticks":start,"boot_id":boot.trim(),"scope":"linux_process_group","disposition_source":"proc_group_members"}),
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
}
#[cfg(not(any(windows, target_os = "linux")))]
mod os {
    use super::*;
    pub struct Group {
        pub identity: Value,
    }
    impl Group {
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
}
pub use os::{Group, departed_empty};
pub fn waiting_identity(group: &Group, token: &str) -> Value {
    json!({"token":token,"process":group.identity,"ready_at_ms":model::now_ms().unwrap_or(0),"control_version":2})
}
