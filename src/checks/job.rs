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
        Foundation::{CloseHandle, FILETIME, HANDLE},
        System::{
            JobObjects::{
                AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
                JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
                QueryInformationJobObject, SetInformationJobObject,
            },
            Threading::{GetCurrentProcess, GetProcessTimes},
        },
    };
    pub struct Group {
        job: HANDLE,
        pub identity: Value,
    }
    impl Group {
        pub fn enter() -> Result<Self> {
            // SAFETY: structures are initialized and handles are local to this worker.
            unsafe {
                let job = CreateJobObjectW(ptr::null(), ptr::null());
                if job.is_null() {
                    return Err(std::io::Error::last_os_error().into());
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
                    identity: json!({"pid":std::process::id(),"creation_filetime":((creation.dwHighDateTime as u64)<<32)|creation.dwLowDateTime as u64,"scope":"windows_job","disposition_source":"job_accounting"}),
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
    impl Group {
        pub fn enter() -> Result<Self> {
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
                    Ok((state, group, _)) if group == self.pgid && !matches!(state, 'Z' | 'X') => {
                        return Ok(false);
                    }
                    Ok(_) => {}
                    Err(e) if e.code == "PROCESS_GONE" => {}
                    Err(e) => return Err(e),
                }
            }
            Ok(true)
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
}
#[cfg(not(any(windows, target_os = "linux")))]
mod os {
    use super::*;
    pub struct Group {
        pub identity: Value,
    }
    impl Group {
        pub fn enter() -> Result<Self> {
            Err(Error::new(
                "CHECK_PLATFORM_UNSUPPORTED",
                "check process disposition is implemented for Windows and Linux",
            ))
        }
        pub fn children_empty(&self) -> Result<bool> {
            Ok(false)
        }
        pub fn disarm(&self) -> Result<()> {
            Err(Error::new(
                "CHECK_PLATFORM_UNSUPPORTED",
                "no process-group evidence",
            ))
        }
    }
}
pub use os::Group;
pub fn waiting_identity(group: &Group, token: &str) -> Value {
    json!({"token":token,"process":group.identity,"ready_at_ms":model::now_ms().unwrap_or(0)})
}
