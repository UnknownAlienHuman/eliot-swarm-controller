use serde_json::Value;
use swarm_contracts::error::{Error, Result};

#[derive(Debug)]
struct ModuleOwner {
    #[cfg(windows)]
    token: String,
    pid: u32,
    #[cfg(windows)]
    creation_filetime: u64,
    #[cfg(target_os = "linux")]
    pgid: i32,
    #[cfg(target_os = "linux")]
    start_ticks: String,
    #[cfg(target_os = "linux")]
    boot_id: String,
}

#[derive(Debug)]
struct ChildImage {
    pid: u32,
    #[cfg(windows)]
    creation_filetime: String,
    #[cfg(target_os = "linux")]
    start_ticks: String,
    #[cfg(target_os = "linux")]
    boot_id: String,
}

fn exact_keys(value: &Value, expected: &[&str], label: &str) -> Result<()> {
    let object = value
        .as_object()
        .ok_or_else(|| Error::invalid(format!("{label} must be an object")))?;
    if object.len() != expected.len() || expected.iter().any(|key| !object.contains_key(*key)) {
        return Err(Error::invalid(format!(
            "{label} does not match the supported identity shape"
        )));
    }
    Ok(())
}

fn string<'a>(value: &'a Value, field: &str) -> Result<&'a str> {
    value
        .get(field)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty() && text.len() <= 32 * 1024)
        .ok_or_else(|| Error::invalid(format!("{field} must be a bounded nonempty string")))
}

fn positive_u32(value: &Value, field: &str) -> Result<u32> {
    let number = value
        .get(field)
        .and_then(Value::as_u64)
        .filter(|number| *number > 0)
        .ok_or_else(|| Error::invalid(format!("{field} must be a positive integer")))?;
    u32::try_from(number).map_err(|_| Error::invalid(format!("{field} is outside the PID range")))
}

fn positive_decimal(value: &str, field: &str) -> Result<()> {
    let number = value
        .parse::<u64>()
        .ok()
        .filter(|number| *number > 0)
        .ok_or_else(|| Error::invalid(format!("{field} must be a positive decimal string")))?;
    if number.to_string() != value {
        return Err(Error::invalid(format!(
            "{field} must use canonical decimal spelling"
        )));
    }
    Ok(())
}

fn valid_uuid(value: &str) -> bool {
    value.len() == 36
        && [8, 13, 18, 23]
            .into_iter()
            .all(|index| value.as_bytes()[index] == b'-')
        && value
            .bytes()
            .enumerate()
            .all(|(index, byte)| [8, 13, 18, 23].contains(&index) || byte.is_ascii_hexdigit())
}

fn valid_image_fields(identity: &Value) -> Result<()> {
    let image_path = string(identity, "image_path")?;
    if image_path.chars().any(char::is_control) {
        return Err(Error::invalid("image_path contains unsupported characters"));
    }
    let hash = string(identity, "image_sha256")?;
    let hex = hash
        .strip_prefix("sha256:")
        .filter(|value| value.len() == 64)
        .ok_or_else(|| Error::invalid("image_sha256 has an unsupported shape"))?;
    if !hex
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::invalid("image_sha256 has an unsupported shape"));
    }
    Ok(())
}

fn parse_owner(record: &Value) -> Result<ModuleOwner> {
    exact_keys(record, &["version", "token", "process"], "owner record")?;
    if record.get("version").and_then(Value::as_u64) != Some(1) {
        return Err(Error::invalid("owner record version is unsupported"));
    }
    let token = string(record, "token")?;
    if !valid_uuid(token) {
        return Err(Error::invalid("owner token is not a UUID"));
    }
    let process = record
        .get("process")
        .filter(|process| process.is_object())
        .ok_or_else(|| Error::invalid("owner process identity is missing"))?;

    #[cfg(windows)]
    {
        exact_keys(
            process,
            &[
                "pid",
                "creation_filetime",
                "scope",
                "purpose",
                "job_name",
                "disposition_source",
            ],
            "Windows module process identity",
        )?;
        let pid = positive_u32(process, "pid")?;
        let creation_filetime = process
            .get("creation_filetime")
            .and_then(Value::as_u64)
            .filter(|value| *value > 0)
            .ok_or_else(|| Error::invalid("creation_filetime is missing"))?;
        let expected_job_name = format!(r"Global\EliotSwarmModule-{token}");
        if process.get("scope").and_then(Value::as_str) != Some("windows_job")
            || process.get("purpose").and_then(Value::as_str) != Some("module")
            || process.get("disposition_source").and_then(Value::as_str) != Some("job_accounting")
            || process.get("job_name").and_then(Value::as_str) != Some(expected_job_name.as_str())
        {
            return Err(Error::invalid(
                "owner identity is not the supported Windows module Job",
            ));
        }
        Ok(ModuleOwner {
            token: token.to_owned(),
            pid,
            creation_filetime,
        })
    }

    #[cfg(target_os = "linux")]
    {
        exact_keys(
            process,
            &[
                "pid",
                "pgid",
                "start_ticks",
                "boot_id",
                "scope",
                "purpose",
                "disposition_source",
            ],
            "Linux module process identity",
        )?;
        let pid = positive_u32(process, "pid")?;
        let pgid = process
            .get("pgid")
            .and_then(Value::as_i64)
            .filter(|value| *value > 0)
            .and_then(|value| i32::try_from(value).ok())
            .ok_or_else(|| Error::invalid("pgid is outside the supported range"))?;
        let start_ticks = string(process, "start_ticks")?.to_owned();
        positive_decimal(&start_ticks, "start_ticks")?;
        let boot_id = string(process, "boot_id")?.to_owned();
        if !valid_uuid(&boot_id) {
            return Err(Error::invalid("boot_id is not a UUID"));
        }
        if i64::from(pid) != i64::from(pgid)
            || process.get("scope").and_then(Value::as_str) != Some("linux_process_group")
            || process.get("purpose").and_then(Value::as_str) != Some("module")
            || process.get("disposition_source").and_then(Value::as_str)
                != Some("proc_group_members")
        {
            return Err(Error::invalid(
                "owner identity is not the supported Linux module process group",
            ));
        }
        Ok(ModuleOwner {
            pid,
            pgid,
            start_ticks,
            boot_id,
        })
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (process, token);
        Err(Error::new(
            "MODULE_MEMBERSHIP_UNSUPPORTED",
            "module process membership is supported only on Windows and Linux",
        ))
    }
}

fn parse_child_image(identity: &Value) -> Result<ChildImage> {
    #[cfg(windows)]
    {
        exact_keys(
            identity,
            &["pid", "creation_filetime", "image_path", "image_sha256"],
            "Windows child image identity",
        )?;
        let pid = positive_u32(identity, "pid")?;
        let creation_filetime = string(identity, "creation_filetime")?.to_owned();
        positive_decimal(&creation_filetime, "creation_filetime")?;
        valid_image_fields(identity)?;
        Ok(ChildImage {
            pid,
            creation_filetime,
        })
    }

    #[cfg(target_os = "linux")]
    {
        exact_keys(
            identity,
            &[
                "pid",
                "start_ticks",
                "boot_id",
                "image_path",
                "image_sha256",
            ],
            "Linux child image identity",
        )?;
        let pid = positive_u32(identity, "pid")?;
        let start_ticks = string(identity, "start_ticks")?.to_owned();
        positive_decimal(&start_ticks, "start_ticks")?;
        let boot_id = string(identity, "boot_id")?.to_owned();
        if !valid_uuid(&boot_id) {
            return Err(Error::invalid("child boot_id is not a UUID"));
        }
        valid_image_fields(identity)?;
        Ok(ChildImage {
            pid,
            start_ticks,
            boot_id,
        })
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = identity;
        Err(Error::new(
            "MODULE_MEMBERSHIP_UNSUPPORTED",
            "module process membership is supported only on Windows and Linux",
        ))
    }
}

fn current_child_image_matches(child: &ChildImage, expected: &Value) -> Result<bool> {
    let current = match crate::process_group::process_image_identity(child.pid) {
        Ok(identity) => identity,
        Err(error) if error.code == "PROCESS_GONE" => return Ok(false),
        Err(error) => return Err(error),
    };
    Ok(&current == expected)
}

/// Check whether an exact live process image receipt currently belongs to the
/// manager-created, non-killing module owner recorded in the owner.json record.
///
/// The owner record is the existing owner.json envelope. The child image
/// identity must be the exact current receipt returned by
/// process_image_identity(pid). This is a point-in-time membership check only:
/// it does not adopt an owner, launch or stop a process, authorize a module
/// action, or prove family departure. After the owner exits, callers must use
/// the owning supervisor's Group::children_empty proof for whole-family
/// departure.
pub fn module_child_belongs_to_owner(
    owner_record: &Value,
    child_image_identity: &Value,
) -> Result<bool> {
    let owner = parse_owner(owner_record)?;
    let child = parse_child_image(child_image_identity)?;
    if owner.pid == child.pid {
        return Ok(false);
    }
    if !current_child_image_matches(&child, child_image_identity)? {
        return Ok(false);
    }

    #[cfg(windows)]
    {
        windows_membership::module_child_belongs_to_owner(&owner, &child)
    }

    #[cfg(target_os = "linux")]
    {
        linux_membership::module_child_belongs_to_owner(&owner, &child)
    }

    #[cfg(not(any(windows, target_os = "linux")))]
    {
        let _ = (owner, child);
        Err(Error::new(
            "MODULE_MEMBERSHIP_UNSUPPORTED",
            "module process membership is supported only on Windows and Linux",
        ))
    }
}

#[cfg(windows)]
mod windows_membership {
    use super::*;
    use std::{mem::size_of, ptr};
    use windows_sys::Win32::{
        Foundation::{
            CloseHandle, ERROR_FILE_NOT_FOUND, ERROR_INVALID_PARAMETER, FILETIME, HANDLE,
            WAIT_FAILED, WAIT_OBJECT_0, WAIT_TIMEOUT,
        },
        System::{
            JobObjects::{
                IsProcessInJob, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
                OpenJobObjectW, QueryInformationJobObject,
            },
            Threading::{
                GetProcessTimes, OpenProcess, PROCESS_QUERY_LIMITED_INFORMATION,
                PROCESS_SYNCHRONIZE, WaitForSingleObject,
            },
        },
    };

    // The documented Job query right is not exposed by the enabled windows-sys API.
    const JOB_QUERY_ACCESS: u32 = 0x0004;

    struct OwnedHandle(HANDLE);

    impl Drop for OwnedHandle {
        fn drop(&mut self) {
            // SAFETY: this wrapper owns the valid handle returned by a Win32 open call.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }

    fn open_job(owner: &ModuleOwner) -> Result<Option<OwnedHandle>> {
        let name: Vec<u16> = format!(r"Global\EliotSwarmModule-{}", owner.token)
            .encode_utf16()
            .chain(Some(0))
            .collect();
        // SAFETY: name is nul-terminated UTF-16 and the returned handle is closed by OwnedHandle.
        let job = unsafe { OpenJobObjectW(JOB_QUERY_ACCESS, 0, name.as_ptr()) };
        if job.is_null() {
            let error = std::io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_FILE_NOT_FOUND as i32) {
                Ok(None)
            } else {
                Err(error.into())
            };
        }
        Ok(Some(OwnedHandle(job)))
    }

    fn process_signaled(process: HANDLE) -> Result<bool> {
        // SAFETY: the caller retains the process handle for this zero-time wait.
        match unsafe { WaitForSingleObject(process, 0) } {
            WAIT_OBJECT_0 => Ok(true),
            WAIT_TIMEOUT => Ok(false),
            WAIT_FAILED => Err(std::io::Error::last_os_error().into()),
            status => Err(Error::new(
                "PROCESS_WAIT",
                format!("unexpected process wait status {status:#x}"),
            )),
        }
    }

    fn process_creation_filetime(process: HANDLE) -> Result<u64> {
        // SAFETY: the caller retains a valid process handle for this query.
        unsafe {
            let mut creation: FILETIME = std::mem::zeroed();
            let mut exit: FILETIME = std::mem::zeroed();
            let mut kernel: FILETIME = std::mem::zeroed();
            let mut user: FILETIME = std::mem::zeroed();
            if GetProcessTimes(process, &mut creation, &mut exit, &mut kernel, &mut user) == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(((creation.dwHighDateTime as u64) << 32) | creation.dwLowDateTime as u64)
        }
    }

    fn process_in_job(process: HANDLE, job: HANDLE) -> Result<bool> {
        // SAFETY: both handles remain open for the duration of the query.
        unsafe {
            let mut member = 0;
            if IsProcessInJob(process, job, &mut member) == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            Ok(member != 0)
        }
    }

    fn check_non_killing_job(job: HANDLE) -> Result<()> {
        // SAFETY: initialized output and a valid exact Job handle.
        unsafe {
            let mut limits: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            if QueryInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                (&mut limits as *mut JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
                ptr::null_mut(),
            ) == 0
            {
                return Err(std::io::Error::last_os_error().into());
            }
            if limits.BasicLimitInformation.LimitFlags & JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE != 0 {
                return Err(Error::new(
                    "MODULE_OWNER_KILLING",
                    "module owner Job has kill-on-close enabled",
                ));
            }
        }
        Ok(())
    }

    fn open_live_process(pid: u32) -> Result<Option<OwnedHandle>> {
        // SAFETY: requested access is limited to identity, Job membership, and wait state.
        let handle = unsafe {
            OpenProcess(
                PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_SYNCHRONIZE,
                0,
                pid,
            )
        };
        if handle.is_null() {
            let error = std::io::Error::last_os_error();
            return if error.raw_os_error() == Some(ERROR_INVALID_PARAMETER as i32) {
                Ok(None)
            } else {
                Err(error.into())
            };
        }
        Ok(Some(OwnedHandle(handle)))
    }

    fn owner_is_live_member(owner: &ModuleOwner, job: HANDLE) -> Result<Option<OwnedHandle>> {
        let Some(process) = open_live_process(owner.pid)? else {
            return Ok(None);
        };
        if process_signaled(process.0)?
            || process_creation_filetime(process.0)? != owner.creation_filetime
        {
            return Ok(None);
        }
        if !process_in_job(process.0, job)? {
            return Ok(None);
        }
        Ok(Some(process))
    }

    pub(super) fn module_child_belongs_to_owner(
        owner: &ModuleOwner,
        child: &ChildImage,
    ) -> Result<bool> {
        let Some(job) = open_job(owner)? else {
            return Ok(false);
        };
        check_non_killing_job(job.0)?;
        let Some(owner_process) = owner_is_live_member(owner, job.0)? else {
            return Ok(false);
        };
        let Some(child_process) = open_live_process(child.pid)? else {
            return Ok(false);
        };
        if process_signaled(child_process.0)?
            || process_creation_filetime(child_process.0)?.to_string() != child.creation_filetime
        {
            return Ok(false);
        }
        let belongs = process_in_job(child_process.0, job.0)?;
        if !belongs || process_signaled(owner_process.0)? || process_signaled(child_process.0)? {
            return Ok(false);
        }
        Ok(true)
    }
}

#[cfg(target_os = "linux")]
mod linux_membership {
    use super::*;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

    fn open_pidfd(pid: u32) -> Result<Option<OwnedFd>> {
        // SAFETY: pidfd_open has no pointer arguments and returns a new owned descriptor.
        let raw = unsafe { libc::syscall(libc::SYS_pidfd_open, pid as libc::pid_t, 0u32) };
        if raw < 0 {
            let error = std::io::Error::last_os_error();
            return if error.raw_os_error() == Some(libc::ESRCH) {
                Ok(None)
            } else {
                Err(error.into())
            };
        }
        // SAFETY: raw is the new descriptor returned by pidfd_open.
        Ok(Some(unsafe { OwnedFd::from_raw_fd(raw as i32) }))
    }

    fn pidfd_exited(pidfd: &OwnedFd) -> Result<bool> {
        let mut poll = libc::pollfd {
            fd: pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one initialized pollfd and a zero-time wait.
        if unsafe { libc::poll(&mut poll, 1, 0) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        if poll.revents & (libc::POLLERR | libc::POLLNVAL) != 0 {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "cannot observe pinned module process identity",
            ));
        }
        Ok(poll.revents & (libc::POLLIN | libc::POLLHUP) != 0)
    }

    fn process_stat(pid: u32) -> Result<(i32, String)> {
        let text = std::fs::read_to_string(format!("/proc/{pid}/stat")).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Error::new("PROCESS_GONE", "process exited during membership check")
            } else {
                error.into()
            }
        })?;
        let tail = text
            .rsplit_once(')')
            .ok_or_else(|| Error::new("PROCESS_IDENTITY", "invalid proc stat"))?
            .1;
        let fields: Vec<_> = tail.split_whitespace().collect();
        if fields.len() < 20 {
            return Err(Error::new("PROCESS_IDENTITY", "incomplete proc stat"));
        }
        let pgid = fields[2]
            .parse::<i32>()
            .map_err(|_| Error::new("PROCESS_IDENTITY", "invalid process group"))?;
        Ok((pgid, fields[19].to_owned()))
    }

    fn boot_id() -> Result<String> {
        let boot = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")?;
        let boot = boot.trim();
        if !valid_uuid(boot) {
            return Err(Error::new(
                "PROCESS_IDENTITY",
                "kernel boot identity is unavailable",
            ));
        }
        Ok(boot.to_owned())
    }

    fn owner_is_live_member(owner: &ModuleOwner, system_boot_id: &str) -> Result<Option<OwnedFd>> {
        if owner.boot_id != system_boot_id {
            return Ok(None);
        }
        let Some(pidfd) = open_pidfd(owner.pid)? else {
            return Ok(None);
        };
        if pidfd_exited(&pidfd)? {
            return Ok(None);
        }
        let (pgid, start_ticks) = match process_stat(owner.pid) {
            Ok(identity) => identity,
            Err(error) if error.code == "PROCESS_GONE" => return Ok(None),
            Err(error) => return Err(error),
        };
        if pgid != owner.pgid || start_ticks != owner.start_ticks || pidfd_exited(&pidfd)? {
            return Ok(None);
        }
        Ok(Some(pidfd))
    }

    pub(super) fn module_child_belongs_to_owner(
        owner: &ModuleOwner,
        child: &ChildImage,
    ) -> Result<bool> {
        let system_boot_id = boot_id()?;
        let Some(owner_pidfd) = owner_is_live_member(owner, &system_boot_id)? else {
            return Ok(false);
        };
        if child.boot_id != system_boot_id {
            return Ok(false);
        }
        let Some(child_pidfd) = open_pidfd(child.pid)? else {
            return Ok(false);
        };
        if pidfd_exited(&child_pidfd)? {
            return Ok(false);
        }
        let (child_pgid, child_start_ticks) = match process_stat(child.pid) {
            Ok(identity) => identity,
            Err(error) if error.code == "PROCESS_GONE" => return Ok(false),
            Err(error) => return Err(error),
        };
        if child_pgid != owner.pgid
            || child_start_ticks != child.start_ticks
            || pidfd_exited(&child_pidfd)?
            || pidfd_exited(&owner_pidfd)?
        {
            return Ok(false);
        }
        Ok(true)
    }
}
