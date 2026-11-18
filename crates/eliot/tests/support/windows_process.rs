//! Birth-verified ownership handle for a background daemon the test did not
//! spawn as a direct child. Creation FILETIME must match the metadata
//! published in this test's fresh private data directory, so termination and
//! liveness always target that exact process object, never a recycled PID.
#![allow(unsafe_code)]

use serde_json::Value;
use std::{
    ffi::c_void,
    io,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle},
    path::{Path, PathBuf},
    time::Duration,
};

#[repr(C)]
#[derive(Default)]
struct FileTime {
    low: u32,
    high: u32,
}

// https://learn.microsoft.com/windows/win32/api/processthreadsapi/nf-processthreadsapi-openprocess
// https://learn.microsoft.com/windows/win32/api/processthreadsapi/nf-processthreadsapi-getprocesstimes
// https://learn.microsoft.com/windows/win32/api/jobapi2/nf-jobapi2-createjobobjectw
// https://learn.microsoft.com/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject
#[link(name = "kernel32")]
unsafe extern "system" {
    fn OpenProcess(access: u32, inherit: i32, pid: u32) -> *mut c_void;
    fn GetLastError() -> u32;
    fn GetProcessTimes(
        process: *mut c_void,
        creation: *mut FileTime,
        exit: *mut FileTime,
        kernel: *mut FileTime,
        user: *mut FileTime,
    ) -> i32;
    fn WaitForSingleObject(handle: *mut c_void, milliseconds: u32) -> u32;
    fn GetExitCodeProcess(process: *mut c_void, code: *mut u32) -> i32;
    fn TerminateProcess(process: *mut c_void, code: u32) -> i32;
    fn CreateJobObjectW(attributes: *const c_void, name: *const u16) -> *mut c_void;
    fn SetInformationJobObject(
        job: *mut c_void,
        class: i32,
        info: *const c_void,
        length: u32,
    ) -> i32;
    fn AssignProcessToJobObject(job: *mut c_void, process: *mut c_void) -> i32;
    fn TerminateJobObject(job: *mut c_void, code: u32) -> i32;
}

/// Access denied proves existence: the PID names a live process this test
/// cannot open, so it must not be mistaken for a gone one.
const ERROR_ACCESS_DENIED: u32 = 5;

/// Query rights for the liveness probe. Termination rights live only on the
/// owned `Daemon` handle below, which is the single handle that may signal
/// that exact process object.
const PROBE_ACCESS: u32 = 0x1000 | 0x0010_0000; // QUERY_LIMITED_INFORMATION | SYNCHRONIZE

/// Termination plus query rights for the owned `Daemon` handle, requested on
/// the same birth-verified open so `TerminateProcess` cannot fail for want of
/// access on an otherwise valid handle.
const OWN_ACCESS: u32 = 0x0001 | 0x1000 | 0x0010_0000; // TERMINATE | QUERY_LIMITED_INFORMATION | SYNCHRONIZE

/// True only when the exact birth-verified process is still running: a gone
/// PID, a recycled PID with a different birth, or a signaled (exited) handle
/// all report not-live. Only a wait timeout is live. An open refused for
/// access still proves the PID exists, so it reports live. Query failures
/// are explicit errors, never silent not-live, so the panic regression cannot
/// mistake an inconclusive probe for a reaped daemon.
pub fn exact_process_is_live(pid: u32, expected_birth: u64) -> io::Result<bool> {
    // Never inherit the handle.
    let raw = unsafe { OpenProcess(PROBE_ACCESS, 0, pid) };
    if raw.is_null() {
        // SAFETY: OpenProcess failed; reading the thread error is valid.
        let code = unsafe { GetLastError() };
        if code == ERROR_ACCESS_DENIED {
            return Ok(true);
        }
        // A missing PID is proof of absence; any other failure is unknown.
        if code == 2 || code == 87 {
            return Ok(false);
        }
        return Err(io::Error::from_raw_os_error(code as i32));
    }
    // SAFETY: OpenProcess returned one owned, non-null process handle.
    let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
    let (mut creation, mut exit, mut kernel, mut user) = (
        FileTime::default(),
        FileTime::default(),
        FileTime::default(),
        FileTime::default(),
    );
    // SAFETY: handle is live and every output is valid writable storage.
    if unsafe {
        GetProcessTimes(
            handle.as_raw_handle(),
            &mut creation,
            &mut exit,
            &mut kernel,
            &mut user,
        )
    } == 0
    {
        return Err(io::Error::last_os_error());
    }
    let birth = (u64::from(creation.high) << 32) | u64::from(creation.low);
    if birth != expected_birth {
        return Ok(false);
    }
    // SAFETY: handle remains live through this zero-duration wait.
    match unsafe { WaitForSingleObject(handle.as_raw_handle(), 0) } {
        258 => Ok(true),
        0 => Ok(false),
        _ => Err(io::Error::last_os_error()),
    }
}

#[repr(C)]
struct JobBasicLimits {
    per_process_time: i64,
    per_job_time: i64,
    limit_flags: u32,
    min_working_set: usize,
    max_working_set: usize,
    active_process_limit: u32,
    affinity: usize,
    priority_class: u32,
    scheduling_class: u32,
}

#[repr(C)]
struct JobIoCounters {
    read_ops: u64,
    write_ops: u64,
    other_ops: u64,
    read_transfer: u64,
    write_transfer: u64,
    other_transfer: u64,
}

#[repr(C)]
struct JobExtendedLimits {
    basic: JobBasicLimits,
    io: JobIoCounters,
    process_memory: usize,
    job_memory: usize,
    peak_process_memory: usize,
    peak_job_memory: usize,
}

const JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE: u32 = 0x2000;
const JOB_OBJECT_EXTENDED_LIMIT_INFORMATION: i32 = 9;
const PROCESS_ASSIGN_ACCESS: u32 = 0x0001 | 0x0100; // TERMINATE | SET_QUOTA

/// Test-owned kill-on-close custody for a detached background daemon the test
/// does not spawn as a direct child. Created before the starter spawns so the
/// starter (and, by inheritance, its daemon child) is assigned at spawn: if
/// startup stalls before `daemon.json` appears, dropping the Job still kills
/// the live daemon. Never adopted by name; the handle is owned here.
pub struct CustodyJob {
    handle: OwnedHandle,
}

impl CustodyJob {
    pub fn new_kill_on_close() -> io::Result<Self> {
        // SAFETY: null attributes request default security; null name is private.
        let raw = unsafe { CreateJobObjectW(std::ptr::null(), std::ptr::null()) };
        if raw.is_null() {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!("create Job Object: {error}"),
            ));
        }
        // SAFETY: CreateJobObjectW returned one owned, non-null Job handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        // Same mechanism as the daemon's custody Job: extended limits with
        // only KILL_ON_JOB_CLOSE set, so closing the last handle terminates
        // every member.
        let limits = JobExtendedLimits {
            basic: JobBasicLimits {
                per_process_time: 0,
                per_job_time: 0,
                limit_flags: JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
                min_working_set: 0,
                max_working_set: 0,
                active_process_limit: 0,
                affinity: 0,
                priority_class: 0,
                scheduling_class: 0,
            },
            io: JobIoCounters {
                read_ops: 0,
                write_ops: 0,
                other_ops: 0,
                read_transfer: 0,
                write_transfer: 0,
                other_transfer: 0,
            },
            process_memory: 0,
            job_memory: 0,
            peak_process_memory: 0,
            peak_job_memory: 0,
        };
        // SAFETY: limits has the exact extended layout the daemon uses.
        if unsafe {
            SetInformationJobObject(
                handle.as_raw_handle(),
                JOB_OBJECT_EXTENDED_LIMIT_INFORMATION,
                (&limits as *const JobExtendedLimits).cast(),
                std::mem::size_of::<JobExtendedLimits>() as u32,
            )
        } == 0
        {
            let error = io::Error::last_os_error();
            return Err(io::Error::new(
                error.kind(),
                format!("configure Job kill-on-close: {error}"),
            ));
        }
        Ok(Self { handle })
    }

    /// Assign one live PID to this Job. Call immediately after the starter
    /// spawns (custody at spawn) and again for the claimed daemon PID so a
    /// daemon that won the spawn race still joins the Job for unwind.
    pub fn assign(&self, pid: u32) -> io::Result<()> {
        // Never inherit the process handle.
        let raw = unsafe { OpenProcess(PROCESS_ASSIGN_ACCESS, 0, pid) };
        if raw.is_null() {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: OpenProcess returned one owned process handle.
        let process = unsafe { OwnedHandle::from_raw_handle(raw) };
        // SAFETY: both handles are live and owned for this call.
        if unsafe { AssignProcessToJobObject(self.handle.as_raw_handle(), process.as_raw_handle()) }
            == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for CustodyJob {
    fn drop(&mut self) {
        // Kill-on-close already guarantees member termination when the last
        // handle closes, but terminate explicitly so unwind reaps promptly
        // even if another duplicated handle still exists.
        // SAFETY: this object uniquely owns the Job handle.
        unsafe {
            TerminateJobObject(self.handle.as_raw_handle(), 1);
        }
    }
}

pub struct Daemon {
    handle: OwnedHandle,
    data: PathBuf,
}

impl Daemon {
    pub fn open(data: &Path, info: &Value) -> Self {
        Self::try_open(data, info).expect("background daemon handle")
    }

    /// Birth-verified open without panicking: None when the PID is gone,
    /// unreadable, or recycled (creation time no longer matches the metadata
    /// published in this test's fresh private data directory). Lets a test
    /// claim ownership of a detached daemon the moment its metadata appears,
    /// before asserting anything that could panic and leak it.
    pub fn try_open(data: &Path, info: &Value) -> Option<Self> {
        let pid = u32::try_from(info["pid"].as_u64()?).ok()?;
        let expected_birth = info["process_birth"].as_str()?;
        // Termination rights on the same birth-verified open; never inherit.
        let raw = unsafe { OpenProcess(OWN_ACCESS, 0, pid) };
        if raw.is_null() {
            return None;
        }
        // SAFETY: OpenProcess returned one owned, non-null process handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        let (mut creation, mut exit, mut kernel, mut user) = (
            FileTime::default(),
            FileTime::default(),
            FileTime::default(),
            FileTime::default(),
        );
        // SAFETY: handle is live and every output is valid writable storage.
        if unsafe {
            GetProcessTimes(
                handle.as_raw_handle(),
                &mut creation,
                &mut exit,
                &mut kernel,
                &mut user,
            )
        } == 0
        {
            return None;
        }
        let birth = (u64::from(creation.high) << 32) | u64::from(creation.low);
        if expected_birth != format!("windows-creation-filetime:{birth}") {
            return None;
        }
        Some(Self {
            handle,
            data: data.to_owned(),
        })
    }

    pub fn wait(&self, timeout: Duration) -> io::Result<Option<u32>> {
        let milliseconds = u32::try_from(timeout.as_millis()).expect("bounded Windows wait");
        // SAFETY: handle remains live through this bounded wait.
        match unsafe { WaitForSingleObject(self.handle.as_raw_handle(), milliseconds) } {
            0 => {
                let mut code = 0;
                // SAFETY: the signaled handle and writable output remain valid.
                if unsafe { GetExitCodeProcess(self.handle.as_raw_handle(), &mut code) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(code))
            }
            258 => Ok(None),
            _ => Err(io::Error::last_os_error()),
        }
    }
}

impl Daemon {
    /// Terminate the exact birth-verified process behind this handle. The
    /// handle names one process object, never a re-resolved PID, so a
    /// recycled PID cannot be hit.
    pub fn terminate(&self) -> io::Result<()> {
        // SAFETY: handle is live; TerminateProcess targets this exact object.
        if unsafe { TerminateProcess(self.handle.as_raw_handle(), 1) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        // Own the exact background process: a test that panics after spawn
        // must not leak it. Unwind uses only the retained birth-verified
        // handle, never IPC by data directory: the original may have exited
        // and a replacement may have bound the directory before an RPC
        // arrives. Both the terminate and the reap results are checked.
        match self.wait(Duration::ZERO) {
            Ok(Some(_)) => return,
            Ok(None) => {}
            Err(error) => {
                eprintln!("background test daemon wait failed: {error}; terminating exact handle");
            }
        }
        if let Err(error) = self.terminate() {
            eprintln!(
                "background test daemon terminate failed (data={}): {error}",
                self.data.display()
            );
        }
        match self.wait(Duration::from_secs(10)) {
            Ok(Some(_)) => {}
            Ok(None) => eprintln!(
                "background test daemon survived termination (data={}); leaked",
                self.data.display()
            ),
            Err(error) => eprintln!(
                "background test daemon reap failed (data={}): {error}; leaked",
                self.data.display()
            ),
        }
    }
}
