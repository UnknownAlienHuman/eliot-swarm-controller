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
/// access still proves the PID exists, so it reports live.
pub fn exact_process_is_live(pid: u32, expected_birth: u64) -> bool {
    // Never inherit the handle.
    let raw = unsafe { OpenProcess(PROBE_ACCESS, 0, pid) };
    if raw.is_null() {
        // SAFETY: OpenProcess failed; reading the thread error is valid.
        return unsafe { GetLastError() } == ERROR_ACCESS_DENIED;
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
        return false;
    }
    let birth = (u64::from(creation.high) << 32) | u64::from(creation.low);
    if birth != expected_birth {
        return false;
    }
    // SAFETY: handle remains live through this zero-duration wait.
    matches!(
        unsafe { WaitForSingleObject(handle.as_raw_handle(), 0) },
        258
    )
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
