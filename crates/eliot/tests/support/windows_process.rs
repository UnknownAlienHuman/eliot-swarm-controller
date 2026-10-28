//! Query/synchronize-only handle for the daemon created by the capture regression.
//! No PID-based termination authority is granted. Creation FILETIME must match
//! the metadata published in this test's fresh private data directory.
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
    fn GetProcessTimes(
        process: *mut c_void,
        creation: *mut FileTime,
        exit: *mut FileTime,
        kernel: *mut FileTime,
        user: *mut FileTime,
    ) -> i32;
    fn WaitForSingleObject(handle: *mut c_void, milliseconds: u32) -> u32;
    fn GetExitCodeProcess(process: *mut c_void, code: *mut u32) -> i32;
}

pub struct Daemon {
    handle: OwnedHandle,
    data: PathBuf,
}

impl Daemon {
    pub fn open(data: &Path, info: &Value) -> Self {
        let pid = u32::try_from(info["pid"].as_u64().expect("daemon PID")).unwrap();
        // PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE; never inherit the handle.
        let raw = unsafe { OpenProcess(0x1000 | 0x0010_0000, 0, pid) };
        assert!(
            !raw.is_null(),
            "open daemon handle: {}",
            io::Error::last_os_error()
        );
        // SAFETY: OpenProcess returned one owned, non-null process handle.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw) };
        let (mut creation, mut exit, mut kernel, mut user) = (
            FileTime::default(),
            FileTime::default(),
            FileTime::default(),
            FileTime::default(),
        );
        // SAFETY: handle is live and every output is valid writable storage.
        assert_ne!(
            unsafe {
                GetProcessTimes(
                    handle.as_raw_handle(),
                    &mut creation,
                    &mut exit,
                    &mut kernel,
                    &mut user,
                )
            },
            0
        );
        let birth = (u64::from(creation.high) << 32) | u64::from(creation.low);
        assert_eq!(
            info["process_birth"],
            format!("windows-creation-filetime:{birth}")
        );
        Self {
            handle,
            data: data.to_owned(),
        }
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

impl Drop for Daemon {
    fn drop(&mut self) {
        if matches!(self.wait(Duration::ZERO), Ok(None)) {
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                super::support::cli(&self.data, &["daemon", "stop", "--force"])
            }));
            let _ = self.wait(Duration::from_secs(20));
        }
    }
}
