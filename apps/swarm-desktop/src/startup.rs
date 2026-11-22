//! How the GUI-subsystem process starts and fails (see `main`).
//!
//! - A start from a terminal attaches to that terminal's console:
//!   `--help`, `--version` and start-up errors print there. Attaching
//!   replaces the standard handles cmd's redirection passes by inheritance
//!   (`> file`, `| more`) with console handles (observed live), so a
//!   handle the process was given is put back, and `CONOUT$` is opened
//!   only for a handle that is still unusable.
//! - Started from Explorer or a shortcut, the process has no console, so
//!   start-up errors and panics are appended to `desktop.log` in the data
//!   directory. The file is bounded: it starts over past [`LOG_BYTES`]. It
//!   is written only into a data directory that already exists (the daemon
//!   creates and secures it), never under its daemon-owned `logs/`.
//! - A fatal start-up error is also shown in a plain message box.
#![allow(
    unsafe_code,
    reason = "Desktop startup requires the Windows process and window APIs."
)]

use std::io::Write;
use std::os::windows::io::IntoRawHandle;
use std::path::PathBuf;

use windows_sys::Win32::Foundation::{HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Storage::FileSystem::{FILE_TYPE_UNKNOWN, GetFileType};
use windows_sys::Win32::System::Console::{
    ATTACH_PARENT_PROCESS, AttachConsole, GetStdHandle, STD_ERROR_HANDLE, STD_OUTPUT_HANDLE,
    SetStdHandle,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{MB_ICONERROR, MB_OK, MessageBoxW};

/// `desktop.log` starts over once it is larger than this.
const LOG_BYTES: u64 = 256 * 1024;
/// One entry keeps at most this many bytes.
const ENTRY_BYTES: usize = 8 * 1024;
const LOG_FILE: &str = "desktop.log";

/// Attach to the console of the process that started the desktop, if it
/// has one (Microsoft Learn: `AttachConsole`). Standard output and error
/// keep a handle the process was given (a redirect or pipe); otherwise
/// they write to that console, or Rust's writes would be dropped.
pub fn attach_parent_console() {
    // SAFETY: plain queries of this process's standard handles.
    let given =
        [STD_OUTPUT_HANDLE, STD_ERROR_HANDLE].map(|which| (which, unsafe { GetStdHandle(which) }));
    // SAFETY: a plain Win32 call; failure leaves the process without a console.
    if unsafe { AttachConsole(ATTACH_PARENT_PROCESS) } == 0 {
        return;
    }
    for (which, handle) in given {
        if usable(handle) {
            // SAFETY: the process's own inherited handle, put back.
            unsafe { SetStdHandle(which, handle) };
            continue;
        }
        // SAFETY: a plain query of this process's standard handle.
        if usable(unsafe { GetStdHandle(which) }) {
            continue;
        }
        let Ok(console) = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("CONOUT$")
        else {
            return;
        };
        // SAFETY: a console handle this process owns; it stays open for the
        // process's life as its standard handle.
        unsafe { SetStdHandle(which, console.into_raw_handle()) };
    }
}

/// A file, pipe or character device (console) handle.
fn usable(handle: HANDLE) -> bool {
    // SAFETY: GetFileType only queries the handle; an invalid one reports
    // FILE_TYPE_UNKNOWN.
    !handle.is_null()
        && handle != INVALID_HANDLE_VALUE
        && unsafe { GetFileType(handle) } != FILE_TYPE_UNKNOWN
}

/// Record every panic in `desktop.log`, then report it as before (to the
/// terminal when there is one).
pub fn install_panic_hook() {
    let report = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        record(&format!("panic: {info}"));
        report(info);
    }));
}

/// A start-up or shutdown problem the desktop runs on after: the terminal
/// (if any) and the log.
pub fn note(error: &str) {
    eprintln!("swarm-desktop: {error}");
    record(error);
}

/// A start-up error that ends the process: the terminal (if any), the log
/// and a message box, then exit status 1.
pub fn fatal(error: &str) -> ! {
    eprintln!("swarm-desktop: {error}");
    let logged = record(&format!("start-up failed: {error}"));
    let mut text = format!("Eliot Swarm could not start.\n\n{error}");
    if let Some(path) = logged {
        text.push_str(&format!("\n\nDetails are in {}.", path.display()));
    }
    let text = wide(&text);
    let title = wide("Eliot Swarm");
    // SAFETY: both strings are NUL-terminated UTF-16 that outlive the call.
    unsafe {
        MessageBoxW(
            std::ptr::null_mut(),
            text.as_ptr(),
            title.as_ptr(),
            MB_OK | MB_ICONERROR,
        )
    };
    std::process::exit(1);
}

/// Append one bounded entry to `desktop.log`; the path when it was written.
fn record(entry: &str) -> Option<PathBuf> {
    let data_dir = crate::client::data_dir().ok()?;
    if !data_dir.is_dir() {
        return None;
    }
    let path = data_dir.join(LOG_FILE);
    let full = std::fs::metadata(&path).is_ok_and(|meta| meta.len() > LOG_BYTES);
    let mut options = std::fs::OpenOptions::new();
    options.create(true);
    if full {
        options.write(true).truncate(true);
    } else {
        options.append(true);
    }
    let mut file = options.open(&path).ok()?;
    let mut end = entry.len().min(ENTRY_BYTES);
    while !entry.is_char_boundary(end) {
        end -= 1;
    }
    let now = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    writeln!(file, "{now} {}", &entry[..end]).ok()?;
    Some(path)
}

fn wide(text: &str) -> Vec<u16> {
    text.encode_utf16().chain(std::iter::once(0)).collect()
}
