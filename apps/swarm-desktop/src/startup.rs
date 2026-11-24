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
/// terminal when there is one). A panic inside a native boundary (a
/// window hook or a WinUI event handler, [`native_boundary`]) only queues
/// its line: file I/O and terminal output there could outlast a hook's
/// time limit; the UI thread writes it ([`drain_deferred`]).
pub fn install_panic_hook() {
    let report = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if NATIVE_DEPTH.with(std::cell::Cell::get) > 0 {
            defer(format!("panic: {info}"));
            return;
        }
        record(&format!("panic: {info}"));
        report(info);
    }));
}

thread_local! {
    /// Native boundaries entered on this thread and not left yet.
    static NATIVE_DEPTH: std::cell::Cell<u32> = const { std::cell::Cell::new(0) };
}

/// Lines a native boundary queued for the UI thread to write.
static DEFERRED: std::sync::Mutex<Vec<String>> = std::sync::Mutex::new(Vec::new());
/// Most lines the queue holds before the UI thread drains it.
const DEFERRED_LINES: usize = 32;

/// Inside a native boundary while this guard lives (see
/// [`install_panic_hook`] and [`caught_panic`]).
pub struct NativeBoundary(());

impl Drop for NativeBoundary {
    fn drop(&mut self) {
        NATIVE_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

/// Enter a native boundary (a hook procedure, a WinUI event handler).
pub fn native_boundary() -> NativeBoundary {
    NATIVE_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
    NativeBoundary(())
}

/// Queue a line for the UI thread without waiting: a boundary never blocks
/// on the queue (a busy one drops the line) and never grows it unbounded.
fn defer(line: String) {
    if let Ok(mut queued) = DEFERRED.try_lock()
        && queued.len() < DEFERRED_LINES
    {
        queued.push(line);
    }
}

/// Write the lines native boundaries queued (the UI thread, outside any
/// boundary: the terminal and `desktop.log`).
pub fn drain_deferred() {
    let lines = DEFERRED
        .lock()
        .map(|mut queued| std::mem::take(&mut *queued))
        .unwrap_or_default();
    for line in lines {
        note(&line);
    }
}

/// A start-up or shutdown problem the desktop runs on after: the terminal
/// (if any) and the log.
pub fn note(error: &str) {
    eprintln!("swarm-desktop: {error}");
    record(error);
}

/// Whether this session already told the window about a caught panic.
static PANIC_SHOWN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

/// A panic stopped at a native boundary (a WinUI event handler or one of
/// the window's hooks), where unwinding would abort the process. Here it
/// only queues its line and, the first time this session, enqueues the
/// window's notice (`Shortcut::Panicked`): no file I/O and no terminal
/// output, which could outlast a hook's time limit or fail and unwind out
/// of the `extern "system"` function. The UI thread writes the line to the
/// log and shows the notice ([`drain_deferred`]).
pub fn caught_panic(source: &str, panic: &(dyn std::any::Any + Send)) {
    let what = panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic");
    defer(format!("{source} panicked and was stopped there: {what}"));
    if !PANIC_SHOWN.swap(true, std::sync::atomic::Ordering::SeqCst) {
        crate::shortcuts::panicked();
    }
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
