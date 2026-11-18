//! Copy text to the Windows clipboard (CF_UNICODETEXT).
//!
//! Reactor 0.100.0 exposes no clipboard API, so this is the one small Win32
//! boundary of the desktop client, through the workspace `windows-sys`
//! dependency. Like the daemon's platform module it is the only place that
//! allows `unsafe`; every handle it creates is either handed to the
//! clipboard (which then owns it) or freed here.
#![allow(unsafe_code)]

use windows_sys::Win32::Foundation::{GetLastError, GlobalFree, HWND};
use windows_sys::Win32::System::DataExchange::{
    CloseClipboard, EmptyClipboard, OpenClipboard, SetClipboardData,
};
use windows_sys::Win32::System::Memory::{GMEM_MOVEABLE, GlobalAlloc, GlobalLock, GlobalUnlock};

const CF_UNICODETEXT: u32 = 13;

/// Closes the clipboard on every exit path once it is open.
struct OpenGuard;

impl Drop for OpenGuard {
    fn drop(&mut self) {
        // SAFETY: constructed only after OpenClipboard succeeded on this thread.
        unsafe {
            CloseClipboard();
        }
    }
}

/// This process's visible WinUI window, the only valid clipboard owner
/// for a copy. Opening with a null handle would leave the owner NULL after
/// `EmptyClipboard`, and the documented Win32 contract then fails
/// `SetClipboardData` -- after the previous contents were already cleared
/// (#647). With no window yet the copy refuses before touching anything.
fn owner() -> Result<HWND, String> {
    crate::app_resources::own_window()
        .ok_or_else(|| "the window is not shown yet, so the clipboard was left alone".to_owned())
}

/// The calling thread's last Win32 error, in words.
fn last_error(while_doing: &str) -> String {
    // SAFETY: reads the calling thread's last Win32 error, called
    // immediately after the failing call on the same thread.
    let code = unsafe { GetLastError() };
    format!("{while_doing} (Win32 error {code})")
}

/// Replace the clipboard contents with `text`.
pub fn set_text(text: &str) -> Result<(), String> {
    let owner = owner()?;
    let wide: Vec<u16> = text.encode_utf16().chain(std::iter::once(0)).collect();
    let bytes = wide.len() * std::mem::size_of::<u16>();
    // SAFETY: plain Win32 calls with checked results. The global block is
    // sized for `wide`, written while locked, and either transferred to the
    // clipboard by a successful SetClipboardData or freed on failure.
    // Memory is fully prepared before the clipboard is touched, so any
    // allocation failure leaves the previous contents alone.
    unsafe {
        let block = GlobalAlloc(GMEM_MOVEABLE, bytes);
        if block.is_null() {
            return Err(last_error("clipboard memory could not be allocated"));
        }
        if block.is_null() {
            return Err("clipboard memory could not be allocated".to_owned());
        }
        let target = GlobalLock(block).cast::<u16>();
        if target.is_null() {
            GlobalFree(block);
            return Err("clipboard memory could not be locked".to_owned());
        }
        std::ptr::copy_nonoverlapping(wide.as_ptr(), target, wide.len());
        GlobalUnlock(block);
        if OpenClipboard(owner) == 0 {
            GlobalFree(block);
            return Err(last_error("the clipboard is in use by another application"));
        }
        let _guard = OpenGuard;
        if EmptyClipboard() == 0 {
            GlobalFree(block);
            return Err(last_error("the clipboard could not be cleared"));
        }
        if SetClipboardData(CF_UNICODETEXT, block).is_null() {
            GlobalFree(block);
            return Err(last_error("the clipboard rejected the text"));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copy_without_a_window_leaves_the_clipboard_alone() {
        // #647: the unit-test process shows no WinUI window, so there is
        // no valid clipboard owner and the copy must refuse before
        // opening the clipboard (never after clearing it).
        assert!(crate::app_resources::own_window().is_none());
        let error = set_text("sentinel").expect_err("no owner window in tests");
        assert!(error.contains("left alone"), "{error}");
    }
}
