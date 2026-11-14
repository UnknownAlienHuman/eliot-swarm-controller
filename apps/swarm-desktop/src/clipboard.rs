//! Copy text to the Windows clipboard (CF_UNICODETEXT).
//!
//! Reactor 0.100.0 exposes no clipboard API, so this is the one small Win32
//! boundary of the desktop client, through the workspace `windows-sys`
//! dependency. Like the daemon's platform module it is the only place that
//! allows `unsafe`; every handle it creates is either handed to the
//! clipboard (which then owns it) or freed here.
#![allow(unsafe_code)]

use windows_sys::Win32::Foundation::GlobalFree;
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

/// Replace the clipboard contents with `text`.
pub fn set_text(text: &str) -> Result<(), String> {
    let wide: Vec<u16> = text.encode_utf16().chain(std::iter::once(0)).collect();
    let bytes = wide.len() * std::mem::size_of::<u16>();
    // SAFETY: plain Win32 calls with checked results. The global block is
    // sized for `wide`, written while locked, and either transferred to the
    // clipboard by a successful SetClipboardData or freed on failure.
    unsafe {
        if OpenClipboard(std::ptr::null_mut()) == 0 {
            return Err("the clipboard is in use by another application".to_owned());
        }
        let _guard = OpenGuard;
        if EmptyClipboard() == 0 {
            return Err("the clipboard could not be cleared".to_owned());
        }
        let block = GlobalAlloc(GMEM_MOVEABLE, bytes);
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
        if SetClipboardData(CF_UNICODETEXT, block).is_null() {
            GlobalFree(block);
            return Err("the clipboard rejected the text".to_owned());
        }
    }
    Ok(())
}
