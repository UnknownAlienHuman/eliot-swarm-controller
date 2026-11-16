//! The window's two keyboard shortcuts: Ctrl+N (new task) and Ctrl+K
//! (search tasks).
//!
//! Reactor 0.100.0 has no key events, and its `KeyAccelerator` keys are R,
//! Enter and the number pad only; WinUI's own `KeyboardAccelerator` needs
//! the native element, which Reactor keeps private. A keyboard hook scoped
//! to the window's own thread (Microsoft Learn: `SetWindowsHookExW` with
//! `WH_KEYBOARD` and that thread's id, and `KeyboardProc`) sees exactly the
//! key messages this window's message loop retrieves; nothing outside the
//! process is observed. The two chords are taken, every other key passes
//! on through `CallNextHookEx`. windows-rs master (unreleased Reactor
//! 0.101.0) adds `on_preview_key_down`; a release with it replaces this.
#![allow(unsafe_code)]

use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Mutex, OnceLock};

use windows_sys::Win32::Foundation::{LPARAM, LRESULT, WPARAM};
use windows_sys::Win32::System::Threading::GetCurrentThreadId;
use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
    GetKeyState, VK_CONTROL, VK_K, VK_N, VK_SHIFT,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    CallNextHookEx, HC_ACTION, SetWindowsHookExW, WH_KEYBOARD,
};

/// A chord the window acts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shortcut {
    /// Ctrl+N.
    NewTask,
    /// Ctrl+K.
    Search,
}

static CHORDS: OnceLock<Mutex<Sender<Shortcut>>> = OnceLock::new();

/// Hook the calling thread (the window's UI thread, before its message
/// loop runs); the receiver yields each chord once per key press. The hook
/// lives as long as the thread.
pub fn install() -> Result<Receiver<Shortcut>, String> {
    let (sender, receiver) = channel();
    CHORDS
        .set(Mutex::new(sender))
        .map_err(|_| "keyboard shortcuts are already installed".to_owned())?;
    // SAFETY: `keyboard` has the HOOKPROC signature; a null module with the
    // current thread's id installs a thread-local hook.
    let hook = unsafe {
        SetWindowsHookExW(
            WH_KEYBOARD,
            Some(keyboard),
            std::ptr::null_mut(),
            GetCurrentThreadId(),
        )
    };
    if hook.is_null() {
        return Err(format!(
            "keyboard shortcuts not installed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(receiver)
}

/// The chord of a key with the modifiers held now: Ctrl alone with N or K.
fn chord(key: u16, ctrl: bool, shift: bool, alt: bool) -> Option<Shortcut> {
    if !ctrl || shift || alt {
        return None;
    }
    match key {
        VK_N => Some(Shortcut::NewTask),
        VK_K => Some(Shortcut::Search),
        _ => None,
    }
}

/// `KeyboardProc`: `wparam` is the virtual key; `lparam` bit 29 is Alt
/// held, bit 30 the previous key state (auto-repeat) and bit 31 the
/// transition (0 when pressed).
unsafe extern "system" fn keyboard(code: i32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    if code == HC_ACTION as i32 {
        let flags = lparam as u32;
        // SAFETY: GetKeyState reads this thread's keyboard state.
        let held = |key: u16| unsafe { GetKeyState(i32::from(key)) } < 0;
        let alt = flags & (1 << 29) != 0;
        if let Some(found) = chord(wparam as u16, held(VK_CONTROL), held(VK_SHIFT), alt) {
            let pressed = flags & (1 << 31) == 0 && flags & (1 << 30) == 0;
            if pressed
                && let Some(chords) = CHORDS.get()
                && let Ok(sender) = chords.lock()
            {
                let _ = sender.send(found);
            }
            // Taken: neither the press nor the release reaches a control.
            return 1;
        }
    }
    // SAFETY: passes the message on unchanged (the hook handle is ignored).
    unsafe { CallNextHookEx(std::ptr::null_mut(), code, wparam, lparam) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_ctrl_n_and_ctrl_k_are_chords() {
        assert_eq!(chord(VK_N, true, false, false), Some(Shortcut::NewTask));
        assert_eq!(chord(VK_K, true, false, false), Some(Shortcut::Search));
        assert_eq!(chord(VK_N, false, false, false), None);
        assert_eq!(chord(VK_N, true, true, false), None);
        assert_eq!(chord(VK_K, true, false, true), None);
        assert_eq!(chord(VK_CONTROL, true, false, false), None);
    }
}
