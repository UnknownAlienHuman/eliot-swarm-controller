//! The window's keyboard: the chords Ctrl+N (new task), Ctrl+K (search
//! tasks), Ctrl+M (model picker) and Ctrl+, (Settings); the keys of an open
//! popover (↑/↓/←/→, Enter, Esc, Ctrl+F); and how the last input arrived,
//! which tells a search suggestion reached with the arrows from one chosen
//! with Enter or a click.
//!
//! Reactor 0.100.0 has no key events, and its `KeyAccelerator` keys are R,
//! Enter and the number pad only; WinUI's own `KeyboardAccelerator` needs
//! the native element, which Reactor keeps private. Hooks scoped to the
//! window's own thread (Microsoft Learn: `SetWindowsHookExW` with that
//! thread's id; `KeyboardProc` for `WH_KEYBOARD`, `GetMsgProc` for
//! `WH_GETMESSAGE`) see exactly the key and pointer messages this window's
//! message loop retrieves; nothing outside the process is observed. The
//! chords, and a popover's keys while one is open, are taken; every other
//! key passes on through `CallNextHookEx`. windows-rs master (unreleased
//! Reactor 0.101.0) adds `on_preview_key_down`; a release with it replaces
//! this.
#![allow(unsafe_code)]

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use windows_sys::Win32::Foundation::{LPARAM, LRESULT, WPARAM};
use windows_sys::Win32::System::Threading::GetCurrentThreadId;
use windows_sys::Win32::UI::Input::KeyboardAndMouse::{
    GetKeyState, VK_CONTROL, VK_DOWN, VK_ESCAPE, VK_F, VK_K, VK_LEFT, VK_M, VK_N, VK_OEM_COMMA,
    VK_RETURN, VK_RIGHT, VK_SHIFT, VK_UP,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    CallNextHookEx, HC_ACTION, MSG, PM_REMOVE, SetWindowsHookExW, WH_GETMESSAGE, WH_KEYBOARD,
    WM_LBUTTONDOWN, WM_MBUTTONDOWN, WM_NCLBUTTONDOWN, WM_POINTERDOWN, WM_RBUTTONDOWN,
};

/// A key of an open popover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PopoverKey {
    Up,
    Down,
    Left,
    Right,
    Enter,
    Escape,
    /// Ctrl+F: mark the highlighted model a favorite.
    Favorite,
}

/// What the window acts on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shortcut {
    /// Ctrl+N.
    NewTask,
    /// Ctrl+K.
    Search,
    /// Ctrl+M.
    ModelPicker,
    /// Ctrl+, (comma).
    Settings,
    /// A key while a popover is open (taken from the focused control).
    Popover(PopoverKey),
    /// Enter outside a popover, passed on to the focused control; carries
    /// the input epoch at the press (see [`last_input`]).
    Enter(u64),
}

/// Which popover is open: none, the model picker (all popover keys), or a
/// menu (no ←/→ or Ctrl+F).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Popover {
    None = 0,
    Picker = 1,
    Menu = 2,
}

/// How the last input arrived.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Input {
    None,
    /// ↑ or ↓.
    Arrow,
    Enter,
    /// Any other key.
    Key,
    /// A pointer or mouse button press.
    Pointer,
}

static CHORDS: OnceLock<Mutex<Sender<Shortcut>>> = OnceLock::new();
static POPOVER: AtomicU8 = AtomicU8::new(Popover::None as u8);
static LAST_INPUT: AtomicU8 = AtomicU8::new(0);
static LAST_INPUT_MS: AtomicU64 = AtomicU64::new(0);
/// Counts inputs that end a keyboard walk through suggestions: any key
/// other than ↑/↓/Enter, and every pointer press.
static INPUT_EPOCH: AtomicU64 = AtomicU64::new(0);

/// Tell the hook which popover is open (its keys are taken while it is).
pub fn set_popover(popover: Popover) {
    POPOVER.store(popover as u8, Ordering::SeqCst);
}

/// How the last input arrived, how long ago (ms) and the input epoch.
pub fn last_input() -> (Input, u64, u64) {
    let input = match LAST_INPUT.load(Ordering::SeqCst) {
        1 => Input::Arrow,
        2 => Input::Enter,
        3 => Input::Key,
        4 => Input::Pointer,
        _ => Input::None,
    };
    let age = now_ms().saturating_sub(LAST_INPUT_MS.load(Ordering::SeqCst));
    (input, age, INPUT_EPOCH.load(Ordering::SeqCst))
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as u64)
}

fn note_input(input: Input) {
    let code = match input {
        Input::None => 0,
        Input::Arrow => 1,
        Input::Enter => 2,
        Input::Key => 3,
        Input::Pointer => 4,
    };
    if matches!(input, Input::Key | Input::Pointer) {
        INPUT_EPOCH.fetch_add(1, Ordering::SeqCst);
    }
    LAST_INPUT.store(code, Ordering::SeqCst);
    LAST_INPUT_MS.store(now_ms(), Ordering::SeqCst);
}

/// Hook the calling thread (the window's UI thread, before its message
/// loop runs); the receiver yields each chord once per key press. The hooks
/// live as long as the thread.
pub fn install() -> Result<Receiver<Shortcut>, String> {
    let (sender, receiver) = channel();
    CHORDS
        .set(Mutex::new(sender))
        .map_err(|_| "keyboard shortcuts are already installed".to_owned())?;
    // SAFETY: both procedures have the HOOKPROC signature; a null module
    // with the current thread's id installs thread-local hooks.
    let (keys, messages) = unsafe {
        let thread = GetCurrentThreadId();
        (
            SetWindowsHookExW(WH_KEYBOARD, Some(keyboard), std::ptr::null_mut(), thread),
            SetWindowsHookExW(WH_GETMESSAGE, Some(messages), std::ptr::null_mut(), thread),
        )
    };
    if keys.is_null() || messages.is_null() {
        return Err(format!(
            "keyboard shortcuts not installed: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(receiver)
}

/// The chord of a key with the modifiers held now.
fn chord(key: u16, ctrl: bool, shift: bool, alt: bool) -> Option<Shortcut> {
    if !ctrl || shift || alt {
        return None;
    }
    match key {
        VK_N => Some(Shortcut::NewTask),
        VK_K => Some(Shortcut::Search),
        VK_M => Some(Shortcut::ModelPicker),
        VK_OEM_COMMA => Some(Shortcut::Settings),
        _ => None,
    }
}

/// The popover key of a key press while `popover` is open.
fn popover_key(popover: u8, key: u16, ctrl: bool, shift: bool, alt: bool) -> Option<PopoverKey> {
    if popover == Popover::None as u8 || shift || alt {
        return None;
    }
    let picker = popover == Popover::Picker as u8;
    match (key, ctrl) {
        (VK_UP, false) => Some(PopoverKey::Up),
        (VK_DOWN, false) => Some(PopoverKey::Down),
        (VK_LEFT, false) if picker => Some(PopoverKey::Left),
        (VK_RIGHT, false) if picker => Some(PopoverKey::Right),
        (VK_RETURN, false) => Some(PopoverKey::Enter),
        (VK_ESCAPE, false) => Some(PopoverKey::Escape),
        (VK_F, true) if picker => Some(PopoverKey::Favorite),
        _ => None,
    }
}

fn send(found: Shortcut) {
    if let Some(chords) = CHORDS.get()
        && let Ok(sender) = chords.lock()
    {
        let _ = sender.send(found);
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
        let pressed = flags & (1 << 31) == 0;
        let first = pressed && flags & (1 << 30) == 0;
        let key = wparam as u16;
        let (ctrl, shift) = (held(VK_CONTROL), held(VK_SHIFT));
        let popover = POPOVER.load(Ordering::SeqCst);
        let taken = popover_key(popover, key, ctrl, shift, alt)
            .map(Shortcut::Popover)
            .or_else(|| chord(key, ctrl, shift, alt));
        if let Some(found) = taken {
            // A popover's arrows repeat while held; a chord fires once.
            if (matches!(found, Shortcut::Popover(_)) && pressed) || first {
                send(found);
            }
            // Taken: neither the press nor the release reaches a control.
            return 1;
        }
        if pressed && !matches!(key, VK_CONTROL | VK_SHIFT) {
            match key {
                VK_UP | VK_DOWN => note_input(Input::Arrow),
                VK_RETURN => {
                    note_input(Input::Enter);
                    if first {
                        send(Shortcut::Enter(INPUT_EPOCH.load(Ordering::SeqCst)));
                    }
                }
                _ => note_input(Input::Key),
            }
        }
    }
    // SAFETY: passes the message on unchanged (the hook handle is ignored).
    unsafe { CallNextHookEx(std::ptr::null_mut(), code, wparam, lparam) }
}

/// `GetMsgProc`: `lparam` points at the retrieved `MSG`; `wparam` is
/// `PM_REMOVE` when it leaves the queue (counted once, not per peek).
unsafe extern "system" fn messages(code: i32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    if code == HC_ACTION as i32 && wparam == PM_REMOVE as WPARAM && lparam != 0 {
        // SAFETY: for HC_ACTION the system passes a valid MSG pointer.
        let message = unsafe { (*(lparam as *const MSG)).message };
        if matches!(
            message,
            WM_LBUTTONDOWN | WM_RBUTTONDOWN | WM_MBUTTONDOWN | WM_NCLBUTTONDOWN | WM_POINTERDOWN
        ) {
            note_input(Input::Pointer);
        }
    }
    // SAFETY: passes the message on unchanged.
    unsafe { CallNextHookEx(std::ptr::null_mut(), code, wparam, lparam) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chords_need_ctrl_alone() {
        assert_eq!(chord(VK_N, true, false, false), Some(Shortcut::NewTask));
        assert_eq!(chord(VK_K, true, false, false), Some(Shortcut::Search));
        assert_eq!(chord(VK_M, true, false, false), Some(Shortcut::ModelPicker));
        assert_eq!(
            chord(VK_OEM_COMMA, true, false, false),
            Some(Shortcut::Settings)
        );
        assert_eq!(chord(VK_N, false, false, false), None);
        assert_eq!(chord(VK_N, true, true, false), None);
        assert_eq!(chord(VK_K, true, false, true), None);
        assert_eq!(chord(VK_CONTROL, true, false, false), None);
    }

    #[test]
    fn popover_keys_only_while_one_is_open() {
        let picker = Popover::Picker as u8;
        let menu = Popover::Menu as u8;
        assert_eq!(popover_key(0, VK_UP, false, false, false), None);
        assert_eq!(
            popover_key(picker, VK_LEFT, false, false, false),
            Some(PopoverKey::Left)
        );
        assert_eq!(popover_key(menu, VK_LEFT, false, false, false), None);
        assert_eq!(
            popover_key(menu, VK_ESCAPE, false, false, false),
            Some(PopoverKey::Escape)
        );
        assert_eq!(
            popover_key(picker, VK_F, true, false, false),
            Some(PopoverKey::Favorite)
        );
        assert_eq!(popover_key(picker, VK_F, false, false, false), None);
        assert_eq!(popover_key(picker, VK_UP, false, true, false), None);
    }
}
