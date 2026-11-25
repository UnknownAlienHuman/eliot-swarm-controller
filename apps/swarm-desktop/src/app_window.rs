//! The desktop's one WinUI window, by Win32 handle: the clipboard owner,
//! whether it is minimized, its class name, and its title-bar icon. The
//! title bar itself is the standard system title bar, which follows the
//! window's theme (Reactor's `WindowVisuals::theme`).
#![allow(
    unsafe_code,
    reason = "The window's handle and icon require direct Win32 calls."
)]

use windows_sys::Win32::Foundation::{HWND, LPARAM};
#[cfg(target_env = "msvc")]
use windows_sys::Win32::System::LibraryLoader::GetModuleHandleW;
use windows_sys::Win32::UI::WindowsAndMessaging::{
    EnumWindows, GetClassNameW, GetWindowThreadProcessId, IsWindowVisible,
};
use windows_sys::core::BOOL;

/// Give the window the executable's icon once it is shown: `false` until
/// the window exists.
#[cfg(target_env = "msvc")]
pub(crate) fn set_icon() -> bool {
    let Some(hwnd) = own_window() else {
        return false;
    };
    use windows_sys::Win32::UI::WindowsAndMessaging::{
        ICON_BIG, ICON_SMALL, LoadIconW, SendMessageW, WM_SETICON,
    };

    // Resource 101 is the icon group embedded by the Windows build script.
    let resource = std::ptr::without_provenance(101);
    // SAFETY: null selects this executable. LoadIconW returns a shared handle
    // that remains valid without a call to DestroyIcon.
    unsafe {
        let module = GetModuleHandleW(std::ptr::null());
        if module.is_null() {
            return true;
        }
        let icon = LoadIconW(module, resource);
        if !icon.is_null() {
            SendMessageW(hwnd, WM_SETICON, ICON_SMALL as usize, icon as LPARAM);
            SendMessageW(hwnd, WM_SETICON, ICON_BIG as usize, icon as LPARAM);
        }
    }
    true
}

/// Without the embedded icon resource there is nothing to set.
#[cfg(not(target_env = "msvc"))]
pub(crate) fn set_icon() -> bool {
    true
}

/// This process's visible WinUI window (the desktop has one). The valid
/// clipboard owner for [`crate::clipboard`] copies.
pub(crate) fn own_window() -> Option<HWND> {
    struct Search {
        pid: u32,
        found: HWND,
    }
    unsafe extern "system" fn visit(hwnd: HWND, search: LPARAM) -> BOOL {
        // SAFETY: `search` is the `Search` passed to EnumWindows below, alive
        // for the whole enumeration.
        let search = unsafe { &mut *(search as *mut Search) };
        let mut pid = 0;
        // SAFETY: plain Win32 queries on a window handle EnumWindows gave us.
        unsafe { GetWindowThreadProcessId(hwnd, &mut pid) };
        // SAFETY: as above.
        if pid != search.pid || unsafe { IsWindowVisible(hwnd) } == 0 {
            return 1;
        }
        let mut class = [0u16; 64];
        // SAFETY: the buffer and its length match.
        let length = unsafe { GetClassNameW(hwnd, class.as_mut_ptr(), class.len() as i32) };
        if String::from_utf16_lossy(&class[..length.max(0) as usize]) == WINUI_WINDOW_CLASS {
            search.found = hwnd;
            return 0;
        }
        1
    }
    let mut search = Search {
        pid: std::process::id(),
        found: std::ptr::null_mut(),
    };
    // SAFETY: the callback only reads and writes `search`, which outlives the
    // call.
    unsafe { EnumWindows(Some(visit), &mut search as *mut Search as LPARAM) };
    (!search.found.is_null()).then_some(search.found)
}

/// Whether this process's window is minimized; the handle is found once
/// and found again when it is gone (no window reads as not minimized).
pub(crate) fn window_minimized() -> bool {
    use std::sync::atomic::{AtomicIsize, Ordering};
    use windows_sys::Win32::UI::WindowsAndMessaging::{IsIconic, IsWindow};
    static WINDOW: AtomicIsize = AtomicIsize::new(0);
    let mut window = WINDOW.load(Ordering::Relaxed) as HWND;
    // SAFETY: a plain Win32 query; a stale or null handle reads as no window.
    if window.is_null() || unsafe { IsWindow(window) } == 0 {
        window = own_window().unwrap_or(std::ptr::null_mut());
        WINDOW.store(window as isize, Ordering::Relaxed);
    }
    // SAFETY: as above, on a window of this process.
    !window.is_null() && unsafe { IsIconic(window) } != 0
}

/// The window class of a WinUI 3 desktop window.
pub(crate) const WINUI_WINDOW_CLASS: &str = "WinUIDesktopWin32WindowClass";
