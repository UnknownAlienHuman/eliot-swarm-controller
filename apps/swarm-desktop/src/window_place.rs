//! The window opens where the owner left it (`[desktop] window`).
//!
//! windows-reactor 0.100.0 sets only the client size and offers no window
//! position, maximized state or close hook, so Windows cascades every new
//! window. A CBT hook scoped to the UI thread (Microsoft Learn:
//! `SetWindowsHookExW` with `WH_CBT` and that thread's id, `CBTProc`) sees
//! the WinUI window as it is created and as it is destroyed:
//! - `HCBT_CREATEWND`: the saved outer top-left, clamped into the work area
//!   of the nearest attached monitor, goes into its `CREATESTRUCTW` (the
//!   documented way to change a window's place before it exists); the
//!   window is never moved after its first frame. Without a saved place
//!   Windows chooses it.
//! - `HCBT_ACTIVATE`, the first one (inside WinUI's first show, before
//!   XAML draws): a window closed maximized is maximized. `WS_MAXIMIZE`
//!   at creation does not survive WinUI's first `ShowWindow` (observed
//!   live); a window still minimized by its start-up show restores
//!   maximized.
//! - `HCBT_DESTROYWND`: its placement (`GetWindowPlacement`) is read once
//!   for the one `[desktop]` write after the window closes.
//!
//! The start's show command (`STARTUPINFO`) also says whether the desktop
//! may take the foreground: a start minimized, hidden or without activation
//! (`start /min`, a Minimized shortcut, `swarm daemon update`) moves
//! keyboard focus only once the window is the foreground window, because
//! focusing a control activates a window that is not active
//! ([`may_take_focus`]).
#![allow(unsafe_code)]

use std::cell::Cell;

use windows_sys::Win32::Foundation::{HWND, LPARAM, LRESULT, RECT, WPARAM};
use windows_sys::Win32::Graphics::Gdi::{
    GetMonitorInfoW, MONITOR_DEFAULTTONEAREST, MONITORINFO, MonitorFromRect,
};
use windows_sys::Win32::System::Threading::{
    GetCurrentThreadId, GetStartupInfoW, STARTF_USESHOWWINDOW, STARTUPINFOW,
};
use windows_sys::Win32::UI::HiDpi::{
    AdjustWindowRectExForDpi, GetDpiForMonitor, MDT_EFFECTIVE_DPI,
};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    CBT_CREATEWNDW, CallNextHookEx, GetClassNameW, GetForegroundWindow, GetWindowPlacement,
    HCBT_ACTIVATE, HCBT_CREATEWND, HCBT_DESTROYWND, HHOOK, IsIconic, IsZoomed, SW_HIDE,
    SW_MINIMIZE, SW_SHOWMAXIMIZED, SW_SHOWMINIMIZED, SW_SHOWMINNOACTIVE, SW_SHOWNA,
    SW_SHOWNOACTIVATE, SetWindowPlacement, SetWindowsHookExW, UnhookWindowsHookEx, WH_CBT,
    WINDOWPLACEMENT, WPF_RESTORETOMAXIMIZED, WS_CHILD,
};

/// Where the window was when it closed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Closed {
    /// The normal (restored) outer top-left in physical screen pixels.
    pub x: i32,
    pub y: i32,
    pub maximized: bool,
}

/// The saved place applied when the window is created.
#[derive(Clone, Copy)]
struct Wanted {
    position: Option<(i32, i32)>,
    /// The saved client size in DIPs, to keep the window inside the work area.
    size: (u32, u32),
    maximized: bool,
}

thread_local! {
    static WANTED: Cell<Option<Wanted>> = const { Cell::new(None) };
    static WINDOW: Cell<HWND> = const { Cell::new(std::ptr::null_mut()) };
    static CLOSED: Cell<Option<Closed>> = const { Cell::new(None) };
    /// The window opens maximized at its first activation.
    static MAXIMIZE: Cell<bool> = const { Cell::new(false) };
    /// The start asked for an activated window (see [`started_to_activate`]).
    static ACTIVATE: Cell<bool> = const { Cell::new(true) };
}

/// The thread's CBT hook; removed by [`Hook::finish`].
pub struct Hook(HHOOK);

/// Install the hook on the UI thread before the window exists, with the
/// saved placement (none: Windows chooses the place).
pub fn install(saved: Option<crate::settings::WindowPlacement>) -> Result<Hook, String> {
    WANTED.set(saved.map(|saved| Wanted {
        position: saved.position(),
        size: (saved.width, saved.height),
        maximized: saved.maximized,
    }));
    ACTIVATE.set(started_to_activate());
    // SAFETY: `cbt` has the HOOKPROC signature; a null module with the
    // current thread's id installs a thread-local hook.
    let hook = unsafe {
        SetWindowsHookExW(
            WH_CBT,
            Some(cbt),
            std::ptr::null_mut(),
            GetCurrentThreadId(),
        )
    };
    if hook.is_null() {
        return Err("cannot watch the window's placement".to_owned());
    }
    Ok(Hook(hook))
}

impl Hook {
    /// Remove the hook and return where the window was when it closed.
    pub fn finish(self) -> Option<Closed> {
        // SAFETY: the handle came from SetWindowsHookExW on this thread.
        unsafe { UnhookWindowsHookEx(self.0) };
        CLOSED.take()
    }
}

/// `CBTProc`: place the WinUI window as it is created, read its placement
/// as it is destroyed; every event passes on.
unsafe extern "system" fn cbt(code: i32, wparam: WPARAM, lparam: LPARAM) -> LRESULT {
    let window = wparam as HWND;
    if code == HCBT_CREATEWND as i32 && WINDOW.get().is_null() && is_winui(window) {
        // SAFETY: for HCBT_CREATEWND lParam is a valid CBT_CREATEWNDW whose
        // CREATESTRUCTW the hook may change.
        let create = unsafe { &mut *(*(lparam as *mut CBT_CREATEWNDW)).lpcs };
        if create.style & WS_CHILD as i32 == 0 {
            WINDOW.set(window);
            if let Some(wanted) = WANTED.get() {
                // A window created at an explicit place keeps its created
                // size (Reactor's ResizeClient does not change it: observed
                // live), so it is created at the saved client size plus
                // its frame.
                if let Some(at) = wanted.position {
                    let frame = (create.style as u32, create.dwExStyle);
                    (create.x, create.y, create.cx, create.cy) = place(at, wanted.size, frame);
                }
                MAXIMIZE.set(wanted.maximized);
            }
        }
    } else if code == HCBT_ACTIVATE as i32 && window == WINDOW.get() && MAXIMIZE.take() {
        maximize(window);
    } else if code == HCBT_DESTROYWND as i32 && !window.is_null() && window == WINDOW.get() {
        CLOSED.set(placement(window));
    }
    // SAFETY: passes the event on unchanged.
    unsafe { CallNextHookEx(std::ptr::null_mut(), code, wparam, lparam) }
}

/// Whether the starter asked for an activated window (Microsoft Learn:
/// `STARTUPINFOW`, `ShowWindow`): a show command that is minimized, hidden
/// or without activation did not.
fn started_to_activate() -> bool {
    let mut startup = STARTUPINFOW {
        cb: size_of::<STARTUPINFOW>() as u32,
        ..Default::default()
    };
    // SAFETY: cb is set; the structure is written in place.
    unsafe { GetStartupInfoW(&mut startup) };
    startup.dwFlags & STARTF_USESHOWWINDOW == 0
        || !matches!(
            i32::from(startup.wShowWindow),
            SW_HIDE
                | SW_SHOWMINIMIZED
                | SW_SHOWNOACTIVATE
                | SW_MINIMIZE
                | SW_SHOWMINNOACTIVE
                | SW_SHOWNA
        )
}

/// Programmatic keyboard focus may go to a control now: the start asked for
/// an activated window, or the window is the foreground window already.
pub fn may_take_focus() -> bool {
    let window = WINDOW.get();
    // SAFETY: a plain query.
    ACTIVATE.get() || (!window.is_null() && unsafe { GetForegroundWindow() } == window)
}

/// Whether the window is maximized now (on the UI thread).
pub fn maximized() -> bool {
    let window = WINDOW.get();
    // SAFETY: a window this thread created, or null (then not maximized).
    !window.is_null() && unsafe { IsZoomed(window) } != 0
}

/// Maximize the window, or, while its start-up show keeps it minimized,
/// make its restore maximized (`WPF_RESTORETOMAXIMIZED`, valid only with
/// `SW_SHOWMINIMIZED`).
fn maximize(window: HWND) {
    let mut placement = WINDOWPLACEMENT {
        length: size_of::<WINDOWPLACEMENT>() as u32,
        ..Default::default()
    };
    // SAFETY: a live window of this thread; length is set.
    if unsafe { GetWindowPlacement(window, &mut placement) } == 0 {
        return;
    }
    // SAFETY: as above.
    if unsafe { IsIconic(window) } != 0 {
        placement.showCmd = SW_SHOWMINIMIZED as u32;
        placement.flags |= WPF_RESTORETOMAXIMIZED;
    } else {
        placement.showCmd = SW_SHOWMAXIMIZED as u32;
    }
    // SAFETY: a placement read from this window.
    unsafe { SetWindowPlacement(window, &placement) };
}

fn is_winui(window: HWND) -> bool {
    let mut class = [0u16; 64];
    // SAFETY: the buffer and its length match.
    let length = unsafe { GetClassNameW(window, class.as_mut_ptr(), class.len() as i32) };
    length > 0
        && String::from_utf16_lossy(&class[..length as usize])
            == crate::app_resources::WINUI_WINDOW_CLASS
}

/// The outer rectangle (x, y, width, height) for a saved top-left and
/// client size. The attached monitor nearest to the saved rectangle is the
/// one the window lands on: the client is converted with that monitor's
/// DPI, the frame of the window's `(style, extended style)` is added, and
/// the result is kept inside that monitor's work area. The settings check
/// bounds the saved values; the math saturates as a defence.
fn place(
    (x, y): (i32, i32),
    (width, height): (u32, u32),
    (style, ex_style): (u32, u32),
) -> (i32, i32, i32, i32) {
    let dips = |value: u32| i32::try_from(value).unwrap_or(i32::MAX);
    // The landing monitor, from the saved rectangle at 96 DPI.
    let estimate = RECT {
        left: x,
        top: y,
        right: x.saturating_add(dips(width)),
        bottom: y.saturating_add(dips(height)),
    };
    // SAFETY: valid rectangle; NEAREST always returns a monitor.
    let monitor = unsafe { MonitorFromRect(&estimate, MONITOR_DEFAULTTONEAREST) };
    let (mut dpi, mut dpi_y) = (96, 96);
    // SAFETY: valid monitor and out-pointers.
    unsafe { GetDpiForMonitor(monitor, MDT_EFFECTIVE_DPI, &mut dpi, &mut dpi_y) };
    let dpi = dpi.max(96);
    let pixels = |value: u32| {
        (f64::from(value) * f64::from(dpi) / 96.0)
            .round()
            .min(f64::from(i32::MAX)) as i32
    };
    // The frame, measured on a client rectangle at the origin.
    let mut outer = RECT {
        left: 0,
        top: 0,
        right: pixels(width),
        bottom: pixels(height),
    };
    // SAFETY: a valid rectangle; on failure it stays the client rectangle.
    unsafe { AdjustWindowRectExForDpi(&mut outer, style, 0, ex_style, dpi) };
    let width = outer.right.saturating_sub(outer.left).max(1);
    let height = outer.bottom.saturating_sub(outer.top).max(1);
    let Some(info) = monitor_info(monitor) else {
        return (x, y, width, height);
    };
    let work = info.rcWork;
    let width = width.min(work.right.saturating_sub(work.left));
    let height = height.min(work.bottom.saturating_sub(work.top));
    (
        x.clamp(work.left, work.right.saturating_sub(width).max(work.left)),
        y.clamp(work.top, work.bottom.saturating_sub(height).max(work.top)),
        width,
        height,
    )
}

fn monitor_info(monitor: windows_sys::Win32::Graphics::Gdi::HMONITOR) -> Option<MONITORINFO> {
    let mut info = MONITORINFO {
        cbSize: size_of::<MONITORINFO>() as u32,
        ..Default::default()
    };
    // SAFETY: cbSize is set; the monitor handle came from the system.
    (unsafe { GetMonitorInfoW(monitor, &mut info) } != 0).then_some(info)
}

/// The window's normal top-left in screen pixels and whether it is (or,
/// minimized, restores to) maximized.
fn placement(window: HWND) -> Option<Closed> {
    let mut placement = WINDOWPLACEMENT {
        length: size_of::<WINDOWPLACEMENT>() as u32,
        ..Default::default()
    };
    // SAFETY: the window still exists during HCBT_DESTROYWND; length is set.
    if unsafe { GetWindowPlacement(window, &mut placement) } == 0 {
        return None;
    }
    let normal = placement.rcNormalPosition;
    // rcNormalPosition is in workspace coordinates: offset by the work
    // area's place on its monitor (a taskbar at the left or top).
    // SAFETY: valid rectangle; NEAREST always returns a monitor.
    let monitor = unsafe { MonitorFromRect(&normal, MONITOR_DEFAULTTONEAREST) };
    let (dx, dy) = monitor_info(monitor).map_or((0, 0), |info| {
        (
            info.rcWork.left - info.rcMonitor.left,
            info.rcWork.top - info.rcMonitor.top,
        )
    });
    let show = placement.showCmd as i32;
    Some(Closed {
        x: normal.left + dx,
        y: normal.top + dy,
        maximized: show == SW_SHOWMAXIMIZED
            || (show == SW_SHOWMINIMIZED && placement.flags & WPF_RESTORETOMAXIMIZED != 0),
    })
}
