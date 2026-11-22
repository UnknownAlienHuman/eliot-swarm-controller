//! Windows High Contrast state (SPI_GETHIGHCONTRAST).
//!
//! Reactor 0.100.0 reports the window's Light/Dark scheme but not High
//! Contrast, which decides whether the M3 palette applies at all (see
//! `theme::winui::Palette`). Like the clipboard module this is a small Win32
//! boundary through the workspace `windows-sys` dependency and allows
//! `unsafe` for the one read-only system call; it allocates nothing.
#![allow(
    unsafe_code,
    reason = "UI Automation access requires COM interface calls."
)]

use windows_sys::Win32::UI::Accessibility::{HCF_HIGHCONTRASTON, HIGHCONTRASTW};
use windows_sys::Win32::UI::WindowsAndMessaging::{SPI_GETHIGHCONTRAST, SystemParametersInfoW};

/// Whether Windows High Contrast is on now. A failed query reads as off.
pub fn high_contrast_on() -> bool {
    let mut info = HIGHCONTRASTW {
        cbSize: std::mem::size_of::<HIGHCONTRASTW>() as u32,
        ..Default::default()
    };
    // SAFETY: `info` is a correctly sized HIGHCONTRASTW that the call fills;
    // the returned scheme-name pointer is system-owned and never read here.
    let ok = unsafe {
        SystemParametersInfoW(
            SPI_GETHIGHCONTRAST,
            info.cbSize,
            (&mut info as *mut HIGHCONTRASTW).cast(),
            0,
        )
    };
    ok != 0 && info.dwFlags & HCF_HIGHCONTRASTON != 0
}
