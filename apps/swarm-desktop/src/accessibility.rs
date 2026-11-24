//! Windows accessibility settings the desktop follows: High Contrast
//! (SPI_GETHIGHCONTRAST) and animation effects (SPI_GETCLIENTAREAANIMATION).
//!
//! Reactor 0.100.0 reports the window's Light/Dark scheme but not High
//! Contrast, which decides whether the M3 palette applies at all (see
//! `theme::winui::Palette`), nor animation effects, which decide whether the
//! control templates move (`app_resources::templates`): WinUI stops its own
//! theme animations when the setting is off, not an app's storyboards. Like
//! the clipboard module this is a small Win32 boundary through the
//! workspace `windows-sys` dependency and allows `unsafe` for the read-only
//! system calls; it allocates nothing.
#![allow(
    unsafe_code,
    reason = "Accessibility settings are read through SystemParametersInfoW."
)]

use windows_sys::Win32::UI::Accessibility::{HCF_HIGHCONTRASTON, HIGHCONTRASTW};
use windows_sys::Win32::UI::WindowsAndMessaging::{
    SPI_GETCLIENTAREAANIMATION, SPI_GETHIGHCONTRAST, SystemParametersInfoW,
};
use windows_sys::core::BOOL;

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

/// Whether Windows plays animations now (Settings › Accessibility › Visual
/// effects › Animation effects). A failed query reads as on.
pub fn animations_on() -> bool {
    let mut on: BOOL = 1;
    // SAFETY: SPI_GETCLIENTAREAANIMATION writes one BOOL through the
    // pointer, which is valid for the call.
    let ok = unsafe {
        SystemParametersInfoW(
            SPI_GETCLIENTAREAANIMATION,
            0,
            (&mut on as *mut BOOL).cast(),
            0,
        )
    };
    ok == 0 || on != 0
}
