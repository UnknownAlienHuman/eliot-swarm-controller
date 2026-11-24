//! Real WinUI flyouts for the sidebar's menus and the composer's popovers
//! (owner: no menu or popover is painted inside the window). Reactor
//! 0.100.0 attaches a `Flyout` (`FlyoutExt::flyout_with`) or a `MenuFlyout`
//! (`MenuExt::menu`) to a Button and opens it on that button's click, but
//! cannot open it from code, open it at a point, close it, or say that it
//! closed. This module adds those calls on the flyout Reactor keeps on the
//! button:
//!
//! - [`show`] calls `FlyoutBase.ShowAt(FrameworkElement)` at the button
//!   itself (a popover anchored to its chip, at the flyout's placement).
//! - [`show_menu_at`] calls `MenuFlyout.ShowAt(UIElement, Point)` at a
//!   point of the window's content (a row's menu at the pointer).
//! - [`hide`] calls `FlyoutBase.Hide`.
//! - `FlyoutBase.Closed` (light dismiss, Esc, a chosen item, [`hide`])
//!   reaches the window as `Shortcut::FlyoutClosed` with the epoch the
//!   flyout was shown with, through the keyboard hook's channel.
//!
//! The button is found by its automation id under the window's content
//! root (`crate::motion::find`). Reactor replaces a `MenuFlyout` when its
//! items change and creates a `Flyout` when one is attached, so every call
//! reads the button's current `Flyout`. The calls run on the UI thread
//! after the render that attached the flyout committed; each one reports
//! what was missing instead of failing loudly.
//!
//! Interface ids and vtable slots are those of `Microsoft.UI.Xaml.winmd`
//! (WinUI 2.3.0), read with System.Reflection.Metadata: `IButton` slot 6
//! `get_Flyout`; `IFlyoutBase` slots 27 `get_IsOpen`, 32 `add_Closed`, 33
//! `remove_Closed`, 38 `ShowAt(FrameworkElement)` and 40 `Hide`;
//! `IMenuFlyout` slot 9
//! `ShowAt(UIElement, Point)`.
#![allow(
    unsafe_code,
    reason = "Opening and closing WinUI flyouts requires their native interfaces."
)]

use std::cell::RefCell;
use std::mem::offset_of;
use std::sync::atomic::{AtomicU64, Ordering};

use windows_core::imp::ConstBuffer;
use windows_core::{GUID, HRESULT, Interface};

use crate::winrt::{
    DelegateVtbl, Getter, InspectableSlots, Raw, borrowed, delegate, delegate_vtable, get,
    winrt_interface,
};

/// The automation id of the chip whose popover is open: only that chip
/// carries its `Flyout` (`ui::picker_ui`).
pub const POPOVER_ANCHOR: &str = "eliot-popover-anchor";

/// The automation id of the window's row-menu button, which carries the
/// sidebar row's `MenuFlyout` (`ui::menu_ui`).
pub const MENU_ANCHOR: &str = "eliot-menu-anchor";

const SLOT: usize = size_of::<usize>();

/// `Windows.Foundation.Point`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Point {
    x: f32,
    y: f32,
}

#[repr(C)]
struct ButtonVtbl {
    base: InspectableSlots,
    /// 6 `get_Flyout`.
    flyout: Getter,
}

#[repr(C)]
struct FlyoutBaseVtbl {
    base: InspectableSlots,
    _6_26: [usize; 21],
    /// 27 `get_IsOpen`.
    is_open: unsafe extern "system" fn(Raw, *mut bool) -> HRESULT,
    _28_31: [usize; 4],
    /// 32 `add_Closed(EventHandler<Object>)`.
    add_closed: unsafe extern "system" fn(Raw, Raw, *mut i64) -> HRESULT,
    /// 33 `remove_Closed`.
    remove_closed: unsafe extern "system" fn(Raw, i64) -> HRESULT,
    _34_37: [usize; 4],
    /// 38 `ShowAt(FrameworkElement)`.
    show_at: unsafe extern "system" fn(Raw, Raw) -> HRESULT,
    _39: usize,
    /// 40 `Hide`.
    hide: unsafe extern "system" fn(Raw) -> HRESULT,
}
const _: () = {
    assert!(offset_of!(FlyoutBaseVtbl, is_open) == 27 * SLOT);
    assert!(offset_of!(FlyoutBaseVtbl, add_closed) == 32 * SLOT);
    assert!(offset_of!(FlyoutBaseVtbl, remove_closed) == 33 * SLOT);
    assert!(offset_of!(FlyoutBaseVtbl, show_at) == 38 * SLOT);
    assert!(offset_of!(FlyoutBaseVtbl, hide) == 40 * SLOT);
};

#[repr(C)]
struct MenuFlyoutVtbl {
    base: InspectableSlots,
    _6_8: [usize; 3],
    /// 9 `ShowAt(UIElement, Point)`.
    show_at: unsafe extern "system" fn(Raw, Raw, Point) -> HRESULT,
}
const _: () = assert!(offset_of!(MenuFlyoutVtbl, show_at) == 9 * SLOT);

/// A class parameter takes this interface's pointer (the ABI passes a
/// class by its default interface); no slot is called on it.
#[repr(C)]
struct ParameterVtbl {
    base: InspectableSlots,
}

winrt_interface!(
    IButton,
    ButtonVtbl,
    GUID::from_u128(0x216c183d_d07a_5aa5_b8a4_0300a2683e87)
);
winrt_interface!(
    IFlyoutBase,
    FlyoutBaseVtbl,
    GUID::from_u128(0xbb6603bf_744d_5c31_a87d_744394634d77)
);
winrt_interface!(
    IMenuFlyout,
    MenuFlyoutVtbl,
    GUID::from_u128(0xf4c77c6c_1fa5_5d85_8559_5d02b7d4e5e7)
);
winrt_interface!(
    IFrameworkElement,
    ParameterVtbl,
    GUID::from_u128(0xfe08f13d_dc6a_5495_ad44_c2d8d21863b0)
);
winrt_interface!(
    IUIElement,
    ParameterVtbl,
    GUID::from_u128(0xc3c01020_320c_5cf6_9d24_d396bbfa4d8b)
);
// `EventHandler<Object>`: a parameterized id.
winrt_interface!(
    ClosedHandler,
    DelegateVtbl,
    GUID::from_signature(ConstBuffer::from_slice(
        b"pinterface({9de1c535-6ae1-11e0-84e1-18a905bcc53f};cinterface(IInspectable))"
    ))
);

static CLOSED_VTABLE: DelegateVtbl = delegate_vtable::<ClosedHandler>();

/// The last epoch handed out: every show takes a new one, so a `Closed`
/// that arrives after a later show is told apart.
static EPOCH: AtomicU64 = AtomicU64::new(0);

/// A flyout shown and not closed yet: its `Closed` registration and the
/// epoch it was shown with.
struct Watch {
    flyout: IFlyoutBase,
    token: i64,
    epoch: u64,
}

thread_local! {
    static WATCHED: RefCell<Vec<Watch>> = const { RefCell::new(Vec::new()) };
}

/// A new show epoch.
pub fn next_epoch() -> u64 {
    EPOCH.fetch_add(1, Ordering::SeqCst) + 1
}

/// Open the `Flyout` of the button with automation id `anchor` at that
/// button, at the flyout's placement; its `Closed` reports `epoch`. An
/// open flyout (its content switched in place) only takes the new epoch.
pub fn show(anchor: &str, epoch: u64) -> Result<(), String> {
    let (button, flyout) = anchored(anchor)?;
    let mut open = false;
    // SAFETY: slot 27 of IFlyoutBase is get_IsOpen.
    let _ = unsafe { (flyout.vtable().is_open)(flyout.as_raw(), &mut open) };
    if open {
        watch(flyout, epoch);
        return Ok(());
    }
    let target: IFrameworkElement = button.cast().map_err(|error| error.to_string())?;
    // SAFETY: slot 38 of IFlyoutBase is ShowAt(FrameworkElement).
    unsafe { (flyout.vtable().show_at)(flyout.as_raw(), target.as_raw()) }
        .ok()
        .map_err(|error| format!("ShowAt: {error}"))?;
    watch(flyout, epoch);
    Ok(())
}

/// Open the `MenuFlyout` of the button with automation id `anchor` at
/// (`x`, `y`) in the window content's DIPs; its `Closed` reports `epoch`.
pub fn show_menu_at(anchor: &str, x: f64, y: f64, epoch: u64) -> Result<(), String> {
    let (_, flyout) = anchored(anchor)?;
    let menu: IMenuFlyout = flyout.cast().map_err(|error| error.to_string())?;
    let root: IUIElement = crate::motion::root()
        .ok_or("no content root yet")?
        .cast()
        .map_err(|error| error.to_string())?;
    let point = Point {
        x: x as f32,
        y: y as f32,
    };
    // SAFETY: slot 9 of IMenuFlyout is ShowAt(UIElement, Point).
    unsafe { (menu.vtable().show_at)(menu.as_raw(), root.as_raw(), point) }
        .ok()
        .map_err(|error| format!("ShowAt: {error}"))?;
    watch(flyout, epoch);
    Ok(())
}

/// Close the flyout of the button with automation id `anchor`, if it is
/// open (nothing when the button or its flyout is gone).
pub fn hide(anchor: &str) {
    if let Ok((_, flyout)) = anchored(anchor) {
        // SAFETY: slot 40 of IFlyoutBase is Hide; closing a closed flyout
        // does nothing.
        let _ = unsafe { (flyout.vtable().hide)(flyout.as_raw()) };
    }
}

/// The button with automation id `anchor` and its current flyout.
fn anchored(anchor: &str) -> Result<(IButton, IFlyoutBase), String> {
    let button: IButton = crate::motion::find(anchor)
        .ok_or_else(|| format!("{anchor} not found"))?
        .cast()
        .map_err(|error| format!("{anchor} is no button: {error}"))?;
    // SAFETY: slot 6 of IButton is get_Flyout; null (no flyout) is an error.
    let flyout = unsafe { get(&button, button.vtable().flyout) }
        .map_err(|_| format!("{anchor} has no flyout"))?
        .cast()
        .map_err(|error| error.to_string())?;
    Ok((button, flyout))
}

/// Report `flyout`'s next `Closed` with `epoch` (a flyout shown again
/// keeps one registration and takes the new epoch).
fn watch(flyout: IFlyoutBase, epoch: u64) {
    let known = WATCHED.with(|watched| {
        let mut watched = watched.borrow_mut();
        watched
            .iter_mut()
            .find(|watch| watch.flyout == flyout)
            .map(|watch| watch.epoch = epoch)
            .is_some()
    });
    if known {
        return;
    }
    let handler: ClosedHandler = delegate(&CLOSED_VTABLE, on_closed);
    let mut token = 0i64;
    // SAFETY: slot 32 of IFlyoutBase is add_Closed; WinUI takes its own
    // reference to the delegate.
    if unsafe { (flyout.vtable().add_closed)(flyout.as_raw(), handler.as_raw(), &mut token) }
        .is_ok()
    {
        WATCHED.with(|watched| {
            watched.borrow_mut().push(Watch {
                flyout,
                token,
                epoch,
            });
        });
    }
}

/// A watched flyout closed: drop its registration and tell the window.
fn on_closed(sender: Raw, _args: Raw) {
    let Some(flyout) = borrowed::<IFlyoutBase>(sender) else {
        return;
    };
    let Some(watch) = WATCHED.with(|watched| {
        let mut watched = watched.borrow_mut();
        let at = watched.iter().position(|watch| watch.flyout == flyout)?;
        Some(watched.swap_remove(at))
    }) else {
        return;
    };
    // SAFETY: slot 33 of IFlyoutBase is remove_Closed with the token
    // add_Closed returned; WinUI allows it inside the event.
    let _ = unsafe { (watch.flyout.vtable().remove_closed)(watch.flyout.as_raw(), watch.token) };
    crate::shortcuts::flyout_closed(watch.epoch);
}
