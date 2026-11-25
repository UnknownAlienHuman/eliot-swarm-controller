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
//!   flyout was shown with, through the keyboard hook's channel. The
//!   handler only enqueues; the window then drops the registrations of
//!   flyouts that are no longer open ([`release`]), as do [`hide`] and
//!   every show.
//! - [`MenuDress`] gives a menu what Reactor's text-only `MenuItem` cannot:
//!   an accessible name on the `MenuFlyout` (its presenter reads it) and,
//!   per item, a `FontIcon` with the item's glyph (Segoe Fluent Icons, the
//!   system symbol font) and, where the item shows more than its label
//!   (the current tags), its display text.
//!
//! The button is found by its automation id under the window's content
//! root (`crate::elements::find`). Reactor replaces a `MenuFlyout` when its
//! items change and creates a `Flyout` when one is attached, so every call
//! reads the button's current `Flyout`. The calls run on the UI thread
//! after the render that attached the flyout committed; each one reports
//! what was missing instead of failing loudly.
//!
//! Interface ids and vtable slots are those of `Microsoft.UI.Xaml.winmd`
//! (WinUI 2.3.0), read with System.Reflection.Metadata: `IButton` slot 6
//! `get_Flyout`; `IFlyoutBase` slots 27 `get_IsOpen`, 32 `add_Closed`, 33
//! `remove_Closed`, 38 `ShowAt(FrameworkElement)` and 40 `Hide`;
//! `IMenuFlyout` slots 6 `get_Items` and 9 `ShowAt(UIElement, Point)`;
//! `IVector<MenuFlyoutItemBase>` slots 6 `GetAt` and 7 `get_Size`;
//! `IMenuFlyoutItem` slots 6 `get_Text`, 12 `get_Icon` and 13 `put_Icon`;
//! `IMenuFlyoutSubItem` slots 6 `get_Items`, 7 `get_Text`, 9 `get_Icon` and
//! 10 `put_Icon`; `IFontIconFactory` slot 6 `CreateInstance`; `IFontIcon`
//! slot 7 `put_Glyph`; `IAutomationPropertiesStatics` slot 32 `SetName`.
#![allow(
    unsafe_code,
    reason = "Opening and closing WinUI flyouts requires their native interfaces."
)]

use std::cell::RefCell;
use std::mem::offset_of;
use std::sync::atomic::{AtomicU64, Ordering};

use windows_core::imp::ConstBuffer;
use windows_core::{GUID, HRESULT, HSTRING, IInspectable, Interface};

use crate::winrt::{
    DelegateVtbl, Getter, InspectableSlots, Raw, borrowed, delegate, delegate_vtable, get,
    runtime_class, winrt_interface,
};

/// The automation id of the chip whose popover is open: only that chip
/// carries its `Flyout` (`ui::picker_ui`).
pub const POPOVER_ANCHOR: &str = "eliot-popover-anchor";

/// The automation id of the window's row-menu button, which carries the
/// sidebar row's `MenuFlyout` (`ui::menu_ui`).
pub const MENU_ANCHOR: &str = "eliot-menu-anchor";

/// The automation id of the window's attach-chooser button, which carries
/// the "Attach to project…" chooser's `Flyout` while it is open
/// (`ui::menu_ui`).
pub const ATTACH_ANCHOR: &str = "eliot-attach-anchor";

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
    /// 6 `get_Items` (`IVector<MenuFlyoutItemBase>`).
    items: Getter,
    _7_8: [usize; 2],
    /// 9 `ShowAt(UIElement, Point)`.
    show_at: unsafe extern "system" fn(Raw, Raw, Point) -> HRESULT,
}
const _: () = {
    assert!(offset_of!(MenuFlyoutVtbl, items) == 6 * SLOT);
    assert!(offset_of!(MenuFlyoutVtbl, show_at) == 9 * SLOT);
};

/// `IVector<MenuFlyoutItemBase>`: a menu's or submenu's items.
#[repr(C)]
struct ItemVectorVtbl {
    base: InspectableSlots,
    /// 6 `GetAt(UInt32)`.
    get_at: unsafe extern "system" fn(Raw, u32, *mut Raw) -> HRESULT,
    /// 7 `get_Size`.
    size: unsafe extern "system" fn(Raw, *mut u32) -> HRESULT,
}
const _: () = {
    assert!(offset_of!(ItemVectorVtbl, get_at) == 6 * SLOT);
    assert!(offset_of!(ItemVectorVtbl, size) == 7 * SLOT);
};

/// A text getter slot: one owned handle into an empty `HSTRING`.
type TextGetter = unsafe extern "system" fn(Raw, *mut HSTRING) -> HRESULT;
/// An object setter slot (`put_Icon`, `put_Foreground`).
type Setter = unsafe extern "system" fn(Raw, Raw) -> HRESULT;

#[repr(C)]
struct MenuFlyoutItemVtbl {
    base: InspectableSlots,
    /// 6 `get_Text`.
    text: TextGetter,
    /// 7 `put_Text(String)`.
    put_text: Setter,
    _8_11: [usize; 4],
    /// 12 `get_Icon`.
    icon: Getter,
    /// 13 `put_Icon(IconElement)`.
    put_icon: Setter,
}
const _: () = {
    assert!(offset_of!(MenuFlyoutItemVtbl, text) == 6 * SLOT);
    assert!(offset_of!(MenuFlyoutItemVtbl, put_text) == 7 * SLOT);
    assert!(offset_of!(MenuFlyoutItemVtbl, icon) == 12 * SLOT);
    assert!(offset_of!(MenuFlyoutItemVtbl, put_icon) == 13 * SLOT);
};

#[repr(C)]
struct MenuFlyoutSubItemVtbl {
    base: InspectableSlots,
    /// 6 `get_Items`.
    items: Getter,
    /// 7 `get_Text`.
    text: TextGetter,
    _8: usize,
    /// 9 `get_Icon`.
    icon: Getter,
    /// 10 `put_Icon(IconElement)`.
    put_icon: Setter,
}
const _: () = {
    assert!(offset_of!(MenuFlyoutSubItemVtbl, items) == 6 * SLOT);
    assert!(offset_of!(MenuFlyoutSubItemVtbl, text) == 7 * SLOT);
    assert!(offset_of!(MenuFlyoutSubItemVtbl, icon) == 9 * SLOT);
    assert!(offset_of!(MenuFlyoutSubItemVtbl, put_icon) == 10 * SLOT);
};

#[repr(C)]
struct FontIconVtbl {
    base: InspectableSlots,
    _6: usize,
    /// 7 `put_Glyph(String)`.
    put_glyph: Setter,
}
const _: () = assert!(offset_of!(FontIconVtbl, put_glyph) == 7 * SLOT);

#[repr(C)]
struct FontIconFactoryVtbl {
    base: InspectableSlots,
    /// 6 `CreateInstance(Object outer, out Object inner)`.
    create: unsafe extern "system" fn(Raw, Raw, *mut Raw, *mut Raw) -> HRESULT,
}
const _: () = assert!(offset_of!(FontIconFactoryVtbl, create) == 6 * SLOT);

#[repr(C)]
struct AutomationNameVtbl {
    base: InspectableSlots,
    _6_31: [usize; 26],
    /// 32 `SetName(DependencyObject, String)`.
    set_name: unsafe extern "system" fn(Raw, Raw, Raw) -> HRESULT,
}
const _: () = assert!(offset_of!(AutomationNameVtbl, set_name) == 32 * SLOT);

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
winrt_interface!(
    IDependencyObject,
    ParameterVtbl,
    GUID::from_u128(0xe7beaee7_160e_50f7_8789_d63463f979fa)
);
// `IVector<MenuFlyoutItemBase>`: a parameterized id.
winrt_interface!(
    ItemVector,
    ItemVectorVtbl,
    GUID::from_signature(ConstBuffer::from_slice(
        b"pinterface({913337e9-11a1-4345-a3a2-4e7f956e222d};\
          rc(Microsoft.UI.Xaml.Controls.MenuFlyoutItemBase;\
          {4bee2715-44a1-5f94-86e8-02ddbe3dc6b9}))"
    ))
);
winrt_interface!(
    IMenuFlyoutItem,
    MenuFlyoutItemVtbl,
    GUID::from_u128(0x4252df5a_44f9_5ee8_b1cc_53de9aaa4095)
);
winrt_interface!(
    IMenuFlyoutSubItem,
    MenuFlyoutSubItemVtbl,
    GUID::from_u128(0x6b0688c1_47b0_53b5_b6f9_5ec5d6623b84)
);
winrt_interface!(
    IFontIcon,
    FontIconVtbl,
    GUID::from_u128(0x6eba5ed9_d233_5f5e_91a8_f5134292658a)
);
winrt_interface!(
    IFontIconFactory,
    FontIconFactoryVtbl,
    GUID::from_u128(0xaa9a24fe_bef8_564a_b200_694cd6f6ba4e)
);
winrt_interface!(
    IIconElement,
    ParameterVtbl,
    GUID::from_u128(0x18f69350_279e_50ea_8d23_138e717ed939)
);
winrt_interface!(
    IAutomationPropertiesStatics,
    AutomationNameVtbl,
    GUID::from_u128(0xb1e3e0f3_112f_5966_87dc_7862d4ad50e5)
);

runtime_class!(FontIcon, "Microsoft.UI.Xaml.Controls.FontIcon");
runtime_class!(
    AutomationProperties,
    "Microsoft.UI.Xaml.Automation.AutomationProperties"
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
    if is_open(&flyout) {
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

/// One menu item's look: the item labelled `label` shows `glyph`, and
/// `display` in place of its label when given (display only: its click still reports `label`).
pub struct MenuLook {
    pub label: &'static str,
    pub display: Option<String>,
    pub glyph: &'static str,
}

impl MenuLook {
    /// Whether a native item showing `text` is this one (before or after
    /// its display text replaced the label).
    fn shows(&self, text: &str) -> bool {
        self.label == text || self.display.as_deref() == Some(text)
    }
}

/// What [`show_menu_at`] gives a menu before it opens.
pub struct MenuDress<'a> {
    /// The menu's accessible name.
    pub name: &'a str,
    /// The items' looks, matched by their text (unique in one menu).
    pub items: &'a [MenuLook],
}

/// Open the `MenuFlyout` of the button with automation id `anchor` at
/// (`x`, `y`) in the window content's DIPs, dressed as `dress` says; its
/// `Closed` reports `epoch`. A menu that cannot be dressed still opens
/// (the failure is noted in `desktop.log`).
pub fn show_menu_at(
    anchor: &str,
    x: f64,
    y: f64,
    epoch: u64,
    dress: &MenuDress<'_>,
) -> Result<(), String> {
    let (_, flyout) = anchored(anchor)?;
    let menu: IMenuFlyout = flyout.cast().map_err(|error| error.to_string())?;
    if let Err(error) = dress_menu(&menu, &flyout, dress) {
        crate::startup::note(&format!("menu not dressed: {error}"));
    }
    let root: IUIElement = crate::elements::root()
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
    drop_closed(None);
}

/// Whether `flyout` is open (false when it cannot say).
fn is_open(flyout: &IFlyoutBase) -> bool {
    let mut open = false;
    // SAFETY: slot 27 of IFlyoutBase is get_IsOpen.
    let _ = unsafe { (flyout.vtable().is_open)(flyout.as_raw(), &mut open) };
    open
}

/// The button with automation id `anchor` and its current flyout.
fn anchored(anchor: &str) -> Result<(IButton, IFlyoutBase), String> {
    let button: IButton = crate::elements::find(anchor)
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

/// The accessible name of `flyout` (its presenter reads the owner's name)
/// and each item's icon.
fn dress_menu(
    menu: &IMenuFlyout,
    flyout: &IFlyoutBase,
    dress: &MenuDress<'_>,
) -> windows_core::Result<()> {
    let statics = windows_core::factory::<AutomationProperties, IAutomationPropertiesStatics>()?;
    let target: IDependencyObject = flyout.cast()?;
    let name = HSTRING::from(dress.name);
    // SAFETY: slot 32 of IAutomationPropertiesStatics is
    // SetName(DependencyObject, String); the handle is borrowed for the call.
    unsafe { (statics.vtable().set_name)(statics.as_raw(), target.as_raw(), abi(&name)) }.ok()?;
    // SAFETY: slot 6 of IMenuFlyout is get_Items.
    let items = unsafe { item_vector(menu.as_raw(), menu.vtable().items) }?;
    dress_items(&items, dress)
}

/// Dress `items` and every submenu under them.
fn dress_items(items: &ItemVector, dress: &MenuDress<'_>) -> windows_core::Result<()> {
    let mut size = 0u32;
    // SAFETY: slot 7 of IVector is get_Size.
    unsafe { (items.vtable().size)(items.as_raw(), &mut size) }.ok()?;
    for index in 0..size {
        let mut out = std::ptr::null_mut();
        // SAFETY: slot 6 of IVector is GetAt; it returns one owned item.
        unsafe { (items.vtable().get_at)(items.as_raw(), index, &mut out) }.ok()?;
        if out.is_null() {
            continue;
        }
        // SAFETY: as above.
        let item = unsafe { IInspectable::from_raw(out) };
        if let Ok(item) = item.cast::<IMenuFlyoutItem>() {
            // SAFETY: slots 6, 7, 12 and 13 of IMenuFlyoutItem are
            // get_Text, put_Text, get_Icon and put_Icon.
            let text = unsafe { text_of(item.as_raw(), item.vtable().text) }?;
            if let Some(look) = dress.items.iter().find(|look| look.shows(&text)) {
                if let Some(display) = look.display.as_deref().filter(|shown| *shown != text) {
                    let display = HSTRING::from(display);
                    unsafe { (item.vtable().put_text)(item.as_raw(), abi(&display)) }.ok()?;
                }
                if unsafe { get(&item, item.vtable().icon) }.is_err() {
                    let icon = font_icon(look.glyph)?;
                    unsafe { (item.vtable().put_icon)(item.as_raw(), icon.as_raw()) }.ok()?;
                }
            }
        } else if let Ok(sub) = item.cast::<IMenuFlyoutSubItem>() {
            // SAFETY: slots 6, 7, 9 and 10 of IMenuFlyoutSubItem are
            // get_Items, get_Text, get_Icon and put_Icon.
            let text = unsafe { text_of(sub.as_raw(), sub.vtable().text) }?;
            if let Some(look) = dress.items.iter().find(|look| look.shows(&text))
                && unsafe { get(&sub, sub.vtable().icon) }.is_err()
            {
                let icon = font_icon(look.glyph)?;
                unsafe { (sub.vtable().put_icon)(sub.as_raw(), icon.as_raw()) }.ok()?;
            }
            let nested = unsafe { item_vector(sub.as_raw(), sub.vtable().items) }?;
            dress_items(&nested, dress)?;
        }
    }
    Ok(())
}

/// A `FontIcon` showing `glyph` in the system symbol font.
fn font_icon(glyph: &str) -> windows_core::Result<IIconElement> {
    let factory = windows_core::factory::<FontIcon, IFontIconFactory>()?;
    let mut out = std::ptr::null_mut();
    // SAFETY: slot 6 of IFontIconFactory is CreateInstance(outer, inner);
    // with no outer object and no inner out-pointer it returns one owned
    // FontIcon, as Reactor's own bindings create its controls.
    unsafe {
        (factory.vtable().create)(
            factory.as_raw(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &mut out,
        )
    }
    .ok()?;
    if out.is_null() {
        return Err(windows_core::Error::empty());
    }
    // SAFETY: as above.
    let icon: IFontIcon = unsafe { IInspectable::from_raw(out) }.cast()?;
    let glyph = HSTRING::from(glyph);
    // SAFETY: slot 7 of IFontIcon is put_Glyph; the handle is borrowed.
    unsafe { (icon.vtable().put_glyph)(icon.as_raw(), abi(&glyph)) }.ok()?;
    icon.cast()
}

/// Call an `Items` getter, which returns the `IVector<MenuFlyoutItemBase>`
/// itself.
///
/// # Safety
/// `getter` must be the `get_Items` slot of the interface `this` points at.
unsafe fn item_vector(this: Raw, getter: Getter) -> windows_core::Result<ItemVector> {
    let mut out = std::ptr::null_mut();
    // SAFETY: the caller guarantees the slot; it returns one owned vector.
    unsafe { getter(this, &mut out) }.ok()?;
    if out.is_null() {
        return Err(windows_core::Error::empty());
    }
    // SAFETY: the getter returned exactly this interface, owned.
    Ok(unsafe { ItemVector::from_raw(out) })
}

/// Call a `get_Text` slot.
///
/// # Safety
/// `getter` must be a text getter slot of the interface `this` points at.
unsafe fn text_of(this: Raw, getter: TextGetter) -> windows_core::Result<String> {
    let mut out = HSTRING::new();
    // SAFETY: the caller guarantees the slot; it writes one owned handle
    // into the empty, transparent HSTRING.
    unsafe { getter(this, &mut out) }.ok()?;
    Ok(out.to_string_lossy())
}

/// A string parameter as the ABI passes it: the handle, borrowed for the
/// call (`text` outlives it).
fn abi(text: &HSTRING) -> Raw {
    // SAFETY: HSTRING is a transparent handle of pointer size.
    unsafe { std::mem::transmute_copy(text) }
}

/// Report `flyout`'s next `Closed` with `epoch` (a flyout shown again
/// keeps one registration and takes the new epoch). Registrations of other
/// flyouts that are no longer open are dropped first: a `ShowAt` that
/// returned OK but never opened would otherwise keep its `Closed` handler
/// for good.
fn watch(flyout: IFlyoutBase, epoch: u64) {
    drop_closed(Some(&flyout));
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

/// Drop the `Closed` registrations of flyouts that are no longer open: one
/// that closed, one Reactor replaced on its button, or one whose `ShowAt`
/// returned OK but never opened. `keep` is the flyout being shown now.
fn drop_closed(keep: Option<&IFlyoutBase>) {
    let stale: Vec<Watch> = WATCHED.with(|watched| {
        let mut watched = watched.borrow_mut();
        let (open, stale) = std::mem::take(&mut *watched)
            .into_iter()
            .partition(|watch| Some(&watch.flyout) == keep || is_open(&watch.flyout));
        *watched = open;
        stale
    });
    for watch in stale {
        // SAFETY: slot 33 of IFlyoutBase is remove_Closed with the token
        // add_Closed returned.
        let _ =
            unsafe { (watch.flyout.vtable().remove_closed)(watch.flyout.as_raw(), watch.token) };
    }
}

/// The window handled a flyout's `Closed`: registrations of flyouts that
/// are no longer open go now, outside WinUI's event.
pub fn release() {
    drop_closed(None);
}

/// A watched flyout closed: tell the window, with the epoch it was shown
/// with. The handler only reads the registry and enqueues; the window
/// drops the registration ([`release`]).
fn on_closed(sender: Raw, _args: Raw) {
    let Some(flyout) = borrowed::<IFlyoutBase>(sender) else {
        return;
    };
    let epoch = WATCHED.with(|watched| {
        watched
            .borrow()
            .iter()
            .find(|watch| watch.flyout == flyout)
            .map(|watch| watch.epoch)
    });
    if let Some(epoch) = epoch {
        crate::shortcuts::flyout_closed(epoch);
    }
}
