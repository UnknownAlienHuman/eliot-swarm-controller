//! The design's press-point ripple ("Ripple from the press point",
//! `System.dc.html`), focus within a sidebar row, and the window's content
//! root that `crate::flyout` finds its anchors under: WinUI calls Reactor
//! 0.100.0 does not expose.
//!
//! - **Ripple.** The button template (`app_resources::templates`) grows its
//!   `Ripple` ellipse from the ellipse's `RenderTransformOrigin` when the
//!   press state starts. One `PointerPressed` handler on the window's XAML
//!   root, added with `handledEventsToo` (a Button marks its own presses
//!   handled), moves that origin to the press point inside the pressed
//!   button before the press animation renders its first frame. The root
//!   is the `XamlRoot.Content` of the first element that takes focus
//!   (`FocusManager.GotFocus`): Reactor hands out no native element.
//! - **Focus within a sidebar row.** The same `GotFocus` records the
//!   focused element and tells the window, which finds the row holding it
//!   (automation id [`ROW_ID_PREFIX`], [`resolve_focused_row`]) so the row
//!   shows its "⋯" to the keyboard as to the pointer; Shift+F10 or the
//!   context-menu key opens its menu at the row ([`focused_row_anchor`]).
//! - **Elements by automation id.** [`find`] walks the content root
//!   breadth-first for an automation id; [`element_bottom_left`] places a
//!   menu under an element.
//!
//! Every call fails quietly: without the root the ripple grows from the
//! centre and nothing is found. All of it runs on the UI thread.
//!
//! Interface ids and vtable slots are those of `Microsoft.UI.Xaml.winmd`
//! (WinUI 2.3.0) and `Microsoft.UI.winmd` (Windows App SDK 2.1.3), read
//! with System.Reflection.Metadata and checked against Reactor's generated
//! bindings where Reactor declares the same interface; each vtable below
//! asserts its slot offsets.
#![allow(
    unsafe_code,
    reason = "Windows pointer motion requires the native input API."
)]

use std::cell::RefCell;
use std::collections::VecDeque;
use std::mem::offset_of;

use windows_core::imp::ConstBuffer;
use windows_core::{GUID, HRESULT, HSTRING, IInspectable, IUnknown, Interface, RuntimeName};
use windows_reference::IReference;

use crate::ui::tokens::layout;
use crate::winrt::{
    DelegateVtbl, Getter, InspectableSlots, Raw, borrowed, delegate, delegate_vtable, get,
    winrt_interface,
};

/// The automation id prefix of a sidebar row's container, followed by the
/// row's key (`agent:<id>`, `project:<key>`; `ui::sidebar_ui`).
pub const ROW_ID_PREFIX: &str = "eliot-row:";

/// The button template's ripple ellipse (`app_resources::templates`).
const RIPPLE_NAME: &str = "Ripple";
const BUTTON_CLASS: &str = "Microsoft.UI.Xaml.Controls.Button";
/// How far up from the pressed element a Button is looked for.
const MAX_ANCESTORS: usize = 24;
/// How deep under the Button its ripple is looked for.
const MAX_TEMPLATE_DEPTH: usize = 8;
/// The most elements one search by automation id visits.
const MAX_SEARCH: usize = 6000;

/// `Windows.Foundation.Point`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Point {
    x: f32,
    y: f32,
}

const SLOT: usize = size_of::<usize>();

#[repr(C)]
struct FocusManagerStaticsVtbl {
    base: InspectableSlots,
    /// 6 `add_GotFocus(EventHandler<FocusManagerGotFocusEventArgs>)`.
    add_got_focus: unsafe extern "system" fn(Raw, Raw, *mut i64) -> HRESULT,
}

#[repr(C)]
struct GotFocusArgsVtbl {
    base: InspectableSlots,
    /// 6 `get_NewFocusedElement`.
    new_focused_element: Getter,
}

#[repr(C)]
struct UIElementVtbl {
    base: InspectableSlots,
    _6_19: [usize; 14],
    /// 20 `put_RenderTransformOrigin`.
    put_render_transform_origin: unsafe extern "system" fn(Raw, Point) -> HRESULT,
    _21_108: [usize; 88],
    /// 109 `get_XamlRoot`.
    xaml_root: Getter,
    _110_214: [usize; 105],
    /// 215 `AddHandler(RoutedEvent, Object, Boolean)`.
    add_handler: unsafe extern "system" fn(Raw, Raw, Raw, bool) -> HRESULT,
    _216: usize,
    /// 217 `TransformToVisual(UIElement)`.
    transform_to_visual: unsafe extern "system" fn(Raw, Raw, *mut Raw) -> HRESULT,
}
const _: () = {
    assert!(offset_of!(UIElementVtbl, put_render_transform_origin) == 20 * SLOT);
    assert!(offset_of!(UIElementVtbl, xaml_root) == 109 * SLOT);
    assert!(offset_of!(UIElementVtbl, add_handler) == 215 * SLOT);
    assert!(offset_of!(UIElementVtbl, transform_to_visual) == 217 * SLOT);
};

#[repr(C)]
struct GeneralTransformVtbl {
    base: InspectableSlots,
    _6: usize,
    /// 7 `TransformPoint(Point)`.
    transform_point: unsafe extern "system" fn(Raw, Point, *mut Point) -> HRESULT,
}
const _: () = assert!(offset_of!(GeneralTransformVtbl, transform_point) == 7 * SLOT);

#[repr(C)]
struct UIElementStaticsVtbl {
    base: InspectableSlots,
    _6_8: [usize; 3],
    /// 9 `get_PointerPressedEvent`.
    pointer_pressed_event: Getter,
}
const _: () = assert!(offset_of!(UIElementStaticsVtbl, pointer_pressed_event) == 9 * SLOT);

#[repr(C)]
struct XamlRootVtbl {
    base: InspectableSlots,
    /// 6 `get_Content`.
    content: Getter,
}

#[repr(C)]
struct RoutedEventArgsVtbl {
    base: InspectableSlots,
    /// 6 `get_OriginalSource`.
    original_source: Getter,
}

#[repr(C)]
struct PointerRoutedEventArgsVtbl {
    base: InspectableSlots,
    _6_10: [usize; 5],
    /// 11 `GetCurrentPoint(UIElement)`.
    get_current_point: unsafe extern "system" fn(Raw, Raw, *mut Raw) -> HRESULT,
}
const _: () = assert!(offset_of!(PointerRoutedEventArgsVtbl, get_current_point) == 11 * SLOT);

#[repr(C)]
struct PointerPointVtbl {
    base: InspectableSlots,
    _6_9: [usize; 4],
    /// 10 `get_Position`.
    position: unsafe extern "system" fn(Raw, *mut Point) -> HRESULT,
}
const _: () = assert!(offset_of!(PointerPointVtbl, position) == 10 * SLOT);

#[repr(C)]
struct VisualTreeHelperStaticsVtbl {
    base: InspectableSlots,
    _6_9: [usize; 4],
    /// 10 `GetChild(DependencyObject, Int32)`.
    child: unsafe extern "system" fn(Raw, Raw, i32, *mut Raw) -> HRESULT,
    /// 11 `GetChildrenCount(DependencyObject)`.
    children_count: unsafe extern "system" fn(Raw, Raw, *mut i32) -> HRESULT,
    /// 12 `GetParent(DependencyObject)`.
    parent: unsafe extern "system" fn(Raw, Raw, *mut Raw) -> HRESULT,
}
const _: () = assert!(offset_of!(VisualTreeHelperStaticsVtbl, child) == 10 * SLOT);

#[repr(C)]
struct FrameworkElementVtbl {
    base: InspectableSlots,
    _6_12: [usize; 7],
    /// 13 `get_ActualWidth`.
    actual_width: unsafe extern "system" fn(Raw, *mut f64) -> HRESULT,
    /// 14 `get_ActualHeight`.
    actual_height: unsafe extern "system" fn(Raw, *mut f64) -> HRESULT,
    _15_32: [usize; 18],
    /// 33 `get_Name`.
    name: unsafe extern "system" fn(Raw, *mut HSTRING) -> HRESULT,
}
const _: () = {
    assert!(offset_of!(FrameworkElementVtbl, actual_width) == 13 * SLOT);
    assert!(offset_of!(FrameworkElementVtbl, name) == 33 * SLOT);
};

#[repr(C)]
struct AutomationPropertiesStaticsVtbl {
    base: InspectableSlots,
    _6_12: [usize; 7],
    /// 13 `GetAutomationId(DependencyObject)`.
    automation_id: unsafe extern "system" fn(Raw, Raw, *mut HSTRING) -> HRESULT,
}
const _: () = assert!(offset_of!(AutomationPropertiesStaticsVtbl, automation_id) == 13 * SLOT);

/// `DependencyObject` parameters take this interface's pointer (the ABI
/// passes a class by its default interface); no slot is called on it.
#[repr(C)]
struct DependencyObjectVtbl {
    base: InspectableSlots,
}

winrt_interface!(
    IDependencyObject,
    DependencyObjectVtbl,
    GUID::from_u128(0xe7beaee7_160e_50f7_8789_d63463f979fa)
);
winrt_interface!(
    IFocusManagerStatics,
    FocusManagerStaticsVtbl,
    GUID::from_u128(0xe73dce04_e23a_5fb3_96ab_7df04c51dff2)
);
winrt_interface!(
    IFocusManagerGotFocusEventArgs,
    GotFocusArgsVtbl,
    GUID::from_u128(0x50aca341_4519_59cf_83b1_c9c45cfdb816)
);
winrt_interface!(
    IUIElement,
    UIElementVtbl,
    GUID::from_u128(0xc3c01020_320c_5cf6_9d24_d396bbfa4d8b)
);
winrt_interface!(
    IUIElementStatics,
    UIElementStaticsVtbl,
    GUID::from_u128(0xd2921d87_3584_5e22_8a3a_c2c78dab4f6e)
);
winrt_interface!(
    IXamlRoot,
    XamlRootVtbl,
    GUID::from_u128(0x60cb215a_ad15_520a_8b01_4416824f0441)
);
winrt_interface!(
    IRoutedEventArgs,
    RoutedEventArgsVtbl,
    GUID::from_u128(0x0908c407_1c7d_5de3_9c50_d971c62ec8ec)
);
winrt_interface!(
    IPointerRoutedEventArgs,
    PointerRoutedEventArgsVtbl,
    GUID::from_u128(0x66e78a9a_1bec_5f92_b1a1_ea6334ee511c)
);
winrt_interface!(
    IPointerPoint,
    PointerPointVtbl,
    GUID::from_u128(0x0d430ee6_252c_59a4_b2a2_d44264dc6a40)
);
winrt_interface!(
    IVisualTreeHelperStatics,
    VisualTreeHelperStaticsVtbl,
    GUID::from_u128(0x5aece43c_7651_5bb5_855c_2198496e455e)
);
winrt_interface!(
    IFrameworkElement,
    FrameworkElementVtbl,
    GUID::from_u128(0xfe08f13d_dc6a_5495_ad44_c2d8d21863b0)
);
winrt_interface!(
    IAutomationPropertiesStatics,
    AutomationPropertiesStaticsVtbl,
    GUID::from_u128(0xb1e3e0f3_112f_5966_87dc_7862d4ad50e5)
);
winrt_interface!(
    IGeneralTransform,
    GeneralTransformVtbl,
    GUID::from_u128(0x04eedeeb_31e5_54c0_ae3f_8bd06645d339)
);
// `EventHandler<FocusManagerGotFocusEventArgs>`: a parameterized id.
winrt_interface!(
    GotFocusHandler,
    DelegateVtbl,
    GUID::from_signature(ConstBuffer::from_slice(
        b"pinterface({9de1c535-6ae1-11e0-84e1-18a905bcc53f};\
          rc(Microsoft.UI.Xaml.Input.FocusManagerGotFocusEventArgs;\
          {50aca341-4519-59cf-83b1-c9c45cfdb816}))"
    ))
);
winrt_interface!(
    PointerEventHandler,
    DelegateVtbl,
    GUID::from_u128(0xa48a71e1_8bb4_5597_9e31_903a3f6a04fb)
);

// `AddHandler` takes the handler as an Object: a delegate is boxed as an
// `IReference<PointerEventHandler>`, which needs its WinRT signature.
impl windows_core::RuntimeType for PointerEventHandler {
    const SIGNATURE: ConstBuffer =
        ConstBuffer::from_slice(b"delegate({a48a71e1-8bb4-5597-9e31-903a3f6a04fb})");
}

macro_rules! runtime_class {
    ($name:ident, $class:literal) => {
        struct $name;
        impl RuntimeName for $name {
            const NAME: &'static str = $class;
        }
    };
}
runtime_class!(FocusManager, "Microsoft.UI.Xaml.Input.FocusManager");
runtime_class!(UIElement, "Microsoft.UI.Xaml.UIElement");
runtime_class!(VisualTreeHelper, "Microsoft.UI.Xaml.Media.VisualTreeHelper");
runtime_class!(
    AutomationProperties,
    "Microsoft.UI.Xaml.Automation.AutomationProperties"
);

static GOT_FOCUS_VTABLE: DelegateVtbl = delegate_vtable::<GotFocusHandler>();
static POINTER_VTABLE: DelegateVtbl = delegate_vtable::<PointerEventHandler>();

#[derive(Default)]
struct State {
    /// Kept for the window's life: WinUI holds its own reference.
    got_focus: Option<GotFocusHandler>,
    /// The XAML root the pointer handler is on.
    root: Option<IUIElement>,
    /// The element that took focus last (resolved by the window).
    focused: Option<IUIElement>,
    /// The sidebar row holding the focused element: its key and container.
    focused_row: Option<(String, IUIElement)>,
}

thread_local! {
    static STATE: RefCell<State> = RefCell::default();
}

/// Listen for the first focus to find the window's XAML root. Called once
/// on the UI thread before the window shows.
pub fn install() -> Result<(), String> {
    let run = || -> windows_core::Result<()> {
        let statics = windows_core::factory::<FocusManager, IFocusManagerStatics>()?;
        let handler: GotFocusHandler = delegate(&GOT_FOCUS_VTABLE, on_got_focus);
        let mut token = 0i64;
        // SAFETY: slot 6 of IFocusManagerStatics is add_GotFocus; WinUI
        // takes its own reference to the delegate.
        unsafe { (statics.vtable().add_got_focus)(statics.as_raw(), handler.as_raw(), &mut token) }
            .ok()?;
        STATE.with(|state| state.borrow_mut().got_focus = Some(handler));
        Ok(())
    };
    run().map_err(|error| format!("motion: {error} ({:#010X})", error.code().0))
}

fn on_got_focus(_sender: Raw, args: Raw) {
    let Some(element) = (|| -> Option<IUIElement> {
        let args: IFocusManagerGotFocusEventArgs = borrowed(args)?;
        // SAFETY: slot 6 of IFocusManagerGotFocusEventArgs is get_NewFocusedElement.
        unsafe { get(&args, args.vtable().new_focused_element) }
            .ok()?
            .cast()
            .ok()
    })() else {
        return;
    };
    let root = (|| -> Option<IUIElement> {
        // SAFETY: slot 109 of IUIElement is get_XamlRoot.
        let xaml_root: IXamlRoot = unsafe { get(&element, element.vtable().xaml_root) }
            .ok()?
            .cast()
            .ok()?;
        // SAFETY: slot 6 of IXamlRoot is get_Content.
        unsafe { get(&xaml_root, xaml_root.vtable().content) }
            .ok()?
            .cast()
            .ok()
    })();
    // The row is resolved later, by the window ([`resolve_focused_row`]):
    // walking the tree inside WinUI's own focus dispatch faults inside WinUI
    // at start (observed live), so the handler only records the element.
    STATE.with(|state| state.borrow_mut().focused = Some(element));
    crate::shortcuts::focus_moved();
    let Some(root) = root else { return };
    if STATE.with(|state| state.borrow().root.as_ref() != Some(&root))
        && add_pointer_handler(&root).is_ok()
    {
        STATE.with(|state| state.borrow_mut().root = Some(root));
    }
}

/// The sidebar row holding the last focused element, worked out from the
/// window's update (outside WinUI's focus dispatch): its key, or `None`.
/// The walk stops at the window's content root.
pub fn resolve_focused_row() -> Option<String> {
    let (focused, root) = STATE.with(|state| {
        let state = state.borrow();
        (state.focused.clone(), state.root.clone())
    });
    let found = (|| -> Option<(String, IUIElement)> {
        let tree = tree()?;
        let statics =
            windows_core::factory::<AutomationProperties, IAutomationPropertiesStatics>().ok()?;
        let top: IUnknown = root?.cast().ok()?;
        let mut node: IInspectable = focused?.cast().ok()?;
        for _ in 0..MAX_ANCESTORS {
            if node.cast::<IUnknown>().ok()? == top {
                return None;
            }
            if let Some(id) = automation_id(&statics, &node)
                && let Some(key) = id.to_string_lossy().strip_prefix(ROW_ID_PREFIX)
            {
                return Some((key.to_owned(), node.cast().ok()?));
            }
            node = parent(&tree, &node)?;
        }
        None
    })();
    crate::shortcuts::set_row_focused(found.is_some());
    let key = found.as_ref().map(|(key, _)| key.clone());
    STATE.with(|state| state.borrow_mut().focused_row = found);
    key
}

/// The focused sidebar row's key and the point under its start where its
/// menu opens (window DIPs), for Shift+F10 and the context-menu key.
pub fn focused_row_anchor() -> Option<(String, f64, f64)> {
    let (key, row) = STATE.with(|state| state.borrow().focused_row.clone())?;
    let (x, y) = bottom_left(&row)?;
    Some((key, x, y))
}

/// The window's content root (`XamlRoot.Content`), once an element took
/// focus.
pub fn root() -> Option<IInspectable> {
    STATE.with(|state| state.borrow().root.clone())?.cast().ok()
}

/// The element with automation id `id`, breadth-first from the content
/// root. Nothing is kept once it is found.
pub fn find(id: &str) -> Option<IInspectable> {
    let tree = tree()?;
    let statics =
        windows_core::factory::<AutomationProperties, IAutomationPropertiesStatics>().ok()?;
    let mut queue = VecDeque::from([root()?]);
    let mut visited = 0;
    while let Some(node) = queue.pop_front() {
        visited += 1;
        if visited > MAX_SEARCH {
            return None;
        }
        for child in children(&tree, &node) {
            if automation_id(&statics, &child).is_some_and(|found| found == id) {
                return Some(child);
            }
            queue.push_back(child);
        }
    }
    None
}

/// The point under the start of the element with automation id `id`, in
/// the content root's DIPs (where its menu opens).
pub fn element_bottom_left(id: &str) -> Option<(f64, f64)> {
    bottom_left(&find(id)?.cast().ok()?)
}

/// The bottom-left corner of `element` in the content root's DIPs.
fn bottom_left(element: &IUIElement) -> Option<(f64, f64)> {
    let frame: IFrameworkElement = element.cast().ok()?;
    let mut height = 0.0;
    let mut transform = std::ptr::null_mut();
    let mut point = Point::default();
    // SAFETY: slot 14 of IFrameworkElement is get_ActualHeight; slot 217 of
    // IUIElement is TransformToVisual (null: to the root), which returns
    // one owned GeneralTransform; slot 7 of IGeneralTransform is
    // TransformPoint.
    unsafe {
        (frame.vtable().actual_height)(frame.as_raw(), &mut height)
            .ok()
            .ok()?;
        (element.vtable().transform_to_visual)(
            element.as_raw(),
            std::ptr::null_mut(),
            &mut transform,
        )
        .ok()
        .ok()?;
        if transform.is_null() {
            return None;
        }
        let transform: IGeneralTransform = IInspectable::from_raw(transform).cast().ok()?;
        (transform.vtable().transform_point)(
            transform.as_raw(),
            Point {
                x: 0.0,
                y: height as f32,
            },
            &mut point,
        )
        .ok()
        .ok()?;
    }
    Some((f64::from(point.x), f64::from(point.y)))
}

fn add_pointer_handler(root: &IUIElement) -> windows_core::Result<()> {
    let statics = windows_core::factory::<UIElement, IUIElementStatics>()?;
    // SAFETY: slot 9 of IUIElementStatics is get_PointerPressedEvent.
    let event = unsafe { get(&statics, statics.vtable().pointer_pressed_event) }?;
    let handler: PointerEventHandler = delegate(&POINTER_VTABLE, on_pointer_pressed);
    let boxed: IInspectable = IReference::<PointerEventHandler>::from(handler).into();
    // SAFETY: slot 215 of IUIElement is AddHandler(RoutedEvent, Object,
    // Boolean); WinUI takes its own reference to the boxed handler.
    unsafe { (root.vtable().add_handler)(root.as_raw(), event.as_raw(), boxed.as_raw(), true) }.ok()
}

fn tree() -> Option<IVisualTreeHelperStatics> {
    windows_core::factory::<VisualTreeHelper, IVisualTreeHelperStatics>().ok()
}

/// `of` as the `IDependencyObject` pointer a `DependencyObject` parameter
/// takes (an object's `IInspectable` pointer is a different vtable).
fn dependency(of: &IInspectable) -> Option<IDependencyObject> {
    of.cast().ok()
}

fn parent(tree: &IVisualTreeHelperStatics, of: &IInspectable) -> Option<IInspectable> {
    let of = dependency(of)?;
    let mut out = std::ptr::null_mut();
    // SAFETY: slot 12 of IVisualTreeHelperStatics is GetParent(DependencyObject);
    // it returns one owned reference or null.
    unsafe { (tree.vtable().parent)(tree.as_raw(), of.as_raw(), &mut out) }
        .ok()
        .ok()?;
    // SAFETY: as above.
    (!out.is_null()).then(|| unsafe { IInspectable::from_raw(out) })
}

fn children(tree: &IVisualTreeHelperStatics, of: &IInspectable) -> Vec<IInspectable> {
    let Some(of) = dependency(of) else {
        return Vec::new();
    };
    let mut count = 0;
    // SAFETY: slot 11 of IVisualTreeHelperStatics is GetChildrenCount.
    if unsafe { (tree.vtable().children_count)(tree.as_raw(), of.as_raw(), &mut count) }.is_err() {
        return Vec::new();
    }
    (0..count)
        .filter_map(|index| {
            let mut out = std::ptr::null_mut();
            // SAFETY: slot 10 of IVisualTreeHelperStatics is GetChild; it
            // returns one owned reference.
            unsafe { (tree.vtable().child)(tree.as_raw(), of.as_raw(), index, &mut out) }
                .ok()
                .ok()?;
            // SAFETY: as above.
            (!out.is_null()).then(|| unsafe { IInspectable::from_raw(out) })
        })
        .collect()
}

fn name(of: &IInspectable) -> Option<HSTRING> {
    let element: IFrameworkElement = of.cast().ok()?;
    let mut out = HSTRING::new();
    // SAFETY: slot 33 of IFrameworkElement is get_Name; it writes one owned
    // handle into the empty, transparent HSTRING.
    unsafe { (element.vtable().name)(element.as_raw(), &mut out) }
        .ok()
        .ok()?;
    Some(out)
}

fn automation_id(statics: &IAutomationPropertiesStatics, of: &IInspectable) -> Option<HSTRING> {
    let of = dependency(of)?;
    let mut out = HSTRING::new();
    // SAFETY: slot 13 of IAutomationPropertiesStatics is GetAutomationId; it
    // writes one owned handle into the empty, transparent HSTRING.
    unsafe { (statics.vtable().automation_id)(statics.as_raw(), of.as_raw(), &mut out) }
        .ok()
        .ok()?;
    Some(out)
}

/// A press anywhere in the window: grow the pressed button's ripple from
/// the press point.
fn on_pointer_pressed(_sender: Raw, args: Raw) {
    let _ = (|| -> Option<()> {
        let pointer: IPointerRoutedEventArgs = borrowed(args)?;
        let routed: IRoutedEventArgs = pointer.cast().ok()?;
        // SAFETY: slot 6 of IRoutedEventArgs is get_OriginalSource.
        let source = unsafe { get(&routed, routed.vtable().original_source) }.ok()?;
        let tree = tree()?;
        let mut button = source;
        let mut steps = 0;
        while !button
            .GetRuntimeClassName()
            .is_ok_and(|class| class == BUTTON_CLASS)
        {
            steps += 1;
            if steps > MAX_ANCESTORS {
                return None;
            }
            button = parent(&tree, &button)?;
        }
        let ripple = find_under(&tree, &button, MAX_TEMPLATE_DEPTH, &|element| {
            name(element).is_some_and(|name| name == RIPPLE_NAME)
        })?;
        // The ripple overhangs its state layer by `RIPPLE_INSET` on every
        // side (the template's margin), so the press point inside the
        // state layer maps onto the ellipse's own box.
        let layer = parent(&tree, &ripple)?;
        let layer_element: IUIElement = layer.cast().ok()?;
        let mut point = std::ptr::null_mut();
        // SAFETY: slot 11 of IPointerRoutedEventArgs is GetCurrentPoint.
        unsafe {
            (pointer.vtable().get_current_point)(
                pointer.as_raw(),
                layer_element.as_raw(),
                &mut point,
            )
        }
        .ok()
        .ok()?;
        if point.is_null() {
            return None;
        }
        // SAFETY: GetCurrentPoint returned one owned PointerPoint.
        let point: IPointerPoint = unsafe { IInspectable::from_raw(point) }.cast().ok()?;
        let mut position = Point::default();
        // SAFETY: slot 10 of IPointerPoint is get_Position.
        unsafe { (point.vtable().position)(point.as_raw(), &mut position) }
            .ok()
            .ok()?;
        let frame: IFrameworkElement = layer.cast().ok()?;
        let (mut width, mut height) = (0.0, 0.0);
        // SAFETY: slots 13 and 14 of IFrameworkElement are the actual size.
        unsafe {
            (frame.vtable().actual_width)(frame.as_raw(), &mut width)
                .ok()
                .ok()?;
            (frame.vtable().actual_height)(frame.as_raw(), &mut height)
                .ok()
                .ok()?;
        }
        let inset = layout::RIPPLE_INSET;
        let origin = |at: f32, size: f64| {
            ((f64::from(at) + inset) / (size + 2.0 * inset)).clamp(0.0, 1.0) as f32
        };
        let origin = Point {
            x: origin(position.x, width),
            y: origin(position.y, height),
        };
        let ripple: IUIElement = ripple.cast().ok()?;
        // SAFETY: slot 20 of IUIElement is put_RenderTransformOrigin.
        unsafe { (ripple.vtable().put_render_transform_origin)(ripple.as_raw(), origin) }
            .ok()
            .ok()
    })();
}

/// The first element under `from` (depth-first, at most `depth` levels)
/// that `matches`.
fn find_under(
    tree: &IVisualTreeHelperStatics,
    from: &IInspectable,
    depth: usize,
    matches: &dyn Fn(&IInspectable) -> bool,
) -> Option<IInspectable> {
    if depth == 0 {
        return None;
    }
    children(tree, from).into_iter().find_map(|child| {
        if matches(&child) {
            Some(child)
        } else {
            find_under(tree, &child, depth - 1, matches)
        }
    })
}
