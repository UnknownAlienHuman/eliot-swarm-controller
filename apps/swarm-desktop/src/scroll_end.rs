//! Scroll a named WinUI `ScrollViewer` to its end through UI Automation's
//! scroll pattern (k195 P2-4: a conversation opened at the top).
//!
//! Reactor 0.100.0 has no scroll offset or element access on a
//! `ScrollViewer`, and WinUI 3 has no static path from the window to its
//! elements; focusing the transcript's last button brought nothing into view
//! (observed live). WinUI's `ScrollViewerAutomationPeer` implements
//! `IScrollProvider`, so the transcript's viewer, found by its automation id
//! under this process's window, takes `SetScrollPercent(NoScroll, 100)`.
//!
//! The UI Automation client interfaces are not in `windows-sys`; the three
//! used here are declared by hand with the slots and ids of
//! `UIAutomationClient.h` (the `windows` 0.62.2 bindings list the same
//! order: `IUIAutomation` 6 `ElementFromHandle`, 23 `CreatePropertyCondition`;
//! `IUIAutomationElement` 5 `FindFirst`, 14 `GetCurrentPatternAs`;
//! `IUIAutomationScrollPattern` 4 `SetScrollPercent`, 6
//! `get_CurrentVerticalScrollPercent`). Call it off the UI thread: UI
//! Automation reaches WinUI's peers on the UI thread, which keeps pumping.
#![allow(unsafe_code)]

use std::ffi::c_void;

use windows_core::{GUID, HRESULT, IUnknown, Interface};
use windows_sys::Win32::Foundation::{HWND, SysAllocString, SysFreeString};
use windows_sys::Win32::System::Com::{
    CLSCTX_INPROC_SERVER, COINIT_MULTITHREADED, CoCreateInstance, CoInitializeEx,
};
use windows_sys::Win32::System::Variant::{VARIANT, VT_BSTR};
use windows_sys::Win32::UI::Accessibility::{
    CUIAutomation, TreeScope_Descendants, UIA_AutomationIdPropertyId, UIA_ScrollPatternId,
    UIA_ScrollPatternNoScroll,
};

type Out = *mut *mut c_void;

#[repr(C)]
struct AutomationVtbl {
    unknown: [usize; 3],
    _before_from_handle: [usize; 3],
    element_from_handle: unsafe extern "system" fn(*mut c_void, HWND, Out) -> HRESULT,
    _before_condition: [usize; 16],
    create_property_condition: unsafe extern "system" fn(*mut c_void, i32, VARIANT, Out) -> HRESULT,
}

#[repr(C)]
struct ElementVtbl {
    unknown: [usize; 3],
    _before_find: [usize; 2],
    find_first: unsafe extern "system" fn(*mut c_void, i32, *mut c_void, Out) -> HRESULT,
    _before_pattern: [usize; 8],
    get_current_pattern_as:
        unsafe extern "system" fn(*mut c_void, i32, *const GUID, Out) -> HRESULT,
}

#[repr(C)]
struct ScrollPatternVtbl {
    unknown: [usize; 3],
    _scroll: usize,
    set_scroll_percent: unsafe extern "system" fn(*mut c_void, f64, f64) -> HRESULT,
    _horizontal_percent: usize,
    vertical_percent: unsafe extern "system" fn(*mut c_void, *mut f64) -> HRESULT,
}

macro_rules! com_interface {
    ($name:ident, $vtable:ident, $iid:literal) => {
        #[repr(transparent)]
        #[derive(Clone, PartialEq, Eq, Debug)]
        struct $name(IUnknown);

        // SAFETY: a transparent wrapper over an owned COM pointer whose
        // vtable begins with the three IUnknown slots, as declared above;
        // the IID is the interface's id from UIAutomationClient.h.
        unsafe impl Interface for $name {
            type Vtable = $vtable;
            const IID: GUID = GUID::from_u128($iid);
        }
    };
}

com_interface!(
    Automation,
    AutomationVtbl,
    0x30cbe57d_d9d0_452a_ab13_7ac5ac4825ee
);
com_interface!(Element, ElementVtbl, 0xd22108aa_8ac5_49a5_837b_37bbb3d7591e);
com_interface!(
    ScrollPattern,
    ScrollPatternVtbl,
    0x88f4d42a_e881_459d_a77c_73bbbb7e02dc
);

/// Take ownership of a returned interface pointer; `None` for null.
///
/// # Safety
/// `raw` is null or one owned reference to an object implementing `T`.
unsafe fn owned<T: Interface>(raw: *mut c_void) -> Option<T> {
    // SAFETY: the caller guarantees one owned reference of type `T`.
    (!raw.is_null()).then(|| unsafe { T::from_raw(raw) })
}

/// A BSTR freed when dropped.
struct Bstr(windows_sys::core::BSTR);

impl Drop for Bstr {
    fn drop(&mut self) {
        // SAFETY: allocated by SysAllocString, freed once.
        unsafe { SysFreeString(self.0) };
    }
}

/// Scroll the viewer with automation id `id` under `window` to its end.
/// `Ok(Some(percent))` is its vertical position afterwards (100 at the
/// end, -1 when it does not scroll); `Ok(None)` means no such viewer is
/// realized yet.
pub fn scroll_to_end(window: HWND, id: &str) -> Result<Option<f64>, String> {
    let fail = |what: &str, hr: HRESULT| format!("{what} failed ({:#010X})", hr.0);
    // SAFETY: initializes COM for this worker thread; an earlier apartment
    // of another kind (RPC_E_CHANGED_MODE) still lets the calls below run.
    let _ = unsafe { CoInitializeEx(std::ptr::null(), COINIT_MULTITHREADED as u32) };
    let mut raw = std::ptr::null_mut();
    // SAFETY: valid CLSID/IID pointers and an out pointer.
    let hr = HRESULT(unsafe {
        CoCreateInstance(
            &CUIAutomation,
            std::ptr::null_mut(),
            CLSCTX_INPROC_SERVER,
            (&Automation::IID as *const GUID).cast(),
            &mut raw,
        )
    });
    // SAFETY: on success `raw` is one owned IUIAutomation.
    let automation: Automation = unsafe { owned(raw) }.ok_or_else(|| fail("CUIAutomation", hr))?;
    let a = automation.vtable();
    let mut raw = std::ptr::null_mut();
    // SAFETY: slot 6 of IUIAutomation is ElementFromHandle.
    let hr = unsafe { (a.element_from_handle)(automation.as_raw(), window, &mut raw) };
    // SAFETY: on success `raw` is one owned IUIAutomationElement.
    let root: Element = unsafe { owned(raw) }.ok_or_else(|| fail("ElementFromHandle", hr))?;
    let wide: Vec<u16> = id.encode_utf16().chain(std::iter::once(0)).collect();
    // SAFETY: a NUL-terminated UTF-16 string; freed by the guard.
    let text = Bstr(unsafe { SysAllocString(wide.as_ptr()) });
    let mut value = VARIANT::default();
    // The VT_BSTR arm of the zeroed VARIANT; the BSTR stays owned by `text`,
    // which outlives the call (the condition copies it).
    value.Anonymous.Anonymous.vt = VT_BSTR;
    value.Anonymous.Anonymous.Anonymous.bstrVal = text.0;
    let mut raw = std::ptr::null_mut();
    // SAFETY: slot 23 of IUIAutomation is CreatePropertyCondition.
    let hr = unsafe {
        (a.create_property_condition)(
            automation.as_raw(),
            UIA_AutomationIdPropertyId,
            value,
            &mut raw,
        )
    };
    // SAFETY: on success `raw` is one owned IUIAutomationCondition.
    let condition: IUnknown =
        unsafe { owned(raw) }.ok_or_else(|| fail("CreatePropertyCondition", hr))?;
    let mut raw = std::ptr::null_mut();
    // SAFETY: slot 5 of IUIAutomationElement is FindFirst.
    let hr = unsafe {
        (root.vtable().find_first)(
            root.as_raw(),
            TreeScope_Descendants,
            condition.as_raw(),
            &mut raw,
        )
    };
    if hr.is_err() {
        return Err(fail("FindFirst", hr));
    }
    // SAFETY: on success `raw` is null (not found) or one owned element.
    let Some(viewer): Option<Element> = (unsafe { owned(raw) }) else {
        return Ok(None);
    };
    let mut raw = std::ptr::null_mut();
    // SAFETY: slot 14 of IUIAutomationElement is GetCurrentPatternAs.
    let hr = unsafe {
        (viewer.vtable().get_current_pattern_as)(
            viewer.as_raw(),
            UIA_ScrollPatternId,
            &ScrollPattern::IID,
            &mut raw,
        )
    };
    // SAFETY: on success `raw` is null or one owned scroll pattern.
    let Some(pattern): Option<ScrollPattern> = (unsafe { owned(raw) }) else {
        return if hr.is_err() {
            Err(fail("GetCurrentPatternAs", hr))
        } else {
            Ok(None)
        };
    };
    let p = pattern.vtable();
    let mut percent = 0.0;
    // SAFETY: slot 6 is get_CurrentVerticalScrollPercent.
    let hr = unsafe { (p.vertical_percent)(pattern.as_raw(), &mut percent) };
    if hr.is_err() {
        return Err(fail("CurrentVerticalScrollPercent", hr));
    }
    if percent == UIA_ScrollPatternNoScroll {
        // Everything fits: nothing to scroll.
        return Ok(Some(percent));
    }
    // SAFETY: slot 4 is SetScrollPercent (horizontal unchanged).
    let hr = unsafe { (p.set_scroll_percent)(pattern.as_raw(), UIA_ScrollPatternNoScroll, 100.0) };
    if hr.is_err() {
        return Err(fail("SetScrollPercent", hr));
    }
    // SAFETY: as above.
    let hr = unsafe { (p.vertical_percent)(pattern.as_raw(), &mut percent) };
    if hr.is_err() {
        return Err(fail("CurrentVerticalScrollPercent", hr));
    }
    Ok(Some(percent))
}
