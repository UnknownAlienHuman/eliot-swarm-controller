//! Hand-declared WinRT interfaces for the few WinUI calls Reactor 0.100.0
//! does not expose (`app_resources`, `motion`). Reactor's own bindings are
//! crate-private, so each caller declares only the vtable slots it calls,
//! with ids and slot numbers read from the Windows App SDK metadata
//! (`Microsoft.UI.Xaml.winmd` 2.3.0, `Microsoft.UI.winmd` 2.1.3).
#![allow(
    unsafe_code,
    reason = "WinRT UI integration requires COM and native interface calls."
)]

use std::ffi::c_void;

use windows_core::{HRESULT, IInspectable, Interface};

/// A WinRT interface pointer with a hand-declared vtable.
macro_rules! winrt_interface {
    ($name:ident, $vtable:ident, $iid:expr) => {
        #[repr(transparent)]
        #[derive(Clone, PartialEq, Eq, Debug)]
        struct $name(windows_core::IUnknown);

        // SAFETY: a transparent wrapper over an owned COM pointer whose
        // vtable is declared by `$vtable` (the six IInspectable slots, or
        // the three IUnknown slots of a delegate, then the slots used); the
        // IID is the interface's id from the Windows App SDK metadata.
        unsafe impl windows_core::Interface for $name {
            type Vtable = $vtable;
            const IID: windows_core::GUID = $iid;
        }
    };
}
pub(crate) use winrt_interface;

/// A raw interface pointer as the ABI passes it.
pub(crate) type Raw = *mut c_void;

/// A property getter slot returning one owned object.
pub(crate) type Getter = unsafe extern "system" fn(Raw, *mut Raw) -> HRESULT;

/// IUnknown and IInspectable slots 0–5, never called through these structs.
pub(crate) type InspectableSlots = [usize; 6];

/// Call a getter slot and take ownership of the returned object.
///
/// # Safety
/// `getter` must be a slot of `this`'s vtable that returns one owned
/// IInspectable-derived pointer.
pub(crate) unsafe fn get<T: Interface>(
    this: &T,
    getter: Getter,
) -> windows_core::Result<IInspectable> {
    let mut out = std::ptr::null_mut();
    // SAFETY: the caller guarantees `getter` belongs to `this`.
    unsafe { getter(this.as_raw(), &mut out) }.ok()?;
    if out.is_null() {
        return Err(windows_core::Error::empty());
    }
    // SAFETY: the getter returned one owned reference.
    Ok(unsafe { IInspectable::from_raw(out) })
}
