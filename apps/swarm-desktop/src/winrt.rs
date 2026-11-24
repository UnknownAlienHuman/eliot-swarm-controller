//! Hand-declared WinRT interfaces for the few WinUI calls Reactor 0.100.0
//! does not expose (`app_resources`, `motion`, `flyout`, `shadow`).
//! Reactor's own bindings are crate-private, so each caller declares only
//! the vtable slots it calls, with ids and slot numbers read from the
//! Windows App SDK metadata (`Microsoft.UI.Xaml.winmd` 2.3.0,
//! `Microsoft.UI.winmd` 2.1.3).
//! The event handlers those callers add are windows-core delegate boxes
//! around a plain function ([`delegate`]).
#![allow(
    unsafe_code,
    reason = "WinRT UI integration requires COM and native interface calls."
)]

use std::ffi::c_void;

use windows_core::imp::DelegateBox;
use windows_core::{HRESULT, IInspectable, IUnknown, Interface};

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
        #[allow(
            unsafe_code,
            reason = "A hand-declared WinRT interface is an unsafe Interface implementation."
        )]
        unsafe impl windows_core::Interface for $name {
            type Vtable = $vtable;
            const IID: windows_core::GUID = $iid;
        }
    };
}
pub(crate) use winrt_interface;

/// A runtime class name, for its activation factory
/// (`windows_core::factory`).
macro_rules! runtime_class {
    ($name:ident, $class:literal) => {
        struct $name;
        impl windows_core::RuntimeName for $name {
            const NAME: &'static str = $class;
        }
    };
}
pub(crate) use runtime_class;

/// `Windows.UI.Color`: the title bar's colors (`app_resources`) and the
/// shadows' (`shadow`).
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct UiColor {
    pub a: u8,
    pub r: u8,
    pub g: u8,
    pub b: u8,
}

impl windows_core::imp::TypeKind for UiColor {
    type TypeKind = windows_core::imp::CopyType;
}

impl windows_core::RuntimeType for UiColor {
    const SIGNATURE: windows_core::imp::ConstBuffer =
        windows_core::imp::ConstBuffer::from_slice(b"struct(Windows.UI.Color;u1;u1;u1;u1)");
}

impl From<crate::theme::Rgb8> for UiColor {
    fn from(value: crate::theme::Rgb8) -> Self {
        Self {
            a: 255,
            r: value.r,
            g: value.g,
            b: value.b,
        }
    }
}

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

/// Borrow an ABI argument as `T`.
pub(crate) fn borrowed<T: Interface>(raw: Raw) -> Option<T> {
    // SAFETY: `raw` is a live interface pointer the caller lends for the
    // call; `cast` takes a new reference of its own.
    unsafe { IUnknown::from_raw_borrowed(&raw) }?.cast().ok()
}

/// A delegate: IUnknown, then `Invoke(sender, args)`.
#[repr(C)]
pub(crate) struct DelegateVtbl {
    base: windows_core::IUnknown_Vtbl,
    invoke: unsafe extern "system" fn(Raw, Raw, Raw) -> HRESULT,
}

/// What a delegate calls with its sender and arguments.
pub(crate) type Callback = fn(Raw, Raw);

/// A delegate calling `callback`: windows-core's delegate box with this
/// module's `Invoke` thunk.
pub(crate) fn delegate<I: Interface<Vtable = DelegateVtbl>>(
    vtable: &'static DelegateVtbl,
    callback: Callback,
) -> I {
    let boxed = Box::new(DelegateBox::<I, Callback>::new(vtable, callback));
    // SAFETY: a `DelegateBox` starts with its vtable pointer and carries
    // one reference; `I` is a transparent interface pointer.
    unsafe { I::from_raw(Box::into_raw(boxed).cast()) }
}

/// The `Invoke` slot of [`delegate`]'s vtables. A panic in the callback
/// stops at this ABI boundary (unwinding out of an `extern "system"`
/// function aborts the process): it is noted, and the window says so once
/// (`crate::startup::caught_panic`).
///
/// The callback is a plain `fn` and its arguments are raw pointers, so the
/// closure is unwind safe as it stands, with no `AssertUnwindSafe`. What
/// the callbacks touch stays consistent after a panic too: the flyout's
/// `Closed` only reads its registry and enqueues a message; the focus and
/// pointer events replace one thread-local value whole (a `RefCell`
/// releases its borrow as it unwinds), count a press or set one native
/// property.
unsafe extern "system" fn invoke<I: Interface>(this: Raw, sender: Raw, args: Raw) -> HRESULT {
    // SAFETY: `this` is the box `delegate` allocated for `I`.
    let this = unsafe { &*(this as *const DelegateBox<I, Callback>) };
    let callback = this.invoke;
    let _boundary = crate::startup::native_boundary();
    if let Err(panic) = std::panic::catch_unwind(|| callback(sender, args)) {
        crate::startup::caught_panic("a WinUI event handler", &*panic);
    }
    HRESULT(0)
}

/// The vtable of a delegate of interface `I` (kept in a `static`).
pub(crate) const fn delegate_vtable<I: Interface>() -> DelegateVtbl {
    DelegateVtbl {
        base: windows_core::IUnknown_Vtbl {
            QueryInterface: DelegateBox::<I, Callback>::QueryInterface,
            AddRef: DelegateBox::<I, Callback>::AddRef,
            Release: DelegateBox::<I, Callback>::Release,
        },
        invoke: invoke::<I>,
    }
}
