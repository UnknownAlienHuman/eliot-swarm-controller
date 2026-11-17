//! Native folder selection on the UI's STA. IFileDialog's ABI is from
//! shobjidl_core.h (Show, SetOptions, GetOptions, SetTitle, GetResult);
//! IShellItem::GetDisplayName returns a CoTaskMem-owned filesystem path.
//! Reactor 0.100.0 does not expose a folder picker. No new dependency.
#![allow(unsafe_code)]

use std::ffi::c_void;

use windows_core::{GUID, HRESULT, IUnknown, Interface};
use windows_sys::Win32::System::Com::{CLSCTX_INPROC_SERVER, CoCreateInstance, CoTaskMemFree};
use windows_sys::Win32::UI::Input::KeyboardAndMouse::GetActiveWindow;

/// The native modal is owned by this desktop, so cancellation changes nothing.
pub(super) fn pick() -> Result<Option<String>, String> {
    // SAFETY: called on Reactor's initialized UI STA. Interfaces own their COM
    // references; each slot and signature below matches the SDK interfaces.
    unsafe { pick_sta().map_err(|error| format!("Could not choose a folder: {error}")) }
}

unsafe fn pick_sta() -> windows_core::Result<Option<String>> {
    // FileOpenDialog / IFileOpenDialog, as declared by the Windows SDK.
    let class = GUID::from_u128(0xdc1c5a9c_e88a_4dde_a5a1_60f82a20aef7);
    let iid = GUID::from_u128(0xd57c7288_d4ad_4768_be02_9d969532d960);
    let mut raw = std::ptr::null_mut();
    // SAFETY: valid GUIDs and out pointer; a successful call owns one reference.
    HRESULT(unsafe {
        CoCreateInstance(
            (&class as *const GUID).cast(),
            std::ptr::null_mut(),
            CLSCTX_INPROC_SERVER,
            (&iid as *const GUID).cast(),
            &mut raw,
        )
    })
    .ok()?;
    // SAFETY: CoCreateInstance returned a non-null owned COM interface.
    let dialog = unsafe { IUnknown::from_raw(raw) };
    // SAFETY: IFileDialog inherits IModalWindow and IUnknown. The SDK slots:
    // Show=3, SetOptions=9, GetOptions=10, SetTitle=17, GetResult=20.
    unsafe {
        let table = *raw.cast::<*const *const c_void>();
        let get_options: extern "system" fn(*mut c_void, *mut u32) -> HRESULT =
            std::mem::transmute(*table.add(10));
        let set_options: extern "system" fn(*mut c_void, u32) -> HRESULT =
            std::mem::transmute(*table.add(9));
        let title: extern "system" fn(*mut c_void, *const u16) -> HRESULT =
            std::mem::transmute(*table.add(17));
        let show: extern "system" fn(*mut c_void, *mut c_void) -> HRESULT =
            std::mem::transmute(*table.add(3));
        let result: extern "system" fn(*mut c_void, *mut *mut c_void) -> HRESULT =
            std::mem::transmute(*table.add(20));
        let mut options = 0;
        get_options(raw, &mut options).ok()?;
        // FOS_PICKFOLDERS | FOS_FORCEFILESYSTEM | FOS_PATHMUSTEXIST.
        set_options(raw, options | 0x20 | 0x40 | 0x800).ok()?;
        let caption: Vec<u16> = "Add a project: choose its checkout folder\0"
            .encode_utf16()
            .collect();
        title(raw, caption.as_ptr()).ok()?;
        let shown = show(raw, GetActiveWindow().cast());
        if shown.0 == 0x800704c7_u32 as i32 {
            return Ok(None); // ERROR_CANCELLED.
        }
        shown.ok()?;
        let mut item_raw = std::ptr::null_mut();
        result(raw, &mut item_raw).ok()?;
        let item = IUnknown::from_raw(item_raw);
        let table = *item_raw.cast::<*const *const c_void>();
        let display_name: extern "system" fn(*mut c_void, u32, *mut *mut u16) -> HRESULT =
            std::mem::transmute(*table.add(5));
        let mut path = std::ptr::null_mut();
        display_name(item_raw, 0x80058000, &mut path).ok()?; // SIGDN_FILESYSPATH.
        let mut len = 0;
        while *path.add(len) != 0 {
            len += 1;
        }
        let selected = String::from_utf16_lossy(std::slice::from_raw_parts(path, len));
        CoTaskMemFree(path.cast());
        drop(item);
        drop(dialog);
        Ok(Some(selected))
    }
}
