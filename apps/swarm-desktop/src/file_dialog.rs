//! Native file and folder selection on the UI's STA. IFileOpenDialog's ABI
//! is from shobjidl_core.h (IFileDialog: Show, SetOptions, GetOptions,
//! SetTitle, GetResult; IFileOpenDialog: GetResults), IShellItemArray
//! (GetCount, GetItemAt) and IShellItem::GetDisplayName, which returns a
//! CoTaskMem-owned filesystem path. Reactor 0.100.0 exposes no picker. No
//! new dependency. The modal is owned by this desktop, so cancelling
//! changes nothing.
#![allow(
    unsafe_code,
    reason = "Native file selection uses the Windows Shell dialog API."
)]

use std::ffi::c_void;

use windows_core::{GUID, HRESULT, IUnknown, Interface};
use windows_sys::Win32::System::Com::{CLSCTX_INPROC_SERVER, CoCreateInstance, CoTaskMemFree};
use windows_sys::Win32::UI::Input::KeyboardAndMouse::GetActiveWindow;

/// FOS_PICKFOLDERS.
const PICK_FOLDERS: u32 = 0x20;
/// FOS_ALLOWMULTISELECT.
const MULTISELECT: u32 = 0x200;
/// FOS_FORCEFILESYSTEM | FOS_PATHMUSTEXIST.
const FILESYSTEM: u32 = 0x40 | 0x800;
/// FOS_FILEMUSTEXIST.
const FILE_MUST_EXIST: u32 = 0x1000;

/// One folder (a project's checkout), or `None` when cancelled.
pub fn pick_folder(title: &str) -> Result<Option<String>, String> {
    // SAFETY: called on Reactor's initialized UI STA; see `pick`.
    unsafe { pick(title, PICK_FOLDERS | FILESYSTEM) }
        .map(|paths| paths.into_iter().next())
        .map_err(|error| format!("Could not choose a folder: {error}"))
}

/// Files to attach (empty when cancelled).
pub fn pick_files(title: &str) -> Result<Vec<String>, String> {
    // SAFETY: called on Reactor's initialized UI STA; see `pick`.
    unsafe { pick(title, MULTISELECT | FILESYSTEM | FILE_MUST_EXIST) }
        .map_err(|error| format!("Could not choose files: {error}"))
}

/// Run the dialog with `flags` added to its options; the chosen paths.
unsafe fn pick(title: &str, flags: u32) -> windows_core::Result<Vec<String>> {
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
    // SAFETY: IFileOpenDialog inherits IFileDialog, IModalWindow and
    // IUnknown. The SDK slots: Show=3, SetOptions=9, GetOptions=10,
    // SetTitle=17, GetResults=27; IShellItemArray GetCount=7, GetItemAt=8.
    unsafe {
        let table = *raw.cast::<*const *const c_void>();
        let get_options: extern "system" fn(*mut c_void, *mut u32) -> HRESULT =
            std::mem::transmute(*table.add(10));
        let set_options: extern "system" fn(*mut c_void, u32) -> HRESULT =
            std::mem::transmute(*table.add(9));
        let set_title: extern "system" fn(*mut c_void, *const u16) -> HRESULT =
            std::mem::transmute(*table.add(17));
        let show: extern "system" fn(*mut c_void, *mut c_void) -> HRESULT =
            std::mem::transmute(*table.add(3));
        let results: extern "system" fn(*mut c_void, *mut *mut c_void) -> HRESULT =
            std::mem::transmute(*table.add(27));
        let mut options = 0;
        get_options(raw, &mut options).ok()?;
        set_options(raw, options | flags).ok()?;
        let caption: Vec<u16> = title.encode_utf16().chain(Some(0)).collect();
        set_title(raw, caption.as_ptr()).ok()?;
        let shown = show(raw, GetActiveWindow().cast());
        if shown.0 == 0x800704c7_u32 as i32 {
            return Ok(Vec::new()); // ERROR_CANCELLED.
        }
        shown.ok()?;
        let mut array_raw = std::ptr::null_mut();
        results(raw, &mut array_raw).ok()?;
        let array = IUnknown::from_raw(array_raw);
        let table = *array_raw.cast::<*const *const c_void>();
        let count: extern "system" fn(*mut c_void, *mut u32) -> HRESULT =
            std::mem::transmute(*table.add(7));
        let item_at: extern "system" fn(*mut c_void, u32, *mut *mut c_void) -> HRESULT =
            std::mem::transmute(*table.add(8));
        let mut total = 0;
        count(array_raw, &mut total).ok()?;
        let mut paths = Vec::new();
        for index in 0..total {
            let mut item_raw = std::ptr::null_mut();
            item_at(array_raw, index, &mut item_raw).ok()?;
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
            paths.push(String::from_utf16_lossy(std::slice::from_raw_parts(
                path, len,
            )));
            CoTaskMemFree(path.cast());
            drop(item);
        }
        drop(array);
        drop(dialog);
        Ok(paths)
    }
}
