#![allow(unsafe_code)]

//! Small FFI boundary using locked windows-sys 0.61.2 (MIT OR Apache-2.0).
//! No upstream implementation is copied. FileIdInfo supplies the full 128-bit ID:
//! https://learn.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-getfileinformationbyhandleex
//! https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_id_info

use super::FileIdentity;
use std::{
    ffi::OsString,
    fs::File,
    io,
    mem::size_of,
    os::windows::{ffi::OsStringExt, io::AsRawHandle},
    path::PathBuf,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ID_INFO, FileIdInfo, GetFileInformationByHandleEx, GetFinalPathNameByHandleW,
    VOLUME_NAME_DOS,
};

pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let mut info = FILE_ID_INFO::default();
    // SAFETY: the borrowed File keeps its handle live throughout this call.
    // The writable buffer has the exact layout and size required by FileIdInfo.
    let success = unsafe {
        GetFileInformationByHandleEx(
            file.as_raw_handle(),
            FileIdInfo,
            (&raw mut info).cast(),
            size_of::<FILE_ID_INFO>() as u32,
        )
    };
    if success == 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(FileIdentity::Windows {
        volume: info.VolumeSerialNumber,
        file_id: info.FileId.Identifier,
    })
}

pub(super) fn file_final_path(file: &File) -> io::Result<PathBuf> {
    // The required size includes the terminator on insufficient-buffer returns;
    // a successful return excludes it. Retry boundedly if the name grows.
    // https://learn.microsoft.com/en-us/windows/win32/api/fileapi/nf-fileapi-getfinalpathnamebyhandlew
    let mut buffer = vec![0_u16; 512];
    for _ in 0..8 {
        let capacity = u32::try_from(buffer.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "final path exceeds Windows buffer size",
            )
        })?;
        // SAFETY: File keeps the borrowed handle alive. The buffer is writable
        // for capacity UTF-16 units; the W API preserves the full Windows name.
        let length = unsafe {
            GetFinalPathNameByHandleW(
                file.as_raw_handle(),
                buffer.as_mut_ptr(),
                capacity,
                // Normalized-name mode is the API default.
                VOLUME_NAME_DOS,
            )
        };
        if length == 0 {
            return Err(io::Error::last_os_error());
        }
        if length < capacity {
            return Ok(PathBuf::from(OsString::from_wide(
                &buffer[..length as usize],
            )));
        }
        let next = length.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "final path exceeds Windows buffer size",
            )
        })? as usize;
        buffer.try_reserve_exact(next - buffer.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                "cannot allocate final path buffer",
            )
        })?;
        buffer.resize(next, 0);
    }
    Err(io::Error::new(
        io::ErrorKind::WouldBlock,
        "open-file final path changed during bounded readback",
    ))
}
