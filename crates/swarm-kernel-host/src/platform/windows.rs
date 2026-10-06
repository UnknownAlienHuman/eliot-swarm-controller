//! Small Win32 ACL boundary. No changes to UAC, global configuration or foreign paths.
use crate::error::Result;
use std::os::windows::ffi::OsStrExt;
use std::{ffi::c_void, path::Path, ptr};
use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};
use windows_sys::Win32::{
    Foundation::{CloseHandle, LocalFree},
    Security::{
        Authorization::{
            ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
        },
        DACL_SECURITY_INFORMATION, GetTokenInformation, PROTECTED_DACL_SECURITY_INFORMATION,
        SECURITY_ATTRIBUTES, SetFileSecurityW, TOKEN_QUERY, TOKEN_USER, TokenUser,
    },
    System::Threading::{GetCurrentProcess, OpenProcessToken},
};

fn last_error() -> crate::error::Error {
    std::io::Error::last_os_error().into()
}
fn current_sid() -> Result<String> {
    // SAFETY: all output pointers reference initialized storage. Every owned Win32
    // allocation/handle is released even on failure; token buffer is pointer-aligned.
    unsafe {
        let mut token = ptr::null_mut();
        if OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) == 0 {
            return Err(last_error());
        }
        let mut bytes = 0;
        GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut bytes);
        let mut buffer = vec![0usize; (bytes as usize).div_ceil(size_of::<usize>())];
        let ok = GetTokenInformation(
            token,
            TokenUser,
            buffer.as_mut_ptr().cast(),
            bytes,
            &mut bytes,
        );
        let error = if ok == 0 { Some(last_error()) } else { None };
        CloseHandle(token);
        if let Some(error) = error {
            return Err(error);
        }
        let user = &*buffer.as_ptr().cast::<TOKEN_USER>();
        let mut out = ptr::null_mut();
        if ConvertSidToStringSidW(user.User.Sid, &mut out) == 0 {
            return Err(last_error());
        }
        let mut len = 0;
        while *out.add(len) != 0 {
            len += 1;
        }
        let value = String::from_utf16_lossy(std::slice::from_raw_parts(out, len));
        LocalFree(out.cast());
        Ok(value)
    }
}
fn with_descriptor<T>(inherit: bool, f: impl FnOnce(*mut c_void) -> Result<T>) -> Result<T> {
    let flags = if inherit { "OICI" } else { "" };
    let sddl: Vec<u16> = format!("D:P(A;{flags};GA;;;{})", current_sid()?)
        .encode_utf16()
        .chain(Some(0))
        .collect();
    // SAFETY: Win32 creates a self-relative descriptor; f uses it only before LocalFree.
    unsafe {
        let mut descriptor = ptr::null_mut();
        if ConvertStringSecurityDescriptorToSecurityDescriptorW(
            sddl.as_ptr(),
            1,
            &mut descriptor,
            ptr::null_mut(),
        ) == 0
        {
            return Err(last_error());
        }
        let result = f(descriptor);
        LocalFree(descriptor);
        result
    }
}
pub fn restrict_path(path: &Path, directory: bool) -> Result<()> {
    let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    with_descriptor(directory, |descriptor| {
        // SAFETY: path is NUL-terminated, descriptor lives throughout the call.
        let ok = unsafe {
            SetFileSecurityW(
                path.as_ptr(),
                DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
                descriptor,
            )
        };
        if ok == 0 { Err(last_error()) } else { Ok(()) }
    })
}
pub fn create_pipe(name: &str, first: bool) -> Result<NamedPipeServer> {
    with_descriptor(false, |descriptor| {
        let mut sa = SECURITY_ATTRIBUTES {
            nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
            lpSecurityDescriptor: descriptor,
            bInheritHandle: 0,
        };
        // SAFETY: the attributes and descriptor live until CreateNamedPipe returns;
        // Windows copies the descriptor, so no borrowed pointer escapes.
        Ok(unsafe {
            ServerOptions::new()
                .first_pipe_instance(first)
                .reject_remote_clients(true)
                .create_with_security_attributes_raw(
                    name,
                    (&mut sa as *mut SECURITY_ATTRIBUTES).cast(),
                )?
        })
    })
}
