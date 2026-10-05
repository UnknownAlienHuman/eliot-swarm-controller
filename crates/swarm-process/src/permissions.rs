use std::{fs::OpenOptions, io::Write, path::Path};
use swarm_contracts::error::Result;

/// Restrict only the explicitly selected path to the current user's access.
pub fn private_permissions(path: &Path, directory: bool) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(
            path,
            std::fs::Permissions::from_mode(if directory { 0o700 } else { 0o600 }),
        )?;
    }
    #[cfg(windows)]
    windows::restrict_path(path, directory)?;
    Ok(())
}

/// Create a new private file, write all bytes, and flush them to the OS.
pub fn write_private_new(path: &Path, data: &[u8]) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    private_permissions(path, false)?;
    file.write_all(data)?;
    file.sync_all()?;
    Ok(())
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::{ffi::c_void, os::windows::ffi::OsStrExt, ptr};
    use swarm_contracts::error::Error;
    use windows_sys::Win32::{
        Foundation::{CloseHandle, LocalFree},
        Security::{
            Authorization::{
                ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
            },
            GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser,
        },
        System::Threading::{GetCurrentProcess, OpenProcessToken},
    };

    fn last_error() -> Error {
        std::io::Error::last_os_error().into()
    }

    fn current_sid() -> Result<String> {
        // SAFETY: output pointers reference initialized storage. Every owned
        // Win32 allocation/handle is released on both success and failure.
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
            let mut output = ptr::null_mut();
            if ConvertSidToStringSidW(user.User.Sid, &mut output) == 0 {
                return Err(last_error());
            }
            let mut length = 0;
            while *output.add(length) != 0 {
                length += 1;
            }
            let value = String::from_utf16_lossy(std::slice::from_raw_parts(output, length));
            LocalFree(output.cast());
            Ok(value)
        }
    }

    fn with_descriptor<T>(inherit: bool, f: impl FnOnce(*mut c_void) -> Result<T>) -> Result<T> {
        let flags = if inherit { "OICI" } else { "" };
        let sddl: Vec<u16> = format!("D:P(A;{flags};GA;;;{})", current_sid()?)
            .encode_utf16()
            .chain(Some(0))
            .collect();
        // SAFETY: Win32 creates a self-relative descriptor; f uses it only
        // before LocalFree releases the allocation.
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

    pub(super) fn restrict_path(path: &Path, directory: bool) -> Result<()> {
        use windows_sys::Win32::Security::{
            DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, SetFileSecurityW,
        };
        let path: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        with_descriptor(directory, |descriptor| {
            // SAFETY: path is NUL-terminated and descriptor lives throughout
            // the SetFileSecurityW call.
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
}
