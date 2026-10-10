use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
};
use swarm_contracts::error::{Error, Result};

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

/// Create a new private file without clobbering an existing path.
///
/// Bytes are written to a same-directory private temporary file and synced
/// before atomic no-replace publication. Unix also syncs the parent directory
/// before success. Windows uses a write-through move; this does not claim a
/// Unix-equivalent parent-directory fsync.
pub fn write_private_new(path: &Path, data: &[u8]) -> Result<()> {
    let target = private_target_path(path)?;
    if target_is_regular_file(&target)? {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "private file already exists",
        )
        .into());
    }
    let temp = private_temp_path(&target)?;
    let result = (|| {
        let mut file = create_private_temp(&temp)?;
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        publish_new(&temp, &target)?;
        sync_parent_directory(&target)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Atomically replace a regular private file using a synced same-directory
/// temporary file. The target may be absent; a symlink, reparse point, or
/// non-file target is rejected. Unix syncs the parent directory after the
/// replacement. Windows uses `MOVEFILE_WRITE_THROUGH` without claiming an
/// equivalent directory fsync.
pub fn replace_private_durable(path: &Path, data: &[u8]) -> Result<()> {
    let target = private_target_path(path)?;
    let _ = target_is_regular_file(&target)?;
    let temp = private_temp_path(&target)?;
    let result = (|| {
        let mut file = create_private_temp(&temp)?;
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        publish_replace(&temp, &target)?;
        sync_parent_directory(&target)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Remove an exact regular private file and sync its parent on Unix.
/// Returns `false` when the file was already absent. On Windows the removal
/// uses the platform file API; no parent-directory flush equivalence is
/// claimed.
pub fn remove_private_durable(path: &Path) -> Result<bool> {
    let target = private_target_path(path)?;
    if !target_is_regular_file(&target)? {
        return Ok(false);
    }
    fs::remove_file(&target)?;
    sync_parent_directory(&target)?;
    Ok(true)
}

/// Sync the parent directory on Unix. Windows currently has no directory
/// flush implementation here, so this returns success without claiming that
/// the directory entry itself is durable there.
pub fn sync_parent_directory(path: &Path) -> Result<()> {
    let target = private_target_path(path)?;
    let parent = target
        .parent()
        .ok_or_else(|| Error::invalid("private file path has no parent directory"))?;
    #[cfg(unix)]
    {
        #[cfg(target_os = "linux")]
        let directory = {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW)
                .open(parent)?
        };
        #[cfg(not(target_os = "linux"))]
        let directory = fs::File::open(parent)?;
        directory.sync_all()?;
    }
    #[cfg(windows)]
    {
        let _ = parent;
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = parent;
        return Err(Error::new(
            "DURABILITY_UNSUPPORTED",
            "parent-directory durability is unsupported on this platform",
        ));
    }
    Ok(())
}

fn private_target_path(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute()
        && path
            .components()
            .any(|component| matches!(component, Component::Prefix(_)))
    {
        return Err(Error::invalid(
            "private file path has a drive-relative prefix",
        ));
    }
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    if absolute
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || absolute.file_name().is_none()
    {
        return Err(Error::invalid(
            "private file path must name one file without traversal components",
        ));
    }
    let parent = absolute
        .parent()
        .ok_or_else(|| Error::invalid("private file path has no parent directory"))?;
    let mut current = PathBuf::new();
    for component in parent.components() {
        current.push(component.as_os_str());
        if matches!(component, Component::Prefix(_)) {
            continue;
        }
        let metadata = fs::symlink_metadata(&current)?;
        if is_link_or_reparse(&metadata) || !metadata.is_dir() {
            return Err(Error::new(
                "PRIVATE_PATH_INVALID",
                "private file path cannot traverse links or non-directories",
            ));
        }
    }
    Ok(absolute)
}

fn target_is_regular_file(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_file() => Err(Error::new(
            "PRIVATE_FILE_INVALID",
            "private file target is a link or is not a regular file",
        )),
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn private_temp_path(target: &Path) -> Result<PathBuf> {
    let parent = target
        .parent()
        .ok_or_else(|| Error::invalid("private file path has no parent directory"))?;
    let mut name = OsString::from(".");
    name.push(
        target
            .file_name()
            .ok_or_else(|| Error::invalid("private file path has no file name"))?,
    );
    name.push(format!(".{}.tmp", uuid::Uuid::new_v4()));
    Ok(parent.join(name))
}

fn create_private_temp(path: &Path) -> Result<fs::File> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options.open(path)?;
    private_permissions(path, false)?;
    Ok(file)
}

#[cfg(unix)]
fn publish_new(temp: &Path, target: &Path) -> Result<()> {
    // Hard-link publication is atomic and fails if the target already exists.
    fs::hard_link(temp, target)?;
    fs::remove_file(temp)?;
    Ok(())
}

#[cfg(windows)]
fn publish_new(temp: &Path, target: &Path) -> Result<()> {
    windows::move_file(temp, target, false)
}

#[cfg(not(any(unix, windows)))]
fn publish_new(temp: &Path, target: &Path) -> Result<()> {
    fs::hard_link(temp, target)?;
    fs::remove_file(temp)?;
    Ok(())
}

#[cfg(unix)]
fn publish_replace(temp: &Path, target: &Path) -> Result<()> {
    fs::rename(temp, target)?;
    Ok(())
}

#[cfg(windows)]
fn publish_replace(temp: &Path, target: &Path) -> Result<()> {
    windows::move_file(temp, target, true)
}

#[cfg(not(any(unix, windows)))]
fn publish_replace(temp: &Path, target: &Path) -> Result<()> {
    fs::rename(temp, target)?;
    Ok(())
}

fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink() || is_reparse(metadata)
}

#[cfg(windows)]
fn is_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_reparse(_metadata: &fs::Metadata) -> bool {
    false
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::{
        ffi::c_void,
        os::windows::ffi::OsStrExt,
        path::{Component, Prefix},
        ptr,
    };
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

    pub(super) fn win32_path(path: &Path) -> Result<Vec<u16>> {
        let raw: Vec<u16> = path.as_os_str().encode_wide().collect();
        let prefix = path
            .components()
            .next()
            .and_then(|component| match component {
                Component::Prefix(prefix) => Some(prefix.kind()),
                _ => None,
            });
        let mut encoded = Vec::with_capacity(raw.len() + 8);
        let mut normalize_separators = false;
        match prefix {
            Some(Prefix::Disk(_)) if path.is_absolute() => {
                encoded.extend("\\\\?\\".encode_utf16());
                encoded.extend_from_slice(&raw);
                normalize_separators = true;
            }
            Some(Prefix::UNC(..)) if path.is_absolute() => {
                if raw.len() < 2 || !is_separator(raw[0]) || !is_separator(raw[1]) {
                    return Err(Error::invalid("invalid absolute UNC path for Win32 API"));
                }
                encoded.extend("\\\\?\\UNC\\".encode_utf16());
                encoded.extend_from_slice(&raw[2..]);
                normalize_separators = true;
            }
            Some(Prefix::VerbatimDisk(_) | Prefix::VerbatimUNC(..) | Prefix::Verbatim(_)) => {
                encoded.extend_from_slice(&raw);
            }
            Some(Prefix::DeviceNS(_)) | Some(Prefix::Disk(_)) | Some(Prefix::UNC(..)) | None => {
                encoded.extend_from_slice(&raw)
            }
        }
        if normalize_separators {
            for unit in &mut encoded {
                if *unit == b'/' as u16 {
                    *unit = b'\\' as u16;
                }
            }
        }
        if encoded.contains(&0) {
            return Err(Error::invalid("Windows API path contains a NUL character"));
        }
        encoded.push(0);
        Ok(encoded)
    }

    fn is_separator(unit: u16) -> bool {
        unit == b'\\' as u16 || unit == b'/' as u16
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

    pub(super) fn move_file(source: &Path, destination: &Path, replace: bool) -> Result<()> {
        use windows_sys::Win32::Storage::FileSystem::{
            MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
        };
        let source = win32_path(source)?;
        let destination = win32_path(destination)?;
        let mut flags = MOVEFILE_WRITE_THROUGH;
        if replace {
            flags |= MOVEFILE_REPLACE_EXISTING;
        }
        // SAFETY: both paths are NUL-terminated UTF-16 strings that remain
        // alive for the duration of the Win32 call.
        let ok = unsafe { MoveFileExW(source.as_ptr(), destination.as_ptr(), flags) };
        if ok == 0 { Err(last_error()) } else { Ok(()) }
    }

    pub(super) fn restrict_path(path: &Path, directory: bool) -> Result<()> {
        use windows_sys::Win32::Security::{
            DACL_SECURITY_INFORMATION, PROTECTED_DACL_SECURITY_INFORMATION, SetFileSecurityW,
        };
        let path = win32_path(path)?;
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

#[cfg(all(test, windows))]
mod tests {
    use super::*;
    use std::os::windows::ffi::OsStrExt;

    struct RemoveTestDirectory(PathBuf);

    impl Drop for RemoveTestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn write_private_new_supports_long_windows_paths() {
        fn path_string(path: &Path) -> String {
            let encoded = super::windows::win32_path(path).expect("encode Win32 path");
            String::from_utf16(&encoded[..encoded.len() - 1]).expect("decode Win32 path")
        }

        assert_eq!(
            path_string(Path::new(r"C:\runtime\credentials.json")),
            r"\\?\C:\runtime\credentials.json"
        );
        assert_eq!(
            path_string(Path::new(r"C:/runtime/credentials.json")),
            r"\\?\C:\runtime\credentials.json"
        );
        assert_eq!(
            path_string(Path::new(r"\\server\share\credentials.json")),
            r"\\?\UNC\server\share\credentials.json"
        );
        assert_eq!(
            path_string(Path::new(r"C:runtime\credentials.json")),
            r"C:runtime\credentials.json"
        );
        assert_eq!(
            path_string(Path::new(r"relative/credentials.json")),
            r"relative/credentials.json"
        );
        assert_eq!(
            path_string(Path::new(r"\\?\C:\runtime/credentials.json")),
            r"\\?\C:\runtime/credentials.json"
        );

        let root = std::env::temp_dir().join(format!(
            "swarm-process-permissions-{}",
            uuid::Uuid::new_v4()
        ));
        let _cleanup = RemoveTestDirectory(root.clone());
        let parent = root.join("p".repeat(160));
        fs::create_dir_all(&parent).expect("create long-path test directory");
        let target = parent.join("credentials.json");
        let temp = private_temp_path(&target).expect("build private temporary path");
        let temp_path_length = temp.as_os_str().encode_wide().count();
        assert!(
            temp_path_length > 260,
            "expected a path over MAX_PATH, got {temp_path_length} UTF-16 code units"
        );

        write_private_new(&target, b"private credentials")
            .expect("write private file at a long Windows path");
        assert_eq!(
            fs::read(&target).expect("read published private file"),
            b"private credentials"
        );
        assert!(write_private_new(&target, b"replacement").is_err());
        assert_eq!(
            fs::read(&target).expect("read file after rejected replacement"),
            b"private credentials"
        );
    }
}
