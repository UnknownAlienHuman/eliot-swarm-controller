use std::{
    ffi::OsString,
    fs::{self, OpenOptions},
    io::{Read, Seek, Write},
    path::{Component, Path, PathBuf},
};
use swarm_contracts::error::{Error, Result};

#[cfg(feature = "test-support")]
#[doc(hidden)]
pub mod test_support {
    use super::Result;
    use std::{cell::RefCell, path::Path, rc::Rc};

    type BarrierHook = Rc<dyn Fn(&Path) -> Result<()> + 'static>;

    std::thread_local! {
        static ACTIVE_BARRIER: RefCell<Option<BarrierHook>> = const { RefCell::new(None) };
        static ACTIVE_PREPUBLICATION_FAULT: RefCell<Option<BarrierHook>> = const { RefCell::new(None) };
    }

    /// Installs a barrier override on this thread until the returned fixture
    /// guard is dropped. Separate tests and worker threads cannot race through
    /// process-wide environment state.
    #[must_use]
    pub struct ScopedDurabilityBarrier {
        previous: Option<BarrierHook>,
    }

    /// Test-only fault at the shared exact-file boundary after the private
    /// temporary is synced but before its final name is published.
    #[must_use]
    pub struct ScopedPrepublicationFault {
        previous: Option<BarrierHook>,
    }

    pub fn scoped_durability_barrier(
        hook: impl Fn(&Path) -> Result<()> + 'static,
    ) -> ScopedDurabilityBarrier {
        let previous = ACTIVE_BARRIER.with(|active| active.borrow_mut().replace(Rc::new(hook)));
        ScopedDurabilityBarrier { previous }
    }

    pub fn scoped_prepublication_fault(
        hook: impl Fn(&Path) -> Result<()> + 'static,
    ) -> ScopedPrepublicationFault {
        let previous =
            ACTIVE_PREPUBLICATION_FAULT.with(|active| active.borrow_mut().replace(Rc::new(hook)));
        ScopedPrepublicationFault { previous }
    }

    impl Drop for ScopedDurabilityBarrier {
        fn drop(&mut self) {
            let previous = self.previous.take();
            ACTIVE_BARRIER.with(|active| *active.borrow_mut() = previous);
        }
    }

    impl Drop for ScopedPrepublicationFault {
        fn drop(&mut self) {
            let previous = self.previous.take();
            ACTIVE_PREPUBLICATION_FAULT.with(|active| *active.borrow_mut() = previous);
        }
    }

    pub(super) fn invoke(path: &Path) -> Option<Result<()>> {
        let hook = ACTIVE_BARRIER.with(|active| active.borrow().clone());
        hook.map(|hook| hook(path))
    }

    pub(super) fn prepublication_fault(path: &Path) -> Option<Result<()>> {
        let hook = ACTIVE_PREPUBLICATION_FAULT.with(|active| active.borrow().clone());
        hook.map(|hook| hook(path))
    }
}

fn durability_barrier(path: &Path) -> Result<()> {
    #[cfg(feature = "test-support")]
    if let Some(result) = test_support::invoke(path) {
        return result;
    }
    sync_parent_directory(path)
}

/// Compare a retained regular-file handle with the current named file. Bytes,
/// timestamps and length do not establish identity across atomic replacement.
pub fn opened_regular_file_matches_path(file: &fs::File, path: &Path) -> Result<bool> {
    if !target_is_regular_file(path)? || !file.metadata()?.is_file() {
        return Ok(false);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let named = options.open(path)?;
    if !named.metadata()?.is_file() {
        return Ok(false);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let opened = file.metadata()?;
        let current = named.metadata()?;
        Ok(opened.dev() == current.dev() && opened.ino() == current.ino())
    }
    #[cfg(windows)]
    {
        use std::os::windows::io::AsRawHandle;
        use windows_sys::Win32::Storage::FileSystem::{
            BY_HANDLE_FILE_INFORMATION, FILE_ATTRIBUTE_REPARSE_POINT, GetFileInformationByHandle,
        };
        fn identity(file: &fs::File) -> Result<Option<(u32, u32, u32)>> {
            // SAFETY: the integer-only output is initialized, and File retains
            // the valid handle throughout this read-only platform query.
            let mut info: BY_HANDLE_FILE_INFORMATION = unsafe { std::mem::zeroed() };
            if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
                return Ok(None);
            }
            Ok(Some((
                info.dwVolumeSerialNumber,
                info.nFileIndexHigh,
                info.nFileIndexLow,
            )))
        }
        Ok(
            matches!((identity(file)?, identity(&named)?), (Some(opened), Some(current)) if opened == current),
        )
    }
    #[cfg(not(any(unix, windows)))]
    {
        Err(Error::new(
            "PRIVATE_FILE_IDENTITY_UNSUPPORTED",
            "opened-file identity is unavailable on this platform",
        ))
    }
}

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
    write_private_new_with_barrier(path, data, durability_barrier)
}

fn write_private_new_with_barrier(
    path: &Path,
    data: &[u8],
    barrier: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let target = private_target_path(path)?;
    if target_is_regular_file(&target)? {
        return Err(private_file_exists());
    }
    let temp = private_temp_path(&target)?;
    let result = (|| {
        let mut file = create_private_temp(&temp)?;
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        #[cfg(feature = "test-support")]
        if let Some(result) = test_support::prepublication_fault(&target) {
            result?;
        }
        publish_new(&temp, &target)?;
        barrier(&target)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

/// Publish exact bounded bytes, or converge on the same retained regular file.
/// An existing file must pass its own file-sync and platform parent boundary;
/// matching visible bytes alone never establish success. Other publication
/// errors remain errors even if they left the target visible.
pub fn write_private_exact(path: &Path, data: &[u8], maximum: u64) -> Result<()> {
    write_private_exact_with_barrier(path, data, maximum, durability_barrier)
}

fn write_private_exact_with_barrier(
    path: &Path,
    data: &[u8],
    maximum: u64,
    mut barrier: impl FnMut(&Path) -> Result<()>,
) -> Result<()> {
    if data.len() as u64 > maximum {
        return Err(Error::invalid("private file exceeds its size limit"));
    }
    match write_private_new_with_barrier(path, data, |target| {
        barrier(target).map_err(durability_unknown)
    }) {
        Ok(()) => Ok(()),
        Err(error) if error.code == "PRIVATE_FILE_ALREADY_EXISTS" => {
            read_private_durable_with_barrier(path, maximum, Some(data), barrier).map(|_| ())
        }
        Err(error) => Err(error),
    }
}

/// Read bounded regular bytes only after re-establishing their durability.
/// Retain and recheck the opened identity and exact bytes across the boundary.
/// Unix syncs the file and its parent; Windows flushes the retained file handle
/// (opened for write access), with no claim of Unix directory-fsync equivalence.
/// New Windows publication still uses the existing write-through move.
pub fn read_private_durable(path: &Path, maximum: u64) -> Result<Vec<u8>> {
    read_private_durable_with_barrier(path, maximum, None, durability_barrier)
}

fn read_private_durable_with_barrier(
    path: &Path,
    maximum: u64,
    expected: Option<&[u8]>,
    barrier: impl FnOnce(&Path) -> Result<()>,
) -> Result<Vec<u8>> {
    let target = private_target_path(path)?;
    if !target_is_regular_file(&target)? {
        return Err(std::io::Error::from(std::io::ErrorKind::NotFound).into());
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT;
        options
            .write(true)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
    }
    let mut file = options.open(&target)?;
    let changed = || {
        Error::new(
            "PRIVATE_FILE_CHANGED",
            "private file changed across durability boundary",
        )
    };
    if !opened_regular_file_matches_path(&file, &target)? {
        return Err(changed());
    }
    let bytes = read_bounded_file(&mut file, maximum)?;
    if expected.is_some_and(|expected| bytes != expected) {
        return Err(Error::conflict("retained private file bytes differ"));
    }
    file.sync_all()
        .map_err(|error| durability_unknown(error.into()))?;
    barrier(&target).map_err(durability_unknown)?;
    if !opened_regular_file_matches_path(&file, &target)? {
        return Err(changed());
    }
    file.rewind()?;
    if read_bounded_file(&mut file, maximum)? != bytes
        || !opened_regular_file_matches_path(&file, &target)?
    {
        return Err(changed());
    }
    Ok(bytes)
}

fn read_bounded_file(file: &mut fs::File, maximum: u64) -> Result<Vec<u8>> {
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > maximum {
        return Err(Error::invalid("private file must be bounded and regular"));
    }
    let mut bytes = Vec::new();
    file.take(maximum.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > maximum {
        return Err(Error::invalid("private file exceeds its size limit"));
    }
    Ok(bytes)
}

fn durability_unknown(error: Error) -> Error {
    Error::new(
        "PRIVATE_FILE_DURABILITY_UNKNOWN",
        format!("private file durability boundary failed: {}", error.code),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RemovalDurability {
    Confirmed,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RemovalOutcome {
    pub removed: bool,
    pub durability: RemovalDurability,
}

fn private_file_exists() -> Error {
    Error::new("PRIVATE_FILE_ALREADY_EXISTS", "private file already exists")
}

#[cfg(not(windows))]
fn publication_error(error: std::io::Error) -> Error {
    if error.kind() == std::io::ErrorKind::AlreadyExists {
        private_file_exists()
    } else {
        error.into()
    }
}

/// Atomically replace a regular private file using a synced same-directory
/// temporary file. The target may be absent; a symlink, reparse point, or
/// non-file target is rejected. Unix syncs the parent directory after the
/// replacement. Windows uses `MOVEFILE_WRITE_THROUGH` without claiming an
/// equivalent directory fsync.
pub fn replace_private_durable(path: &Path, data: &[u8]) -> Result<()> {
    replace_private_durable_with_barrier(path, data, sync_parent_directory)
}

fn replace_private_durable_with_barrier(
    path: &Path,
    data: &[u8],
    barrier: impl FnOnce(&Path) -> Result<()>,
) -> Result<()> {
    let target = private_target_path(path)?;
    let _ = target_is_regular_file(&target)?;
    let temp = private_temp_path(&target)?;
    let result = (|| {
        let mut file = create_private_temp(&temp)?;
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        publish_replace(&temp, &target)?;
        barrier(&target)
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

/// Remove an exact regular private file and report the platform's durability
/// boundary. Unix syncs the parent directory after unlinking or confirming
/// that the path is already absent. Windows performs the file removal but
/// reports `Unknown`: its documented write-through move does not establish an
/// equivalent parent-directory barrier for `DeleteFile`.
pub fn remove_private_acknowledged(path: &Path) -> Result<RemovalOutcome> {
    remove_private_acknowledged_with_barrier(path, sync_parent_directory)
}

fn remove_private_acknowledged_with_barrier(
    path: &Path,
    barrier: impl FnOnce(&Path) -> Result<()>,
) -> Result<RemovalOutcome> {
    let target = private_target_path(path)?;
    let existed = target_is_regular_file(&target)?;
    #[cfg(unix)]
    {
        if existed {
            fs::remove_file(&target)?;
        }
        barrier(&target).map_err(durability_unknown)?;
        Ok(RemovalOutcome {
            removed: existed,
            durability: RemovalDurability::Confirmed,
        })
    }
    #[cfg(windows)]
    {
        let _ = barrier;
        if existed {
            fs::remove_file(&target)?;
        }
        Ok(RemovalOutcome {
            removed: existed,
            durability: RemovalDurability::Unknown,
        })
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = barrier;
        let _ = existed;
        Err(Error::new(
            "DURABILITY_UNSUPPORTED",
            "acknowledged removal requires a parent-directory durability barrier unavailable on this platform",
        ))
    }
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
    fs::hard_link(temp, target).map_err(publication_error)?;
    fs::remove_file(temp)?;
    Ok(())
}

#[cfg(windows)]
fn publish_new(temp: &Path, target: &Path) -> Result<()> {
    windows::move_file(temp, target, false)
}

#[cfg(not(any(unix, windows)))]
fn publish_new(temp: &Path, target: &Path) -> Result<()> {
    fs::hard_link(temp, target).map_err(publication_error)?;
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
        if ok == 0 {
            let error = std::io::Error::last_os_error();
            if !replace && error.kind() == std::io::ErrorKind::AlreadyExists {
                Err(super::private_file_exists())
            } else {
                Err(error.into())
            }
        } else {
            Ok(())
        }
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

#[cfg(all(test, any(unix, windows)))]
mod publication_tests {
    use super::*;

    fn fixture() -> PathBuf {
        let root =
            std::env::temp_dir().join(format!("swarm-durable-exact-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        root
    }

    fn failed_barrier(_: &Path) -> Result<()> {
        Err(Error::new(
            "INJECTED_SYNC_FAILURE",
            "injected durability boundary failure",
        ))
    }

    #[test]
    fn atomic_replacement_reports_barrier_failure_without_partial_publication() {
        let root = fixture();
        let path = root.join("journal.jsonl");
        write_private_new(&path, b"old complete record\n").unwrap();
        let replacement = b"replacement complete record\n";
        let mut barrier_calls = 0;
        let error = replace_private_durable_with_barrier(&path, replacement, |target| {
            assert_eq!(target, path.as_path());
            barrier_calls += 1;
            assert_eq!(fs::read(target).unwrap(), replacement);
            failed_barrier(target)
        })
        .unwrap_err();

        assert_eq!(error.code, "INJECTED_SYNC_FAILURE");
        assert_eq!(barrier_calls, 1);
        assert_eq!(fs::read(&path).unwrap(), replacement);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);

        replace_private_durable(&path, replacement).unwrap();
        assert_eq!(fs::read(&path).unwrap(), replacement);
        assert_eq!(fs::read_dir(&root).unwrap().count(), 1);
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn acknowledged_removal_reports_parent_barrier_failure_and_is_idempotent() {
        let root = fixture();
        let path = root.join("pending.json");
        fs::write(&path, b"acknowledged payload").unwrap();

        let mut barrier_calls = 0;
        let error = remove_private_acknowledged_with_barrier(&path, |target| {
            assert_eq!(target, path.as_path());
            barrier_calls += 1;
            failed_barrier(target)
        })
        .unwrap_err();
        assert_eq!(error.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
        assert_eq!(barrier_calls, 1);
        assert!(!path.exists());

        let mut barrier_calls = 0;
        let error = remove_private_acknowledged_with_barrier(&path, |target| {
            assert_eq!(target, path.as_path());
            barrier_calls += 1;
            failed_barrier(target)
        })
        .unwrap_err();
        assert_eq!(error.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
        assert_eq!(barrier_calls, 1);
        let absent = remove_private_acknowledged(&path).unwrap();
        assert!(!absent.removed);
        assert_eq!(absent.durability, RemovalDurability::Confirmed);
        fs::write(&path, b"acknowledged payload").unwrap();
        let removed = remove_private_acknowledged(&path).unwrap();
        assert!(removed.removed);
        assert_eq!(removed.durability, RemovalDurability::Confirmed);
        assert!(!path.exists());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn acknowledged_removal_reports_unknown_after_windows_namespace_delete() {
        let root = fixture();
        let path = root.join("pending.json");
        fs::write(&path, b"acknowledged payload").unwrap();

        let removed = remove_private_acknowledged_with_barrier(&path, |_| {
            panic!("Windows removal must not invoke a parent barrier")
        })
        .unwrap();
        assert!(removed.removed);
        assert_eq!(removed.durability, RemovalDurability::Unknown);
        assert!(!path.exists());

        let absent = root.join("already-absent.json");
        let absent = remove_private_acknowledged_with_barrier(&absent, |_| {
            panic!("Windows removal must report an unbarriered absence as unknown")
        })
        .unwrap();
        assert!(!absent.removed);
        assert_eq!(absent.durability, RemovalDurability::Unknown);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exact_publication_failure_stays_unknown_until_retry_barrier_succeeds() {
        let root = fixture();
        let path = root.join("go.json");
        let error =
            write_private_exact_with_barrier(&path, b"allow", 32, failed_barrier).unwrap_err();
        assert_eq!(error.code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
        assert_eq!(fs::read(&path).unwrap(), b"allow");
        assert_eq!(
            write_private_exact_with_barrier(&path, b"allow", 32, failed_barrier)
                .unwrap_err()
                .code,
            "PRIVATE_FILE_DURABILITY_UNKNOWN"
        );
        let mut barrier_calls = 0;
        write_private_exact_with_barrier(&path, b"allow", 32, |target| {
            barrier_calls += 1;
            sync_parent_directory(target)
        })
        .unwrap();
        assert_eq!(barrier_calls, 1);
        assert_eq!(read_private_durable(&path, 32).unwrap(), b"allow");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn visible_authority_and_release_receipts_require_read_barrier() {
        let root = fixture();
        for name in [
            "start-decision.json",
            "go.json",
            "terminal.json",
            "completion.json",
            "release-evidence.json",
        ] {
            let path = root.join(name);
            assert!(
                write_private_exact_with_barrier(&path, b"retained", 32, failed_barrier).is_err()
            );
            assert_eq!(
                read_private_durable_with_barrier(&path, 32, None, failed_barrier)
                    .unwrap_err()
                    .code,
                "PRIVATE_FILE_DURABILITY_UNKNOWN"
            );
            assert_eq!(read_private_durable(&path, 32).unwrap(), b"retained");
        }
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn concurrent_same_bytes_cannot_skip_its_own_barrier() {
        use std::{sync::mpsc, time::Duration};
        let root = fixture();
        let path = root.join("decision.json");
        let writer_path = path.clone();
        let (visible_tx, visible_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let writer = std::thread::spawn(move || {
            write_private_exact_with_barrier(&writer_path, b"allow", 32, |target| {
                visible_tx.send(()).unwrap();
                resume_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                sync_parent_directory(target)
            })
        });
        visible_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let publisher = write_private_exact_with_barrier(&path, b"allow", 32, failed_barrier);
        let reader = read_private_durable_with_barrier(&path, 32, None, failed_barrier);
        resume_tx.send(()).unwrap();
        writer.join().unwrap().unwrap();
        assert_eq!(
            publisher.unwrap_err().code,
            "PRIVATE_FILE_DURABILITY_UNKNOWN"
        );
        assert_eq!(reader.unwrap_err().code, "PRIVATE_FILE_DURABILITY_UNKNOWN");
        write_private_exact(&path, b"allow", 32).unwrap();
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn exact_conflicting_and_oversized_bytes_never_overwrite_or_sync() {
        let root = fixture();
        let path = root.join("receipt.json");
        write_private_exact(&path, b"original", 32).unwrap();
        let mut barrier_calls = 0;
        let error = write_private_exact_with_barrier(&path, b"changed", 32, |_| {
            barrier_calls += 1;
            Ok(())
        })
        .unwrap_err();
        assert_eq!(error.code, "CONFLICT");
        assert_eq!(barrier_calls, 0);
        assert!(write_private_exact(&path, b"too long", 1).is_err());
        assert!(read_private_durable(&path, 1).is_err());
        assert_eq!(fs::read(&path).unwrap(), b"original");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn durable_read_rejects_identical_replacement_during_barrier() {
        let root = fixture();
        let path = root.join("receipt.json");
        write_private_exact(&path, b"original", 32).unwrap();
        let error = read_private_durable_with_barrier(&path, 32, None, |target| {
            fs::rename(target, root.join("previous.json"))?;
            write_private_new(target, b"original")
        })
        .unwrap_err();
        assert_eq!(error.code, "PRIVATE_FILE_CHANGED");
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn exact_convergence_rejects_symlink_and_nonregular_targets() {
        let root = fixture();
        let source = root.join("source.json");
        let link = root.join("link.json");
        write_private_exact(&source, b"original", 32).unwrap();
        std::os::unix::fs::symlink(&source, &link).unwrap();
        assert!(write_private_exact(&link, b"original", 32).is_err());
        assert!(read_private_durable(&link, 32).is_err());
        assert!(write_private_exact(&root, b"original", 32).is_err());
        fs::remove_dir_all(root).unwrap();
    }

    #[cfg(windows)]
    #[test]
    fn windows_write_through_publication_and_existing_exact_convergence() {
        let root = fixture();
        let path = root.join("receipt.json");
        write_private_exact(&path, b"original", 32).unwrap();
        assert_eq!(
            write_private_new(&path, b"original").unwrap_err().code,
            "PRIVATE_FILE_ALREADY_EXISTS"
        );
        write_private_exact(&path, b"original", 32).unwrap();
        assert_eq!(read_private_durable(&path, 32).unwrap(), b"original");
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn existing_target_never_clobbers_and_publication_race_is_explicit() {
        let root = std::env::temp_dir().join(format!("swarm-publication-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        let target = root.join("receipt.json");
        write_private_new(&target, b"original").unwrap();
        assert_eq!(
            write_private_new(&target, b"changed").unwrap_err().code,
            "PRIVATE_FILE_ALREADY_EXISTS"
        );
        let temp = root.join("racing.tmp");
        fs::write(&temp, b"racing").unwrap();
        assert_eq!(
            publish_new(&temp, &target).unwrap_err().code,
            "PRIVATE_FILE_ALREADY_EXISTS"
        );
        assert_eq!(fs::read(&target).unwrap(), b"original");
        assert_eq!(fs::read(&temp).unwrap(), b"racing");
        assert!(write_private_new(&root, b"not-a-regular-file").is_err());
        fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(all(test, any(unix, windows)))]
mod identity_tests {
    use super::*;

    #[test]
    fn opened_file_identity_rejects_replacement_with_identical_bytes() {
        let root =
            std::env::temp_dir().join(format!("swarm-file-identity-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&root).unwrap();
        private_permissions(&root, true).unwrap();
        let path = root.join("journal.jsonl");
        write_private_new(&path, b"immutable prefix\n").unwrap();
        let opened = fs::File::open(&path).unwrap();
        assert!(opened_regular_file_matches_path(&opened, &path).unwrap());

        let prior_path = root.join("prior-journal.jsonl");
        fs::rename(&path, &prior_path).unwrap();
        write_private_new(&path, b"immutable prefix\n").unwrap();
        assert!(!opened_regular_file_matches_path(&opened, &path).unwrap());
        let current = fs::File::open(&path).unwrap();
        assert!(opened_regular_file_matches_path(&current, &path).unwrap());
        drop(current);
        drop(opened);
        fs::remove_file(&path).unwrap();
        fs::remove_file(&prior_path).unwrap();
        fs::remove_dir(&root).unwrap();
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
