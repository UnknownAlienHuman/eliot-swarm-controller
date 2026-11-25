//! NTFS name enumeration and handle pins for regenerable Cargo caches.

use super::{
    MAX_CLEANUP_TREE_ENTRIES, cleanup_file_size, cleanup_name, cleanup_open_error, native, plain,
};
use eliot_core::{Error, Result};
use std::collections::HashSet;
use std::ffi::OsString;
use std::fs::File;
use std::os::windows::{
    ffi::{OsStrExt, OsStringExt},
    io::AsRawHandle,
};
use std::path::{Component, Path, PathBuf};
use std::time::Instant;
use windows_sys::Win32::Foundation::{ERROR_HANDLE_EOF, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_DISPOSITION_FLAG_DELETE, FILE_DISPOSITION_FLAG_FORCE_IMAGE_SECTION_CHECK,
    FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE, FILE_DISPOSITION_FLAG_POSIX_SEMANTICS,
    FILE_NAME_NORMALIZED, FindClose, FindFirstFileNameW, FindNextFileNameW,
    GetFinalPathNameByHandleW, VOLUME_NAME_NONE,
};

// Win32 extended-length paths can use up to 32,767 UTF-16 code units.
const WIDE_PATH_CAPACITY: usize = 32_768;

pub(super) struct Scan<'a> {
    root: &'a File,
    disk_path: &'a Path,
    volume_path: PathBuf,
    buffer: Vec<u16>,
    pending: HashSet<PathBuf>,
}

impl<'a> Scan<'a> {
    pub(super) fn new(root: &'a File, disk_path: &'a Path) -> Result<Self> {
        let mut buffer = vec![0; WIDE_PATH_CAPACITY];
        // SAFETY: root and the writable buffer live through the call.
        let len = unsafe {
            GetFinalPathNameByHandleW(
                root.as_raw_handle(),
                buffer.as_mut_ptr(),
                buffer.len() as u32,
                FILE_NAME_NORMALIZED | VOLUME_NAME_NONE,
            )
        } as usize;
        if len == 0 || len >= buffer.len() {
            return Err(unverified());
        }
        let volume_path = PathBuf::from(OsString::from_wide(&buffer[..len]));
        if !volume_path.has_root()
            || !matches!(volume_path.components().next(), Some(Component::RootDir))
        {
            return Err(unverified());
        }
        Ok(Self {
            root,
            disk_path,
            volume_path,
            buffer,
            pending: HashSet::new(),
        })
    }

    /// A link pinned from an earlier name still has to appear in the guarded
    /// traversal. This prevents preopening an alias from bypassing tree rules.
    pub(super) fn visited(&mut self, relative: &Path) -> bool {
        self.pending.remove(relative)
    }

    pub(super) fn finish(self) -> Result<()> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(unverified())
        }
    }

    pub(super) fn pin(
        &mut self,
        primary: File,
        relative: &Path,
        count: u64,
        scanned: usize,
        deadline: Instant,
    ) -> Result<Group> {
        let count = usize::try_from(count).map_err(|_| unverified())?;
        if count < 2
            || count - 1 > MAX_CLEANUP_TREE_ENTRIES.saturating_sub(scanned + self.pending.len())
        {
            return Err(unverified());
        }
        let path = self.disk_path.join(relative);
        let mut names = names(&path, &mut self.buffer, &self.volume_path, count, deadline)?;
        if !names.remove(relative) {
            return Err(unverified());
        }
        let mut aliases = Vec::with_capacity(names.len());
        for name in names {
            if Instant::now() >= deadline || !self.pending.insert(name.clone()) {
                return Err(unverified());
            }
            let alias = native::open_cleanup_alias(self.root, name.as_os_str())
                .map_err(cleanup_open_error)?;
            cleanup_file_size(&alias)?;
            if !native::same(&alias, &primary).map_err(|_| unverified())? {
                return Err(unverified());
            }
            aliases.push(alias);
        }
        let group = Group { primary, aliases };
        group.validate()?;
        Ok(group)
    }
}

fn names(
    path: &Path,
    buffer: &mut [u16],
    volume_root: &Path,
    expected: usize,
    deadline: Instant,
) -> Result<HashSet<PathBuf>> {
    let path: Vec<_> = path.as_os_str().encode_wide().chain(Some(0)).collect();
    let mut len = buffer.len() as u32;
    // SAFETY: path is NUL-terminated and the output buffer has len elements.
    let raw = unsafe { FindFirstFileNameW(path.as_ptr(), 0, &mut len, buffer.as_mut_ptr()) };
    if raw == INVALID_HANDLE_VALUE {
        return Err(unverified());
    }
    let search = Search(raw);
    let mut found = HashSet::new();
    loop {
        if Instant::now() >= deadline || found.len() == expected {
            return Err(unverified());
        }
        let end = buffer
            .iter()
            .position(|unit| *unit == 0)
            .ok_or_else(unverified)?;
        let name = PathBuf::from(OsString::from_wide(&buffer[..end]));
        // Compare actual volume-relative names component by component. Case
        // folding could admit a sibling of a case-sensitive NTFS directory.
        let relative = name.strip_prefix(volume_root).map_err(|_| {
            Error::new(
                "external_hard_link",
                "Cargo target has a hard link outside its root",
            )
        })?;
        if relative.as_os_str().is_empty() {
            return Err(unverified());
        }
        for component in relative.components() {
            let Component::Normal(name) = component else {
                return Err(unverified());
            };
            let name = name.to_str().ok_or_else(unverified)?;
            plain(name)?;
            cleanup_name(name)?;
        }
        if !found.insert(relative.to_owned()) {
            return Err(unverified());
        }
        len = buffer.len() as u32;
        // SAFETY: search owns the live handle and the buffer is writable.
        if unsafe { FindNextFileNameW(search.0, &mut len, buffer.as_mut_ptr()) } == 0 {
            if std::io::Error::last_os_error().raw_os_error() != Some(ERROR_HANDLE_EOF as i32)
                || found.len() != expected
            {
                return Err(unverified());
            }
            return Ok(found);
        }
    }
}

struct Search(HANDLE);

impl Drop for Search {
    fn drop(&mut self) {
        // SAFETY: this is the unique owner of a FindFirstFileNameW handle.
        unsafe { FindClose(self.0) };
    }
}

pub(super) struct Group {
    primary: File,
    aliases: Vec<File>,
}

impl Group {
    pub(super) fn validate(&self) -> Result<()> {
        self.validate_count(self.aliases.len() + 1)
    }

    fn validate_count(&self, expected: usize) -> Result<()> {
        let count = native::links(&self.primary).map_err(|_| unverified())?;
        if count == expected as u64 {
            Ok(())
        } else {
            Err(unverified())
        }
    }

    pub(super) fn remove(mut self, cancelled: &impl Fn() -> bool) -> Result<()> {
        let unknown = || {
            Error::new(
                "unknown",
                "Cargo hard-link cleanup did not finish; no replay",
            )
        };
        while let Some(alias) = self.aliases.pop() {
            self.validate_count(self.aliases.len() + 2)
                .map_err(|_| unknown())?;
            if cancelled() {
                return Err(unknown());
            }
            native::delete_with_flags(
                &alias,
                FILE_DISPOSITION_FLAG_DELETE
                    | FILE_DISPOSITION_FLAG_POSIX_SEMANTICS
                    | FILE_DISPOSITION_FLAG_FORCE_IMAGE_SECTION_CHECK
                    | FILE_DISPOSITION_FLAG_IGNORE_READONLY_ATTRIBUTE,
            )
            .map_err(|_| unknown())?;
            // POSIX disposition removes this name when its handle closes.
            drop(alias);
        }
        self.validate().map_err(|_| unknown())?;
        if cancelled() {
            return Err(unknown());
        }
        native::delete_cleanup_entry(&self.primary).map_err(|_| unknown())
    }
}

pub(super) fn unverified() -> Error {
    Error::new(
        "hard_link_unverified",
        "Cargo target hard-link names could not be verified",
    )
}

pub(super) fn refuse_multiple_links() -> Error {
    Error::new("hard_link", "cleanup refuses files with multiple links")
}
