//! Directory-relative script I/O with pinned ancestors.
//! Windows: Microsoft Learn NtCreateFile/OBJECT_ATTRIBUTES and FILE_RENAME_INFORMATION.
//! Unix: existing libc openat/renameat; external namespace limits are in the spec.
#![allow(unsafe_code)]

use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Component, Path, PathBuf};

use eliot_core::{Error, Result, api::MAX_SCRIPT_EDIT_BYTES};

fn refused() -> Error {
    Error::new(
        "not_allowed",
        "script entry is unavailable, linked, busy or unsafe",
    )
}

fn io(error: std::io::Error) -> Error {
    Error::new("unavailable", format!("script file I/O: {error}"))
}

pub(crate) struct Directory {
    // Retain every ancestor, including the root. Windows denies directory
    // delete sharing, preventing ancestor rename/removal while held.
    chain: Vec<File>,
    pub(crate) path: PathBuf,
}

impl Directory {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        if !path.is_absolute() {
            return Err(refused());
        }
        let mut anchor = PathBuf::new();
        let mut names = Vec::new();
        for part in path.components() {
            match part {
                Component::Prefix(prefix) => {
                    // Remote rename protocols cannot use our RootDirectory
                    // contract. Accept local drive paths only, never UNC/devices.
                    if !matches!(
                        prefix.kind(),
                        std::path::Prefix::Disk(_) | std::path::Prefix::VerbatimDisk(_)
                    ) {
                        return Err(refused());
                    }
                    anchor.push(part);
                }
                Component::RootDir => anchor.push(part),
                Component::Normal(name) => names.push(name.to_owned()),
                _ => return Err(refused()),
            }
        }
        let mut chain = vec![native::anchor(&anchor).map_err(|_| refused())?];
        for name in names {
            let file =
                native::open(chain.last().unwrap(), &name, true, false).map_err(|_| refused())?;
            check(&file, true)?;
            chain.push(file);
        }
        check(chain.last().unwrap(), true)?;
        Ok(Self {
            chain,
            path: path.to_owned(),
        })
    }

    pub(crate) fn descend(mut self, relative: &str) -> Result<(Self, String)> {
        let mut parts: Vec<_> = relative.split('/').collect();
        let name = parts.pop().ok_or_else(refused)?;
        for part in parts {
            plain(part)?;
            let file =
                native::open(self.handle(), part.as_ref(), true, false).map_err(|_| refused())?;
            check(&file, true)?;
            self.chain.push(file);
            self.path.push(part);
        }
        plain(name)?;
        Ok((self, name.to_owned()))
    }

    fn handle(&self) -> &File {
        self.chain.last().unwrap()
    }

    pub(crate) fn file(&self, name: &str) -> Result<File> {
        plain(name)?;
        let file =
            native::open(self.handle(), name.as_ref(), false, false).map_err(|_| refused())?;
        check(&file, false)?;
        Ok(file)
    }

    /// Create a new directory relative to the pinned parent. Never adopt an
    /// existing directory as an Agent ownership root.
    pub(crate) fn create_directory(&self, name: &str) -> Result<Self> {
        plain(name)?;
        let file = native::open(self.handle(), name.as_ref(), true, true).map_err(io)?;
        check(&file, true)?;
        let mut chain = self
            .chain
            .iter()
            .map(File::try_clone)
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(io)?;
        chain.push(file);
        Ok(Self {
            chain,
            path: self.path.join(name),
        })
    }

    pub(crate) fn identity(&self) -> Result<String> {
        serde_json::to_string(&eliot_plugin::file_identity(self.handle()).map_err(io)?)
            .map_err(|_| refused())
    }

    /// Temp write/sync precedes the final handle identity/revision recheck.
    /// Caller holds the daemon save mutex through this entire operation.
    pub(crate) fn replace(
        &self,
        name: &str,
        original: &File,
        expected: &[u8],
        bytes: &[u8],
    ) -> Result<()> {
        // Preserve the existing entry's permission bits on the replacement
        // inode: the staged temporary starts 0600 inside a private 0700
        // staging directory on the same filesystem and would otherwise strip
        // execute and group/other readability on rename. Only
        // owner/group/other read/write/execute carry over; set-user-ID,
        // set-group-ID and sticky bits are never copied onto a saved edit.
        // New private artifacts keep their restrictive creation mode; this
        // path only replaces an existing project script. Staging in a
        // private directory (not a same-directory `.tmp`) means a rejected
        // save leaves no group/other-readable file behind.
        #[cfg(unix)]
        let preserved = native::permission_bits(original).map_err(io)? & 0o777;
        #[cfg(unix)]
        let stage_name = format!(".{}.stage", ulid::Ulid::new());
        #[cfg(unix)]
        let stage = native::open(self.handle(), stage_name.as_ref(), true, true).map_err(io)?;
        #[cfg(unix)]
        let temp_name = "data";
        #[cfg(unix)]
        let mut temp = native::open(&stage, temp_name.as_ref(), false, true).map_err(io)?;
        #[cfg(not(unix))]
        let temporary = format!(".{}.tmp", ulid::Ulid::new());
        #[cfg(not(unix))]
        let mut temp = native::open(self.handle(), temporary.as_ref(), false, true).map_err(io)?;
        let mut replaced = false;
        let result = (|| {
            temp.write_all(bytes).map_err(io)?;
            temp.sync_all().map_err(io)?;
            let mut current = self.file(name).map_err(|_| conflict())?;
            if !native::same(original, &current).map_err(io)? || read(&mut current)? != expected {
                return Err(conflict());
            }
            // The conflict check passed, so the staged replacement may now
            // carry the preserved mode; until this point it stayed 0600
            // inside the private staging directory.
            #[cfg(unix)]
            native::apply_mode(&temp, preserved).map_err(io)?;
            // Directory handle + single leaf name: never follows an ancestor.
            // The staging directory lives in the same parent, so the rename
            // stays on one filesystem.
            #[cfg(unix)]
            native::replace_staged(self.handle(), &stage, temp_name, name)
                .map_err(|error| Error::new("unavailable", format!("script rename: {error}")))?;
            #[cfg(not(unix))]
            native::replace(self.handle(), &temp, &temporary, name)
                .map_err(|error| Error::new("unavailable", format!("script rename: {error}")))?;
            replaced = true;
            #[cfg(unix)]
            {
                // POSIX renameat names its source instead of consuming the
                // open inode. Do not report success for a substituted source.
                // All 0o7777 bits are compared so an unexpected set-user-ID,
                // set-group-ID or sticky bit on the installed entry is
                // unknown, never success.
                let verify = || -> Result<()> {
                    let mut installed = self.file(name)?;
                    if !native::same(&temp, &installed).map_err(io)?
                        || read(&mut installed)? != bytes
                        || native::permission_bits(&installed).map_err(io)? != preserved
                    {
                        return Err(conflict());
                    }
                    Ok(())
                };
                verify().map_err(|_| {
                    Error::new(
                        "unknown",
                        "script replacement identity, bytes or mode could not be verified",
                    )
                })?;
            }
            native::sync(self.handle())
                .map_err(|_| Error::new("unknown", "script replaced but directory sync failed"))
        })();
        #[cfg(unix)]
        {
            if result.is_err() && !replaced {
                native::remove_staged(self.handle(), &stage, &stage_name, temp_name);
            } else {
                native::remove_stage_dir(self.handle(), &stage_name);
            }
        }
        #[cfg(not(unix))]
        if result.is_err() && !replaced {
            native::remove(self.handle(), &temp, &temporary);
        }
        result
    }
}

fn conflict() -> Error {
    Error::new(
        "conflict",
        "script changed before replacement; read it again",
    )
}

/// A complete bounded tree, opened exclusively before the first delete.
/// Ancestors cannot be renamed and leaves are opened relative to held parents.
/// No pathname unlink fallback is available on Unix.
#[cfg(windows)]
pub(crate) struct CleanupTree {
    _parent: Directory,
    entries: Vec<File>,
    pub(crate) identity: String,
    pub(crate) bytes: u64,
}

#[cfg(windows)]
impl CleanupTree {
    pub(crate) fn open(path: &Path) -> Result<Self> {
        if let Some(name) = path.file_name().and_then(|name| name.to_str()) {
            cleanup_name(name)?;
        }
        Self::open_inner(path, None)
    }

    /// Only the exact reciprocal .git pointer already read through a held
    /// handle may accompany a retained, clean Swarm worktree.
    pub(crate) fn open_worktree(path: &Path, marker_identity: &str) -> Result<Self> {
        Self::open_inner(path, Some(marker_identity))
    }

    fn open_inner(path: &Path, marker_identity: Option<&str>) -> Result<Self> {
        let parent = Directory::open(path.parent().ok_or_else(refused)?)?;
        let name = path.file_name().ok_or_else(refused)?;
        let root = native::open_cleanup(parent.handle(), name, true).map_err(|_| refused())?;
        check(&root, true)?;
        let identity = serde_json::to_string(&eliot_plugin::file_identity(&root).map_err(io)?)
            .map_err(|_| refused())?;
        let mut tree = Self {
            _parent: parent,
            entries: Vec::new(),
            identity,
            bytes: 0,
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        tree.visit(root, 0, deadline, marker_identity)?;
        Ok(tree)
    }

    fn visit(
        &mut self,
        directory: File,
        depth: usize,
        deadline: std::time::Instant,
        marker_identity: Option<&str>,
    ) -> Result<()> {
        if depth >= 64 {
            return Err(Error::new("unavailable", "cleanup depth bound"));
        }
        let children = native::children(&directory).map_err(io)?;
        if ["HEAD", "objects", "refs"].iter().all(|expected| {
            children
                .iter()
                .any(|(name, _)| name.eq_ignore_ascii_case(expected))
        }) {
            return Err(Error::new(
                "protected_content",
                "repository root is protected",
            ));
        }
        for (name, is_dir) in children {
            if self.entries.len() >= 10_000 || std::time::Instant::now() >= deadline {
                return Err(Error::new("unavailable", "cleanup tree exceeds scan bound"));
            }
            let text = name.to_str().ok_or_else(refused)?;
            plain(text)?;
            let marker = depth == 0 && !is_dir && text == ".git" && marker_identity.is_some();
            if !marker {
                cleanup_name(text)?;
            }
            let child = native::open_cleanup(&directory, &name, is_dir).map_err(|_| refused())?;
            if is_dir {
                check(&child, true)?;
            } else {
                check_cleanup_file(&child)?;
                if marker
                    && serde_json::to_string(&eliot_plugin::file_identity(&child).map_err(io)?)
                        .map_err(|_| refused())?
                        .as_str()
                        != marker_identity.unwrap()
                {
                    return Err(Error::new(
                        "identity_changed",
                        "worktree .git pointer changed",
                    ));
                }
            }
            if is_dir {
                self.visit(child, depth + 1, deadline, None)?;
            } else {
                let len = child.metadata().map_err(io)?.len();
                self.bytes = self.bytes.checked_add(len).ok_or_else(refused)?;
                self.entries.push(child);
            }
        }
        self.entries.push(directory);
        Ok(())
    }

    /// Every error after the first attempted disposition is unknown. No retry.
    pub(crate) fn remove(mut self) -> Result<u64> {
        for file in self.entries.drain(..) {
            let unknown = |_| {
                Error::new(
                    "unknown",
                    "cleanup validation failed after reservation; no replay",
                )
            };
            if file.metadata().map_err(unknown)?.is_file()
                && native::links(&file).map_err(unknown)? != 1
            {
                return Err(Error::new(
                    "unknown",
                    "link count changed after preflight; no replay",
                ));
            }
            native::delete_handle(&file)
                .map_err(|_| Error::new("unknown", "partial cleanup; no replay"))?;
            drop(file);
        }
        Ok(self.bytes)
    }
}

/// Existing directory identity with every ancestor pinned and no links followed.
pub(crate) fn directory_identity(path: &Path) -> Result<String> {
    let directory = Directory::open(path)?;
    serde_json::to_string(&eliot_plugin::file_identity(directory.handle()).map_err(io)?)
        .map_err(|_| refused())
}

#[cfg(all(test, windows))]
mod cleanup_tests {
    use super::*;
    use std::os::windows::fs::OpenOptionsExt;

    #[test]
    fn held_file_refuses_whole_tree_before_any_deletion() {
        let root = std::env::temp_dir().join(format!("eliot-cleanup-{}", ulid::Ulid::new()));
        let _temp_1 = crate::test_support::TempDir::own(root.clone());
        std::fs::create_dir(&root).unwrap();
        let path = root.join("held");
        std::fs::write(&path, b"owned").unwrap();
        let holder = std::fs::OpenOptions::new()
            .read(true)
            .share_mode(0)
            .open(&path)
            .unwrap();
        assert!(CleanupTree::open(&root).is_err());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 1);
        drop(holder);
        let tree = CleanupTree::open(&root).unwrap();
        assert_eq!(tree.bytes, 5);
        assert_eq!(tree.remove().unwrap(), 5);
        assert!(!root.exists());
    }

    #[test]
    fn hardlinked_pair_inside_tree_is_kept() {
        let root = std::env::temp_dir().join(format!("eliot-cleanup-{}", ulid::Ulid::new()));
        let _temp_2 = crate::test_support::TempDir::own(root.clone());
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("first"), b"1234567").unwrap();
        std::fs::hard_link(root.join("first"), root.join("second")).unwrap();
        assert_eq!(CleanupTree::open(&root).err().unwrap().code, "hard_link");
    }

    #[test]
    fn externally_linked_file_is_kept() {
        let parent = std::env::temp_dir().join(format!("eliot-cleanup-{}", ulid::Ulid::new()));
        let _temp_3 = crate::test_support::TempDir::own(parent.clone());
        let root = parent.join("tree");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("inner"), b"1234567").unwrap();
        std::fs::hard_link(root.join("inner"), parent.join("outer")).unwrap();
        assert_eq!(CleanupTree::open(&root).err().unwrap().code, "hard_link");
        assert!(root.join("inner").exists());
        assert_eq!(std::fs::read(parent.join("outer")).unwrap(), b"1234567");
    }

    #[test]
    fn cleanup_handles_prevent_leaf_and_ancestor_rename() {
        assert_eq!(
            native::CLEANUP_SHARE_MODE & windows_sys::Win32::Storage::FileSystem::FILE_SHARE_DELETE,
            0
        );
        let parent = std::env::temp_dir().join(format!("eliot-cleanup-{}", ulid::Ulid::new()));
        let _temp_4 = crate::test_support::TempDir::own(parent.clone());
        let root = parent.join("tree");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("leaf"), b"owned").unwrap();
        let tree = CleanupTree::open(&root).unwrap();
        assert!(std::fs::rename(root.join("leaf"), parent.join("escaped")).is_err());
        assert!(std::fs::rename(&root, parent.join("moved")).is_err());
        tree.remove().unwrap();
    }

    #[test]
    fn cleanup_refuses_nested_repository_and_protected_config_entries() {
        for name in [".git", ".codex", ".claude", ".config", ".ssh"] {
            let root = std::env::temp_dir().join(format!("eliot-cleanup-{}", ulid::Ulid::new()));
            let _temp_5 = crate::test_support::TempDir::own(root.clone());
            std::fs::create_dir_all(root.join("nested")).unwrap();
            drop(CleanupTree::open(&root).unwrap());
            std::fs::write(root.join("nested").join(name), b"protected").unwrap();
            assert_eq!(
                CleanupTree::open(&root).err().unwrap().code,
                "protected_content"
            );
            assert!(root.join("nested").join(name).exists());
        }
    }

    #[test]
    fn nested_tree_deletion_uses_opened_identity_and_counts_bytes() {
        let parent = std::env::temp_dir().join(format!("eliot-cleanup-{}", ulid::Ulid::new()));
        let _temp_6 = crate::test_support::TempDir::own(parent.clone());
        let root = parent.join("tree");
        std::fs::create_dir_all(root.join("nested")).unwrap();
        std::fs::write(root.join("nested/data"), b"1234567").unwrap();
        let original = directory_identity(&root).unwrap();
        std::fs::rename(&root, parent.join("original")).unwrap();
        std::fs::create_dir(&root).unwrap();
        assert_ne!(CleanupTree::open(&root).unwrap().identity, original);
        CleanupTree::open(&root).unwrap().remove().unwrap();
        let held = CleanupTree::open(&parent.join("original")).unwrap();
        assert_eq!(held.identity, original);
        assert_eq!(held.remove().unwrap(), 7);
        CleanupTree::open(&parent).unwrap().remove().unwrap();
    }
}

fn plain(name: &str) -> Result<()> {
    if name.is_empty()
        || matches!(name, "." | "..")
        || name.ends_with(['.', ' '])
        || name.contains(['/', '\\', ':', '\0'])
        || name.chars().any(char::is_control)
    {
        return Err(refused());
    }
    Ok(())
}

fn check(file: &File, directory: bool) -> Result<()> {
    let metadata = file.metadata().map_err(|_| refused())?;
    if crate::platform::is_reparse_point(&metadata)
        || (directory && !metadata.is_dir())
        || (!directory && !metadata.is_file())
        || (!directory && native::links(file).map_err(|_| refused())? != 1)
    {
        return Err(refused());
    }
    Ok(())
}

#[cfg(windows)]
fn cleanup_name(name: &str) -> Result<()> {
    if protected_cleanup_name(name) {
        return Err(Error::new(
            "protected_content",
            "repository or configuration entry is protected",
        ));
    }
    Ok(())
}

pub(crate) fn protected_cleanup_name(name: &str) -> bool {
    [
        ".git",
        ".codex",
        ".claude",
        ".config",
        ".ssh",
        ".muse",
        ".opencode",
        ".antigravity",
    ]
    .iter()
    .any(|reserved| name.eq_ignore_ascii_case(reserved))
}

#[cfg(windows)]
fn check_cleanup_file(file: &File) -> Result<()> {
    let metadata = file.metadata().map_err(|_| refused())?;
    if crate::platform::is_reparse_point(&metadata) || !metadata.is_file() {
        return Err(refused());
    }
    if native::links(file).map_err(|_| refused())? != 1 {
        return Err(Error::new(
            "hard_link",
            "cleanup refuses files with multiple links",
        ));
    }
    Ok(())
}

/// At most the editor limit plus one byte, always from the verified handle.
pub(crate) fn read(file: &mut File) -> Result<Vec<u8>> {
    check(file, false)?;
    let before = file.metadata().map_err(io)?;
    file.rewind().map_err(io)?;
    let mut bytes = Vec::new();
    file.take((MAX_SCRIPT_EDIT_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(io)?;
    let after = file.metadata().map_err(io)?;
    check(file, false)?;
    if bytes.len() > MAX_SCRIPT_EDIT_BYTES {
        return Err(Error::new(
            "not_allowed",
            "script exceeds 256 KiB editor bound",
        ));
    }
    if before.len() != bytes.len() as u64
        || after.len() != before.len()
        || after.modified().ok() != before.modified().ok()
    {
        return Err(conflict());
    }
    Ok(bytes)
}

#[cfg(windows)]
mod native {
    use std::ffi::OsStr;
    use std::fs::{File, OpenOptions};
    use std::io;
    use std::os::windows::{
        ffi::{OsStrExt, OsStringExt},
        fs::OpenOptionsExt,
        io::{AsRawHandle, FromRawHandle},
    };
    use std::path::Path;
    use windows_sys::Wdk::Foundation::OBJECT_ATTRIBUTES;
    use windows_sys::Wdk::Storage::FileSystem::{
        FILE_CREATE, FILE_DIRECTORY_FILE, FILE_NON_DIRECTORY_FILE, FILE_OPEN,
        FILE_OPEN_REPARSE_POINT, FILE_SYNCHRONOUS_IO_NONALERT, NtCreateFile,
    };
    use windows_sys::Win32::Foundation::{
        GENERIC_READ, GENERIC_WRITE, OBJ_CASE_INSENSITIVE, OBJ_DONT_REPARSE, UNICODE_STRING,
    };
    use windows_sys::Win32::Storage::FileSystem::*;
    use windows_sys::Win32::System::IO::IO_STATUS_BLOCK;

    pub(super) const CLEANUP_SHARE_MODE: u32 = 0;

    pub(super) fn anchor(path: &Path) -> io::Result<File> {
        // GetDriveTypeW return value from Microsoft Learn; the binding's named
        // constant lives in an otherwise unneeded WindowsProgramming feature.
        const DRIVE_REMOTE: u32 = 4;
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        if unsafe { GetDriveTypeW(wide.as_ptr()) } == DRIVE_REMOTE {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "remote script roots are unsupported",
            ));
        }
        OpenOptions::new()
            .access_mode(FILE_READ_ATTRIBUTES | FILE_TRAVERSE | SYNCHRONIZE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
    }

    pub(super) fn open(
        parent: &File,
        name: &OsStr,
        directory: bool,
        create: bool,
    ) -> io::Result<File> {
        open_mode(parent, name, directory, create, false)
    }

    pub(super) fn open_cleanup(parent: &File, name: &OsStr, directory: bool) -> io::Result<File> {
        open_mode(parent, name, directory, false, true)
    }

    fn open_mode(
        parent: &File,
        name: &OsStr,
        directory: bool,
        create: bool,
        cleanup: bool,
    ) -> io::Result<File> {
        let mut wide: Vec<u16> = name.encode_wide().collect();
        let length =
            u16::try_from(wide.len() * 2).map_err(|_| io::Error::other("name too long"))?;
        let mut string = UNICODE_STRING {
            Length: length,
            MaximumLength: length,
            Buffer: wide.as_mut_ptr(),
        };
        let attributes = OBJECT_ATTRIBUTES {
            Length: std::mem::size_of::<OBJECT_ATTRIBUTES>() as u32,
            RootDirectory: parent.as_raw_handle(),
            ObjectName: &mut string,
            Attributes: OBJ_CASE_INSENSITIVE | OBJ_DONT_REPARSE,
            SecurityDescriptor: std::ptr::null_mut(),
            SecurityQualityOfService: std::ptr::null_mut(),
        };
        let mut handle = std::ptr::null_mut();
        let mut status_block: IO_STATUS_BLOCK = unsafe { std::mem::zeroed() };
        let access = if directory {
            FILE_READ_ATTRIBUTES | FILE_TRAVERSE | SYNCHRONIZE
        } else if create {
            GENERIC_READ | GENERIC_WRITE | DELETE | SYNCHRONIZE
        } else {
            GENERIC_READ | SYNCHRONIZE
        };
        // Deny ordinary file writes while read/revision handles remain open;
        // delete sharing is necessary for atomic replacement of an open entry.
        // Cleanup excludes rename/delete and writes until disposition/close.
        let share = if cleanup {
            CLEANUP_SHARE_MODE
        } else if directory {
            FILE_SHARE_READ | FILE_SHARE_WRITE
        } else {
            FILE_SHARE_READ | FILE_SHARE_DELETE
        };
        let options = FILE_SYNCHRONOUS_IO_NONALERT
            | FILE_OPEN_REPARSE_POINT
            | if directory {
                FILE_DIRECTORY_FILE
            } else {
                FILE_NON_DIRECTORY_FILE
            };
        // SAFETY: all buffers live through the synchronous call; returned handle
        // has one owner and is converted to File only on NT_SUCCESS.
        let status = unsafe {
            NtCreateFile(
                &mut handle,
                if cleanup {
                    GENERIC_READ | DELETE | SYNCHRONIZE
                } else {
                    access
                },
                &attributes,
                &mut status_block,
                std::ptr::null(),
                FILE_ATTRIBUTE_NORMAL,
                share,
                if create { FILE_CREATE } else { FILE_OPEN },
                options,
                std::ptr::null(),
                0,
            )
        };
        if status < 0 {
            return Err(io::Error::other(format!("NtCreateFile status {status:#x}")));
        }
        Ok(unsafe { File::from_raw_handle(handle) })
    }

    fn info(file: &File) -> io::Result<BY_HANDLE_FILE_INFORMATION> {
        let mut info = unsafe { std::mem::zeroed() };
        if unsafe { GetFileInformationByHandle(file.as_raw_handle(), &mut info) } == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(info)
    }
    pub(super) fn links(file: &File) -> io::Result<u64> {
        Ok(info(file)?.nNumberOfLinks as u64)
    }
    pub(super) fn same(a: &File, b: &File) -> io::Result<bool> {
        let a = info(a)?;
        let b = info(b)?;
        Ok((a.dwVolumeSerialNumber, a.nFileIndexHigh, a.nFileIndexLow)
            == (b.dwVolumeSerialNumber, b.nFileIndexHigh, b.nFileIndexLow))
    }
    pub(super) fn replace(parent: &File, temp: &File, _: &str, name: &str) -> io::Result<()> {
        use windows_sys::Wdk::Storage::FileSystem::{
            FILE_RENAME_INFORMATION, FILE_RENAME_POSIX_SEMANTICS, FILE_RENAME_REPLACE_IF_EXISTS,
            FileRenameInformationEx, NtSetInformationFile,
        };
        let wide: Vec<u16> = name.encode_utf16().collect();
        let length = std::mem::size_of::<FILE_RENAME_INFORMATION>() + wide.len() * 2;
        // usize backing provides the alignment required by FILE_RENAME_INFORMATION.
        let mut buffer = vec![0usize; length.div_ceil(std::mem::size_of::<usize>())];
        let rename = buffer.as_mut_ptr().cast::<FILE_RENAME_INFORMATION>();
        unsafe {
            (*rename).Anonymous.Flags = FILE_RENAME_REPLACE_IF_EXISTS | FILE_RENAME_POSIX_SEMANTICS;
            (*rename).RootDirectory = parent.as_raw_handle();
            (*rename).FileNameLength = (wide.len() * 2) as u32;
            std::ptr::copy_nonoverlapping(
                wide.as_ptr(),
                (*rename).FileName.as_mut_ptr(),
                wide.len(),
            );
            // Use the documented native rename form: the Win32 wrapper on
            // supported hosts can reject a non-null RootDirectory (error 87).
            // There is no pathname fallback.
            let mut status_block: IO_STATUS_BLOCK = std::mem::zeroed();
            let status = NtSetInformationFile(
                temp.as_raw_handle(),
                &mut status_block,
                rename.cast(),
                length as u32,
                FileRenameInformationEx,
            );
            if status < 0 {
                return Err(io::Error::other(format!(
                    "NtSetInformationFile status {status:#x}"
                )));
            }
        }
        Ok(())
    }
    pub(super) fn remove(_: &File, temp: &File, _: &str) {
        let info = FILE_DISPOSITION_INFO { DeleteFile: true };
        unsafe {
            SetFileInformationByHandle(
                temp.as_raw_handle(),
                FileDispositionInfo,
                (&info as *const FILE_DISPOSITION_INFO).cast(),
                std::mem::size_of_val(&info) as u32,
            );
        }
    }
    pub(super) fn delete_handle(file: &File) -> io::Result<()> {
        let info = FILE_DISPOSITION_INFO { DeleteFile: true };
        if unsafe {
            SetFileInformationByHandle(
                file.as_raw_handle(),
                FileDispositionInfo,
                (&info as *const FILE_DISPOSITION_INFO).cast(),
                std::mem::size_of_val(&info) as u32,
            )
        } == 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    pub(super) fn children(file: &File) -> io::Result<Vec<(std::ffi::OsString, bool)>> {
        let mut result = Vec::new();
        let mut restart = true;
        loop {
            let mut buffer = vec![0u64; 8192];
            let class = if restart {
                FileIdBothDirectoryRestartInfo
            } else {
                FileIdBothDirectoryInfo
            };
            restart = false;
            if unsafe {
                GetFileInformationByHandleEx(
                    file.as_raw_handle(),
                    class,
                    buffer.as_mut_ptr().cast(),
                    (buffer.len() * 8) as u32,
                )
            } == 0
            {
                let error = io::Error::last_os_error();
                if error.raw_os_error()
                    == Some(windows_sys::Win32::Foundation::ERROR_NO_MORE_FILES as i32)
                {
                    break;
                }
                return Err(error);
            }
            let mut offset = 0usize;
            loop {
                let prefix = std::mem::offset_of!(FILE_ID_BOTH_DIR_INFO, FileName);
                if offset + prefix > buffer.len() * 8 {
                    return Err(io::Error::other("directory entry truncated"));
                }
                let entry = unsafe {
                    &*buffer
                        .as_ptr()
                        .cast::<u8>()
                        .add(offset)
                        .cast::<FILE_ID_BOTH_DIR_INFO>()
                };
                let length = entry.FileNameLength as usize;
                if !length.is_multiple_of(2) || offset + prefix + length > buffer.len() * 8 {
                    return Err(io::Error::other("directory name truncated"));
                }
                let name = std::ffi::OsString::from_wide(unsafe {
                    std::slice::from_raw_parts(entry.FileName.as_ptr(), length / 2)
                });
                if name != "." && name != ".." {
                    if entry.FileAttributes & FILE_ATTRIBUTE_REPARSE_POINT != 0
                        || result.len() >= 10_000
                    {
                        return Err(io::Error::other("linked or oversized cleanup tree"));
                    }
                    result.push((name, entry.FileAttributes & FILE_ATTRIBUTE_DIRECTORY != 0));
                }
                if entry.NextEntryOffset == 0 {
                    break;
                }
                if entry.NextEntryOffset as usize <= prefix {
                    return Err(io::Error::other("invalid directory entry"));
                }
                offset += entry.NextEntryOffset as usize;
            }
        }
        Ok(result)
    }
    pub(super) fn sync(_: &File) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(unix)]
mod native {
    use std::{
        ffi::{CString, OsStr},
        fs::{File, OpenOptions},
        io,
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::{
                ffi::OsStrExt,
                fs::{MetadataExt, OpenOptionsExt},
            },
        },
        path::Path,
    };
    pub(super) fn anchor(path: &Path) -> io::Result<File> {
        OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(path)
    }
    fn name(name: &OsStr) -> io::Result<CString> {
        CString::new(name.as_bytes()).map_err(io::Error::other)
    }
    pub(super) fn open(
        parent: &File,
        raw: &OsStr,
        directory: bool,
        create: bool,
    ) -> io::Result<File> {
        let name = name(raw)?;
        if directory
            && create
            && unsafe { libc::mkdirat(parent.as_raw_fd(), name.as_ptr(), 0o700) } != 0
        {
            return Err(io::Error::last_os_error());
        }
        let flags = libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if directory {
                libc::O_RDONLY | libc::O_DIRECTORY
            } else if create {
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL
            } else {
                libc::O_RDONLY
            };
        let fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags, 0o600) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(unsafe { File::from_raw_fd(fd) })
    }
    pub(super) fn links(file: &File) -> io::Result<u64> {
        Ok(file.metadata()?.nlink())
    }
    pub(super) fn same(a: &File, b: &File) -> io::Result<bool> {
        let a = a.metadata()?;
        let b = b.metadata()?;
        Ok((a.dev(), a.ino()) == (b.dev(), b.ino()))
    }
    #[allow(dead_code)]
    pub(super) fn replace(parent: &File, _: &File, temp: &str, target: &str) -> io::Result<()> {
        let a = name(OsStr::new(temp))?;
        let b = name(OsStr::new(target))?;
        if unsafe {
            libc::renameat(
                parent.as_raw_fd(),
                a.as_ptr(),
                parent.as_raw_fd(),
                b.as_ptr(),
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    /// Rename a staged replacement out of its private staging directory into
    /// the parent. Both directories live on the same filesystem (the stage
    /// is a child of the parent), so the rename stays atomic.
    pub(super) fn replace_staged(
        parent: &File,
        stage: &File,
        temp: &str,
        target: &str,
    ) -> io::Result<()> {
        let a = name(OsStr::new(temp))?;
        let b = name(OsStr::new(target))?;
        if unsafe {
            libc::renameat(
                stage.as_raw_fd(),
                a.as_ptr(),
                parent.as_raw_fd(),
                b.as_ptr(),
            )
        } != 0
        {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    /// Installed mode bits for the post-rename readback: all 0o7777 bits so
    /// an unexpected set-user-ID, set-group-ID or sticky bit is unknown,
    /// never success. The preserved value masks to 0o777 at the call site.
    pub(super) fn permission_bits(file: &File) -> io::Result<u32> {
        Ok(file.metadata()?.mode() & 0o7777)
    }
    pub(super) fn apply_mode(file: &File, mode: u32) -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(mode))
    }
    pub(super) fn remove(_: &File, _: &File, _: &str) {
        // unlinkat cannot consume the open inode. A name recheck followed by
        // unlink still races another directory writer; leave a failed temp for
        // explicit owner cleanup rather than possibly deleting their entry.
    }
    /// Remove a failed staged replacement: the staging directory name is a
    /// fresh ULID, so no other writer can hold the same names and both
    /// unlinks are safe. Best-effort; failures leave the private stage for
    /// explicit owner cleanup.
    pub(super) fn remove_staged(parent: &File, stage: &File, stage_name: &str, temp: &str) {
        if let Ok(temp_name) = name(OsStr::new(temp)) {
            unsafe {
                libc::unlinkat(stage.as_raw_fd(), temp_name.as_ptr(), 0);
            }
        }
        remove_stage_dir(parent, stage_name);
    }
    /// Remove the (now empty) staging directory after a successful rename.
    /// Best-effort; a leftover private 0700 directory never exposes entry
    /// bytes.
    pub(super) fn remove_stage_dir(parent: &File, stage_name: &str) {
        if let Ok(dir_name) = name(OsStr::new(stage_name)) {
            unsafe {
                libc::unlinkat(parent.as_raw_fd(), dir_name.as_ptr(), libc::AT_REMOVEDIR);
            }
        }
    }
    pub(super) fn sync(parent: &File) -> io::Result<()> {
        parent.sync_all()
    }
}
