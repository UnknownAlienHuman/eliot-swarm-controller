//! Temporary Windows write fencing for managed worktree removal.

use eliot_core::{Error, Result};
use std::path::Path;

#[cfg(windows)]
#[allow(
    unsafe_code,
    reason = "Windows worktree admission requires this narrowly scoped handle API."
)]
mod windows {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::{
        collections::{BTreeMap, HashSet},
        fs::{self, File, OpenOptions},
        os::windows::fs::OpenOptionsExt,
        path::{Component, PathBuf},
        time::{Duration, Instant},
    };
    use windows_sys::Win32::{
        Foundation::{HLOCAL, LocalFree},
        Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT, SetSecurityInfo},
        Security::{
            DACL_SECURITY_INFORMATION, GetSecurityDescriptorControl, GetSecurityDescriptorDacl,
            IsValidAcl, PROTECTED_DACL_SECURITY_INFORMATION, SE_DACL_PROTECTED,
            UNPROTECTED_DACL_SECURITY_INFORMATION,
        },
        Storage::FileSystem::{
            FILE_ADD_FILE, FILE_ADD_SUBDIRECTORY, FILE_FLAG_BACKUP_SEMANTICS,
            FILE_FLAG_OPEN_REPARSE_POINT, FILE_LIST_DIRECTORY, FILE_READ_ATTRIBUTES,
            FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, READ_CONTROL, SYNCHRONIZE,
            WRITE_DAC,
        },
    };

    const MAX_ENTRIES: usize = 20_000;
    const MAX_DEPTH: usize = 64;
    const MAX_JOURNAL_BYTES: usize = 64 * 1024;
    const FENCE_TIME: Duration = Duration::from_secs(10);
    const DENY_CREATE_MASK: u32 = FILE_ADD_FILE | FILE_ADD_SUBDIRECTORY;
    const WORLD_DENY_ACE: [u8; 20] = [
        1,
        0,
        20,
        0,
        DENY_CREATE_MASK as u8,
        (DENY_CREATE_MASK >> 8) as u8,
        (DENY_CREATE_MASK >> 16) as u8,
        (DENY_CREATE_MASK >> 24) as u8,
        1,
        1,
        0,
        0,
        0,
        0,
        0,
        1, // SECURITY_WORLD_SID_AUTHORITY, one subauthority
        0,
        0,
        0,
        0, // SECURITY_WORLD_RID
    ];

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct Journal {
        version: u8,
        root_identity: String,
        acls: Vec<JournalAcl>,
        entries: Vec<JournalEntry>,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct JournalAcl {
        original_acl: String,
        original_protected: bool,
        fenced_acl: String,
    }

    #[derive(Clone, Debug, Deserialize, Serialize)]
    struct JournalEntry {
        relative_path: String,
        identity: String,
        acl_index: usize,
    }

    struct DirectoryEntry {
        relative_path: String,
        path: PathBuf,
        handle: File,
        identity: String,
        original_acl: Vec<u8>,
        original_protected: bool,
        fenced_acl: Vec<u8>,
    }

    pub(crate) struct WorktreeWriteGuard {
        root: PathBuf,
        root_identity: String,
        _parent: File,
        directories: Vec<DirectoryEntry>,
        _files: Vec<File>,
        inventory: Vec<(String, String)>,
        restored: bool,
    }

    impl WorktreeWriteGuard {
        /// Open and snapshot without changing ACLs. Persist journal() before activate().
        pub(crate) fn prepare(path: &Path) -> Result<Self> {
            let deadline = Instant::now() + FENCE_TIME;
            let root = fs::canonicalize(path).map_err(|_| refused())?;
            let parent = open_directory(root.parent().ok_or_else(refused)?, false)?;
            let mut guard = Self {
                root: root.clone(),
                root_identity: String::new(),
                _parent: parent,
                directories: Vec::new(),
                _files: Vec::new(),
                inventory: Vec::new(),
                restored: false,
            };
            guard.visit(&root, String::new(), 0, deadline)?;
            guard.root_identity = guard
                .directories
                .first()
                .ok_or_else(refused)?
                .identity
                .clone();
            let _ = guard.journal()?;
            Ok(guard)
        }

        /// Bounded snapshot to store in the existing workspace removal outcome.
        pub(crate) fn journal(&self) -> Result<serde_json::Value> {
            let mut acl_indexes = BTreeMap::new();
            let mut acls = Vec::new();
            let mut entries = Vec::with_capacity(self.directories.len());
            for entry in &self.directories {
                let key = (entry.original_acl.clone(), entry.original_protected);
                let index = *acl_indexes.entry(key.clone()).or_insert_with(|| {
                    let index = acls.len();
                    acls.push(JournalAcl {
                        original_acl: hex(&key.0),
                        original_protected: key.1,
                        fenced_acl: hex(&entry.fenced_acl),
                    });
                    index
                });
                entries.push(JournalEntry {
                    relative_path: entry.relative_path.clone(),
                    identity: entry.identity.clone(),
                    acl_index: index,
                });
            }
            let journal = Journal {
                version: 1,
                root_identity: self.root_identity.clone(),
                acls,
                entries,
            };
            let value = serde_json::to_value(journal)
                .map_err(|_| Error::new("internal", "worktree ACL journal encoding failed"))?;
            if serde_json::to_vec(&value).map_err(|_| refused())?.len() > MAX_JOURNAL_BYTES {
                return Err(refused());
            }
            Ok(value)
        }

        /// Apply deny ACEs only after the caller durably stored journal().
        pub(crate) fn activate(&mut self) -> Result<()> {
            let deadline = Instant::now() + FENCE_TIME;
            for entry in &mut self.directories {
                if Instant::now() >= deadline
                    || identity(&entry.handle)? != entry.identity
                    || read_acl(&entry.handle)?
                        != (entry.original_acl.clone(), entry.original_protected)
                {
                    return Err(refused());
                }
                set_acl(&entry.handle, &entry.fenced_acl, entry.original_protected)?;
                if read_acl(&entry.handle)? != (entry.fenced_acl.clone(), entry.original_protected)
                {
                    return Err(Error::new(
                        "unknown",
                        "worktree ACL fence could not be verified",
                    ));
                }
            }
            self.require_same_inventory(deadline)
        }

        pub(crate) fn restore(mut self) -> Result<()> {
            self.restore_all()?;
            self.restored = true;
            Ok(())
        }

        /// Keep the live deny ACL until explicit settlement or restart recovery.
        pub(crate) fn leave_active(&mut self) {
            self.restored = true;
        }

        /// Restore a journal left by restart. Missing entries were removed.
        pub(crate) fn recover(path: &Path, value: &serde_json::Value) -> Result<()> {
            let journal: Journal = serde_json::from_value(value.clone())
                .map_err(|_| Error::new("source_integrity", "worktree ACL journal is invalid"))?;
            if serde_json::to_vec(value).map_err(|_| refused())?.len() > MAX_JOURNAL_BYTES
                || journal.version != 1
                || journal.entries.len() > MAX_ENTRIES
                || journal.acls.len() > MAX_ENTRIES
                || journal
                    .entries
                    .iter()
                    .filter(|entry| entry.relative_path == ".")
                    .count()
                    != 1
            {
                return Err(Error::new(
                    "source_integrity",
                    "worktree ACL journal is invalid",
                ));
            }
            let root = match fs::canonicalize(path) {
                Ok(root) => root,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
                Err(_) => return Err(refused()),
            };
            let _parent = open_directory(root.parent().ok_or_else(refused)?, false)?;
            let mut handles = Vec::new();
            let mut seen = HashSet::new();
            for entry in &journal.entries {
                if !seen.insert(entry.relative_path.as_str()) {
                    return Err(Error::new(
                        "source_integrity",
                        "worktree ACL journal repeats a directory",
                    ));
                }
                let directory = journal_directory(&root, &entry.relative_path)?;
                let handle = match open_directory(&directory, true) {
                    Ok(handle) => handle,
                    Err(_) if path_is_missing(&directory) => continue,
                    Err(error) => return Err(error),
                };
                if identity(&handle)? != entry.identity
                    || (entry.relative_path == "." && entry.identity != journal.root_identity)
                {
                    return Err(Error::new(
                        "identity_changed",
                        "worktree directory was substituted",
                    ));
                }
                let acl = journal.acls.get(entry.acl_index).ok_or_else(|| {
                    Error::new("source_integrity", "worktree ACL journal index is invalid")
                })?;
                let original = unhex(&acl.original_acl)?;
                let fenced = unhex(&acl.fenced_acl)?;
                let current = read_acl(&handle)?;
                if current == (original.clone(), acl.original_protected) {
                    handles.push(handle);
                } else if current == (fenced, acl.original_protected) {
                    set_acl(&handle, &original, acl.original_protected)?;
                    if read_acl(&handle)? != (original, acl.original_protected) {
                        return Err(Error::new("unknown", "worktree ACL restoration failed"));
                    }
                    handles.push(handle);
                } else {
                    return Err(Error::new("unknown", "worktree ACL changed unexpectedly"));
                }
            }
            drop(handles);
            Ok(())
        }

        fn visit(
            &mut self,
            directory: &Path,
            relative: String,
            depth: usize,
            deadline: Instant,
        ) -> Result<()> {
            if depth >= MAX_DEPTH
                || Instant::now() >= deadline
                || self.inventory.len() >= MAX_ENTRIES
            {
                return Err(refused());
            }
            let handle = open_directory(directory, true)?;
            let object_id = identity(&handle)?;
            let (original_acl, original_protected) = read_acl(&handle)?;
            let fenced_acl = deny_create_acl(&original_acl)?;
            self.inventory.push((relative.clone(), object_id.clone()));
            self.directories.push(DirectoryEntry {
                relative_path: if relative.is_empty() {
                    ".".to_owned()
                } else {
                    relative.clone()
                },
                path: directory.to_owned(),
                handle,
                identity: object_id,
                original_acl,
                original_protected,
                fenced_acl,
            });
            for child in fs::read_dir(directory).map_err(|_| refused())? {
                if Instant::now() >= deadline || self.inventory.len() >= MAX_ENTRIES {
                    return Err(refused());
                }
                let child = child.map_err(|_| refused())?;
                let name = child.file_name().into_string().map_err(|_| refused())?;
                let child_path = child.path();
                let child_relative = if relative.is_empty() {
                    name
                } else {
                    format!("{relative}/{name}")
                };
                let kind = child.file_type().map_err(|_| refused())?;
                if kind.is_symlink() {
                    return Err(refused());
                }
                if kind.is_dir() {
                    self.visit(&child_path, child_relative, depth + 1, deadline)?;
                } else if kind.is_file() {
                    let file = open_file(&child_path)?;
                    self.inventory.push((child_relative, identity(&file)?));
                    self._files.push(file);
                } else {
                    return Err(refused());
                }
            }
            Ok(())
        }

        fn require_same_inventory(&self, deadline: Instant) -> Result<()> {
            let mut found = Vec::new();
            walk_inventory(&self.root, String::new(), 0, deadline, &mut found)?;
            found.sort();
            let mut expected = self.inventory.clone();
            expected.sort();
            if found != expected {
                return Err(Error::new(
                    "unknown",
                    "worktree entries changed while write fencing was installed",
                ));
            }
            Ok(())
        }

        fn restore_all(&mut self) -> Result<()> {
            for entry in self.directories.iter_mut().rev() {
                if path_is_missing(&entry.path) {
                    continue;
                }
                if identity(&entry.handle)? != entry.identity {
                    return Err(Error::new(
                        "identity_changed",
                        "worktree directory was substituted",
                    ));
                }
                let current = read_acl(&entry.handle)?;
                if current == (entry.original_acl.clone(), entry.original_protected) {
                } else if current == (entry.fenced_acl.clone(), entry.original_protected) {
                    set_acl(&entry.handle, &entry.original_acl, entry.original_protected)?;
                    if read_acl(&entry.handle)?
                        != (entry.original_acl.clone(), entry.original_protected)
                    {
                        return Err(Error::new("unknown", "worktree ACL restoration failed"));
                    }
                } else {
                    return Err(Error::new("unknown", "worktree ACL changed unexpectedly"));
                }
            }
            Ok(())
        }
    }

    impl Drop for WorktreeWriteGuard {
        fn drop(&mut self) {
            if !self.restored {
                let _ = self.restore_all();
            }
        }
    }

    fn walk_inventory(
        directory: &Path,
        relative: String,
        depth: usize,
        deadline: Instant,
        output: &mut Vec<(String, String)>,
    ) -> Result<()> {
        if depth >= MAX_DEPTH || Instant::now() >= deadline || output.len() >= MAX_ENTRIES {
            return Err(refused());
        }
        let handle = open_inventory_directory(directory)?;
        output.push((relative.clone(), identity(&handle)?));
        for child in fs::read_dir(directory).map_err(|_| refused())? {
            let child = child.map_err(|_| refused())?;
            let name = child.file_name().into_string().map_err(|_| refused())?;
            let path = child.path();
            let child_relative = if relative.is_empty() {
                name
            } else {
                format!("{relative}/{name}")
            };
            let kind = child.file_type().map_err(|_| refused())?;
            if kind.is_symlink() {
                return Err(refused());
            }
            if kind.is_dir() {
                walk_inventory(&path, child_relative, depth + 1, deadline, output)?;
            } else if kind.is_file() {
                let file = open_file(&path)?;
                output.push((child_relative, identity(&file)?));
            } else {
                return Err(refused());
            }
        }
        Ok(())
    }

    fn journal_directory(root: &Path, relative: &str) -> Result<PathBuf> {
        if relative == "." {
            return Ok(root.to_owned());
        }
        let mut path = root.to_owned();
        for component in Path::new(relative).components() {
            match component {
                Component::Normal(name) => path.push(name),
                _ => {
                    return Err(Error::new(
                        "source_integrity",
                        "worktree ACL path is invalid",
                    ));
                }
            }
        }
        if path.starts_with(root) {
            Ok(path)
        } else {
            Err(Error::new(
                "source_integrity",
                "worktree ACL path escaped root",
            ))
        }
    }

    fn path_is_missing(path: &Path) -> bool {
        matches!(fs::symlink_metadata(path), Err(error) if error.kind() == std::io::ErrorKind::NotFound)
    }

    fn open_directory(path: &Path, in_tree: bool) -> Result<File> {
        let share = if in_tree {
            FILE_SHARE_READ | FILE_SHARE_DELETE
        } else {
            FILE_SHARE_READ
        };
        let access = FILE_LIST_DIRECTORY
            | FILE_READ_ATTRIBUTES
            | SYNCHRONIZE
            | if in_tree { READ_CONTROL | WRITE_DAC } else { 0 };
        let handle = OpenOptions::new()
            .access_mode(access)
            .share_mode(share)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
            .map_err(|_| refused())?;
        let metadata = handle.metadata().map_err(|_| refused())?;
        if !metadata.is_dir() || crate::platform::is_reparse_point(&metadata) {
            return Err(refused());
        }
        Ok(handle)
    }

    fn open_inventory_directory(path: &Path) -> Result<File> {
        let handle = OpenOptions::new()
            .access_mode(FILE_LIST_DIRECTORY | FILE_READ_ATTRIBUTES | SYNCHRONIZE)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
            .map_err(|_| refused())?;
        let metadata = handle.metadata().map_err(|_| refused())?;
        if !metadata.is_dir() || crate::platform::is_reparse_point(&metadata) {
            return Err(refused());
        }
        Ok(handle)
    }

    fn open_file(path: &Path) -> Result<File> {
        let handle = OpenOptions::new()
            .read(true)
            .share_mode(FILE_SHARE_READ | FILE_SHARE_DELETE)
            .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
            .open(path)
            .map_err(|_| refused())?;
        let metadata = handle.metadata().map_err(|_| refused())?;
        if !metadata.is_file() || crate::platform::is_reparse_point(&metadata) {
            return Err(refused());
        }
        Ok(handle)
    }

    fn identity(handle: &File) -> Result<String> {
        serde_json::to_string(&eliot_plugin::file_identity(handle).map_err(|_| refused())?)
            .map_err(|_| refused())
    }

    fn read_acl(handle: &File) -> Result<(Vec<u8>, bool)> {
        let mut dacl = std::ptr::null_mut();
        let mut descriptor = std::ptr::null_mut();
        // SAFETY: handle is live and all output pointers refer to writable locals.
        let status = unsafe {
            GetSecurityInfo(
                std::os::windows::io::AsRawHandle::as_raw_handle(handle).cast(),
                SE_FILE_OBJECT,
                DACL_SECURITY_INFORMATION,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut dacl,
                std::ptr::null_mut(),
                &mut descriptor,
            )
        };
        if status != 0 {
            return Err(refused());
        }
        let result = (|| {
            if dacl.is_null() {
                return Err(refused());
            }
            let mut present = 0;
            let mut defaulted = 0;
            let mut pointer = std::ptr::null_mut();
            // SAFETY: descriptor and output pointers remain valid for the call.
            if unsafe {
                GetSecurityDescriptorDacl(descriptor, &mut present, &mut pointer, &mut defaulted)
            } == 0
                || present == 0
                || pointer.is_null()
            {
                return Err(refused());
            }
            // SAFETY: pointer is the DACL within the live security descriptor.
            let length = usize::from(unsafe { (*pointer).AclSize });
            if !(8..=u16::MAX as usize).contains(&length) || unsafe { IsValidAcl(pointer) } == 0 {
                return Err(refused());
            }
            // SAFETY: IsValidAcl validated the ACL and AclSize bounds the slice.
            let bytes =
                unsafe { std::slice::from_raw_parts(pointer.cast::<u8>(), length).to_vec() };
            let mut control = 0u16;
            let mut revision = 0u32;
            // SAFETY: descriptor is live and control/revision are writable outputs.
            if unsafe { GetSecurityDescriptorControl(descriptor, &mut control, &mut revision) } == 0
            {
                return Err(refused());
            }
            Ok((bytes, control & SE_DACL_PROTECTED != 0))
        })();
        // SAFETY: descriptor was allocated by GetSecurityInfo above.
        unsafe {
            LocalFree(descriptor as HLOCAL);
        }
        result
    }

    fn deny_create_acl(old: &[u8]) -> Result<Vec<u8>> {
        if old.len() < 8
            || old
                .len()
                .checked_add(WORLD_DENY_ACE.len())
                .is_none_or(|len| len > u16::MAX as usize)
        {
            return Err(refused());
        }
        let old_size = u16::from_le_bytes([old[2], old[3]]) as usize;
        let old_count = u16::from_le_bytes([old[4], old[5]]);
        if old_size != old.len() || old_count == u16::MAX {
            return Err(refused());
        }
        let mut result = Vec::with_capacity(old.len() + WORLD_DENY_ACE.len());
        result.extend_from_slice(&old[..2]);
        result.extend_from_slice(&((old.len() + WORLD_DENY_ACE.len()) as u16).to_le_bytes());
        result.extend_from_slice(&(old_count + 1).to_le_bytes());
        result.extend_from_slice(&old[6..8]);
        result.extend_from_slice(&WORLD_DENY_ACE);
        result.extend_from_slice(&old[8..]);
        Ok(result)
    }

    fn set_acl(handle: &File, bytes: &[u8], protected: bool) -> Result<()> {
        let storage = aligned_acl(bytes)?;
        let flags = DACL_SECURITY_INFORMATION
            | if protected {
                PROTECTED_DACL_SECURITY_INFORMATION
            } else {
                UNPROTECTED_DACL_SECURITY_INFORMATION
            };
        // SAFETY: handle is live and storage contains an aligned ACL of matching size.
        let status = unsafe {
            SetSecurityInfo(
                std::os::windows::io::AsRawHandle::as_raw_handle(handle).cast(),
                SE_FILE_OBJECT,
                flags,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                storage.as_ptr().cast(),
                std::ptr::null_mut(),
            )
        };
        if status == 0 { Ok(()) } else { Err(refused()) }
    }

    fn aligned_acl(bytes: &[u8]) -> Result<Vec<u32>> {
        if bytes.len() < 8
            || bytes.len() > u16::MAX as usize
            || !bytes.len().is_multiple_of(4)
            || usize::from(u16::from_le_bytes([bytes[2], bytes[3]])) != bytes.len()
        {
            return Err(refused());
        }
        let words: Vec<u32> = bytes
            .as_chunks::<4>()
            .0
            .iter()
            .map(|chunk| u32::from_ne_bytes(*chunk))
            .collect();
        // SAFETY: words are 4-byte aligned and the ACL size was validated above.
        if unsafe { IsValidAcl(words.as_ptr().cast()) } == 0 {
            return Err(refused());
        }
        Ok(words)
    }

    fn hex(bytes: &[u8]) -> String {
        const DIGITS: &[u8; 16] = b"0123456789abcdef";
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push(DIGITS[(byte >> 4) as usize] as char);
            out.push(DIGITS[(byte & 15) as usize] as char);
        }
        out
    }

    fn unhex(text: &str) -> Result<Vec<u8>> {
        if !text.len().is_multiple_of(2) || text.len() > u16::MAX as usize * 2 {
            return Err(Error::new(
                "source_integrity",
                "worktree ACL journal is invalid",
            ));
        }
        text.as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| {
                match (
                    (pair[0] as char).to_digit(16),
                    (pair[1] as char).to_digit(16),
                ) {
                    (Some(a), Some(b)) => Ok(((a << 4) | b) as u8),
                    _ => Err(Error::new(
                        "source_integrity",
                        "worktree ACL journal is invalid",
                    )),
                }
            })
            .collect()
    }

    fn refused() -> Error {
        Error::new(
            "busy",
            "worktree has a writer, unsafe entry, or could not be write-fenced",
        )
    }
}

#[cfg(windows)]
pub(crate) use windows::WorktreeWriteGuard;

#[cfg(not(windows))]
pub(crate) struct WorktreeWriteGuard;

#[cfg(not(windows))]
impl WorktreeWriteGuard {
    pub(crate) fn prepare(_path: &Path) -> Result<Self> {
        Err(Error::new(
            "unsupported",
            "worktree write fencing is unavailable",
        ))
    }

    pub(crate) fn journal(&self) -> Result<serde_json::Value> {
        Err(Error::new(
            "unsupported",
            "worktree ACL journaling is unavailable",
        ))
    }

    pub(crate) fn activate(&mut self) -> Result<()> {
        Err(Error::new(
            "unsupported",
            "worktree write fencing is unavailable",
        ))
    }

    pub(crate) fn leave_active(&mut self) {}

    pub(crate) fn restore(self) -> Result<()> {
        Err(Error::new(
            "unsupported",
            "worktree ACL restoration is unavailable",
        ))
    }

    pub(crate) fn recover(_path: &Path, _value: &serde_json::Value) -> Result<()> {
        Err(Error::new(
            "unsupported",
            "worktree ACL recovery is unavailable",
        ))
    }
}
