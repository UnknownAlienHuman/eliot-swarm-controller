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

    /// Temp write/sync precedes the final handle identity/revision recheck.
    /// Caller holds the daemon save mutex through this entire operation.
    pub(crate) fn replace(
        &self,
        name: &str,
        original: &File,
        expected: &[u8],
        bytes: &[u8],
    ) -> Result<()> {
        let temporary = format!(".{}.tmp", ulid::Ulid::new());
        let mut temp = native::open(self.handle(), temporary.as_ref(), false, true).map_err(io)?;
        let mut replaced = false;
        let result = (|| {
            temp.write_all(bytes).map_err(io)?;
            temp.sync_all().map_err(io)?;
            let mut current = self.file(name).map_err(|_| conflict())?;
            if !native::same(original, &current).map_err(io)? || read(&mut current)? != expected {
                return Err(conflict());
            }
            // Directory handle + single leaf name: never follows an ancestor.
            native::replace(self.handle(), &temp, &temporary, name)
                .map_err(|error| Error::new("unavailable", format!("script rename: {error}")))?;
            replaced = true;
            #[cfg(unix)]
            {
                // POSIX renameat names its source instead of consuming the
                // open inode. Do not report success for a substituted source.
                let verify = || -> Result<()> {
                    let mut installed = self.file(name)?;
                    if !native::same(&temp, &installed).map_err(io)?
                        || read(&mut installed)? != bytes
                    {
                        return Err(conflict());
                    }
                    Ok(())
                };
                verify().map_err(|_| {
                    Error::new(
                        "unknown",
                        "script replacement identity or bytes could not be verified",
                    )
                })?;
            }
            native::sync(self.handle())
                .map_err(|_| Error::new("unknown", "script replaced but directory sync failed"))
        })();
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
        ffi::OsStrExt,
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
        let share = if directory {
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
                access,
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
    pub(super) fn remove(_: &File, _: &File, _: &str) {
        // unlinkat cannot consume the open inode. A name recheck followed by
        // unlink still races another directory writer; leave a failed temp for
        // explicit owner cleanup rather than possibly deleting their entry.
    }
    pub(super) fn sync(parent: &File) -> io::Result<()> {
        parent.sync_all()
    }
}
