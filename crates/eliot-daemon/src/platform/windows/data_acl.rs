//! The Windows private-directory DACL policy and diagnostics.

use super::{
    DATA_DIR_ACL_UNREPAIRABLE_LOCATION, DATA_DIR_OWNER_LOCATION, LocalSecurityDescriptor,
    SecurityHandle, check_no_reparse_components, current_user_sid, foreign_owner, io_error,
    open_security_handle, security_handle_identity, sid_pointer, sid_text, sid_to_string,
    write_protected_dacl,
};
use crate::platform::ForeignDataDirAce;
use crate::platform::data_dir::DATA_DIR_ACL_LOCATION;
use eliot_core::{Error, Result, display_path};
use std::fmt::Write as _;
use std::path::Path;
use std::ptr::null_mut;
use std::slice;
use windows_sys::Win32::Foundation::ERROR_ACCESS_DENIED;
use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
use windows_sys::Win32::Security::{
    ACCESS_ALLOWED_ACE, ACE_HEADER, ACE_INHERITED_OBJECT_TYPE_PRESENT, ACE_OBJECT_TYPE_PRESENT,
    ACL, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION, EqualSid, GetAce,
    GetSecurityDescriptorControl, INHERITED_ACE, IsValidAcl, IsValidSid, OBJECT_INHERIT_ACE,
    OWNER_SECURITY_INFORMATION, PSID, SE_DACL_PROTECTED,
};
use windows_sys::Win32::Storage::FileSystem::{
    FILE_ALL_ACCESS, FILE_TRAVERSE, READ_CONTROL, SYNCHRONIZE, WRITE_DAC,
};
use windows_sys::Win32::System::SystemServices::{
    ACCESS_ALLOWED_ACE_TYPE, ACCESS_ALLOWED_CALLBACK_ACE_TYPE,
    ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE, ACCESS_ALLOWED_COMPOUND_ACE_TYPE,
    ACCESS_ALLOWED_OBJECT_ACE_TYPE, ACCESS_DENIED_ACE_TYPE, ACCESS_DENIED_CALLBACK_ACE_TYPE,
    ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE, ACCESS_DENIED_OBJECT_ACE_TYPE,
};

pub(crate) fn data_dir_acl_report(path: &Path) -> Result<Vec<ForeignDataDirAce>> {
    check_no_reparse_components(path)?;
    let handle = open_security_handle(path, READ_CONTROL, true)?;
    Ok(inspect_current_user_acl(path, &handle, true, false)?.entries)
}

pub(crate) fn check_acl_write_access(path: &Path, problem: &Error) -> Result<()> {
    check_no_reparse_components(path)?;
    open_security_handle(path, READ_CONTROL | WRITE_DAC, true)
        .map(drop)
        .map_err(|error| acl_repair_failed(problem, &error))
}

pub(super) fn validate_current_user_acl(path: &Path, directory: bool) -> Result<()> {
    let handle = open_security_handle(path, READ_CONTROL, directory)?;
    let acl = inspect_current_user_acl(path, &handle, directory, true)?;
    match acl.problem(path, directory) {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

pub(super) fn validate_store_file_acl(path: &Path) -> Result<()> {
    let handle = open_security_handle(path, READ_CONTROL, false)?;
    let acl = inspect_current_user_acl(path, &handle, false, true)?;
    if let Some(error) = acl.problem(path, false)
        && !acl.is_unprotected_inherited_owner_only()
    {
        return Err(error);
    }
    Ok(())
}

pub(crate) fn repair_managed_dir_acl(path: &Path) -> Result<Vec<ForeignDataDirAce>> {
    check_no_reparse_components(path)?;
    let handle = open_security_handle(path, READ_CONTROL, true)?;
    let acl = inspect_current_user_acl(path, &handle, true, true)?;
    let Some(problem) = acl.problem(path, true) else {
        return Ok(Vec::new());
    };

    // Reopen for writing and reread on that same handle. A concurrent repair
    // may already have restored the expected access, or ownership may differ.
    let original_identity = security_handle_identity(path, &handle)
        .map_err(|error| acl_repair_failed(&problem, &error))?;
    check_no_reparse_components(path).map_err(|error| acl_repair_failed(&problem, &error))?;
    let handle = open_security_handle(path, READ_CONTROL | WRITE_DAC, true)
        .map_err(|error| acl_repair_failed(&problem, &error))?;
    let reopened_identity = security_handle_identity(path, &handle)
        .map_err(|error| acl_repair_failed(&problem, &error))?;
    if reopened_identity != original_identity {
        let path_text = display_path(&path.to_string_lossy());
        let error = Error::new(
            "unsafe_data_dir",
            format!(
                "private data directory identity changed before ACL repair on {path_text} (original volume {:08x}, file index {:016x}; reopened volume {:08x}, file index {:016x})",
                original_identity.volume_serial,
                original_identity.file_index,
                reopened_identity.volume_serial,
                reopened_identity.file_index,
            ),
        );
        return Err(acl_repair_failed(&problem, &error));
    }
    let acl = inspect_current_user_acl(path, &handle, true, true)
        .map_err(|error| acl_repair_failed(&problem, &error))?;
    let Some(problem) = acl.problem(path, true) else {
        return Ok(Vec::new());
    };

    let owner_sid = acl.current_user_sid.clone();
    let mut sddl = format!("D:P(A;OICI;FA;;;{owner_sid})");
    for sid in acl
        .entries
        .iter()
        .filter(|entry| entry.allowed)
        .filter_map(|entry| entry.sid.as_deref())
    {
        let _ = write!(sddl, "(A;;0x{:08x};;;{sid})", FILE_TRAVERSE | SYNCHRONIZE);
    }
    write_protected_dacl(path, &handle, &sddl, true)
        .map_err(|error| acl_repair_failed(&problem, &error))?;
    let verified = inspect_current_user_acl(path, &handle, true, true)
        .map_err(|error| acl_repair_unrepairable(&problem, &error))?;
    if let Some(error) = verified.problem(path, true) {
        return Err(acl_repair_unrepairable(&problem, &error));
    }
    for entry in acl.entries.iter().filter(|entry| entry.allowed) {
        if !verified
            .entries
            .iter()
            .any(|retained| retained.sid == entry.sid)
        {
            let error = Error::new(
                "platform_acl",
                format!(
                    "retained traversal rights 0x{:08x} for SID {} could not be verified",
                    FILE_TRAVERSE | SYNCHRONIZE,
                    entry.sid.as_deref().unwrap_or("unavailable"),
                ),
            );
            return Err(acl_repair_unrepairable(&problem, &error));
        }
    }
    Ok(acl
        .entries
        .into_iter()
        .filter(|entry| !entry.allowed)
        .collect())
}

fn acl_repair_failed(problem: &Error, error: &Error) -> Error {
    let failure = Error::new(
        "unsafe_data_dir",
        format!(
            "could not repair private data ACL: {}; {}",
            problem.message, error.message
        ),
    );
    match error.failure_where.as_deref() {
        Some(where_) => failure.with_where(where_),
        None => failure,
    }
}

fn acl_repair_unrepairable(problem: &Error, error: &Error) -> Error {
    let failure = acl_repair_failed(problem, error);
    if error.failure_where.as_deref() == Some(DATA_DIR_OWNER_LOCATION) {
        failure
    } else {
        failure.with_where(DATA_DIR_ACL_UNREPAIRABLE_LOCATION)
    }
}

struct PrivateAcl {
    protected: bool,
    null_dacl: bool,
    has_owner_entry: bool,
    current_user_sid: String,
    entries: Vec<ForeignDataDirAce>,
}

impl PrivateAcl {
    fn is_unprotected_inherited_owner_only(&self) -> bool {
        !self.protected
            && !self.null_dacl
            && !self.has_owner_entry
            && self.entries.len() == 1
            && self.entries.first().is_some_and(|entry| {
                entry.sid.as_deref() == Some(self.current_user_sid.as_str())
                    && entry.rights == Some(FILE_ALL_ACCESS)
                    && u32::from(entry.ace_type) == ACCESS_ALLOWED_ACE_TYPE
                    && entry.flags == INHERITED_ACE as u8
                    && !entry.allowed
            })
    }

    fn problem(&self, path: &Path, directory: bool) -> Option<Error> {
        let error = |message| {
            let error = Error::new("unsafe_data_dir", message);
            if directory {
                error.with_where(DATA_DIR_ACL_LOCATION)
            } else {
                error
            }
        };
        let path = display_path(&path.to_string_lossy());
        if let Some(entry) = self.entries.iter().find(|entry| !entry.allowed) {
            let sid = entry.sid.as_deref().unwrap_or("unavailable");
            let rights = entry
                .rights
                .map_or_else(|| "unavailable".into(), |m| format!("0x{m:08x}"));
            return Some(error(format!(
                "private data ACL on {path} has an unexpected entry for SID {sid}, rights {rights}, type 0x{:02x}, flags 0x{:02x}",
                entry.ace_type, entry.flags,
            )));
        }
        let issue = if self.null_dacl {
            "has no DACL, granting unrestricted access"
        } else if !self.protected {
            "is not protected from inherited access"
        } else if !self.has_owner_entry {
            return Some(error(format!(
                "private data ACL on {path} is missing the required current-user full-access entry (SID {}, rights 0x{FILE_ALL_ACCESS:08x})",
                self.current_user_sid,
            )));
        } else {
            return None;
        };
        Some(error(format!(
            "private data ACL on {path} {issue} (expected current-user SID {}, rights 0x{FILE_ALL_ACCESS:08x})",
            self.current_user_sid,
        )))
    }
}

fn inspect_current_user_acl(
    path: &Path,
    handle: &SecurityHandle,
    directory: bool,
    require_owner: bool,
) -> Result<PrivateAcl> {
    let mut owner = null_mut();
    let mut dacl = null_mut();
    let mut descriptor = null_mut();
    // SAFETY: outputs are writable and the handle stays live for the call.
    let status = unsafe {
        GetSecurityInfo(
            handle.0,
            SE_FILE_OBJECT,
            OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
            &mut owner,
            null_mut(),
            &mut dacl,
            null_mut(),
            &mut descriptor,
        )
    };
    if status != 0 {
        let error = io_error(
            &format!(
                "read private data ACL on {}",
                display_path(&path.to_string_lossy())
            ),
            std::io::Error::from_raw_os_error(status as i32),
        );
        return Err(if directory && status == ERROR_ACCESS_DENIED {
            error.with_where(DATA_DIR_ACL_UNREPAIRABLE_LOCATION)
        } else {
            error
        });
    }
    let descriptor = LocalSecurityDescriptor(descriptor);
    let user_sid = current_user_sid()?;
    // SAFETY: both SIDs remain live during the call.
    if require_owner && (owner.is_null() || unsafe { EqualSid(owner, sid_pointer(&user_sid)) } == 0)
    {
        return Err(foreign_owner(path, owner, &user_sid, directory));
    }
    let mut control = 0u16;
    let mut revision = 0u32;
    // SAFETY: descriptor is valid and both outputs are writable.
    if unsafe { GetSecurityDescriptorControl(descriptor.0, &mut control, &mut revision) } == 0 {
        let source = std::io::Error::last_os_error();
        let permanent_read_failure =
            directory && source.raw_os_error() == Some(ERROR_ACCESS_DENIED as i32);
        let error = io_error(
            &format!(
                "inspect private data ACL protection on {}",
                display_path(&path.to_string_lossy())
            ),
            source,
        );
        return Err(if permanent_read_failure {
            error.with_where(DATA_DIR_ACL_UNREPAIRABLE_LOCATION)
        } else {
            error
        });
    }
    let current_user_sid = sid_to_string(&user_sid)?;
    let mut acl = PrivateAcl {
        protected: control & SE_DACL_PROTECTED != 0,
        null_dacl: dacl.is_null(),
        has_owner_entry: false,
        current_user_sid,
        entries: Vec::new(),
    };
    if dacl.is_null() {
        return Ok(acl);
    }
    // SAFETY: dacl and its entries remain backed by the descriptor guard, and
    // user_sid remains backed by the current token buffer.
    unsafe { collect_acl_entries(path, &mut acl, dacl, sid_pointer(&user_sid), directory) }
        .map_err(|error| {
            if directory {
                error.with_where(DATA_DIR_ACL_UNREPAIRABLE_LOCATION)
            } else {
                error
            }
        })?;
    Ok(acl)
}

/// Both the DACL and the current-user SID must stay live during inspection.
unsafe fn collect_acl_entries(
    path: &Path,
    acl: &mut PrivateAcl,
    dacl: *const ACL,
    user_sid: PSID,
    directory: bool,
) -> Result<()> {
    // SAFETY: dacl comes from GetSecurityInfo and is still live.
    if unsafe { IsValidAcl(dacl) } == 0 {
        return Err(Error::new(
            "unsafe_data_dir",
            format!("invalid private data ACL on {}", path.display()),
        ));
    }
    let expected_flags = if directory {
        (OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE) as u8
    } else {
        0
    };
    // SAFETY: dacl was validated and its backing descriptor is live.
    let entry_count = unsafe { (*dacl).AceCount };
    for index in 0..u32::from(entry_count) {
        let mut pointer = null_mut();
        // SAFETY: the validated ACL has this index and pointer is writable.
        if unsafe { GetAce(dacl, index, &mut pointer) } == 0 || pointer.is_null() {
            return Err(io_error(
                "read private data ACL entry",
                std::io::Error::last_os_error(),
            ));
        }
        // SAFETY: GetAce returned an entry in an ACL validated by IsValidAcl.
        let header = unsafe { *pointer.cast::<ACE_HEADER>() };
        // SAFETY: the validated ACL bounds the entry and lives through the iteration.
        let bytes =
            unsafe { slice::from_raw_parts(pointer.cast::<u8>(), usize::from(header.AceSize)) };
        let (rights, sid) = read_ace(bytes, u32::from(header.AceType));
        let basic_allow = u32::from(header.AceType) == ACCESS_ALLOWED_ACE_TYPE && sid.is_some();
        if basic_allow
            && header.AceFlags == expected_flags
            && rights == Some(FILE_ALL_ACCESS)
            // SAFETY: read_ace supplies only a validated SID backed by bytes.
            && sid.is_some_and(|sid| unsafe { EqualSid(sid, user_sid) } != 0)
            && !acl.has_owner_entry
        {
            acl.has_owner_entry = true;
            continue;
        }
        let sid = sid.and_then(|sid| sid_text(sid).ok());
        let allowed = directory
            && basic_allow
            && header.AceFlags == 0
            && rights == Some(FILE_TRAVERSE | SYNCHRONIZE)
            && sid
                .as_deref()
                .is_some_and(|sid| sid.starts_with("S-1-15-3-"));
        acl.entries.push(ForeignDataDirAce {
            path: display_path(&path.to_string_lossy()).into(),
            sid,
            rights,
            ace_type: header.AceType,
            flags: header.AceFlags,
            allowed,
        });
    }
    Ok(())
}

/// Read only the mask and trustee for known DACL entry layouts. Object ACEs
/// may include zero, one or two GUIDs before the SID. Callback data follows it
/// and never participates in the basic allow rule.
fn read_ace(bytes: &[u8], ace_type: u32) -> (Option<u32>, Option<PSID>) {
    let sid_offset = std::mem::offset_of!(ACCESS_ALLOWED_ACE, SidStart);
    let offset = match ace_type {
        ACCESS_ALLOWED_ACE_TYPE
        | ACCESS_DENIED_ACE_TYPE
        | ACCESS_ALLOWED_CALLBACK_ACE_TYPE
        | ACCESS_DENIED_CALLBACK_ACE_TYPE => sid_offset,
        ACCESS_ALLOWED_COMPOUND_ACE_TYPE => sid_offset + 4,
        ACCESS_ALLOWED_OBJECT_ACE_TYPE
        | ACCESS_DENIED_OBJECT_ACE_TYPE
        | ACCESS_ALLOWED_CALLBACK_OBJECT_ACE_TYPE
        | ACCESS_DENIED_CALLBACK_OBJECT_ACE_TYPE => {
            let Some(flags) = ace_word(bytes, sid_offset) else {
                return (ace_word(bytes, 4), None);
            };
            let guid_count = (flags
                & (ACE_OBJECT_TYPE_PRESENT | ACE_INHERITED_OBJECT_TYPE_PRESENT))
                .count_ones();
            sid_offset + 4 + 16 * guid_count as usize
        }
        _ => return (None, None),
    };
    let rights = ace_word(bytes, std::mem::offset_of!(ACCESS_ALLOWED_ACE, Mask));
    let sid = bytes.get(offset..).and_then(|sid_bytes| {
        let header = sid_bytes.get(..8)?;
        let length = 8 + usize::from(header[1]) * 4;
        if ace_type == ACCESS_ALLOWED_ACE_TYPE && sid_bytes.len() != length {
            return None;
        }
        let sid_bytes = sid_bytes.get(..length)?;
        let sid = sid_bytes.as_ptr().cast_mut().cast();
        // SAFETY: the SID header and declared subauthorities are within the ACE.
        (unsafe { IsValidSid(sid) } != 0).then_some(sid)
    });
    (rights, sid)
}

fn ace_word(bytes: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_le_bytes(
        bytes.get(offset..offset + 4)?.try_into().ok()?,
    ))
}
