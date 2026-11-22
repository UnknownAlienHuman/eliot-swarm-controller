//! Verbatim (`\\?\`) Windows path prefix shared by database and file opens.
//!
//! Pure path computation, no I/O: a local disk-absolute path gains the
//! verbatim prefix so raw Win32 calls work past the legacy parse limit;
//! every other path is returned unchanged.

/// Owner-facing rendering of a retained path: the verbatim prefix leaves, so
/// a person and an ordinary tool can read it.
///
/// `\\?\C:\repo` reads `C:\repo`, `\\?\UNC\server\share` reads
/// `\\server\share`, and anything else is unchanged. This never reaches the
/// Store: retained identity, Git keys and path-keyed rows keep their exact
/// stored spelling, so only the view that leaves the daemon uses this form.
pub fn display_path(path: &str) -> String {
    match path.strip_prefix(r"\\?\") {
        Some(rest) if rest.starts_with(r"UNC\") => format!(r"\\{}", &rest[4..]),
        Some(rest) => rest.to_owned(),
        None => path.to_owned(),
    }
}

use std::path::{Path, PathBuf};

/// Comparison key for an already-resolved workspace path. Keep the original
/// spelling for filesystem access and retained Git identity.
pub fn workspace_path_key(path: &str) -> std::borrow::Cow<'_, str> {
    #[cfg(windows)]
    {
        let path = path.replace('\\', "/").to_lowercase();
        let path = match path.strip_prefix("//?/unc/") {
            Some(rest) => format!("//{rest}"),
            None => path.strip_prefix("//?/").unwrap_or(&path).to_owned(),
        };
        std::borrow::Cow::Owned(path)
    }
    #[cfg(not(windows))]
    {
        std::borrow::Cow::Borrowed(path)
    }
}

/// Verbatim-prefixed form of `path` for raw Win32 file opens.
///
/// Local disk-absolute paths gain the `\\?\` prefix after `absolute`
/// normalization (so `folder.` components do not survive literally);
/// already-prefixed, non-disk and relative paths return unchanged.
#[cfg(windows)]
pub fn verbatim_path(path: &Path) -> PathBuf {
    use std::ffi::OsString;
    use std::os::windows::ffi::{OsStrExt, OsStringExt};
    use std::path::{Component, Prefix};

    const BACKSLASH: u16 = 92;
    const QUESTION: u16 = 63;
    const DOT: u16 = 46;

    let wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    let already = wide.len() >= 4
        && wide[0] == BACKSLASH
        && wide[1] == BACKSLASH
        && (wide[2] == QUESTION || wide[2] == DOT)
        && wide[3] == BACKSLASH;
    if already {
        return path.to_path_buf();
    }
    let local_disk = path.is_absolute()
        && matches!(
            path.components().next(),
            Some(Component::Prefix(p))
                if matches!(p.kind(), Prefix::Disk(_) | Prefix::VerbatimDisk(_))
        );
    if !local_disk {
        return path.to_path_buf();
    }
    let normalized = match std::path::absolute(path) {
        Ok(normalized) => normalized,
        Err(_) => return path.to_path_buf(),
    };
    let wide: Vec<u16> = normalized.as_os_str().encode_wide().collect();
    let already = wide.len() >= 4
        && wide[0] == BACKSLASH
        && wide[1] == BACKSLASH
        && (wide[2] == QUESTION || wide[2] == DOT)
        && wide[3] == BACKSLASH;
    if already {
        return normalized;
    }
    let mut prefixed = vec![BACKSLASH, BACKSLASH, QUESTION, BACKSLASH];
    let mut wide = wide;
    prefixed.append(&mut wide);
    PathBuf::from(OsString::from_wide(&prefixed))
}

/// Non-Windows platforms have no verbatim prefix: return the path unchanged.
#[cfg(not(windows))]
pub fn verbatim_path(path: &Path) -> PathBuf {
    path.to_path_buf()
}
