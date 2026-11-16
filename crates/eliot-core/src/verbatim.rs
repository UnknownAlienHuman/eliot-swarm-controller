//! Verbatim (`\\?\`) Windows path prefix shared by database and file opens.
//!
//! Pure path computation, no I/O: a local disk-absolute path gains the
//! verbatim prefix so raw Win32 calls work past the legacy parse limit;
//! every other path is returned unchanged.

use std::path::{Path, PathBuf};

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

#[cfg(all(test, windows))]
mod verbatim_path_tests {
    use super::verbatim_path;
    use std::path::PathBuf;

    #[test]
    fn verbatim_leaves_drive_relative_unchanged() {
        let input = PathBuf::from("C:eliot.db");
        assert_eq!(
            verbatim_path(&input),
            input,
            "drive-relative must not gain a verbatim prefix"
        );
    }

    #[test]
    fn verbatim_normalizes_trailing_dot_component() {
        let input = PathBuf::from(r"C:\eliot-verbatim-check\folder.\eliot.db");
        let normalized = std::path::absolute(&input).expect("normalize");
        assert!(
            !normalized.to_string_lossy().contains("folder."),
            "absolute() must normalize folder.: {}",
            normalized.display()
        );
        let out = verbatim_path(&input);
        assert!(
            !out.to_string_lossy().contains("folder."),
            "verbatim must not preserve literal folder.: {}",
            out.display()
        );
    }

    #[test]
    fn verbatim_leaves_non_disk_paths_unchanged() {
        for raw in [r"\\server\share\eliot.db", ":memory:", r"relative\eliot.db"] {
            let input = PathBuf::from(raw);
            assert_eq!(verbatim_path(&input), input, "{raw} must stay unchanged");
        }
        let verb = PathBuf::from(r"\\?\C:\already\verbatim.db");
        assert_eq!(verbatim_path(&verb), verb);
    }
}
