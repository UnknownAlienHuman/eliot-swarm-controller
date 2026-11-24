//! Read-only comparison with documented native user instruction files.

use eliot_core::Result;
use eliot_core::api::{
    HarnessCapabilities, MAX_OWNER_RULES_BYTES, OwnerRulesGlobalFile, OwnerRulesGlobalFileStatus,
    SUPPORTED_HARNESSES,
};
use std::{
    fs::{self, File},
    io::{self, Read},
    path::{Path, PathBuf},
};

const UTF8_BOM: &[u8] = b"\xef\xbb\xbf";
// Source bytes, worst-case CRLF expansion, and one source-sized trailing-newline run.
const MAX_INSTRUCTION_FILE_BYTES: u64 = (MAX_OWNER_RULES_BYTES * 3 + UTF8_BOM.len()) as u64;

pub(super) fn check(data_root: &Path) -> Result<Vec<OwnerRulesGlobalFile>> {
    let Some((_, rules)) = crate::config::settings::owner_rules_snapshot(data_root)? else {
        return Ok(Vec::new());
    };
    let home = absolute_env_path(if cfg!(windows) { "USERPROFILE" } else { "HOME" });
    let mut files = Vec::new();
    for &harness in SUPPORTED_HARNESSES {
        let Some(declaration) = HarnessCapabilities::of(harness).user_instruction_files else {
            continue;
        };
        let Some(root) = declaration
            .root_override_env
            .and_then(absolute_env_path)
            .or_else(|| home.as_ref().map(|path| path.join(declaration.home_subdir)))
        else {
            continue;
        };
        for relative in declaration.paths {
            let path = root.join(relative);
            let display = eliot_core::display_path(&path.to_string_lossy());
            if display.chars().any(char::is_control) {
                continue;
            }
            files.push(OwnerRulesGlobalFile {
                harness: harness.to_owned(),
                path: display,
                status: compare(&path, rules.as_bytes()),
            });
        }
    }
    Ok(files)
}

fn absolute_env_path(name: &str) -> Option<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .filter(|p| p.is_absolute())
}

fn compare(path: &Path, rules: &[u8]) -> OwnerRulesGlobalFileStatus {
    use OwnerRulesGlobalFileStatus::{Different, Match, Missing, Unreadable};
    if let Err(error) = inspect_instruction_path(path) {
        return if error.kind() == io::ErrorKind::NotFound {
            Missing
        } else {
            Unreadable
        };
    }

    let expected = match normalize_text(rules) {
        Some(expected) => expected,
        None => return Different,
    };
    let file = match open_instruction_file(path) {
        Ok(file) => file,
        Err(_) => return Unreadable,
    };
    let mut bytes = Vec::new();
    let result = file
        .take(MAX_INSTRUCTION_FILE_BYTES + 1)
        .read_to_end(&mut bytes);
    match result {
        Ok(_) if bytes.len() as u64 > MAX_INSTRUCTION_FILE_BYTES => Unreadable,
        Ok(_) => match normalize_text(&bytes) {
            Some(actual) if actual == expected => Match,
            Some(_) | None => Different,
        },
        Err(_) => Unreadable,
    }
}

fn inspect_instruction_path(path: &Path) -> io::Result<()> {
    let mut ancestors: Vec<_> = path.ancestors().collect();
    while let Some(ancestor) = ancestors.pop() {
        let metadata = fs::symlink_metadata(ancestor)?;
        let is_expected_type = if ancestors.is_empty() {
            metadata.is_file()
        } else {
            metadata.is_dir()
        };
        if metadata.file_type().is_symlink()
            || crate::platform::is_reparse_point(&metadata)
            || !is_expected_type
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "instruction path contains a link, reparse point, or non-regular component",
            ));
        }
    }
    Ok(())
}

fn open_instruction_file(path: &Path) -> io::Result<File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        options.custom_flags(windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OPEN_REPARSE_POINT);
    }

    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file() || crate::platform::is_reparse_point(&metadata) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "instruction file is not a regular file",
        ));
    }
    Ok(file)
}

fn normalize_text(bytes: &[u8]) -> Option<Vec<u8>> {
    let bytes = bytes.strip_prefix(UTF8_BOM).unwrap_or(bytes);
    std::str::from_utf8(bytes).ok()?;

    let mut normalized = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'\r' && bytes.get(index + 1) == Some(&b'\n') {
            normalized.push(b'\n');
            index += 2;
        } else {
            normalized.push(bytes[index]);
            index += 1;
        }
    }
    while normalized.last() == Some(&b'\n') {
        normalized.pop();
    }
    Some(normalized)
}
