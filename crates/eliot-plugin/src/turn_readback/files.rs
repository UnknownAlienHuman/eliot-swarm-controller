use eliot_core::{Error, Result};
use serde_json::Value;
use std::{
    fs::File,
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    time::Instant,
};

const MAX_HISTORY_ENTRIES: usize = 32_768;
const MAX_HISTORY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_RECORD_BYTES: u64 = 4 * 1024 * 1024;

fn unavailable() -> Error {
    Error::new(
        "native_readback",
        "Native history is missing, ambiguous or exceeds its read bound",
    )
}

pub fn is_native_uuid(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_hexdigit()
            }
        })
}

fn is_link(metadata: &std::fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}

/// Search only the selected native store. No links, first-match guesses or
/// partial searches are accepted as proof of an exact subject.
pub fn find_exact_history(
    roots: impl IntoIterator<Item = PathBuf>,
    depth: usize,
    matches: impl Fn(&str) -> bool,
    deadline: Instant,
) -> Result<Option<PathBuf>> {
    let mut pending: Vec<_> = roots.into_iter().map(|root| (root, depth)).collect();
    let mut remaining = MAX_HISTORY_ENTRIES;
    let mut result = None;
    while let Some((dir, left)) = pending.pop() {
        let metadata = match std::fs::symlink_metadata(&dir) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return Err(unavailable()),
        };
        if is_link(&metadata) || !metadata.is_dir() {
            continue;
        }
        let entries = std::fs::read_dir(&dir).map_err(|_| unavailable())?;
        for entry in entries {
            remaining = remaining.checked_sub(1).ok_or_else(unavailable)?;
            if Instant::now() >= deadline {
                return Err(unavailable());
            }
            let entry = entry.map_err(|_| unavailable())?;
            let path = entry.path();
            let ty = entry.file_type().map_err(|_| unavailable())?;
            if ty.is_dir() && left > 0 {
                pending.push((path, left - 1));
            } else if ty.is_file() && entry.file_name().to_str().is_some_and(&matches) {
                let metadata = std::fs::symlink_metadata(&path).map_err(|_| unavailable())?;
                if !is_link(&metadata) && result.replace(path).is_some() {
                    return Err(unavailable());
                }
            }
        }
    }
    Ok(result)
}

pub fn read_history_records(
    path: &Path,
    canonical_root: &Path,
    deadline: Instant,
    mut visit: impl FnMut(Value) -> Result<()>,
) -> Result<()> {
    let file = File::open(path).map_err(|_| unavailable())?;
    let metadata = file.metadata().map_err(|_| unavailable())?;
    let final_path = crate::file_final_path(&file).map_err(|_| unavailable())?;
    if !metadata.is_file()
        || metadata.len() > MAX_HISTORY_BYTES
        || !final_path.starts_with(canonical_root)
    {
        return Err(unavailable());
    }
    let mut reader = BufReader::new((&file).take(metadata.len()));
    let mut bytes = Vec::new();
    loop {
        if Instant::now() >= deadline {
            return Err(unavailable());
        }
        bytes.clear();
        let n = (&mut reader)
            .take(MAX_RECORD_BYTES + 1)
            .read_until(b'\n', &mut bytes)
            .map_err(|_| unavailable())?;
        if n == 0 {
            let latest = file.metadata().map_err(|_| unavailable())?;
            return if latest.len() == metadata.len()
                && latest.modified().ok() == metadata.modified().ok()
            {
                Ok(())
            } else {
                Err(unavailable())
            };
        }
        if n as u64 > MAX_RECORD_BYTES {
            return Err(unavailable());
        }
        if !bytes.ends_with(b"\n") {
            return Err(unavailable());
        }
        visit(serde_json::from_slice(&bytes).map_err(|_| unavailable())?)?;
    }
}
