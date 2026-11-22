//! Filesystem containment for native edit observations.
use eliot_core::{Error, Result};
use std::{
    ffi::OsString,
    fs,
    path::{Component, Path, PathBuf},
};

#[cfg(windows)]
fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_reparse_point(_: &fs::Metadata) -> bool {
    false
}

fn unavailable(message: &str) -> Error {
    Error::new("unavailable", message)
}

/// Normalize verified native file-change hints, including deleted files, without
/// following a symlink/reparse point outside (or within) the selected worktree.
pub fn normalize(workspace: &str, path: &str) -> Result<String> {
    let root = canonical_root(workspace)?;
    if path.is_empty() || path.contains('\0') {
        return Err(unavailable("file-change hint is empty or malformed"));
    }
    let input = Path::new(path);
    if input
        .components()
        .any(|part| matches!(part, Component::ParentDir))
    {
        return Err(unavailable("file-change hint contains parent traversal"));
    }
    if !input.is_absolute()
        && input
            .components()
            .any(|part| matches!(part, Component::RootDir | Component::Prefix(_)))
    {
        return Err(unavailable("file-change hint has an ambiguous root"));
    }
    let target = if input.is_absolute() {
        input.to_path_buf()
    } else {
        root.join(input)
    };
    reject_links(&target)?;
    let mut existing = target.clone();
    let mut missing: Vec<OsString> = Vec::new();
    loop {
        match fs::symlink_metadata(&existing) {
            Ok(metadata) => {
                if !missing.is_empty() && !metadata.is_dir() {
                    return Err(unavailable("hint parent is not a directory"));
                }
                break;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(
                    existing
                        .file_name()
                        .ok_or_else(|| unavailable("hint has no existing root"))?
                        .to_owned(),
                );
                if !existing.pop() {
                    return Err(unavailable("hint has no existing parent"));
                }
            }
            Err(_) => return Err(unavailable("hint path cannot be inspected")),
        }
    }
    let mut canonical =
        fs::canonicalize(existing).map_err(|_| unavailable("hint parent cannot be resolved"))?;
    for component in missing.into_iter().rev() {
        canonical.push(component);
    }
    let relative = canonical
        .strip_prefix(&root)
        .map_err(|_| unavailable("file-change hint escapes its worktree"))?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(name) => {
                let name = name
                    .to_str()
                    .ok_or_else(|| unavailable("hint path is not UTF-8"))?;
                #[cfg(windows)]
                if name.contains(':') {
                    return Err(unavailable("hint path contains a Windows alternate stream"));
                }
                parts.push(name);
            }
            Component::CurDir => {}
            _ => return Err(unavailable("hint path has an invalid component")),
        }
    }
    if parts.is_empty() {
        return Err(unavailable("hint names the worktree root"));
    }
    Ok(parts.join("/"))
}

pub fn canonical_root(workspace: &str) -> Result<PathBuf> {
    let path = Path::new(workspace);
    if !path.is_absolute() {
        return Err(unavailable("workspace must be absolute"));
    }
    reject_links(path)?;
    let root = fs::canonicalize(path).map_err(|_| unavailable("workspace cannot be resolved"))?;
    if !root.is_dir() {
        return Err(unavailable("workspace is not a directory"));
    }
    Ok(root)
}

pub fn reject_links(path: &Path) -> Result<()> {
    let ancestors: Vec<_> = path.ancestors().collect();
    for prefix in ancestors.into_iter().rev() {
        match fs::symlink_metadata(prefix) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || is_reparse_point(&metadata) {
                    return Err(unavailable(
                        "workspace path contains a symlink or reparse point",
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err(unavailable("workspace path cannot be inspected")),
        }
    }
    Ok(())
}
