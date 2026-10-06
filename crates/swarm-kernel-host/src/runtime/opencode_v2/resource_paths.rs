use crate::error::{Error, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};

const REQUIRED: &[&str] = &[
    "serve.mjs",
    "native-mcp-proof.mjs",
    "index.mjs",
    "package.json",
    "package-lock.json",
];

pub(crate) fn required_file(name: &str) -> Result<PathBuf> {
    if !REQUIRED.iter().any(|candidate| *candidate == name) {
        return Err(Error::new(
            "OPENCODE_RESOURCE_FILE_UNAVAILABLE",
            "requested OpenCode resource is outside the pinned resource set",
        ));
    }
    if let Some(root) = installed_root().or_else(repository_root) {
        let path = root.join(name);
        if regular_file_under(&root, &path) {
            return Ok(path);
        }
        return Err(Error::new(
            "OPENCODE_RESOURCE_FILE_UNAVAILABLE",
            "the selected OpenCode resource root is missing the requested file",
        ));
    }
    Err(Error::new(
        "OPENCODE_RESOURCE_ROOT_UNAVAILABLE",
        "pinned OpenCode resources are unavailable",
    ))
}

fn installed_root() -> Option<PathBuf> {
    let executable = std::env::current_exe().ok()?;
    let parent = executable.parent()?;
    complete_root(parent.join("resources").join("modules").join("opencode"))
}

fn repository_root() -> Option<PathBuf> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("modules")
        .join("opencode");
    complete_root(root)
}

fn complete_root(root: PathBuf) -> Option<PathBuf> {
    let metadata = fs::symlink_metadata(&root).ok()?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return None;
    }
    let canonical = root.canonicalize().ok()?;
    if !REQUIRED
        .iter()
        .all(|name| regular_file_under(&canonical, &canonical.join(name)))
    {
        return None;
    }
    Some(canonical)
}

fn regular_file_under(root: &Path, path: &Path) -> bool {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(_) => return false,
    };
    if !metadata.is_file() || metadata.file_type().is_symlink() {
        return false;
    }
    let canonical = match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(_) => return false,
    };
    canonical.starts_with(root)
}
