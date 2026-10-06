use crate::error::{Error, Result};
use sha2::{Digest, Sha256};
#[cfg(debug_assertions)]
use std::ffi::OsStr;
use std::{
    collections::HashMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

const REQUIRED: &[&str] = &[
    "serve.mjs",
    "native-mcp-proof.mjs",
    "index.mjs",
    "package.json",
    "package-lock.json",
];
const INSTALL_RECEIPT: &str = "swarm-kernel-host.install-receipt.json";
const BUILD_MANIFEST: &str = "swarm-kernel-host.build-manifest.json";
const RESOURCE_COORDINATE: &str = "swarm-kernel-host-opencode-resources";
const MAX_RECEIPT_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Clone)]
struct CachedInstalledRoot {
    resource_root: PathBuf,
    receipt_sha256: String,
}

static VERIFIED_ROOTS: OnceLock<Mutex<HashMap<PathBuf, CachedInstalledRoot>>> = OnceLock::new();

pub(crate) fn required_file(name: &str) -> Result<PathBuf> {
    if !REQUIRED.contains(&name) {
        return Err(Error::new(
            "OPENCODE_RESOURCE_FILE_UNAVAILABLE",
            "requested OpenCode resource is outside the pinned resource set",
        ));
    }
    let root = match installed_root()? {
        Some(root) => root,
        None => repository_root()?.ok_or_else(|| {
            Error::new(
                "OPENCODE_RESOURCE_ROOT_UNAVAILABLE",
                "pinned OpenCode resources are unavailable",
            )
        })?,
    };
    let path = root.join(name);
    if regular_file_under(&root, &path) {
        return Ok(path);
    }
    Err(Error::new(
        "OPENCODE_RESOURCE_FILE_UNAVAILABLE",
        "the selected OpenCode resource root is missing the requested file",
    ))
}

fn installed_root() -> Result<Option<PathBuf>> {
    let executable = std::env::current_exe()
        .map_err(|_| closure_error("the installed kernel executable path is unavailable"))?;
    let install_root = executable
        .parent()
        .ok_or_else(|| closure_error("the installed kernel executable has no parent directory"))?;
    let install_root = install_root
        .canonicalize()
        .map_err(|_| closure_error("the installed kernel directory could not be resolved"))?;
    let resource_path = install_root
        .join("resources")
        .join("modules")
        .join("opencode");
    let receipt_path = install_root.join(INSTALL_RECEIPT);
    let build_path = install_root.join(BUILD_MANIFEST);
    let resource_present = path_present(&resource_path)?;
    let receipt_present = path_present(&receipt_path)?;
    let build_present = path_present(&build_path)?;
    if !resource_present {
        if receipt_present || build_present {
            return Err(closure_error(
                "installed OpenCode resources are incomplete; source-tree fallback is disabled",
            ));
        }
        return Ok(None);
    }
    let resource_root = resource_path
        .canonicalize()
        .map_err(|_| closure_error("installed OpenCode resources could not be resolved"))?;
    if !resource_root.starts_with(&install_root) {
        return Err(closure_error(
            "installed OpenCode resources escaped the package root",
        ));
    }
    let receipt_sha256 = sha256_small_regular_file(&receipt_path)?;
    let cache = VERIFIED_ROOTS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut cache = cache
        .lock()
        .map_err(|_| closure_error("installed OpenCode verification cache is unavailable"))?;
    if let Some(previous) = cache.get(&install_root) {
        if previous.resource_root != resource_root || previous.receipt_sha256 != receipt_sha256 {
            return Err(closure_error(
                "the pinned OpenCode installation changed while this kernel process was active",
            ));
        }
        return Ok(Some(previous.resource_root.clone()));
    }
    let verified = swarm_process::dependency_closure::verify_installed_dependency_closure(
        &install_root,
        "swarm-kernel-host",
        "swarm-kernel-host",
        RESOURCE_COORDINATE,
    )
    .map_err(|_| closure_error("installed OpenCode dependency closure failed verification"))?;
    if verified.resource_root != resource_root
        || sha256_small_regular_file(&receipt_path)? != receipt_sha256
    {
        return Err(closure_error(
            "the pinned OpenCode installation changed during dependency verification",
        ));
    }
    cache.insert(
        install_root,
        CachedInstalledRoot {
            resource_root: verified.resource_root.clone(),
            receipt_sha256,
        },
    );
    Ok(Some(verified.resource_root))
}

fn path_present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(closure_error(
            "installed OpenCode package metadata could not be read",
        )),
    }
}

fn sha256_small_regular_file(path: &Path) -> Result<String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| closure_error("the installed package receipt is missing"))?;
    if !metadata.is_file() || is_reparse_point(&metadata) || metadata.len() > MAX_RECEIPT_BYTES {
        return Err(closure_error(
            "the installed package receipt is not a bounded regular file",
        ));
    }
    let file = fs::File::open(path)
        .map_err(|_| closure_error("the installed package receipt could not be read"))?;
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    file.take(MAX_RECEIPT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| closure_error("the installed package receipt could not be read"))?;
    if bytes.len() as u64 != metadata.len() {
        return Err(closure_error(
            "the installed package receipt changed while it was read",
        ));
    }
    let digest = Sha256::digest(bytes);
    Ok(hex_lower(&digest))
}

fn hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn closure_error(message: &'static str) -> Error {
    Error::new("OPENCODE_RESOURCE_CLOSURE_INVALID", message)
}

#[cfg(debug_assertions)]
fn repository_root() -> Result<Option<PathBuf>> {
    if std::env::var_os("ELIOT_OPENCODE_SOURCE_RESOURCES").as_deref() != Some(OsStr::new("1")) {
        return Ok(None);
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("modules")
        .join("opencode");
    let root = match complete_source_root(root) {
        Some(root) => root,
        None => {
            return Err(Error::new(
                "OPENCODE_RESOURCE_ROOT_UNAVAILABLE",
                "explicit OpenCode source resources or node_modules are incomplete",
            ));
        }
    };
    Ok(Some(root))
}

#[cfg(not(debug_assertions))]
fn repository_root() -> Result<Option<PathBuf>> {
    Ok(None)
}

#[cfg(debug_assertions)]
fn complete_source_root(root: PathBuf) -> Option<PathBuf> {
    let canonical = complete_base_root(root)?;
    let node_modules = canonical.join("node_modules");
    let metadata = fs::symlink_metadata(&node_modules).ok()?;
    if !metadata.is_dir() || is_reparse_point(&metadata) {
        return None;
    }
    let canonical_node_modules = node_modules.canonicalize().ok()?;
    if !canonical_node_modules.starts_with(&canonical) {
        return None;
    }
    Some(canonical)
}

#[cfg(debug_assertions)]
fn complete_base_root(root: PathBuf) -> Option<PathBuf> {
    let metadata = fs::symlink_metadata(&root).ok()?;
    if !metadata.is_dir() || is_reparse_point(&metadata) {
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
    if !metadata.is_file() || is_reparse_point(&metadata) {
        return false;
    }
    let canonical = match path.canonicalize() {
        Ok(canonical) => canonical,
        Err(_) => return false,
    };
    canonical.starts_with(root)
}

fn is_reparse_point(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0400;
        metadata.file_type().is_symlink()
            || metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0
    }
    #[cfg(not(windows))]
    {
        metadata.file_type().is_symlink()
    }
}
