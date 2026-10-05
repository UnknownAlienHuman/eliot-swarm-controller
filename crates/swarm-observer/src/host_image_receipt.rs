//! Private exact process-image identity for the currently locked host.
//!
//! The caller publishes only after acquiring the existing exclusive DataRoot
//! lock and removes the exact receipt before releasing the Store owner. This is
//! a read-only sampling input; it grants no ownership or business authority.

use serde_json::Value;
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};
use swarm_contracts::error::{Error, Result};

pub const HOST_IMAGE_RECEIPT_FILE: &str = "host-image.json";
const MAX_RECEIPT_BYTES: u64 = 64 * 1024;

/// A published host image receipt. The caller must hold the exclusive
/// DataRoot/Store lock for `publish` and `cleanup_under_store_lock`.
pub struct HostImageReceipt {
    path: PathBuf,
    pending_path: PathBuf,
    bytes: Vec<u8>,
}

/// Stable default path consumed by the optional local metrics command.
/// Returning a path does not open or inspect it.
pub fn default_path(data_root: &Path) -> PathBuf {
    data_root.join(HOST_IMAGE_RECEIPT_FILE)
}

impl HostImageReceipt {
    /// Capture the current process image using the shared OS identity API and
    /// publish it under the already-held private DataRoot lock. Existing
    /// receipts are removed only after their exact process birth is proven
    /// departed and the file bytes remain unchanged.
    pub fn publish(data_root: &Path) -> Result<Self> {
        if !data_root.is_absolute() || !data_root.is_dir() {
            return Err(unavailable());
        }
        let pid = std::process::id();
        let identity = swarm_process::process_image_identity(pid).map_err(|_| unavailable())?;
        validate_image_identity(&identity)?;
        let bytes = serde_json::to_vec(&identity).map_err(|_| invalid_receipt())?;
        if bytes.len() as u64 > MAX_RECEIPT_BYTES {
            return Err(invalid_receipt());
        }

        let path = default_path(data_root);
        let pending_path = pending_path(data_root);
        let mut published_bytes = bytes.clone();

        if path_exists_without_following(&pending_path)? {
            let pending = read_private_file(&pending_path)?;
            if pending != bytes {
                // This fixed scratch path is owned by the host publication
                // protocol. The caller's exclusive DataRoot lock proves that
                // a prior writer no longer owns it; compare bytes again before
                // removing even this non-authoritative pending artifact.
                remove_if_exact(&pending_path, &pending)?;
                swarm_process::write_private_new(&pending_path, &bytes)
                    .map_err(|_| write_failed())?;
            }
        } else {
            swarm_process::write_private_new(&pending_path, &bytes).map_err(|_| write_failed())?;
        }

        if path_exists_without_following(&path)? {
            let existing_bytes = read_private_file(&path)?;
            let existing: Value =
                serde_json::from_slice(&existing_bytes).map_err(|_| invalid_receipt())?;
            validate_image_identity(&existing)?;
            if existing == identity {
                // A prior invocation in this same host process may have
                // stopped after publication; the current exclusive Store lock
                // is the owner-lifecycle proof for adopting this exact record.
                if path_exists_without_following(&pending_path)? {
                    remove_if_exact(&pending_path, &bytes)?;
                }
                return Ok(Self {
                    path,
                    pending_path,
                    bytes: existing_bytes,
                });
            }

            let old_pid = image_pid(&existing)?;
            let live_birth =
                swarm_process::process_birth_identity(old_pid).map_err(|_| unavailable())?;
            if live_birth
                .as_ref()
                .is_some_and(|current| birth_matches(&existing, current))
            {
                return Err(active_receipt());
            }
            remove_if_exact(&path, &existing_bytes)?;
            published_bytes = bytes;
        }

        fs::rename(&pending_path, &path).map_err(|_| write_failed())?;
        Ok(Self {
            path,
            pending_path,
            bytes: published_bytes,
        })
    }

    /// Remove only this receipt's exact serialized identity while the caller
    /// still owns the existing Store lock. A newer host receipt is preserved.
    pub fn cleanup_under_store_lock(self) -> Result<()> {
        remove_if_exact(&self.path, &self.bytes)?;
        if path_exists_without_following(&self.pending_path)? {
            remove_if_exact(&self.pending_path, &self.bytes)?;
        }
        Ok(())
    }
}

fn pending_path(data_root: &Path) -> PathBuf {
    data_root.join(".host-image.pending")
}

fn validate_image_identity(identity: &Value) -> Result<()> {
    let object = identity.as_object().ok_or_else(invalid_receipt)?;
    let windows = object.len() == 4
        && ["pid", "creation_filetime", "image_path", "image_sha256"]
            .iter()
            .all(|key| object.contains_key(*key));
    let linux = object.len() == 5
        && [
            "pid",
            "start_ticks",
            "boot_id",
            "image_path",
            "image_sha256",
        ]
        .iter()
        .all(|key| object.contains_key(*key));
    if !windows && !linux {
        return Err(invalid_receipt());
    }
    let pid = image_pid(identity)?;
    if pid == 0 {
        return Err(invalid_receipt());
    }
    if windows {
        let creation = object
            .get("creation_filetime")
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty() && text.len() <= 20)
            .and_then(|text| {
                text.parse::<u64>()
                    .ok()
                    .filter(|value| value.to_string() == text)
            });
        if creation.is_none() {
            return Err(invalid_receipt());
        }
    } else {
        let boot = object
            .get("boot_id")
            .and_then(Value::as_str)
            .filter(|text| {
                !text.is_empty() && text.len() <= 128 && !text.chars().any(char::is_control)
            });
        if boot.is_none() || object.get("start_ticks").and_then(Value::as_u64).is_none() {
            return Err(invalid_receipt());
        }
    }
    let image_path = object
        .get("image_path")
        .and_then(Value::as_str)
        .filter(|text| {
            !text.is_empty()
                && text.len() <= 32 * 1024
                && !text.chars().any(char::is_control)
                && Path::new(text).is_absolute()
        });
    let hash = object
        .get("image_sha256")
        .and_then(Value::as_str)
        .and_then(|text| text.strip_prefix("sha256:"));
    if image_path.is_none()
        || !hash.is_some_and(|text| {
            text.len() == 64
                && text
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(invalid_receipt());
    }
    Ok(())
}

fn image_pid(identity: &Value) -> Result<u32> {
    identity
        .get("pid")
        .and_then(Value::as_u64)
        .and_then(|pid| u32::try_from(pid).ok())
        .filter(|pid| *pid > 0)
        .ok_or_else(invalid_receipt)
}

fn birth_matches(image: &Value, current_birth: &Value) -> bool {
    if image.get("pid") != current_birth.get("pid") {
        return false;
    }
    if let Some(creation) = image.get("creation_filetime") {
        return current_birth.get("creation_filetime") == Some(creation);
    }
    image.get("start_ticks") == current_birth.get("start_ticks")
        && image.get("boot_id") == current_birth.get("boot_id")
}

fn read_private_file(path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path).map_err(|_| unavailable())?;
    if metadata.file_type().is_symlink() || is_reparse(&metadata) || !metadata.is_file() {
        return Err(invalid_receipt());
    }
    if metadata.len() > MAX_RECEIPT_BYTES {
        return Err(invalid_receipt());
    }
    let mut file = File::open(path).map_err(|_| unavailable())?;
    let opened = file.metadata().map_err(|_| unavailable())?;
    if !opened.is_file() || opened.len() > MAX_RECEIPT_BYTES {
        return Err(invalid_receipt());
    }
    let mut bytes = Vec::with_capacity(opened.len() as usize);
    (&mut file)
        .take(MAX_RECEIPT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| unavailable())?;
    if bytes.len() as u64 > MAX_RECEIPT_BYTES {
        return Err(invalid_receipt());
    }
    Ok(bytes)
}

fn path_exists_without_following(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() || is_reparse(&metadata) {
                return Err(invalid_receipt());
            }
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(unavailable()),
    }
}

fn remove_if_exact(path: &Path, expected: &[u8]) -> Result<()> {
    let current = read_private_file(path)?;
    if current != expected {
        return Err(changed_receipt());
    }
    fs::remove_file(path).map_err(|_| unavailable())
}

#[cfg(windows)]
fn is_reparse(metadata: &std::fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_reparse(_metadata: &std::fs::Metadata) -> bool {
    false
}

fn unavailable() -> Error {
    Error::new(
        "OBSERVER_HOST_RECEIPT_UNAVAILABLE",
        "host process identity receipt is unavailable",
    )
}

fn write_failed() -> Error {
    Error::new(
        "OBSERVER_HOST_RECEIPT_WRITE_FAILED",
        "host process identity receipt could not be published",
    )
}

fn invalid_receipt() -> Error {
    Error::new(
        "OBSERVER_HOST_RECEIPT_INVALID",
        "host process identity receipt is outside the supported private schema",
    )
}

fn changed_receipt() -> Error {
    Error::new(
        "OBSERVER_HOST_RECEIPT_CHANGED",
        "host process identity receipt changed during bounded cleanup",
    )
}

fn active_receipt() -> Error {
    Error::new(
        "OBSERVER_HOST_RECEIPT_ACTIVE",
        "an existing host identity receipt still names a live process",
    )
}
