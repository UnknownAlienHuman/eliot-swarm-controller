//! Host-only validation of the private config used by a managed bus worker.
//!
//! The secret is parsed only long enough to compare its token digest, then
//! zeroized. The verifier returns no credential material.

use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    fs,
    fs::OpenOptions,
    io::Read,
    path::{Path, PathBuf},
};
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};
use zeroize::Zeroize;

const MAX_CONFIG_BYTES: u64 = 8 * 1024;
const MIN_POLL_MS: u64 = 100;
const MAX_POLL_MS: u64 = 60_000;

#[derive(Debug, Clone)]
pub struct ManagedWorkerConfigExpectation {
    pub path: PathBuf,
    pub store_root: PathBuf,
    pub manager_id: String,
    pub project_id: String,
    pub automation_id: String,
    pub consumer_client_id: String,
    pub credential_token_sha256: String,
    pub worker_config_sha256: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkerConfig {
    schema_version: u32,
    store_root: PathBuf,
    manager_id: String,
    project_id: String,
    automation_id: String,
    register_request_id: String,
    poll_interval_ms: u64,
    credential: Credential,
    #[serde(default)]
    managed_service: bool,
}

impl Drop for WorkerConfig {
    fn drop(&mut self) {
        self.credential.token.zeroize();
    }
}

/// Validate the exact configured file, Store token hash, and durable scope.
/// This must run immediately before process spawn. It makes a point-in-time
/// check only; it does not claim an atomic filesystem pin through spawn.
pub fn verify_managed_worker_config(expected: &ManagedWorkerConfigExpectation) -> Result<()> {
    if !expected.path.is_absolute()
        || !expected.store_root.is_absolute()
        || !valid_sha256(&expected.credential_token_sha256)
        || !valid_sha256(&expected.worker_config_sha256)
    {
        return Err(Error::new(
            "BUS_WORKER_CONFIG_EXPECTATION_INVALID",
            "managed worker config expectation is invalid",
        ));
    }
    reject_link_components(&expected.path)?;
    let metadata = fs::symlink_metadata(&expected.path).map_err(|_| {
        Error::new(
            "BUS_WORKER_CONFIG_MISSING",
            "managed worker config is unavailable",
        )
    })?;
    if !metadata.is_file() || is_link_or_reparse(&metadata) || metadata.len() > MAX_CONFIG_BYTES {
        return Err(Error::new(
            "BUS_WORKER_CONFIG_INVALID",
            "managed worker config is not a bounded regular file",
        ));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::new(
                "BUS_WORKER_CONFIG_NOT_PRIVATE",
                "managed worker config permissions are not owner-only",
            ));
        }
    }
    #[cfg(windows)]
    swarm_process::private_permissions(&expected.path, false)?;

    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    let file = OpenOptions::new().read(true).open(&expected.path)?;
    file.take(MAX_CONFIG_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES
        || hex_sha256(&bytes) != expected.worker_config_sha256.to_ascii_lowercase()
    {
        bytes.zeroize();
        return Err(Error::new(
            "BUS_WORKER_CONFIG_DIGEST_MISMATCH",
            "managed worker config changed after Store registration",
        ));
    }
    let parsed = serde_json::from_slice::<WorkerConfig>(&bytes);
    bytes.zeroize();
    let mut config = parsed.map_err(|_| {
        Error::new(
            "BUS_WORKER_CONFIG_INVALID",
            "managed worker config is malformed",
        )
    })?;
    let actual_store_root = fs::canonicalize(&config.store_root)?;
    let expected_store_root = fs::canonicalize(&expected.store_root)?;
    let token_hash = hex_sha256(config.credential.token.as_bytes());
    let valid = config.schema_version == 1
        && config.managed_service
        && actual_store_root == expected_store_root
        && config.manager_id == expected.manager_id
        && config.project_id == expected.project_id
        && config.automation_id == expected.automation_id
        && config.credential.client_id == expected.consumer_client_id
        && config.credential.client_id.starts_with("bus-script-")
        && !config.credential.token.is_empty()
        && token_hash == expected.credential_token_sha256.to_ascii_lowercase()
        && !config.register_request_id.is_empty()
        && (MIN_POLL_MS..=MAX_POLL_MS).contains(&config.poll_interval_ms);
    config.credential.token.zeroize();
    if !valid {
        return Err(Error::new(
            "BUS_WORKER_CONFIG_SCOPE_MISMATCH",
            "private worker config does not match the retained managed scope",
        ));
    }
    Ok(())
}

fn reject_link_components(path: &Path) -> Result<()> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        return Err(Error::invalid(
            "managed worker config path must be absolute",
        ));
    };
    let mut current = PathBuf::new();
    for component in absolute.components() {
        current.push(component.as_os_str());
        if matches!(component, std::path::Component::Prefix(_)) {
            continue;
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) if is_link_or_reparse(&metadata) => {
                return Err(Error::new(
                    "BUS_WORKER_CONFIG_INVALID",
                    "managed worker config path traverses a link or reparse point",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn hex_sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(windows)]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    use std::os::windows::fs::MetadataExt;
    metadata.file_type().is_symlink() || metadata.file_attributes() & 0x400 != 0
}

#[cfg(not(windows))]
fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    metadata.file_type().is_symlink()
}

#[cfg(all(test, windows))]
mod tests {
    use super::*;

    #[test]
    fn bootstrap_path_canonical_managed_config_is_readable() {
        let path =
            std::env::temp_dir().join(format!("bus-config-path-{}.json", uuid::Uuid::new_v4()));
        drop(
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .unwrap(),
        );
        swarm_process::private_permissions(&path, false).unwrap();
        reject_link_components(&path).unwrap();
        reject_link_components(&fs::canonicalize(&path).unwrap()).unwrap();
        fs::remove_file(path).unwrap();
    }
}
