//! Safe filesystem layout for a declared managed service scope.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use swarm_contracts::{
    DeclaredServiceScope,
    error::{Error, Result},
};

/// A reversible-free directory component for service state. Business IDs are
/// never joined directly to a path; generation is included to prevent an old
/// receipt from being treated as a new enrollment.
pub fn managed_service_component(scope: &DeclaredServiceScope) -> Result<String> {
    scope.validate()?;
    let bytes = serde_json::to_vec(scope)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub fn managed_worker_config_path(data_root: &Path, service_id: &str) -> Result<PathBuf> {
    if !data_root.is_absolute() {
        return Err(Error::invalid("managed bus root must be absolute"));
    }
    // Config identity is the uniquely issued consumer credential. The owner
    // receipt additionally includes its Store-persisted generation.
    DeclaredServiceScope::validate_service_id(service_id)?;
    let component = format!("{:x}", Sha256::digest(service_id.as_bytes()));
    Ok(data_root
        .join("bus-workers")
        .join(component)
        .join("worker.json"))
}

pub fn managed_service_state_path(
    service_root: &Path,
    scope: &DeclaredServiceScope,
) -> Result<PathBuf> {
    if !service_root.is_absolute() {
        return Err(Error::invalid("managed service root must be absolute"));
    }
    let component = managed_service_component(scope)?;
    Ok(service_root.join("bus-service-owner").join(component))
}
