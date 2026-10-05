use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use serde::Deserialize;
use swarm_contracts::{
    credential::Credential,
    error::{Error, Result},
};

use crate::wire::ARTIFACT_ID;

const MAX_CONFIG_BYTES: u64 = 65_536;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AdapterConfig {
    pub host_data_dir: PathBuf,
    pub native_executable: PathBuf,
    pub module_artifact_id: String,
}

impl AdapterConfig {
    pub fn read(path: &Path) -> Result<(Self, Credential)> {
        if !path.is_absolute() {
            return Err(Error::invalid("adapter config path must be absolute"));
        }
        let bytes = read_bounded(path)?;
        let mut config: Self = serde_json::from_slice(&bytes)
            .map_err(|_| Error::invalid("adapter config is invalid"))?;
        if config.module_artifact_id != ARTIFACT_ID {
            return Err(Error::new(
                "MODULE_ARTIFACT_MISMATCH",
                "adapter config must name the Rust headless artifact",
            ));
        }
        if !config.host_data_dir.is_absolute() || !config.native_executable.is_absolute() {
            return Err(Error::invalid("adapter paths must be absolute"));
        }
        config.host_data_dir = config.host_data_dir.canonicalize().map_err(|_| {
            Error::new(
                "HOST_DATA_DIR_INVALID",
                "host data directory is unavailable",
            )
        })?;
        if !std::fs::metadata(&config.host_data_dir)
            .map(|metadata| metadata.is_dir())
            .unwrap_or(false)
        {
            return Err(Error::new(
                "HOST_DATA_DIR_INVALID",
                "host data path must be a directory",
            ));
        }
        let credential_file = std::env::var_os("ELIOT_SWARM_MODULE_CREDENTIAL_FILE")
            .map(PathBuf::from)
            .filter(|path| path.is_absolute())
            .ok_or_else(|| {
                Error::new(
                    "CREDENTIAL_INVALID",
                    "module owner did not supply the protected credential file path",
                )
            })?;
        let credential_file = credential_file.canonicalize().map_err(|_| {
            Error::new(
                "CREDENTIAL_INVALID",
                "module credential file is unavailable",
            )
        })?;
        if !std::fs::metadata(&credential_file)
            .map(|metadata| metadata.is_file())
            .unwrap_or(false)
        {
            return Err(Error::new(
                "CREDENTIAL_INVALID",
                "module credential path must be a regular file",
            ));
        }
        config.native_executable = config.native_executable.canonicalize().map_err(|_| {
            Error::new(
                "NATIVE_EXECUTABLE_INVALID",
                "native executable is unavailable",
            )
        })?;
        let executable_metadata = std::fs::metadata(&config.native_executable).map_err(|_| {
            Error::new(
                "NATIVE_EXECUTABLE_INVALID",
                "native executable is unavailable",
            )
        })?;
        if !executable_metadata.is_file() || !valid_path_text(&config.native_executable) {
            return Err(Error::new(
                "NATIVE_EXECUTABLE_INVALID",
                "native executable must be a regular absolute file path",
            ));
        }
        #[cfg(windows)]
        if matches!(
            config.native_executable.extension().and_then(|value| value.to_str()),
            Some(extension) if extension.eq_ignore_ascii_case("cmd") || extension.eq_ignore_ascii_case("bat")
        ) {
            return Err(Error::new(
                "SHELL_WRAPPER_NOT_ALLOWED",
                "native executable must not be a shell wrapper",
            ));
        }

        let credential_bytes = read_bounded(&credential_file)?;
        let credential: Credential = serde_json::from_slice(&credential_bytes)
            .map_err(|_| Error::new("CREDENTIAL_INVALID", "module credential file is invalid"))?;
        if credential.client_id.trim().is_empty() || credential.token.trim().is_empty() {
            return Err(Error::new(
                "CREDENTIAL_INVALID",
                "module credential file is incomplete",
            ));
        }
        Ok((config, credential))
    }
}

fn read_bounded(path: &Path) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)
        .map_err(|_| Error::new("CONFIG_READ_FAILED", "required private file is unavailable"))?
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| {
            Error::new(
                "CONFIG_READ_FAILED",
                "required private file could not be read",
            )
        })?;
    if bytes.len() as u64 > MAX_CONFIG_BYTES {
        return Err(Error::new(
            "CONFIG_TOO_LARGE",
            "private file exceeds its size limit",
        ));
    }
    Ok(bytes)
}

fn valid_path_text(path: &Path) -> bool {
    path.to_str()
        .is_some_and(|value| value.len() <= 32 * 1024 && !value.chars().any(char::is_control))
}
