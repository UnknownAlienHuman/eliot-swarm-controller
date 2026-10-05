use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::Read,
    net::IpAddr,
    path::{Path, PathBuf},
};
use swarm_client::IpcConfig;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};

pub const ARTIFACT_ID: &str = "eliot-opencode-v2.rust-http.1";
pub const ARTIFACT_VERSION: &str = "0.1.0";
pub const MODULE_ID: &str = "eliot.opencode.v2";
pub const RUNTIME: &str = "module";
const MAX_CONFIG_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelRef {
    pub id: String,
    #[serde(rename = "providerID")]
    pub provider_id: String,
    pub variant: String,
}

impl ModelRef {
    pub fn valid(&self) -> bool {
        [&self.id, &self.provider_id, &self.variant]
            .into_iter()
            .all(|value| {
                !value.trim().is_empty()
                    && value.len() <= 256
                    && !value.bytes().any(|byte| byte.is_ascii_control())
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NativeOptions {
    pub service_id: String,
    pub connection_file: PathBuf,
    pub expected_version: String,
    pub directory: PathBuf,
    pub model: ModelRef,
}

impl NativeOptions {
    pub fn validate(&self) -> Result<()> {
        if self.service_id.is_empty()
            || self.service_id.len() > 128
            || !self
                .service_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || !self.connection_file.is_absolute()
            || !self.directory.is_absolute()
            || self.directory.to_str().is_none()
            || self.expected_version.trim().is_empty()
            || !self.model.valid()
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "OpenCode requires absolute paths, a service ID, exact version and explicit provider/model/variant",
            ));
        }
        Ok(())
    }

    pub fn scope_key(&self) -> String {
        format!("opencode-v2:{}", self.service_id)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub schema_version: u32,
    pub host_data_dir: PathBuf,
    pub credential_file: PathBuf,
    pub state_dir: PathBuf,
    pub binding_id: String,
    pub generation: i64,
    pub module_artifact_id: String,
    pub native_options: NativeOptions,
    #[serde(default)]
    pub ipc: IpcConfig,
}

/// Static, route-neutral IPC location selected by the exact `--config` path
/// in the trusted module launch descriptor. Binding identity, credentials,
/// and OpenCode route/model values are always resolved from the supervisor's
/// authenticated launch envelope and typed binding config.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostConnectionConfig {
    pub schema_version: u32,
    pub host_data_dir: PathBuf,
    #[serde(default)]
    pub ipc: IpcConfig,
}

impl HostConnectionConfig {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1 || !self.host_data_dir.is_absolute() {
            return Err(Error::new(
                "ADAPTER_CONFIG",
                "host connection config requires schema 1 and an absolute IPC root",
            ));
        }
        Ok(())
    }
}

impl AdapterConfig {
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != 1
            || !self.host_data_dir.is_absolute()
            || !self.credential_file.is_absolute()
            || !self.state_dir.is_absolute()
            || self.binding_id.trim().is_empty()
            || self.binding_id.len() > 256
            || self.binding_id.bytes().any(|b| b.is_ascii_control())
            || self.generation < 1
            || self.module_artifact_id != ARTIFACT_ID
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "module config requires exact artifact, binding generation and absolute local paths",
            ));
        }
        self.native_options.validate()
    }
}

pub fn read_host_config(path: &Path) -> Result<HostConnectionConfig> {
    let bytes = read_bounded_file(path, MAX_CONFIG_BYTES, "ADAPTER_CONFIG")?;
    let config: HostConnectionConfig = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("ADAPTER_CONFIG", "invalid host connection config schema"))?;
    config.validate()?;
    Ok(config)
}

pub fn read_credential(path: &Path) -> Result<Credential> {
    let bytes = read_bounded_file(path, 64 * 1024, "ADAPTER_CREDENTIAL")?;
    let credential: Credential = serde_json::from_slice(&bytes)
        .map_err(|_| Error::new("ADAPTER_CREDENTIAL", "invalid host credential file"))?;
    if credential.client_id.trim().is_empty() || credential.token.is_empty() {
        return Err(Error::new(
            "ADAPTER_CREDENTIAL",
            "host credential is incomplete",
        ));
    }
    Ok(credential)
}

fn read_bounded_file(path: &Path, maximum: usize, code: &'static str) -> Result<Vec<u8>> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|_| Error::new(code, "configured file is unavailable"))?;
    if !metadata.is_file() || metadata.len() > maximum as u64 {
        return Err(Error::new(
            code,
            "configured file must be a bounded regular file",
        ));
    }
    let mut file =
        File::open(path).map_err(|_| Error::new(code, "configured file cannot be read"))?;
    if !file
        .metadata()
        .is_ok_and(|m| m.is_file() && m.len() <= maximum as u64)
    {
        return Err(Error::new(code, "configured file changed while opening"));
    }
    let mut bytes = Vec::new();
    file.take((maximum + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| Error::new(code, "configured file read failed"))?;
    if bytes.len() > maximum {
        return Err(Error::new(
            code,
            "configured file exceeds its size boundary",
        ));
    }
    Ok(bytes)
}

pub fn is_loopback_endpoint(url: &reqwest::Url) -> bool {
    url.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    })
}
