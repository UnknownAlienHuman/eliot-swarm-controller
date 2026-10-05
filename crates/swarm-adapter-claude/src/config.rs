use serde::{Deserialize, Serialize};
use std::{
    env,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};
pub use swarm_client::HostConnectionConfig;
use swarm_client::IpcConfig;
use swarm_contracts::{
    Credential,
    error::{Error, Result},
};

pub const MODULE_ID: &str = "claude";
pub const ARTIFACT_ID: &str = "claude-agent-sdk-0.3.287-rust-controller.4";
pub const ARTIFACT_VERSION: &str = "4";
pub const RUNTIME: &str = "module";
const MAX_CONFIG_BYTES: usize = 64 * 1024;
const MAX_LAUNCH_VALUE_BYTES: usize = 64 * 1024;

/// Values pinned by the retained module route and its descriptor. Paths are
/// explicit; the adapter does not search for Node, a Claude executable, or an
/// SDK installation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "camelCase")]
pub struct NativeOptions {
    pub workspace_root: PathBuf,
    pub sdk_runtime_root: PathBuf,
    pub node_executable: PathBuf,
    pub native_executable: Option<PathBuf>,
    pub claude_config_dir: PathBuf,
    pub model_id: String,
    pub permission_mode: Option<String>,
    pub allow_dangerously_skip_permissions: bool,
}

impl NativeOptions {
    pub fn validate(&self) -> Result<()> {
        let paths = [
            &self.workspace_root,
            &self.sdk_runtime_root,
            &self.node_executable,
            &self.claude_config_dir,
        ];
        if paths.iter().any(|path| !path.is_absolute())
            || shell_wrapper(&self.node_executable)
            || self
                .native_executable
                .as_ref()
                .is_some_and(|path| !path.is_absolute() || shell_wrapper(path))
            || self.workspace_root.to_str().is_none()
            || self.model_id.trim().is_empty()
            || self.model_id.len() > 256
            || self.model_id.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "Claude SDK adapter requires absolute local paths and an explicit model ID",
            ));
        }
        if let Some(mode) = self.permission_mode.as_deref()
            && !matches!(
                mode,
                "default" | "acceptEdits" | "bypassPermissions" | "plan" | "dontAsk" | "auto"
            )
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "Claude permission mode is outside the pinned SDK bridge contract",
            ));
        }
        if self.permission_mode.as_deref() == Some("bypassPermissions")
            && !self.allow_dangerously_skip_permissions
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "bypassPermissions requires the descriptor's explicit dangerous-permissions flag",
            ));
        }
        Ok(())
    }

    pub fn scope_key(&self) -> String {
        format!("claude:{}", self.claude_config_dir.display())
    }
}

#[derive(Debug, Clone)]
pub struct AdapterConfig {
    pub host_data_dir: PathBuf,
    pub credential_file: PathBuf,
    pub state_dir: PathBuf,
    pub sdk_harness_dir: PathBuf,
    pub binding_id: String,
    pub generation: i64,
    pub native_options: NativeOptions,
    pub ipc: IpcConfig,
}

impl AdapterConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.host_data_dir.is_absolute()
            || !self.credential_file.is_absolute()
            || !self.state_dir.is_absolute()
            || !self.sdk_harness_dir.is_absolute()
            || self.binding_id.trim().is_empty()
            || self.binding_id.len() > 256
            || self.binding_id.bytes().any(|byte| byte.is_ascii_control())
            || self.generation < 1
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "module config requires a binding generation and absolute local paths",
            ));
        }
        self.native_options.validate()
    }
}

/// Connection and owner-scoped state received from the verified supervisor.
/// Native SDK options are intentionally absent until authenticated `module.hello`
/// returns the exact admitted binding route.
#[derive(Debug, Clone)]
pub struct HostBootstrapConfig {
    pub host_data_dir: PathBuf,
    pub credential_file: PathBuf,
    pub state_dir: PathBuf,
    pub binding_id: String,
    pub generation: i64,
    pub ipc: IpcConfig,
}

impl HostBootstrapConfig {
    pub fn validate(&self) -> Result<()> {
        if !self.host_data_dir.is_absolute()
            || !self.credential_file.is_absolute()
            || !self.state_dir.is_absolute()
            || self.binding_id.trim().is_empty()
            || self.binding_id.len() > 256
            || self.binding_id.bytes().any(|byte| byte.is_ascii_control())
            || self.generation < 1
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "module bootstrap requires a binding generation and absolute local paths",
            ));
        }
        Ok(())
    }

    pub fn with_native_options(self, native_options: NativeOptions) -> AdapterConfig {
        AdapterConfig {
            host_data_dir: self.host_data_dir,
            credential_file: self.credential_file,
            sdk_harness_dir: self.state_dir.join("sdk-harness"),
            state_dir: self.state_dir,
            binding_id: self.binding_id,
            generation: self.generation,
            native_options,
            ipc: self.ipc,
        }
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
        .map_err(|_| Error::new("ADAPTER_CREDENTIAL", "invalid module credential file"))?;
    if credential.client_id.trim().is_empty() || credential.token.is_empty() {
        return Err(Error::new(
            "ADAPTER_CREDENTIAL",
            "module credential is incomplete",
        ));
    }
    Ok(credential)
}

pub fn read_credential_path_from_env() -> Result<PathBuf> {
    required_absolute_path("ELIOT_SWARM_MODULE_CREDENTIAL_FILE")
}

pub fn required_env(name: &'static str) -> Result<String> {
    let value = env::var(name)
        .map_err(|_| Error::new("MODULE_LAUNCH_CONFIG", "required launch value is missing"))?;
    if value.is_empty()
        || value.len() > MAX_LAUNCH_VALUE_BYTES
        || value.chars().any(char::is_control)
    {
        return Err(Error::new(
            "MODULE_LAUNCH_CONFIG",
            "launch value is empty, too large, or contains control characters",
        ));
    }
    Ok(value)
}

fn required_absolute_path(name: &'static str) -> Result<PathBuf> {
    let path = PathBuf::from(required_env(name)?);
    if !path.is_absolute() {
        return Err(Error::new(
            "MODULE_LAUNCH_CONFIG",
            "descriptor-configured path must be absolute",
        ));
    }
    Ok(path)
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
        .is_ok_and(|value| value.is_file() && value.len() <= maximum as u64)
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

fn shell_wrapper(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .is_some_and(|value| value.eq_ignore_ascii_case("cmd") || value.eq_ignore_ascii_case("bat"))
}
