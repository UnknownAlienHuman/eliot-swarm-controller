//! Bounded, effect-free preparation for the pinned local OpenCode MCP plugin.
//!
//! The adapter only projects the plugin package into its fresh private
//! OpenCode config. It does not install a global plugin, start an MCP effect,
//! or claim that OpenCode loaded the plugin.

use crate::config::NativeOptions;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};
use swarm_contracts::error::{Error, Result};

const PINNED_VERSION: &str = "2.0.7";
const MAX_PLUGIN_SOURCE_BYTES: u64 = 512 * 1024;
const MAX_PLUGIN_ENTRY_BYTES: usize = 4096;
const MAX_CONFIG_BYTES: usize = 16 * 1024;
const PLUGIN_ENTRY_BYTES: &[u8] = b"export { default } from './native-mcp-proof.mjs';\n";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PluginSourceIdentity {
    pub(crate) module_sha256: String,
    pub(crate) entrypoint_sha256: String,
    pub(crate) config_sha256: String,
}

pub(crate) struct PreparedPluginConfig {
    bytes: Vec<u8>,
    identity: PluginSourceIdentity,
}

impl PreparedPluginConfig {
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn identity(&self) -> &PluginSourceIdentity {
        &self.identity
    }
}

/// Prepare the exact OpenCode 2.0.7 file-schema tuple from bounded local
/// source. The package path is kept local and is never written to user config.
pub(crate) fn prepare_plugin_config(options: &NativeOptions) -> Result<PreparedPluginConfig> {
    validate_options(options)?;
    let (module_path, module_sha256) = module_source()?;
    let entrypoint_path = plugin_entry_path(&module_path)?;
    let entrypoint_bytes = read_bounded_regular(&entrypoint_path, MAX_PLUGIN_ENTRY_BYTES as u64)?;
    let entrypoint_sha256 = digest(&entrypoint_bytes);
    let package_dir = module_path
        .parent()
        .ok_or_else(|| source_error("native MCP plugin directory is missing"))?;
    let package = plugin_config_package_path(package_dir)?;
    let value = json!({
        "plugin": [[package, {
            "serviceId": options.service_id.as_str(),
            "serviceVersion": PINNED_VERSION,
            "moduleSha256": module_sha256.as_str(),
        }]]
    });
    let bytes = serialize_bounded(&value)?;
    let identity = verify_plugin_config_value(&value, options)?;
    if identity.module_sha256 != module_sha256
        || identity.entrypoint_sha256 != entrypoint_sha256
    {
        return Err(source_error("native MCP plugin source changed during preparation"));
    }
    Ok(PreparedPluginConfig {
        bytes,
        identity,
    })
}

/// Revalidate the exact config shape and pinned source closure before use.
pub(crate) fn verify_plugin_config_value(
    value: &Value,
    options: &NativeOptions,
) -> Result<PluginSourceIdentity> {
    validate_options(options)?;
    let (module_path, module_sha256) = module_source()?;
    let entrypoint_path = plugin_entry_path(&module_path)?;
    let entrypoint_bytes = read_bounded_regular(&entrypoint_path, MAX_PLUGIN_ENTRY_BYTES as u64)?;
    let entrypoint_sha256 = digest(&entrypoint_bytes);
    let package_dir = module_path
        .parent()
        .ok_or_else(|| source_error("native MCP plugin directory is missing"))?;
    let package = plugin_config_package_path(package_dir)?;

    let entries = value.get("plugin").and_then(Value::as_array);
    let descriptor = entries
        .and_then(|items| items.first())
        .and_then(Value::as_array);
    let package_value = descriptor.and_then(|parts| parts.first());
    let plugin_options = descriptor.and_then(|parts| parts.get(1));
    if value.as_object().is_none_or(|object| object.len() != 1)
        || entries.is_none_or(|items| items.len() != 1)
        || descriptor.is_none_or(|parts| parts.len() != 2)
        || package_value.and_then(Value::as_str) != Some(package.as_str())
        || plugin_options
            .and_then(Value::as_object)
            .is_none_or(|object| object.len() != 3)
        || plugin_options.is_none_or(|options_value| {
            options_value.get("serviceId").and_then(Value::as_str)
                != Some(options.service_id.as_str())
                || options_value
                    .get("serviceVersion")
                    .and_then(Value::as_str)
                    != Some(PINNED_VERSION)
                || options_value
                    .get("moduleSha256")
                    .and_then(Value::as_str)
                    != Some(module_sha256.as_str())
        })
    {
        return Err(source_error(
            "private OpenCode config does not match the pinned native MCP plugin",
        ));
    }

    let config_bytes = serialize_bounded(value)?;
    Ok(PluginSourceIdentity {
        module_sha256,
        entrypoint_sha256,
        config_sha256: digest(&config_bytes),
    })
}

/// Verify the private file after creation, including its exact bounded JSON
/// encoding, before the owner process can be spawned.
pub(crate) fn verify_plugin_config_file(
    path: &Path,
    options: &NativeOptions,
) -> Result<PluginSourceIdentity> {
    let bytes = read_bounded_regular(path, MAX_CONFIG_BYTES as u64)?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| source_error("private OpenCode plugin config is invalid JSON"))?;
    if serialize_bounded(&value)? != bytes {
        return Err(source_error(
            "private OpenCode plugin config is not in the prepared bounded encoding",
        ));
    }
    verify_plugin_config_value(&value, options)
}

fn validate_options(options: &NativeOptions) -> Result<()> {
    if options.expected_version != PINNED_VERSION
        || options.service_id.is_empty()
        || options.service_id.len() > 128
        || !options
            .service_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.".contains(&byte))
    {
        return Err(source_error(
            "native MCP plugin requires a bounded service ID and OpenCode 2.0.7",
        ));
    }
    Ok(())
}

/// Resolve the adapter workspace's exact local source file and return its
/// digest. The source is never copied, modified, downloaded, or executed here.
fn module_source() -> Result<(PathBuf, String)> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| source_error("adapter workspace root is unavailable"))?;
    let package_dir = workspace_root.join("modules").join("opencode");
    verify_directory(&package_dir)?;
    let path = package_dir.join("native-mcp-proof.mjs");
    let bytes = read_bounded_regular(&path, MAX_PLUGIN_SOURCE_BYTES)?;
    if bytes.is_empty() {
        return Err(source_error("native MCP plugin source is empty"));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| source_error("native MCP plugin source path is invalid"))?;
    if canonical.to_str().is_none() || !same_lexical_path(&canonical, &path) {
        return Err(source_error(
            "native MCP plugin source path is redirected or not valid Unicode",
        ));
    }
    Ok((canonical, digest(&bytes)))
}

/// OpenCode's directory plugin resolver enters through this exact transparent
/// wrapper; keeping its bytes pinned prevents a different entrypoint from
/// being selected by the private config.
fn plugin_entry_path(module_path: &Path) -> Result<PathBuf> {
    let directory = module_path
        .parent()
        .ok_or_else(|| source_error("native MCP plugin directory is missing"))?;
    verify_directory(directory)?;
    let entry = directory.join("index.mjs");
    let bytes = read_bounded_regular(&entry, MAX_PLUGIN_ENTRY_BYTES as u64)?;
    if bytes != PLUGIN_ENTRY_BYTES {
        return Err(source_error(
            "native MCP plugin entry differs from the pinned transparent wrapper",
        ));
    }
    let canonical = entry
        .canonicalize()
        .map_err(|_| source_error("native MCP plugin entry path is invalid"))?;
    if canonical.parent() != Some(directory) || !same_lexical_path(&canonical, &entry) {
        return Err(source_error("native MCP plugin entry was redirected"));
    }
    Ok(canonical)
}

/// Project the pinned package directory into the spelling accepted by Bun's
/// OpenCode config loader while preserving the canonical source path for checks.
fn plugin_config_package_path(path: &Path) -> Result<String> {
    let value = path
        .to_str()
        .ok_or_else(|| source_error("native MCP plugin path is not valid Unicode"))?;
    const VERBATIM_PREFIX: &str = "\\\\?\\";
    const VERBATIM_UNC_PREFIX: &str = "\\\\?\\UNC\\";

    if value
        .get(..VERBATIM_UNC_PREFIX.len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case(VERBATIM_UNC_PREFIX))
    {
        let ordinary = &value[VERBATIM_UNC_PREFIX.len()..];
        let mut components = ordinary.split(['\\', '/']);
        if components
            .next()
            .is_none_or(|component| component.is_empty())
            || components
                .next()
                .is_none_or(|component| component.is_empty())
        {
            return Err(source_error("native MCP plugin UNC path is invalid"));
        }
        return Ok(format!("\\\\{ordinary}"));
    }
    if let Some(ordinary) = value.strip_prefix(VERBATIM_PREFIX) {
        let bytes = ordinary.as_bytes();
        if bytes.len() >= 3
            && bytes[0].is_ascii_alphabetic()
            && bytes[1] == b':'
            && matches!(bytes[2], b'\\' | b'/')
        {
            return Ok(ordinary.to_owned());
        }
        return Err(source_error(
            "native MCP plugin Windows path namespace is unsupported",
        ));
    }
    Ok(value.to_owned())
}

fn verify_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| source_error("native MCP plugin directory is unavailable"))?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(source_error(
            "native MCP plugin directory must be a canonical regular directory",
        ));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| source_error("native MCP plugin directory cannot be resolved"))?;
    if !same_lexical_path(&canonical, path) {
        return Err(source_error("native MCP plugin directory was redirected"));
    }
    Ok(())
}

fn read_bounded_regular(path: &Path, max_bytes: u64) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| source_error("native MCP plugin file is unavailable"))?;
    if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > max_bytes {
        return Err(source_error(
            "native MCP plugin file must be a bounded regular file",
        ));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| source_error("native MCP plugin file cannot be resolved"))?;
    if !same_lexical_path(&canonical, path) {
        return Err(source_error("native MCP plugin file was redirected"));
    }
    let file = File::open(&canonical)
        .map_err(|_| source_error("native MCP plugin file cannot be read"))?;
    let file_metadata = file
        .metadata()
        .map_err(|_| source_error("native MCP plugin file metadata is unavailable"))?;
    if !file_metadata.is_file() || file_metadata.len() > max_bytes {
        return Err(source_error(
            "native MCP plugin file changed outside its byte boundary",
        ));
    }
    let mut bytes = Vec::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|_| source_error("native MCP plugin file read failed"))?;
    if bytes.len() as u64 > max_bytes {
        return Err(source_error("native MCP plugin file exceeds its byte boundary"));
    }
    Ok(bytes)
}

fn serialize_bounded(value: &Value) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| source_error("native MCP plugin config cannot be serialized"))?;
    if bytes.is_empty() || bytes.len() > MAX_CONFIG_BYTES {
        return Err(source_error("native MCP plugin config exceeds its byte boundary"));
    }
    Ok(bytes)
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn same_lexical_path(left: &Path, right: &Path) -> bool {
    #[cfg(windows)]
    {
        let key = |path: &Path| {
            let text = path.to_string_lossy().replace('/', "\\");
            if let Some(unc) = text.strip_prefix("\\\\?\\UNC\\") {
                format!("\\\\{unc}")
            } else if let Some(local) = text.strip_prefix("\\\\?\\") {
                local.to_owned()
            } else {
                text
            }
        };
        key(left).eq_ignore_ascii_case(&key(right))
    }
    #[cfg(not(windows))]
    {
        left == right
    }
}

fn source_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_PLUGIN_SOURCE", message)
}
