//! Effect-free OpenCode 2.0.7 plugin configuration for a fresh owned service.
//!
//! This module only validates the pinned local source and constructs an
//! in-memory configuration document. It never writes global config, contacts a
//! service, or claims that the plugin was loaded.

use super::mcp_tools;
use crate::{
    error::{Error, Result},
    model::{self, Role},
    store::launcher::LaunchActor,
};
use serde_json::{Value, json};
use std::{fs, path::Path};

const PINNED_VERSION: &str = "2.0.7";
const MAX_ID_BYTES: usize = 256;
const MAX_CONFIG_BYTES: usize = 16 * 1024;

/// Origin is explicit and cannot be deserialized from a route request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnedServiceOrigin {
    FreshOwnedService,
}

/// Store-admitted provenance for one opening binding and one exact workspace
/// lease. Store constructs this only after its current-actor and CAS checks.
pub(crate) struct OwnedServiceSeed {
    pub(crate) launch_operation_id: String,
    pub(crate) open_operation_id: String,
    pub(crate) open_operation_state: String,
    pub(crate) actor: LaunchActor,
    pub(crate) task_id: String,
    pub(crate) task_revision: i64,
    pub(crate) attempt_id: String,
    pub(crate) lease_id: String,
    pub(crate) lease_state: String,
    pub(crate) lease_generation: i64,
    pub(crate) binding_id: String,
    pub(crate) binding_state: String,
    pub(crate) binding_generation: i64,
    pub(crate) binding_digest: String,
    pub(crate) service_id: String,
    pub(crate) service_version: String,
    pub(crate) route_digest: String,
    pub(crate) owner_nonce: String,
    pub(crate) origin: OwnedServiceOrigin,
}

/// Opaque Store admission. This is not request-deserializable and cannot be
/// constructed from a generic external OpenCode route.
#[derive(Debug, Clone)]
pub(crate) struct OwnedServiceIntent {
    service_id: String,
    service_version: String,
    route_digest: String,
    owner_nonce: String,
    origin: OwnedServiceOrigin,
}

impl OwnedServiceIntent {
    /// Mint the in-process proof only from Store's exact fresh-service gate.
    pub(crate) fn from_store_admission(seed: OwnedServiceSeed) -> Result<Self> {
        match seed.actor.role() {
            Role::Manager | Role::Operator => {}
            _ => return Err(scope_error("owned OpenCode requires a Manager or Operator")),
        }
        for (field, value) in [
            ("launch operation", seed.launch_operation_id.as_str()),
            ("open operation", seed.open_operation_id.as_str()),
            ("actor caller", seed.actor.technical_requester_id()),
            ("effective manager", seed.actor.effective_manager_id()),
            ("Task", seed.task_id.as_str()),
            ("Attempt", seed.attempt_id.as_str()),
            ("workspace lease", seed.lease_id.as_str()),
            ("binding", seed.binding_id.as_str()),
            ("service", seed.service_id.as_str()),
        ] {
            validate_identity(field, value, MAX_ID_BYTES)?;
        }
        if let Some(link_id) = seed.actor.link_id() {
            validate_identity("actor link", link_id, MAX_ID_BYTES)?;
        }
        if seed.task_revision <= 0
            || seed.lease_generation <= 0
            || seed.binding_generation <= 0
            || seed.lease_state != "held"
            || seed.binding_state != "opening"
            || seed.open_operation_state != "queued"
        {
            return Err(scope_error(
                "fresh service requires an exact opening binding, queued open, and held lease",
            ));
        }
        if seed.service_version != PINNED_VERSION {
            return Err(scope_error("owned native MCP requires OpenCode 2.0.7"));
        }
        validate_digest(&seed.binding_digest, "binding digest")?;
        validate_digest(&seed.route_digest, "service route digest")?;
        if !valid_uuid(&seed.owner_nonce) {
            return Err(scope_error("owned service owner nonce is invalid"));
        }
        Ok(Self {
            service_id: seed.service_id,
            service_version: seed.service_version,
            route_digest: seed.route_digest,
            owner_nonce: seed.owner_nonce,
            origin: seed.origin,
        })
    }

    pub(crate) fn owner_nonce(&self) -> &str {
        &self.owner_nonce
    }
    pub(crate) fn route_digest(&self) -> &str {
        &self.route_digest
    }
}

/// A validated, configuration-only projection for a fresh service.
pub(crate) struct PreparedPluginConfig {
    owner_nonce: String,
    config_value: Value,
    config_digest: String,
    module_sha256: String,
    entrypoint_sha256: String,
}

impl PreparedPluginConfig {
    pub(crate) fn config_value(&self) -> &Value {
        &self.config_value
    }
    pub(crate) fn config_digest(&self) -> &str {
        &self.config_digest
    }
    pub(crate) fn module_sha256(&self) -> &str {
        &self.module_sha256
    }
    pub(crate) fn entrypoint_sha256(&self) -> &str {
        &self.entrypoint_sha256
    }
    pub(crate) fn owner_nonce(&self) -> &str {
        &self.owner_nonce
    }
}

/// Pure preparation from the typed route and Store-minted exact intent.
pub(crate) fn prepare_plugin_config(
    route: &super::owned_service::OwnedServiceRoute,
    intent: &OwnedServiceIntent,
) -> Result<PreparedPluginConfig> {
    if intent.origin != OwnedServiceOrigin::FreshOwnedService
        || route.service_id() != intent.service_id
        || route.version() != intent.service_version
        || route.owner_nonce() != intent.owner_nonce
        || route.route_digest()? != intent.route_digest
    {
        return Err(scope_error(
            "owned service route differs from Store admission",
        ));
    }
    let options = route.options();
    let plugin_descriptor = mcp_tools::plugin_config_value(&options)?;
    let (module_path, module_sha256) = mcp_tools::module_source()?;
    let entrypoint_path = mcp_tools::plugin_entry_path(&module_path)?;
    let entrypoint_bytes = read_bounded_regular(&entrypoint_path, 4096)?;
    let entrypoint_sha256 = model::digest(&entrypoint_bytes);
    let package_dir = module_path
        .parent()
        .ok_or_else(|| source_error("plugin directory is missing"))?;
    let package_text = package_dir
        .to_str()
        .ok_or_else(|| source_error("plugin path is not valid Unicode"))?;
    if plugin_descriptor["package"] != package_text
        || plugin_descriptor["options"]["serviceId"] != intent.service_id
        || plugin_descriptor["options"]["serviceVersion"] != PINNED_VERSION
        || plugin_descriptor["options"]["moduleSha256"] != module_sha256
        || plugin_descriptor
            .as_object()
            .is_none_or(|object| object.len() != 2)
        || plugin_descriptor["options"]
            .as_object()
            .is_none_or(|object| object.len() != 3)
    {
        return Err(source_error(
            "plugin package configuration is not the pinned directory entry",
        ));
    }
    // OpenCode 2.0.7's file schema uses `plugin` entries in the form
    // `[package, options]`. The server normalizes these to `config.plugins`
    // objects only after parsing, so persist the input form here.
    let config_value = json!({
        "plugin": [[plugin_descriptor["package"].clone(), plugin_descriptor["options"].clone()]]
    });
    let canonical = model::canonical(&config_value)?;
    if canonical.len() > MAX_CONFIG_BYTES {
        return Err(source_error("plugin config exceeds the retained bound"));
    }
    Ok(PreparedPluginConfig {
        owner_nonce: intent.owner_nonce.clone(),
        config_value,
        config_digest: model::digest(canonical.as_bytes()),
        module_sha256,
        entrypoint_sha256,
    })
}

/// Revalidate the serialized OpenCode 2.0.7 input form against the exact
/// package directory, wrapper, and inner observer source currently on disk.
pub(crate) fn verify_plugin_config_value(
    value: &Value,
    service_id: &str,
) -> Result<(String, String)> {
    let (module_path, module_sha256) = mcp_tools::module_source()?;
    let entrypoint_path = mcp_tools::plugin_entry_path(&module_path)?;
    let entrypoint_bytes = read_bounded_regular(&entrypoint_path, 4096)?;
    let entrypoint_sha256 = model::digest(&entrypoint_bytes);
    let package_dir = module_path
        .parent()
        .ok_or_else(|| source_error("plugin directory is missing"))?;
    let package_text = package_dir
        .to_str()
        .ok_or_else(|| source_error("plugin path is not valid Unicode"))?;
    let entries = value["plugin"].as_array();
    let descriptor = entries
        .and_then(|items| items.first())
        .and_then(Value::as_array);
    let options = descriptor.and_then(|parts| parts.get(1));
    if value.as_object().is_none_or(|object| object.len() != 1)
        || entries.is_none_or(|items| items.len() != 1)
        || descriptor.is_none_or(|parts| parts.len() != 2)
        || descriptor
            .and_then(|parts| parts.first())
            .and_then(Value::as_str)
            != Some(package_text)
        || options
            .and_then(Value::as_object)
            .is_none_or(|object| object.len() != 3)
        || options.is_none_or(|opts| {
            opts["serviceId"] != service_id
                || opts["serviceVersion"] != PINNED_VERSION
                || opts["moduleSha256"] != module_sha256
        })
    {
        return Err(source_error(
            "owned config does not match the pinned plugin directory tuple",
        ));
    }
    Ok((module_sha256, entrypoint_sha256))
}

fn read_bounded_regular(path: &Path, max_bytes: usize) -> Result<Vec<u8>> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| source_error("plugin entry is unavailable"))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() as usize > max_bytes
    {
        return Err(source_error("plugin entry is not a bounded regular file"));
    }
    let canonical = path
        .canonicalize()
        .map_err(|_| source_error("plugin entry cannot be resolved"))?;
    if !same_lexical_path(&canonical, path) {
        return Err(source_error("plugin entry was redirected"));
    }
    fs::read(path).map_err(|_| source_error("plugin entry cannot be read"))
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

fn validate_identity(field: &str, value: &str, limit: usize) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > limit
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(scope_error(&format!("invalid owned service {field}")));
    }
    Ok(())
}
fn validate_digest(value: &str, label: &str) -> Result<()> {
    if !is_sha256(value) {
        return Err(scope_error(&format!("owned service {label} is invalid")));
    }
    Ok(())
}
fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn valid_uuid(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 36
        && [8, 13, 18, 23].iter().all(|i| bytes[*i] == b'-')
        && bytes.iter().enumerate().all(|(i, b)| {
            [8, 13, 18, 23].contains(&i) || b.is_ascii_hexdigit() && !b.is_ascii_uppercase()
        })
        && bytes[14] == b'4'
        && matches!(bytes[19], b'8' | b'9' | b'a' | b'b')
}
fn scope_error(message: &str) -> Error {
    Error::new("OWNED_SERVICE_SCOPE", message)
}
fn source_error(message: &str) -> Error {
    Error::new("NATIVE_MCP_PLUGIN_SOURCE", message)
}
