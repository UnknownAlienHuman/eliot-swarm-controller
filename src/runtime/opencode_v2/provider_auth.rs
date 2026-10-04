//! One-shot private OpenCode provider credential bootstrap.
//!
//! The selected key is read only by the controller host and exists only in a
//! non-serializable in-memory value until the owned service's exact startup
//! transaction completes. No helper plan, configuration file, Store proof, or
//! public response contains credential material.

use super::{ModelRef, Service};
use crate::{
    error::{Error, Result},
    model,
};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
    sync::atomic::{Ordering, compiler_fence},
};

const PROVIDER_ID: &str = "opencode-go";
const MAX_AUTH_FILE_BYTES: u64 = 1024 * 1024;
const MAX_KEY_BYTES: usize = 16 * 1024;
const MAX_SAFE_ID_BYTES: usize = 256;

/// Secret bytes are never serializable or cloneable and are cleared on drop.
struct SecretKey(Vec<u8>);

impl SecretKey {
    fn from_string(value: String) -> Self {
        Self(value.into_bytes())
    }

    fn as_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.0)
            .map_err(|_| auth_error("configured provider credential is not valid UTF-8"))
    }

    fn is_usable(&self) -> bool {
        self.0.len() <= MAX_KEY_BYTES
            && std::str::from_utf8(&self.0)
                .is_ok_and(|value| !value.trim().is_empty() && !value.chars().any(char::is_control))
    }

    fn clear(&mut self) {
        wipe(&mut self.0);
        self.0.clear();
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        wipe(&mut self.0);
    }
}

/// Host-memory-only credential tied to one exact launch scope and model.
pub(super) struct PreparedProviderCredential {
    credential_ref: String,
    model_digest: String,
    owner_nonce: String,
    route_digest: String,
    scope_digest: String,
    key: SecretKey,
}

/// Opaque single-use authorization to perform the one native key POST after
/// this exact owned service reports ready. It has no Serde or Clone path.
pub(super) struct ProviderAuthPermit {
    credential_ref: String,
    model_digest: String,
    owner_nonce: String,
    route_digest: String,
    scope_digest: String,
    consumed: bool,
}

#[derive(Clone, Copy)]
pub(super) struct OwnedServiceAuthScope<'a> {
    pub credential_ref: &'a str,
    pub model_ref: &'a ModelRef,
    pub service_id: &'a str,
    pub service_version: &'a str,
    pub owner_nonce: &'a str,
    pub pid: u32,
    pub birth_token: &'a str,
    pub directory: &'a Path,
}

impl ProviderAuthPermit {
    pub(super) fn from_start_scope(
        credential: &PreparedProviderCredential,
        owner_nonce: &str,
        route_digest: &str,
        scope_digest: &str,
    ) -> Result<Self> {
        if credential.owner_nonce != owner_nonce
            || credential.route_digest != route_digest
            || credential.scope_digest != scope_digest
        {
            return Err(auth_error(
                "provider authorization differs from the admitted start scope",
            ));
        }
        Ok(Self {
            credential_ref: credential.credential_ref.clone(),
            model_digest: credential.model_digest.clone(),
            owner_nonce: owner_nonce.to_owned(),
            route_digest: route_digest.to_owned(),
            scope_digest: scope_digest.to_owned(),
            consumed: false,
        })
    }
}

/// Resolve one configured provider entry before the Store's durable start
/// boundary. `source_path` comes only from the explicit host Config registry.
pub(super) fn prepare_credential(
    credential_ref: &str,
    model_ref: &ModelRef,
    source_path: &Path,
    owner_nonce: &str,
    route_digest: &str,
    scope_digest: &str,
) -> Result<PreparedProviderCredential> {
    validate_ref(credential_ref)?;
    if model_ref.provider_id != PROVIDER_ID || !model_ref.valid() {
        return Err(auth_error(
            "configured provider credential does not match the retained model",
        ));
    }
    let key = read_selected_key(source_path)?;
    Ok(PreparedProviderCredential {
        credential_ref: credential_ref.to_owned(),
        model_digest: model_digest(model_ref)?,
        owner_nonce: owner_nonce.to_owned(),
        route_digest: route_digest.to_owned(),
        scope_digest: scope_digest.to_owned(),
        key,
    })
}

/// Perform exactly one authenticated native key connection for the exact
/// ready process. This function is never used for retained-service readback.
pub(super) async fn bootstrap_once(
    service: &Service,
    credential: PreparedProviderCredential,
    mut permit: ProviderAuthPermit,
    scope: OwnedServiceAuthScope<'_>,
) -> Result<Value> {
    let expected_model_digest = model_digest(scope.model_ref)?;
    if permit.consumed
        || credential.credential_ref != permit.credential_ref
        || credential.model_digest != permit.model_digest
        || credential.model_digest != expected_model_digest
        || credential.owner_nonce != permit.owner_nonce
        || credential.route_digest != permit.route_digest
        || credential.scope_digest != permit.scope_digest
        || scope.owner_nonce != permit.owner_nonce
        || scope.credential_ref != credential.credential_ref
        || scope.model_ref.provider_id != PROVIDER_ID
        || scope.pid == 0
        || !is_sha256(scope.birth_token)
    {
        return Err(auth_error(
            "provider bootstrap scope is stale or mismatched",
        ));
    }

    // Consume before even the capability GET: a failed attempt remains under
    // the durable startup uncertainty fence and can never replay the POST.
    permit.consumed = true;
    let location = scope
        .directory
        .to_str()
        .filter(|text| !text.is_empty() && text.len() <= 4096)
        .ok_or_else(|| auth_error("owned service directory is not a bounded path"))?;
    let before = get_integration(service, location).await?;
    validate_key_method(&before)?;
    if !before.credential_ids.is_empty() {
        return Err(auth_error(
            "owned provider already has a credential connection before bootstrap",
        ));
    }

    let post_result = service
        .post_integration_key(PROVIDER_ID, credential.key.as_str()?)
        .await;
    let mut credential = credential;
    credential.key.clear();
    post_result?;
    // The API key is dropped and zeroed as soon as the one POST completes.
    // Only safe credential metadata is read back and retained below.
    let after = get_integration(service, location).await?;
    validate_key_method(&after)?;
    let metadata = one_credential_connection(&after)?;
    provider_auth_proof(scope, &credential.model_digest, metadata)
}

/// Read-only verification for retained services. It never posts or repairs.
pub(super) async fn verify_retained_connection(
    service: &Service,
    scope: OwnedServiceAuthScope<'_>,
) -> Result<Value> {
    validate_ref(scope.credential_ref)?;
    if scope.model_ref.provider_id != PROVIDER_ID || scope.pid == 0 || !is_sha256(scope.birth_token)
    {
        return Err(auth_error("retained provider identity is invalid"));
    }
    let location = scope
        .directory
        .to_str()
        .filter(|text| !text.is_empty() && text.len() <= 4096)
        .ok_or_else(|| auth_error("owned service directory is not a bounded path"))?;
    let integration = get_integration(service, location).await?;
    validate_key_method(&integration)?;
    let metadata = one_credential_connection(&integration)?;
    provider_auth_proof(scope, &model_digest(scope.model_ref)?, metadata)
}

/// Validate the non-secret provider readback against the exact configured
/// scope. The `stored_unverified` state is deliberately weaker than key or
/// model validation.
pub(super) fn validate_proof(proof: &Value, scope: OwnedServiceAuthScope<'_>) -> Result<()> {
    let object = proof
        .as_object()
        .ok_or_else(|| auth_error("retained provider proof is malformed"))?;
    let expected_keys = [
        "status",
        "credential_ref",
        "provider_id",
        "model_digest",
        "service_id",
        "service_version",
        "owner_nonce_digest",
        "process",
        "integration_id",
        "key_method",
        "connection_metadata_digest",
    ];
    let expected_model_digest = model_digest(scope.model_ref)?;
    let expected_nonce_digest = model::digest(scope.owner_nonce.as_bytes());
    let process = &proof["process"];
    if object.len() != expected_keys.len()
        || object
            .keys()
            .any(|key| !expected_keys.contains(&key.as_str()))
        || proof["status"] != "stored_unverified"
        || proof["credential_ref"] != scope.credential_ref
        || proof["provider_id"] != PROVIDER_ID
        || proof["model_digest"] != expected_model_digest
        || proof["service_id"] != scope.service_id
        || proof["service_version"] != scope.service_version
        || proof["owner_nonce_digest"] != expected_nonce_digest
        || process.as_object().is_none_or(|object| {
            object.len() != 2
                || object
                    .keys()
                    .any(|key| !["pid", "birth_token"].contains(&key.as_str()))
                || object.get("pid").and_then(Value::as_u64) != Some(u64::from(scope.pid))
                || object.get("birth_token").and_then(Value::as_str) != Some(scope.birth_token)
        })
        || proof["integration_id"] != PROVIDER_ID
        || proof["key_method"] != "key"
        || proof["connection_metadata_digest"]
            .as_str()
            .is_none_or(|value| !is_sha256(value))
    {
        return Err(auth_error(
            "retained provider proof differs from its exact owned route",
        ));
    }
    Ok(())
}

fn provider_auth_proof(
    scope: OwnedServiceAuthScope<'_>,
    model_digest: &str,
    credential_id: &str,
) -> Result<Value> {
    if !safe_metadata_text(credential_id) {
        return Err(auth_error("provider credential metadata is malformed"));
    }
    let metadata = json!({"credential_id":credential_id});
    Ok(json!({
        "status":"stored_unverified",
        "credential_ref":scope.credential_ref,
        "provider_id":PROVIDER_ID,
        "model_digest":model_digest,
        "service_id":scope.service_id,
        "service_version":scope.service_version,
        "owner_nonce_digest":model::digest(scope.owner_nonce.as_bytes()),
        "process":{"pid":scope.pid,"birth_token":scope.birth_token},
        "integration_id":PROVIDER_ID,
        "key_method":"key",
        "connection_metadata_digest":model::digest(model::canonical(&metadata)?.as_bytes()),
    }))
}

struct IntegrationSnapshot {
    key_method_count: usize,
    credential_ids: Vec<String>,
}

async fn get_integration(service: &Service, directory: &str) -> Result<IntegrationSnapshot> {
    let response = service
        .get(
            "/api/integration",
            &[("location[directory]", directory.to_owned())],
        )
        .await?;
    if response["location"]["directory"].as_str() != Some(directory) {
        return Err(auth_error(
            "provider integration response is outside the owned workspace",
        ));
    }
    let integrations = response
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| auth_error("provider integration list is malformed"))?;
    let mut selected = None;
    for integration in integrations {
        let object = integration
            .as_object()
            .ok_or_else(|| auth_error("provider integration list entry is malformed"))?;
        let id = object
            .get("id")
            .and_then(Value::as_str)
            .filter(|value| safe_metadata_text(value))
            .ok_or_else(|| auth_error("provider integration identity is malformed"))?;
        if id == PROVIDER_ID {
            if selected.is_some() {
                return Err(auth_error("provider integration identity is ambiguous"));
            }
            selected = Some(integration);
        }
    }
    let integration =
        selected.ok_or_else(|| auth_error("provider integration identity is unavailable"))?;
    let methods = integration["methods"]
        .as_array()
        .ok_or_else(|| auth_error("provider integration methods are malformed"))?;
    let connections = integration["connections"]
        .as_array()
        .ok_or_else(|| auth_error("provider integration connections are malformed"))?;
    let credential_ids = connections
        .iter()
        .filter(|connection| connection["type"] == "credential")
        .map(|connection| {
            connection["id"]
                .as_str()
                .filter(|value| safe_metadata_text(value))
                .map(str::to_owned)
                .ok_or_else(|| auth_error("provider credential metadata is malformed"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(IntegrationSnapshot {
        key_method_count: methods
            .iter()
            .filter(|method| method["type"] == "key")
            .count(),
        credential_ids,
    })
}

fn validate_key_method(integration: &IntegrationSnapshot) -> Result<()> {
    if integration.key_method_count != 1 {
        return Err(auth_error(
            "provider does not expose one unambiguous API-key method",
        ));
    }
    Ok(())
}

fn one_credential_connection(integration: &IntegrationSnapshot) -> Result<&str> {
    if integration.credential_ids.len() != 1 {
        return Err(auth_error(
            "provider credential metadata readback is absent or ambiguous",
        ));
    }
    integration
        .credential_ids
        .first()
        .map(String::as_str)
        .ok_or_else(|| auth_error("provider credential metadata is absent"))
}

fn safe_metadata_text(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_SAFE_ID_BYTES
        && !value.chars().any(char::is_control)
}

fn read_selected_key(path: &Path) -> Result<SecretKey> {
    validate_source_path(path)?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| auth_error("authorized provider credential source is unavailable"))?;
    if !metadata.is_file()
        || is_reparse(&metadata)
        || metadata.len() == 0
        || metadata.len() > MAX_AUTH_FILE_BYTES
    {
        return Err(auth_error(
            "authorized provider credential source is not a bounded plain file",
        ));
    }
    let canonical = fs::canonicalize(path)
        .map_err(|_| auth_error("authorized provider credential source cannot be resolved"))?;
    if !same_path(&canonical, path)? {
        return Err(auth_error(
            "authorized provider credential source path was redirected",
        ));
    }
    let mut file = File::open(path)
        .map_err(|_| auth_error("authorized provider credential source cannot be opened"))?;
    let opened = file
        .metadata()
        .map_err(|_| auth_error("authorized provider credential source cannot be inspected"))?;
    if !opened.is_file() || opened.len() != metadata.len() || is_reparse(&opened) {
        return Err(auth_error(
            "authorized provider credential source changed while opening",
        ));
    }
    let modified_before = opened.modified().ok();
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    let read_failed = {
        let mut reader = (&mut file).take(MAX_AUTH_FILE_BYTES + 1);
        reader.read_to_end(&mut bytes).is_err()
    };
    let opened_after = file.metadata().ok();
    if read_failed
        || bytes.len() as u64 != metadata.len()
        || bytes.len() as u64 > MAX_AUTH_FILE_BYTES
        || opened_after.as_ref().is_none_or(|after| {
            !after.is_file()
                || is_reparse(after)
                || after.len() != opened.len()
                || after.modified().ok() != modified_before
        })
    {
        wipe(&mut bytes);
        return Err(auth_error(
            "authorized provider credential source changed while reading",
        ));
    }
    if fs::canonicalize(path)
        .map(|after| !same_path(&after, path).unwrap_or(false))
        .unwrap_or(true)
    {
        wipe(&mut bytes);
        return Err(auth_error(
            "authorized provider credential source was redirected while reading",
        ));
    }
    let parsed = serde_json::from_slice::<SelectedAuthFile>(&bytes);
    wipe(&mut bytes);
    let key = parsed
        .map_err(|_| auth_error("authorized provider credential entry is invalid or ambiguous"))?
        .0
        .0;
    if !key.is_usable() {
        return Err(auth_error(
            "authorized provider credential entry is unusable",
        ));
    }
    Ok(key)
}

fn validate_source_path(path: &Path) -> Result<()> {
    if !path.is_absolute()
        || path.file_name().and_then(|value| value.to_str()) != Some("auth.json")
        || path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(auth_error(
            "authorized provider credential source path is malformed",
        ));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::Normal(part) => {
                current.push(part);
                let metadata = fs::symlink_metadata(&current).map_err(|_| {
                    auth_error("authorized provider credential source path is unavailable")
                })?;
                if is_reparse(&metadata) {
                    return Err(auth_error(
                        "authorized provider credential source contains a reparse point",
                    ));
                }
                if current != path && !metadata.is_dir() {
                    return Err(auth_error(
                        "authorized provider credential source ancestor is not a directory",
                    ));
                }
                let canonical = fs::canonicalize(&current).map_err(|_| {
                    auth_error("authorized provider credential source ancestor is redirected")
                })?;
                if !same_path(&canonical, &current)? {
                    return Err(auth_error(
                        "authorized provider credential source ancestor is redirected",
                    ));
                }
            }
            Component::CurDir | Component::ParentDir => {
                return Err(auth_error(
                    "authorized provider source path is not normalized",
                ));
            }
        }
    }
    Ok(())
}

fn is_reparse(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x0000_0400 != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

fn same_path(left: &Path, right: &Path) -> Result<bool> {
    #[cfg(windows)]
    {
        let normalize = |path: &Path| -> Result<String> {
            let text = path
                .to_str()
                .ok_or_else(|| auth_error("authorized credential path is not valid Unicode"))?
                .replace('/', "\\");
            let normalized = if let Some(unc) = text.strip_prefix("\\\\?\\UNC\\") {
                format!("\\\\{unc}")
            } else if let Some(local) = text.strip_prefix("\\\\?\\") {
                local.to_owned()
            } else {
                text
            };
            Ok(normalized.to_ascii_lowercase())
        };
        Ok(normalize(left)? == normalize(right)?)
    }
    #[cfg(not(windows))]
    {
        Ok(left == right)
    }
}

pub(super) fn validate_ref(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(auth_error(
            "authorized provider credential reference is malformed",
        ));
    }
    Ok(())
}

fn model_digest(model_ref: &ModelRef) -> Result<String> {
    Ok(model::digest(
        model::canonical(&serde_json::to_value(model_ref)?)?.as_bytes(),
    ))
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn wipe(bytes: &mut [u8]) {
    for byte in bytes {
        // Volatile stores keep the explicit secret clear from being optimized
        // away when the host drops its short-lived source and key buffers.
        unsafe { std::ptr::write_volatile(byte, 0) };
    }
    compiler_fence(Ordering::SeqCst);
}

struct SelectedProviderEntry(SecretKey);

impl<'de> Deserialize<'de> for SelectedProviderEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EntryVisitor;
        impl<'de> Visitor<'de> for EntryVisitor {
            type Value = SelectedProviderEntry;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("one unambiguous API-key provider entry")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut seen = BTreeSet::new();
                let mut entry_type: Option<String> = None;
                let mut key: Option<SecretKey> = None;
                while let Some(name) = map.next_key::<String>()? {
                    if !seen.insert(name.clone()) {
                        return Err(serde::de::Error::custom("duplicate provider-entry field"));
                    }
                    match name.as_str() {
                        "type" => entry_type = Some(map.next_value::<String>()?),
                        "key" => key = Some(SecretKey::from_string(map.next_value::<String>()?)),
                        _ => {
                            map.next_value::<IgnoredAny>()?;
                        }
                    }
                }
                if entry_type.as_deref() != Some("api") {
                    return Err(serde::de::Error::custom(
                        "provider entry is not API-key shaped",
                    ));
                }
                let key =
                    key.ok_or_else(|| serde::de::Error::custom("provider API key is missing"))?;
                if !key.is_usable() {
                    return Err(serde::de::Error::custom("provider API key is unusable"));
                }
                Ok(SelectedProviderEntry(key))
            }
        }
        deserializer.deserialize_map(EntryVisitor)
    }
}

struct SelectedAuthFile(SelectedProviderEntry);

impl<'de> Deserialize<'de> for SelectedAuthFile {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AuthVisitor;
        impl<'de> Visitor<'de> for AuthVisitor {
            type Value = SelectedAuthFile;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("an auth.json provider map with one opencode-go entry")
            }

            fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut selected: Option<SelectedProviderEntry> = None;
                while let Some(provider) = map.next_key::<String>()? {
                    if provider == PROVIDER_ID {
                        if selected.is_some() {
                            return Err(serde::de::Error::custom(
                                "duplicate opencode-go provider entry",
                            ));
                        }
                        selected = Some(map.next_value::<SelectedProviderEntry>()?);
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                selected
                    .map(SelectedAuthFile)
                    .ok_or_else(|| serde::de::Error::custom("opencode-go entry is missing"))
            }
        }
        deserializer.deserialize_map(AuthVisitor)
    }
}

fn auth_error(message: &str) -> Error {
    Error::new("OWNED_PROVIDER_AUTH", message)
}
