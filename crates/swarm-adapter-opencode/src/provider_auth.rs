//! One-shot provider authorization for an adapter-owned OpenCode service.
//!
//! The source path is a supervisor-resolved protected value.  This module
//! reads only the exact selected model-provider API-key entry, consumes it for
//! one native POST, and retains only bounded non-secret readback metadata.  It has
//! no Store, SQL, retry, or completion authority.

use crate::{
    config::{ModelRef, NativeOptions},
    native::NativeClient,
};
use serde::de::{IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
};
use swarm_contracts::error::{Error, Result};

const MAX_AUTH_FILE_BYTES: u64 = 1024 * 1024;
const MAX_KEY_BYTES: usize = 16 * 1024;
const MAX_SAFE_ID_BYTES: usize = 256;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderAuthOptions {
    pub source_file: PathBuf,
    pub credential_ref: Option<String>,
}

impl ProviderAuthOptions {
    pub fn validate_for(&self, native: &NativeOptions) -> Result<()> {
        if !valid_provider_id(&native.model.provider_id)
            || !absolute_auth_path(&self.source_file)
            || self
                .credential_ref
                .as_deref()
                .is_some_and(|value| !valid_ref(value))
        {
            return Err(auth_error(
                "provider authorization does not match the OpenCode route",
            ));
        }
        Ok(())
    }
}

/// Perform the single provider key POST for the just-ready native owner.
/// `NATIVE_REJECTED` is used only for a deterministic preflight or native
/// rejection; all transport/readback uncertainty remains unknown to the
/// caller.
pub async fn bootstrap_once(
    native: &NativeClient,
    options: &NativeOptions,
    auth: &ProviderAuthOptions,
) -> Result<Value> {
    auth.validate_for(options)?;
    let provider_id = options.model.provider_id.as_str();
    let key = read_selected_key(&auth.source_file, provider_id)?;
    let directory = canonical_directory_text(&options.directory)?;
    let before = native.provider_integration(&directory).await?;
    validate_integration(&before, &directory, provider_id, false)?;
    native
        .post_provider_key(provider_id, key.as_str()?, &directory)
        .await?;
    let after = native.provider_integration(&directory).await?;
    let credential_id = validate_integration(&after, &directory, provider_id, true)?;
    Ok(json!({
        "status":"stored_unverified",
        "provider_id":provider_id,
        "credential_ref":auth.credential_ref,
        "model_digest":model_digest(&options.model)?,
        "service":{"pid":native.pid(),"version":native.version()},
        "integration_id":provider_id,
        "key_method":"key",
        "credential_metadata_digest":sha256_json(&json!({"credential_id":credential_id}))
    }))
}

struct SecretKey(Vec<u8>);

impl SecretKey {
    fn as_str(&self) -> Result<&str> {
        std::str::from_utf8(&self.0).map_err(|_| auth_error("provider key is not valid UTF-8"))
    }

    fn is_usable(&self) -> bool {
        self.0.len() <= MAX_KEY_BYTES
            && std::str::from_utf8(&self.0)
                .is_ok_and(|value| !value.trim().is_empty() && !value.chars().any(char::is_control))
    }
}

impl Drop for SecretKey {
    fn drop(&mut self) {
        for byte in &mut self.0 {
            // SAFETY: each byte belongs to this owned secret buffer.
            unsafe { std::ptr::write_volatile(byte, 0) };
        }
        std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    }
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
                let mut entry_type = None::<String>;
                let mut key = None::<SecretKey>;
                while let Some(name) = map.next_key::<String>()? {
                    if !seen.insert(name.clone()) {
                        return Err(serde::de::Error::custom("duplicate provider entry field"));
                    }
                    match name.as_str() {
                        "type" => entry_type = Some(map.next_value::<String>()?),
                        "key" => key = Some(SecretKey(map.next_value::<String>()?.into_bytes())),
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

fn parse_selected_auth(
    bytes: &[u8],
    provider_id: &str,
) -> std::result::Result<SelectedAuthFile, serde_json::Error> {
    struct AuthVisitor<'a> {
        provider_id: &'a str,
    }

    impl<'de, 'a> Visitor<'de> for AuthVisitor<'a> {
        type Value = SelectedAuthFile;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an auth.json provider map with one selected provider entry")
        }

        fn visit_map<M>(self, mut map: M) -> std::result::Result<Self::Value, M::Error>
        where
            M: MapAccess<'de>,
        {
            let mut selected = None::<SelectedProviderEntry>;
            while let Some(provider) = map.next_key::<String>()? {
                if provider == self.provider_id {
                    if selected.is_some() {
                        return Err(serde::de::Error::custom(
                            "duplicate selected provider entry",
                        ));
                    }
                    selected = Some(map.next_value::<SelectedProviderEntry>()?);
                } else {
                    map.next_value::<IgnoredAny>()?;
                }
            }
            selected
                .map(SelectedAuthFile)
                .ok_or_else(|| serde::de::Error::custom("selected provider entry is missing"))
        }
    }

    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    let selected = deserializer.deserialize_map(AuthVisitor { provider_id })?;
    deserializer.end()?;
    Ok(selected)
}

fn read_selected_key(path: &Path, provider_id: &str) -> Result<SecretKey> {
    if !valid_provider_id(provider_id) {
        return Err(auth_error("selected provider ID is malformed"));
    }
    validate_auth_source(path)?;
    let metadata = fs::symlink_metadata(path)
        .map_err(|_| auth_error("provider authorization source is unavailable"))?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.len() == 0
        || metadata.len() > MAX_AUTH_FILE_BYTES
    {
        return Err(auth_error(
            "provider authorization source is not a bounded plain file",
        ));
    }
    let canonical = fs::canonicalize(path)
        .map_err(|_| auth_error("provider authorization source cannot be resolved"))?;
    if canonical != path {
        return Err(auth_error(
            "provider authorization source path was redirected",
        ));
    }
    let mut file = File::open(path)
        .map_err(|_| auth_error("provider authorization source cannot be opened"))?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_AUTH_FILE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 != metadata.len() || bytes.len() as u64 > MAX_AUTH_FILE_BYTES {
        return Err(auth_error(
            "provider authorization source changed while reading",
        ));
    }
    let parsed = parse_selected_auth(&bytes, provider_id);
    for byte in &mut bytes {
        // SAFETY: clear the temporary source buffer before it is dropped.
        unsafe { std::ptr::write_volatile(byte, 0) };
    }
    std::sync::atomic::compiler_fence(std::sync::atomic::Ordering::SeqCst);
    parsed
        .map_err(|_| auth_error("provider authorization entry is invalid or ambiguous"))
        .map(|file| file.0.0)
}

fn validate_integration(
    value: &Value,
    directory: &str,
    provider_id: &str,
    require_credential: bool,
) -> Result<String> {
    if !valid_provider_id(provider_id) {
        return Err(auth_error("selected provider ID is malformed"));
    }
    if value["location"]["directory"] != directory {
        return Err(Error::new(
            "NATIVE_LOCATION_MISMATCH",
            "provider integration readback names another workspace",
        ));
    }
    let integrations = value["data"]
        .as_array()
        .ok_or_else(|| auth_error("provider integration list is malformed"))?;
    let mut selected = None::<&Value>;
    for integration in integrations {
        let id = integration["id"]
            .as_str()
            .filter(|value| safe_metadata_text(value))
            .ok_or_else(|| auth_error("provider integration identity is malformed"))?;
        if id == provider_id {
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
    if methods
        .iter()
        .filter(|method| method["type"] == "key")
        .count()
        != 1
    {
        return Err(auth_error(
            "provider does not expose one unambiguous API-key method",
        ));
    }
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
    if (!require_credential && !credential_ids.is_empty())
        || (require_credential && credential_ids.len() != 1)
    {
        return Err(auth_error(if require_credential {
            "provider credential metadata readback is absent or ambiguous"
        } else {
            "provider already has a credential connection before bootstrap"
        }));
    }
    Ok(credential_ids.into_iter().next().unwrap_or_default())
}

fn validate_auth_source(path: &Path) -> Result<()> {
    if !absolute_auth_path(path) {
        return Err(auth_error(
            "provider authorization source path is malformed",
        ));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            Component::RootDir => current.push(component.as_os_str()),
            Component::Normal(part) => {
                current.push(part);
                let metadata = fs::symlink_metadata(&current)
                    .map_err(|_| auth_error("provider authorization source path is unavailable"))?;
                if metadata.file_type().is_symlink() || (current != path && !metadata.is_dir()) {
                    return Err(auth_error(
                        "provider authorization source contains a redirected ancestor",
                    ));
                }
                if fs::canonicalize(&current)
                    .map_err(|_| auth_error("provider authorization source path was redirected"))?
                    != current
                {
                    return Err(auth_error(
                        "provider authorization source path was redirected",
                    ));
                }
            }
            Component::CurDir | Component::ParentDir => {
                return Err(auth_error(
                    "provider authorization source is not normalized",
                ));
            }
        }
    }
    Ok(())
}

fn absolute_auth_path(path: &Path) -> bool {
    path.is_absolute()
        && path.file_name().and_then(|name| name.to_str()) == Some("auth.json")
        && !path
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
}

fn canonical_directory_text(path: &Path) -> Result<String> {
    let canonical = fs::canonicalize(path).map_err(|_| {
        Error::new(
            "NATIVE_LOCATION_UNAVAILABLE",
            "selected workspace cannot be resolved",
        )
    })?;
    canonical
        .to_str()
        .filter(|value| !value.is_empty() && value.len() <= 4096)
        .map(str::to_owned)
        .ok_or_else(|| {
            Error::new(
                "NATIVE_LOCATION_UNAVAILABLE",
                "selected workspace is not a bounded path",
            )
        })
}

fn model_digest(model: &ModelRef) -> Result<String> {
    let bytes = serde_json::to_vec(model)?;
    Ok(hex_digest(&Sha256::digest(&bytes)))
}

fn sha256_json(value: &Value) -> String {
    let bytes = serde_json::to_vec(value).unwrap_or_default();
    hex_digest(&Sha256::digest(&bytes))
}

fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn safe_metadata_text(value: &str) -> bool {
    !value.trim().is_empty()
        && value.len() <= MAX_SAFE_ID_BYTES
        && !value.chars().any(char::is_control)
}

fn valid_ref(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn valid_provider_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SAFE_ID_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn auth_error(message: &str) -> Error {
    Error::new("OWNED_PROVIDER_AUTH", message)
}
