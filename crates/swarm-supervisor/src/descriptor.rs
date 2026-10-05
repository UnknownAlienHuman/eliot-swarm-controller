use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use swarm_contracts::error::{Error, Result};
use swarm_contracts::module_contract::ModuleContractClaim;

const RESOLVER_MAP_LIMIT: usize = 65_536;
const RESOLVER_MAP_VERSION: u64 = 2;

pub use swarm_contracts::module_catalog::{
    ActivationPolicy, ArtifactIdentity, ArtifactSelector, CapabilityId, EnvironmentVariable,
    LaunchSpec, LaunchValue, LifecycleOwnership, ModuleCatalog, ModuleDescriptor, ModuleId,
    ProtectedRef, ProtocolRange, ProtocolVersion, RestartPolicy, SchemaDescriptor, Sha256Digest,
};

pub type DescriptorCatalog = ModuleCatalog;

const INSTALL_RECEIPT_FORMAT: &str = "eliot.module_install_receipt.v1";
const INSTALL_RECORD_LIMIT: u64 = 65_536;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct InstallReceipt {
    schema_version: u16,
    format: String,
    module_id: String,
    artifact_id: String,
    version: String,
    build_id: Option<String>,
    source_file: PathBuf,
    installed_file: PathBuf,
    source_sha256: String,
    staged_sha256: String,
    installed_sha256: String,
    descriptor_file: PathBuf,
    descriptor_sha256: String,
}

/// Load one exact installed descriptor under an explicitly configured root.
/// The unsigned receipt is consistency evidence only; executable bytes are
/// independently hashed before Store registration by `register_descriptor`.
pub fn load_installed_descriptor(
    descriptor_path: &Path,
    install_root: &Path,
) -> Result<ModuleDescriptor> {
    if !install_root.is_absolute() || !descriptor_path.is_absolute() {
        return Err(Error::invalid("module installation paths must be absolute"));
    }
    reject_link_ancestors(install_root)?;
    let root = fs::canonicalize(install_root)?;
    let descriptor_path = checked_file_under_root(&root, descriptor_path, "descriptor")?;
    if fs::metadata(&descriptor_path)?.len() > INSTALL_RECORD_LIMIT {
        return Err(Error::new(
            "MODULE_DESCRIPTOR_LIMIT",
            "installed module descriptor exceeds its size bound",
        ));
    }
    let receipt_path = descriptor_path
        .parent()
        .ok_or_else(|| Error::invalid("installed descriptor has no parent directory"))?
        .join("install-receipt.json");
    let receipt_path = checked_file_under_root(&root, &receipt_path, "install receipt")?;
    if fs::metadata(&receipt_path)?.len() > INSTALL_RECORD_LIMIT {
        return Err(Error::new(
            "MODULE_INSTALL_RECEIPT_LIMIT",
            "module install receipt exceeds its size bound",
        ));
    }
    let receipt: InstallReceipt = read_limited_json(&receipt_path)?;
    if receipt.schema_version != 1 || receipt.format != INSTALL_RECEIPT_FORMAT {
        return Err(Error::new(
            "MODULE_INSTALL_RECEIPT_INVALID",
            "unsupported module install receipt version or format",
        ));
    }
    let descriptor: ModuleDescriptor = read_limited_json(&descriptor_path)?;
    descriptor
        .validate()
        .map_err(|error| Error::new("MODULE_DESCRIPTOR_INVALID", error.to_string()))?;
    let receipt_descriptor =
        checked_file_under_root(&root, &receipt.descriptor_file, "receipt descriptor")?;
    let installed_file =
        checked_file_under_root(&root, &receipt.installed_file, "installed executable")?;
    if receipt_descriptor != descriptor_path
        || checked_file_under_root(
            &root,
            &descriptor.launch.executable,
            "descriptor executable",
        )? != installed_file
        || descriptor.module_id.as_str() != receipt.module_id
        || descriptor.artifact.artifact_id.as_str() != receipt.artifact_id
        || descriptor.artifact.version.as_str() != receipt.version
        || descriptor.artifact.build_id != receipt.build_id
        || descriptor
            .launch
            .executable_sha256
            .as_ref()
            .map(Sha256Digest::as_str)
            != Some(receipt.installed_sha256.as_str())
        || receipt.source_sha256 != receipt.staged_sha256
        || receipt.source_sha256 != receipt.installed_sha256
        || hash_file_sha256(&descriptor_path)? != receipt.descriptor_sha256
        || hash_file_sha256(&installed_file)? != receipt.installed_sha256
    {
        return Err(Error::new(
            "MODULE_INSTALL_RECEIPT_MISMATCH",
            "descriptor, install receipt, executable path, or installed bytes do not match exactly",
        ));
    }
    let source_file = checked_existing_file(&receipt.source_file, "source executable")?;
    if hash_file_sha256(&source_file)? != receipt.source_sha256 {
        return Err(Error::new(
            "MODULE_INSTALL_RECEIPT_MISMATCH",
            "source executable bytes do not match the install receipt",
        ));
    }
    for digest in [
        &receipt.source_sha256,
        &receipt.staged_sha256,
        &receipt.installed_sha256,
        &receipt.descriptor_sha256,
    ] {
        if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(Error::new(
                "MODULE_INSTALL_RECEIPT_INVALID",
                "install receipt contains a malformed SHA-256 digest",
            ));
        }
    }
    Ok(descriptor)
}

fn read_limited_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(INSTALL_RECORD_LIMIT + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > INSTALL_RECORD_LIMIT {
        return Err(Error::invalid(
            "module installation JSON exceeds its size bound",
        ));
    }
    serde_json::from_slice(&bytes).map_err(Into::into)
}

fn checked_file_under_root(root: &Path, path: &Path, field: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(Error::new(
            "MODULE_INSTALL_PATH_INVALID",
            format!("{field} path must be absolute"),
        ));
    }
    let relative = path.strip_prefix(root).map_err(|_| {
        Error::new(
            "MODULE_INSTALL_PATH_INVALID",
            format!("{field} path escapes the configured install root"),
        )
    })?;
    if relative.as_os_str().is_empty() {
        return Err(Error::new(
            "MODULE_INSTALL_PATH_INVALID",
            format!("{field} path names the install root itself"),
        ));
    }
    let mut current = root.to_path_buf();
    for component in relative.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err(Error::new(
                "MODULE_INSTALL_PATH_INVALID",
                format!("{field} path contains a non-normal component"),
            ));
        }
        current.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&current)?;
        if is_link_or_reparse(&metadata) {
            return Err(Error::new(
                "MODULE_INSTALL_PATH_INVALID",
                format!("{field} path traverses a symlink or reparse point"),
            ));
        }
    }
    let canonical = fs::canonicalize(path)?;
    if !canonical.starts_with(root) || !fs::metadata(&current)?.is_file() {
        return Err(Error::new(
            "MODULE_INSTALL_PATH_INVALID",
            format!("{field} path is not the exact regular file under the install root"),
        ));
    }
    Ok(canonical)
}

fn reject_link_ancestors(path: &Path) -> Result<()> {
    let mut chain = path.ancestors().collect::<Vec<_>>();
    chain.reverse();
    for component in chain
        .into_iter()
        .filter(|item| !item.as_os_str().is_empty())
    {
        let metadata = fs::symlink_metadata(component)?;
        if is_link_or_reparse(&metadata) {
            return Err(Error::new(
                "MODULE_INSTALL_PATH_INVALID",
                "configured install root traverses a symlink or reparse point",
            ));
        }
    }
    Ok(())
}

fn checked_existing_file(path: &Path, field: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(Error::new(
            "MODULE_INSTALL_PATH_INVALID",
            format!("{field} path must be absolute"),
        ));
    }
    let metadata = fs::symlink_metadata(path)?;
    if is_link_or_reparse(&metadata) || !metadata.is_file() {
        return Err(Error::new(
            "MODULE_INSTALL_PATH_INVALID",
            format!("{field} path must be a regular file without a link leaf"),
        ));
    }
    fs::canonicalize(path).map_err(Into::into)
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

/// A host-validated binding-specific configuration value set. Config schema
/// validation remains with the host admission path; this type preserves the
/// literal/protected-reference split through process construction.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindingLaunchConfig {
    #[serde(default)]
    pub values: BTreeMap<String, LaunchValue>,
}

impl BindingLaunchConfig {
    pub fn validate(&self) -> Result<()> {
        for (key, value) in &self.values {
            validate_environment_name(key)?;
            validate_launch_value(value)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceScope {
    pub binding_id: String,
    pub generation: u64,
}

impl ServiceScope {
    pub fn validate(&self) -> Result<()> {
        validate_identifier(&self.binding_id, "binding_id")?;
        if self.generation == 0 {
            return Err(Error::invalid("binding generation must be positive"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ProtectedResolverContext {
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub scope: ServiceScope,
    pub protocol: ProtocolVersion,
}

/// Trusted host-side selector for a private resolver-map file. The selected
/// map itself binds module, artifact, binding generation and protocol, and the
/// helper validates that exact context before resolving any reference. This
/// interface returns a path only; raw protected values never enter this crate.
pub trait ProtectedResolver: Send + Sync + 'static {
    fn resolver_map_path(&self, context: &ProtectedResolverContext) -> Result<PathBuf>;
}

/// Resolve an exact protected-reference map beneath a path supplied by the
/// installer or local host configuration. It does not create a map, infer a
/// default secret store, or interpret any opaque reference as a path.
#[derive(Debug, Clone)]
pub struct ResolverMapDirectory {
    root: PathBuf,
}

impl ResolverMapDirectory {
    pub fn new(root: PathBuf) -> Result<Self> {
        if !root.is_absolute() {
            return Err(Error::invalid(
                "protected resolver map root must be an absolute configured path",
            ));
        }
        let metadata = fs::symlink_metadata(&root)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(Error::new(
                "MODULE_RESOLVER_ROOT_INVALID",
                "protected resolver map root must be a directory without a symlink leaf",
            ));
        }
        let root = fs::canonicalize(root)?;
        Ok(Self { root })
    }
}

impl ProtectedResolver for ResolverMapDirectory {
    fn resolver_map_path(&self, context: &ProtectedResolverContext) -> Result<PathBuf> {
        let artifact = serde_json::to_vec(&context.artifact)?;
        Ok(self
            .root
            .join(hash_component(context.module_id.as_str().as_bytes()))
            .join(hash_component(&artifact))
            .join(hash_component(context.scope.binding_id.as_bytes()))
            .join(context.scope.generation.to_string())
            .join("resolver-v2.json"))
    }
}

/// All evidence needed to publish a binding-scoped resolver map. Keeping the
/// credential file and digest together prevents callers from omitting the
/// immediate file-integrity check while keeping this API below seven inputs.
pub struct BindingMapPublication<'a> {
    pub context: &'a ProtectedResolverContext,
    pub descriptor: &'a ModuleDescriptor,
    pub claim: &'a ModuleContractClaim,
    pub credential_ref: &'a ProtectedRef,
    pub credential_file: &'a Path,
    pub credential_file_sha256: &'a str,
    pub additional_files: &'a BTreeMap<ProtectedRef, PathBuf>,
}

impl ResolverMapDirectory {
    /// Publish the exact per-binding protected-reference map consumed by the
    /// shared `swarm-module-owner` bootstrap. The map contains file paths only;
    /// credential bytes stay in the Store-provisioned private file.
    pub fn publish_binding_map(&self, publication: BindingMapPublication<'_>) -> Result<PathBuf> {
        let BindingMapPublication {
            context,
            descriptor,
            claim,
            credential_ref,
            credential_file,
            credential_file_sha256,
            additional_files,
        } = publication;
        descriptor
            .validate()
            .map_err(|error| Error::new("MODULE_DESCRIPTOR_INVALID", error.to_string()))?;
        if descriptor.module_id != context.module_id
            || descriptor.artifact != context.artifact
            || !descriptor.protocol.contains(context.protocol)
            || descriptor.launch.credential_ref.as_ref() != Some(credential_ref)
            || crate::module_contract_claim(descriptor, ProtocolRange::exact(context.protocol))?
                != *claim
        {
            return Err(Error::new(
                "MODULE_RESOLVER_CONTEXT_MISMATCH",
                "credential resolver context differs from the exact selected module descriptor",
            ));
        }
        let mut required = BTreeSet::<ProtectedRef>::new();
        required.insert(credential_ref.clone());
        for environment in &descriptor.launch.environment {
            if let LaunchValue::Protected(reference) = &environment.value {
                required.insert(reference.clone());
            }
        }
        if descriptor
            .launch
            .argv
            .iter()
            .any(|argument| matches!(argument, LaunchValue::Protected(_)))
        {
            return Err(Error::new(
                "MODULE_PROTECTED_ARGV_UNSUPPORTED",
                "protected argv references require an indexed resolver that the module-owner plan does not expose",
            ));
        }

        let credential_file = validate_existing_private_file(credential_file)?;
        let actual_credential_sha256 = hash_file_sha256(&credential_file)?;
        if !actual_credential_sha256.eq_ignore_ascii_case(credential_file_sha256) {
            return Err(Error::new(
                "MODULE_CREDENTIAL_FILE_CHANGED",
                "provisioned binding credential file differs from its Store receipt",
            ));
        }
        if credential_file_sha256.len() != 64
            || !credential_file_sha256
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::new(
                "MODULE_CREDENTIAL_FILE_CHANGED",
                "Store credential digest is not a SHA-256 hex value",
            ));
        }
        let mut refs = Vec::with_capacity(required.len());
        for reference in required {
            let (path, sha256) = if &reference == credential_ref {
                (credential_file.clone(), credential_file_sha256.to_owned())
            } else {
                let configured = additional_files.get(&reference).ok_or_else(|| {
                    Error::new(
                        "MODULE_RESOLVER_REF_UNKNOWN",
                        "an explicit protected launch reference has no configured file mapping",
                    )
                })?;
                let path = validate_existing_private_file(configured)?;
                let sha256 = hash_file_sha256(&path)?;
                (path, sha256)
            };
            refs.push(serde_json::json!({
                "reference": reference.as_str(),
                "credential_file": path,
                "sha256": sha256,
            }));
        }
        let resolver_map = self.resolver_map_path(context)?;
        let protocol = format!("{}.{}", context.protocol.major, context.protocol.minor);
        let build_id = context.artifact.build_id.clone();
        let map = serde_json::json!({
            "version": RESOLVER_MAP_VERSION,
            "module": context.module_id.as_str(),
            "binding": context.scope.binding_id,
            "generation": context.scope.generation.to_string(),
            "artifact_id": context.artifact.artifact_id.as_str(),
            "artifact_version": context.artifact.version.as_str(),
            "build_id": build_id,
            "protocol": protocol,
            "module_contract": serde_json::to_string(claim)?,
            "refs": refs,
        });
        let bytes = serde_json::to_vec(&map)?;
        if bytes.len() > RESOLVER_MAP_LIMIT {
            return Err(Error::invalid("module resolver map exceeds its size bound"));
        }
        if resolver_map.exists() {
            let metadata = fs::symlink_metadata(&resolver_map)?;
            if metadata.file_type().is_symlink()
                || !metadata.is_file()
                || metadata.len() > RESOLVER_MAP_LIMIT as u64
            {
                return Err(Error::new(
                    "MODULE_RESOLVER_MAP_INVALID",
                    "existing resolver map is not a bounded regular file",
                ));
            }
            let mut existing = Vec::new();
            File::open(&resolver_map)?
                .take(RESOLVER_MAP_LIMIT as u64 + 1)
                .read_to_end(&mut existing)?;
            if existing != bytes {
                return Err(Error::new(
                    "MODULE_RESOLVER_MAP_CONFLICT",
                    "an existing binding resolver map differs from the retained credential and descriptor",
                ));
            }
            return Ok(resolver_map);
        }

        let parent = resolver_map
            .parent()
            .ok_or_else(|| Error::invalid("resolver map path has no parent directory"))?;
        fs::create_dir_all(parent)?;
        if !fs::canonicalize(parent)?.starts_with(&self.root) {
            return Err(Error::new(
                "MODULE_RESOLVER_MAP_INVALID",
                "resolver map directory escaped its explicitly configured root",
            ));
        }
        swarm_process::private_permissions(parent, true)?;
        let mut file = match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&resolver_map)
        {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let metadata = fs::symlink_metadata(&resolver_map)?;
                if metadata.file_type().is_symlink()
                    || !metadata.is_file()
                    || metadata.len() > RESOLVER_MAP_LIMIT as u64
                {
                    return Err(Error::new(
                        "MODULE_RESOLVER_MAP_INVALID",
                        "concurrent resolver map is not a bounded regular file",
                    ));
                }
                let mut existing = Vec::new();
                File::open(&resolver_map)?
                    .take(RESOLVER_MAP_LIMIT as u64 + 1)
                    .read_to_end(&mut existing)?;
                if existing == bytes {
                    return Ok(resolver_map);
                }
                return Err(Error::new(
                    "MODULE_RESOLVER_MAP_CONFLICT",
                    "concurrent binding resolver map differs from the retained credential and descriptor",
                ));
            }
            Err(error) => return Err(error.into()),
        };
        swarm_process::private_permissions(&resolver_map, false)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        Ok(resolver_map)
    }
}

fn validate_existing_private_file(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(Error::invalid(
            "protected credential file must be an absolute configured path",
        ));
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 65_536 {
        return Err(Error::new(
            "MODULE_CREDENTIAL_FILE_INVALID",
            "protected credential file must be a bounded regular file without a symlink leaf",
        ));
    }
    fs::canonicalize(path).map_err(Into::into)
}

fn hash_file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut file = File::open(path)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 16 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn hash_component(value: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModuleOwnerExecutable {
    pub path: PathBuf,
    pub sha256: Sha256Digest,
}

impl ModuleOwnerExecutable {
    pub fn validate(&self) -> Result<()> {
        if !self.path.is_absolute() || self.path.as_os_str().is_empty() {
            return Err(Error::invalid(
                "module-owner helper executable must be an absolute path",
            ));
        }
        Ok(())
    }
}

pub(crate) fn validate_identifier(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || matches!(value, "." | "..")
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Error::invalid(format!("invalid {field}")));
    }
    Ok(())
}

pub(crate) fn validate_environment_name(name: &str) -> Result<()> {
    let mut bytes = name.bytes();
    let Some(first) = bytes.next() else {
        return Err(Error::invalid("environment variable name is empty"));
    };
    if name.len() > 128
        || !(first.is_ascii_alphabetic() || first == b'_')
        || !bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
        || name.starts_with("ELIOT_SWARM_")
    {
        return Err(Error::invalid(
            "environment variable is invalid or uses a reserved ELIOT_SWARM_ name",
        ));
    }
    Ok(())
}

pub(crate) fn validate_launch_value(value: &LaunchValue) -> Result<()> {
    match value {
        LaunchValue::Literal(value) if value.contains('\0') => {
            Err(Error::invalid("launch value contains NUL"))
        }
        LaunchValue::Literal(_) => Ok(()),
        LaunchValue::Protected(reference)
            if reference.as_str().is_empty()
                || reference.as_str().len() > 512
                || reference.as_str().chars().any(char::is_control) =>
        {
            Err(Error::invalid("protected launch reference is invalid"))
        }
        LaunchValue::Protected(_) => Ok(()),
    }
}
