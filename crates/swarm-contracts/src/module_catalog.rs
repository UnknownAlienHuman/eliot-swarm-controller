//! Pure metadata contracts for installed, independently built modules.
//!
//! This module intentionally does not read files, inspect executables, launch
//! processes, resolve protected values, or own activation state. A caller may
//! build a ModuleCatalog from retained descriptors and select an exact
//! artifact without probing or starting any module.

use std::{collections::BTreeSet, fmt, path::PathBuf};

use serde::{Deserialize, Serialize};

const MAX_ID_BYTES: usize = 128;
const MAX_VERSION_BYTES: usize = 128;
const MAX_PROTECTED_REF_BYTES: usize = 512;
const MAX_LAUNCH_ITEM_BYTES: usize = 16 * 1024;
const MAX_LAUNCH_ARGUMENTS: usize = 256;
const MAX_ENVIRONMENT_ITEMS: usize = 256;
const MAX_LAUNCH_BYTES: usize = 64 * 1024;
const MAX_RESTART_STARTS: u16 = 64;
const MAX_RESTART_WINDOW_MS: u64 = 30 * 60 * 1000;
const MAX_RESTART_BACKOFF_MS: u64 = 5 * 60 * 1000;

fn invalid(field: &'static str) -> CatalogError {
    CatalogError::InvalidDescriptor { field }
}

fn valid_atom(value: &str, extra: &[u8]) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= MAX_ID_BYTES
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || b"._:-".contains(&byte) || extra.contains(&byte)
        })
}

fn valid_version(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && value.len() <= MAX_VERSION_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".+_-".contains(&byte))
}

macro_rules! string_id {
    ($name:ident, $field:literal, $validator:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self, CatalogError> {
                let value = value.into();
                if !$validator(&value) {
                    return Err(invalid($field));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }

            fn validate(&self) -> Result<(), CatalogError> {
                if !$validator(&self.0) {
                    return Err(invalid($field));
                }
                Ok(())
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }
    };
}

fn valid_module_id(value: &str) -> bool {
    valid_atom(value, b"")
}

fn valid_artifact_id(value: &str) -> bool {
    valid_atom(value, b"")
}

fn valid_capability_id(value: &str) -> bool {
    valid_atom(value, b"/@")
}

string_id!(ModuleId, "module_id", valid_module_id);
string_id!(ArtifactId, "artifact.artifact_id", valid_artifact_id);
string_id!(CapabilityId, "capability_id", valid_capability_id);

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ArtifactVersion(String);

impl ArtifactVersion {
    pub fn new(value: impl Into<String>) -> Result<Self, CatalogError> {
        let value = value.into();
        if !valid_version(&value) {
            return Err(invalid("artifact.version"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), CatalogError> {
        if !valid_version(&self.0) {
            return Err(invalid("artifact.version"));
        }
        Ok(())
    }
}

impl fmt::Display for ArtifactVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Sha256Digest(String);

impl Sha256Digest {
    pub fn new(value: impl Into<String>) -> Result<Self, CatalogError> {
        let value = value.into();
        if !is_sha256_hex(&value) {
            return Err(invalid("launch.executable_sha256"));
        }
        Ok(Self(value.to_ascii_lowercase()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self, field: &'static str) -> Result<(), CatalogError> {
        if !is_sha256(&self.0) {
            return Err(invalid(field));
        }
        Ok(())
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// A local reference to protected configuration. The descriptor carries the
/// reference only; resolution is deferred to the trusted supervisor boundary.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProtectedRef(String);

impl ProtectedRef {
    pub fn new(value: impl Into<String>) -> Result<Self, CatalogError> {
        let value = value.into();
        if value.is_empty()
            || value.len() > MAX_PROTECTED_REF_BYTES
            || value.chars().any(char::is_control)
        {
            return Err(invalid("protected_ref"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn validate(&self) -> Result<(), CatalogError> {
        if self.0.is_empty()
            || self.0.len() > MAX_PROTECTED_REF_BYTES
            || self.0.chars().any(char::is_control)
        {
            return Err(invalid("protected_ref"));
        }
        Ok(())
    }
}

impl fmt::Debug for ProtectedRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProtectedRef([redacted])")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactIdentity {
    pub artifact_id: ArtifactId,
    pub version: ArtifactVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_id: Option<String>,
}

impl ArtifactIdentity {
    fn validate(&self) -> Result<(), CatalogError> {
        self.artifact_id.validate()?;
        self.version.validate()?;
        if self
            .build_id
            .as_ref()
            .is_some_and(|build_id| !valid_atom(build_id, b"+"))
        {
            return Err(invalid("artifact.build_id"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolVersion {
    pub major: u16,
    pub minor: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProtocolRange {
    pub minimum: ProtocolVersion,
    pub maximum: ProtocolVersion,
}

impl ProtocolRange {
    pub fn exact(version: ProtocolVersion) -> Self {
        Self {
            minimum: version,
            maximum: version,
        }
    }

    pub fn contains(self, version: ProtocolVersion) -> bool {
        self.minimum.major == version.major
            && self.maximum.major == version.major
            && self.minimum.minor <= version.minor
            && version.minor <= self.maximum.minor
    }

    pub fn intersects(self, other: Self) -> bool {
        self.minimum.major == self.maximum.major
            && other.minimum.major == other.maximum.major
            && self.minimum.major == other.minimum.major
            && self.minimum.minor <= other.maximum.minor
            && other.minimum.minor <= self.maximum.minor
    }

    fn validate(self, field: &'static str) -> Result<(), CatalogError> {
        if self.minimum.major != self.maximum.major || self.minimum.minor > self.maximum.minor {
            return Err(invalid(field));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDescriptor {
    pub schema_id: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<Sha256Digest>,
}

impl SchemaDescriptor {
    fn validate(&self, field: &'static str) -> Result<(), CatalogError> {
        if !valid_atom(&self.schema_id, b"/@") || !valid_version(&self.version) {
            return Err(invalid(field));
        }
        if let Some(digest) = &self.sha256 {
            digest.validate(field)?;
        }
        Ok(())
    }
}

/// A command-line argument or environment value is literal, protected, or a
/// typed request for host-materialized launch config. The descriptor never
/// contains resolved secret bytes or operator-selected host IPC paths.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum LaunchValue {
    Literal(String),
    Protected(ProtectedRef),
    /// Resolve to the private, scope-specific host connection config path
    /// that the supervisor materializes immediately before an owned launch.
    ModuleHostConfigPath {
        schema_version: u16,
    },
}

impl LaunchValue {
    fn validate(&self) -> Result<usize, CatalogError> {
        match self {
            Self::Literal(value) => {
                if value.len() > MAX_LAUNCH_ITEM_BYTES || value.contains('\0') {
                    return Err(invalid("launch.argv_or_environment"));
                }
                Ok(value.len())
            }
            Self::Protected(reference) => {
                reference.validate()?;
                Ok(reference.as_str().len())
            }
            // Reserve the complete owner-plan argument bound for the path
            // that the host substitutes at launch time.
            Self::ModuleHostConfigPath { schema_version: 1 } => Ok(4_096),
            Self::ModuleHostConfigPath { .. } => Err(invalid("launch.module_host_config_path")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentVariable {
    pub name: String,
    pub value: LaunchValue,
}

impl EnvironmentVariable {
    fn validate(&self) -> Result<usize, CatalogError> {
        if !valid_environment_name(&self.name) {
            return Err(invalid("launch.environment.name"));
        }
        self.value.validate()
    }
}

fn valid_environment_name(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LaunchSpec {
    pub executable: PathBuf,
    #[serde(default)]
    pub argv: Vec<LaunchValue>,
    #[serde(default)]
    pub environment: Vec<EnvironmentVariable>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<ProtectedRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub working_directory: Option<PathBuf>,
    #[serde(default)]
    pub inherited_environment_allowlist: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_sha256: Option<Sha256Digest>,
}

impl LaunchSpec {
    fn validate(&self) -> Result<(), CatalogError> {
        if !self.executable.is_absolute()
            || self.executable.as_os_str().is_empty()
            || self.executable.to_string_lossy().len() > 4096
        {
            return Err(invalid("launch.executable"));
        }
        if let Some(directory) = &self.working_directory
            && (!directory.is_absolute()
                || directory.as_os_str().is_empty()
                || directory.to_string_lossy().len() > 4096)
        {
            return Err(invalid("launch.working_directory"));
        }
        if self.argv.len() > MAX_LAUNCH_ARGUMENTS {
            return Err(invalid("launch.argv"));
        }
        if self.environment.len() > MAX_ENVIRONMENT_ITEMS
            || self.inherited_environment_allowlist.len() > MAX_ENVIRONMENT_ITEMS
        {
            return Err(invalid("launch.environment"));
        }

        let mut total_bytes = 0usize;
        if self
            .argv
            .iter()
            .filter(|value| matches!(value, LaunchValue::ModuleHostConfigPath { .. }))
            .count()
            > 1
        {
            return Err(invalid("launch.argv.module_host_config_path"));
        }
        for argument in &self.argv {
            total_bytes = total_bytes
                .checked_add(argument.validate()?)
                .ok_or_else(|| invalid("launch.argv"))?;
        }

        let mut environment_names = BTreeSet::new();
        for variable in &self.environment {
            if matches!(&variable.value, LaunchValue::ModuleHostConfigPath { .. }) {
                return Err(invalid("launch.environment.module_host_config_path"));
            }
            let normalized_name = variable.name.to_ascii_uppercase();
            if !environment_names.insert(normalized_name) {
                return Err(invalid("launch.environment.duplicate_name"));
            }
            let value_size = variable.validate()?;
            total_bytes = total_bytes
                .checked_add(variable.name.len())
                .and_then(|size| size.checked_add(value_size))
                .ok_or_else(|| invalid("launch.environment"))?;
        }

        for name in &self.inherited_environment_allowlist {
            if !valid_environment_name(name) {
                return Err(invalid("launch.inherited_environment_allowlist"));
            }
            if !environment_names.insert(name.to_ascii_uppercase()) {
                return Err(invalid("launch.environment.duplicate_name"));
            }
            total_bytes = total_bytes
                .checked_add(name.len())
                .ok_or_else(|| invalid("launch.inherited_environment_allowlist"))?;
        }

        if total_bytes > MAX_LAUNCH_BYTES {
            return Err(invalid("launch.argv_or_environment"));
        }
        if let Some(reference) = &self.credential_ref {
            reference.validate()?;
        }
        if let Some(digest) = &self.executable_sha256 {
            digest.validate("launch.executable_sha256")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleOwnership {
    ExternalAttach,
    OwnedService,
    OneShot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActivationPolicy {
    OnDemand,
    Continuous,
}

/// A trusted descriptor's exact location and meaning for the workspace path
/// admitted by the host. `native_options_pointer` uses RFC 6901 JSON Pointer
/// syntax and resolves only through existing JSON objects to an existing
/// string value; it cannot create or replace unrelated native options.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceOptionContract {
    pub schema_version: u16,
    pub native_options_pointer: String,
    pub semantics: WorkspaceOptionSemantics,
}

/// Workspace path semantics supported by host admission version 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceOptionSemantics {
    ReplaceWithAdmittedAbsoluteWorkspace,
}

impl WorkspaceOptionContract {
    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.schema_version != 1
            || self.semantics != WorkspaceOptionSemantics::ReplaceWithAdmittedAbsoluteWorkspace
        {
            return Err(invalid("workspace_option"));
        }
        self.native_options_segments().map(|_| ())
    }

    /// Decode the bounded JSON Pointer after descriptor validation. Host
    /// mutation still requires every segment to name an existing object key.
    pub fn native_options_segments(&self) -> Result<Vec<String>, CatalogError> {
        const MAX_POINTER_BYTES: usize = 1024;
        const MAX_POINTER_SEGMENTS: usize = 8;
        const MAX_SEGMENT_BYTES: usize = 128;

        let pointer = self.native_options_pointer.as_str();
        if pointer.is_empty() || pointer.len() > MAX_POINTER_BYTES || !pointer.starts_with('/') {
            return Err(invalid("workspace_option.native_options_pointer"));
        }
        let encoded_segments = pointer[1..].split('/').collect::<Vec<_>>();
        if encoded_segments.is_empty() || encoded_segments.len() > MAX_POINTER_SEGMENTS {
            return Err(invalid("workspace_option.native_options_pointer"));
        }
        encoded_segments
            .into_iter()
            .map(|segment| {
                if segment.is_empty() || segment.len() > MAX_SEGMENT_BYTES {
                    return Err(invalid("workspace_option.native_options_pointer"));
                }
                let mut decoded = String::with_capacity(segment.len());
                let mut chars = segment.chars();
                while let Some(ch) = chars.next() {
                    if ch == '~' {
                        match chars.next() {
                            Some('0') => decoded.push('~'),
                            Some('1') => decoded.push('/'),
                            _ => return Err(invalid("workspace_option.native_options_pointer")),
                        }
                    } else {
                        decoded.push(ch);
                    }
                }
                if decoded.is_empty() || decoded.chars().any(char::is_control) {
                    return Err(invalid("workspace_option.native_options_pointer"));
                }
                Ok(decoded)
            })
            .collect()
    }
}

/// Optional, descriptor-pinned contract for adapters that can prepare an
/// executor before the native session identity exists. It describes receipt
/// semantics only; it grants no native-effect authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreInputOpenContract {
    pub schema_version: u16,
    pub kind: PreInputOpenKind,
    pub completion_condition: PreInputOpenCompletionCondition,
    pub native_identity: PreInputNativeIdentity,
    pub initial_identity_adoption: PreInputIdentityAdoption,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreInputOpenKind {
    PreInputExecutorReady,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreInputOpenCompletionCondition {
    NativeExecutorPrepared,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreInputNativeIdentity {
    Rootless,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreInputIdentityAdoption {
    FirstTaskDispatchExactNativeEcho,
}

impl PreInputOpenContract {
    pub const fn first_task_dispatch_exact_native_echo() -> Self {
        Self {
            schema_version: 1,
            kind: PreInputOpenKind::PreInputExecutorReady,
            completion_condition: PreInputOpenCompletionCondition::NativeExecutorPrepared,
            native_identity: PreInputNativeIdentity::Rootless,
            initial_identity_adoption: PreInputIdentityAdoption::FirstTaskDispatchExactNativeEcho,
        }
    }

    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.schema_version != 1 || *self != Self::first_task_dispatch_exact_native_echo() {
            return Err(invalid("pre_input_open"));
        }
        Ok(())
    }
}

/// Bounded local restart settings. These values control only worker recovery;
/// restarting a worker never replays a module command or uncertain external
/// effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RestartPolicy {
    pub max_starts: u16,
    pub window_ms: u64,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub reset_after_healthy_ms: u64,
    pub jitter: bool,
}

impl Default for RestartPolicy {
    fn default() -> Self {
        Self {
            max_starts: 5,
            window_ms: 60_000,
            initial_backoff_ms: 250,
            max_backoff_ms: 30_000,
            reset_after_healthy_ms: 60_000,
            jitter: true,
        }
    }
}

impl RestartPolicy {
    fn validate(self) -> Result<(), CatalogError> {
        if self.max_starts == 0
            || self.max_starts > MAX_RESTART_STARTS
            || self.window_ms == 0
            || self.window_ms > MAX_RESTART_WINDOW_MS
            || self.initial_backoff_ms == 0
            || self.initial_backoff_ms > self.max_backoff_ms
            || self.max_backoff_ms > MAX_RESTART_BACKOFF_MS
            || self.reset_after_healthy_ms == 0
            || self.reset_after_healthy_ms > MAX_RESTART_WINDOW_MS
        {
            return Err(invalid("restart"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleDescriptor {
    #[serde(default = "descriptor_schema_version")]
    pub schema_version: u16,
    pub module_id: ModuleId,
    pub artifact: ArtifactIdentity,
    pub launch: LaunchSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_schema: Option<SchemaDescriptor>,
    /// Optional, versioned location for host-admitted workspace injection.
    /// Absence preserves compatibility for legacy descriptors; selected
    /// workspace-backed adapters must declare it before a new launch is queued.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_option: Option<WorkspaceOptionContract>,
    /// Exact rootless pre-input receipt and first-identity adoption contract.
    /// Absence preserves the existing open-with-native-identity behavior.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_input_open: Option<PreInputOpenContract>,
    #[serde(default)]
    pub command_schemas: BTreeSet<SchemaDescriptor>,
    #[serde(default)]
    pub event_schemas: BTreeSet<SchemaDescriptor>,
    pub protocol: ProtocolRange,
    #[serde(default)]
    pub capabilities: BTreeSet<CapabilityId>,
    pub lifecycle: LifecycleOwnership,
    pub activation: ActivationPolicy,
    pub enabled: bool,
    #[serde(default)]
    pub restart: RestartPolicy,
}

const fn descriptor_schema_version() -> u16 {
    1
}

impl ModuleDescriptor {
    pub fn validate(&self) -> Result<(), CatalogError> {
        if self.schema_version != 1 {
            return Err(CatalogError::UnsupportedDescriptorSchema {
                version: self.schema_version,
            });
        }
        self.module_id.validate()?;
        self.artifact.validate()?;
        self.launch.validate()?;
        self.protocol.validate("protocol")?;
        self.restart.validate()?;
        if let Some(schema) = &self.config_schema {
            schema.validate("config_schema")?;
        }
        if let Some(workspace_option) = &self.workspace_option {
            workspace_option.validate()?;
        }
        if let Some(pre_input_open) = &self.pre_input_open {
            pre_input_open.validate()?;
        }
        for schema in &self.command_schemas {
            schema.validate("command_schemas")?;
        }
        for schema in &self.event_schemas {
            schema.validate("event_schemas")?;
        }
        for capability in &self.capabilities {
            capability.validate()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSelector {
    pub module_id: ModuleId,
    pub artifact_id: ArtifactId,
    pub version: ArtifactVersion,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executable_sha256: Option<Sha256Digest>,
}

impl ArtifactSelector {
    pub fn new(
        module_id: ModuleId,
        artifact_id: ArtifactId,
        version: ArtifactVersion,
        executable_sha256: Option<Sha256Digest>,
    ) -> Self {
        Self {
            module_id,
            artifact_id,
            version,
            executable_sha256,
        }
    }

    fn validate(&self) -> Result<(), CatalogError> {
        self.module_id.validate()?;
        self.artifact_id.validate()?;
        self.version.validate()?;
        if let Some(digest) = &self.executable_sha256 {
            digest.validate("selector.executable_sha256")?;
        }
        Ok(())
    }
}

/// An in-memory view over already retained module descriptors. Construction
/// validates metadata only; it has no filesystem, process, network or Store
/// access.
#[derive(Debug, Clone, Default)]
pub struct ModuleCatalog {
    descriptors: Vec<ModuleDescriptor>,
}

impl ModuleCatalog {
    pub fn from_descriptors(
        descriptors: impl IntoIterator<Item = ModuleDescriptor>,
    ) -> Result<Self, CatalogError> {
        let mut catalog = Self {
            descriptors: descriptors.into_iter().collect(),
        };
        let mut identities = BTreeSet::new();
        for descriptor in &catalog.descriptors {
            descriptor.validate()?;
            let identity = (
                descriptor.module_id.clone(),
                descriptor.artifact.artifact_id.clone(),
                descriptor.artifact.version.clone(),
            );
            if !identities.insert(identity) {
                return Err(CatalogError::DuplicateArtifact {
                    module_id: descriptor.module_id.to_string(),
                    artifact_id: descriptor.artifact.artifact_id.to_string(),
                    version: descriptor.artifact.version.to_string(),
                });
            }
        }
        catalog.descriptors.sort_by(|left, right| {
            (
                &left.module_id,
                &left.artifact.artifact_id,
                &left.artifact.version,
            )
                .cmp(&(
                    &right.module_id,
                    &right.artifact.artifact_id,
                    &right.artifact.version,
                ))
        });
        Ok(catalog)
    }

    pub fn descriptors(&self) -> &[ModuleDescriptor] {
        &self.descriptors
    }

    /// Selects only the exact artifact identity requested by retained config.
    /// There is no "latest", fallback, launch or compatibility-based downgrade.
    pub fn select_exact(
        &self,
        selector: &ArtifactSelector,
        host_protocol: ProtocolRange,
        required_capabilities: &BTreeSet<CapabilityId>,
    ) -> Result<&ModuleDescriptor, CatalogError> {
        selector.validate()?;
        host_protocol.validate("host_protocol")?;
        for capability in required_capabilities {
            capability.validate()?;
        }

        let mut digest_mismatch = false;
        let descriptor = self
            .descriptors
            .iter()
            .find(|descriptor| {
                descriptor.module_id == selector.module_id
                    && descriptor.artifact.artifact_id == selector.artifact_id
                    && descriptor.artifact.version == selector.version
                    && match &selector.executable_sha256 {
                        Some(expected) => {
                            let matches =
                                descriptor.launch.executable_sha256.as_ref() == Some(expected);
                            digest_mismatch |= !matches;
                            matches
                        }
                        None => true,
                    }
            })
            .ok_or_else(|| {
                if digest_mismatch {
                    CatalogError::ArtifactDigestMismatch {
                        module_id: selector.module_id.to_string(),
                        artifact_id: selector.artifact_id.to_string(),
                        version: selector.version.to_string(),
                    }
                } else {
                    CatalogError::ArtifactNotFound {
                        module_id: selector.module_id.to_string(),
                        artifact_id: selector.artifact_id.to_string(),
                        version: selector.version.to_string(),
                    }
                }
            })?;

        if !descriptor.enabled {
            return Err(CatalogError::ModuleDisabled {
                module_id: descriptor.module_id.to_string(),
            });
        }
        if !descriptor.protocol.intersects(host_protocol) {
            return Err(CatalogError::ProtocolIncompatible {
                module_id: descriptor.module_id.to_string(),
                artifact_id: descriptor.artifact.artifact_id.to_string(),
                module_protocol: descriptor.protocol,
                host_protocol,
            });
        }

        let missing_capabilities: Vec<_> = required_capabilities
            .difference(&descriptor.capabilities)
            .cloned()
            .collect();
        if !missing_capabilities.is_empty() {
            return Err(CatalogError::MissingCapabilities {
                module_id: descriptor.module_id.to_string(),
                missing: missing_capabilities,
            });
        }
        Ok(descriptor)
    }
}

/// Evidence about the last worker start. Unknown means readback only: do not
/// start a second worker. A safe worker restart does not authorize replaying
/// the command or external effect the worker may have been processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchEvidence {
    NeverAttempted,
    ConfirmedExited,
    ConfirmedRunning,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LaunchDecision {
    StartNewProcess,
    AttachExisting,
    ReadBackOnly,
}

pub const fn launch_decision(evidence: LaunchEvidence) -> LaunchDecision {
    match evidence {
        LaunchEvidence::NeverAttempted | LaunchEvidence::ConfirmedExited => {
            LaunchDecision::StartNewProcess
        }
        LaunchEvidence::ConfirmedRunning => LaunchDecision::AttachExisting,
        LaunchEvidence::Unknown => LaunchDecision::ReadBackOnly,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CatalogError {
    #[error("invalid module descriptor field: {field}")]
    InvalidDescriptor { field: &'static str },
    #[error("unsupported module descriptor schema version {version}")]
    UnsupportedDescriptorSchema { version: u16 },
    #[error("duplicate module artifact {module_id}/{artifact_id}@{version}")]
    DuplicateArtifact {
        module_id: String,
        artifact_id: String,
        version: String,
    },
    #[error("module artifact {module_id}/{artifact_id}@{version} is not installed")]
    ArtifactNotFound {
        module_id: String,
        artifact_id: String,
        version: String,
    },
    #[error(
        "module artifact {module_id}/{artifact_id}@{version} has a different executable digest"
    )]
    ArtifactDigestMismatch {
        module_id: String,
        artifact_id: String,
        version: String,
    },
    #[error("module {module_id} is disabled")]
    ModuleDisabled { module_id: String },
    #[error(
        "module {module_id}/{artifact_id} protocol {module_protocol:?} is incompatible with host protocol {host_protocol:?}"
    )]
    ProtocolIncompatible {
        module_id: String,
        artifact_id: String,
        module_protocol: ProtocolRange,
        host_protocol: ProtocolRange,
    },
    #[error("module {module_id} is missing required capabilities: {missing:?}")]
    MissingCapabilities {
        module_id: String,
        missing: Vec<CapabilityId>,
    },
}
