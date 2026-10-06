//! Strict, bounded hot reload for the optional local observer recorder.
//!
//! This file is read only by the already-existing recorder writer thread. The
//! path is pinned by trusted host configuration and the scope is the
//! canonical local DataRoot. No file contents or path are exported.

use serde::Deserialize;
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use swarm_contracts::error::{Error, Result};

use crate::{MAX_RETENTION_BYTES, MAX_RETENTION_DAYS, now_unix_ms};

const MAX_LIVE_CONFIG_BYTES: usize = 16_384;
const MAX_SCOPE_ID_BYTES: usize = 4_096;
const MAX_SCOPED_OVERRIDES: usize = 64;
const MAX_SELECTOR_BYTES: usize = 128;

#[derive(Clone, Debug)]
pub struct LiveConfigSource {
    path: Option<PathBuf>,
    scope_id: String,
    text_settings: Arc<RwLock<Option<LiveSettings>>>,
}

impl LiveConfigSource {
    /// Bind the watcher to an operator-pinned file and the current canonical
    /// controller data root. `scope_id` is compared locally and never emitted.
    pub fn new(path: PathBuf, current_scope: &Path) -> Self {
        Self::for_scope(Some(path), current_scope)
    }

    /// Keep the live policy seam available when no operator file is configured.
    /// This exposes only the safe Info/metadata baseline, which an exact active
    /// Manager policy may override; it does not create a watcher or file.
    pub fn defaults_for_scope(current_scope: &Path) -> Self {
        Self::for_scope(None, current_scope)
    }

    fn for_scope(path: Option<PathBuf>, current_scope: &Path) -> Self {
        let default_settings = LiveSettings::initial(MAX_RETENTION_BYTES, MAX_RETENTION_DAYS);
        let text_settings = path.is_none().then_some(default_settings);
        Self {
            path,
            scope_id: current_scope.to_string_lossy().into_owned(),
            text_settings: Arc::new(RwLock::new(text_settings)),
        }
    }

    /// Share the live content decision with the telemetry producer. The
    /// producer checks this policy before redacting or queueing any text.
    pub fn text_capture_policy(&self) -> swarm_telemetry::TextCapturePolicy {
        let settings = Arc::clone(&self.text_settings);
        Arc::new(
            move |severity, kind, module_id, client_id, operation_id, manager_policy| {
                let settings = settings
                    .read()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let Some(settings) = settings.as_ref() else {
                    // An operator-pinned file is fail-closed until the existing
                    // recorder worker validates and publishes its first snapshot.
                    return false;
                };
                let severity = match severity {
                    swarm_telemetry::Severity::Error => Severity::Error,
                    swarm_telemetry::Severity::Warn => Severity::Warn,
                    swarm_telemetry::Severity::Info => Severity::Info,
                    swarm_telemetry::Severity::Debug => Severity::Debug,
                    swarm_telemetry::Severity::Trace => Severity::Trace,
                };
                let kind = match kind {
                    swarm_telemetry::Kind::ClientDisconnected => Kind::ClientDisconnected,
                    swarm_telemetry::Kind::StoreOperationFailed => Kind::StoreOperationFailed,
                    swarm_telemetry::Kind::ModuleStarted => Kind::ModuleStarted,
                    swarm_telemetry::Kind::ModuleStopped => Kind::ModuleStopped,
                    swarm_telemetry::Kind::AgentDeliveryFailed => Kind::AgentDeliveryFailed,
                    swarm_telemetry::Kind::RecorderFailure => Kind::RecorderFailure,
                };
                settings.allows_text_capture(
                    now_unix_ms(),
                    severity,
                    kind,
                    module_id,
                    client_id,
                    operation_id,
                    manager_policy,
                )
            },
        )
    }

    pub(crate) fn publish_text_settings(&self, settings: &LiveSettings) {
        *self
            .text_settings
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(settings.clone());
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.path.as_ref().is_some_and(|path| !path.is_absolute())
            || self.scope_id.len() > MAX_SCOPE_ID_BYTES
            || !Path::new(&self.scope_id).is_absolute()
        {
            return Err(Error::new(
                "OBSERVER_LIVE_CONFIG_INVALID",
                "live observer config path or scope is invalid",
            ));
        }
        Ok(())
    }

    pub(crate) fn load_update(
        &self,
        current: &LiveSettings,
        segment_bytes: u64,
    ) -> Result<Option<LiveSettings>> {
        let Some(path) = self.path.as_ref() else {
            return Ok(None);
        };
        let metadata = fs::symlink_metadata(path).map_err(|_| unavailable())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid());
        }
        if metadata.len() > MAX_LIVE_CONFIG_BYTES as u64 {
            return Err(invalid());
        }

        let file = File::open(path).map_err(|_| unavailable())?;
        let opened_metadata = file.metadata().map_err(|_| unavailable())?;
        if !opened_metadata.is_file() || opened_metadata.len() > MAX_LIVE_CONFIG_BYTES as u64 {
            return Err(invalid());
        }
        let mut bytes = Vec::with_capacity(opened_metadata.len() as usize);
        file.take((MAX_LIVE_CONFIG_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|_| unavailable())?;
        if bytes.len() > MAX_LIVE_CONFIG_BYTES {
            return Err(invalid());
        }

        let envelope: serde_json::Value = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        let schema_version = envelope
            .get("schema_version")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u8::try_from(value).ok())
            .ok_or_else(invalid)?;
        let parsed = match schema_version {
            1 => {
                let file: LiveConfigFileV1 =
                    serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if file.schema_version != schema_version {
                    return Err(invalid());
                }
                ParsedLiveConfig {
                    config_version: file.config_version,
                    scope_id: file.scope_id,
                    level: FilterLevel::from_severity(file.minimum_severity),
                    content: ContentMode::Metadata,
                    included_kinds: file.included_kinds,
                    overrides: Vec::new(),
                    retention_bytes: file.retention_bytes,
                    retention_days: file.retention_days,
                }
            }
            2 => {
                let file: LiveConfigFileV2 =
                    serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if file.schema_version != schema_version {
                    return Err(invalid());
                }
                ParsedLiveConfig {
                    config_version: file.config_version,
                    scope_id: file.scope_id,
                    level: file.level,
                    content: ContentMode::Metadata,
                    included_kinds: file.included_kinds,
                    overrides: file.overrides,
                    retention_bytes: file.retention_bytes,
                    retention_days: file.retention_days,
                }
            }
            3 => {
                let file: LiveConfigFileV3 =
                    serde_json::from_slice(&bytes).map_err(|_| invalid())?;
                if file.schema_version != schema_version {
                    return Err(invalid());
                }
                ParsedLiveConfig {
                    config_version: file.config_version,
                    scope_id: file.scope_id,
                    level: file.level,
                    content: file.content,
                    included_kinds: file.included_kinds,
                    overrides: file.overrides,
                    retention_bytes: file.retention_bytes,
                    retention_days: file.retention_days,
                }
            }
            _ => return Err(invalid()),
        };
        if parsed.scope_id.len() > MAX_SCOPE_ID_BYTES {
            return Err(invalid());
        }
        if parsed.content == ContentMode::RedactedNativeFrames
            || parsed
                .overrides
                .iter()
                .any(|item| item.content == Some(ContentMode::RedactedNativeFrames))
        {
            return Err(Error::new(
                "OBSERVER_CONTENT_UNSUPPORTED",
                "no bounded native-frame producer is available",
            ));
        }
        if parsed.scope_id != self.scope_id {
            return Err(Error::new(
                "OBSERVER_LIVE_CONFIG_SCOPE_MISMATCH",
                "live observer config belongs to another local data scope",
            ));
        }
        if parsed.config_version == 0 || parsed.config_version < current.config_version {
            return Err(version_rejected());
        }
        if schema_version < 3 && parsed.overrides.iter().any(|item| item.content.is_some()) {
            return Err(invalid());
        }
        if parsed.retention_bytes < segment_bytes
            || parsed.retention_bytes > MAX_RETENTION_BYTES
            || parsed.retention_days == 0
            || parsed.retention_days > MAX_RETENTION_DAYS
            || parsed.included_kinds.len() > Kind::ALL.len()
            || parsed.overrides.len() > MAX_SCOPED_OVERRIDES
        {
            return Err(invalid());
        }

        let mut included_kinds = BTreeSet::new();
        for kind in parsed.included_kinds {
            if !included_kinds.insert(kind) {
                return Err(invalid());
            }
        }
        validate_overrides(&parsed.overrides)?;
        let next = LiveSettings {
            config_version: parsed.config_version,
            level: parsed.level,
            content: parsed.content,
            included_kinds,
            overrides: parsed.overrides,
            retention_bytes: parsed.retention_bytes,
            retention_days: parsed.retention_days,
        };
        if next.config_version == current.config_version {
            return if next == *current {
                Ok(None)
            } else {
                Err(version_rejected())
            };
        }
        Ok(Some(next))
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Severity {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Severity {
    pub(crate) fn from_record(value: &str) -> Option<Self> {
        match value {
            "error" => Some(Self::Error),
            "warn" => Some(Self::Warn),
            "info" => Some(Self::Info),
            "debug" => Some(Self::Debug),
            "trace" => Some(Self::Trace),
            _ => None,
        }
    }
}

/// A filter policy is distinct from a record severity: `off` suppresses the
/// matching scope and is never accepted as a diagnostic record level.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub(crate) enum FilterLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

/// Optional content modes accepted by the local recorder. Native frames are
/// unsupported until a real bounded native-frame producer exists.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ContentMode {
    Metadata,
    RedactedText,
    RedactedNativeFrames,
}

impl FilterLevel {
    fn from_severity(value: Severity) -> Self {
        match value {
            Severity::Error => Self::Error,
            Severity::Warn => Self::Warn,
            Severity::Info => Self::Info,
            Severity::Debug => Self::Debug,
            Severity::Trace => Self::Trace,
        }
    }

    fn allows(self, severity: Severity) -> bool {
        match self {
            Self::Off => false,
            Self::Error => matches!(severity, Severity::Error),
            Self::Warn => matches!(severity, Severity::Error | Severity::Warn),
            Self::Info => matches!(severity, Severity::Error | Severity::Warn | Severity::Info),
            Self::Debug => matches!(
                severity,
                Severity::Error | Severity::Warn | Severity::Info | Severity::Debug
            ),
            Self::Trace => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Kind {
    ClientDisconnected,
    StoreOperationFailed,
    ModuleStarted,
    ModuleStopped,
    AgentDeliveryFailed,
    RecorderFailure,
}

impl Kind {
    const ALL: [Self; 6] = [
        Self::ClientDisconnected,
        Self::StoreOperationFailed,
        Self::ModuleStarted,
        Self::ModuleStopped,
        Self::AgentDeliveryFailed,
        Self::RecorderFailure,
    ];

    pub(crate) fn from_record(value: &str) -> Option<Self> {
        match value {
            "client_disconnected" => Some(Self::ClientDisconnected),
            "store_operation_failed" => Some(Self::StoreOperationFailed),
            "module_started" => Some(Self::ModuleStarted),
            "module_stopped" => Some(Self::ModuleStopped),
            "agent_delivery_failed" => Some(Self::AgentDeliveryFailed),
            "recorder_failure" => Some(Self::RecorderFailure),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LiveSettings {
    pub(crate) config_version: u64,
    pub(crate) level: FilterLevel,
    pub(crate) content: ContentMode,
    pub(crate) included_kinds: BTreeSet<Kind>,
    pub(crate) overrides: Vec<ScopedOverride>,
    pub(crate) retention_bytes: u64,
    pub(crate) retention_days: u64,
}

impl LiveSettings {
    pub(crate) fn initial(retention_bytes: u64, retention_days: u64) -> Self {
        Self {
            config_version: 0,
            level: FilterLevel::Info,
            content: ContentMode::Metadata,
            included_kinds: Kind::ALL.into_iter().collect(),
            overrides: Vec::new(),
            retention_bytes,
            retention_days,
        }
    }

    pub(crate) fn allows_with_manager_override(
        &self,
        now_unix_ms: u64,
        severity: Severity,
        kind: Kind,
        module_id: Option<&str>,
        client_id: Option<&str>,
        operation_id: Option<&str>,
        manager_policy: Option<swarm_telemetry::ScopedPolicyOverride>,
    ) -> bool {
        if !self.included_kinds.contains(&kind) {
            return false;
        }
        let operator_level = self.effective_level(now_unix_ms, module_id, client_id, operation_id);
        // An operator's explicit off remains the master fail-closed rule. The
        // ordinary global Info default is not a ceiling on an exact active
        // Manager scope's level choice.
        if operator_level == FilterLevel::Off {
            return false;
        }
        match active_manager_policy(now_unix_ms, manager_policy) {
            Some(policy) => manager_level_allows(policy.level, severity),
            None => operator_level.allows(severity),
        }
    }

    pub(crate) fn allows_record(
        &self,
        now_unix_ms: u64,
        severity: Severity,
        kind: Kind,
        module_id: Option<&str>,
        client_id: Option<&str>,
        operation_id: Option<&str>,
        manager_policy: Option<swarm_telemetry::ScopedPolicyOverride>,
        contains_redacted_text: bool,
    ) -> bool {
        let manager_policy = active_manager_policy(now_unix_ms, manager_policy);
        if !self.allows_with_manager_override(
            now_unix_ms,
            severity,
            kind,
            module_id,
            client_id,
            operation_id,
            manager_policy,
        ) {
            return false;
        }
        if !contains_redacted_text {
            return true;
        }
        match manager_policy.map(|policy| policy.content) {
            Some(swarm_telemetry::FilterContent::Metadata) => false,
            Some(swarm_telemetry::FilterContent::RedactedText) => selected_text_content_override(
                &self.overrides,
                now_unix_ms,
                module_id,
                client_id,
                operation_id,
            )
            .is_none_or(|content| content == ContentMode::RedactedText),
            None => text_content_allows(
                self.content,
                &self.overrides,
                now_unix_ms,
                module_id,
                client_id,
                operation_id,
            ),
        }
    }

    fn effective_level(
        &self,
        now_unix_ms: u64,
        module_id: Option<&str>,
        client_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> FilterLevel {
        // Operation is the narrowest existing identity, followed by the
        // agent/client and then module. Each v2 entry has exactly one selector,
        // so this precedence is deterministic even when scopes overlap.
        if let Some(value) = operation_id
            && let Some(level) = self.overrides.iter().find_map(|item| {
                (item.expires_at_unix_ms > now_unix_ms
                    && item.operation_id.as_deref() == Some(value))
                .then_some(item.level)
            })
        {
            return level;
        }
        if let Some(value) = client_id
            && let Some(level) = self.overrides.iter().find_map(|item| {
                (item.expires_at_unix_ms > now_unix_ms && item.client_id.as_deref() == Some(value))
                    .then_some(item.level)
            })
        {
            return level;
        }
        if let Some(value) = module_id
            && let Some(level) = self.overrides.iter().find_map(|item| {
                (item.expires_at_unix_ms > now_unix_ms && item.module_id.as_deref() == Some(value))
                    .then_some(item.level)
            })
        {
            return level;
        }
        self.level
    }

    fn allows_text_capture(
        &self,
        now_unix_ms: u64,
        severity: Severity,
        kind: Kind,
        module_id: Option<&str>,
        client_id: Option<&str>,
        operation_id: Option<&str>,
        manager_policy: Option<swarm_telemetry::ScopedPolicyOverride>,
    ) -> bool {
        if !self.allows_with_manager_override(
            now_unix_ms,
            severity,
            kind,
            module_id,
            client_id,
            operation_id,
            manager_policy,
        ) {
            return false;
        }
        match active_manager_policy(now_unix_ms, manager_policy).map(|policy| policy.content) {
            Some(swarm_telemetry::FilterContent::Metadata) => false,
            Some(swarm_telemetry::FilterContent::RedactedText) => {
                // A scoped operator metadata override is an explicit content
                // restriction. The global metadata default is overridable by
                // an exact Manager opt-in.
                selected_text_content_override(
                    &self.overrides,
                    now_unix_ms,
                    module_id,
                    client_id,
                    operation_id,
                )
                .is_none_or(|content| content == ContentMode::RedactedText)
            }
            None => text_content_allows(
                self.content,
                &self.overrides,
                now_unix_ms,
                module_id,
                client_id,
                operation_id,
            ),
        }
    }
}

fn active_manager_policy(
    now_unix_ms: u64,
    policy: Option<swarm_telemetry::ScopedPolicyOverride>,
) -> Option<swarm_telemetry::ScopedPolicyOverride> {
    let now_unix_ms = i64::try_from(now_unix_ms).ok()?;
    policy.filter(|policy| {
        policy
            .expires_at_ms
            .is_none_or(|expires_at_ms| expires_at_ms > now_unix_ms)
    })
}

fn manager_level_allows(level: swarm_telemetry::FilterLevel, severity: Severity) -> bool {
    match level {
        swarm_telemetry::FilterLevel::Off => false,
        swarm_telemetry::FilterLevel::Error => matches!(severity, Severity::Error),
        swarm_telemetry::FilterLevel::Warn => {
            matches!(severity, Severity::Error | Severity::Warn)
        }
        swarm_telemetry::FilterLevel::Info => {
            matches!(severity, Severity::Error | Severity::Warn | Severity::Info)
        }
        swarm_telemetry::FilterLevel::Debug => {
            matches!(
                severity,
                Severity::Error | Severity::Warn | Severity::Info | Severity::Debug
            )
        }
        swarm_telemetry::FilterLevel::Trace => true,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveConfigFileV1 {
    schema_version: u8,
    config_version: u64,
    scope_id: String,
    minimum_severity: Severity,
    included_kinds: Vec<Kind>,
    retention_bytes: u64,
    retention_days: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveConfigFileV2 {
    schema_version: u8,
    config_version: u64,
    scope_id: String,
    level: FilterLevel,
    included_kinds: Vec<Kind>,
    overrides: Vec<ScopedOverride>,
    retention_bytes: u64,
    retention_days: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveConfigFileV3 {
    schema_version: u8,
    config_version: u64,
    scope_id: String,
    level: FilterLevel,
    content: ContentMode,
    included_kinds: Vec<Kind>,
    overrides: Vec<ScopedOverride>,
    retention_bytes: u64,
    retention_days: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedOverride {
    pub(crate) module_id: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) operation_id: Option<String>,
    pub(crate) level: FilterLevel,
    #[serde(default)]
    pub(crate) content: Option<ContentMode>,
    /// Absolute Unix epoch milliseconds. Relative TTLs are not reconstructed
    /// across a recorder restart.
    pub(crate) expires_at_unix_ms: u64,
}

struct ParsedLiveConfig {
    config_version: u64,
    scope_id: String,
    level: FilterLevel,
    content: ContentMode,
    included_kinds: Vec<Kind>,
    overrides: Vec<ScopedOverride>,
    retention_bytes: u64,
    retention_days: u64,
}

fn text_content_allows(
    baseline: ContentMode,
    overrides: &[ScopedOverride],
    now_unix_ms: u64,
    module_id: Option<&str>,
    client_id: Option<&str>,
    operation_id: Option<&str>,
) -> bool {
    selected_text_content_override(overrides, now_unix_ms, module_id, client_id, operation_id)
        .unwrap_or(baseline)
        == ContentMode::RedactedText
}

fn selected_text_content_override(
    overrides: &[ScopedOverride],
    now_unix_ms: u64,
    module_id: Option<&str>,
    client_id: Option<&str>,
    operation_id: Option<&str>,
) -> Option<ContentMode> {
    for (selector, value) in [
        (Selector::Operation, operation_id),
        (Selector::Client, client_id),
        (Selector::Module, module_id),
    ] {
        if let Some(value) = value
            && let Some(content) = overrides.iter().find_map(|item| {
                (item.expires_at_unix_ms > now_unix_ms
                    && item.content.is_some()
                    && selector.matches(item, value))
                .then_some(item.content)
                .flatten()
            })
        {
            return Some(content);
        }
    }
    None
}

#[derive(Clone, Copy)]
enum Selector {
    Module,
    Client,
    Operation,
}

impl Selector {
    fn matches(self, item: &ScopedOverride, value: &str) -> bool {
        match self {
            Self::Module => item.module_id.as_deref() == Some(value),
            Self::Client => item.client_id.as_deref() == Some(value),
            Self::Operation => item.operation_id.as_deref() == Some(value),
        }
    }
}

fn validate_overrides(overrides: &[ScopedOverride]) -> Result<()> {
    let mut selectors = BTreeSet::new();
    for item in overrides {
        let selector_count = [
            item.module_id.is_some(),
            item.client_id.is_some(),
            item.operation_id.is_some(),
        ]
        .into_iter()
        .filter(|present| *present)
        .count();
        if selector_count != 1 || item.expires_at_unix_ms == 0 {
            return Err(invalid());
        }
        for value in [
            item.module_id.as_deref(),
            item.client_id.as_deref(),
            item.operation_id.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            if !valid_selector(value) {
                return Err(invalid());
            }
        }
        if !selectors.insert((
            item.module_id.clone(),
            item.client_id.clone(),
            item.operation_id.clone(),
        )) {
            return Err(invalid());
        }
    }
    Ok(())
}

fn valid_selector(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_SELECTOR_BYTES
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn unavailable() -> Error {
    Error::new(
        "OBSERVER_LIVE_CONFIG_UNAVAILABLE",
        "live observer config could not be read",
    )
}

fn invalid() -> Error {
    Error::new(
        "OBSERVER_LIVE_CONFIG_INVALID",
        "live observer config does not match the bounded schema",
    )
}

fn version_rejected() -> Error {
    Error::new(
        "OBSERVER_LIVE_CONFIG_VERSION_REJECTED",
        "live observer config version is not a new consistent version",
    )
}
