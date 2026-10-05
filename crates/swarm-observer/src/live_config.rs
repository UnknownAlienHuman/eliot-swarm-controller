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
};
use swarm_contracts::error::{Error, Result};

use crate::{MAX_RETENTION_BYTES, MAX_RETENTION_DAYS};

const MAX_LIVE_CONFIG_BYTES: usize = 16_384;
const MAX_SCOPE_ID_BYTES: usize = 4_096;
const MAX_SCOPED_OVERRIDES: usize = 64;
const MAX_SELECTOR_BYTES: usize = 128;

#[derive(Clone, Debug)]
pub struct LiveConfigSource {
    path: PathBuf,
    scope_id: String,
}

impl LiveConfigSource {
    /// Bind the watcher to an operator-pinned file and the current canonical
    /// controller data root. `scope_id` is compared locally and never emitted.
    pub fn new(path: PathBuf, current_scope: &Path) -> Self {
        Self {
            path,
            scope_id: current_scope.to_string_lossy().into_owned(),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if !self.path.is_absolute()
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
        let metadata = fs::symlink_metadata(&self.path).map_err(|_| unavailable())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err(invalid());
        }
        if metadata.len() > MAX_LIVE_CONFIG_BYTES as u64 {
            return Err(invalid());
        }

        let file = File::open(&self.path).map_err(|_| unavailable())?;
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
                    included_kinds: file.included_kinds,
                    overrides: file.overrides,
                    retention_bytes: file.retention_bytes,
                    retention_days: file.retention_days,
                }
            }
            // Schema 3 belongs to the diagnostic record wire format. This
            // file deliberately has only the additive v2 filter schema.
            _ => return Err(invalid()),
        };
        if parsed.scope_id.len() > MAX_SCOPE_ID_BYTES {
            return Err(invalid());
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
            included_kinds: Kind::ALL.into_iter().collect(),
            overrides: Vec::new(),
            retention_bytes,
            retention_days,
        }
    }

    pub(crate) fn allows(
        &self,
        now_unix_ms: u64,
        severity: Severity,
        kind: Kind,
        module_id: Option<&str>,
        client_id: Option<&str>,
        operation_id: Option<&str>,
    ) -> bool {
        if !self.included_kinds.contains(&kind) {
            return false;
        }
        self.effective_level(now_unix_ms, module_id, client_id, operation_id)
            .allows(severity)
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScopedOverride {
    pub(crate) module_id: Option<String>,
    pub(crate) client_id: Option<String>,
    pub(crate) operation_id: Option<String>,
    pub(crate) level: FilterLevel,
    /// Absolute Unix epoch milliseconds. Relative TTLs are not reconstructed
    /// across a recorder restart.
    pub(crate) expires_at_unix_ms: u64,
}

struct ParsedLiveConfig {
    config_version: u64,
    scope_id: String,
    level: FilterLevel,
    included_kinds: Vec<Kind>,
    overrides: Vec<ScopedOverride>,
    retention_bytes: u64,
    retention_days: u64,
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
