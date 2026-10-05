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

        let file: LiveConfigFile = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if file.schema_version != 1 {
            return Err(invalid());
        }
        if file.scope_id != self.scope_id {
            return Err(Error::new(
                "OBSERVER_LIVE_CONFIG_SCOPE_MISMATCH",
                "live observer config belongs to another local data scope",
            ));
        }
        if file.config_version == 0 || file.config_version < current.config_version {
            return Err(version_rejected());
        }
        if file.retention_bytes < segment_bytes
            || file.retention_bytes > MAX_RETENTION_BYTES
            || file.retention_days == 0
            || file.retention_days > MAX_RETENTION_DAYS
            || file.included_kinds.len() > Kind::ALL.len()
        {
            return Err(invalid());
        }

        let mut included_kinds = BTreeSet::new();
        for kind in file.included_kinds {
            if !included_kinds.insert(kind) {
                return Err(invalid());
            }
        }
        let next = LiveSettings {
            config_version: file.config_version,
            minimum_severity: file.minimum_severity,
            included_kinds,
            retention_bytes: file.retention_bytes,
            retention_days: file.retention_days,
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
    fn rank(self) -> u8 {
        match self {
            Self::Error => 0,
            Self::Warn => 1,
            Self::Info => 2,
            Self::Debug => 3,
            Self::Trace => 4,
        }
    }

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
    pub(crate) minimum_severity: Severity,
    pub(crate) included_kinds: BTreeSet<Kind>,
    pub(crate) retention_bytes: u64,
    pub(crate) retention_days: u64,
}

impl LiveSettings {
    pub(crate) fn initial(retention_bytes: u64, retention_days: u64) -> Self {
        Self {
            config_version: 0,
            minimum_severity: Severity::Trace,
            included_kinds: Kind::ALL.into_iter().collect(),
            retention_bytes,
            retention_days,
        }
    }

    pub(crate) fn allows(&self, severity: Severity, kind: Kind) -> bool {
        severity.rank() <= self.minimum_severity.rank() && self.included_kinds.contains(&kind)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveConfigFile {
    schema_version: u8,
    config_version: u64,
    scope_id: String,
    minimum_severity: Severity,
    included_kinds: Vec<Kind>,
    retention_bytes: u64,
    retention_days: u64,
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
