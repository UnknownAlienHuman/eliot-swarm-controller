//! Explicit optional host configuration for the module supervisor actor.
//! This file is proposed for inclusion by the tracked `config.rs` patch.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleRouteConfigMapper {
    /// Select the launch-config adapter by the exact retained descriptor
    /// schema. Descriptors without a launch config schema receive no extra
    /// process environment; their admitted options travel in RuntimeCommand.
    DescriptorSchema,
    EmptyOnly,
    OpenCodeSevenField,
}

impl Default for ModuleRouteConfigMapper {
    fn default() -> Self {
        Self::DescriptorSchema
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ModuleSupervisorConfig {
    pub enabled: bool,
    pub install_root: Option<PathBuf>,
    pub descriptor_files: Vec<PathBuf>,
    pub owner_helper: Option<PathBuf>,
    pub owner_helper_sha256: Option<String>,
    /// Exact opaque reference -> existing file path. This map contains no
    /// credential material and is never written to Store metadata.
    pub protected_files: BTreeMap<String, PathBuf>,
    pub route_config_mapper: ModuleRouteConfigMapper,
}

impl Default for ModuleSupervisorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            install_root: None,
            descriptor_files: Vec::new(),
            owner_helper: None,
            owner_helper_sha256: None,
            protected_files: BTreeMap::new(),
            route_config_mapper: ModuleRouteConfigMapper::DescriptorSchema,
        }
    }
}

impl ModuleSupervisorConfig {
    pub(crate) fn resolve_paths(&mut self, config_dir: &Path) {
        for path in self
            .install_root
            .iter_mut()
            .chain(self.owner_helper.iter_mut())
            .chain(self.descriptor_files.iter_mut())
            .chain(self.protected_files.values_mut())
        {
            if path.is_relative() {
                *path = config_dir.join(&*path);
            }
        }
    }

    /// Runtime-only validation. A malformed optional actor config is isolated
    /// by its own restart loop and cannot prevent the base host from starting.
    pub(crate) fn validate(&self) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let install_root = self.install_root.as_ref().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "install_root is required when enabled",
            )
        })?;
        let owner_helper = self.owner_helper.as_ref().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "owner_helper is required when enabled",
            )
        })?;
        let owner_digest = self.owner_helper_sha256.as_deref().ok_or_else(|| {
            Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "owner_helper_sha256 is required when enabled",
            )
        })?;
        if !install_root.is_absolute()
            || !owner_helper.is_absolute()
            || self.descriptor_files.is_empty()
            || self.descriptor_files.len() > 256
            || self.protected_files.len() > 128
            || self.descriptor_files.iter().any(|path| !path.is_absolute())
            || self
                .protected_files
                .values()
                .any(|path| !path.is_absolute())
            || owner_digest.len() != 64
            || !owner_digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Err(Error::new(
                "MODULE_SUPERVISOR_CONFIG_INVALID",
                "module supervisor paths, package count, or helper SHA-256 are invalid",
            ));
        }
        for reference in self.protected_files.keys() {
            if reference.is_empty()
                || reference.len() > 512
                || reference.chars().any(char::is_control)
            {
                return Err(Error::new(
                    "MODULE_SUPERVISOR_CONFIG_INVALID",
                    "protected file reference is invalid",
                ));
            }
        }
        Ok(())
    }
}
