//! OpenCode V2 connects to explicit external HTTP services or a separately
//! admitted fresh owned foreground service. Native effects never replay.
mod background;
mod configuration;
mod effects;
mod execution;
mod goal;
mod http;
mod mcp;
pub(crate) mod mcp_install;
pub(crate) mod mcp_plugin;
pub(crate) mod mcp_tools;
pub(crate) mod owned_service;
mod prerequisites;
mod provider_auth;
mod resource_paths;
mod results;
mod snapshot;
#[cfg(test)]
pub(crate) mod tests;

use crate::error::{Error, Result};
pub(crate) use background::BACKGROUND_CONTRACT_REVISION;
pub(crate) use configuration::{
    ConfigurationExpectation, ValidatedConfiguration, configuration_contract,
    configuration_expectation,
};
pub(crate) use execution::{
    ExecutionRead, ExecutionScan, NativeInputDescriptor, RootCreationRead, RootCreationScan,
    SessionRead, SessionScan,
};
pub(crate) use goal::GOAL_CONTRACT_REVISION;
pub(crate) use http::{EventReader, EventState, Service};
pub(crate) use prerequisites::Validator as PrerequisiteValidator;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const ARTIFACT_ID: &str = "eliot-opencode-v2.http.1";
pub const RUNTIME: &str = "opencode_v2";

/// Internal foreground owner entry used by the controller's dedicated helper.
/// It does not open the controller database or admit a launch request.
pub fn run_owned_service_helper(plan_path: &std::path::Path) -> Result<()> {
    owned_service::run_owned_service_helper(plan_path)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ModelRef {
    pub id: String,
    #[serde(rename = "providerID")]
    pub provider_id: String,
    // Deliberately required: choosing a root model does not retain its variant.
    pub variant: String,
}
impl ModelRef {
    pub(crate) fn valid(&self) -> bool {
        [&self.id, &self.provider_id, &self.variant]
            .iter()
            .all(|value| {
                !value.trim().is_empty()
                    && value.len() <= 256
                    && !value.bytes().any(|byte| byte.is_ascii_control())
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Options {
    /// Stable operator-assigned namespace. A port/PID is not durable session identity.
    pub service_id: String,
    /// Explicit adapter connection record; read only, never generated or repaired.
    pub connection_file: PathBuf,
    pub expected_version: String,
    pub directory: PathBuf,
    pub model: ModelRef,
}
impl Options {
    pub fn parse(value: &serde_json::Value) -> Result<Self> {
        let options: Self = serde_json::from_value(value.clone())
            .map_err(|_| Error::new("CONFIG_ERROR", "invalid OpenCode V2 route options"))?;
        if options.service_id.is_empty()
            || options.service_id.len() > 128
            || !options
                .service_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || !options.connection_file.is_absolute()
            || !options.directory.is_absolute()
            || options.directory.to_str().is_none()
            || options.expected_version.trim().is_empty()
            || !options.model.valid()
        {
            return Err(Error::new(
                "CONFIG_ERROR",
                "OpenCode requires absolute paths, a service ID, exact version and explicit provider/model/variant",
            ));
        }
        Ok(options)
    }
    pub(crate) fn scope(&self) -> String {
        format!("opencode-v2:{}", self.service_id)
    }
}

/// Capture only the native MCP facts exposed by the pinned OpenCode HTTP API.
/// The Store supplies an exact current assignment; native tool loading remains
/// unknown unless a separate trusted producer proves it.
pub(crate) async fn observe_mcp(
    service: &Service,
    options: &Options,
    assignment: crate::native_mcp::AssignmentContext,
) -> Result<crate::native_mcp::NativeMcpReadback> {
    mcp::observe(service, options, assignment).await
}

/// Public API IDs are opaque, but they must remain a single safe path segment.
pub(crate) fn valid_id(value: &str, prefix: &str) -> Result<()> {
    if !value.starts_with(prefix)
        || value.len() <= prefix.len()
        || value.len() > 256
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(Error::new(
            "NATIVE_SCHEMA_ERROR",
            "invalid native identifier",
        ));
    }
    Ok(())
}

pub(crate) fn root_id(binding: &str, generation: i64) -> String {
    format!(
        "ses_swarm_{}",
        crate::model::digest(format!("{binding}/{generation}").as_bytes())
    )
}
pub(crate) fn input_id(operation: &str) -> String {
    format!("msg_swarm_{}", crate::model::digest(operation.as_bytes()))
}

/// Keep diagnostics useful without persisting reflected prompts, credentials or URLs.
pub(crate) fn diagnostic(error: &Error) -> serde_json::Value {
    serde_json::json!({"code":error.code})
}
