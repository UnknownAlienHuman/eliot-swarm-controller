//! Safe, status-only observations emitted by the host actor to the Store.
//!
//! The DTO deliberately excludes process paths, PIDs, image receipts, launch
//! arguments, credentials, Operation inputs, and results. A `not_started`
//! certainty is produced only by the helper's exact pre-spawn receipt after
//! whole-family departure has been proven.

use crate::{
    LifecycleState, ServiceScope, SupervisorStatus,
    error::{Error, Result},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleSupervisorPhase {
    WaitingForDemand,
    WaitingForKernel,
    Starting,
    Ready,
    RestartBackoff,
    Exited,
    OwnerRetained,
    IdentityUnknown,
    Completed,
    Isolated,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleEffectCertainty {
    Unknown,
    NotStarted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModuleFailureStage {
    ResolveRefs,
    ValidateLaunch,
    Spawn,
    Worker,
    Owner,
    Store,
    Journal,
}

impl ModuleFailureStage {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ResolveRefs => "resolve_refs",
            Self::ValidateLaunch => "validate_launch",
            Self::Spawn => "spawn",
            Self::Worker => "worker",
            Self::Owner => "owner",
            Self::Store => "store",
            Self::Journal => "journal",
        }
    }

    pub(crate) fn from_helper(value: &str) -> Option<Self> {
        match value {
            "resolve_refs" => Some(Self::ResolveRefs),
            "validate_launch" => Some(Self::ValidateLaunch),
            _ => None,
        }
    }
}

/// A bounded, status-only Store callback. `event_id` is stable for retries of
/// this callback within the host process and names the exact helper boot when
/// one exists; otherwise it uses the host actor instance UUID.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleSupervisorObservation {
    pub schema_version: u16,
    pub actor_instance_id: String,
    pub event_id: String,
    pub sequence: u64,
    pub module_id: String,
    pub artifact_id: String,
    pub artifact_version: String,
    pub build_id: Option<String>,
    pub scope: ServiceScope,
    pub boot_id: Option<String>,
    pub phase: ModuleSupervisorPhase,
    pub effect_certainty: ModuleEffectCertainty,
    pub stage: Option<ModuleFailureStage>,
    pub error_code: Option<String>,
    pub unknown_operation_ids: Vec<String>,
    pub unknown_operation_count: usize,
    pub unknown_operation_ids_truncated: bool,
}

impl ModuleSupervisorObservation {
    pub fn from_status(
        status: &SupervisorStatus,
        actor_instance_id: &str,
        sequence: u64,
    ) -> Result<Self> {
        validate_token(actor_instance_id, "actor_instance_id")?;
        if sequence == 0 || status.module_id.is_empty() || status.artifact_id.is_empty() {
            return Err(Error::invalid(
                "module observation identity or sequence is invalid",
            ));
        }
        let boot_id = status.worker_boot_id.clone();
        let event_id = match boot_id.as_deref() {
            Some(boot_id) => format!("{boot_id}:{actor_instance_id}:{sequence}"),
            None => format!("{actor_instance_id}:{sequence}"),
        };
        if event_id.len() > 256 {
            return Err(Error::invalid(
                "module observation event_id exceeds its bound",
            ));
        }
        let mut unknown_operation_ids = status.unknown_operation_ids.clone();
        unknown_operation_ids.sort();
        unknown_operation_ids.dedup();
        if unknown_operation_ids.len() > 256 {
            return Err(Error::new(
                "MODULE_OBSERVATION_LIMIT",
                "module observation exceeds the unresolved Operation ID bound",
            ));
        }
        for operation_id in &unknown_operation_ids {
            validate_token(operation_id, "operation_id")?;
        }
        let error_code = status
            .last_failure
            .as_ref()
            .map(|failure| failure.code.clone());
        if let Some(code) = error_code.as_deref() {
            if code.len() > 128
                || code.is_empty()
                || !code
                    .bytes()
                    .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
            {
                return Err(Error::new(
                    "MODULE_OBSERVATION_INVALID",
                    "module observation error code is outside its closed safe format",
                ));
            }
        }
        Ok(Self {
            schema_version: 1,
            actor_instance_id: actor_instance_id.to_owned(),
            event_id,
            sequence,
            module_id: status.module_id.clone(),
            artifact_id: status.artifact_id.clone(),
            artifact_version: status.artifact_version.clone(),
            build_id: status.build_id.clone(),
            scope: status.scope.clone(),
            boot_id,
            phase: ModuleSupervisorPhase::from(&status.lifecycle),
            effect_certainty: status.effect_certainty,
            stage: status.failure_stage,
            error_code,
            unknown_operation_ids,
            unknown_operation_count: status.unknown_operation_count,
            unknown_operation_ids_truncated: status.unknown_operation_ids_truncated,
        })
    }
}

impl From<&LifecycleState> for ModuleSupervisorPhase {
    fn from(value: &LifecycleState) -> Self {
        match value {
            LifecycleState::WaitingForDemand => Self::WaitingForDemand,
            LifecycleState::WaitingForKernel => Self::WaitingForKernel,
            LifecycleState::Starting { .. } => Self::Starting,
            LifecycleState::ProcessRunning { .. } => Self::Ready,
            LifecycleState::RestartBackoff { .. } => Self::RestartBackoff,
            LifecycleState::ProcessExited { .. } => Self::Exited,
            LifecycleState::OwnerGroupRetained { .. } => Self::OwnerRetained,
            LifecycleState::OwnerIdentityUnknown
            | LifecycleState::ProcessIdentityUnknown { .. } => Self::IdentityUnknown,
            LifecycleState::Completed { .. } => Self::Completed,
            LifecycleState::Isolated { .. } => Self::Isolated,
        }
    }
}

fn validate_token(value: &str, field: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(Error::invalid(format!(
            "module observation {field} is invalid"
        )));
    }
    Ok(())
}
