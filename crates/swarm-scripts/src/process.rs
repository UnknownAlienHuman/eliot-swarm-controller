//! Platform-neutral command planning. This module returns arguments and
//! bounded runtime settings; process creation and cancellation stay with the
//! host's process-group owner.

use crate::{
    MAX_ARGUMENT_BYTES, MAX_ARGUMENTS, MAX_ENVIRONMENT_BYTES, MAX_ENVIRONMENT_VALUE_BYTES,
    MAX_INHERITED_ENVIRONMENT, MAX_SCRIPT_DURATION_MS, MAX_STDERR_BYTES, MAX_STDOUT_BYTES, Result,
    ScriptError, canonical_json, schema::InterpreterKind,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeMap;

pub const PROCESS_CONTROL_FAILURE_FILE: &str = "process-control-failure.json";
pub const MAX_PROCESS_CONTROL_FAILURE_BYTES: usize = 4 * 1024;

/// One private, write-once worker receipt for a failed cancellation request.
/// It is evidence only; it never proves process departure or completion.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProcessControlFailure {
    pub schema_version: u32,
    pub run_id: String,
    pub operation_id: String,
    pub token: String,
    pub process_identity: Value,
    pub action: ProcessControlFailureAction,
    pub failure_class: ProcessControlFailureClass,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessControlFailureAction {
    CancelChildren,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProcessControlFailureClass {
    OsControl,
    ProcessInventory,
    ProcessIdentity,
    Other,
}

impl ProcessControlFailure {
    pub fn cancel_children(
        run_id: impl Into<String>,
        operation_id: impl Into<String>,
        token: impl Into<String>,
        process_identity: Value,
        error_code: &str,
    ) -> Self {
        Self {
            schema_version: 1,
            run_id: run_id.into(),
            operation_id: operation_id.into(),
            token: token.into(),
            process_identity,
            action: ProcessControlFailureAction::CancelChildren,
            failure_class: match error_code {
                "IO_ERROR" => ProcessControlFailureClass::OsControl,
                "JOB_INVENTORY" => ProcessControlFailureClass::ProcessInventory,
                "PROCESS_IDENTITY" | "PROCESS_GONE" | "INVALID_PARAMS" => {
                    ProcessControlFailureClass::ProcessIdentity
                }
                _ => ProcessControlFailureClass::Other,
            },
        }
    }

    pub fn validate_for(
        &self,
        run_id: &str,
        operation_id: &str,
        token: &str,
        process_identity: &Value,
    ) -> Result<()> {
        if self.schema_version != 1
            || self.run_id != run_id
            || self.operation_id != operation_id
            || self.token != token
            || self.run_id.trim().is_empty()
            || self.run_id.len() > 128
            || self.operation_id.trim().is_empty()
            || self.operation_id.len() > 128
            || self.token.is_empty()
            || self.token.len() > 128
            || self.process_identity != *process_identity
            || !self.process_identity.is_object()
            || self.process_identity["purpose"] != "script"
            || !matches!(
                self.process_identity["scope"].as_str(),
                Some("windows_job" | "linux_process_group")
            )
            || crate::canonical_json(&serde_json::to_value(self)?)?.len()
                > MAX_PROCESS_CONTROL_FAILURE_BYTES
        {
            return Err(ScriptError::new(
                "SCRIPT_PROCESS_CONTROL_DIAGNOSTIC_INVALID",
            ));
        }
        Ok(())
    }

    /// Manager-safe diagnostic; the private token and native process identity
    /// remain in the protected receipt and are never projected to Operation.
    pub fn manager_projection(&self) -> Result<Value> {
        let identity = crate::canonical_json(&self.process_identity)?;
        Ok(json!({
            "schema_version":1,
            "code":"SCRIPT_PROCESS_CANCEL_FAILED",
            "phase":"cancel_children",
            "status":"control_error_observed",
            "failure_class":self.failure_class,
            "process_scope":self.process_identity["scope"],
            "process_identity_sha256":crate::sha256_hex(identity.as_bytes()),
        }))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProcessPlan {
    pub executable: String,
    pub arguments: Vec<String>,
    pub environment: BTreeMap<String, String>,
    pub timeout_ms: u64,
    pub stdout_limit_bytes: usize,
    pub stderr_limit_bytes: usize,
}

/// Build the exact argv prefix used by the current script runner. No shell
/// command string is generated and this function starts no process.
pub fn plan_process(
    interpreter: InterpreterKind,
    executable: &str,
    entrypoint: &str,
    configured_arguments: &[String],
    environment: BTreeMap<String, String>,
) -> Result<ProcessPlan> {
    if executable.trim().is_empty()
        || executable.contains('\0')
        || entrypoint.trim().is_empty()
        || entrypoint.contains('\0')
        || configured_arguments.len() > MAX_ARGUMENTS
        || configured_arguments
            .iter()
            .any(|argument| argument.len() > MAX_ARGUMENT_BYTES || argument.contains('\0'))
    {
        return Err(ScriptError::new("SCRIPT_COMMAND_INVALID"));
    }
    validate_environment(&environment)?;
    let mut arguments = match interpreter {
        InterpreterKind::Python => vec!["-I".to_owned(), entrypoint.to_owned()],
        InterpreterKind::Powershell => vec![
            "-NoLogo".to_owned(),
            "-NoProfile".to_owned(),
            "-NonInteractive".to_owned(),
            "-File".to_owned(),
            entrypoint.to_owned(),
        ],
    };
    arguments.extend(configured_arguments.iter().cloned());
    Ok(ProcessPlan {
        executable: executable.to_owned(),
        arguments,
        environment,
        timeout_ms: MAX_SCRIPT_DURATION_MS,
        stdout_limit_bytes: MAX_STDOUT_BYTES,
        stderr_limit_bytes: MAX_STDERR_BYTES,
    })
}

fn validate_environment(environment: &BTreeMap<String, String>) -> Result<()> {
    if environment.len() > MAX_INHERITED_ENVIRONMENT + 8
        || environment.iter().any(|(name, value)| {
            name.is_empty()
                || name.to_ascii_uppercase().starts_with("SWARM_")
                || name
                    .bytes()
                    .any(|byte| !(byte.is_ascii_alphanumeric() || byte == b'_'))
                || value.len() > MAX_ENVIRONMENT_VALUE_BYTES
                || value.contains('\0')
        })
        || canonical_json(&serde_json::to_value(environment)?)?.len() > MAX_ENVIRONMENT_BYTES
    {
        return Err(ScriptError::new("SCRIPT_ENVIRONMENT_INVALID"));
    }
    Ok(())
}
