//! Platform-neutral command planning. This module returns arguments and
//! bounded runtime settings; process creation and cancellation stay with the
//! host's process-group owner.

use crate::{
    MAX_ARGUMENT_BYTES, MAX_ARGUMENTS, MAX_ENVIRONMENT_BYTES, MAX_ENVIRONMENT_VALUE_BYTES,
    MAX_INHERITED_ENVIRONMENT, MAX_SCRIPT_DURATION_MS, MAX_STDERR_BYTES, MAX_STDOUT_BYTES, Result,
    ScriptError, canonical_json, schema::InterpreterKind,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

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
