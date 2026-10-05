//! Pure ScriptResult validation and terminal result projection. Artifact
//! writes, durable state changes, cancellation, and readback remain host-owned.

use crate::{
    MAX_CONTROLLER_EFFECTS, MAX_RESULT_BYTES, Result, ScriptError,
    protocol::ScriptResult,
    schema::{ScriptControllerEffect, ScriptValueSchema},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessOutcome<'a> {
    pub stdout: &'a [u8],
    pub exit: ProcessExit,
    pub timed_out: bool,
    pub output_overflow: bool,
    pub input_failed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessExit {
    /// The owner has not yet observed process departure. No terminal result may
    /// be projected from this state.
    Unknown,
    /// The process was reaped; `None` means it exited without a numeric code.
    Observed { exit_code: Option<i32> },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct CompletionProjection {
    pub state: String,
    pub exit_code: Option<i32>,
    pub error_code: Option<String>,
    pub result: Option<Value>,
    pub controller_effects: Vec<crate::protocol::ScriptEffectRequest>,
}

pub fn project_completion(
    outcome: ProcessOutcome<'_>,
    operation_id: &str,
    run_id: &str,
    result_schema: &ScriptValueSchema,
    granted_effects: &[ScriptControllerEffect],
) -> Result<Option<CompletionProjection>> {
    let ProcessExit::Observed { exit_code } = outcome.exit else {
        return Ok(None);
    };
    let mut error_code = if outcome.timed_out {
        Some("SCRIPT_TIMEOUT")
    } else if outcome.output_overflow {
        Some("SCRIPT_OUTPUT_LIMIT")
    } else if outcome.input_failed {
        Some("SCRIPT_INPUT_FAILED")
    } else if exit_code != Some(0) {
        Some("SCRIPT_EXIT_NONZERO")
    } else {
        None
    };

    let mut result_value = None;
    let mut controller_effects = Vec::new();
    if error_code.is_none() {
        match parse_result(
            outcome.stdout,
            operation_id,
            run_id,
            result_schema,
            granted_effects,
        ) {
            Ok((result, effects)) => {
                result_value = Some(result);
                controller_effects = effects;
            }
            Err(error) => error_code = Some(error.code()),
        }
    }
    Ok(Some(CompletionProjection {
        state: if error_code.is_none() {
            "completed"
        } else {
            "failed"
        }
        .to_owned(),
        exit_code,
        error_code: error_code.map(str::to_owned),
        result: result_value,
        controller_effects,
    }))
}

fn parse_result(
    bytes: &[u8],
    operation_id: &str,
    run_id: &str,
    result_schema: &ScriptValueSchema,
    granted_effects: &[ScriptControllerEffect],
) -> Result<(Value, Vec<crate::protocol::ScriptEffectRequest>)> {
    result_schema.validate_definition(0)?;
    if bytes.is_empty() || bytes.len() > MAX_RESULT_BYTES {
        return Err(ScriptError::new("SCRIPT_RESULT_INVALID"));
    }
    let result: ScriptResult =
        serde_json::from_slice(bytes).map_err(|_| ScriptError::new("SCRIPT_RESULT_INVALID"))?;
    if result.protocol_version != 1
        || result.operation_id != operation_id
        || result.run_id != run_id
    {
        return Err(ScriptError::new("SCRIPT_RESULT_INVALID"));
    }
    result_schema.validate_value(&result.result)?;
    if result.effects.len() > MAX_CONTROLLER_EFFECTS {
        return Err(ScriptError::new("SCRIPT_EFFECTS_INVALID"));
    }
    for effect in &result.effects {
        if !granted_effects.contains(&effect.effect) {
            return Err(ScriptError::new("SCRIPT_EFFECTS_UNGRANTED"));
        }
        effect.validate()?;
    }
    Ok((result.result, result.effects))
}
