use crate::{
    MAX_INPUT_BYTES, MAX_INVOCATION_BYTES, Result, ScriptError, canonical_json,
    schema::{ScriptControllerEffect, ScriptValueSchema, validate_effect_text},
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptRunRequest {
    pub client_request_id: String,
    pub script_id: String,
    pub expected_script_revision: i64,
    #[serde(default)]
    pub attempt_id: Option<String>,
    #[serde(default)]
    pub expected_task_revision: Option<i64>,
    pub input: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptInvocation {
    pub protocol_version: u32,
    pub operation_id: String,
    pub run_id: String,
    pub script_id: String,
    pub script_revision: i64,
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub task_revision: Option<i64>,
    #[serde(default)]
    pub attempt_id: Option<String>,
    pub input: Value,
    #[serde(default)]
    pub controller_effects: Vec<ScriptControllerEffect>,
}

impl ScriptInvocation {
    pub fn validate(&self) -> Result<()> {
        crate::schema::validate_script_id(&self.script_id)?;
        let scope_is_complete = match (&self.task_id, self.task_revision, &self.attempt_id) {
            (None, None, None) => true,
            (Some(_), Some(revision), Some(_)) => revision > 0,
            _ => false,
        };
        if self.protocol_version != 1
            || self.operation_id.trim().is_empty()
            || self.operation_id.len() > 128
            || self.run_id.trim().is_empty()
            || self.run_id.len() > 128
            || self.script_revision <= 0
            || !scope_is_complete
        {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        if self
            .task_id
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
            || self
                .attempt_id
                .as_ref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        validate_input_bytes(&self.input)?;
        crate::schema::validate_effect_grants(&self.controller_effects)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptResult {
    pub protocol_version: u32,
    pub operation_id: String,
    pub run_id: String,
    pub result: Value,
    #[serde(default)]
    pub effects: Vec<ScriptEffectRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScriptEffectRequest {
    pub effect: ScriptControllerEffect,
    pub text: String,
}

impl ScriptEffectRequest {
    pub fn validate(&self) -> Result<()> {
        validate_effect_text(&self.text)
    }
}

impl ScriptRunRequest {
    pub fn validate(&self) -> Result<()> {
        crate::schema::validate_script_id(&self.script_id)?;
        if self.client_request_id.is_empty()
            || self.client_request_id.len() > 128
            || self
                .client_request_id
                .bytes()
                .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
            || self.expected_script_revision <= 0
            || !matches!(
                (&self.attempt_id, self.expected_task_revision),
                (None, None) | (Some(_), Some(_))
            )
            || self
                .attempt_id
                .as_ref()
                .is_some_and(|attempt| attempt.trim().is_empty())
            || self
                .expected_task_revision
                .is_some_and(|revision| revision <= 0)
        {
            return Err(ScriptError::new("INVALID_PARAMS"));
        }
        validate_input_bytes(&self.input)
    }
}

pub fn validate_input_bytes(input: &Value) -> Result<()> {
    if canonical_json(input)?.len() > MAX_INPUT_BYTES {
        return Err(ScriptError::new("SCRIPT_INPUT_TOO_LARGE"));
    }
    Ok(())
}

pub fn validate_invocation_size(invocation: &ScriptInvocation) -> Result<Vec<u8>> {
    invocation.validate()?;
    let bytes = canonical_json(&serde_json::to_value(invocation)?)?.into_bytes();
    if bytes.len() > MAX_INVOCATION_BYTES {
        return Err(ScriptError::new("SCRIPT_INVOCATION_TOO_LARGE"));
    }
    Ok(bytes)
}

pub fn validate_input(schema: &ScriptValueSchema, input: &Value) -> Result<()> {
    validate_input_bytes(input)?;
    schema.validate_value(input)
}
