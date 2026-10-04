use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::manifest::{
    MAX_INPUT_BYTES, ScriptBundleRequest, ScriptValueSchema, validate_script_id,
};

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterRequest {
    pub client_request_id: String,
    pub bundle: ScriptBundleRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReviseRequest {
    pub client_request_id: String,
    pub script_id: String,
    pub expected_revision: i64,
    pub bundle: ScriptBundleRequest,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidateRequest {
    pub script_id: String,
    pub revision: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ActivateRequest {
    pub client_request_id: String,
    pub script_id: String,
    pub revision: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RunRequest {
    pub client_request_id: String,
    pub script_id: String,
    pub expected_script_revision: i64,
    pub attempt_id: String,
    pub expected_task_revision: i64,
    pub input: Value,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GetRequest {
    pub script_id: String,
    #[serde(default)]
    pub revision: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ListRequest {
    #[serde(default)]
    pub after: Option<i64>,
    #[serde(default)]
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptInvocation {
    pub protocol_version: u32,
    pub operation_id: String,
    pub run_id: String,
    pub script_id: String,
    pub script_revision: i64,
    pub task_id: String,
    pub task_revision: i64,
    pub attempt_id: String,
    pub input: Value,
    /// O6 first slice is direct-only and grants no controller API methods.
    pub controller_effects: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptResult {
    pub protocol_version: u32,
    pub operation_id: String,
    pub run_id: String,
    pub result: Value,
}

impl RegisterRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["client_request_id", "bundle"])?;
        model::text(value, "client_request_id")?;
        let request: Self = serde_json::from_value(value.clone())?;
        request.bundle.validate()?;
        Ok(request)
    }
}

impl ReviseRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "client_request_id",
                "script_id",
                "expected_revision",
                "bundle",
            ],
        )?;
        model::text(value, "client_request_id")?;
        let request: Self = serde_json::from_value(value.clone())?;
        validate_script_id(&request.script_id)?;
        if request.expected_revision <= 0 || request.bundle.script_id != request.script_id {
            return Err(Error::invalid(
                "revise script ID and expected revision must match the bundle",
            ));
        }
        request.bundle.validate()?;
        Ok(request)
    }
}

impl ValidateRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["script_id", "revision"])?;
        let request: Self = serde_json::from_value(value.clone())?;
        validate_script_id(&request.script_id)?;
        if request.revision <= 0 {
            return Err(Error::invalid("revision must be positive"));
        }
        Ok(request)
    }
}

impl ActivateRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["client_request_id", "script_id", "revision"])?;
        model::text(value, "client_request_id")?;
        let request: Self = serde_json::from_value(value.clone())?;
        validate_script_id(&request.script_id)?;
        if request.revision <= 0 {
            return Err(Error::invalid("revision must be positive"));
        }
        Ok(request)
    }
}

impl RunRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(
            value,
            &[
                "client_request_id",
                "script_id",
                "expected_script_revision",
                "attempt_id",
                "expected_task_revision",
                "input",
            ],
        )?;
        model::text(value, "client_request_id")?;
        let request: Self = serde_json::from_value(value.clone())?;
        validate_script_id(&request.script_id)?;
        if request.expected_script_revision <= 0
            || request.expected_task_revision <= 0
            || request.attempt_id.trim().is_empty()
        {
            return Err(Error::invalid(
                "script run requires positive expected revisions and an Attempt ID",
            ));
        }
        if model::canonical(&request.input)?.len() > MAX_INPUT_BYTES {
            return Err(Error::invalid("script input exceeds 256 KiB"));
        }
        Ok(request)
    }
}

impl GetRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["script_id", "revision"])?;
        let request: Self = serde_json::from_value(value.clone())?;
        validate_script_id(&request.script_id)?;
        if request.revision.is_some_and(|revision| revision <= 0) {
            return Err(Error::invalid("revision must be positive"));
        }
        Ok(request)
    }
}

impl ListRequest {
    pub fn parse(value: &Value) -> Result<Self> {
        model::fields(value, &["after", "limit"])?;
        let request: Self = serde_json::from_value(value.clone())?;
        if request.after.is_some_and(|after| after < 0)
            || request
                .limit
                .is_some_and(|limit| !(1..=100).contains(&limit))
        {
            return Err(Error::invalid("script list cursor/limit is out of range"));
        }
        Ok(request)
    }
}

pub fn validate_input(schema: &ScriptValueSchema, input: &Value) -> Result<()> {
    if model::canonical(input)?.len() > MAX_INPUT_BYTES {
        return Err(Error::invalid("script input exceeds 256 KiB"));
    }
    schema.validate_value(input)
}
