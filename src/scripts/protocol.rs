use crate::{
    error::{Error, Result},
    model,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::manifest::{
    MAX_CONTROLLER_EFFECT_TEXT_BYTES, MAX_INPUT_BYTES, ScriptBundleRequest, ScriptControllerEffect,
    ScriptValueSchema, validate_script_id,
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
    #[serde(default)]
    pub attempt_id: Option<String>,
    #[serde(default)]
    pub expected_task_revision: Option<i64>,
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
    #[serde(default)]
    pub task_id: Option<String>,
    #[serde(default)]
    pub task_revision: Option<i64>,
    #[serde(default)]
    pub attempt_id: Option<String>,
    pub input: Value,
    /// Closed, immutable grants copied from this exact bundle revision.
    #[serde(default)]
    pub controller_effects: Vec<ScriptControllerEffect>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptResult {
    pub protocol_version: u32,
    pub operation_id: String,
    pub run_id: String,
    pub result: Value,
    /// At most one requested effect, each checked against the retained grant.
    #[serde(default)]
    pub effects: Vec<ScriptEffectRequest>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ScriptEffectRequest {
    pub effect: ScriptControllerEffect,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec: Option<Value>,
}

impl ScriptEffectRequest {
    pub fn validate(&self) -> Result<()> {
        match self.effect {
            ScriptControllerEffect::TaskOwnerMessage
            | ScriptControllerEffect::ManagerNotification
                if self.spec.is_none() =>
            {
                if self.text.trim().is_empty()
                    || self.text.len() > MAX_CONTROLLER_EFFECT_TEXT_BYTES
                    || self.text.contains('\0')
                {
                    return Err(Error::invalid(
                        "controller-effect text must be nonempty and at most 4096 UTF-8 bytes",
                    ));
                }
            }
            ScriptControllerEffect::TaskCreate
                if self.text.is_empty() && self.spec.as_ref().is_some_and(Value::is_object) =>
            {
                let spec = self.spec.as_ref().expect("guard checked spec");
                if model::canonical(spec)?.len() > MAX_INPUT_BYTES {
                    return Err(Error::invalid(
                        "task_create spec exceeds the 256 KiB input limit",
                    ));
                }
            }
            _ => {
                return Err(Error::invalid(
                    "controller-effect fields do not match the selected closed effect",
                ));
            }
        }
        Ok(())
    }
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
        let valid_task_scope = match (&request.attempt_id, request.expected_task_revision) {
            (Some(attempt_id), Some(revision)) => !attempt_id.trim().is_empty() && revision > 0,
            (None, None) => true,
            _ => false,
        };
        if request.expected_script_revision <= 0 || !valid_task_scope {
            return Err(Error::invalid(
                "script Task scope must provide both a nonempty Attempt ID and positive Task revision",
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
