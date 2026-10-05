//! Provider-neutral durable dispatch-failure facts.
//!
//! The Store remains the authority for transaction admission, operation rows,
//! authorization, and observation SQL. This module owns the small bounded
//! payload contract shared by the Store's admission writer and readback
//! projection; it never interprets a provider error or claims a native effect.

use serde_json::{Value, json};

const KEYS: [&str; 6] = [
    "schema_version",
    "status",
    "stage",
    "error_code",
    "native_effect",
    "retry_authorized",
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DispatchFailure {
    status: String,
    stage: String,
    error_code: String,
}

impl DispatchFailure {
    pub fn status(&self) -> &str {
        &self.status
    }

    pub fn stage(&self) -> &str {
        &self.stage
    }

    pub fn error_code(&self) -> &str {
        &self.error_code
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DispatchFailureError {
    Shape,
    Schema,
    Effect,
    Retry,
    StageStatus,
    ErrorCode,
}

/// Build the exact closed, pre-dispatch fact accepted by the existing Store
/// admission transaction. Invalid values return a bounded validation error;
/// no raw provider error text crosses this contract.
pub fn not_dispatched_payload(
    status: &str,
    stage: &str,
    error_code: &str,
) -> Result<Value, DispatchFailureError> {
    validate_fields(status, stage, error_code)?;
    Ok(json!({
        "schema_version": 1,
        "status": status,
        "stage": stage,
        "error_code": error_code,
        "native_effect": "not_dispatched",
        "retry_authorized": false,
    }))
}

/// Validate the persisted six-field failure fact before projecting it to an
/// operator or manager. Exact shape and closed effect are required.
pub fn validate(value: &Value) -> Result<DispatchFailure, DispatchFailureError> {
    let object = value.as_object().ok_or(DispatchFailureError::Shape)?;
    if object.len() != KEYS.len() || KEYS.iter().any(|key| !object.contains_key(*key)) {
        return Err(DispatchFailureError::Shape);
    }
    if value["schema_version"].as_i64() != Some(1) {
        return Err(DispatchFailureError::Schema);
    }
    if value["native_effect"] != "not_dispatched" {
        return Err(DispatchFailureError::Effect);
    }
    if value["retry_authorized"] != false {
        return Err(DispatchFailureError::Retry);
    }
    let status = value["status"].as_str().unwrap_or_default();
    let stage = value["stage"].as_str().unwrap_or_default();
    let error_code = value["error_code"].as_str().unwrap_or_default();
    validate_fields(status, stage, error_code)?;
    Ok(DispatchFailure {
        status: status.to_owned(),
        stage: stage.to_owned(),
        error_code: error_code.to_owned(),
    })
}

fn validate_fields(
    status: &str,
    stage: &str,
    error_code: &str,
) -> Result<(), DispatchFailureError> {
    let valid_pair = matches!(
        (status, stage),
        ("selection_error", "runtime_command_select")
            | ("rejected_before_dispatch", "opening_actor_validate")
    );
    if !valid_pair {
        return Err(DispatchFailureError::StageStatus);
    }
    if !safe_error_code(error_code) {
        return Err(DispatchFailureError::ErrorCode);
    }
    Ok(())
}

fn safe_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value.as_bytes().first().is_some_and(u8::is_ascii_uppercase)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
}
