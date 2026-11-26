//! Shared pre-effect start classification (k86, Refs #384).
//! Failures before spawn or native session creation, and native refusals
//! before a turn is dispatched, are definite failed. Anything after dispatch,
//! or with an uncertain transport or cleanup, stays unknown.
//! Per-plugin phase enums stay local; each plugin maps its phases into
//! StartPoint and calls is_proven_no_effect. Reason formatting goes through
//! format_start_reason so persisted reasons stay short non-secret literals.

use eliot_core::{Error, FailureOrigin};
use serde_json::Value;

use crate::{
    AgentStartParams,
    session::{classify_start_failure, failed_start_value},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartPoint {
    BeforeSpawn,
    SpawnRefused,
    NativeRefusalBeforeDispatch,
    NativeSessionRemoved,
    AfterDispatch,
    TransportLost,
}

pub fn is_proven_no_effect(point: StartPoint) -> bool {
    matches!(
        point,
        StartPoint::BeforeSpawn
            | StartPoint::SpawnRefused
            | StartPoint::NativeRefusalBeforeDispatch
            | StartPoint::NativeSessionRemoved
    )
}

/// Form the reply from native effect evidence. A known subject on an
/// unresolved start is diagnostic evidence, not a readiness acknowledgement.
pub fn start_failure(
    params: &AgentStartParams,
    harness: &str,
    phase: &str,
    point: StartPoint,
    error: Error,
    native_ref: Option<&str>,
) -> eliot_core::Result<Value> {
    let error = classify_start_failure(error, harness, phase);
    if is_proven_no_effect(point) {
        return Ok(failed_start_value(params, error));
    }
    let error = if let Some(native_ref) = native_ref {
        let message = format!("Native session {native_ref}: {}", error.message);
        error.with_message(message)
    } else {
        error
    };
    Err(error.with_code("unknown"))
}

pub fn format_start_reason(what: &str, code: &str, suffix: &str) -> String {
    format!("{what} ({code}); {suffix}")
}

pub fn harness_unavailable_model(harness: &str, phase: &str, message: &str) -> Error {
    Error::new("native_model_unavailable", message)
        .with_where(format!("plugin.{harness}.start.{phase}"))
        .with_origin(
            FailureOrigin::Harness,
            "native harness refused the request without structured upstream evidence",
        )
}
