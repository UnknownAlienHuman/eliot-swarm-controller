//! Native start evidence carried by the bound agent.start response.

use eliot_core::{Error, FailureOrigin};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    AgentStartParams,
    session::{classify_start_failure, failed_start_value},
};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StartPoint {
    BeforeSpawn,
    SpawnRefused,
    NativeRefusalBeforeDispatch,
    AfterDispatch,
    TransportLost,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StartFailureFact {
    pub phase: String,
    pub point: StartPoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_subject: Option<StartNativeSubject>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_pid: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StartNativeSubject {
    pub native_scope: String,
    pub native_ref: String,
}

pub fn is_proven_no_effect(point: StartPoint) -> bool {
    matches!(
        point,
        StartPoint::BeforeSpawn
            | StartPoint::SpawnRefused
            | StartPoint::NativeRefusalBeforeDispatch
    )
}

/// Bind the native facts to the dispatched start. The host decides settlement.
pub fn start_failure(
    params: &AgentStartParams,
    harness: &str,
    phase: &str,
    point: StartPoint,
    error: Error,
    native_subject: Option<(&str, &str)>,
    native_pid: Option<u32>,
) -> eliot_core::Result<Value> {
    let error = classify_start_failure(error, harness, phase);
    let mut value = failed_start_value(params, error);
    value["state"] = Value::String("start_failure".to_owned());
    value["start_failure"] = serde_json::to_value(StartFailureFact {
        phase: phase.to_owned(),
        point,
        native_subject: native_subject.map(|(scope, reference)| StartNativeSubject {
            native_scope: scope.to_owned(),
            native_ref: reference.to_owned(),
        }),
        native_pid,
    })
    .map_err(|_| Error::new("internal", "native start evidence could not be encoded"))?;
    Ok(value)
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
