//! Shared pre-effect start classification (k86, Refs #384).
//! Failures before spawn or native session creation, and native refusals
//! before a turn is dispatched, are definite failed. Anything after dispatch,
//! or with an uncertain transport or cleanup, stays unknown.
//! Per-plugin phase enums stay local; each plugin maps its phases into
//! StartPoint and calls is_proven_no_effect. Reason formatting goes through
//! format_start_reason so persisted reasons stay short non-secret literals.

use eliot_core::{Error, FailureOrigin, Result};
use serde_json::Value;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartPoint {
    BeforeSpawn,
    SpawnRefused,
    NativeRefusalBeforeDispatch,
    AfterDispatch,
    TransportLost,
}

pub fn is_proven_no_effect(point: StartPoint) -> bool {
    matches!(
        point,
        StartPoint::BeforeSpawn
            | StartPoint::SpawnRefused
            | StartPoint::NativeRefusalBeforeDispatch
    )
}

pub fn format_start_reason(what: &str, code: &str, suffix: &str) -> String {
    format!("{what} ({code}); {suffix}")
}

/// How one retained start failure replays. `Uncertain` keeps the historical
/// error replay (the daemon settles by error code, and a native effect may
/// have happened). `ProvenNoEffect` carries the failed-state result value:
/// no native session or prompt was ever dispatched, so the launch is
/// definitely failed.
#[derive(Clone, Debug)]
pub enum SettledStartFailure {
    Uncertain(Error),
    ProvenNoEffect(Value),
}

impl SettledStartFailure {
    /// Replay the retained outcome: the uncertain error, or the proven
    /// failed-state value.
    pub fn replay(&self) -> Result<Value> {
        match self {
            Self::Uncertain(error) => Err(error.clone()),
            Self::ProvenNoEffect(value) => Ok(value.clone()),
        }
    }

    /// Whether this failure proves no native effect happened.
    pub fn is_proven(&self) -> bool {
        matches!(self, Self::ProvenNoEffect(_))
    }
}

pub fn harness_unavailable_model(harness: &str, phase: &str, message: &str) -> Error {
    Error::new("native_model_unavailable", message)
        .with_where(format!("plugin.{harness}.start.{phase}"))
        .with_origin(
            FailureOrigin::Harness,
            "native harness refused the request without structured upstream evidence",
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pre_effect_points_are_proven_post_effect_and_lost_are_not() {
        assert!(is_proven_no_effect(StartPoint::BeforeSpawn));
        assert!(is_proven_no_effect(StartPoint::SpawnRefused));
        assert!(is_proven_no_effect(StartPoint::NativeRefusalBeforeDispatch));
        assert!(!is_proven_no_effect(StartPoint::AfterDispatch));
        assert!(!is_proven_no_effect(StartPoint::TransportLost));
    }

    #[test]
    fn reason_format_is_typed_static_plus_code() {
        let reason = format_start_reason(
            "Codex model probe failed",
            "native_model_unavailable",
            "no thread or input was sent",
        );
        assert!(reason.contains("native_model_unavailable"));
        assert!(reason.contains("no thread or input was sent"));
    }

    #[test]
    fn unavailable_model_carries_harness_origin() {
        let error = harness_unavailable_model("codex", "model_list", "model is missing");
        assert_eq!(error.code, "native_model_unavailable");
        assert_eq!(error.origin, Some(FailureOrigin::Harness));
    }

    #[test]
    fn settled_start_failure_replays_its_retained_outcome() {
        let failure = SettledStartFailure::Uncertain(Error::new("unknown", "lost reply"));
        assert!(failure.replay().is_err());
        assert!(!failure.is_proven());
        let value = serde_json::json!({"state": "failed"});
        let proven = SettledStartFailure::ProvenNoEffect(value.clone());
        assert_eq!(proven.replay().unwrap(), value);
        assert!(proven.is_proven());
    }
}
