//! Shared pre-effect start classification (k86, Refs #384).
//! Failures before spawn or native session creation, and native refusals
//! before a turn is dispatched, are definite failed. Anything after dispatch,
//! or with an uncertain transport or cleanup, stays unknown.
//! Per-plugin phase enums stay local; each plugin maps its phases into
//! StartPoint and calls is_proven_no_effect. Reason formatting goes through
//! format_start_reason so persisted reasons stay short non-secret literals.

use eliot_core::{Error, FailureOrigin};

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
}
