//! Daemon adapters for the shared, I/O-free Core secret policy.
use eliot_core::{
    Error, MAX_STDERR_DIAGNOSTIC_BYTES, SecretPolicy, StderrDiagnostic, StderrSnapshot,
    StderrSource, bound_stderr_diagnostics,
};
use serde_json::{Value, json};

pub(crate) fn redact_value(value: &mut Value, secrets: &[String]) -> bool {
    SecretPolicy::new(secrets).redact_values(value)
}

pub(crate) fn redact_error(mut error: Error, secrets: &[String]) -> Error {
    let policy = SecretPolicy::new(secrets);
    error.message = policy.redact_text(&error.message);
    redact_tails(&mut error.diagnostics, &policy);
    bound_stderr_diagnostics(&mut error.diagnostics, MAX_STDERR_DIAGNOSTIC_BYTES);
    error
}

fn take_diagnostics(value: &mut Value) -> Vec<StderrDiagnostic> {
    let Some(raw) = value
        .as_object_mut()
        .and_then(|object| object.remove("diagnostics"))
    else {
        return Vec::new();
    };
    // Reuse Core's tolerant, bounded wire parser. Invalid optional metadata is
    // omitted, never allowed to invalidate the primary command/error outcome.
    serde_json::from_value::<Error>(
        json!({"code": "diagnostic", "message": "", "diagnostics": raw}),
    )
    .map(|error| error.diagnostics)
    .unwrap_or_default()
}

fn redact_tails(values: &mut [StderrDiagnostic], policy: &SecretPolicy) {
    for value in values {
        value.snapshot.text = policy.redact_text(&value.snapshot.text);
    }
}

fn put_diagnostics(value: &mut Value, mut tails: Vec<StderrDiagnostic>, max_encoded_bytes: usize) {
    let base = serde_json::to_vec(value).map_or(usize::MAX, |bytes| bytes.len());
    let budget = max_encoded_bytes
        .saturating_sub(base.saturating_add(",\"diagnostics\":".len()))
        .min(MAX_STDERR_DIAGNOSTIC_BYTES);
    bound_stderr_diagnostics(&mut tails, budget);
    if !tails.is_empty()
        && let Ok(tails) = serde_json::to_value(tails)
        && let Some(object) = value.as_object_mut()
    {
        object.insert("diagnostics".to_owned(), tails);
    }
}

/// Redact before calculating the remaining encoded outcome budget. Optional
/// stderr never makes an otherwise valid <=64-KiB outcome fail Store admission.
pub(crate) fn redact_outcome(value: &mut Value, secrets: &[String]) {
    let mut tails = take_diagnostics(value);
    let policy = SecretPolicy::new(secrets);
    policy.redact_values(value);
    redact_tails(&mut tails, &policy);
    put_diagnostics(value, tails, eliot_plugin::MAX_EVENT_BYTES);
}

pub(crate) fn attach_stderr(
    value: &mut Value,
    source: StderrSource,
    snapshot: StderrSnapshot,
    secrets: &[String],
) {
    let mut tails = take_diagnostics(value);
    let policy = SecretPolicy::new(secrets);
    policy.redact_values(value);
    redact_tails(&mut tails, &policy);
    // Capture snapshots are already sanitized before retention. Replace only
    // this source; a Harness tail received from the plugin remains distinct.
    tails.retain(|tail| tail.source != source);
    tails.push(StderrDiagnostic { source, snapshot });
    put_diagnostics(value, tails, eliot_plugin::MAX_EVENT_BYTES);
}

pub(crate) fn bound_outcome_diagnostics(value: &mut Value, max_encoded_bytes: usize) {
    let tails = take_diagnostics(value);
    put_diagnostics(value, tails, max_encoded_bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escaped_short_secret_diagnostics_preserve_primary_outcome_and_fit() {
        let text = "\u{1}x".repeat(1024);
        let snapshot = StderrSnapshot {
            text,
            eof: true,
            ..StderrSnapshot::default()
        };
        let diagnostics = vec![
            StderrDiagnostic {
                source: StderrSource::Plugin,
                snapshot: snapshot.clone(),
            },
            StderrDiagnostic {
                source: StderrSource::Harness,
                snapshot,
            },
        ];
        let mut outcome = json!({"code": "unknown", "cause": "plugin_transport_unavailable",
            "state": "unknown", "message": "unchanged", "diagnostics": diagnostics});
        redact_outcome(&mut outcome, &["x".to_owned()]);
        assert_eq!(outcome["code"], "unknown");
        assert_eq!(outcome["message"], "unchanged");
        assert_eq!(outcome["cause"], "plugin_transport_unavailable");
        assert_eq!(outcome["state"], "unknown");
        assert!(serde_json::to_vec(&outcome).unwrap().len() <= eliot_plugin::MAX_EVENT_BYTES);
        let tails = outcome["diagnostics"].as_array().unwrap();
        assert_eq!(tails.len(), 2);
        assert!(
            tails
                .iter()
                .all(|tail| tail["snapshot"]["truncated"] == true)
        );
        assert!(
            tails
                .iter()
                .all(|tail| !tail["snapshot"]["text"].as_str().unwrap().contains('x'))
        );
        // Exercise actual settlement without starting a process/model. Optional
        // diagnostics must not turn this Unknown result into a Store failure.
        let mut store = eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap();
        let id = ulid::Ulid::new();
        let now = i64::try_from(id.timestamp_ms()).unwrap();
        let id = id.to_string();
        store
            .submit(
                "owner",
                eliot_core::SubmitCommand {
                    id: id.clone(),
                    target: None,
                    verb: "launch".to_owned(),
                    args: json!({"project": "diagnostic-budget", "workspace": "synthetic-workspace"}),
                    expected_generation: Some(0),
                    expected_turn_id: None,
                    request_id: None,
                },
                now,
            )
            .unwrap();
        store.dispatch(&id, now).unwrap();
        let settled = store
            .settle(&id, eliot_core::CommandState::Unknown, Some(outcome), now)
            .unwrap();
        assert_eq!(settled.value.state, eliot_core::CommandState::Unknown);
        let outcome = settled.value.outcome.unwrap();
        assert_eq!(outcome["code"], "unknown");
        assert_eq!(outcome["message"], "unchanged");
    }

    #[test]
    fn diagnostics_cannot_overflow_an_already_full_primary_outcome() {
        let mut outcome =
            json!({"code": "unknown", "padding": "p".repeat(eliot_plugin::MAX_EVENT_BYTES - 35)});
        let original = outcome.clone();
        attach_stderr(
            &mut outcome,
            StderrSource::Plugin,
            StderrSnapshot {
                text: "diagnostic".to_owned(),
                eof: true,
                ..StderrSnapshot::default()
            },
            &[],
        );
        assert_eq!(outcome, original);
        assert!(serde_json::to_vec(&outcome).unwrap().len() <= eliot_plugin::MAX_EVENT_BYTES);
    }
}
