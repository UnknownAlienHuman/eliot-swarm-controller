//! Daemon adapters for the shared, I/O-free Core secret policy.
use eliot_core::{
    Error, MAX_STDERR_DIAGNOSTIC_BYTES, SecretPolicy, StderrDiagnostic, StderrSnapshot,
    StderrSource, bound_stderr_diagnostics,
};
use serde_json::{Value, json};

pub(crate) fn redact_value(value: &mut Value, secrets: &[String]) -> bool {
    SecretPolicy::new(secrets).redact_values(value)
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
    .map(|error| *error.diagnostics)
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
