//! Immutable literal-secret policy shared by admission, transports and logging.

use serde_json::Value;

const MARKER: &[u8] = b"[REDACTED]";

#[derive(Clone, Default)]
pub struct SecretPolicy {
    values: Vec<String>,
}

impl SecretPolicy {
    pub fn new<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut values: Vec<_> = values
            .into_iter()
            .map(|value| value.as_ref().to_owned())
            .filter(|value| !value.is_empty())
            .collect();
        values.sort_by(|left, right| right.len().cmp(&left.len()).then_with(|| left.cmp(right)));
        values.dedup();
        Self { values }
    }

    /// Raw literal detection, including literals inside marker-looking input.
    pub fn contains_text(&self, text: &str) -> bool {
        self.values.iter().any(|secret| text.contains(secret))
    }

    /// Complete redaction markers are preserved across repeated sanitization.
    pub fn redact_text(&self, text: &str) -> String {
        let (safe, _) = self.redact_prefix(text.as_bytes(), true);
        String::from_utf8_lossy(&safe).into_owned()
    }

    /// Inspect both string keys and string values; other JSON scalars stay typed.
    pub fn contains_value(&self, value: &Value) -> bool {
        match value {
            Value::String(text) => self.contains_text(text),
            Value::Array(values) => values.iter().any(|value| self.contains_value(value)),
            Value::Object(values) => values
                .iter()
                .any(|(key, value)| self.contains_text(key) || self.contains_value(value)),
            _ => false,
        }
    }

    /// Redact string values, never object keys. Returns whether values changed.
    pub fn redact_values(&self, value: &mut Value) -> bool {
        match value {
            Value::String(text) => {
                let safe = self.redact_text(text);
                if safe == *text {
                    return false;
                }
                *text = safe;
                true
            }
            Value::Array(values) => {
                let mut changed = false;
                for value in values {
                    changed |= self.redact_values(value);
                }
                changed
            }
            Value::Object(values) => {
                let mut changed = false;
                for value in values.values_mut() {
                    changed |= self.redact_values(value);
                }
                changed
            }
            _ => false,
        }
    }

    /// Return safe output and the number of ORIGINAL bytes consumed. The caller
    /// must retain bytes[consumed..] for the next chunk and call with eof=true at
    /// finalization. A possible unfinished literal or marker stays pending;
    /// complete markers are preserved without scanning their contents. Retain/truncate only
    /// the returned safe bytes, never the pending original suffix.
    pub fn redact_prefix(&self, bytes: &[u8], eof: bool) -> (Vec<u8>, usize) {
        let mut safe = Vec::with_capacity(bytes.len());
        let mut consumed = 0;
        while consumed < bytes.len() {
            let rest = &bytes[consumed..];
            if rest.starts_with(MARKER) {
                safe.extend_from_slice(MARKER);
                consumed += MARKER.len();
                continue;
            }
            if !eof && rest.len() < MARKER.len() && MARKER.starts_with(rest) {
                break;
            }
            if !eof
                && self
                    .values
                    .iter()
                    .any(|secret| secret.len() > rest.len() && secret.as_bytes().starts_with(rest))
            {
                break;
            }
            if let Some(secret) = self
                .values
                .iter()
                .find(|secret| rest.starts_with(secret.as_bytes()))
            {
                safe.extend_from_slice(MARKER);
                consumed += secret.len();
            } else {
                safe.push(bytes[consumed]);
                consumed += 1;
            }
        }
        (safe, consumed)
    }
}

#[cfg(test)]
mod tests {
    use super::SecretPolicy;
    use crate::{
        CommandState, Error, MAX_STDERR_BYTES, MAX_STDERR_DIAGNOSTIC_BYTES, StderrSnapshot,
        StderrSource, bound_stderr_diagnostics, classify_dispatched_error,
    };
    use serde_json::json;

    #[test]
    fn empty_short_overlap_and_generated_markers() {
        let empty = SecretPolicy::new([""]);
        assert!(!empty.contains_text("anything"));
        assert_eq!(empty.redact_text("anything"), "anything");
        let policy = SecretPolicy::new(["", "x", "token", "token-long", "token", "REDACTED"]);
        assert!(policy.contains_text("prefix-x"));
        assert_eq!(
            policy.redact_text("token-long token x REDACTED"),
            "[REDACTED] [REDACTED] [REDACTED] [REDACTED]"
        );
    }

    #[test]
    fn detects_keys_but_redacts_only_values_recursively() {
        let policy = SecretPolicy::new(["key-secret", "value-secret", "x"]);
        let mut value = json!({"key-secret": {"items": ["value-secret", "x", "safe", 123, true]}});
        assert!(policy.contains_value(&value));
        assert!(policy.redact_values(&mut value));
        assert_eq!(
            value,
            json!({"key-secret": {"items": ["[REDACTED]", "[REDACTED]", "safe", 123, true]}})
        );
        assert!(policy.contains_value(&value));
        assert!(!policy.redact_values(&mut value));
        let key_only = json!({"key-secret": "safe"});
        assert!(policy.contains_value(&key_only));
        assert!(!policy.contains_value(&json!({"safe": [false, 123]})));
    }

    #[test]
    fn stream_waits_for_a_possible_longer_literal() {
        let policy = SecretPolicy::new(["ab", "abcdef"]);
        assert_eq!(policy.redact_prefix(b"abc", false), (Vec::new(), 0));
        assert_eq!(
            policy.redact_prefix(b"abcdef!", false),
            (b"[REDACTED]!".to_vec(), 7)
        );
        assert_eq!(
            policy.redact_prefix(b"ab", true),
            (b"[REDACTED]".to_vec(), 2)
        );
    }

    #[test]
    fn every_byte_split_preserves_unicode_and_hides_cross_chunk_secrets() {
        let policy = SecretPolicy::new(["密🔑token", "token", "x"]);
        let input = "雪:密🔑token:x:終".as_bytes();
        let expected = policy
            .redact_text(std::str::from_utf8(input).unwrap())
            .into_bytes();
        for split in 0..=input.len() {
            let (mut safe, consumed) = policy.redact_prefix(&input[..split], false);
            let mut pending = input[consumed..split].to_vec();
            pending.extend_from_slice(&input[split..]);
            let (last, consumed) = policy.redact_prefix(&pending, true);
            assert_eq!(consumed, pending.len());
            safe.extend_from_slice(&last);
            assert_eq!(safe, expected, "split={split}");
        }
    }

    #[test]
    fn repeated_passes_and_chunk_split_markers_are_idempotent() {
        let policy = SecretPolicy::new(["secret", "R", "A"]);
        let once = policy.redact_text("secret R A [REDACTED] R/A");
        assert_eq!(policy.redact_text(&once), once);
        assert_eq!(policy.redact_text(&policy.redact_text(&once)), once);
        // Detection remains a raw literal check for immutable/untrusted data.
        assert!(policy.contains_text("[REDACTED]"));
        assert!(policy.contains_value(&json!({"R":"[REDACTED]"})));
        for split in 0..=once.len() {
            let (mut safe, consumed) = policy.redact_prefix(&once.as_bytes()[..split], false);
            let mut pending = once.as_bytes()[consumed..split].to_vec();
            pending.extend_from_slice(&once.as_bytes()[split..]);
            let (last, consumed) = policy.redact_prefix(&pending, true);
            assert_eq!(consumed, pending.len());
            safe.extend_from_slice(&last);
            assert_eq!(safe, once.as_bytes(), "split={split}");
        }
        let mut pending = Vec::new();
        let mut safe = Vec::new();
        for byte in once.as_bytes() {
            pending.push(*byte);
            let (prefix, consumed) = policy.redact_prefix(&pending, false);
            safe.extend_from_slice(&prefix);
            pending.drain(..consumed);
        }
        let (last, consumed) = policy.redact_prefix(&pending, true);
        assert_eq!(consumed, pending.len());
        safe.extend_from_slice(&last);
        assert_eq!(safe, once.as_bytes());
    }

    #[test]
    fn bytes_are_redacted_before_tail_retention() {
        let secret = "credential-fragment-".repeat(1000);
        let policy = SecretPolicy::new([secret.as_str()]);
        let input = format!("before:{secret}:after");
        let mut pending = Vec::new();
        let mut retained = Vec::new();
        for chunk in input.as_bytes().chunks(97) {
            pending.extend_from_slice(chunk);
            let (safe, consumed) = policy.redact_prefix(&pending, false);
            retained.extend_from_slice(&safe);
            pending.drain(..consumed);
            if retained.len() > 16 {
                retained.drain(..retained.len() - 16);
            }
        }
        let (safe, consumed) = policy.redact_prefix(&pending, true);
        assert_eq!(consumed, pending.len());
        retained.extend_from_slice(&safe);
        if retained.len() > 16 {
            retained.drain(..retained.len() - 16);
        }
        assert_eq!(retained, b"[REDACTED]:after");
    }

    #[test]
    fn error_copies_keep_both_stderr_sources_and_primary_meaning() {
        let plugin = StderrSnapshot {
            text: "plugin tail".to_owned(),
            eof: true,
            ..Default::default()
        };
        let harness = StderrSnapshot {
            text: "harness tail".to_owned(),
            read_error: true,
            ..Default::default()
        };
        let error = Error::new("unavailable", "response lost")
            .with_stderr(StderrSource::Plugin, plugin.clone())
            .with_stderr(StderrSource::Harness, harness.clone());
        assert_eq!(error.diagnostics.len(), 2);
        assert_eq!(error.code, "unavailable");
        assert_eq!(error.message, "response lost");
        assert_eq!(
            classify_dispatched_error(&error.code),
            CommandState::Unknown
        );
        let copied = error
            .clone()
            .with_code("unknown")
            .with_message("safe message");
        assert_eq!(copied.diagnostics, error.diagnostics);
        assert_eq!(
            serde_json::from_value::<Error>(serde_json::to_value(&error).unwrap()).unwrap(),
            error
        );
        let replaced = error.with_stderr(StderrSource::Plugin, StderrSnapshot::default());
        assert_eq!(replaced.diagnostics.len(), 2);
        assert_eq!(
            replaced
                .diagnostics
                .iter()
                .find(|value| value.source == StderrSource::Harness)
                .unwrap()
                .snapshot,
            harness
        );
        assert_eq!(
            serde_json::to_value(Error::new("unknown", "safe")).unwrap(),
            json!({"code":"unknown","message":"safe"})
        );
    }

    #[test]
    fn encoded_control_bytes_and_short_secret_expansion_cannot_break_settlement() {
        let policy = SecretPolicy::new(["s"]);
        let safe = policy.redact_text(&"\0s\n".repeat(MAX_STDERR_BYTES / 3));
        assert!(safe.len() > MAX_STDERR_BYTES);
        let snapshot = StderrSnapshot {
            text: safe,
            eof: true,
            ..Default::default()
        };
        let mut error = Error::new("unavailable", "response lost")
            .with_stderr(StderrSource::Plugin, snapshot.clone())
            .with_stderr(StderrSource::Harness, snapshot);
        assert_eq!(error.diagnostics.len(), 2);
        assert!(
            error
                .diagnostics
                .iter()
                .all(|value| value.snapshot.truncated
                    && value.snapshot.text.len() <= MAX_STDERR_BYTES
                    && !policy.contains_text(&value.snapshot.text))
        );
        assert!(
            serde_json::to_vec(&error.diagnostics).unwrap().len() <= MAX_STDERR_DIAGNOSTIC_BYTES
        );
        assert!(serde_json::to_vec(&error).unwrap().len() <= 64 * 1024);
        assert_eq!(error.code, "unavailable");
        assert_eq!(error.message, "response lost");
        assert_eq!(
            classify_dispatched_error(&error.code),
            CommandState::Unknown
        );
        bound_stderr_diagnostics(&mut error.diagnostics, 512);
        assert!(serde_json::to_vec(&error.diagnostics).unwrap().len() <= 512);
        assert_eq!(error.diagnostics.len(), 2);
        let large_primary = error.with_message("m".repeat(64 * 1024 - 50));
        assert!(serde_json::to_vec(&large_primary).unwrap().len() <= 64 * 1024);
        assert_eq!(large_primary.message.len(), 64 * 1024 - 50);
        assert_eq!(large_primary.code, "unavailable");
        assert!(large_primary.diagnostics.is_empty());
    }

    #[test]
    fn malformed_optional_diagnostics_do_not_replace_the_primary_error() {
        for diagnostics in [
            json!("not an array"),
            json!([{"source":"unknown","snapshot":{}}]),
            json!([{"source":"plugin","snapshot":{"text":7,"truncated":false,"eof":false,"read_error":false}}]),
        ] {
            let error: Error = serde_json::from_value(
                json!({"code":"unavailable","message":"response lost","diagnostics":diagnostics}),
            )
            .unwrap();
            assert_eq!(error.code, "unavailable");
            assert_eq!(error.message, "response lost");
            assert!(error.diagnostics.is_empty());
        }
        let error: Error = serde_json::from_value(json!({"code":"unavailable","message":"response lost","diagnostics":[{
            "source":"plugin","snapshot":{"text":"private".repeat(MAX_STDERR_BYTES),"truncated":false,"eof":true,"read_error":false}
        }]})).unwrap();
        assert_eq!(error.code, "unavailable");
        assert!(error.diagnostics[0].snapshot.text.is_empty());
        assert!(error.diagnostics[0].snapshot.truncated);
    }
}
