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

    /// Redact complete input before applying any bound. Complete redaction
    /// markers are preserved across repeated sanitization. Streaming callers
    /// must retain the unfinished suffix from `redact_prefix(_, false)`;
    /// guessing whether an arbitrary text suffix is a secret is unsafe.
    pub fn redact_text(&self, text: &str) -> String {
        let (safe, _) = self.redact_prefix(text.as_bytes(), true);
        String::from_utf8_lossy(&safe).into_owned()
    }

    /// Redact complete text, then bound it to `max_bytes` at a character
    /// boundary (a progress fact's label, never the whole native payload).
    pub fn redact_bounded(&self, text: &str, max_bytes: usize) -> String {
        let mut safe = self.redact_text(text);
        safe.truncate(safe.floor_char_boundary(max_bytes));
        safe
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
    /// the returned safe bytes, never the pending original suffix. At end of
    /// input a trailing fragment that matches the start of a longer secret is
    /// redacted (see [`MIN_TRUNCATED_SECRET_PREFIX`]).
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
