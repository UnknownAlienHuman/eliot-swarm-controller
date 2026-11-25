//! Shared complete-text redaction and streaming literal-secret protection.

mod credentials;

use serde_json::Value;
use std::borrow::Cow;

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

    /// Redact recognizable shapes and the scoped set of exact secrets.
    /// Call on complete text before truncation, hashing or persistence.
    pub fn redact_text(&self, text: &str) -> String {
        self.redact_field_text(text, None).into_owned()
    }

    fn redact_field_text<'a>(&self, text: &'a str, field: Option<&str>) -> Cow<'a, str> {
        let shaped = credentials::redact_field(text, field);
        if self.contains_text(&shaped) {
            let (safe, _) = self.redact_prefix(shaped.as_bytes(), true);
            Cow::Owned(String::from_utf8_lossy(&safe).into_owned())
        } else {
            shaped
        }
    }

    /// Redact complete text, then bound it to `max_bytes` at a character
    /// boundary (a progress fact's label, never the whole native payload).
    pub fn redact_bounded(&self, text: &str, max_bytes: usize) -> String {
        let mut safe = self.redact_text(text);
        safe.truncate(safe.floor_char_boundary(max_bytes));
        safe
    }

    /// Check exact credentials and recognizable shapes without altering an identity.
    pub fn contains_native_secret(&self, text: &str) -> bool {
        self.contains_text(text) || credentials::contains(text)
    }

    /// Redact string values with context from explicit secret fields.
    /// Object keys remain intact for the caller's metadata validation.
    pub fn redact_values(&self, value: &mut Value) -> bool {
        self.redact_field(value, None)
    }

    fn redact_field(&self, value: &mut Value, field: Option<&str>) -> bool {
        match value {
            Value::String(text) => {
                let Cow::Owned(safe) = self.redact_field_text(text, field) else {
                    return false;
                };
                if *text == safe {
                    return false;
                }
                *text = safe;
                true
            }
            Value::Array(values) => {
                let mut changed = false;
                for value in values {
                    changed |= self.redact_field(value, field);
                }
                changed
            }
            Value::Object(values) => {
                let mut changed = false;
                for (key, value) in values {
                    changed |= self.redact_field(value, Some(key));
                }
                changed
            }
            _ => false,
        }
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

    /// Recognize credentials in a structured input without changing its bindings.
    pub fn contains_native_value(&self, value: &Value) -> bool {
        self.contains_native_field(value, None)
    }

    fn contains_native_field(&self, value: &Value, field: Option<&str>) -> bool {
        match value {
            Value::String(text) => matches!(self.redact_field_text(text, field), Cow::Owned(_)),
            Value::Array(values) => values
                .iter()
                .any(|value| self.contains_native_field(value, field)),
            Value::Object(values) => values.iter().any(|(key, value)| {
                self.contains_native_secret(key) || self.contains_native_field(value, Some(key))
            }),
            _ => false,
        }
    }

    /// Publishing keys is safe only if they are credential-free. Keys carry
    /// schema and must never be silently replaced with another key.
    pub fn contains_native_key(&self, value: &Value) -> bool {
        match value {
            Value::Object(values) => values.iter().any(|(key, value)| {
                self.contains_native_secret(key) || self.contains_native_key(value)
            }),
            Value::Array(values) => values.iter().any(|value| self.contains_native_key(value)),
            _ => false,
        }
    }

    /// Prepare an owned display value before applying a caller's size bound.
    pub fn redacted_values(&self, mut value: Value) -> Value {
        self.redact_values(&mut value);
        value
    }

    /// Compare public offer values using the same text as their retained view.
    pub fn same_redacted_values(&self, left: &Value, right: &Value) -> bool {
        if left == right {
            return true;
        }
        self.redacted_values(left.clone()) == self.redacted_values(right.clone())
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
