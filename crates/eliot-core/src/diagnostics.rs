//! Bounded stderr data. Producers redact before retaining/truncating bytes.

use serde::{Deserialize, Serialize};

pub const MAX_STDERR_BYTES: usize = 16 * 1024;
/// Aggregate encoded JSON budget, including source/flags and JSON escaping.
pub const MAX_STDERR_DIAGNOSTIC_BYTES: usize = 16 * 1024;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StderrSnapshot {
    pub text: String,
    pub truncated: bool,
    pub eof: bool,
    pub read_error: bool,
}

impl Serialize for StderrSnapshot {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Snapshot<'a> {
            text: &'a str,
            truncated: bool,
            eof: bool,
            read_error: bool,
        }
        let safe = self.clone().bounded();
        Snapshot {
            text: &safe.text,
            truncated: safe.truncated,
            eof: safe.eof,
            read_error: safe.read_error,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for StderrSnapshot {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Snapshot {
            text: String,
            truncated: bool,
            eof: bool,
            read_error: bool,
        }
        let mut snapshot = Snapshot::deserialize(deserializer)?;
        if snapshot.text.len() > MAX_STDERR_BYTES {
            // Incoming data is not yet trusted as sanitized. Omit it rather
            // than cutting a raw secret before the receiver can redact it.
            snapshot.text = String::new();
            snapshot.truncated = true;
        }
        Ok(Self {
            text: snapshot.text,
            truncated: snapshot.truncated,
            eof: snapshot.eof,
            read_error: snapshot.read_error,
        })
    }
}

impl StderrSnapshot {
    pub(crate) fn bounded(mut self) -> Self {
        if self.text.len() > MAX_STDERR_BYTES {
            let mut start = self.text.len() - MAX_STDERR_BYTES;
            while !self.text.is_char_boundary(start) {
                start += 1;
            }
            self.text = self.text.split_off(start);
            self.truncated = true;
        }
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StderrSource {
    Plugin,
    Harness,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StderrDiagnostic {
    pub source: StderrSource,
    pub snapshot: StderrSnapshot,
}

/// Bound already-sanitized tails BEFORE attaching to an outcome/RPC/log. The
/// caller may pass the smaller remaining encoded budget of its enclosing data.
/// Optional diagnostics never invalidate the primary result. No raw bytes belong
/// here: stream producers redact before retention, and receivers sanitize first.
pub fn bound_stderr_diagnostics(values: &mut Vec<StderrDiagnostic>, max_encoded_bytes: usize) {
    let mut unique: Vec<StderrDiagnostic> = Vec::with_capacity(2);
    for mut value in values.drain(..) {
        value.snapshot = value.snapshot.bounded();
        if let Some(existing) = unique.iter_mut().find(|old| old.source == value.source) {
            *existing = value;
        } else {
            unique.push(value);
        }
    }
    *values = unique;
    let fits = |values: &[StderrDiagnostic]| {
        serde_json::to_vec(values).is_ok_and(|bytes| bytes.len() <= max_encoded_bytes)
    };
    while !values.is_empty() && !fits(values) {
        // Preserve both sources, flags and the newest tail bytes while space
        // permits. Halving is bounded by the two 16-KiB texts.
        if let Some(value) = values
            .iter_mut()
            .max_by_key(|value| value.snapshot.text.len())
            && !value.snapshot.text.is_empty()
        {
            let mut start = value.snapshot.text.len().div_ceil(2);
            while !value.snapshot.text.is_char_boundary(start) {
                start += 1;
            }
            value.snapshot.text = value.snapshot.text.split_off(start);
            value.snapshot.truncated = true;
        } else {
            values.clear();
        }
    }
}

pub(crate) fn deserialize_diagnostics<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<StderrDiagnostic>, D::Error> {
    let raw = serde_json::Value::deserialize(deserializer)?;
    let Some(raw) = raw.as_array() else {
        return Ok(Vec::new());
    };
    let mut values = Vec::with_capacity(2);
    let mut malformed = raw.len() > 2;
    for value in raw {
        let Ok(value) = serde_json::from_value::<StderrDiagnostic>(value.clone()) else {
            malformed = true;
            continue;
        };
        if values
            .iter()
            .any(|old: &StderrDiagnostic| old.source == value.source)
        {
            malformed = true;
            continue;
        }
        values.push(value);
    }
    // An oversized untrusted encoded payload is omitted, not truncated before
    // redaction. Keep flags so loss of optional diagnostic text stays visible.
    if malformed
        || serde_json::to_vec(&values).is_ok_and(|bytes| bytes.len() > MAX_STDERR_DIAGNOSTIC_BYTES)
    {
        for value in &mut values {
            value.snapshot.text = String::new();
            value.snapshot.truncated = true;
        }
    }
    bound_stderr_diagnostics(&mut values, MAX_STDERR_DIAGNOSTIC_BYTES);
    Ok(values)
}
