//! Minimal server-sent-events reader for `GET /api/event`.
//!
//! The installed stream sends one JSON envelope per `data:` payload,
//! comment heartbeats (`: heartbeat`), and blank-line dispatch. Only
//! `data:` payloads are surfaced; `event:`/`id:`/`retry:` fields are
//! ignored because the envelope carries its own `id` and `type`.
//! A frame larger than [`MAX_NATIVE_INBOUND_FRAME_BYTES`] is drained to its
//! blank-line boundary without retaining its payload and surfaced as an
//! oversized notification. Other malformed input ends the stream.

use eliot_core::limits::MAX_NATIVE_INBOUND_FRAME_BYTES;

use crate::native::types::SseEnvelope;

/// One parsed stream item from `GET /api/event`.
#[derive(Debug, Clone, PartialEq)]
pub enum SseFrame {
    Event(SseEnvelope),
    /// A notification discarded at its SSE frame boundary. `size` counts
    /// the actual wire bytes through the terminating blank line.
    Oversized {
        size: usize,
        method: Option<String>,
    },
    Malformed,
}

#[derive(Debug)]
struct DiscardingFrame {
    size: usize,
    method: Option<String>,
    line_nonblank: bool,
}

/// Incremental SSE parser over a byte stream.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
    data: Option<String>,
    frame_size: usize,
    discarding: Option<DiscardingFrame>,
    spent: bool,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed newly arrived bytes; returns the complete frames they finish.
    /// After [`SseFrame::Malformed`] the parser is spent and yields nothing
    /// further. Oversized frames are drained and do not spend the parser.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<SseFrame> {
        if self.spent {
            return Vec::new();
        }
        let mut frames = Vec::new();
        let mut pending = std::mem::take(&mut self.buffer);
        for part in bytes.split_inclusive(|&byte| byte == b'\n') {
            let complete = part.strip_suffix(b"\n");
            let raw = complete.unwrap_or(part);

            if let Some(discarding) = &mut self.discarding {
                discarding.size = discarding.size.saturating_add(part.len());
                if raw.iter().any(|&byte| byte != b'\r') {
                    discarding.line_nonblank = true;
                }
                let boundary = complete.is_some() && !discarding.line_nonblank;
                if complete.is_some() {
                    discarding.line_nonblank = false;
                }
                if boundary && let Some(discarding) = self.discarding.take() {
                    frames.push(SseFrame::Oversized {
                        size: discarding.size,
                        method: discarding.method,
                    });
                    self.frame_size = 0;
                    self.data = None;
                    pending.clear();
                }
                continue;
            }

            self.frame_size = self.frame_size.saturating_add(part.len());
            let line_len = pending.len().saturating_add(raw.len());
            let line_is_data = starts_with_field(&pending, raw, b"data:");
            if self.frame_size > MAX_NATIVE_INBOUND_FRAME_BYTES
                || line_len > MAX_NATIVE_INBOUND_FRAME_BYTES
            {
                if self.data.is_none() && !line_is_data {
                    self.spent = true;
                    frames.push(SseFrame::Malformed);
                    break;
                }

                let method = self.method_hint(&pending, raw);
                let line_is_blank = joined_line_is_blank(&pending, raw);
                let size = self.frame_size;
                self.data = None;
                pending.clear();
                if complete.is_some() && line_is_blank {
                    frames.push(SseFrame::Oversized { size, method });
                    self.frame_size = 0;
                } else {
                    self.discarding = Some(DiscardingFrame {
                        size,
                        method,
                        line_nonblank: complete.is_none() && !line_is_blank,
                    });
                }
                continue;
            }

            if complete.is_none() {
                pending.extend_from_slice(raw);
                break;
            }
            let line_is_blank = joined_line_is_blank(&pending, raw);
            let frame = if pending.is_empty() {
                self.accept_line(raw)
            } else {
                pending.extend_from_slice(raw);
                let frame = self.accept_line(&pending);
                pending.clear();
                frame
            };
            if line_is_blank {
                self.frame_size = 0;
            }
            if let Some(frame) = frame {
                self.spent = frame == SseFrame::Malformed;
                frames.push(frame);
                if self.spent {
                    break;
                }
            }
        }
        if self.spent {
            self.data = None;
            self.buffer.clear();
        } else if self.discarding.is_some() {
            self.buffer.clear();
        } else {
            self.buffer = pending;
        }
        frames
    }

    fn method_hint(&self, pending: &[u8], raw: &[u8]) -> Option<String> {
        if let Some(data) = self.data.as_ref()
            && let Some(method) = extract_event_method(data.as_bytes())
        {
            return Some(method);
        }
        const METHOD_SCAN_BYTES: usize = 64 * 1024;
        let mut prefix = Vec::with_capacity(METHOD_SCAN_BYTES);
        append_bounded(&mut prefix, pending, METHOD_SCAN_BYTES);
        append_bounded(&mut prefix, raw, METHOD_SCAN_BYTES);
        extract_event_method(&prefix)
    }

    fn accept_line(&mut self, raw: &[u8]) -> Option<SseFrame> {
        let raw = raw.strip_suffix(b"\r").unwrap_or(raw);
        let Ok(line) = std::str::from_utf8(raw) else {
            return Some(SseFrame::Malformed);
        };
        if line.is_empty() {
            let payload = self.data.take()?;
            return Some(match serde_json::from_str::<SseEnvelope>(&payload) {
                Ok(envelope) if !envelope.id.trim().is_empty() => SseFrame::Event(envelope),
                _ => SseFrame::Malformed,
            });
        }
        let value = line.strip_prefix("data:")?;
        let value = value.strip_prefix(' ').unwrap_or(value);
        match &mut self.data {
            Some(data) => {
                data.push('\n');
                data.push_str(value);
            }
            None => self.data = Some(value.to_owned()),
        }
        None
    }
}

fn starts_with_field(left: &[u8], right: &[u8], field: &[u8]) -> bool {
    field.iter().enumerate().all(|(index, expected)| {
        let byte = if index < left.len() {
            left[index]
        } else {
            right.get(index - left.len()).copied().unwrap_or_default()
        };
        byte == *expected
    })
}

fn joined_line_is_blank(left: &[u8], right: &[u8]) -> bool {
    left.iter().chain(right).all(|&byte| byte == b'\r')
}

fn append_bounded(destination: &mut Vec<u8>, source: &[u8], limit: usize) {
    let remaining = limit.saturating_sub(destination.len());
    destination.extend_from_slice(&source[..source.len().min(remaining)]);
}

/// OpenCode has emitted both direct event envelopes and an outer global-event
/// wrapper. Read the `type` from the envelope header, never arbitrary tool
/// payload fields.
fn extract_event_method(payload: &[u8]) -> Option<String> {
    let payload = payload.strip_prefix(b"data:").unwrap_or(payload);
    let direct = find_json_field(payload, "type").and_then(parse_event_type);
    direct.or_else(|| {
        find_json_field(payload, "payload")
            .and_then(|payload| find_json_field(payload, "type"))
            .and_then(parse_event_type)
    })
}

fn find_json_field<'a>(payload: &'a [u8], field: &str) -> Option<&'a [u8]> {
    let start = payload
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())?;
    if payload[start] != b'{' {
        return None;
    }
    let mut depth = 0usize;
    let mut offset = 0;
    while offset < payload.len() {
        match payload[offset] {
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth = depth.checked_sub(1)?;
                if depth == 0 {
                    return None;
                }
            }
            b'"' => {
                let end = json_string_end(payload, offset)?;
                if depth == 1 && &payload[offset + 1..end] == field.as_bytes() {
                    let rest = &payload[end + 1..];
                    if let Some(colon) = rest.iter().position(|byte| !byte.is_ascii_whitespace())
                        && rest[colon] == b':'
                    {
                        return Some(&rest[colon + 1..]);
                    }
                }
                offset = end;
            }
            _ => {}
        }
        offset += 1;
    }
    None
}

fn parse_event_type(payload: &[u8]) -> Option<String> {
    let start = payload
        .iter()
        .position(|byte| !byte.is_ascii_whitespace())?;
    if payload[start] != b'"' {
        return None;
    }
    let end = json_string_end(payload, start)?;
    let value = &payload[start + 1..end];
    if value.is_empty() || value.len() > 256 || !value.iter().copied().all(is_method_byte) {
        return None;
    }
    let value = std::str::from_utf8(value).ok()?;
    (!matches!(value, "event" | "notification")).then(|| value.to_owned())
}

fn json_string_end(payload: &[u8], start: usize) -> Option<usize> {
    let mut offset = start + 1;
    while let Some(&byte) = payload.get(offset) {
        match byte {
            b'\\' => offset = offset.saturating_add(2),
            b'"' => return Some(offset),
            _ => offset += 1,
        }
    }
    None
}

fn is_method_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'/' | b'-' | b':')
}
