//! Minimal server-sent-events reader for `GET /api/event`.
//!
//! The installed stream sends one JSON envelope per `data:` payload,
//! comment heartbeats (`: heartbeat`), and blank-line dispatch. Only
//! `data:` payloads are surfaced; `event:`/`id:`/`retry:` fields are
//! ignored because the envelope carries its own `id` and `type`.
//! A payload (including separators) or raw line larger than
//! [`MAX_NATIVE_INBOUND_FRAME_BYTES`], or a non-UTF-8 line, ends the stream as malformed
//! rather than growing without bound.

use eliot_core::limits::MAX_NATIVE_INBOUND_FRAME_BYTES;

use crate::native::types::SseEnvelope;

/// One parsed stream item: either an envelope or a malformed payload
/// marker (the pump converts the marker into the stream state and ends).
#[derive(Debug, Clone, PartialEq)]
pub enum SseFrame {
    Event(SseEnvelope),
    Malformed,
}

/// Incremental SSE parser over a byte stream.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
    data: Option<String>,
    spent: bool,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed newly arrived bytes; returns the complete frames they finish.
    /// After [`SseFrame::Malformed`] the parser is spent and yields nothing
    /// further.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<SseFrame> {
        if self.spent {
            return Vec::new();
        }
        let mut frames = Vec::new();
        let mut pending = std::mem::take(&mut self.buffer);
        for part in bytes.split_inclusive(|&byte| byte == b'\n') {
            let complete = part.strip_suffix(b"\n");
            let raw = complete.unwrap_or(part);
            if pending.len().saturating_add(raw.len()) > MAX_NATIVE_INBOUND_FRAME_BYTES {
                self.spent = true;
                frames.push(SseFrame::Malformed);
                break;
            }
            if complete.is_none() {
                pending.extend_from_slice(raw);
                break;
            }
            let frame = if pending.is_empty() {
                self.accept_line(raw)
            } else {
                pending.extend_from_slice(raw);
                let frame = self.accept_line(&pending);
                pending.clear();
                frame
            };
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
        } else {
            self.buffer = pending;
        }
        frames
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
        let prefix_len = self.data.as_ref().map_or(0, |data| data.len() + 1);
        if prefix_len + value.len() > MAX_NATIVE_INBOUND_FRAME_BYTES {
            return Some(SseFrame::Malformed);
        }
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
