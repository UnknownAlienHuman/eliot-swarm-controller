//! Minimal server-sent-events reader for `GET /api/event`.
//!
//! The installed stream sends one JSON envelope per `data:` payload,
//! comment heartbeats (`: heartbeat`), and blank-line dispatch. Only
//! `data:` payloads are surfaced; `event:`/`id:`/`retry:` fields are
//! ignored because the envelope carries its own `id` and `type`.
//! A payload larger than [`MAX_SSE_BYTES`] ends the stream as malformed
//! rather than growing without bound.

use crate::native::types::SseEnvelope;

/// Largest single SSE payload accepted before the stream is declared
/// malformed. Envelopes are small control records; message bodies travel
/// over the readback endpoints, never the stream.
pub const MAX_SSE_BYTES: usize = 256 * 1024;

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
    data: Vec<String>,
    oversize: bool,
}

impl SseParser {
    pub fn new() -> Self {
        Self::default()
    }

    /// Feed newly arrived bytes; returns the complete frames they finish.
    /// After [`SseFrame::Malformed`] the parser is spent and yields nothing
    /// further.
    pub fn feed(&mut self, bytes: &[u8]) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        self.buffer.extend_from_slice(bytes);
        while let Some(end) = find_line_end(&self.buffer) {
            let raw: Vec<u8> = self.buffer.drain(..end).collect();
            // Drop the line terminator (`\n`, or `\r\n` with the `\r` kept in
            // `raw` and trimmed below).
            if self.buffer.first() == Some(&b'\n') {
                self.buffer.drain(..1);
            }
            let line = raw
                .strip_suffix(b"\r")
                .map_or(raw.as_slice(), |stripped| stripped);
            if self.oversize {
                // The stream is already declared malformed; discard the rest.
                continue;
            }
            if line.is_empty() {
                if let Some(frame) = self.dispatch() {
                    frames.push(frame);
                }
                continue;
            }
            if line.starts_with(b":") {
                continue;
            }
            let Some(field) = parse_field(line) else {
                continue;
            };
            if field.name != "data" {
                continue;
            }
            if self.data_bytes().saturating_add(field.value.len()) > MAX_SSE_BYTES {
                self.oversize = true;
                self.data.clear();
                frames.push(SseFrame::Malformed);
                continue;
            }
            self.data.push(field.value);
        }
        frames
    }

    fn data_bytes(&self) -> usize {
        self.data.iter().map(|line| line.len()).sum()
    }

    fn dispatch(&mut self) -> Option<SseFrame> {
        if self.data.is_empty() {
            return None;
        }
        let payload = std::mem::take(&mut self.data).join("\n");
        match serde_json::from_str::<SseEnvelope>(&payload) {
            Ok(envelope) if !envelope.id.trim().is_empty() => Some(SseFrame::Event(envelope)),
            _ => Some(SseFrame::Malformed),
        }
    }
}

struct Field {
    name: String,
    value: String,
}

fn find_line_end(buffer: &[u8]) -> Option<usize> {
    buffer.iter().position(|byte| *byte == b'\n')
}

fn parse_field(line: &[u8]) -> Option<Field> {
    let line = std::str::from_utf8(line).ok()?;
    let (name, value) = line.split_once(':')?;
    let value = value.strip_prefix(' ').unwrap_or(value);
    Some(Field {
        name: name.to_owned(),
        value: value.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed_all(parser: &mut SseParser, chunks: &[&[u8]]) -> Vec<SseFrame> {
        let mut frames = Vec::new();
        for chunk in chunks {
            frames.extend(parser.feed(chunk));
        }
        frames
    }

    #[test]
    fn installed_stream_shape_parses() {
        let mut parser = SseParser::new();
        let frames = feed_all(
            &mut parser,
            &[
                b"data: {\"id\":\"evt_1\",\"type\":\"server.connected\",\"data\":{}}",
                b"\n\n: heartbeat\n\ndata: {\"id\":",
                b"\"evt_2\",\"type\":\"session.created\",\"data\":{\"sessionID\":\"s1\"}}\n\n",
            ],
        );
        assert_eq!(frames.len(), 2);
        match &frames[0] {
            SseFrame::Event(envelope) => {
                assert_eq!(envelope.id, "evt_1");
                assert_eq!(envelope.event_type, "server.connected");
            }
            other => panic!("expected event, got {other:?}"),
        }
        match &frames[1] {
            SseFrame::Event(envelope) => assert_eq!(envelope.event_type, "session.created"),
            other => panic!("expected event, got {other:?}"),
        }
    }

    #[test]
    fn crlf_and_multiline_data_join() {
        let mut parser = SseParser::new();
        let frames = feed_all(
            &mut parser,
            &[b"data: {\"id\":\"evt_9\",\"type\":\"t\",\r\ndata: \"data\":{}}\r\n\r\n"],
        );
        // CRLF terminates lines and the two `data:` lines join with `\n`
        // into one valid envelope.
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            SseFrame::Event(envelope) => assert_eq!(envelope.id, "evt_9"),
            other => panic!("expected event, got {other:?}"),
        }
    }

    #[test]
    fn oversize_payload_is_malformed_and_spent() {
        let mut parser = SseParser::new();
        let big = "x".repeat(MAX_SSE_BYTES + 1);
        let chunk = format!("data: {big}\n\n");
        let frames = feed_all(&mut parser, &[chunk.as_bytes()]);
        assert_eq!(frames, vec![SseFrame::Malformed]);
        let later = feed_all(
            &mut parser,
            &[b"data: {\"id\":\"evt_1\",\"type\":\"t\",\"data\":{}}\n\n"],
        );
        assert!(later.is_empty());
    }

    #[test]
    fn invalid_envelope_is_malformed() {
        let mut parser = SseParser::new();
        let frames = feed_all(&mut parser, &[b"data: {\"id\":\"\",\"type\":\"t\"}\n\n"]);
        assert_eq!(frames, vec![SseFrame::Malformed]);
    }
}
