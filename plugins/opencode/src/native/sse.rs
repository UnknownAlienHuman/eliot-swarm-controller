//! Minimal server-sent-events reader for `GET /api/event`.
//!
//! The installed stream sends one JSON envelope per `data:` payload,
//! comment heartbeats (`: heartbeat`), and blank-line dispatch. Only
//! `data:` payloads are surfaced; `event:`/`id:`/`retry:` fields are
//! ignored because the envelope carries its own `id` and `type`.
//! A payload (including separators) or raw line larger than
//! [`MAX_SSE_BYTES`], or a non-UTF-8 line, ends the stream as malformed
//! rather than growing without bound.

use crate::native::types::SseEnvelope;

/// Largest single SSE payload accepted before the stream is declared
/// malformed. Envelopes are small control records; message bodies travel
/// over the readback endpoints, never the stream. The same bound caps each
/// raw line, excluding its LF terminator.
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
            if pending.len().saturating_add(raw.len()) > MAX_SSE_BYTES {
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
        if prefix_len + value.len() > MAX_SSE_BYTES {
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
    fn empty_data_lines_count_toward_the_payload_bound() {
        let mut parser = SseParser::new();
        let full_line = format!("data:{}\n", "x".repeat(MAX_SSE_BYTES - "data:".len()));
        assert!(parser.feed(full_line.as_bytes()).is_empty());
        assert!(
            parser
                .feed(b"data:\ndata:\ndata:\ndata:\ndata:\n")
                .is_empty()
        );
        assert!(parser.feed(b"dat").is_empty());
        assert_eq!(parser.feed(b"a:\n"), vec![SseFrame::Malformed]);
        assert!(parser.feed(b"\n").is_empty());
    }

    #[test]
    fn invalid_envelope_is_malformed() {
        let mut parser = SseParser::new();
        let frames = feed_all(&mut parser, &[b"data: {\"id\":\"\",\"type\":\"t\"}\n\n"]);
        assert_eq!(frames, vec![SseFrame::Malformed]);
    }

    #[test]
    fn unterminated_line_past_the_bound_is_malformed_and_spent() {
        let mut parser = SseParser::new();
        // 300 KiB with no newline, fed in two chunks: the pending buffer
        // must trip the bound instead of growing without limit.
        let line = "x".repeat(300 * 1024);
        let frames = feed_all(
            &mut parser,
            &[
                &line.as_bytes()[..200 * 1024],
                &line.as_bytes()[200 * 1024..],
            ],
        );
        assert_eq!(frames, vec![SseFrame::Malformed]);
        let later = feed_all(
            &mut parser,
            &[b"data: {\"id\":\"evt_1\",\"type\":\"t\",\"data\":{}}\n\n"],
        );
        assert!(later.is_empty());
    }

    #[test]
    fn non_utf8_lines_are_malformed_not_dropped() {
        for chunk in [
            b"data: \xff\xfe invalid\n\n".as_slice(),
            b": \xff heartbeat\n\n".as_slice(),
        ] {
            let mut parser = SseParser::new();
            let frames = feed_all(&mut parser, &[chunk]);
            assert_eq!(frames, vec![SseFrame::Malformed], "{chunk:?}");
            let later = feed_all(
                &mut parser,
                &[b"data: {\"id\":\"evt_1\",\"type\":\"t\",\"data\":{}}\n\n"],
            );
            assert!(later.is_empty());
        }
    }
}
