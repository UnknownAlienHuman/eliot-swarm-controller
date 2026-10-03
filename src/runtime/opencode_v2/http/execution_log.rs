//! A bounded, non-following durable log read over the existing pooled client.
use super::{REQUEST_TIMEOUT, Service, header};
use crate::{
    error::{Error, Result},
    runtime::opencode_v2::{
        ExecutionRead, ExecutionScan, NativeInputDescriptor, RootCreationRead, RootCreationScan,
        SessionRead, SessionScan,
    },
};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::Value;
use std::collections::BTreeSet;

/// One scan's view of the shared stream loop. Anchor replay is verified, never
/// assumed; consumption and watermark handling stay with the scan itself.
trait LogScan {
    fn session_id(&self) -> &str;
    fn after(&self) -> Option<u64>;
    fn has_anchor(&self) -> bool;
    fn verify_anchor(&self, value: &Value) -> Result<()>;
    fn consume(&mut self, value: &Value) -> Result<()>;
    fn synchronize(&mut self, value: &Value) -> Result<()>;
}
struct InputScan<'a> {
    scan: ExecutionScan,
    descriptor: &'a NativeInputDescriptor,
}
impl LogScan for InputScan<'_> {
    fn session_id(&self) -> &str {
        self.scan.session_id()
    }
    fn after(&self) -> Option<u64> {
        self.scan.after()
    }
    fn has_anchor(&self) -> bool {
        self.scan.anchor().is_some()
    }
    fn verify_anchor(&self, value: &Value) -> Result<()> {
        self.scan.verify_anchor(value)
    }
    fn consume(&mut self, value: &Value) -> Result<()> {
        self.scan.consume(value, self.descriptor)
    }
    fn synchronize(&mut self, value: &Value) -> Result<()> {
        self.scan.synchronize(value)
    }
}
impl LogScan for SessionScan {
    fn session_id(&self) -> &str {
        self.session_id()
    }
    fn after(&self) -> Option<u64> {
        self.after()
    }
    fn has_anchor(&self) -> bool {
        self.anchor().is_some()
    }
    fn verify_anchor(&self, value: &Value) -> Result<()> {
        self.verify_anchor(value)
    }
    fn consume(&mut self, value: &Value) -> Result<()> {
        self.consume(value)
    }
    fn synchronize(&mut self, value: &Value) -> Result<()> {
        self.synchronize(value)
    }
}
impl LogScan for RootCreationScan {
    fn session_id(&self) -> &str {
        self.session_id()
    }
    fn after(&self) -> Option<u64> {
        None
    }
    fn has_anchor(&self) -> bool {
        self.has_anchor()
    }
    fn verify_anchor(&self, value: &Value) -> Result<()> {
        self.verify_anchor(value)
    }
    fn consume(&mut self, value: &Value) -> Result<()> {
        self.consume(value)
    }
    fn synchronize(&mut self, value: &Value) -> Result<()> {
        self.synchronize(value)
    }
}

impl Service {
    async fn stream_log<S: LogScan>(&self, scan: &mut S) -> Result<(bool, Option<&'static str>)> {
        let mut url = self
            .endpoint
            .join(&format!(
                "/api/experimental/session/{}/log",
                scan.session_id()
            ))
            .map_err(|_| Error::new("NATIVE_ENDPOINT", "invalid native log route"))?;
        url.query_pairs_mut().append_pair("follow", "false");
        if let Some(after) = scan.after() {
            url.query_pairs_mut()
                .append_pair("after", &after.to_string());
        }
        let response = self
            .client
            .get(url)
            .header(header::ACCEPT, "text/event-stream")
            .timeout(REQUEST_TIMEOUT)
            .send()
            .await
            .map_err(|_| Error::new("NATIVE_LOG_UNAVAILABLE", "native log request failed"))?;
        if !response.status().is_success()
            || response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|h| h.to_str().ok())
                .and_then(|s| s.split(';').next())
                != Some("text/event-stream")
        {
            return Err(Error::new(
                "NATIVE_LOG_UNAVAILABLE",
                "native log response is not an SSE read",
            ));
        }
        // Cap raw bytes before the upstream parser, including unterminated frames.
        let mut bytes = 0usize;
        let stream = response
            .bytes_stream()
            .map(move |chunk| {
                let chunk = chunk.map_err(|_| std::io::Error::other("native log transport"))?;
                bytes = bytes.saturating_add(chunk.len());
                if bytes > 8 * 1024 * 1024 {
                    return Err(std::io::Error::other("native log bound"));
                }
                Ok(chunk)
            })
            .eventsource();
        futures_util::pin_mut!(stream);
        let mut needs_anchor = scan.has_anchor();
        let mut synced = false;
        let mut ids = BTreeSet::new();
        let mut count = 0usize;
        let mut read_gap = None;
        while let Some(item) = stream.next().await {
            let event = match item {
                Ok(event) => event,
                Err(_) => {
                    read_gap = Some("NATIVE_LOG_STREAM_INCOMPLETE");
                    break;
                }
            };
            if event.event == "effect/httpapi/stream/failure" {
                read_gap = Some("NATIVE_LOG_NATIVE_FAILURE");
                break;
            }
            if synced {
                return Err(Error::new(
                    "NATIVE_LOG_AFTER_WATERMARK",
                    "non-following log continued after its watermark",
                ));
            }
            let value: Value = serde_json::from_str(&event.data)
                .map_err(|_| Error::new("NATIVE_LOG_SCHEMA", "native log JSON is invalid"))?;
            if value["type"] == "log.synced" {
                if needs_anchor {
                    return Err(Error::new(
                        "NATIVE_LOG_ANCHOR_MISSING",
                        "saved log anchor was not replayed",
                    ));
                }
                scan.synchronize(&value)?;
                synced = true;
                continue;
            }
            count += 1;
            if count > 8192 {
                read_gap = Some("NATIVE_LOG_EVENT_LIMIT");
                break;
            }
            let id = value["id"]
                .as_str()
                .ok_or_else(|| Error::new("NATIVE_LOG_SCHEMA", "missing durable event ID"))?;
            if !ids.insert(id.to_owned()) {
                return Err(Error::new(
                    "NATIVE_LOG_DUPLICATE",
                    "native log repeated an event ID",
                ));
            }
            if needs_anchor {
                scan.verify_anchor(&value)?;
                needs_anchor = false;
            } else {
                scan.consume(&value)?;
            }
        }
        // A partial response may advance checked scan progress, never producer
        // disposition. No prefix is published if its saved anchor was not seen.
        if needs_anchor {
            return Err(Error::new(
                "NATIVE_LOG_ANCHOR_MISSING",
                "saved log anchor was not replayed",
            ));
        }
        let synced = synced && read_gap.is_none();
        let gap = read_gap.or(if synced {
            None
        } else {
            Some("NATIVE_LOG_NOT_SYNCED")
        });
        Ok((synced, gap))
    }

    pub(in crate::runtime::opencode_v2) async fn execution_log(
        &self,
        descriptor: &NativeInputDescriptor,
        scan: ExecutionScan,
    ) -> Result<ExecutionRead> {
        let root = descriptor.session_id();
        let mut input = InputScan { scan, descriptor };
        if input.session_id() != root {
            return Err(Error::invalid("execution scan belongs to another session"));
        }
        let (synced, gap) = self.stream_log(&mut input).await?;
        Ok(ExecutionRead {
            scan: input.scan,
            synced,
            gap,
        })
    }

    pub(in crate::runtime::opencode_v2) async fn session_log(
        &self,
        mut scan: SessionScan,
    ) -> Result<SessionRead> {
        let (synced, gap) = self.stream_log(&mut scan).await?;
        Ok(SessionRead { scan, synced, gap })
    }

    pub(in crate::runtime::opencode_v2) async fn root_creation_log(
        &self,
        mut scan: RootCreationScan,
    ) -> Result<RootCreationRead> {
        let (synced, gap) = self.stream_log(&mut scan).await?;
        Ok(RootCreationRead { scan, synced, gap })
    }
}
