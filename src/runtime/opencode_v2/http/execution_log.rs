//! A bounded, non-following durable log read over the existing pooled client.
use super::{REQUEST_TIMEOUT, Service, header};
use crate::{
    error::{Error, Result},
    runtime::{
        RuntimeCommand,
        opencode_v2::{ExecutionRead, ExecutionScan},
    },
};
use eventsource_stream::Eventsource;
use futures_util::StreamExt;
use serde_json::Value;
use std::collections::BTreeSet;

impl Service {
    pub(in crate::runtime::opencode_v2) async fn execution_log(
        &self,
        command: &RuntimeCommand,
        mut scan: ExecutionScan,
    ) -> Result<ExecutionRead> {
        let root = command
            .native_root_id
            .as_deref()
            .ok_or_else(|| Error::invalid("execution read needs the exact native root"))?;
        let mut url = self
            .endpoint
            .join(&format!("/api/experimental/session/{root}/log"))
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
        let mut needs_anchor = scan.anchor().is_some();
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
                scan.consume(&value, command)?;
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
        Ok(ExecutionRead {
            scan,
            synced,
            gap: read_gap.or(if synced {
                None
            } else {
                Some("NATIVE_LOG_NOT_SYNCED")
            }),
        })
    }
}
