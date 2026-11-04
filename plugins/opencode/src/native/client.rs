//! HTTP/SSE client for one per-Agent `opencode serve`.
//!
//! Installed transport (v2.0.32): plain HTTP on loopback, routes under
//! `/api/...`, success bodies wrapped as `{"data": ...}`, errors as
//! `{"_tag": ..., "message": ...}`, auth as
//! `Authorization: Basic base64("opencode:" + password)` where the password
//! is the server's `OPENCODE_PASSWORD`. The client never retries (a repeat
//! would be a second native write) and never follows redirects.

use eliot_core::{Error, Result};
use eliot_plugin::native_stdio::StreamState;
use reqwest::{Client, redirect, retry};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};

use crate::native::{
    sse::{SseFrame, SseParser},
    types::{
        ConfigEntry, DataEnvelope, ErrorBody, InboxUser, LocationRef, MessagePage, ModelRef,
        PromptRequest, SessionCreateRequest, SessionInfo,
    },
};

/// Largest single HTTP body accepted. Prompt/readback payloads are small;
/// anything larger is a protocol violation, never truncated into a decision.
const MAX_HTTP_BYTES: usize = 1024 * 1024;
const SSE_QUEUE: usize = 64;

/// Classified native HTTP outcome. `Rejected` is authoritative
/// non-admission (a complete error body proving no effect, or a transport
/// failure before any byte was sent); `Unknown` is every ambiguous case
/// after a write may have been admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HttpOutcome {
    Rejected { status: Option<u16>, reason: String },
    Unknown { reason: String },
    NotDispatched { reason: String },
}

impl HttpOutcome {
    fn rejected(status: u16, tag: &str, message: &str) -> Self {
        Self::Rejected {
            status: Some(status),
            reason: format!("OpenCode refused the write ({tag}): {message}"),
        }
    }

    fn unknown(reason: impl Into<String>) -> Self {
        Self::Unknown {
            reason: reason.into(),
        }
    }
}

/// Typed client bound to one owned server. `Clone` shares the connection
/// pool; the password travels only in the auth header, never in logs.
#[derive(Clone)]
pub struct OpenCodeHttp {
    client: Client,
    base: String,
    password: String,
}

impl OpenCodeHttp {
    pub fn new(port: u16, password: String) -> Result<Self> {
        let client = Client::builder()
            .no_proxy()
            .redirect(redirect::Policy::none())
            .retry(retry::never())
            .build()
            .map_err(|_| Error::new("internal", "OpenCode HTTP client could not be built"))?;
        Ok(Self {
            client,
            base: format!("http://127.0.0.1:{port}"),
            password,
        })
    }

    /// `GET /api/config`: effective configuration documents. A read, so any
    /// failure is `native_readback`, never `unknown`.
    pub async fn get_config(&self, deadline: Instant) -> Result<Vec<ConfigEntry>> {
        let response = self
            .send(reqwest::Method::GET, "/api/config", &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status != 200 {
            return Err(readback_error("config", &response));
        }
        serde_json::from_slice(&response.body)
            .map_err(|_| Error::new("native_readback", "OpenCode config readback was invalid"))
    }

    /// `POST /api/session`: create a session without input.
    pub async fn create_session(
        &self,
        title: &str,
        model: &ModelRef,
        directory: &str,
        deadline: Instant,
    ) -> std::result::Result<SessionInfo, HttpOutcome> {
        let body = serde_json::to_value(SessionCreateRequest {
            title: Some(title.to_owned()),
            model: Some(model.clone()),
            location: Some(LocationRef {
                directory: directory.to_owned(),
            }),
        })
        .map_err(|_| HttpOutcome::unknown("OpenCode session/create params could not be encoded"))?;
        let response = self
            .send(
                reqwest::Method::POST,
                "/api/session",
                &[],
                Some(body),
                deadline,
            )
            .await?;
        if response.status == 200 {
            let envelope: DataEnvelope<SessionInfo> = serde_json::from_slice(&response.body)
                .map_err(|_| {
                    HttpOutcome::unknown(
                        "OpenCode session/create returned no exact session identity",
                    )
                })?;
            if envelope.data.id.trim().is_empty() {
                return Err(HttpOutcome::unknown(
                    "OpenCode session/create returned no exact session identity",
                ));
            }
            return Ok(envelope.data);
        }
        Err(map_write_error("session/create", &response))
    }

    /// `GET /api/session/{id}`: exact session readback.
    pub async fn get_session(&self, session_id: &str, deadline: Instant) -> Result<SessionInfo> {
        let path = format!(
            "/api/session/{}",
            verified_segment(session_id)
                .map_err(|_| { Error::new("invalid", "OpenCode session identity is invalid") })?
        );
        let response = self
            .send(reqwest::Method::GET, &path, &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            let envelope: DataEnvelope<SessionInfo> = serde_json::from_slice(&response.body)
                .map_err(|_| {
                    Error::new("native_readback", "OpenCode session readback was invalid")
                })?;
            return Ok(envelope.data);
        }
        Err(readback_error("session", &response))
    }

    /// Exact absence readback: `Ok(true)` only on a 404 with a
    /// `SessionNotFoundError` body, `Ok(false)` on 200, `Err` on anything
    /// ambiguous (a transport failure never proves absence).
    pub async fn session_absent(&self, session_id: &str, deadline: Instant) -> Result<bool> {
        let path = format!(
            "/api/session/{}",
            verified_segment(session_id)
                .map_err(|_| { Error::new("invalid", "OpenCode session identity is invalid") })?
        );
        let response = self
            .send(reqwest::Method::GET, &path, &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            return Ok(false);
        }
        if response.status == 404
            && serde_json::from_slice::<ErrorBody>(&response.body)
                .ok()
                .and_then(|body| body.tag)
                .as_deref()
                == Some("SessionNotFoundError")
        {
            return Ok(true);
        }
        Err(readback_error("session absence", &response))
    }

    /// `DELETE /api/session/{id}`: the native close operation. `Ok(true)`
    /// proves removal (204), `Ok(false)` proves the session is already gone
    /// (404); anything else is `Unknown`.
    pub async fn delete_session(
        &self,
        session_id: &str,
        deadline: Instant,
    ) -> std::result::Result<bool, HttpOutcome> {
        let path = format!("/api/session/{}", verified_segment(session_id)?);
        let response = self
            .send(reqwest::Method::DELETE, &path, &[], None, deadline)
            .await?;
        match response.status {
            204 => Ok(true),
            404 => Ok(false),
            _ => Err(map_write_error("session/remove", &response)),
        }
    }

    /// `POST /api/session/{id}/prompt`: admit one input. The 200 response
    /// only proves inbox admission; input-to-execution association is
    /// observed on the event stream.
    pub async fn prompt(
        &self,
        session_id: &str,
        text: &str,
        deadline: Instant,
    ) -> std::result::Result<InboxUser, HttpOutcome> {
        let path = format!("/api/session/{}/prompt", verified_segment(session_id)?);
        let body = serde_json::to_value(PromptRequest {
            text: text.to_owned(),
        })
        .map_err(|_| HttpOutcome::unknown("OpenCode prompt params could not be encoded"))?;
        let response = self
            .send(reqwest::Method::POST, &path, &[], Some(body), deadline)
            .await?;
        if response.status == 200 {
            let envelope: DataEnvelope<InboxUser> = serde_json::from_slice(&response.body)
                .map_err(|_| {
                    HttpOutcome::unknown("OpenCode prompt returned no exact inbox identity")
                })?;
            if envelope.data.id.trim().is_empty() {
                return Err(HttpOutcome::unknown(
                    "OpenCode prompt returned no exact inbox identity",
                ));
            }
            return Ok(envelope.data);
        }
        Err(map_write_error("session/prompt", &response))
    }

    /// `GET /api/session/{id}/message`: newest-first message page.
    /// `cursor=None` starts at the live edge (`order=desc`); otherwise the
    /// opaque `cursor.next` continues toward older messages.
    pub async fn list_messages(
        &self,
        session_id: &str,
        cursor: Option<&str>,
        deadline: Instant,
    ) -> Result<MessagePage> {
        let path = format!(
            "/api/session/{}/message",
            verified_segment(session_id)
                .map_err(|_| Error::new("invalid", "OpenCode session identity is invalid"))?
        );
        let query: Vec<(&str, &str)> = match cursor {
            Some(cursor) => vec![("limit", "100"), ("cursor", cursor)],
            None => vec![("limit", "100"), ("order", "desc")],
        };
        let response = self
            .send(reqwest::Method::GET, &path, &query, None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            let page: MessagePage = serde_json::from_slice(&response.body).map_err(|_| {
                Error::new("native_readback", "OpenCode message readback was invalid")
            })?;
            return Ok(page);
        }
        Err(readback_error("message list", &response))
    }

    /// `GET /api/event`: subscribe to the broadcast event stream. Returns
    /// once the response headers arrive (the subscription is live before
    /// any session create/input that follows); frames flow to the channel.
    pub async fn subscribe(
        &self,
        deadline: Instant,
    ) -> Result<(mpsc::Receiver<SseFrame>, watch::Receiver<StreamState>)> {
        let url = format!("{}/api/event", self.base);
        let request = self
            .client
            .get(url)
            .basic_auth("opencode", Some(&self.password));
        let response = timeout_at(deadline, request.send())
            .await
            .map_err(|_| Error::new("native_readback", "OpenCode event subscribe timed out"))?
            .map_err(|error| {
                Error::new(
                    "native_readback",
                    format!(
                        "OpenCode event subscribe failed: {}",
                        transport_noun(&error)
                    ),
                )
            })?;
        if response.status() != reqwest::StatusCode::OK {
            return Err(Error::new(
                "native_readback",
                format!(
                    "OpenCode event subscribe was refused with HTTP {}",
                    response.status()
                ),
            ));
        }
        let sse = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|content| content.starts_with("text/event-stream"));
        if !sse {
            return Err(Error::new(
                "native_readback",
                "OpenCode event subscribe did not return an event stream",
            ));
        }
        let (frames_tx, frames_rx) = mpsc::channel(SSE_QUEUE);
        let (stream_tx, stream_rx) = watch::channel(StreamState::Open);
        tokio::spawn(async move {
            pump_sse(response, frames_tx, stream_tx).await;
        });
        Ok((frames_rx, stream_rx))
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<serde_json::Value>,
        deadline: Instant,
    ) -> std::result::Result<RawResponse, HttpOutcome> {
        // The workspace reqwest has no `query` serializer; encode the few
        // readback pairs by hand.
        let mut url = format!("{base}{path}", base = self.base);
        if !query.is_empty() {
            url.push('?');
            url.push_str(&encode_query(query));
        }
        let mut request = self
            .client
            .request(method, url)
            .basic_auth("opencode", Some(&self.password));
        if let Some(body) = body {
            // The workspace reqwest has no `json` feature; encode manually.
            let encoded = serde_json::to_vec(&body)
                .map_err(|_| HttpOutcome::unknown("OpenCode request body could not be encoded"))?;
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(encoded);
        }
        let outcome = timeout_at(deadline, async {
            let mut response = request.send().await?;
            let status = response.status().as_u16();
            // The workspace reqwest has no `stream` feature; `chunk` is core.
            let mut body = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                if body.len().saturating_add(chunk.len()) > MAX_HTTP_BYTES {
                    return Err(HttpOutcome::unknown("OpenCode response exceeded its bound"));
                }
                body.extend_from_slice(&chunk);
            }
            Ok(RawResponse { status, body })
        })
        .await;
        match outcome {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(outcome)) => Err(outcome),
            Err(_) => Err(HttpOutcome::unknown(
                "OpenCode request timed out; its write outcome is unknown",
            )),
        }
    }
}

impl From<reqwest::Error> for HttpOutcome {
    fn from(error: reqwest::Error) -> Self {
        // Only failures before any byte could be sent prove non-dispatch:
        // request construction and the TCP connect itself. Everything past
        // that point (send, wait, body) leaves a write uncertain.
        if error.is_builder() || error.is_connect() {
            return Self::NotDispatched {
                reason: format!(
                    "OpenCode request was not dispatched ({})",
                    transport_noun(&error)
                ),
            };
        }
        Self::unknown(format!(
            "OpenCode request failed after dispatch ({}); its write outcome is unknown",
            transport_noun(&error)
        ))
    }
}

struct RawResponse {
    status: u16,
    body: Vec<u8>,
}

fn transport_noun(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() {
        "body"
    } else if error.is_decode() {
        "decode"
    } else if error.is_builder() {
        "request"
    } else {
        "transport"
    }
}

/// Map a non-2xx write response from its complete error body. Only errors
/// the installed API documents as not admitted prove rejection: 400
/// `InvalidRequestError` (validation), 404 `SessionNotFoundError` /
/// `LocationNotFoundError` (no such subject, so no effect) and 401
/// `UnauthorizedError` (refused before admission). 409 conflict, 5xx and
/// unknown shapes stay `Unknown`.
fn map_write_error(method: &str, response: &RawResponse) -> HttpOutcome {
    let body: Option<ErrorBody> = serde_json::from_slice(&response.body).ok();
    let tag = body.as_ref().and_then(|body| body.tag.as_deref());
    let message = body
        .as_ref()
        .and_then(|body| body.message.as_deref())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("no message");
    match (response.status, tag) {
        (400, Some("InvalidRequestError"))
        | (404, Some("SessionNotFoundError" | "LocationNotFoundError"))
        | (401, Some("UnauthorizedError")) => {
            HttpOutcome::rejected(response.status, tag.unwrap_or("error"), message)
        }
        (404, None) if response.body.is_empty() => {
            HttpOutcome::rejected(response.status, "not_found", "empty 404 body")
        }
        _ => HttpOutcome::unknown(format!(
            "OpenCode {method} answered HTTP {} ({}); its write outcome is unknown",
            response.status,
            tag.unwrap_or("unclassified error"),
        )),
    }
}

fn readback_outcome_as_error(outcome: HttpOutcome) -> Error {
    match outcome {
        HttpOutcome::Rejected { reason, .. } => {
            Error::new("native_readback", format!("OpenCode readback: {reason}"))
        }
        HttpOutcome::Unknown { reason } | HttpOutcome::NotDispatched { reason } => {
            Error::new("native_readback", reason)
        }
    }
}

/// Percent-encode query pairs (`application/x-www-form-urlencoded`
/// without the `+`-for-space rule; spaces encode as `%20`).
fn encode_query(pairs: &[(&str, &str)]) -> String {
    fn encode(into: &mut String, text: &str) {
        for byte in text.bytes() {
            if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
                into.push(byte as char);
            } else {
                into.push_str(&format!("%{byte:02X}"));
            }
        }
    }

    let mut out = String::new();
    for (index, (key, value)) in pairs.iter().enumerate() {
        if index > 0 {
            out.push('&');
        }
        encode(&mut out, key);
        out.push('=');
        encode(&mut out, value);
    }
    out
}

fn readback_error(what: &str, response: &RawResponse) -> Error {
    let tag = serde_json::from_slice::<ErrorBody>(&response.body)
        .ok()
        .and_then(|body| body.tag)
        .unwrap_or_else(|| "unclassified error".to_owned());
    Error::new(
        "native_readback",
        format!(
            "OpenCode {what} readback answered HTTP {} ({tag})",
            response.status
        ),
    )
}

/// Path segments carry only server-issued ids; anything else is refused
/// before it can reach the URL. No byte was sent, so this is provably
/// not dispatched, never unknown.
fn verified_segment(segment: &str) -> std::result::Result<&str, HttpOutcome> {
    if segment.trim().is_empty()
        || segment.len() > 256
        || !segment
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(HttpOutcome::NotDispatched {
            reason: "OpenCode request carried an invalid identity".to_owned(),
        });
    }
    Ok(segment)
}

async fn pump_sse(
    mut response: reqwest::Response,
    frames: mpsc::Sender<SseFrame>,
    stream: watch::Sender<StreamState>,
) {
    let mut parser = SseParser::new();
    loop {
        let chunk = match response.chunk().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => {
                let _ = stream.send(StreamState::Closed);
                return;
            }
            Err(_) => {
                let _ = stream.send(StreamState::Closed);
                return;
            }
        };
        for frame in parser.feed(&chunk) {
            if frame == SseFrame::Malformed {
                let _ = stream.send(StreamState::Malformed);
                return;
            }
            // Backpressure, never loss: a slow event task holds this pump
            // (and the server's send buffer) instead of dropping facts.
            if frames.send(frame).await.is_err() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn installed_write_errors_classify() {
        let invalid = RawResponse {
            status: 400,
            body: br#"{"_tag":"InvalidRequestError","message":"Expected a valid JSON body","kind":"Payload"}"#.to_vec(),
        };
        assert!(matches!(
            map_write_error("session/prompt", &invalid),
            HttpOutcome::Rejected { .. }
        ));
        let gone = RawResponse {
            status: 404,
            body: br#"{"_tag":"SessionNotFoundError","sessionID":"s","message":"gone"}"#.to_vec(),
        };
        assert!(matches!(
            map_write_error("session/prompt", &gone),
            HttpOutcome::Rejected { .. }
        ));
        let auth = RawResponse {
            status: 401,
            body: br#"{"_tag":"UnauthorizedError","message":"Authentication required"}"#.to_vec(),
        };
        assert!(matches!(
            map_write_error("session/prompt", &auth),
            HttpOutcome::Rejected { .. }
        ));
        for (status, body) in [
            (409, r#"{"_tag":"SessionBusyError","message":"busy"}"#),
            (500, r#"{"_tag":"UnknownError","message":"boom"}"#),
            (400, r#"{"unexpected":"shape"}"#),
            (204, ""),
        ] {
            let response = RawResponse {
                status,
                body: body.as_bytes().to_vec(),
            };
            assert!(
                matches!(
                    map_write_error("session/prompt", &response),
                    HttpOutcome::Unknown { .. }
                ),
                "status={status} body={body}"
            );
        }
    }

    #[test]
    fn path_segments_reject_anything_but_an_id() {
        assert_eq!(
            verified_segment("ses_abc123.X-9").unwrap(),
            "ses_abc123.X-9"
        );
        for bad in ["", " ", "a/b", "a?b", "a%b", "ses id", &"x".repeat(257)] {
            assert!(
                matches!(
                    verified_segment(bad),
                    Err(HttpOutcome::NotDispatched { .. })
                ),
                "{bad:?}"
            );
        }
    }
}
