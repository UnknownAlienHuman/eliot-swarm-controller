//! HTTP/SSE client for one per-Agent `opencode serve`.
//!
//! Installed transport (v2.0.32): plain HTTP on loopback, routes under
//! `/api/...`, success bodies wrapped as `{"data": ...}`, errors as
//! `{"_tag": ..., "message": ...}`, auth as
//! `Authorization: Basic base64("opencode:" + password)` where the password
//! is the server's `OPENCODE_PASSWORD`. The client never retries (a repeat
//! would be a second native write) and never follows redirects.

use eliot_core::{Error, Result};
use eliot_plugin::native_stdio::{NativeStreamFault, StreamState};
use reqwest::{Client, redirect, retry};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};

use crate::native::{
    sse::{SseFrame, SseParser},
    types::{
        AgentList, ConfigEntry, DataEnvelope, ErrorBody, FormInfo, FormReplyRequest, InboxPage,
        InboxUser, InterruptResponse, LocationRef, MessagePage, ModelRef, PermissionReplyRequest,
        PermissionRequest, PromptRequest, SessionCreateRequest, SessionInfo, SwitchAgentRequest,
        SwitchModelRequest,
    },
};

/// Largest single HTTP body accepted. Prompt/readback payloads are small;
/// anything larger is a protocol violation, never truncated into a decision.
const MAX_HTTP_BYTES: usize = 1024 * 1024;
const SSE_QUEUE: usize = 64;

/// Classified native HTTP outcome. `Rejected` is authoritative
/// non-admission (a complete error body proving no effect);
/// `NotDispatched` proves no byte was sent (request construction,
/// invalid identity, or TCP connect failure); `Unknown` is every
/// ambiguous case after a write may have been admitted.
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

    /// Exact absence readback: `Ok(true)` on a 404, `Ok(false)` on
    /// 200, `Err` on anything ambiguous (a transport failure never proves
    /// absence). The installed server answers a missing session with an
    /// EMPTY 404 on every session route; a tagged `SessionNotFoundError`
    /// body proves the same absence where one is sent.
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
        if response.status == 404 {
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
    /// observed on the event stream. The send path leaves `delivery`
    /// native-default.
    pub async fn prompt(
        &self,
        session_id: &str,
        text: &str,
        deadline: Instant,
    ) -> std::result::Result<InboxUser, HttpOutcome> {
        self.prompt_with_delivery(session_id, text, None, deadline)
            .await
    }

    /// `POST /api/session/{id}/prompt` with explicit `delivery`
    /// (`Session.Inbox.Delivery`): the steer path names `steer` so a
    /// running execution re-drains the input at a step boundary instead
    /// of queueing it behind later work.
    pub async fn prompt_with_delivery(
        &self,
        session_id: &str,
        text: &str,
        delivery: Option<&str>,
        deadline: Instant,
    ) -> std::result::Result<InboxUser, HttpOutcome> {
        let path = format!("/api/session/{}/prompt", verified_segment(session_id)?);
        let body = serde_json::to_value(PromptRequest {
            text: text.to_owned(),
            delivery: delivery.map(str::to_owned),
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

    /// `POST /api/session/{id}/interrupt` (`session.interrupt`):
    /// abort the session's current execution. The 200 body is unwrapped
    /// (`{"interrupted": bool}`), unlike every other success body: `true`
    /// means an execution owned by this server was interrupted, `false`
    /// proves no execution was running, never an error. A 404 proves the
    /// session is gone (no effect).
    pub async fn interrupt(
        &self,
        session_id: &str,
        deadline: Instant,
    ) -> std::result::Result<bool, HttpOutcome> {
        let path = format!("/api/session/{}/interrupt", verified_segment(session_id)?);
        let response = self
            .send(reqwest::Method::POST, &path, &[], None, deadline)
            .await?;
        if response.status == 200 {
            let body: InterruptResponse = serde_json::from_slice(&response.body)
                .map_err(|_| HttpOutcome::unknown("OpenCode interrupt answer was invalid"))?;
            return Ok(body.interrupted);
        }
        Err(map_write_error("session/interrupt", &response))
    }

    /// `DELETE /api/session/{id}/inbox/{inboxID}` (`session.inbox.cancel`):
    /// drop one pending input that has not been delivered. 204 also
    /// answers the no-op (already delivered or gone), so success never
    /// proves removal: callers settle unknown regardless and treat the
    /// write as best-effort orphan prevention for an exact inbox id.
    pub async fn cancel_inbox(
        &self,
        session_id: &str,
        inbox_id: &str,
        deadline: Instant,
    ) -> std::result::Result<(), HttpOutcome> {
        let path = format!(
            "/api/session/{}/inbox/{}",
            verified_segment(session_id)?,
            verified_segment(inbox_id)?
        );
        let response = self
            .send(reqwest::Method::DELETE, &path, &[], None, deadline)
            .await?;
        if response.status == 204 {
            return Ok(());
        }
        Err(map_write_error("session.inbox.cancel", &response))
    }

    /// `GET /api/session/{id}/permission` (`session.permission.list`):
    /// pending permission requests. A read, so any failure is
    /// `native_readback`, never `unknown`.
    pub async fn list_permissions(
        &self,
        session_id: &str,
        deadline: Instant,
    ) -> Result<Vec<PermissionRequest>> {
        let path = format!(
            "/api/session/{}/permission",
            verified_segment(session_id)
                .map_err(|_| Error::new("invalid", "OpenCode session identity is invalid"))?
        );
        let response = self
            .send(reqwest::Method::GET, &path, &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            let envelope: DataEnvelope<Vec<PermissionRequest>> =
                serde_json::from_slice(&response.body).map_err(|_| {
                    Error::new(
                        "native_readback",
                        "OpenCode permission readback was invalid",
                    )
                })?;
            return Ok(envelope.data);
        }
        Err(readback_error("permission list", &response))
    }

    /// `POST /api/session/{id}/permission/{requestID}/reply`
    /// (`session.permission.reply`): decide one pending permission request.
    /// 204 applies; 400/404 prove the reply was not admitted (invalid
    /// decision, or the request settled or never existed — both answered
    /// with an empty body by the installed server).
    pub async fn reply_permission(
        &self,
        session_id: &str,
        request_id: &str,
        decision: &str,
        message: Option<String>,
        deadline: Instant,
    ) -> std::result::Result<(), HttpOutcome> {
        let path = format!(
            "/api/session/{}/permission/{}/reply",
            verified_segment(session_id)?,
            verified_segment(request_id)?,
        );
        let body = serde_json::to_value(PermissionReplyRequest {
            decision: decision.to_owned(),
            message,
        })
        .map_err(|_| HttpOutcome::unknown("OpenCode reply params could not be encoded"))?;
        let response = self
            .send(reqwest::Method::POST, &path, &[], Some(body), deadline)
            .await?;
        if response.status == 204 {
            return Ok(());
        }
        Err(map_write_error("session.permission.reply", &response))
    }

    /// `GET /api/session/{id}/form` (`session.form.list`): pending forms.
    /// A read, so any failure is `native_readback`, never `unknown`.
    pub async fn list_forms(&self, session_id: &str, deadline: Instant) -> Result<Vec<FormInfo>> {
        let path = format!(
            "/api/session/{}/form",
            verified_segment(session_id)
                .map_err(|_| Error::new("invalid", "OpenCode session identity is invalid"))?
        );
        let response = self
            .send(reqwest::Method::GET, &path, &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            let envelope: DataEnvelope<Vec<FormInfo>> = serde_json::from_slice(&response.body)
                .map_err(|_| Error::new("native_readback", "OpenCode form readback was invalid"))?;
            return Ok(envelope.data);
        }
        Err(readback_error("form list", &response))
    }

    /// `POST /api/session/{id}/form/{formID}/reply` (`session.form.reply`):
    /// answer one pending form. 204 applies; 400 proves the answer was
    /// refused (not among the offered schema), 404/409 prove the form is
    /// gone or already settled — all answered with an empty body.
    pub async fn reply_form(
        &self,
        session_id: &str,
        form_id: &str,
        answer: serde_json::Map<String, serde_json::Value>,
        deadline: Instant,
    ) -> std::result::Result<(), HttpOutcome> {
        let path = format!(
            "/api/session/{}/form/{}/reply",
            verified_segment(session_id)?,
            verified_segment(form_id)?,
        );
        let body = serde_json::to_value(FormReplyRequest { answer })
            .map_err(|_| HttpOutcome::unknown("OpenCode reply params could not be encoded"))?;
        let response = self
            .send(reqwest::Method::POST, &path, &[], Some(body), deadline)
            .await?;
        if response.status == 204 {
            return Ok(());
        }
        if response.status == 409 {
            return Err(HttpOutcome::Rejected {
                status: Some(409),
                reason: "OpenCode refused the write (already_settled): the form is already settled"
                    .to_owned(),
            });
        }
        Err(map_write_error("session.form.reply", &response))
    }

    /// `DELETE /api/session/{id}/form/{formID}`: cancel one pending form
    /// (the native deny). 204 applies; 404 proves the form is already gone
    /// and 409 (`FormAlreadySettledError`) proves it is already settled.
    pub async fn delete_form(
        &self,
        session_id: &str,
        form_id: &str,
        deadline: Instant,
    ) -> std::result::Result<(), HttpOutcome> {
        let path = format!(
            "/api/session/{}/form/{}",
            verified_segment(session_id)?,
            verified_segment(form_id)?,
        );
        let response = self
            .send(reqwest::Method::DELETE, &path, &[], None, deadline)
            .await?;
        if response.status == 204 {
            return Ok(());
        }
        if response.status == 409 {
            return Err(HttpOutcome::Rejected {
                status: Some(409),
                reason: "OpenCode refused the write (already_settled): the form is already settled"
                    .to_owned(),
            });
        }
        Err(map_write_error("session.form.delete", &response))
    }

    /// `GET /api/session/{id}/inbox` (`session.inbox.list`): durable
    /// enqueued work not yet delivered. A read, so any failure is
    /// `native_readback`, never `unknown`.
    pub async fn list_inbox(
        &self,
        session_id: &str,
        deadline: Instant,
    ) -> Result<Vec<serde_json::Value>> {
        let path = format!(
            "/api/session/{}/inbox",
            verified_segment(session_id)
                .map_err(|_| Error::new("invalid", "OpenCode session identity is invalid"))?
        );
        let response = self
            .send(reqwest::Method::GET, &path, &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            let page: InboxPage = serde_json::from_slice(&response.body).map_err(|_| {
                Error::new("native_readback", "OpenCode inbox readback was invalid")
            })?;
            return Ok(page.data);
        }
        Err(readback_error("inbox list", &response))
    }

    /// `POST /api/session/{id}/model` (`session.switchModel`): set the
    /// session model. 204 applies; the caller proves the effective value
    /// with `session.get` (the installed server validates nothing).
    pub async fn switch_model(
        &self,
        session_id: &str,
        model: &ModelRef,
        deadline: Instant,
    ) -> std::result::Result<(), HttpOutcome> {
        let path = format!("/api/session/{}/model", verified_segment(session_id)?,);
        let body = serde_json::to_value(SwitchModelRequest {
            model: model.clone(),
        })
        .map_err(|_| HttpOutcome::unknown("OpenCode switch params could not be encoded"))?;
        let response = self
            .send(reqwest::Method::POST, &path, &[], Some(body), deadline)
            .await?;
        if response.status == 204 {
            return Ok(());
        }
        Err(map_write_error("session.switchModel", &response))
    }

    /// `POST /api/session/{id}/agent` (`session.switchAgent`): set the
    /// session agent. 204 applies; the caller proves the effective value
    /// with `session.get`.
    pub async fn switch_agent(
        &self,
        session_id: &str,
        agent: &str,
        deadline: Instant,
    ) -> std::result::Result<(), HttpOutcome> {
        let path = format!("/api/session/{}/agent", verified_segment(session_id)?,);
        let body = serde_json::to_value(SwitchAgentRequest {
            agent: agent.to_owned(),
        })
        .map_err(|_| HttpOutcome::unknown("OpenCode switch params could not be encoded"))?;
        let response = self
            .send(reqwest::Method::POST, &path, &[], Some(body), deadline)
            .await?;
        if response.status == 204 {
            return Ok(());
        }
        Err(map_write_error("session.switchAgent", &response))
    }

    /// `GET /api/agent`: installed agent ids for the membership check
    /// before `session.switchAgent`. A read, so any failure is
    /// `native_readback`, never `unknown`.
    pub async fn list_agents(&self, deadline: Instant) -> Result<Vec<String>> {
        let response = self
            .send(reqwest::Method::GET, "/api/agent", &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            let list: AgentList = serde_json::from_slice(&response.body).map_err(|_| {
                Error::new("native_readback", "OpenCode agent readback was invalid")
            })?;
            return Ok(list.data.into_iter().map(|entry| entry.id).collect());
        }
        Err(readback_error("agent list", &response))
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
        // The loopback server is plain HTTP with retry disabled, so
        // reqwest 0.13.5 is_connect (ErrorKind::Connect only) can only fire
        // before the first request byte leaves the socket.
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

/// Map a non-2xx write response. On the owned loopback server (no
/// proxy, auth gate first) the app answers validation refusals with 400,
/// missing subjects with 404, and gate refusals with 401 — usually with an
/// EMPTY body (verified on every session/permission/form route), sometimes
/// with a tagged body (`InvalidRequestError`, `SessionNotFoundError`,
/// `PermissionNotFoundError`, `FormInvalidAnswerError`, ...). Any 400/401/404
/// therefore proves the write was not admitted. 409 conflict, 5xx and anything
/// else stay `Unknown` (the form-reply caller maps its own 409 explicitly).
fn map_write_error(method: &str, response: &RawResponse) -> HttpOutcome {
    let body: Option<ErrorBody> = serde_json::from_slice(&response.body).ok();
    let tag = body.as_ref().and_then(|body| body.tag.as_deref());
    let message = body
        .as_ref()
        .and_then(|body| body.message.as_deref())
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("no message");
    match response.status {
        400 | 401 | 404 => {
            HttpOutcome::rejected(response.status, tag.unwrap_or("refused"), message)
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
                let _ = stream.send(StreamState::Malformed(NativeStreamFault::Unclassified));
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
        // Tagged bodies (the documented shapes) and empty bodies (the
        // installed reality on session/permission/form routes) both prove
        // non-admission on 400/401/404.
        for (status, body) in [
            (
                400,
                r#"{"_tag":"InvalidRequestError","message":"Expected a valid JSON body","kind":"Payload"}"#,
            ),
            (
                404,
                r#"{"_tag":"SessionNotFoundError","sessionID":"s","message":"gone"}"#,
            ),
            (
                404,
                r#"{"_tag":"PermissionNotFoundError","requestID":"per_x","message":"gone"}"#,
            ),
            (
                401,
                r#"{"_tag":"UnauthorizedError","message":"Authentication required"}"#,
            ),
            (400, ""),
            (401, ""),
            (404, ""),
        ] {
            let response = RawResponse {
                status,
                body: body.as_bytes().to_vec(),
            };
            assert!(
                matches!(
                    map_write_error("session/prompt", &response),
                    HttpOutcome::Rejected { .. }
                ),
                "status={status} body={body}"
            );
        }
        for (status, body) in [
            (409, r#"{"_tag":"SessionBusyError","message":"busy"}"#),
            (409, ""),
            (500, r#"{"_tag":"UnknownError","message":"boom"}"#),
            (500, ""),
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

    #[tokio::test]
    async fn refused_connection_is_not_dispatched() {
        // Reserve then release a loopback port: nothing listens, so the
        // TCP connect is refused before any HTTP byte can leave.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();
        drop(listener);

        let http = OpenCodeHttp::new(port, "test-password".to_owned()).expect("client");
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let model = ModelRef {
            id: "test-model".to_owned(),
            provider_id: "test-provider".to_owned(),
            variant: None,
        };
        match http.create_session("test", &model, "/tmp", deadline).await {
            Err(HttpOutcome::NotDispatched { reason }) => {
                assert!(reason.contains("not dispatched"), "reason={reason}");
                assert!(reason.contains("connect"), "reason={reason}");
            }
            other => panic!("refused connect must be NotDispatched, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn dropped_reply_after_request_is_unknown_without_retry() {
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        use tokio::io::AsyncReadExt;

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let port = listener.local_addr().expect("local addr").port();

        let hits = Arc::new(AtomicUsize::new(0));
        let seen = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));

        // Accept loop: count every TCP connect, read its request bytes,
        // then drop without any reply. Runs until the test aborts it.
        let accept_hits = hits.clone();
        let accept_seen = seen.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                let accept =
                    tokio::time::timeout(std::time::Duration::from_millis(500), listener.accept())
                        .await;
                let Ok(Ok((mut socket, _))) = accept else {
                    continue;
                };
                accept_hits.fetch_add(1, Ordering::SeqCst);
                let seen = accept_seen.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let start = tokio::time::Instant::now();
                    loop {
                        if start.elapsed() > std::time::Duration::from_secs(1) {
                            break;
                        }
                        match tokio::time::timeout(
                            std::time::Duration::from_millis(200),
                            socket.read(&mut tmp),
                        )
                        .await
                        {
                            Ok(Ok(0)) => break,
                            Ok(Ok(n)) => {
                                buf.extend_from_slice(&tmp[..n]);
                                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                                    if let Ok(Ok(n)) = tokio::time::timeout(
                                        std::time::Duration::from_millis(100),
                                        socket.read(&mut tmp),
                                    )
                                    .await
                                    {
                                        buf.extend_from_slice(&tmp[..n]);
                                    }
                                    break;
                                }
                            }
                            _ => break,
                        }
                    }
                    if let Ok(mut guard) = seen.lock() {
                        guard.extend_from_slice(&buf);
                    }
                    // Drop without any reply: the write outcome stays ambiguous.
                });
            }
        });

        let http = OpenCodeHttp::new(port, "test-password".to_owned()).expect("client");
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        let model = ModelRef {
            id: "test-model".to_owned(),
            provider_id: "test-provider".to_owned(),
            variant: None,
        };
        match http.create_session("test", &model, "/tmp", deadline).await {
            Err(HttpOutcome::Unknown { reason }) => {
                assert!(reason.contains("after dispatch"), "reason={reason}");
                assert!(reason.contains("unknown"), "reason={reason}");
            }
            other => {
                accept_task.abort();
                panic!("dropped reply must be Unknown, got {other:?}");
            }
        }

        // Give any automatic retry a window to arrive, then prove the
        // client wrote exactly once.
        tokio::time::sleep(std::time::Duration::from_millis(400)).await;
        accept_task.abort();
        assert_eq!(
            hits.load(Ordering::SeqCst),
            1,
            "client must not retry a write"
        );
        let seen = seen.lock().expect("seen").clone();
        assert!(
            seen.windows(b"POST /api/session".len())
                .any(|w| w == b"POST /api/session"),
            "server must have seen the request bytes, got {} bytes",
            seen.len()
        );
    }
}
