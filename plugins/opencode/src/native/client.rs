//! HTTP/SSE client for one per-Agent `opencode serve`.
//!
//! Installed transport (v2.2.7): plain HTTP on loopback, routes under
//! `/api/...`, success bodies wrapped as `{"data": ...}`, errors as
//! `{"_tag": ..., "message": ...}`, auth as
//! `Authorization: Basic base64("opencode:" + password)` where the password
//! is the server's `OPENCODE_PASSWORD`. The client never retries (a repeat
//! would be a second native write) and never follows redirects.

use std::{
    collections::HashSet,
    path::{Path, PathBuf},
};

use eliot_core::{Error, Result, limits::MAX_NATIVE_INBOUND_FRAME_BYTES};
use eliot_plugin::native_stdio::{NativeStreamFault, StreamState};
use reqwest::{Client, header::HeaderValue, redirect, retry};
use tokio::{
    sync::{mpsc, watch},
    time::{Instant, timeout_at},
};

use super::types::SessionPage;
use crate::native::{
    sse::{SseFrame, SseParser},
    types::{
        AgentList, ConfigEntry, DataEnvelope, ErrorBody, FormInfo, FormReplyRequest, InboxPage,
        InboxUser, InterruptResponse, LocationRef, McpServerReadback, MessagePage, ModelRef,
        NativeModelEntry, PermissionReplyRequest, PermissionRequest, PermissionRule, PromptRequest,
        SessionCreateRequest, SessionInfo, SwitchAgentRequest, SwitchModelRequest,
    },
};

const SSE_QUEUE: usize = 64;
// OpenCode 2.2.7 uses this header to select the native directory context.
const OPENCODE_DIRECTORY_HEADER: &str = "x-opencode-directory";

pub(crate) fn server_native_directory(path: &Path) -> std::io::Result<PathBuf> {
    let canonical = std::fs::canonicalize(path)?;
    #[cfg(windows)]
    {
        use std::os::windows::ffi::{OsStrExt, OsStringExt};

        const VERBATIM_PREFIX: [u16; 4] = [92, 92, 63, 92];
        let wide: Vec<u16> = canonical.as_os_str().encode_wide().collect();
        if wide.starts_with(&VERBATIM_PREFIX) {
            let is_unc = wide.len() >= 8
                && wide[4] <= 0x7f
                && (wide[4] as u8).eq_ignore_ascii_case(&b'U')
                && wide[5] <= 0x7f
                && (wide[5] as u8).eq_ignore_ascii_case(&b'N')
                && wide[6] <= 0x7f
                && (wide[6] as u8).eq_ignore_ascii_case(&b'C')
                && wide[7] == 92;
            if is_unc {
                let mut native = vec![92, 92];
                native.extend_from_slice(&wide[8..]);
                return Ok(PathBuf::from(std::ffi::OsString::from_wide(&native)));
            }
            let is_drive = wide.len() >= 7
                && wide[4] <= 0x7f
                && (wide[4] as u8).is_ascii_alphabetic()
                && wide[5] == 58
                && wide[6] == 92;
            if is_drive {
                return Ok(PathBuf::from(std::ffi::OsString::from_wide(&wide[4..])));
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "OpenCode workspace has no server-native Windows spelling",
            ));
        }
    }
    Ok(canonical)
}

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

/// Validated facts from one complete, unfiltered native session listing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArchiveSessionInventory {
    pub target_present: bool,
    pub has_children: bool,
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

/// Typed client bound to one owned server and its server-native directory.
/// `Clone` shares the connection pool; the password travels only in the auth
/// header, never in logs.
#[derive(Clone)]
pub struct OpenCodeHttp {
    client: Client,
    base: String,
    password: String,
    directory: String,
    directory_header: HeaderValue,
}

impl OpenCodeHttp {
    /// Bind every native request to the server-native directory used for cwd.
    pub fn new(port: u16, password: String, directory: &Path) -> Result<Self> {
        let directory = server_native_directory(directory)
            .map_err(|_| Error::new("invalid", "OpenCode directory could not be canonicalized"))?;
        let directory = directory.to_string_lossy().into_owned();
        // OpenCode 2.2.7 URI-decodes this header. Raw UTF-8 is read as
        // Latin-1 by its HTTP stack (observed on Linux), changing the location.
        // Reuse the query component encoder; a literal '%' must be escaped too.
        let directory_header =
            HeaderValue::from_str(&encode_component(&directory)).map_err(|_| {
                Error::new(
                    "invalid",
                    "OpenCode directory cannot be used as an HTTP header",
                )
            })?;
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
            directory,
            directory_header,
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

    /// `POST /api/session`: create a session without input. An omitted
    /// model defers to the configured native default. `permissions` carries
    /// the per-session ruleset the installed server merges after every
    /// agent/project rule with last-match-wins evaluation; the launch path
    /// sets the subagent deny here for `deny` profiles.
    pub async fn create_session(
        &self,
        title: &str,
        model: Option<&ModelRef>,
        directory: &str,
        permissions: Option<&[PermissionRule]>,
        deadline: Instant,
    ) -> std::result::Result<SessionInfo, HttpOutcome> {
        let path = server_native_directory(Path::new(directory)).map_err(|_| {
            HttpOutcome::NotDispatched {
                reason: "OpenCode session directory could not be canonicalized".to_owned(),
            }
        })?;
        let location_directory = path.to_string_lossy().into_owned();
        if location_directory != self.directory {
            return Err(HttpOutcome::NotDispatched {
                reason: "OpenCode session directory did not match the owned server location"
                    .to_owned(),
            });
        }
        let body = serde_json::to_value(SessionCreateRequest {
            title: Some(title.to_owned()),
            model: model.cloned(),
            location: Some(LocationRef {
                directory: location_directory,
            }),
            permissions: permissions.map(<[PermissionRule]>::to_vec),
        })
        .map_err(|_| HttpOutcome::unknown("OpenCode session/create params could not be encoded"))?;
        if Instant::now() >= deadline {
            return Err(HttpOutcome::NotDispatched {
                reason: "OpenCode session/create deadline elapsed before dispatch".to_owned(),
            });
        }
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

    /// A stored session carries the deny rule exactly when the wildcard
    /// subagent deny is the last rule of the entire session ruleset. The
    /// installed server matches rule actions with wildcard patterns (so a
    /// later `*` allow also matches subagent actions) and evaluates with
    /// last-match-wins over session rules merged after every agent/project
    /// rule (`packages/core/src/permission.ts` evaluator). Any rule after
    /// the deny — a `subagent` allow for one agent, a `*` allow, or anything
    /// else — can beat it, so only the final position verifies. The rule
    /// must name the installed `subagent` action with a wildcard resource:
    /// narrower resources would leave other agents callable, and the legacy
    /// `task` key is not evaluated by the installed tool check.
    pub fn session_subagent_deny_verified(info: &SessionInfo) -> bool {
        info.permissions.as_deref().is_some_and(|rules| {
            rules.last().is_some_and(|rule| {
                rule.action == "subagent" && rule.resource == "*" && rule.effect == "deny"
            })
        })
    }

    /// Preserve the stored session rules and append the wildcard subagent
    /// deny as the final entry of the entire ruleset. An already-correct
    /// ruleset (the last rule is the wildcard deny) is returned unchanged so
    /// a repair never duplicates it or drops unrelated session restrictions.
    /// Appending last keeps the deny winning under the installed
    /// last-match-wins evaluation.
    pub fn with_subagent_deny(existing: &[PermissionRule]) -> Vec<PermissionRule> {
        if existing.last().is_some_and(|rule| {
            rule.action == "subagent" && rule.resource == "*" && rule.effect == "deny"
        }) {
            return existing.to_vec();
        }
        let mut rules = existing.to_vec();
        rules.push(PermissionRule {
            action: "subagent".to_owned(),
            resource: "*".to_owned(),
            effect: "deny".to_owned(),
        });
        rules
    }

    /// `PATCH /api/session/{id}` (`session.update`): store the per-session
    /// ruleset. 204 applies; the caller proves the effective value with
    /// `session.get`. Used only to repair a resumed session that predates
    /// the launch path's deny ruleset: the caller passes the preserved
    /// stored rules with the wildcard deny appended as the final entry, so
    /// unrelated session restrictions survive. Sessions the launch path
    /// creates already carry the deny.
    pub async fn update_session_permissions(
        &self,
        session_id: &str,
        permissions: &[PermissionRule],
        deadline: Instant,
    ) -> std::result::Result<(), HttpOutcome> {
        let path = format!("/api/session/{}", verified_segment(session_id)?,);
        let body = serde_json::to_value(serde_json::json!({ "permissions": permissions }))
            .map_err(|_| {
                HttpOutcome::unknown("OpenCode session/update params could not be encoded")
            })?;
        let response = self
            .send(reqwest::Method::PATCH, &path, &[], Some(body), deadline)
            .await?;
        if response.status == 204 {
            return Ok(());
        }
        Err(map_write_error("session/update", &response))
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

    /// One `GET /api/session` (`session.list`) page on this client's owned
    /// server and store. It sends only the 100-row page size and optional native
    /// cursor: no directory, search, parent, project, subpath, or archived
    /// filters. The fixed `x-opencode-directory` header still binds the native
    /// directory context. A complete inventory must follow every `cursor.next`.
    pub async fn list_sessions(
        &self,
        cursor: Option<&str>,
        deadline: Instant,
    ) -> Result<SessionPage> {
        let query: Vec<(&str, &str)> = match cursor {
            Some(cursor) => vec![("limit", "100"), ("cursor", cursor)],
            None => vec![("limit", "100")],
        };
        let response = self
            .send(reqwest::Method::GET, "/api/session", &query, None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status != 200 {
            return Err(readback_error("session list", &response));
        }
        serde_json::from_slice(&response.body)
            .map_err(|_| Error::new("native_readback", "OpenCode session list was invalid"))
    }

    /// Read the exact retained session. A 404 is only a candidate absence;
    /// the caller must validate it against a complete session inventory.
    pub async fn archive_session_present(
        &self,
        session_id: &str,
        deadline: Instant,
    ) -> Result<bool> {
        let path = format!(
            "/api/session/{}",
            verified_segment(session_id)
                .map_err(|_| Error::new("invalid", "OpenCode session identity is invalid"))?
        );
        let response = self
            .send(reqwest::Method::GET, &path, &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 404 {
            return Ok(false);
        }
        if response.status != 200 {
            return Err(readback_error("session archive lookup", &response));
        }
        let envelope: DataEnvelope<SessionInfo> = serde_json::from_slice(&response.body)
            .map_err(|_| Error::new("native_readback", "OpenCode session readback was invalid"))?;
        if envelope.data.id != session_id || !self.session_matches_directory(&envelope.data) {
            return Err(Error::new(
                "native_readback",
                "OpenCode session readback did not match the retained id and directory",
            ));
        }
        Ok(true)
    }

    /// Exhaust the unfiltered native session list on this same server/store
    /// and directory-bound client. The OpenCode contract defaults to 50 rows,
    /// so each request asks for 100 and follows every opaque `cursor.next`
    /// without an aggregate row/page cap. It validates page cursors, unique
    /// session identities, parent identities, locations, target binding, and
    /// child relationships before returning any facts.
    pub async fn archive_session_inventory(
        &self,
        session_id: &str,
        deadline: Instant,
    ) -> Result<ArchiveSessionInventory> {
        if verified_segment(session_id).is_err() {
            return Err(Error::new(
                "invalid",
                "OpenCode session identity is invalid",
            ));
        }
        let mut cursor: Option<String> = None;
        let mut seen_cursors = HashSet::new();
        let mut seen_ids = HashSet::new();
        let mut inventory = ArchiveSessionInventory {
            target_present: false,
            has_children: false,
        };

        loop {
            let page = self.list_sessions(cursor.as_deref(), deadline).await?;
            for session in page.data {
                if verified_segment(&session.id).is_err() || !seen_ids.insert(session.id.clone()) {
                    return Err(Error::new(
                        "native_readback",
                        "OpenCode session list contained an invalid or duplicate identity",
                    ));
                }
                if session
                    .parent_id
                    .as_deref()
                    .is_some_and(|parent| verified_segment(parent).is_err())
                {
                    return Err(Error::new(
                        "native_readback",
                        "OpenCode session list contained an invalid parent identity",
                    ));
                }
                let Some(location) = session.location.as_ref() else {
                    return Err(Error::new(
                        "native_readback",
                        "OpenCode session list omitted a required directory location",
                    ));
                };
                if location.directory.trim().is_empty()
                    || location.directory.chars().any(char::is_control)
                    || !Path::new(&location.directory).is_absolute()
                {
                    return Err(Error::new(
                        "native_readback",
                        "OpenCode session list contained an invalid directory location",
                    ));
                }
                if session.id == session_id {
                    if !self.session_matches_directory(&session) {
                        return Err(Error::new(
                            "native_readback",
                            "OpenCode session list id did not match the retained directory",
                        ));
                    }
                    inventory.target_present = true;
                }
                if session.parent_id.as_deref() == Some(session_id) {
                    inventory.has_children = true;
                }
            }

            match page.cursor.next {
                None => return Ok(inventory),
                Some(next)
                    if next.trim().is_empty()
                        || next.chars().any(char::is_control)
                        || !seen_cursors.insert(next.clone()) =>
                {
                    return Err(Error::new(
                        "native_readback",
                        "OpenCode session list cursor was empty or repeated",
                    ));
                }
                Some(next) => cursor = Some(next),
            }
        }
    }

    fn session_matches_directory(&self, session: &SessionInfo) -> bool {
        session
            .location
            .as_ref()
            .is_some_and(|location| location.directory == self.directory)
    }

    /// `DELETE /api/session/{id}` removes the session and any child sessions.
    /// The caller first proves the exact retained session has no children.
    /// `Ok(true)` is the native 204; `Ok(false)` is a native 404 that still
    /// requires complete-list confirmation.
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

    /// `GET /api/mcp` (`mcp.list`): installed MCP inventory — configured
    /// server names with connection status (`connected`, `pending`,
    /// `disabled`, `failed`, `needs_auth`). Verified live on 2.2.7: an
    /// Agent-less `swarm mcp --require-agent` entry reports `failed` with
    /// the refusal text, and the query itself needs no session. A read, so
    /// any failure is `native_readback`, never `unknown`.
    pub async fn list_mcp_servers(&self, deadline: Instant) -> Result<Vec<McpServerReadback>> {
        let response = self
            .send(reqwest::Method::GET, "/api/mcp", &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            let envelope: DataEnvelope<Vec<McpServerReadback>> =
                serde_json::from_slice(&response.body).map_err(|_| {
                    Error::new("native_readback", "OpenCode MCP readback was invalid")
                })?;
            return Ok(envelope.data);
        }
        Err(readback_error("MCP list", &response))
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

    /// `GET /api/model`: effective model catalogue. The endpoint answers
    /// an array; the caller selects the exact API model id locally. A read,
    /// so any failure is `native_readback`, never `unknown`.
    pub async fn list_native_models(&self, deadline: Instant) -> Result<Vec<NativeModelEntry>> {
        let response = self
            .send(reqwest::Method::GET, "/api/model", &[], None, deadline)
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status == 200 {
            if let Ok(envelope) =
                serde_json::from_slice::<DataEnvelope<Vec<NativeModelEntry>>>(&response.body)
            {
                return Ok(envelope.data);
            }
            if let Ok(models) = serde_json::from_slice::<Vec<NativeModelEntry>>(&response.body) {
                return Ok(models);
            }
            return Err(Error::new(
                "native_readback",
                "OpenCode model readback was invalid",
            ));
        }
        Err(readback_error("model list", &response))
    }

    /// Location default when there is no explicit session selection.
    pub async fn default_model(&self, deadline: Instant) -> Result<Option<NativeModelEntry>> {
        let response = self
            .send(
                reqwest::Method::GET,
                "/api/model/default",
                &[],
                None,
                deadline,
            )
            .await
            .map_err(readback_outcome_as_error)?;
        if response.status != 200 {
            return Err(readback_error("model default", &response));
        }
        serde_json::from_slice::<DataEnvelope<Option<NativeModelEntry>>>(&response.body)
            .map(|envelope| envelope.data)
            .map_err(|_| {
                Error::new(
                    "native_readback",
                    "OpenCode default model response was invalid",
                )
            })
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

    /// `POST /api/rpc/eliot-context/bind` over the existing authenticated
    /// transport. Idempotent per session id; a foreign id echoes the bound
    /// one so the caller detects the mismatch by comparison. Classified
    /// like a write so an uncertain bind never advertises `turn_safe_point`.
    pub async fn eliot_context_bind(
        &self,
        session_id: &str,
        deadline: Instant,
    ) -> std::result::Result<crate::native::types::EliotContextBindOutput, HttpOutcome> {
        let input = crate::native::types::EliotContextBindInput {
            session_id: session_id.to_owned(),
        };
        self.eliot_context_rpc("bind", input, deadline).await
    }

    /// `POST /api/rpc/eliot-context/next`: long-poll for the next bound-
    /// session context hook. A poll carries no native effect, so a lost
    /// reply is retried by the caller's loop; it never latches unknown.
    pub async fn eliot_context_next(
        &self,
        session_id: &str,
        wait_ms: i64,
        deadline: Instant,
    ) -> std::result::Result<crate::native::types::EliotContextNextOutput, HttpOutcome> {
        let input = crate::native::types::EliotContextNextInput {
            session_id: session_id.to_owned(),
            wait_ms,
        };
        self.eliot_context_rpc("next", input, deadline).await
    }

    /// `POST /api/rpc/eliot-context/resolve`: settle exactly one pending
    /// hook. The caller must invoke this at most once per hook and never
    /// retry an uncertain outcome (a repeat risks a second effect).
    pub async fn eliot_context_resolve(
        &self,
        input: crate::native::types::EliotContextResolveInput,
        deadline: Instant,
    ) -> std::result::Result<crate::native::types::EliotContextResolveOutput, HttpOutcome> {
        self.eliot_context_rpc("resolve", input, deadline).await
    }

    /// One `POST /api/rpc/eliot-context/{method}` round trip. Success is
    /// `200 {output}`; 400/401/404 prove non-admission (`Rejected`), any
    /// other outcome after dispatch is ambiguous (`Unknown`). The client
    /// never retries: a repeat would be a second native write.
    async fn eliot_context_rpc<T, U>(
        &self,
        method: &str,
        input: T,
        deadline: Instant,
    ) -> std::result::Result<U, HttpOutcome>
    where
        T: serde::Serialize,
        U: for<'de> serde::Deserialize<'de>,
    {
        let path = format!("/api/rpc/eliot-context/{method}");
        let body = serde_json::json!({"input": input});
        let response = self
            .send(reqwest::Method::POST, &path, &[], Some(body), deadline)
            .await?;
        if response.status == 200 {
            let envelope: serde_json::Value = serde_json::from_slice(&response.body)
                .map_err(|_| HttpOutcome::unknown("OpenCode context RPC returned no output"))?;
            let output = envelope
                .get("output")
                .cloned()
                .ok_or_else(|| HttpOutcome::unknown("OpenCode context RPC returned no output"))?;
            return serde_json::from_value::<U>(output)
                .map_err(|_| HttpOutcome::unknown("OpenCode context RPC output was invalid"));
        }
        Err(map_write_error(
            &format!("eliot-context/{method}"),
            &response,
        ))
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
            .basic_auth("opencode", Some(&self.password))
            .header(OPENCODE_DIRECTORY_HEADER, self.directory_header.clone());
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
            .basic_auth("opencode", Some(&self.password))
            .header(OPENCODE_DIRECTORY_HEADER, self.directory_header.clone());
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
                if body.len().saturating_add(chunk.len()) > MAX_NATIVE_INBOUND_FRAME_BYTES {
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

/// URI component encoding for native directory headers and query pairs.
fn encode_component(text: &str) -> String {
    let mut out = String::new();
    for byte in text.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

/// Percent-encode query pairs without the `+`-for-space rule.
fn encode_query(pairs: &[(&str, &str)]) -> String {
    let mut out = String::new();
    for (index, (key, value)) in pairs.iter().enumerate() {
        if index > 0 {
            out.push('&');
        }
        out.push_str(&encode_component(key));
        out.push('=');
        out.push_str(&encode_component(value));
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
