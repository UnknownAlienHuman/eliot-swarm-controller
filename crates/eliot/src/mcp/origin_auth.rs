//! Cloudflare application-assertion verification at the HTTP origin.
//! Managed OAuth's opaque Authorization token is never interpreted as a JWT.
//! jsonwebtoken 11.1.0 (MIT, MSRV 1.88) supplies signature/claim verification;
//! no cryptographic or OAuth implementation is copied here.
use axum::http::{HeaderMap, Method, StatusCode};
use eliot_core::{RemoteCallContext, RemoteIdentity};
use eliot_plugin::sync::LockExt;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    errors::ErrorKind,
    jwk::{JwkSet, KeyAlgorithm, PublicKeyUse},
};
use rmcp::transport::streamable_http_server::session::{
    SessionManager, local::LocalSessionManager,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore, watch},
    time::timeout,
};

const MAX_ASSERTION_BYTES: usize = 16 * 1024;
const MAX_JWKS_BYTES: usize = 256 * 1024;
const MAX_SESSIONS: usize = 1024;
const KEY_LIFETIME: Duration = Duration::from_secs(300);
const REFRESH_BACKOFF: Duration = Duration::from_secs(30);
const KEY_REFRESH_TIMEOUT: Duration = Duration::from_secs(10);
const REJECTION_LOG_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) enum Rejection {
    AssertionHeader,
    InvalidAssertion,
    KeyUnavailable,
    KeyUnknown,
    BadSignature,
    Expired,
    NotYetValid,
    AudienceMismatch,
    IssuerMismatch,
    ConfigUnavailable,
    InvalidSession,
    SessionUnavailable,
    SessionNotFound,
    SessionForbidden,
    InvalidSessionResponse,
}

impl Rejection {
    pub fn status(self) -> StatusCode {
        match self {
            Self::KeyUnavailable | Self::ConfigUnavailable | Self::SessionUnavailable => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::SessionNotFound => StatusCode::NOT_FOUND,
            Self::SessionForbidden => StatusCode::FORBIDDEN,
            Self::InvalidSessionResponse => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::UNAUTHORIZED,
        }
    }

    fn code(self) -> &'static str {
        match self {
            Self::AssertionHeader => "assertion_header",
            Self::InvalidAssertion => "invalid_assertion",
            Self::KeyUnavailable => "key_unavailable",
            Self::KeyUnknown => "key_unknown",
            Self::BadSignature => "bad_signature",
            Self::Expired => "expired",
            Self::NotYetValid => "not_yet_valid",
            Self::AudienceMismatch => "audience_mismatch",
            Self::IssuerMismatch => "issuer_mismatch",
            Self::ConfigUnavailable => "config_unavailable",
            Self::InvalidSession => "invalid_session",
            Self::SessionUnavailable => "session_unavailable",
            Self::SessionNotFound => "session_not_found",
            Self::SessionForbidden => "session_forbidden",
            Self::InvalidSessionResponse => "invalid_session_response",
        }
    }
}

#[derive(Deserialize, Serialize)]
struct Claims {
    iss: String,
    aud: Vec<String>,
    sub: String,
    exp: u64,
    nbf: u64,
    #[serde(rename = "type")]
    token_type: String,
}

struct Keys {
    set: JwkSet,
    valid_until: Instant,
    retry_after: Instant,
}

struct Session {
    identity: RemoteIdentity,
    connection_id: String,
    lease: Arc<SessionLease>,
    slot: Arc<SessionSlot>,
}

impl Drop for Session {
    fn drop(&mut self) {
        self.slot.release();
        self.lease.close();
    }
}

/// Reservation is released by the actual SDK handler lifetime, even when an
/// auth binding or an in-flight request still holds an Arc to this reservation.
pub(super) struct SessionSlot(StdMutex<Option<OwnedSemaphorePermit>>);

impl SessionSlot {
    fn release(&self) {
        drop(self.0.lock_recover().take());
    }
}

#[derive(Default)]
pub(super) struct SessionLifetime {
    binding: StdMutex<Option<(Arc<SessionSlot>, Arc<SessionLease>)>>,
}

impl SessionLifetime {
    pub fn bind(&self, verified: &VerifiedRequest) -> Result<(), StatusCode> {
        let mut binding = self.binding.lock_recover();
        match (&*binding, &verified.slot) {
            (None, Some(slot)) => {
                *binding = Some((slot.clone(), verified.lease.clone()));
                Ok(())
            }
            (Some((slot, lease)), candidate)
                if Arc::ptr_eq(lease, &verified.lease)
                    && candidate
                        .as_ref()
                        .is_none_or(|candidate| Arc::ptr_eq(slot, candidate)) =>
            {
                Ok(())
            }
            _ => Err(StatusCode::UNAUTHORIZED),
        }
    }
}

impl Drop for SessionLifetime {
    fn drop(&mut self) {
        if let Some((slot, lease)) = self.binding.lock_recover().take() {
            slot.release();
            lease.close();
        }
    }
}

pub(super) struct OriginAuth {
    /// Private data directory; grant audiences are reloaded from it on every
    /// verification so grant add and revoke apply without a restart. The
    /// team issuer and owner audience tag are pinned at startup: changing
    /// them still needs a restart.
    data_dir: PathBuf,
    issuer: String,
    owner_audience: String,
    client: reqwest::Client,
    keys: Mutex<Keys>,
    key_refresh: Mutex<()>,
    rejection_logs: StdMutex<HashMap<Rejection, Instant>>,
    sessions: Mutex<HashMap<String, Session>>,
    slots: Arc<Semaphore>,
    sdk_sessions: Arc<LocalSessionManager>,
}

/// Only nonsecret, verified identity is propagated through SDK extensions.
#[derive(Clone)]
pub(super) struct VerifiedRequest {
    pub context: RemoteCallContext,
    pub lease: Arc<SessionLease>,
    pub request_deadline: Instant,
    slot: Option<Arc<SessionSlot>>,
}

/// Shared only with the SDK session's existing notification task. Renewal is
/// possible only through a verified later assertion; None means closed.
pub(super) struct SessionLease {
    expiry: watch::Sender<Option<LeaseAuthorization>>,
}

#[derive(Clone, Copy)]
pub(super) struct LeaseAuthorization {
    pub expires_at: u64,
    pub deadline: Instant,
    pub expired: bool,
}

impl SessionLease {
    fn new(expires_at: u64, deadline: Instant) -> Self {
        Self {
            expiry: watch::channel(Some(LeaseAuthorization {
                expires_at,
                deadline,
                expired: false,
            }))
            .0,
        }
    }

    fn extend(&self, expires_at: u64, deadline: Instant) {
        self.expiry.send_if_modified(|current| {
            if let Some(current) = current
                && expires_at > current.expires_at
            {
                current.expires_at = expires_at;
                current.deadline = current.deadline.max(deadline);
                current.expired = false;
                true
            } else {
                false
            }
        });
    }

    pub fn current_authorization(&self) -> Option<LeaseAuthorization> {
        // Expiry is sticky: a later wall-clock rollback cannot revive this
        // authorization. Only extend(), after verified renewal, can rearm it.
        self.expiry.send_if_modified(|current| {
            if let Some(current) = current
                && !current.expired
                && (Instant::now() >= current.deadline || now_seconds() >= current.expires_at)
            {
                current.expired = true;
                true
            } else {
                false
            }
        });
        *self.expiry.borrow()
    }

    pub fn subscribe(&self) -> watch::Receiver<Option<LeaseAuthorization>> {
        self.expiry.subscribe()
    }

    fn close(&self) {
        self.expiry.send_replace(None);
    }
}

pub(super) fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl OriginAuth {
    pub async fn load(
        data_dir: &Path,
        sdk_sessions: Arc<LocalSessionManager>,
    ) -> Result<Self, String> {
        // Validate team, owner audience and grant table at startup (same
        // failure as before) but retain only the team binding: audiences are
        // reloaded on every verification.
        let (issuer, owner_audience, _) = eliot_daemon::load_cloud_access_snapshot(data_dir)
            .map_err(|error| format!("{}: {}", error.code, error.message))?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| "origin verifier HTTP client unavailable")?;
        let auth = Self {
            data_dir: data_dir.to_owned(),
            issuer,
            owner_audience,
            client,
            key_refresh: Mutex::new(()),
            rejection_logs: StdMutex::default(),
            keys: Mutex::new(Keys {
                set: JwkSet { keys: Vec::new() },
                valid_until: Instant::now(),
                retry_after: Instant::now(),
            }),
            sessions: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(MAX_SESSIONS)),
            sdk_sessions,
        };
        auth.refresh()
            .await
            .map_err(|_| "origin verifier signing keys unavailable")?;
        Ok(auth)
    }

    pub fn log_rejection(&self, rejection: Rejection, ray: Option<&str>, call_id: Option<&str>) {
        {
            let mut logged = self.rejection_logs.lock_recover();
            let now = Instant::now();
            if logged
                .get(&rejection)
                .is_some_and(|last| now.duration_since(*last) < REJECTION_LOG_INTERVAL)
            {
                return;
            }
            logged.insert(rejection, now);
        }
        eliot_daemon::log_origin_rejection(
            rejection.status().as_u16(),
            rejection.code(),
            ray,
            call_id,
        );
    }

    async fn fetch_keys(&self) -> Result<JwkSet, Rejection> {
        // Never derive a URL from an unverified JWT's iss/jku/x5u/kid.
        let mut response = self
            .client
            .get(format!("{}/cdn-cgi/access/certs", self.issuer))
            .send()
            .await
            .map_err(|_| Rejection::KeyUnavailable)?;
        if !response.status().is_success() {
            return Err(Rejection::KeyUnavailable);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| Rejection::KeyUnavailable)?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_JWKS_BYTES {
                return Err(Rejection::KeyUnavailable);
            }
            bytes.extend_from_slice(&chunk);
        }
        let set: JwkSet = serde_json::from_slice(&bytes).map_err(|_| Rejection::KeyUnavailable)?;
        if set.keys.is_empty() || set.keys.len() > 32 {
            return Err(Rejection::KeyUnavailable);
        }
        Ok(set)
    }

    async fn refresh(&self) -> Result<(), Rejection> {
        timeout(KEY_REFRESH_TIMEOUT, async {
            let _refresh = self.key_refresh.lock().await;
            self.fetch_and_cache_keys().await
        })
        .await
        .map_err(|_| Rejection::KeyUnavailable)?
    }

    async fn refresh_for_key(&self, kid: &str) -> Result<(), Rejection> {
        timeout(KEY_REFRESH_TIMEOUT, async {
            let _refresh = self.key_refresh.lock().await;
            {
                let keys = self.keys.lock().await;
                let now = Instant::now();
                if now < keys.valid_until && keys.set.find(kid).is_some() {
                    return Ok(());
                }
                if now < keys.retry_after {
                    return Err(Rejection::KeyUnavailable);
                }
            }
            // The cache lock is released before the bounded network refresh.
            self.fetch_and_cache_keys().await
        })
        .await
        .map_err(|_| Rejection::KeyUnavailable)?
    }

    async fn fetch_and_cache_keys(&self) -> Result<(), Rejection> {
        {
            let mut keys = self.keys.lock().await;
            keys.retry_after = Instant::now() + REFRESH_BACKOFF;
        }
        let set = self.fetch_keys().await?;
        let mut keys = self.keys.lock().await;
        keys.set = set;
        keys.valid_until = Instant::now() + KEY_LIFETIME;
        Ok(())
    }

    fn decoding_key(jwk: &jsonwebtoken::jwk::Jwk) -> Result<DecodingKey, Rejection> {
        if jwk.common.key_algorithm != Some(KeyAlgorithm::RS256)
            || jwk.common.public_key_use != Some(PublicKeyUse::Signature)
        {
            return Err(Rejection::InvalidAssertion);
        }
        DecodingKey::from_jwk(jwk).map_err(|_| Rejection::KeyUnavailable)
    }

    async fn verify(&self, assertion: &str) -> Result<(Claims, String), Rejection> {
        if assertion.is_empty() || assertion.len() > MAX_ASSERTION_BYTES {
            return Err(Rejection::InvalidAssertion);
        }
        let header = decode_header(assertion).map_err(|_| Rejection::InvalidAssertion)?;
        if header.alg != Algorithm::RS256 {
            return Err(Rejection::InvalidAssertion);
        }
        let kid = header
            .kid
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or(Rejection::InvalidAssertion)?;
        let key = {
            let keys = self.keys.lock().await;
            if Instant::now() < keys.valid_until {
                keys.set.find(&kid).map(Self::decoding_key).transpose()?
            } else {
                None
            }
        };
        let key = match key {
            Some(key) => key,
            None => {
                self.refresh_for_key(&kid).await?;
                let keys = self.keys.lock().await;
                if Instant::now() >= keys.valid_until {
                    return Err(Rejection::KeyUnavailable);
                }
                let jwk = keys.set.find(&kid).ok_or(Rejection::KeyUnknown)?;
                Self::decoding_key(jwk)?
            }
        };
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.issuer]);
        validation.validate_aud = false;
        validation.set_required_spec_claims(&["iss", "aud", "sub", "exp", "nbf"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        let claims = decode::<Claims>(assertion, &key, &validation)
            .map_err(|error| match error.kind() {
                ErrorKind::InvalidSignature => Rejection::BadSignature,
                ErrorKind::ExpiredSignature => Rejection::Expired,
                ErrorKind::ImmatureSignature => Rejection::NotYetValid,
                ErrorKind::InvalidIssuer => Rejection::IssuerMismatch,
                _ => Rejection::InvalidAssertion,
            })?
            .claims;
        if claims.sub.is_empty()
            || claims.sub.len() > 512
            || claims.sub.chars().any(char::is_control)
            || claims.token_type != "app"
        {
            return Err(Rejection::InvalidAssertion);
        }
        if claims.iss != self.issuer {
            return Err(Rejection::IssuerMismatch);
        }
        let now = now_seconds();
        if claims.exp <= now {
            return Err(Rejection::Expired);
        }
        if claims.nbf > now {
            return Err(Rejection::NotYetValid);
        }
        // One config snapshot per verification: a newly added grant is
        // accepted and a revoked grant is rejected before the next request
        // without a restart, with no values paired across separate reads.
        // A snapshot load failure still fails closed but reports 503: the
        // local config is unavailable (retryable), not an expired grant.
        // Token, issuer, and audience mismatches stay 401, as does a team
        // or owner-audience change, which still needs a restart. No token
        // or secret is logged or persisted here.
        let (issuer, owner_audience, audiences) =
            eliot_daemon::load_cloud_access_snapshot(&self.data_dir)
                .map_err(|_| Rejection::ConfigUnavailable)?;
        if issuer != self.issuer {
            return Err(Rejection::IssuerMismatch);
        }
        if owner_audience != self.owner_audience {
            return Err(Rejection::AudienceMismatch);
        }
        let mut matches = claims
            .aud
            .iter()
            .filter(|claim_audience| audiences.iter().any(|allowed| allowed == *claim_audience));
        let selected = matches.next().cloned().ok_or(Rejection::AudienceMismatch)?;
        if matches.next().is_some() {
            return Err(Rejection::AudienceMismatch);
        }
        Ok((claims, selected))
    }

    pub async fn authenticate(
        &self,
        headers: &mut HeaderMap,
        method: &Method,
    ) -> Result<VerifiedRequest, Rejection> {
        let values = headers
            .get_all("cf-access-jwt-assertion")
            .iter()
            .collect::<Vec<_>>();
        if values.len() != 1 {
            return Err(Rejection::AssertionHeader);
        }
        let assertion = values[0].to_str().map_err(|_| Rejection::AssertionHeader)?;
        let (claims, audience) = self.verify(assertion).await?;
        let observed_at = Instant::now();
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Rejection::InvalidAssertion)?;
        let remaining = Duration::from_secs(claims.exp)
            .checked_sub(epoch)
            .ok_or(Rejection::Expired)?;
        let deadline = observed_at
            .checked_add(remaining)
            .ok_or(Rejection::InvalidAssertion)?;
        let identity = RemoteIdentity {
            issuer: self.issuer.clone(),
            audience,
            subject: claims.sub,
        };
        let session_id = headers
            .get("mcp-session-id")
            .map(|v| v.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| Rejection::InvalidSession)?;
        if headers.get_all("mcp-session-id").iter().count() > 1 {
            return Err(Rejection::InvalidSession);
        }
        let (connection_id, lease, slot) = if let Some(id) = &session_id {
            let mut sessions = self.sessions.lock().await;
            if !self
                .sdk_sessions
                .has_session(&id.clone().into())
                .await
                .map_err(|_| Rejection::SessionUnavailable)?
            {
                drop(sessions.remove(id));
                return Err(Rejection::SessionNotFound);
            }
            let session = sessions.get_mut(id).ok_or(Rejection::SessionNotFound)?;
            if session.identity != identity {
                return Err(Rejection::SessionForbidden);
            }
            if session.lease.current_authorization().is_none() {
                return Err(Rejection::SessionNotFound);
            }
            session.lease.extend(claims.exp, deadline);
            (session.connection_id.clone(), session.lease.clone(), None)
        } else {
            if method != Method::POST {
                return Err(Rejection::InvalidSession);
            }
            // Reclaim a bounded batch before taking a new-session permit.
            // Otherwise a full set of abandoned expired sessions cannot GC.
            self.reap_expired_sessions().await;
            let slot = self
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| Rejection::SessionUnavailable)?;
            (
                ulid::Ulid::new().to_string(),
                Arc::new(SessionLease::new(claims.exp, deadline)),
                Some(Arc::new(SessionSlot(StdMutex::new(Some(slot))))),
            )
        };
        // SDK request Parts must never retain bearer material.
        for name in [
            "cf-access-jwt-assertion",
            "authorization",
            "proxy-authorization",
            "cookie",
            "cf-access-client-id",
            "cf-access-client-secret",
            "cf-access-token",
        ] {
            headers.remove(name);
        }
        let context = RemoteCallContext {
            identity,
            connection_id,
            session_id,
            call_id: ulid::Ulid::new().to_string(),
            mcp_request_id: None,
            expires_at: claims.exp,
        };
        context.validate().map_err(|_| Rejection::InvalidSession)?;
        // Refresh the addressed session before reaping, so a valid renewed
        // assertion can continue its existing SDK session after the old expiry.
        if context.session_id.is_some() {
            self.reap_expired_sessions().await;
        }
        Ok(VerifiedRequest {
            context,
            lease,
            request_deadline: deadline,
            slot,
        })
    }

    // Reap through the SDK's own session lifecycle, not just the auth binding.
    // Work is bounded per authenticated request; no background registry/driver.
    async fn reap_expired_sessions(&self) {
        let expired = {
            let sessions = self.sessions.lock().await;
            // This is the actual pinned SDK manager's public session map;
            // its worker removes idle/dead sessions via close_session.
            let sdk = self.sdk_sessions.sessions.read().await;
            sessions
                .iter()
                .filter(|(id, session)| {
                    !sdk.contains_key(id.as_str())
                        || session
                            .lease
                            .current_authorization()
                            .is_none_or(|authorization| authorization.expired)
                })
                .take(64)
                .map(|(id, _)| id.clone())
                .collect::<Vec<_>>()
        };
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            for id in expired {
                // Serialize closure with refresh and recheck the live lease:
                // a previously captured expiry cannot close a renewed session.
                let mut sessions = self.sessions.lock().await;
                let Some(session) = sessions.get(&id) else {
                    continue;
                };
                let sdk_closed = matches!(
                    self.sdk_sessions.has_session(&id.clone().into()).await,
                    Ok(false)
                );
                if !sdk_closed
                    && session
                        .lease
                        .current_authorization()
                        .is_some_and(|authorization| !authorization.expired)
                {
                    continue;
                }
                session.lease.close();
                if self
                    .sdk_sessions
                    .close_session(&id.clone().into())
                    .await
                    .is_ok()
                {
                    drop(sessions.remove(&id));
                }
            }
        })
        .await;
    }

    pub async fn finish(
        &self,
        request: &VerifiedRequest,
        method: &Method,
        status: StatusCode,
        headers: &HeaderMap,
    ) -> Result<(), Rejection> {
        let mut sessions = self.sessions.lock().await;
        if let Some(id) = &request.context.session_id
            && matches!(
                self.sdk_sessions.has_session(&id.clone().into()).await,
                Ok(false)
            )
        {
            drop(sessions.remove(id));
        }
        if method == Method::DELETE && status.is_success() {
            if let Some(id) = &request.context.session_id
                && let Some(session) = sessions.remove(id)
            {
                session.lease.close();
            }
        } else if status.is_success()
            && request.context.session_id.is_none()
            && let Some(id) = headers.get("mcp-session-id")
        {
            let id = id.to_str().map_err(|_| Rejection::InvalidSessionResponse)?;
            if id.len() > 256 {
                return Err(Rejection::InvalidSessionResponse);
            }
            if request.lease.current_authorization().is_none()
                || !self
                    .sdk_sessions
                    .has_session(&id.to_owned().into())
                    .await
                    .map_err(|_| Rejection::SessionUnavailable)?
            {
                return Err(Rejection::SessionNotFound);
            }
            let slot = request
                .slot
                .clone()
                .ok_or(Rejection::InvalidSessionResponse)?;
            sessions.insert(
                id.to_owned(),
                Session {
                    identity: request.context.identity.clone(),
                    connection_id: request.context.connection_id.clone(),
                    lease: request.lease.clone(),
                    slot,
                },
            );
        }
        Ok(())
    }
}
