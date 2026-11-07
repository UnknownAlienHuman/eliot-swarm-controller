//! Cloudflare application-assertion verification at the HTTP origin.
//! Managed OAuth's opaque Authorization token is never interpreted as a JWT.
//! jsonwebtoken 11.1.0 (MIT, MSRV 1.88) supplies signature/claim verification;
//! no cryptographic or OAuth implementation is copied here.
use axum::http::{HeaderMap, Method, StatusCode};
use eliot_core::{RemoteCallContext, RemoteIdentity};
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyAlgorithm, PublicKeyUse},
};
use rmcp::transport::streamable_http_server::session::{
    SessionManager, local::LocalSessionManager,
};
use serde::Deserialize;
use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, watch};

const MAX_ASSERTION_BYTES: usize = 16 * 1024;
const MAX_JWKS_BYTES: usize = 256 * 1024;
const MAX_SESSIONS: usize = 1024;
const KEY_LIFETIME: Duration = Duration::from_secs(300);
const REFRESH_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Deserialize)]
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
    _slot: Arc<OwnedSemaphorePermit>,
}

pub(super) struct OriginAuth {
    issuer: String,
    audience: String,
    client: reqwest::Client,
    keys: Mutex<Keys>,
    sessions: Mutex<HashMap<String, Session>>,
    slots: Arc<Semaphore>,
    sdk_sessions: Arc<LocalSessionManager>,
}

/// Only nonsecret, verified identity is propagated through SDK extensions.
#[derive(Clone)]
pub(super) struct VerifiedRequest {
    pub context: RemoteCallContext,
    pub lease: Arc<SessionLease>,
    slot: Option<Arc<OwnedSemaphorePermit>>,
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
        let (issuer, audience) =
            eliot_daemon::load_cloud_access_config(data_dir).map_err(|e| e.code)?;
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| "origin verifier HTTP client unavailable")?;
        let auth = Self {
            issuer,
            audience,
            client,
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

    async fn fetch_keys(&self) -> Result<JwkSet, StatusCode> {
        // Never derive a URL from an unverified JWT's iss/jku/x5u/kid.
        let mut response = self
            .client
            .get(format!("{}/cdn-cgi/access/certs", self.issuer))
            .send()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if !response.status().is_success() {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_JWKS_BYTES {
                return Err(StatusCode::SERVICE_UNAVAILABLE);
            }
            bytes.extend_from_slice(&chunk);
        }
        let set: JwkSet =
            serde_json::from_slice(&bytes).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        if set.keys.is_empty() || set.keys.len() > 32 {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        Ok(set)
    }

    async fn refresh(&self) -> Result<(), StatusCode> {
        let mut keys = self.keys.lock().await;
        keys.retry_after = Instant::now() + REFRESH_BACKOFF;
        keys.set = self.fetch_keys().await?;
        keys.valid_until = Instant::now() + KEY_LIFETIME;
        Ok(())
    }

    async fn verify(&self, assertion: &str) -> Result<Claims, StatusCode> {
        if assertion.is_empty() || assertion.len() > MAX_ASSERTION_BYTES {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let header = decode_header(assertion).map_err(|_| StatusCode::UNAUTHORIZED)?;
        if header.alg != Algorithm::RS256 {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let kid = header
            .kid
            .filter(|s| !s.is_empty() && s.len() <= 256)
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let mut keys = self.keys.lock().await;
        let expired = Instant::now() >= keys.valid_until;
        if (expired || keys.set.find(&kid).is_none()) && Instant::now() >= keys.retry_after {
            keys.retry_after = Instant::now() + REFRESH_BACKOFF;
            keys.set = self.fetch_keys().await?;
            keys.valid_until = Instant::now() + KEY_LIFETIME;
        }
        if Instant::now() >= keys.valid_until {
            return Err(StatusCode::SERVICE_UNAVAILABLE);
        }
        let jwk = keys.set.find(&kid).ok_or(StatusCode::UNAUTHORIZED)?;
        if jwk.common.key_algorithm != Some(KeyAlgorithm::RS256)
            || jwk.common.public_key_use != Some(PublicKeyUse::Signature)
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let key = DecodingKey::from_jwk(jwk).map_err(|_| StatusCode::UNAUTHORIZED)?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        validation.set_required_spec_claims(&["iss", "aud", "sub", "exp", "nbf"]);
        validation.validate_nbf = true;
        validation.leeway = 0;
        let claims = decode::<Claims>(assertion, &key, &validation)
            .map_err(|_| StatusCode::UNAUTHORIZED)?
            .claims;
        if claims.sub.is_empty()
            || claims.sub.len() > 512
            || claims.sub.chars().any(char::is_control)
            || claims.token_type != "app"
            || claims.iss != self.issuer
            || !claims.aud.contains(&self.audience)
            || claims.exp <= now_seconds()
            || claims.nbf > now_seconds()
        {
            return Err(StatusCode::UNAUTHORIZED);
        }
        Ok(claims)
    }

    pub async fn authenticate(
        &self,
        headers: &mut HeaderMap,
        method: &Method,
    ) -> Result<VerifiedRequest, StatusCode> {
        let values = headers
            .get_all("cf-access-jwt-assertion")
            .iter()
            .collect::<Vec<_>>();
        if values.len() != 1 {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let assertion = values[0].to_str().map_err(|_| StatusCode::UNAUTHORIZED)?;
        let claims = self.verify(assertion).await?;
        let observed_at = Instant::now();
        let epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        let remaining = Duration::from_secs(claims.exp)
            .checked_sub(epoch)
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let deadline = observed_at
            .checked_add(remaining)
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let identity = RemoteIdentity {
            issuer: self.issuer.clone(),
            audience: self.audience.clone(),
            subject: claims.sub,
        };
        let session_id = headers
            .get("mcp-session-id")
            .map(|v| v.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| StatusCode::UNAUTHORIZED)?;
        if headers.get_all("mcp-session-id").iter().count() > 1 {
            return Err(StatusCode::UNAUTHORIZED);
        }
        let (connection_id, lease, slot) = if let Some(id) = &session_id {
            let mut sessions = self.sessions.lock().await;
            let session = sessions.get_mut(id).ok_or(StatusCode::NOT_FOUND)?;
            if session.identity != identity {
                return Err(StatusCode::FORBIDDEN);
            }
            if session.lease.current_authorization().is_none() {
                return Err(StatusCode::NOT_FOUND);
            }
            session.lease.extend(claims.exp, deadline);
            (session.connection_id.clone(), session.lease.clone(), None)
        } else {
            if method != Method::POST {
                return Err(StatusCode::UNAUTHORIZED);
            }
            // Reclaim a bounded batch before taking a new-session permit.
            // Otherwise a full set of abandoned expired sessions cannot GC.
            self.reap_expired_sessions().await;
            let slot = self
                .slots
                .clone()
                .try_acquire_owned()
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
            (
                ulid::Ulid::new().to_string(),
                Arc::new(SessionLease::new(claims.exp, deadline)),
                Some(Arc::new(slot)),
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
        context.validate().map_err(|_| StatusCode::UNAUTHORIZED)?;
        // Refresh the addressed session before reaping, so a valid renewed
        // assertion can continue its existing SDK session after the old expiry.
        if context.session_id.is_some() {
            self.reap_expired_sessions().await;
        }
        Ok(VerifiedRequest {
            context,
            lease,
            slot,
        })
    }

    // Reap through the SDK's own session lifecycle, not just the auth binding.
    // Work is bounded per authenticated request; no background registry/driver.
    async fn reap_expired_sessions(&self) {
        let expired = self
            .sessions
            .lock()
            .await
            .iter()
            .filter(|(_, session)| {
                session
                    .lease
                    .current_authorization()
                    .is_none_or(|authorization| authorization.expired)
            })
            .take(64)
            .map(|(id, _)| id.clone())
            .collect::<Vec<_>>();
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            for id in expired {
                // Serialize closure with refresh and recheck the live lease:
                // a previously captured expiry cannot close a renewed session.
                let mut sessions = self.sessions.lock().await;
                let Some(session) = sessions.get(&id) else {
                    continue;
                };
                if session
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
    ) -> Result<(), StatusCode> {
        let mut sessions = self.sessions.lock().await;
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
            let id = id.to_str().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            if id.len() > 256 {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            let slot = request
                .slot
                .clone()
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;
            sessions.insert(
                id.to_owned(),
                Session {
                    identity: request.context.identity.clone(),
                    connection_id: request.context.connection_id.clone(),
                    lease: request.lease.clone(),
                    _slot: slot,
                },
            );
        }
        Ok(())
    }
}
