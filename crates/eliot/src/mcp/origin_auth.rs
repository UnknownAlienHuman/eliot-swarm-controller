//! Cloudflare application-assertion verification at the HTTP origin.
//! Managed OAuth's opaque Authorization token is never interpreted as a JWT.
//! jsonwebtoken 11.1.0 (MIT, MSRV 1.88) supplies signature/claim verification;
//! no cryptographic or OAuth implementation is copied here.
use axum::http::{HeaderMap, Method, StatusCode};
use eliot_core::{RemoteCallContext, RemoteIdentity};
use eliot_plugin::sync::LockExt;
use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{JwkSet, KeyAlgorithm, PublicKeyUse},
};
use rmcp::transport::streamable_http_server::session::{
    SessionManager, local::LocalSessionManager,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::Path,
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore, watch};

const MAX_ASSERTION_BYTES: usize = 16 * 1024;
const MAX_JWKS_BYTES: usize = 256 * 1024;
const MAX_SESSIONS: usize = 1024;
const KEY_LIFETIME: Duration = Duration::from_secs(300);
const REFRESH_BACKOFF: Duration = Duration::from_secs(30);

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
        let (issuer, audience) = eliot_daemon::load_cloud_access_config(data_dir)
            .map_err(|error| format!("{}: {}", error.code, error.message))?;
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
            if !self
                .sdk_sessions
                .has_session(&id.clone().into())
                .await
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            {
                drop(sessions.remove(id));
                return Err(StatusCode::NOT_FOUND);
            }
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
        context.validate().map_err(|_| StatusCode::UNAUTHORIZED)?;
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
    ) -> Result<(), StatusCode> {
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
            let id = id.to_str().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            if id.len() > 256 {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            if request.lease.current_authorization().is_none()
                || !self
                    .sdk_sessions
                    .has_session(&id.to_owned().into())
                    .await
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
            {
                return Err(StatusCode::NOT_FOUND);
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
                    slot,
                },
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;
    use jsonwebtoken::{EncodingKey, Header, encode};

    const TEST_ISSUER: &str = "https://k47-qa-faketeam.cloudflareaccess.com";
    const TEST_AUDIENCE: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const TEST_KID: &str = "k47-test-key-1";
    const TEST_SUBJECT_A: &str = "k47-subject-a";
    const TEST_SUBJECT_B: &str = "k47-subject-b";
    const TEST_RSA_N: &str = "xKFhuV1yaOfBJrPtZOsg98CdgG-TbgHeK2YlaQ0le4RKMNyx88033DiBa6zpdxNcW4vfQ8pCgFQVENVf1sZmfU5TkOUHmApBwxI-LYpTvp5d5JIpOLXt2hUmTfRMea-bfP5e-DPx9UOoir714Jt3LeN71RbWWidM2O818N36mXC3pDiM8VVo5bvVYFBNDILJVGL8n6esyF7z9eWp--HWREBrMKLisRUVX4XeJc6ooSHIlc4rxB7LQtcUJX7wznu0OOpk6XGIqcDLAp51chvXyDhVMXJreteTIVqjLu7Vf_cbZKzjHRPz7EoRMuvjFlDyBHorVs2F8TatCy5c_XTQ-Q";
    // Throwaway RSA key generated only for this test module. It protects
    // nothing and must never leave test code.
    const TEST_RSA_PEM: &str = concat!(
        "-----BEGIN PRIVATE KEY-----\n",
        "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDEoWG5XXJo58Em\n",
        "s+1k6yD3wJ2Ab5NuAd4rZiVpDSV7hEow3LHzzTfcOIFrrOl3E1xbi99DykKAVBUQ\n",
        "1V/WxmZ9TlOQ5QeYCkHDEj4tilO+nl3kkik4te3aFSZN9Ex5r5t8/l74M/H1Q6iK\n",
        "vvXgm3ct43vVFtZaJ0zY7zXw3fqZcLekOIzxVWjlu9VgUE0MgslUYvyfp6zIXvP1\n",
        "5an74dZEQGswouKxFRVfhd4lzqihIciVzivEHstC1xQlfvDOe7Q46mTpcYipwMsC\n",
        "nnVyG9fIOFUxcmt615MhWqMu7tV/9xtkrOMdE/PsShEy6+MWUPIEeitWzYXxNq0L\n",
        "Llz9dND5AgMBAAECggEBAL7Oi/JAyAEcw8Y226sOsBM7CpvTqhEaJumeWbczVqgh\n",
        "m40G3HhlMEu3WX+Pb45pbzo25RT3dMjqzvZi2HmmVVZLZ7Q3S8MyUNZnIYc3sERN\n",
        "kXd4jh/GC2WRGHaVFpEO46fLf4UDg0wpt6fL3vVyZ5hUI5qIasFZWa05k/KEteWm\n",
        "U4hlRAlOXhzV2yXSlBNA0G3i/eNAiU0IUyL2H8I5xSVfkwXhtigwuMgBklqUilAm\n",
        "QBYrQi9JHQwGl+uPNxjJnbfSoAZnPSiYRr+94NcnMIl1p5tRORWZO+YoAyUjwy1T\n",
        "1GVKYT6MOlhSFXOV80CTgw2tmw3idFS5Hi5fdpHEEvECgYEA0uGqeiIw9ms5WlM3\n",
        "gv4tkHnsX57wSad94Ewu2ixZ5r6UsevN641jbxaaOMZFt1UUcMIXKxl7tVbqEYtl\n",
        "vmyfjEasKEgIRaDs29Ov0mU/2eWbnnGGZoWUerSMo78F/YQuTCmhFUKbc9fCSw2z\n",
        "QZIK6fxiHRaCnP8wiUVsCtdxxv8CgYEA7rMomWR1lfAFBEloGSwNjhGIhOv72WOA\n",
        "UkTdTMIMOrAk3vDb33I+BQ5hrXjBKniiovShFjuFv0IJ8e2Ww0fq2MTJa4fL4tws\n",
        "F+uf8iMo9XYgZ6eOjDdyVOOeegOpoJsInQVuvQ4/Znnz8A0elUkOg6/7rn8p2Cea\n",
        "uueQ3nQHoAcCgYEAkaHMhFnWbGayDeHIRUtZlhvNFGAD2ymzMVILTRox37sfnxuU\n",
        "cqaK063NRYYPoUM9QX2YWGhZ2WqzbPhShEYpjMtFmu/b3pH64Pj4xBiuFsGNTcdi\n",
        "pKx9VJwZP//smB5PhsN8/dyo1EBdak2EzLSZmutzqljgTKGjGDl8GqSca2sCgYEA\n",
        "rF4peiMmGiDZro/HoS3DmAynNb0m/4MvnbjmyU/y/spFxcvOKpvXl1UcSeVCMAYv\n",
        "A1d9n41Z9pOclNwJm3iqaOApxwGnxGlTWe6ncHXdVOt5WQCRGkXLxOsdfGR6sGRj\n",
        "oufSYKkSNVDpSzZeGFVKKuQA7bAcKl/nBwLDBTBqdS8CgYBHXqW8cGATu3bCmEUI\n",
        "jpPjKG5HyRRhDoWS8FBq5BUmkzoN/wiFlyo3p7FvX50NKQiJk1+M0aL3YGIpfliT\n",
        "45mqHTvdOib7wGjXcdbwJavmoLYDF0L1rjSVuylXNEhap13HT91NBczfgmflHlmC\n",
        "uWp8w4Hp9lfXgiybQiXyrhvmtw==\n",
        "-----END PRIVATE KEY-----",
    );
    fn test_jwks() -> JwkSet {
        serde_json::from_value(serde_json::json!({
            "keys": [{
                "kty": "RSA",
                "kid": TEST_KID,
                "alg": "RS256",
                "use": "sig",
                "n": TEST_RSA_N,
                "e": "AQAB",
            }]
        }))
        .expect("test JWKS parses")
    }

    fn test_auth() -> OriginAuth {
        OriginAuth {
            issuer: TEST_ISSUER.to_owned(),
            audience: TEST_AUDIENCE.to_owned(),
            client: reqwest::Client::builder()
                .https_only(true)
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .expect("test HTTP client builds"),
            keys: Mutex::new(Keys {
                set: test_jwks(),
                valid_until: Instant::now() + KEY_LIFETIME,
                retry_after: Instant::now() + REFRESH_BACKOFF,
            }),
            sessions: Mutex::new(HashMap::new()),
            slots: Arc::new(Semaphore::new(MAX_SESSIONS)),
            sdk_sessions: Arc::new(LocalSessionManager::default()),
        }
    }

    fn signing_key() -> EncodingKey {
        EncodingKey::from_rsa_pem(TEST_RSA_PEM.as_bytes()).expect("test RSA key parses")
    }

    fn mint(subject: &str, kid: &str, issuer: &str, audience: &str, exp: u64, nbf: u64) -> String {
        let claims = Claims {
            iss: issuer.to_owned(),
            aud: vec![audience.to_owned()],
            sub: subject.to_owned(),
            exp,
            nbf,
            token_type: "app".to_owned(),
        };
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_owned());
        encode(&header, &claims, &signing_key()).expect("test token mints")
    }

    fn fresh_token(subject: &str) -> String {
        let now = now_seconds();
        mint(
            subject,
            TEST_KID,
            TEST_ISSUER,
            TEST_AUDIENCE,
            now + 300,
            now.saturating_sub(10),
        )
    }

    fn post_headers(token: &str, session_id: Option<&str>) -> (HeaderMap, Method) {
        let mut headers = HeaderMap::new();
        headers.insert(
            "cf-access-jwt-assertion",
            token.parse().expect("token is a header value"),
        );
        if let Some(id) = session_id {
            headers.insert(
                "mcp-session-id",
                id.parse().expect("session id is a header value"),
            );
        }
        (headers, Method::POST)
    }

    async fn session_count(auth: &OriginAuth) -> usize {
        auth.sessions.lock().await.len()
    }

    async fn insert_session(auth: &OriginAuth, id: &str, subject: &str, expires_at: u64) {
        auth.sessions.lock().await.insert(
            id.to_owned(),
            Session {
                identity: RemoteIdentity {
                    issuer: TEST_ISSUER.to_owned(),
                    audience: TEST_AUDIENCE.to_owned(),
                    subject: subject.to_owned(),
                },
                connection_id: "k47-test-connection".to_owned(),
                lease: Arc::new(SessionLease::new(
                    expires_at,
                    Instant::now() + Duration::from_secs(300),
                )),
                slot: Arc::new(SessionSlot(StdMutex::new(None))),
            },
        );
    }
    #[tokio::test]
    async fn valid_token_authenticates_without_session_state() {
        let auth = test_auth();
        let token = fresh_token(TEST_SUBJECT_A);
        let (mut headers, method) = post_headers(&token, None);
        let verified = auth
            .authenticate(&mut headers, &method)
            .await
            .expect("valid token authenticates");
        assert_eq!(verified.context.identity.subject, TEST_SUBJECT_A);
        assert!(verified.context.session_id.is_none());
        assert_eq!(session_count(&auth).await, 0);
        assert!(headers.get("cf-access-jwt-assertion").is_none());
    }

    #[tokio::test]
    async fn expired_token_rejected() {
        let auth = test_auth();
        let now = now_seconds();
        let token = mint(
            TEST_SUBJECT_A,
            TEST_KID,
            TEST_ISSUER,
            TEST_AUDIENCE,
            now - 60,
            now - 600,
        );
        let (mut headers, method) = post_headers(&token, None);
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn not_yet_valid_token_rejected() {
        let auth = test_auth();
        let now = now_seconds();
        let token = mint(
            TEST_SUBJECT_A,
            TEST_KID,
            TEST_ISSUER,
            TEST_AUDIENCE,
            now + 600,
            now + 300,
        );
        let (mut headers, method) = post_headers(&token, None);
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn wrong_audience_rejected() {
        let auth = test_auth();
        let now = now_seconds();
        let token = mint(
            TEST_SUBJECT_A,
            TEST_KID,
            TEST_ISSUER,
            "https://other.example.com/app",
            now + 300,
            now - 10,
        );
        let (mut headers, method) = post_headers(&token, None);
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn wrong_issuer_rejected() {
        let auth = test_auth();
        let now = now_seconds();
        let token = mint(
            TEST_SUBJECT_A,
            TEST_KID,
            "https://other-team.cloudflareaccess.com",
            TEST_AUDIENCE,
            now + 300,
            now - 10,
        );
        let (mut headers, method) = post_headers(&token, None);
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn unknown_kid_rejected_without_network_refresh() {
        let auth = test_auth();
        let now = now_seconds();
        let token = mint(
            TEST_SUBJECT_A,
            "k47-unknown-kid",
            TEST_ISSUER,
            TEST_AUDIENCE,
            now + 300,
            now - 10,
        );
        let (mut headers, method) = post_headers(&token, None);
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn missing_assertion_rejected() {
        let auth = test_auth();
        let mut headers = HeaderMap::new();
        let method = Method::POST;
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        assert_eq!(session_count(&auth).await, 0);
    }
    #[tokio::test]
    async fn duplicate_assertion_rejected() {
        let auth = test_auth();
        let token = fresh_token(TEST_SUBJECT_A);
        let value: HeaderValue = token.parse().expect("token is a header value");
        let mut headers = HeaderMap::new();
        headers.insert("cf-access-jwt-assertion", value.clone());
        let _ = headers.append("cf-access-jwt-assertion", value);
        let method = Method::POST;
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn cross_subject_session_reuse_forbidden() {
        let auth = test_auth();
        let (sdk_id, _transport) = auth
            .sdk_sessions
            .create_session()
            .await
            .expect("SDK session starts");
        let sid = sdk_id.to_string();
        insert_session(&auth, &sid, TEST_SUBJECT_A, now_seconds() + 300).await;
        let token = fresh_token(TEST_SUBJECT_B);
        let (mut headers, method) = post_headers(&token, Some(&sid));
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::FORBIDDEN));
        assert_eq!(session_count(&auth).await, 1);
        let sessions = auth.sessions.lock().await;
        assert_eq!(
            sessions.get(&sid).map(|s| s.identity.subject.as_str()),
            Some(TEST_SUBJECT_A)
        );
    }

    #[tokio::test]
    async fn unknown_session_id_not_found() {
        let auth = test_auth();
        let token = fresh_token(TEST_SUBJECT_A);
        let (mut headers, method) = post_headers(&token, Some("k47-no-such-session"));
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::NOT_FOUND));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn closed_lease_session_not_found() {
        let auth = test_auth();
        let (sdk_id, _transport) = auth
            .sdk_sessions
            .create_session()
            .await
            .expect("SDK session starts");
        let sid = sdk_id.to_string();
        insert_session(&auth, &sid, TEST_SUBJECT_A, now_seconds() + 300).await;
        auth.sessions
            .lock()
            .await
            .get(&sid)
            .expect("entry present")
            .lease
            .close();
        let token = fresh_token(TEST_SUBJECT_A);
        let (mut headers, method) = post_headers(&token, Some(&sid));
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::NOT_FOUND));
        assert_eq!(session_count(&auth).await, 1);
    }

    #[tokio::test]
    async fn sdk_closed_session_reaps_entry() {
        let auth = test_auth();
        let (sdk_id, _transport) = auth
            .sdk_sessions
            .create_session()
            .await
            .expect("SDK session starts");
        let sid = sdk_id.to_string();
        insert_session(&auth, &sid, TEST_SUBJECT_A, now_seconds() + 300).await;
        auth.sdk_sessions
            .close_session(&sdk_id)
            .await
            .expect("SDK session closes");
        let token = fresh_token(TEST_SUBJECT_A);
        let (mut headers, method) = post_headers(&token, Some(&sid));
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::NOT_FOUND));
        assert_eq!(session_count(&auth).await, 0);
    }

    #[tokio::test]
    async fn rejections_mutate_no_session_state() {
        let auth = test_auth();
        let now = now_seconds();
        let bad = [
            mint(
                TEST_SUBJECT_A,
                TEST_KID,
                TEST_ISSUER,
                TEST_AUDIENCE,
                now - 60,
                now - 600,
            ),
            mint(
                TEST_SUBJECT_A,
                TEST_KID,
                TEST_ISSUER,
                "https://other.example.com/app",
                now + 300,
                now - 10,
            ),
            mint(
                TEST_SUBJECT_A,
                "k47-unknown-kid",
                TEST_ISSUER,
                TEST_AUDIENCE,
                now + 300,
                now - 10,
            ),
        ];
        for token in &bad {
            let (mut headers, method) = post_headers(token, None);
            let status = auth.authenticate(&mut headers, &method).await.err();
            assert_eq!(status, Some(StatusCode::UNAUTHORIZED));
        }
        let (mut headers, method) =
            post_headers(&fresh_token(TEST_SUBJECT_A), Some("k47-no-such-session"));
        let status = auth.authenticate(&mut headers, &method).await.err();
        assert_eq!(status, Some(StatusCode::NOT_FOUND));
        assert_eq!(session_count(&auth).await, 0);
    }
}
