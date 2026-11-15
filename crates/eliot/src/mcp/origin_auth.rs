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
    const TEST_RSA_N: &str = "2aMvnTpVG7geArR3Js5XB9KUd5iairv2YToTxew9wotoIL4xNT_Y5SWC2a-EUaxBNbutddvZhpULZDi2nrZNgoofRq9veWpwUqHW4WB69QfJKhjeBQsNjzOpAJceR6UVWV5E1QqbaJfQOZ7FtVYQTFS-_WsENfR8hD4IS75Povdqld5AgZorFMqVn2gHkJ8A0s3mtTij5fAjzzf-0S6QSFgT5hqnSnLZ-YoGV9Z-vJiWzi49ftYEMOnxZul7rMVBsAZ3zAAYmfgjn1qPj6vjlH2PW4jcZ8jS_16ADaBUhuKoGvuEYXZwoHl4VozCRXNXTlEdcccHB_zhnhUx3RkVzQ";
    const TEST_RSA_PKCS1_B64: &str = "MIIEpgIBAAKCAQEA2aMvnTpVG7geArR3Js5XB9KUd5iairv2YToTxew9wotoIL4xNT/Y5SWC2a+EUaxBNbutddvZhpULZDi2nrZNgoofRq9veWpwUqHW4WB69QfJKhjeBQsNjzOpAJceR6UVWV5E1QqbaJfQOZ7FtVYQTFS+/WsENfR8hD4IS75Povdqld5AgZorFMqVn2gHkJ8A0s3mtTij5fAjzzf+0S6QSFgT5hqnSnLZ+YoGV9Z+vJiWzi49ftYEMOnxZul7rMVBsAZ3zAAYmfgjn1qPj6vjlH2PW4jcZ8jS/16ADaBUhuKoGvuEYXZwoHl4VozCRXNXTlEdcccHB/zhnhUx3RkVzQIDAQABAoIBAQDCQxkDHMhPH78Z5v/cEZ45i0uenQaFA1XeXzq7Fi3icxEU+O9vengdwafUqwhO5pSmY8lnREAcEvnCEaVgcZUqCiP0si+sjseH2pG1Rqs6ACMdgmja04zc393gewM6LC6sQKrsBrXFXI0ugXw0aofNum9oQJfXmU1ztPMuuMbYzJ3ucT8CFp0vLDjpXesoCVTKhIFR9dMYwaJbqCaFionr3ZjPkw9oKBoSDltf2AY2G/X2itKva54BtM+oCTqoEk8qSRp7eLmw49ngRW/5lwDRFv+6V6CHNZNdIfmrjg//ulvt5itQhkkOcQ/Cg0m8c+5gj5mIpWuGHsWOALVZsj75AoGBAOFVID/T15oadNKFtw0yoewamv8IKuaWpN+0To/nYvZ54WE4Yh0Araz4G09YBtC8MAjAPFMqekSVWoEMCwNRwisrjZZnquMKfwWPNBt+izYhp7qbjtrXDqEIZUHVIirMHGBu+aphbGhaRd/LrERwkBJeDo+OuvZdtWG2PXPyM33bAoGBAPdB9CFPjOrFIa0OBUvy5EOf3sKAmpIACZmyw67nVwnIjJN603VY1dbmAB8eC3ejvuE+gkFRpND5tybHPOEvvpp+K1SbQgKSB+R/boLYY9wFFtuqzqkFpFQ+s+TcQIfCB1sXqWlhKHCri6T9hT8NL1VEtlt4x5XV4fSOAa+16U93AoGBAMZfiuSmO0kPwYkU7vJPgP267nlXV21pcfz/X6qQqi8TXbeZS8b8sel1A5X2iQl6o08Q7grJepqmYeb6Ezf44lffH+cpnOgEdY8PY3BRF32wY1mFWV6PtfFO0tYCLPJAfzrmeCXnXuj/kERVqnMWJcAFzgvUPARouyYqqi9JKsvBAoGBAOwngAY4tnycn5drpr9tU3sLPHVE41zC0GiOBrwMlGLxuuI5E7ImkMbZkhAmYAl6yZrUWxAL7IDzoGxXKtxg/4kLLFN4nklaNFOXKnOqPWYtLjHMJ+njS5py8NqLSs7fEKt+I/5UH43NKmkBAry/2Q91uz4OU4HWcCNlZlLuSdHtAoGBANUUterI20AU8R3sYv7Ene4UTN6vErydirWSRHeFymmX800PybtJkEKw3TaErFMBL2kGev4yiOaV4D5qv4EGMYbm0n377qRTkBwRGSzzejqoDbRpgHr1tKp6TXMSpOlKjZLNIOo16M7fWCKoJqmu6KM142nxTZNfcy0PpeaesiNb";
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

    fn decode_standard_base64(input: &str) -> Vec<u8> {
        let mut out = Vec::with_capacity(input.len() / 4 * 3);
        let mut block = [0u8; 4];
        let mut have = 0usize;
        let mut padded = 0usize;
        for byte in input.bytes() {
            let sextet = match byte {
                b'A'..=b'Z' => byte - b'A',
                b'a'..=b'z' => byte - b'a' + 26,
                b'0'..=b'9' => byte - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => {
                    padded += 1;
                    0
                }
                _ => panic!("test key uses standard base64"),
            };
            block[have] = sextet;
            have += 1;
            if have == 4 {
                out.push(block[0] << 2 | block[1] >> 4);
                if padded < 2 {
                    out.push(block[1] << 4 | block[2] >> 2);
                }
                if padded == 0 {
                    out.push(block[2] << 6 | block[3]);
                }
                have = 0;
                padded = 0;
            }
        }
        assert_eq!(have, 0, "test key length is a base64 block");
        out
    }

    fn signing_key() -> EncodingKey {
        EncodingKey::from_rsa_der(&decode_standard_base64(TEST_RSA_PKCS1_B64))
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
