use super::{MODE_OPEN, Principal, Router, require_owner};
use crate::common::now_ms;
use crate::platform::custody::LocalPeer;
use eliot_core::{
    Error, Event, Result,
    api::{
        HarnessCapabilities, auth_login::*, discovery::decode_params, harness::SUPPORTED_HARNESSES,
    },
};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
#[cfg(windows)]
use std::collections::HashMap;
use std::sync::atomic::Ordering;
#[cfg(windows)]
use std::sync::{Mutex, OnceLock};

pub(super) fn authorize_local(
    method: &str,
    principal: &Principal,
    peer: Option<&LocalPeer>,
    remote: bool,
) -> Result<()> {
    if !matches!(
        method,
        "auth.login" | "auth.desktop.poll" | "auth.desktop.claim" | "auth.desktop.report"
    ) {
        return Ok(());
    }
    require_owner(principal)?;
    if remote || peer.is_none() {
        return Err(Error::new(
            "not_allowed",
            "vendor sign-in requires a local Owner connection",
        ));
    }
    if method == "auth.login" {
        return Ok(());
    }
    #[cfg(windows)]
    if peer.is_some_and(LocalPeer::is_interactive_desktop) {
        return Ok(());
    }
    #[cfg(windows)]
    note_desktop_refusal(method, peer.map(LocalPeer::process_id).unwrap_or(0));
    Err(Error::new("not_allowed", INTERACTIVE_DESKTOP_REFUSAL))
}

/// Wire-stable refusal when the peer is not the interactive Swarm desktop.
/// The message is matched by [`is_interactive_desktop_refusal`] so the
/// daemon can throttle this log line without changing the desktop contract.
const INTERACTIVE_DESKTOP_REFUSAL: &str = "vendor sign-in requires the interactive Swarm desktop";

/// Re-log a refused desktop peer at most this often; the first refusal per
/// peer always logs. A refused poller retries every few seconds, so a line
/// per call buries the daemon log.
#[cfg(windows)]
const DESKTOP_REFUSAL_LOG_COOLDOWN_MS: i64 = 15 * 60 * 1000;

/// Last refusal-log time per peer pid (`0` when the peer had no pid), plus
/// the refusals suppressed since that log.
#[cfg(windows)]
static DESKTOP_REFUSAL_LOGS: OnceLock<Mutex<HashMap<u32, (i64, u64)>>> = OnceLock::new();

/// Whether this error is the interactive-desktop guard refusal. The daemon
/// logs those once per peer (rate-limited) at the refusal site instead of on
/// every call.
pub(crate) fn is_interactive_desktop_refusal(error: &Error) -> bool {
    error.code == "not_allowed" && error.message.as_str() == INTERACTIVE_DESKTOP_REFUSAL
}

/// Log a desktop guard refusal once per peer, rate-limited. Callers keep
/// returning the wire-stable refusal for every poll.
#[cfg(windows)]
fn note_desktop_refusal(method: &str, pid: u32) {
    let suppressed = {
        let mut logs = DESKTOP_REFUSAL_LOGS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock_recover();
        let now = now_ms();
        match logs.get_mut(&pid) {
            Some((last, suppressed)) => {
                if now.wrapping_sub(*last) < DESKTOP_REFUSAL_LOG_COOLDOWN_MS {
                    *suppressed += 1;
                    return;
                }
                let dropped = *suppressed;
                *last = now;
                *suppressed = 0;
                dropped
            }
            None => {
                logs.insert(pid, (now, 0));
                0
            }
        }
    };
    let pid_text = pid.to_string();
    let suppressed_text = suppressed.to_string();
    let mut ids = vec![
        ("principal", "owner"),
        ("method", method),
        ("peer_pid", pid_text.as_str()),
    ];
    if suppressed > 0 {
        ids.push(("suppressed_refusals", suppressed_text.as_str()));
    }
    crate::logging::record(
        &eliot_core::SecretPolicy::default(),
        "rpc.error",
        &ids,
        Some(&Error::new("not_allowed", INTERACTIVE_DESKTOP_REFUSAL)),
    );
}

pub(super) fn supports_login(harness: &str) -> bool {
    cfg!(windows)
        && HarnessCapabilities::of(harness)
            .native_auth
            .and_then(|capability| capability.login)
            .is_some()
}

impl Router {
    async fn recover_auth_attempts(&self) -> Result<()> {
        #[cfg(windows)]
        {
            let now = now_ms();
            let candidates = self
                .inner
                .store
                .lock()
                .await
                .harness_login_console_candidates(now)?;
            let gone = if candidates.is_empty() {
                Vec::new()
            } else {
                tokio::task::spawn_blocking(move || {
                    candidates
                        .into_iter()
                        .filter(|console| {
                            u64::try_from(console.start_time).is_ok_and(|start_time| {
                                matches!(
                                    crate::platform::windows::recorded_process_alive(
                                        console.pid,
                                        start_time
                                    ),
                                    Ok(false)
                                )
                            })
                        })
                        .collect::<Vec<_>>()
                })
                .await
                .map_err(|_| Error::new("internal", "could not observe sign-in consoles"))?
            };
            let mut store = self.inner.store.lock().await;
            let events = store.recover_harness_logins(now, |console| {
                gone.iter()
                    .any(|old| old.pid == console.pid && old.start_time == console.start_time)
            })?;
            self.publish_locked(&events);
        }
        Ok(())
    }

    pub(super) fn harness_auth_applies(&self, harness: &str, snapshot: &Value) -> bool {
        let env = &snapshot["pass_env_values"];
        let profile = HarnessCapabilities::of(harness).profile;
        super::catalog::external_quota_route(harness, snapshot, env).is_none()
            && env.as_object().is_none_or(|values| {
                !values.iter().any(|(name, value)| {
                    !value.is_null()
                        && crate::config::profile_environments()
                            .flat_map(|profile| profile.auth_scope.iter())
                            .chain(profile.route_scope_secret_names)
                            .any(|key| name.eq_ignore_ascii_case(key))
                })
            })
    }

    pub(super) async fn note_native_auth_hold(&self, event: &Event) -> Result<()> {
        if event.kind != "provider.hold_updated"
            || event.source != "daemon"
            || event.data["kind"] != "auth"
        {
            return Ok(());
        }
        let (Some(agent), Some(generation), Some(scope)) = (
            event.agent.as_deref(),
            event.generation,
            event.data["scope"].as_str(),
        ) else {
            return Ok(());
        };
        let mut store = self.inner.store.lock().await;
        let Some(agent) = store.get_agent(agent)?.filter(|agent| {
            agent.generation == generation
                && !agent.observe_only
                && self.harness_auth_applies(&agent.harness, &agent.profile_snapshot)
        }) else {
            return Ok(());
        };
        let scope = scope.to_owned();
        if store
            .provider_holds_for_scopes(std::slice::from_ref(&scope))?
            .get(&scope)
            .is_none_or(|hold| {
                hold.kind != eliot_core::ProviderRestrictionKind::Auth
                    || hold.updated_at != event.ts
            })
        {
            return Ok(());
        }
        let supported = supports_login(&agent.harness);
        store.request_harness_login(&agent.harness, None, false, supported, now_ms())?;
        if supported {
            store.bind_harness_auth_hold(&agent.harness, &scope, event.ts)?;
        }
        self.signal_progress();
        Ok(())
    }

    pub(super) async fn auth_login(&self, params: &Value) -> Result<Value> {
        let request: AuthLoginRequest = decode_params(params.clone())?;
        Ok(json!(self.request_auth_login(request, false).await?))
    }

    pub(super) async fn request_auth_login(
        &self,
        request: AuthLoginRequest,
        inbox_decision: bool,
    ) -> Result<AuthLoginResponse> {
        if !SUPPORTED_HARNESSES.contains(&request.harness.as_str()) {
            return Err(Error::new("invalid", "unknown vendor harness"));
        }
        if !supports_login(&request.harness) {
            return Err(Error::new(
                "unsupported",
                "no verified interactive sign-in for this harness and platform",
            ));
        }
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new("unavailable", "daemon is shutting down"));
        }
        self.recover_auth_attempts().await?;
        let status = {
            let mut store = self.inner.store.lock().await;
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(Error::new("unavailable", "daemon is shutting down"));
            }
            let now = now_ms();
            if inbox_decision {
                super::owner_inbox::check_binding(&store)?;
            }
            store.request_harness_login(
                &request.harness,
                request.revision.as_deref(),
                true,
                true,
                now,
            )?
        };
        self.signal_progress();
        Ok(AuthLoginResponse { status })
    }

    pub(super) async fn note_harness_auth_required(&self, harness: &str, code: Option<&str>) {
        if code != Some("auth_required")
            || !SUPPORTED_HARNESSES.contains(&harness)
            || self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN
        {
            return;
        }
        let result = self.inner.store.lock().await.request_harness_login(
            harness,
            None,
            false,
            supports_login(harness),
            now_ms(),
        );
        match result {
            Ok(_) => self.signal_progress(),
            Err(error) => crate::logging::record(
                &self.secret_policy_snapshot(),
                "harness.auth_notice_failed",
                &[("harness", harness)],
                Some(&error),
            ),
        };
    }

    pub(super) async fn auth_desktop_poll(&self, params: &Value) -> Result<Value> {
        decode_params::<AuthDesktopPollRequest>(params.clone())?;
        let _write = self.inner.restart_writes.read().await;
        if self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN && !self.is_restart_requested() {
            self.recover_auth_attempts().await?;
        }
        let pending = self
            .inner
            .store
            .lock()
            .await
            .harness_auth_pending(now_ms())?;
        Ok(json!(AuthDesktopPollResponse { pending }))
    }

    pub(super) async fn auth_desktop_claim(&self, params: &Value) -> Result<Value> {
        let request: AuthDesktopClaimRequest = decode_params(params.clone())?;
        let response = if supports_login(&request.harness)
            && self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN
        {
            self.recover_auth_attempts().await?;
            let mut store = self.inner.store.lock().await;
            if self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN {
                store.claim_harness_login(&request.harness, &request.revision, now_ms())?
            } else {
                AuthDesktopClaimResponse {
                    attempt: None,
                    start_by_ms: None,
                }
            }
        } else {
            AuthDesktopClaimResponse {
                attempt: None,
                start_by_ms: None,
            }
        };
        Ok(json!(response))
    }

    pub(super) async fn auth_desktop_report(&self, params: &Value) -> Result<Value> {
        let request: AuthDesktopReportRequest = decode_params(params.clone())?;
        #[cfg(windows)]
        if let (AuthDesktopState::Prepared, Some(console)) = (request.state, request.console) {
            let start_time = u64::try_from(console.start_time)
                .map_err(|_| Error::new("invalid", "invalid console creation time"))?;
            if !crate::platform::windows::recorded_process_alive(console.pid, start_time)? {
                return Ok(json!(AuthDesktopReportResponse { accepted: false }));
            }
        }
        let (response, resolved) = {
            let mut store = self.inner.store.lock().await;
            let (response, resolved, events) = store.report_harness_login(&request, now_ms())?;
            self.publish_locked(&events);
            (response, resolved)
        };
        self.signal_progress();
        if response.accepted
            && matches!(
                request.state,
                AuthDesktopState::SignedIn | AuthDesktopState::SignedOut
            )
        {
            self.inner.catalog_cache.lock_recover().invalidate_auth();
            self.inner.catalog_refresh.notify_one();
        }
        if resolved {
            self.signal_launch_arbitration();
        }
        Ok(json!(response))
    }
}
