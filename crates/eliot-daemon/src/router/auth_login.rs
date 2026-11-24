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
use std::sync::atomic::Ordering;

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
    Err(Error::new(
        "not_allowed",
        "vendor sign-in requires the interactive Swarm desktop",
    ))
}

pub(super) fn supports_login(harness: &str) -> bool {
    cfg!(windows)
        && HarnessCapabilities::of(harness)
            .native_auth
            .and_then(|capability| capability.login)
            .is_some()
}

fn recover_attempts(store: &mut eliot_store::Store, now: i64) -> Result<()> {
    #[cfg(windows)]
    return store.recover_harness_logins(now, |console| {
        u64::try_from(console.start_time).is_ok_and(|start_time| {
            matches!(
                crate::platform::windows::recorded_process_alive(console.pid, start_time),
                Ok(false)
            )
        })
    });
    #[cfg(not(windows))]
    {
        let _ = (store, now);
        Ok(())
    }
}

impl Router {
    pub(super) fn harness_auth_applies(&self, harness: &str, snapshot: &Value) -> bool {
        let env = &snapshot["pass_env_values"];
        let profile = HarnessCapabilities::of(harness).profile;
        let selectors = [
            "CLAUDE_CONFIG_DIR",
            "CODEX_HOME",
            "CODEX_SQLITE_HOME",
            "XDG_CONFIG_HOME",
            "XDG_DATA_HOME",
            "MUSE_AUTH_PATH",
            "META_API_KEY",
            "OPENCODE_CONFIG_CONTENT",
        ];
        super::catalog::external_quota_route(harness, snapshot, env).is_none()
            && env.as_object().is_none_or(|values| {
                !values.iter().any(|(name, value)| {
                    !value.is_null()
                        && selectors
                            .iter()
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
        let status = {
            let mut store = self.inner.store.lock().await;
            let now = now_ms();
            recover_attempts(&mut store, now)?;
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
        let mut store = self.inner.store.lock().await;
        let now = now_ms();
        if self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN && !self.is_restart_requested() {
            recover_attempts(&mut store, now)?;
        }
        let pending = store.harness_auth_pending(now)?;
        Ok(json!(AuthDesktopPollResponse { pending }))
    }

    pub(super) async fn auth_desktop_claim(&self, params: &Value) -> Result<Value> {
        let request: AuthDesktopClaimRequest = decode_params(params.clone())?;
        let mut store = self.inner.store.lock().await;
        let response = if supports_login(&request.harness)
            && self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN
        {
            let now = now_ms();
            recover_attempts(&mut store, now)?;
            store.claim_harness_login(&request.harness, &request.revision, now)?
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
        if resolved {
            *self.inner.catalog_cache.lock_recover() = super::catalog::CatalogCache::default();
            self.signal_launch_arbitration();
        }
        Ok(json!(response))
    }
}
