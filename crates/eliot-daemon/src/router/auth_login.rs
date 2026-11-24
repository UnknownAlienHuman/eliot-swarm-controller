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

impl Router {
    pub(super) fn harness_auth_applies(&self, harness: &str, snapshot: &Value) -> bool {
        let env = &snapshot["pass_env_values"];
        super::catalog::external_quota_route(harness, snapshot, env).is_none()
            && [
                "CLAUDE_CONFIG_DIR",
                "CODEX_HOME",
                "CODEX_SQLITE_HOME",
                "XDG_CONFIG_HOME",
                "XDG_DATA_HOME",
            ]
            .iter()
            .all(|selector| env.get(selector).is_none_or(Value::is_null))
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
            if inbox_decision {
                super::owner_inbox::check_binding(&store)?;
            }
            store.request_harness_login(
                &request.harness,
                request.revision.as_deref(),
                true,
                true,
                now_ms(),
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
        let mut store = self.inner.store.lock().await;
        let attempt = if supports_login(&request.harness)
            && self.inner.mode.load(Ordering::SeqCst) == MODE_OPEN
        {
            store.claim_harness_login(&request.harness, &request.revision, now_ms())?
        } else {
            None
        };
        Ok(json!(AuthDesktopClaimResponse { attempt }))
    }

    pub(super) async fn auth_desktop_report(&self, params: &Value) -> Result<Value> {
        let request: AuthDesktopReportRequest = decode_params(params.clone())?;
        let resolved = {
            let mut store = self.inner.store.lock().await;
            let (resolved, events) = store.report_harness_login(&request, now_ms())?;
            self.publish_locked(&events);
            resolved
        };
        self.signal_progress();
        if resolved {
            *self.inner.catalog_cache.lock_recover() = super::catalog::CatalogCache::default();
            self.signal_launch_arbitration();
        }
        Ok(json!(AuthDesktopReportResponse {}))
    }
}
