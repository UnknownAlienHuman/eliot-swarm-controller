//! Rate-limited native allowance reads shared by the CLI, routing and maintenance.

use super::{Router, catalog, now_ms};
use crate::router::plugin_host::MetadataRoute;
use eliot_core::api::{HARNESS_READ_TTL_MS, HarnessQuotaResponse, harness::QuotaObservation};
use eliot_core::{Error, Result};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};

const READ_INTERVAL: Duration = Duration::from_millis(HARNESS_READ_TTL_MS);
const MAX_ACCOUNTS: usize = 256;

#[derive(Default)]
pub(super) struct AllowanceReads(HashMap<String, Arc<Mutex<AccountRead>>>);

#[derive(Clone, Default)]
struct AccountRead {
    next_read: Option<Instant>,
    good: Option<Value>,
    error: Option<Error>,
}

impl AllowanceReads {
    fn account(&mut self, key: String) -> Result<Arc<Mutex<AccountRead>>> {
        if let Some(entry) = self.0.get(&key) {
            return Ok(entry.clone());
        }
        if self.0.len() >= MAX_ACCOUNTS {
            let now = Instant::now();
            let oldest = self
                .0
                .iter()
                .filter(|(_, entry)| Arc::strong_count(entry) == 1)
                .filter_map(|(key, entry)| {
                    let next = entry.try_lock().ok()?.next_read;
                    next.is_none_or(|next| next <= now).then_some((key, next))
                })
                .min_by_key(|(_, next)| *next)
                .map(|(key, _)| key.clone())
                .ok_or_else(|| Error::new("busy", "native allowance reader capacity reached"))?;
            self.0.remove(&oldest);
        }
        Ok(self.0.entry(key).or_default().clone())
    }
}

/// Scope the read to the vendor's effective file-backed sign-in. Profile labels,
/// model choices and upgrades of the launcher binary do not select accounts.
fn account_key(harness: &str, environment: &Value) -> Option<String> {
    let env = |name: &str| {
        environment
            .as_object()?
            .iter()
            .find(|(key, _)| {
                key.as_str() == name || (cfg!(windows) && key.eq_ignore_ascii_case(name))
            })?
            .1
            .as_str()
            .filter(|value| !value.trim().is_empty())
    };
    let config_home = match env("XDG_CONFIG_HOME") {
        Some(path) => Path::new(path).to_path_buf(),
        None => Path::new(env(if cfg!(windows) { "USERPROFILE" } else { "HOME" })?).join(".config"),
    };
    if !config_home.is_absolute() {
        return None;
    }
    // Canonicalize the directory when present to collapse aliases without
    // resolving or opening any credential file.
    let path = config_home.canonicalize().unwrap_or(config_home);
    let path = path.to_str()?;
    let key = if cfg!(windows) {
        path.to_lowercase()
    } else {
        path.to_owned()
    };
    Some(format!("{harness}:{key}"))
}

impl Router {
    pub(super) async fn read_allowance(
        &self,
        profile: Value,
        route: MetadataRoute,
        response: &mut HarnessQuotaResponse,
        deadline: Instant,
    ) -> Result<bool> {
        let Some(identity) = route.identity.as_deref() else {
            catalog::quota_read_error(
                response,
                eliot_plugin::quota::environment("native allowance sign-in scope is unverified"),
            );
            return Ok(false);
        };
        let scope_harness = response.harness.clone();
        let scope_environment = route.environment.clone();
        let Some(key) = crate::adhoc::blocking_discovery(deadline.into(), move || {
            account_key(&scope_harness, &scope_environment)
        })
        .await?
        else {
            catalog::quota_read_error(
                response,
                eliot_plugin::quota::environment("native allowance sign-in scope is unavailable"),
            );
            return Ok(false);
        };
        let entry = self.inner.allowance_reads.lock_recover().account(key)?;
        let mut read = tokio::time::timeout_at(deadline, entry.lock_owned())
            .await
            .map_err(|_| crate::adhoc::discovery_timeout())?;
        let read = if read.next_read.is_some_and(|next| next > Instant::now()) {
            crate::logging::record_debug(
                &self.secret_policy_snapshot(),
                "quota.allowance.cached",
                &[("harness", &response.harness)],
                &[],
            );
            let cached = read.clone();
            drop(read);
            cached
        } else {
            let router = self.clone();
            let harness = response.harness.clone();
            let profile = profile.clone();
            // Hold the account lock through reader cleanup and cache settlement,
            // even if the initiating client drops its request.
            let task =
                tokio::spawn(async move {
                    crate::logging::record_debug(
                        &router.secret_policy_snapshot(),
                        "quota.allowance.read",
                        &[("harness", &harness)],
                        &[],
                    );
                    let result = router
                        .inner
                        .plugin_host
                        .quota(profile, route.environment, deadline)
                        .await
                        .and_then(allowance_details);
                    read.next_read = Some(Instant::now() + READ_INTERVAL);
                    match result {
                        Ok(details) => {
                            if read.good.as_ref().is_none_or(|previous| {
                                observed_at(&details) >= observed_at(previous)
                            }) {
                                read.good = Some(details);
                            }
                            read.error = None;
                        }
                        Err(error) => read.error = Some(error),
                    }
                    read.clone()
                });
            tokio::time::timeout_at(deadline, task)
                .await
                .map_err(|_| crate::adhoc::discovery_timeout())?
                .map_err(|_| Error::new("internal", "native allowance reader stopped"))?
        };
        let AccountRead { good, error, .. } = read;
        let stream = self.inner.store.lock().await.latest_quota_observation(
            &response.harness,
            identity,
            &profile,
            now_ms(),
        )?;
        let mut details = good;
        if let Some(observation) = stream
            && details
                .as_ref()
                .is_none_or(|current| Some(observation.observed_at_ms) > observed_at(current))
        {
            let tier = details.as_ref().and_then(|item| item.get("tier")).cloned();
            details = Some(json!({
                "state": "ready", "origin": "provider", "source": "Muse MSP usage/changed",
                "observedAtMs": observation.observed_at_ms, "windows": observation.windows,
            }));
            if let (Some(details), Some(tier)) = (&mut details, tier) {
                details["tier"] = tier;
            }
        }
        if let Some(details) = details {
            response.fetched_at_ms = observed_at(&details).expect("validated allowance timestamp");
            response.note = Some(
                match (error.is_some(), details["source"].as_str()) {
                    (true, _) => "Last subscription allowance; latest read failed",
                    (false, Some("Muse MSP usage/changed")) => {
                        "Muse subscription usage observed during a turn"
                    }
                    _ => "Muse subscription allowance from usage --json",
                }
                .to_owned(),
            );
            response.details = Some(details);
        } else if let Some(error) = error {
            catalog::quota_read_error(response, error);
        }
        Ok(true)
    }
}

fn observed_at(details: &Value) -> Option<i64> {
    details.get("observedAtMs")?.as_i64()
}

fn allowance_details(mut raw: Value) -> Result<Value> {
    let details = raw["details"].take();
    let invalid = || {
        Error::new(
            "source_integrity",
            "native subscription allowance is invalid",
        )
    };
    let at = observed_at(&details).ok_or_else(invalid)?;
    let observation: QuotaObservation = serde_json::from_value(json!({
        "observedAtMs": at, "windows": details.get("windows"),
    }))
    .map_err(|_| invalid())?;
    observation.validate()?;
    if at > now_ms() || details["state"] != "ready" {
        return Err(invalid());
    }
    Ok(details)
}
