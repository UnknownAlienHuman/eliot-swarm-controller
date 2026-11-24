//! Rate-limited native allowance reads shared by the CLI, routing and maintenance.

use super::{Router, now_ms};
use crate::router::plugin_host::MetadataRoute;
use eliot_core::api::{HARNESS_READ_TTL_MS, HarnessQuotaResponse, harness::QuotaObservation};
use eliot_core::{Error, Result};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use tokio::{sync::Mutex, time::Instant};

const READ_INTERVAL: Duration = Duration::from_millis(HARNESS_READ_TTL_MS);
// Covers the metadata reader slot wait, plugin startup and the native read.
const READ_BUDGET: Duration = Duration::from_secs(35);
const MAX_CLOCK_LEAD_MS: i64 = 30_000;
const MAX_ACCOUNTS: usize = 256;

#[derive(Default)]
pub(super) struct AllowanceReads(HashMap<String, Arc<AccountRead>>);

#[derive(Default)]
struct AccountRead {
    refresh: Arc<Mutex<()>>,
    cached: std::sync::Mutex<AllowanceSnapshot>,
}

#[derive(Clone, Default)]
struct AllowanceSnapshot {
    next_read: Option<Instant>,
    good: Option<Value>,
    failure: Option<AllowanceFailure>,
}

#[derive(Clone)]
struct AllowanceFailure {
    at_ms: i64,
    error: Error,
    reported_unavailable: bool,
}

enum AllowanceReading {
    Ready(Value),
    Unavailable(Error),
}

impl AllowanceReads {
    fn account(&mut self, key: String) -> Result<Arc<AccountRead>> {
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
                    let next = entry.cached.lock_recover().next_read;
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
    ) -> Result<(bool, Option<Error>)> {
        let Some(identity) = route.identity.as_deref() else {
            return Ok((
                false,
                Some(eliot_plugin::quota::environment(
                    "native allowance sign-in scope is unverified",
                )),
            ));
        };
        let scope_harness = response.harness.clone();
        let scope_environment = route.environment.clone();
        let key = crate::adhoc::blocking_discovery(deadline.into(), move || {
            account_key(&scope_harness, &scope_environment)
        })
        .await;
        let key = match key {
            Ok(Some(key)) => key,
            Ok(None) => {
                return Ok((
                    false,
                    Some(eliot_plugin::quota::environment(
                        "native allowance sign-in scope is unavailable",
                    )),
                ));
            }
            Err(error) if error.code == "timeout" => return Ok((false, Some(error))),
            Err(error) => return Err(error),
        };
        let entry = self.inner.allowance_reads.lock_recover().account(key)?;
        if let Ok(refresh) =
            tokio::time::timeout_at(deadline, entry.refresh.clone().lock_owned()).await
        {
            let cached = entry
                .cached
                .lock_recover()
                .next_read
                .is_some_and(|next| next > Instant::now());
            if cached {
                crate::logging::record_debug(
                    &self.secret_policy_snapshot(),
                    "quota.allowance.cached",
                    &[("harness", &response.harness)],
                    &[],
                );
            } else {
                let router = self.clone();
                let harness = response.harness.clone();
                let profile = profile.clone();
                let account = entry.clone();
                // The refresh owns its slot through cleanup and cache settlement.
                // A caller that runs out of time can still read the cache.
                let task = tokio::spawn(async move {
                    let _refresh = refresh;
                    crate::logging::record_debug(
                        &router.secret_policy_snapshot(),
                        "quota.allowance.read",
                        &[("harness", &harness)],
                        &[],
                    );
                    let result = router
                        .inner
                        .plugin_host
                        .quota(profile, route.environment, Instant::now() + READ_BUDGET)
                        .await
                        .and_then(allowance_details);
                    let mut read = account.cached.lock_recover();
                    read.next_read = Some(Instant::now() + READ_INTERVAL);
                    match result {
                        Ok(AllowanceReading::Ready(details)) => {
                            if read.good.as_ref().is_none_or(|previous| {
                                observed_at(&details) >= observed_at(previous)
                            }) {
                                read.good = Some(details);
                            }
                            read.failure = None;
                        }
                        Ok(AllowanceReading::Unavailable(error)) => {
                            read.good = None;
                            read.failure = Some(AllowanceFailure {
                                at_ms: now_ms(),
                                error,
                                reported_unavailable: true,
                            });
                        }
                        Err(error) => {
                            read.failure = Some(AllowanceFailure {
                                at_ms: now_ms(),
                                error,
                                reported_unavailable: false,
                            });
                        }
                    }
                });
                if let Ok(result) = tokio::time::timeout_at(deadline, task).await {
                    result
                        .map_err(|_| Error::new("internal", "native allowance reader stopped"))?;
                }
            }
        }
        let AllowanceSnapshot { good, failure, .. } = entry.cached.lock_recover().clone();
        let stream = match tokio::time::timeout_at(deadline, self.inner.store.lock()).await {
            Ok(store) => {
                store.latest_quota_observation(&response.harness, identity, &profile, now_ms())?
            }
            Err(_) => None,
        };
        let mut details = good;
        if let Some(observation) = stream
            && !failure.as_ref().is_some_and(|failure| {
                failure.reported_unavailable && failure.at_ms > observation.observed_at_ms
            })
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
                match (
                    failure.is_some_and(|failure| failure.at_ms > response.fetched_at_ms),
                    details["source"].as_str(),
                ) {
                    (true, _) => "Last subscription allowance; latest read failed",
                    (false, Some("Muse MSP usage/changed")) => {
                        "Muse subscription usage observed during a turn"
                    }
                    _ => "Muse subscription allowance from usage --json",
                }
                .to_owned(),
            );
            response.details = Some(details);
            return Ok((true, None));
        }
        let error = failure.map_or_else(
            || {
                eliot_plugin::quota::environment(
                    "Native subscription allowance read is still in progress",
                )
            },
            |failure| failure.error,
        );
        Ok((true, Some(error)))
    }
}

fn observed_at(details: &Value) -> Option<i64> {
    details.get("observedAtMs")?.as_i64()
}

fn allowance_details(mut raw: Value) -> Result<AllowanceReading> {
    let mut details = raw["details"].take();
    let invalid = || {
        Error::new(
            "source_integrity",
            "native subscription allowance is invalid",
        )
    };
    match details["state"].as_str() {
        Some("unavailable") => {
            let reason = details["reason"].as_str().ok_or_else(invalid)?;
            let error = match details["code"].as_str() {
                Some("auth_required") => Error::new("auth_required", reason).with_origin(
                    eliot_core::FailureOrigin::Environment,
                    "Muse usage requires native sign-in",
                ),
                Some("quota_unavailable") => {
                    eliot_plugin::quota::provider("Muse reported no subscription allowance")
                        .with_message(reason)
                }
                _ => return Err(invalid()),
            };
            return Ok(AllowanceReading::Unavailable(error));
        }
        Some("ready") => {}
        _ => return Err(invalid()),
    }
    let now = now_ms();
    let at = details
        .get("observedAtMs")
        .map_or(Ok(now), |at| at.as_i64().ok_or_else(invalid))?;
    let observation: QuotaObservation = serde_json::from_value(json!({
        "observedAtMs": at, "windows": details.get("windows"),
    }))
    .map_err(|_| invalid())?;
    observation.validate()?;
    if at > now.saturating_add(MAX_CLOCK_LEAD_MS) {
        return Err(invalid());
    }
    // Downstream freshness and hold release require a local, non-future timestamp.
    details["observedAtMs"] = json!(at.min(now));
    Ok(AllowanceReading::Ready(details))
}
