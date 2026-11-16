//! Bounded cached catalogue/quota scope derivation for `harness.models`
//! and `harness.quota`.
//!
//! The cache is keyed by the nonsecret native route/account scope, so one
//! live native read serves every caller on that scope until its TTL lapses.
//! Scope and credential identity derive only from frozen snapshot selectors
//! (harness, profile label, route/binding names); credential values,
//! `pass_env` resolutions and literal environment bodies are never read here.

use eliot_core::api::{
    HARNESS_READ_TTL_MS, HarnessEntry, HarnessModelsResponse, HarnessQuotaResponse,
    ModelsCatalogEntry,
};
use serde_json::Value;
use std::collections::HashMap;

/// Maximum cached scopes per read kind.
const MAX_CACHED_SCOPES: usize = 32;

/// Serve cached catalogue/quota entries for this long after the native read.
pub(crate) const CATALOG_TTL_MS: u64 = HARNESS_READ_TTL_MS;

/// Derive the nonsecret `(scope, credential)` pair for one frozen snapshot.
///
/// `profile_label` is the validated profile name, or `adhoc` for daemon
/// defaults. A `None` snapshot covers static harnesses (Claude,
/// Antigravity) that need no native selection. Never reads credential
/// values: only route/binding/integration names and the provider authority.
pub(crate) fn describe_snapshot(
    profile_label: &str,
    harness: &str,
    snapshot: Option<&Value>,
) -> (String, String) {
    let Some(snapshot) = snapshot else {
        return (
            format!("profile:{profile_label} harness:{harness}"),
            "none".to_owned(),
        );
    };
    // The caller verifies the snapshot harness matches; prefer the frozen
    // value so scope always names the route that was actually read.
    let harness = snapshot
        .get("harness")
        .and_then(Value::as_str)
        .unwrap_or(harness);
    let mut scope = format!("profile:{profile_label} harness:{harness}");
    let mut credential = "none".to_owned();
    match harness {
        "codex" => match snapshot.get("route_binding").and_then(Value::as_object) {
            Some(binding) => {
                let name = binding
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("unnamed");
                scope.push_str(&format!(" route:{name}"));
                if let Some(base) = binding.get("provider_base_url").and_then(Value::as_str) {
                    scope.push_str(&format!(" provider:{}", endpoint_authority(base)));
                }
                credential = binding
                    .get("client_key_env_name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .map_or_else(|| "none".to_owned(), |name| format!("env:{name}"));
            }
            None => {
                scope.push_str(" route:direct");
                credential = "ambient".to_owned();
            }
        },
        "opencode" => {
            if let Some(integration) = snapshot
                .get("auth_integration")
                .and_then(Value::as_str)
                .filter(|name| !name.trim().is_empty())
            {
                scope.push_str(&format!(" auth:{integration}"));
                credential = format!("auth:{integration}");
            } else if let Some(base) = snapshot.get("provider_base_url").and_then(Value::as_str) {
                if let Some(backend) = snapshot
                    .get("backend")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                {
                    scope.push_str(&format!(" backend:{backend}"));
                }
                scope.push_str(&format!(" provider:{}", endpoint_authority(base)));
                credential = snapshot
                    .get("api_key_env")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .map_or_else(|| "no-auth".to_owned(), |name| format!("env:{name}"));
            }
        }
        "muse" => {
            credential = "ambient".to_owned();
        }
        _ => {}
    }
    (scope, credential)
}

/// Strip a provider base URL to its authority (host with port, no userinfo
/// or path) for scope keys. Pure string slicing; never a network read.
fn endpoint_authority(url: &str) -> &str {
    let after_scheme = url.split("://").nth(1).unwrap_or(url);
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    authority.rsplit('@').next().unwrap_or(authority)
}

/// Bounded per-scope cache for catalogue and quota readback. One live
/// native read per scope per TTL; concurrent misses may double-read once
/// but still share the single cached entry (no per-Agent polling: quota and
/// catalogue reads are never tied to an Agent).
#[derive(Default)]
pub(crate) struct CatalogCache {
    models: HashMap<String, HarnessModelsResponse>,
    quotas: HashMap<String, HarnessQuotaResponse>,
    /// One aggregated `models.catalog` entry keyed by harness
    /// executable+version. Concurrent callers coalesce on the daemon
    /// `catalog_aggregate` async lock: the first rebuilds, the rest share the
    /// fresh entry until its TTL lapses.
    aggregate: Option<CachedAggregate>,
}

/// Aggregated `models.catalog` behind the paged response: the full sorted
/// model list plus the harness inventory it was built from, keyed by the
/// nonsecret executable+version fingerprint.
#[derive(Clone, Debug)]
pub(crate) struct CachedAggregate {
    /// `harness\0cmd\0version` per harness, sorted; a version or executable
    /// change invalidates the entry.
    pub(crate) key: String,
    pub(crate) harnesses: Vec<HarnessEntry>,
    pub(crate) models: Vec<ModelsCatalogEntry>,
    pub(crate) fetched_at_ms: i64,
    pub(crate) ttl_ms: u64,
}

impl CachedAggregate {
    pub(crate) fn fresh(&self, now: i64) -> bool {
        self.fetched_at_ms >= 0
            && now >= 0
            && (now.saturating_sub(self.fetched_at_ms) as u64) <= self.ttl_ms
    }
}

/// Fingerprint for the aggregate cache: per-harness executable and version.
/// Status-only changes (uninstalled with no executable) still fingerprint by
/// harness code so a later install invalidates.
pub(crate) fn aggregate_key(harnesses: &[HarnessEntry]) -> String {
    let mut parts: Vec<String> = harnesses
        .iter()
        .map(|entry| {
            format!(
                "{}\0{}\0{}",
                entry.harness,
                entry.cmd.as_deref().unwrap_or(""),
                entry.version.as_deref().unwrap_or("")
            )
        })
        .collect();
    parts.sort();
    parts.join("\n")
}

impl CatalogCache {
    pub(crate) fn get_models(&self, key: &str, now: i64) -> Option<HarnessModelsResponse> {
        self.models
            .get(key)
            .filter(|entry| fresh(entry.fetched_at_ms, entry.ttl_ms, now))
            .cloned()
    }

    pub(crate) fn insert_models(&mut self, key: String, entry: HarnessModelsResponse) {
        evict_models(&mut self.models, entry.fetched_at_ms);
        self.models.insert(key, entry);
    }

    pub(crate) fn get_quota(&self, scope: &str, now: i64) -> Option<HarnessQuotaResponse> {
        self.quotas
            .get(scope)
            .filter(|entry| fresh(entry.fetched_at_ms, entry.ttl_ms, now))
            .cloned()
    }

    pub(crate) fn insert_quota(&mut self, entry: HarnessQuotaResponse) {
        evict_quotas(&mut self.quotas, entry.fetched_at_ms);
        self.quotas.insert(entry.scope.clone(), entry);
    }

    pub(crate) fn get_aggregate(&self, key: &str, now: i64) -> Option<CachedAggregate> {
        self.aggregate
            .as_ref()
            .filter(|entry| entry.key == key && entry.fresh(now))
            .cloned()
    }

    pub(crate) fn insert_aggregate(&mut self, entry: CachedAggregate) {
        self.aggregate = Some(entry);
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> (usize, usize) {
        (self.models.len(), self.quotas.len())
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn aggregate_len(&self) -> usize {
        usize::from(self.aggregate.is_some())
    }
}

/// The private native-read key includes executable/version identity without
/// changing the public account scope. A failed version probe cannot reuse
/// a previously cached native read.
pub(crate) fn models_key(scope: &str, cmd: &str, version: Option<&str>) -> Option<String> {
    version.map(|version| format!("{scope}\0{cmd}\0{version}"))
}

/// Catalogue membership establishes identity, not account availability.
/// Only complete hold readback plus affirmative, fresh native quota signals
/// can establish availability. Unsupported and missing signals stay unknown.
pub(crate) fn availability(
    held: Option<bool>,
    quota: Option<&HarnessQuotaResponse>,
    model: &str,
    now: i64,
) -> &'static str {
    use eliot_core::api::harness::{CATALOG_AVAILABLE, CATALOG_HELD, CATALOG_UNKNOWN};
    if held == Some(true) {
        return CATALOG_HELD;
    }
    let Some(quota) = quota.filter(|quota| {
        held == Some(false)
            && quota.status == "supported"
            && fresh(quota.fetched_at_ms, quota.ttl_ms, now)
    }) else {
        return CATALOG_UNKNOWN;
    };
    let Some(details) = quota.details.as_ref() else {
        return CATALOG_UNKNOWN;
    };
    let headroom = |window: &Value| {
        window["usedPercent"]
            .as_i64()
            .is_some_and(|used| (0..100).contains(&used))
            && window["resetsAtMs"]
                .as_i64()
                .or_else(|| {
                    window["resetsAt"]
                        .as_i64()
                        .map(|time| time.saturating_mul(1000))
                })
                .is_some_and(|reset| reset > now)
    };
    let available = match quota.harness.as_str() {
        "muse" => headroom(&details["window"]) && headroom(&details["weekly"]),
        "codex" => {
            let rates = &details["rateLimits"];
            details["ordinaryUsageAllowed"].as_bool() == Some(true)
                && headroom(&rates["primary"])
                && headroom(&rates["secondary"])
                && match details.get("rateLimitsByLimitId") {
                    None | Some(Value::Null) => true,
                    Some(Value::Object(limits)) if limits.is_empty() => true,
                    Some(Value::Object(limits)) => limits.get(model).is_some_and(|rates| {
                        headroom(&rates["primary"]) && headroom(&rates["secondary"])
                    }),
                    _ => false,
                }
        }
        _ => false,
    };
    if available {
        CATALOG_AVAILABLE
    } else {
        CATALOG_UNKNOWN
    }
}

fn fresh(fetched_at_ms: i64, ttl_ms: u64, now: i64) -> bool {
    fetched_at_ms >= 0 && now >= 0 && (now.saturating_sub(fetched_at_ms) as u64) <= ttl_ms
}

fn evict_models(map: &mut HashMap<String, HarnessModelsResponse>, now: i64) {
    if map.len() < MAX_CACHED_SCOPES {
        return;
    }
    map.retain(|_, entry| fresh(entry.fetched_at_ms, entry.ttl_ms, now));
    if map.len() >= MAX_CACHED_SCOPES
        && let Some(key) = map.keys().next().cloned()
    {
        map.remove(&key);
    }
}

fn evict_quotas(map: &mut HashMap<String, HarnessQuotaResponse>, now: i64) {
    if map.len() < MAX_CACHED_SCOPES {
        return;
    }
    map.retain(|_, entry| fresh(entry.fetched_at_ms, entry.ttl_ms, now));
    if map.len() >= MAX_CACHED_SCOPES
        && let Some(key) = map.keys().next().cloned()
    {
        map.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn native_model_cache_invalidates_inside_ttl_on_executable_or_version_change() {
        let scope = "profile:adhoc harness:codex route:direct";
        let mut cache = CatalogCache::default();
        let key = models_key(scope, "C:/v1/codex.exe", Some("1.0")).unwrap();
        cache.insert_models(
            key.clone(),
            HarnessModelsResponse {
                harness: "codex".to_owned(),
                catalogue: "native".to_owned(),
                models: Vec::new(),
                default: Some("old-model".to_owned()),
                note: None,
                scope: scope.to_owned(),
                fetched_at_ms: 1_000,
                ttl_ms: CATALOG_TTL_MS,
                credential: "ambient".to_owned(),
                configure: Default::default(),
            },
        );
        assert_eq!(
            cache.get_models(&key, 1_001).unwrap().default.as_deref(),
            Some("old-model")
        );
        for (cmd, version) in [("C:/v1/codex.exe", "1.1"), ("C:/v2/codex.exe", "1.0")] {
            let changed = models_key(scope, cmd, Some(version)).unwrap();
            assert!(cache.get_models(&changed, 1_001).is_none());
        }
        assert!(models_key(scope, "C:/v1/codex.exe", None).is_none());
    }

    #[test]
    fn availability_requires_complete_holds_and_affirmative_fresh_quota() {
        let now = 1_000;
        let mut quota = HarnessQuotaResponse {
            harness: "muse".to_owned(),
            scope: "profile:adhoc harness:muse".to_owned(),
            fetched_at_ms: now,
            ttl_ms: CATALOG_TTL_MS,
            credential: "ambient".to_owned(),
            status: "supported".to_owned(),
            note: None,
            details: Some(json!({"window":{"usedPercent":10,"resetsAtMs":2_000},
                "weekly":{"usedPercent":20,"resetsAtMs":3_000}})),
        };
        assert_eq!(
            availability(Some(false), Some(&quota), "m", now),
            "available"
        );
        assert_eq!(availability(None, Some(&quota), "m", now), "unknown");
        assert_eq!(availability(Some(true), None, "m", now), "held");
        assert_eq!(availability(Some(false), None, "m", now), "unknown");
        for status in ["unsupported", "unknown"] {
            quota.status = status.to_owned();
            assert_eq!(availability(Some(false), Some(&quota), "m", now), "unknown");
        }
        quota.status = "supported".to_owned();
        for details in [
            json!({}),
            json!({"window":{"usedPercent":0,"resetsAtMs":2_000}}),
            json!({"window":{"usedPercent":0,"resetsAtMs":2_000},"weekly":{"usedPercent":100,"resetsAtMs":3_000}}),
            json!({"window":{"usedPercent":0,"resetsAtMs":999},"weekly":{"usedPercent":0,"resetsAtMs":3_000}}),
        ] {
            quota.details = Some(details);
            assert_eq!(availability(Some(false), Some(&quota), "m", now), "unknown");
        }
        quota.harness = "codex".to_owned();
        quota.details = Some(json!({"ordinaryUsageAllowed":true,
            "rateLimits":{"primary":{"usedPercent":0,"resetsAt":2},"secondary":{"usedPercent":0,"resetsAt":3}}}));
        assert_eq!(
            availability(Some(false), Some(&quota), "m", now),
            "available"
        );
        assert_eq!(
            availability(
                Some(false),
                Some(&quota),
                "m",
                now + CATALOG_TTL_MS as i64 + 1
            ),
            "unknown"
        );
        quota.details.as_mut().unwrap()["ordinaryUsageAllowed"] = Value::Bool(false);
        assert_eq!(availability(Some(false), Some(&quota), "m", now), "unknown");
        quota.details.as_mut().unwrap()["ordinaryUsageAllowed"] = Value::Bool(true);
        quota.details.as_mut().unwrap()["rateLimitsByLimitId"] = json!({"other":{}});
        assert_eq!(availability(Some(false), Some(&quota), "m", now), "unknown");
        quota.harness = "opencode".to_owned();
        assert_eq!(availability(Some(false), Some(&quota), "m", now), "unknown");
    }

    #[test]
    fn codex_direct_scope_uses_ambient_identity() {
        let snapshot = json!({"harness": "codex"});
        let (scope, credential) = describe_snapshot("adhoc", "codex", Some(&snapshot));
        assert_eq!(scope, "profile:adhoc harness:codex route:direct");
        assert_eq!(credential, "ambient");
    }

    #[test]
    fn codex_route_scope_names_key_but_never_value() {
        let snapshot = json!({
            "harness": "codex",
            "route_binding": {
                "name": "corp",
                "provider_base_url": "http://127.0.0.1:10300/v1",
                "client_key_env_name": "LOCAL_CLIENT",
            },
            "pass_env_values": {"LOCAL_CLIENT": "super-secret-value"},
            "literal_env": {"OTHER": "super-secret-value"},
        });
        let (scope, credential) = describe_snapshot("codex-main", "codex", Some(&snapshot));
        assert_eq!(
            scope,
            "profile:codex-main harness:codex route:corp provider:127.0.0.1:10300"
        );
        assert_eq!(credential, "env:LOCAL_CLIENT");
        assert!(!scope.contains("super-secret-value"));
        assert!(!credential.contains("super-secret-value"));
    }

    #[test]
    fn opencode_auth_and_backend_scopes() {
        let snapshot = json!({"harness": "opencode", "auth_integration": "personal"});
        let (scope, credential) = describe_snapshot("oc", "opencode", Some(&snapshot));
        assert_eq!(scope, "profile:oc harness:opencode auth:personal");
        assert_eq!(credential, "auth:personal");

        let snapshot = json!({
            "harness": "opencode",
            "backend": "lmstudio",
            "provider_base_url": "http://127.0.0.1:1234/v1",
        });
        let (scope, credential) = describe_snapshot("adhoc", "opencode", Some(&snapshot));
        assert_eq!(
            scope,
            "profile:adhoc harness:opencode backend:lmstudio provider:127.0.0.1:1234"
        );
        assert_eq!(credential, "no-auth");
    }

    #[test]
    fn static_harnesses_use_minimal_scope() {
        let (scope, credential) = describe_snapshot("adhoc", "antigravity", None);
        assert_eq!(scope, "profile:adhoc harness:antigravity");
        assert_eq!(credential, "none");
        let snapshot = json!({"harness": "muse", "cmd": "C:/Tools/muse.exe"});
        let (scope, credential) = describe_snapshot("adhoc", "muse", Some(&snapshot));
        assert_eq!(scope, "profile:adhoc harness:muse");
        assert_eq!(credential, "ambient");
    }

    #[test]
    fn cache_serves_fresh_and_drops_stale() {
        let mut cache = CatalogCache::default();
        let entry = HarnessQuotaResponse {
            harness: "codex".to_owned(),
            scope: "profile:adhoc harness:codex route:direct".to_owned(),
            fetched_at_ms: 1_000,
            ttl_ms: 60_000,
            credential: "ambient".to_owned(),
            status: "unsupported".to_owned(),
            note: None,
            details: None,
        };
        cache.insert_quota(entry);
        assert!(
            cache
                .get_quota("profile:adhoc harness:codex route:direct", 61_000)
                .is_some()
        );
        assert!(
            cache
                .get_quota("profile:adhoc harness:codex route:direct", 61_001)
                .is_none()
        );
        assert!(cache.get_quota("other", 1_000).is_none());
    }

    #[test]
    fn cache_stays_bounded() {
        let mut cache = CatalogCache::default();
        for index in 0..(MAX_CACHED_SCOPES + 5) {
            cache.insert_quota(HarnessQuotaResponse {
                harness: "codex".to_owned(),
                scope: format!("scope-{index}"),
                fetched_at_ms: 1_000 + index as i64,
                ttl_ms: CATALOG_TTL_MS,
                credential: "ambient".to_owned(),
                status: "unsupported".to_owned(),
                note: None,
                details: None,
            });
        }
        assert!(cache.len().1 <= MAX_CACHED_SCOPES);
    }

    fn harness_entry(harness: &str, cmd: &str, version: &str) -> HarnessEntry {
        HarnessEntry {
            harness: harness.to_owned(),
            installed: true,
            cmd: Some(cmd.to_owned()),
            version: Some(version.to_owned()),
            code: None,
            error: None,
            candidates: None,
        }
    }

    #[test]
    fn aggregate_key_follows_executable_and_version() {
        let first = vec![
            harness_entry("codex", "codex", "1.0"),
            harness_entry("muse", "C:/Tools/muse.exe", "1.4.0"),
        ];
        let same = vec![
            harness_entry("muse", "C:/Tools/muse.exe", "1.4.0"),
            harness_entry("codex", "codex", "1.0"),
        ];
        assert_eq!(aggregate_key(&first), aggregate_key(&same));
        let upgraded = vec![
            harness_entry("codex", "codex", "1.1"),
            harness_entry("muse", "C:/Tools/muse.exe", "1.4.0"),
        ];
        assert_ne!(aggregate_key(&first), aggregate_key(&upgraded));
    }

    #[test]
    fn aggregate_cache_coalesces_until_ttl_and_version_change() {
        let mut cache = CatalogCache::default();
        let harnesses = vec![harness_entry("codex", "codex", "1.0")];
        let key = aggregate_key(&harnesses);
        let entry = CachedAggregate {
            key: key.clone(),
            harnesses: harnesses.clone(),
            models: Vec::new(),
            fetched_at_ms: 1_000,
            ttl_ms: CATALOG_TTL_MS,
        };
        cache.insert_aggregate(entry);
        assert_eq!(cache.aggregate_len(), 1);
        // Concurrent callers share the fresh entry: the same key hits.
        assert!(cache.get_aggregate(&key, 1_000).is_some());
        assert!(
            cache
                .get_aggregate(&key, 1_000 + CATALOG_TTL_MS as i64)
                .is_some()
        );
        assert!(
            cache
                .get_aggregate(&key, 1_000 + CATALOG_TTL_MS as i64 + 1)
                .is_none()
        );
        let upgraded = vec![harness_entry("codex", "codex", "1.1")];
        assert!(
            cache
                .get_aggregate(&aggregate_key(&upgraded), 1_000)
                .is_none()
        );
        // A rebuild replaces the single bounded entry, never grows it.
        cache.insert_aggregate(CachedAggregate {
            key: aggregate_key(&upgraded),
            harnesses: upgraded,
            models: Vec::new(),
            fetched_at_ms: 2_000,
            ttl_ms: CATALOG_TTL_MS,
        });
        assert_eq!(cache.aggregate_len(), 1);
    }
}
