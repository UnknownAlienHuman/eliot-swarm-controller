//! Bounded cached catalogue/quota scope derivation for `harness.models`
//! and `harness.quota`.
//!
//! The cache is keyed by the nonsecret native route/account scope, so one
//! live native read serves every caller on that scope until its TTL lapses.
//! Scope and credential identity derive only from frozen snapshot selectors
//! (harness, profile label, route/binding names); credential values,
//! `pass_env` resolutions and literal environment bodies are never read here.

use eliot_core::api::{HARNESS_READ_TTL_MS, HarnessModelsResponse, HarnessQuotaResponse};
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
}

impl CatalogCache {
    pub(crate) fn get_models(&self, scope: &str, now: i64) -> Option<HarnessModelsResponse> {
        self.models
            .get(scope)
            .filter(|entry| fresh(entry.fetched_at_ms, entry.ttl_ms, now))
            .cloned()
    }

    pub(crate) fn insert_models(&mut self, entry: HarnessModelsResponse) {
        evict_models(&mut self.models, entry.fetched_at_ms);
        self.models.insert(entry.scope.clone(), entry);
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

    #[cfg(test)]
    pub(crate) fn len(&self) -> (usize, usize) {
        (self.models.len(), self.quotas.len())
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
}
