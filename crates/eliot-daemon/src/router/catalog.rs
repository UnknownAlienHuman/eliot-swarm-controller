//! Bounded cached catalogue/quota scope derivation for `harness.models`
//! and `harness.quota`.
//!
//! Live cache reuse requires a frozen route verified against the reader's
//! private process environment. Public scope labels contain only route names.

use super::{MODE_OPEN, Principal, Router, catalog, now_ms, require_owner};
use eliot_core::api::{
    CLAUDE_EXTERNAL_ROUTE_MARKERS, HARNESS_READ_TTL_MS, HarnessCandidate, HarnessEntry,
    HarnessModelsResponse, HarnessQuotaResponse, ModelsCatalogEntry, ModelsCatalogFailure,
    SUPPORTED_HARNESSES,
};
use eliot_core::{Error, Result};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::{sync::atomic::Ordering, time::Instant};

impl Router {
    /// Read-only harness installation facts: no model turn, no inference.
    pub(super) async fn harness_list(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: eliot_core::api::HarnessListRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        Self::harness_inventory(Instant::now() + crate::adhoc::DISCOVERY_BUDGET).await
    }

    async fn harness_inventory(deadline: Instant) -> Result<Value> {
        // Filesystem presence plus bounded `--version` probes is blocking
        // work: run it on the blocking pool so slow native CLIs never stall
        // async command/event service. Claude candidates are enumerated once
        // and reused for both the entry and the candidate list instead of
        // versioning every install twice per inventory pass.
        let harnesses: Vec<eliot_core::api::HarnessEntry> =
            crate::adhoc::blocking_discovery(deadline, move || {
                let claude_candidates = crate::adhoc::discover_claude_candidates(deadline);
                let mut harnesses = Vec::new();
                for harness in SUPPORTED_HARNESSES {
                    let entry = if *harness == "claude" {
                        crate::adhoc::claude_installation(&claude_candidates)
                    } else {
                        crate::adhoc::harness_installation(harness, deadline)
                    };
                    // Report eligible standalone Claude installs newest first.
                    let candidates = if *harness == "claude" {
                        (!claude_candidates.is_empty()).then(|| claude_candidates.clone())
                    } else {
                        None
                    };
                    harnesses.push(eliot_core::api::HarnessEntry {
                        harness: (*harness).to_owned(),
                        installed: entry
                            .get("installed")
                            .and_then(Value::as_bool)
                            .unwrap_or(false),
                        cmd: entry.get("cmd").and_then(Value::as_str).map(str::to_owned),
                        version: entry
                            .get("version")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        code: entry.get("code").and_then(Value::as_str).map(str::to_owned),
                        error: entry
                            .get("error")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        candidates,
                    });
                }
                harnesses
            })
            .await?;
        let response = eliot_core::api::HarnessListResponse { harnesses };
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness list could not be encoded"))
    }
    /// Resolve a named profile or an ad-hoc snapshot with its discovered version.
    async fn resolve_catalog_snapshot(
        &self,
        harness: &str,
        profile: Option<&str>,
        quota: bool,
        deadline: Instant,
    ) -> Result<(Option<Value>, String, Option<HarnessCandidate>)> {
        if let Some(name) = profile {
            let mut snapshot = self.resolve_profile_cached(name)?;
            crate::config::capture_native_store_inputs(&mut snapshot, |name| {
                std::env::var_os(name)
            })?;
            if snapshot.get("harness").and_then(Value::as_str) != Some(harness) {
                return Err(Error::new(
                    "invalid",
                    "harness does not match the selected profile",
                ));
            }
            return Ok((Some(snapshot), name.to_owned(), None));
        }
        let (cmd, discovered) = if harness == "claude" && !quota {
            match crate::adhoc::claude_reader_executable(deadline).await {
                Ok(candidate) => (Some(candidate.cmd.clone()), Some(candidate)),
                Err(error) if catalog::claude_fallback_eligible(&error) => {
                    return Ok((None, "adhoc".to_owned(), None));
                }
                Err(error) => return Err(error),
            }
        } else {
            (
                crate::adhoc::discover_cmd_until(harness, deadline).await?,
                None,
            )
        };
        let mut snapshot = crate::adhoc::adhoc_snapshot(
            harness,
            None,
            None,
            cmd,
            crate::adhoc::AdhocAuth::Skip,
            None,
        )?;
        if quota && harness == "opencode" {
            // Named launch routes bind their own integration.
            snapshot["auth_integration"] = json!("opencode-go");
        }
        crate::config::stamp_cloud_mcp_hostname(&mut snapshot, &self.inner.data_root);
        crate::config::capture_native_store_inputs(&mut snapshot, |name| std::env::var_os(name))?;
        Ok((Some(snapshot), "adhoc".to_owned(), discovered))
    }

    /// The route an existing Agent's `configure` draws from: its frozen
    /// launch snapshot, never the current profile file. The label carries the
    /// exact Agent generation so a later profile edit cannot share its cache
    /// entry. Observed Agents have no Eliot launch route. OpenCode uses the
    /// captured generation to read the live session catalogue.
    async fn agent_catalog_snapshot(
        &self,
        agent: &str,
        harness: &str,
    ) -> Result<(Value, String, (String, i64))> {
        let agent = self
            .inner
            .store
            .lock()
            .await
            .get_agent(agent)?
            .ok_or_else(|| Error::new("not_found", "Agent was not found"))?;
        if agent.observe_only || !agent.profile_snapshot.is_object() {
            return Err(Error::new(
                "not_allowed",
                "an observed Agent has no Eliot launch route",
            ));
        }
        if agent.harness != harness {
            return Err(Error::new(
                "invalid",
                "harness does not match the selected Agent",
            ));
        }
        let label = format!("{}@{}g{}", agent.profile, agent.id, agent.generation);
        Ok((agent.profile_snapshot, label, (agent.id, agent.generation)))
    }

    /// Read-only native model catalogue for a harness: no model turn, no
    /// inference. Dedicated metadata readers use the harness's own catalogue
    /// and cache by verified route and executable. Claude can fall back to
    /// documented aliases when its native inventory is unavailable.
    pub(super) async fn harness_models(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        self.harness_models_until(
            params,
            principal,
            Instant::now() + crate::adhoc::DISCOVERY_BUDGET,
        )
        .await
    }

    async fn harness_models_until(
        &self,
        params: &Value,
        principal: &Principal,
        deadline: Instant,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: eliot_core::api::HarnessModelsRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let harness = request.harness.trim().to_owned();
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        let profile = request.profile.as_deref().map(str::trim);
        let (snapshot, label, discovered, live_identity) = match request.agent.as_deref() {
            Some(agent) => {
                let (snapshot, label, identity) =
                    self.agent_catalog_snapshot(agent, &harness).await?;
                (Some(snapshot), label, None, Some(identity))
            }
            None => {
                let (snapshot, label, discovered) = self
                    .resolve_catalog_snapshot(&harness, profile, false, deadline)
                    .await?;
                (snapshot, label, discovered, None)
            }
        };
        let response =
            if let Some((agent, generation)) = live_identity.filter(|_| harness == "opencode") {
                let (scope, credential) =
                    catalog::describe_snapshot(&label, &harness, snapshot.as_ref());
                let raw = self
                    .inner
                    .plugin_host
                    .agent_models(&agent, generation)
                    .await?;
                if !self
                    .inner
                    .store
                    .lock()
                    .await
                    .get_agent(&agent)?
                    .is_some_and(|agent| agent.generation == generation)
                {
                    return Err(Error::new(
                        "stale_generation",
                        "Agent generation changed during its model read",
                    ));
                }
                catalog::native_models_response(&harness, "", scope, credential, "session", &raw)?
            } else {
                self.models_for_snapshot(
                    &harness,
                    snapshot,
                    &label,
                    discovered,
                    request.workspace.clone(),
                    deadline,
                )
                .await?
            };
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness models could not be encoded"))
    }

    pub(super) async fn models_for_snapshot(
        &self,
        harness: &str,
        snapshot: Option<Value>,
        label: &str,
        discovered: Option<HarnessCandidate>,
        workspace: Option<String>,
        deadline: Instant,
    ) -> Result<HarnessModelsResponse> {
        let (scope, credential) = catalog::describe_snapshot(label, harness, snapshot.as_ref());
        let read = async {
            let mut profile = snapshot.ok_or_else(|| {
                Error::new(
                    "unavailable",
                    "native model catalogue executable was not discovered",
                )
            })?;
            let route = crate::plugin_host::metadata_route(&profile)?;
            if harness == "claude" && route.identity.is_none() {
                return Err(Error::new(
                    "unavailable",
                    "Claude model catalogue route could not be verified",
                ));
            }
            let cmd = profile["cmd"].as_str().unwrap_or(harness);
            let (cmd, version) = match discovered {
                Some(candidate) => (candidate.cmd, candidate.version),
                None => crate::adhoc::reader_executable(cmd, harness, deadline).await?,
            };
            profile["cmd"] = Value::String(cmd.clone());
            let cache_key = if workspace.is_none() {
                catalog::canonical_cache_identity(
                    label,
                    &profile,
                    route.identity.as_deref(),
                    &route.environment,
                )
                .and_then(|key| catalog::models_key(&key, &cmd, version.as_deref()))
            } else {
                None
            };
            if let Some(hit) = cache_key.as_deref().and_then(|key| {
                self.inner
                    .catalog_cache
                    .lock_recover()
                    .get_models(key, now_ms())
            }) {
                return Ok((hit, None));
            }
            let raw = self
                .inner
                .plugin_host
                .models(profile, route.environment, workspace, deadline.into())
                .await?;
            let provenance = if raw["provenance"] == "preview" {
                "preview"
            } else {
                "canonical"
            };
            let response = catalog::native_models_response(
                harness,
                &cmd,
                scope.clone(),
                credential.clone(),
                provenance,
                &raw,
            )?;
            Ok((response, cache_key))
        }
        .await;
        match read {
            Ok((response, cache_key)) => {
                response.validate()?;
                if let Some(key) = cache_key {
                    self.inner
                        .catalog_cache
                        .lock_recover()
                        .insert_models(key, response.clone());
                }
                Ok(response)
            }
            Err(error) if harness == "claude" && catalog::claude_fallback_eligible(&error) => {
                Ok(catalog::claude_alias_fallback(scope, credential))
            }
            Err(error) => Err(error),
        }
    }

    /// Provider-supported account quota readback: timestamp, scope and an
    /// explicit supported/unsupported/unknown status, cached per account.
    /// Starts no model turn, performs no inference and never polls per
    /// Agent. Codex reads `account/rateLimits/read` (plus `account/usage/read`
    /// when useful) and Muse reads `usage/read` through a dedicated metadata
    /// reader; Claude reads the CLI OAuth usage endpoint, OpenCode reads the
    /// Go account usage endpoint, and Antigravity reads
    /// `retrieveUserQuotaSummary`. Every reader projects provider quota
    /// signals into `details`: Codex keeps its `rateLimits` shape, the other
    /// four use `windows[]` with `usedPercent`/`resetsAtMs` (Muse live reads
    /// nest the same windows under `usage`, retained Muse observations carry
    /// `windows[]` directly). Routing and the catalogue consume all five
    /// through one normalized projection ([`catalog::project_quota`]).
    /// A method-not-found or schema mismatch stays `unsupported` or `unknown`
    /// naming the checked method; no numbers are fabricated. A quota failure
    /// never selects another model or account.
    pub(super) async fn harness_quota(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let evidence = self
            .harness_quota_until(
                params,
                principal,
                Instant::now() + crate::adhoc::DISCOVERY_BUDGET,
            )
            .await?;
        serde_json::to_value(evidence.response)
            .map_err(|_| Error::new("internal", "quota encoding failed"))
    }

    async fn harness_quota_until(
        &self,
        params: &Value,
        principal: &Principal,
        deadline: Instant,
    ) -> Result<catalog::ScopedQuota> {
        require_owner(principal)?;
        let request: eliot_core::api::HarnessQuotaRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let harness = request.harness.trim().to_owned();
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        let profile = request.profile.as_deref().map(str::trim);
        let (snapshot, label, _) = self
            .resolve_catalog_snapshot(&harness, profile, true, deadline)
            .await?;
        self.quota_for_snapshot(harness, snapshot, label, deadline)
            .await
    }

    pub(super) async fn quota_for_snapshot(
        &self,
        harness: String,
        snapshot: Option<Value>,
        label: String,
        deadline: Instant,
    ) -> Result<catalog::ScopedQuota> {
        let request = eliot_core::api::HarnessQuotaRequest {
            harness: harness.clone(),
            profile: None,
        };
        let (scope, credential) = catalog::describe_snapshot(&label, &harness, snapshot.as_ref());
        let mut profile = snapshot
            .ok_or_else(|| Error::new("profile_error", "quota requires a native profile"))?;
        let route = crate::plugin_host::metadata_route(&profile)?;
        // Universal policy, decided before any reader spawns and before any
        // cache access: an external provider route never reports CLI/keyring
        // quota for another route. The refusal flows through the same shaped
        // error arm as a native reader refusal, so routing and
        // `harness.quota` see identical facts, and it is never cached.
        // Unavailable quota stays `unknown` via the existing projection,
        // never 0.
        if let Some(note) = catalog::external_quota_route(&harness, &profile, &route.environment) {
            let mut response = eliot_core::api::HarnessQuotaResponse {
                harness: harness.clone(),
                scope,
                fetched_at_ms: now_ms(),
                ttl_ms: catalog::CATALOG_TTL_MS,
                credential,
                status: eliot_core::api::HARNESS_QUOTA_SUPPORTED.to_owned(),
                note: None,
                details: None,
            };
            catalog::quota_read_error(&mut response, eliot_plugin::quota::environment(note));
            request.validate_response(&response)?;
            let route_verified = catalog::canonical_cache_identity(
                &label,
                &profile,
                route.identity.as_deref(),
                &route.environment,
            )
            .is_some();
            return Ok(catalog::ScopedQuota {
                response,
                route_verified,
            });
        }
        if harness == "antigravity" {
            let cmd = profile["cmd"].as_str().unwrap_or("agy");
            let (cmd, _) = crate::adhoc::reader_executable(cmd, &harness, deadline).await?;
            profile["cmd"] = Value::String(cmd);
        }
        let cache_key = catalog::canonical_cache_identity(
            &label,
            &profile,
            route.identity.as_deref(),
            &route.environment,
        );
        let route_verified = cache_key.is_some();
        if harness == "muse"
            && route_verified
            && let Some(observation) = self
                .inner
                .store
                .lock()
                .await
                .latest_quota_observation(&profile)?
        {
            return Ok(catalog::ScopedQuota {
                route_verified,
                response: HarnessQuotaResponse {
                    harness,
                    scope,
                    credential,
                    fetched_at_ms: observation.observed_at_ms,
                    ttl_ms: catalog::CATALOG_TTL_MS,
                    status: "supported".to_owned(),
                    note: Some("Muse subscription usage observed during a turn".to_owned()),
                    details: Some(json!({"windows": observation.windows, "state": "ready",
                        "origin": "provider", "source": "Muse MSP usage/changed"})),
                },
            });
        }
        if let Some(hit) = cache_key.as_deref().and_then(|key| {
            self.inner
                .catalog_cache
                .lock_recover()
                .get_quota(key, now_ms())
        }) {
            request.validate_response(&hit)?;
            return Ok(catalog::ScopedQuota {
                response: hit,
                route_verified,
            });
        }
        let raw = self
            .inner
            .plugin_host
            .quota(profile, route.environment, deadline.into())
            .await;
        let mut response = eliot_core::api::HarnessQuotaResponse {
            harness: harness.clone(),
            scope,
            fetched_at_ms: now_ms(),
            ttl_ms: catalog::CATALOG_TTL_MS,
            credential,
            status: eliot_core::api::HARNESS_QUOTA_SUPPORTED.to_owned(),
            note: None,
            details: None,
        };
        match raw {
            Ok(raw) => {
                if raw.get("status").and_then(Value::as_str) == Some("unsupported") {
                    response.status = "unsupported".to_owned();
                    response.note = Some(
                        raw.get("note")
                            .and_then(Value::as_str)
                            .unwrap_or("Native quota source unavailable")
                            .to_owned(),
                    );
                } else if let Some(details) = raw.get("details").filter(|v| v.is_object()) {
                    if let Some(windows) = details.get("windows") {
                        let observation: eliot_core::api::harness::QuotaObservation =
                            serde_json::from_value(
                                json!({"observedAtMs":now_ms(),"windows":windows}),
                            )
                            .map_err(|_| Error::new("source_integrity", "quota windows invalid"))?;
                        observation.validate()?;
                    }
                    response.details = Some(details.clone());
                    response.note=Some(match harness.as_str() {
                        "codex" => "Codex account/rateLimits/read; provider read-only values",
                        "muse" => "Muse subscription usage observed during a turn; timestamp is the native observation",
                        "claude" => "Claude CLI OAuth usage; provider read-only values",
                        "antigravity" => "Antigravity account quota; provider read-only values",
                        _ if details["integration"] == "opencode" => "OpenCode Console workspace budgets and Go allowance; provider read-only values",
                        _ => "OpenCode Go account usage; provider read-only values",
                    }.to_owned());
                    if harness == "muse" {
                        if let Some(at) = details
                            .pointer("/usage/observedAtMs")
                            .and_then(Value::as_i64)
                        {
                            response.fetched_at_ms = at;
                        } else {
                            response.status = "unsupported".to_owned();
                            response.details = None;
                            response.note=Some("Muse MSP usage/read has no observed subscription usage; no turn is started to obtain quota".to_owned());
                        }
                    }
                } else {
                    response.details = Some(
                        json!({"state":"error","origin":"provider","error":"native quota schema unavailable"}),
                    );
                }
            }
            // A local reader-capacity refusal says nothing about the provider:
            // return it uncached so the next read tries again.
            Err(error) if error.code == "busy" => return Err(error),
            Err(error) => {
                catalog::quota_read_error(&mut response, error);
            }
        }
        request.validate_response(&response)?;
        if let Some(key) = cache_key {
            self.inner
                .catalog_cache
                .lock_recover()
                .insert_quota(key, response.clone());
        }
        Ok(catalog::ScopedQuota {
            response,
            route_verified,
        })
    }

    /// One owner-scoped read-only search over every installed harness
    /// catalogue. Results are cached per harness executable and version with
    /// a bounded TTL; concurrent callers coalesce on the aggregate lock. The
    /// optional query filters case-insensitively over model id, display
    /// name, provider and harness. The response is bounded and paginated.
    pub(super) async fn models_catalog(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let deadline = Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
        self.models_catalog_until(
            params,
            principal,
            deadline,
            Self::harness_inventory(deadline),
            |params| async move {
                self.harness_models_until(&params, principal, deadline)
                    .await
            },
            |params| async move { self.harness_quota_until(&params, principal, deadline).await },
        )
        .await
    }

    async fn models_catalog_until<ModelsRead, QuotaRead>(
        &self,
        params: &Value,
        principal: &Principal,
        deadline: Instant,
        inventory: impl Future<Output = Result<Value>> + Send,
        read_models: impl Fn(Value) -> ModelsRead + Send,
        read_quota: impl Fn(Value) -> QuotaRead + Send,
    ) -> Result<Value>
    where
        ModelsRead: Future<Output = Result<Value>> + Send,
        QuotaRead: Future<Output = Result<catalog::ScopedQuota>> + Send,
    {
        use eliot_core::api::{ModelsCatalogEntry, ModelsCatalogFailure, ModelsCatalogRequest};
        require_owner(principal)?;
        let request: ModelsCatalogRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        // The aggregate budget starts here and covers inventory: a slow
        // inventory leaves less (or no) time for per-harness reads, which
        // then fail open into the partial aggregate below.
        let discovery_deadline = tokio::time::Instant::from_std(deadline);
        let query = request.query.as_deref().unwrap_or("").to_owned();
        let limit = request.effective_limit();
        let after = request.after.clone();
        // Coalesce concurrent callers: the first rebuilds, the rest share
        // the fresh aggregate until its TTL lapses.
        let _coalesce =
            tokio::time::timeout_at(discovery_deadline, self.inner.catalog_aggregate_lock.lock())
                .await
                .map_err(|_| crate::adhoc::discovery_timeout())?;
        // Harness inventory with versions for the aggregate key.
        let harnesses_value = inventory.await?;
        let harnesses_response: eliot_core::api::HarnessListResponse =
            serde_json::from_value(harnesses_value)
                .map_err(|_| Error::new("internal", "harness list could not be decoded"))?;
        let harnesses = harnesses_response.harnesses;
        let key = catalog::aggregate_key(&harnesses);
        let now = now_ms();
        if let Some(hit) =
            self.inner
                .catalog_cache
                .lock_recover()
                .get_aggregate(&key, now, after.is_some())
        {
            return Self::catalog_page(
                &request,
                &hit.harnesses,
                &hit.models,
                &hit.failures,
                &query,
                limit,
                after.as_deref(),
                hit.fetched_at_ms,
                hit.ttl_ms,
            );
        }
        // Provider holds for availability: a hold whose scope names the
        // harness marks that harness's models held.
        const HOLD_LIMIT: usize = 1000;
        let holds = {
            let store = tokio::time::timeout_at(discovery_deadline, self.inner.store.lock())
                .await
                .map_err(|_| crate::adhoc::discovery_timeout())?;
            store.list_provider_holds(None, HOLD_LIMIT).ok()
        };
        let held_harness = |harness: &str| {
            let page = holds.as_ref()?;
            let scope = format!("profile:{}", crate::adhoc::adhoc_profile_name(harness));
            if page.holds.iter().any(|hold| {
                hold.scope == scope
                    && matches!(
                        hold.state,
                        eliot_core::ProviderHoldState::Held
                            | eliot_core::ProviderHoldState::Probing
                    )
            }) {
                Some(true)
            } else if page.holds.len() < HOLD_LIMIT {
                Some(false)
            } else {
                None
            }
        };
        let mut models: Vec<ModelsCatalogEntry> = Vec::new();
        let mut failures: Vec<ModelsCatalogFailure> = Vec::new();
        let mut quotas = HashMap::new();
        for entry in &harnesses {
            let harness = entry.harness.as_str();
            if tokio::time::Instant::now() >= discovery_deadline {
                failures.push(ModelsCatalogFailure::from_error(
                    harness,
                    &Error::new(
                        "unavailable",
                        "catalogue discovery budget elapsed; harness skipped",
                    ),
                ));
                continue;
            }
            if !entry.installed {
                continue;
            }
            // Reuse the per-scope cached `harness.models` path so one live
            // native read serves every caller until its TTL lapses.
            let params = json!({"harness": harness});
            // A slow read past the request deadline fails open into the
            // partial aggregate instead of holding the coalescing lock.
            let models_read =
                tokio::time::timeout_at(discovery_deadline, read_models(params.clone())).await;
            let ad_hoc_response = match models_read {
                Ok(Ok(value)) => Some(value),
                Ok(Err(error)) => {
                    // The aggregate names the failed harness with its typed
                    // error instead of silently listing no models for it.
                    // Named profile discovery below still runs independently.
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "catalog.harness_failed",
                        &[("source", harness), ("method", "harness.models")],
                        Some(&error),
                    );
                    failures.push(ModelsCatalogFailure::from_error(harness, &error));
                    None
                }
                Err(_) => {
                    failures.push(ModelsCatalogFailure::from_error(
                        harness,
                        &Error::new(
                            "unavailable",
                            "catalogue discovery budget elapsed; harness skipped",
                        ),
                    ));
                    None
                }
            };
            let mut ad_hoc_quota_for_cache: Option<catalog::ScopedQuota> = None;
            if let Some(response_value) = ad_hoc_response {
                let response: eliot_core::api::HarnessModelsResponse =
                    serde_json::from_value(response_value).map_err(|_| {
                        Error::new("internal", "harness models could not be decoded")
                    })?;
                let held = held_harness(harness);
                let quota = tokio::time::timeout_at(discovery_deadline, read_quota(params))
                    .await
                    .ok()
                    .and_then(Result::ok);
                for model in response.models {
                    models.push(catalog::catalog_entry(
                        harness,
                        model,
                        held,
                        quota.as_ref(),
                        now_ms(),
                    ));
                }
                ad_hoc_quota_for_cache = quota;
            }
            // Profile models are independent preset rows. They remain visible
            // when ad-hoc discovery returns the same id, and never inherit the
            // ad-hoc hold or quota.
            if harness == "opencode" {
                let profile_names =
                    crate::config::profile_names_for_harness(&self.inner.data_root, harness);
                let total = profile_names.len();
                let mut attempted: usize = 0;
                // Named-preview failures coalesce into one bounded entry
                // naming the affected profiles: per-profile pushes would
                // exceed the response failure bound and invalidate the whole
                // catalogue. A profile-scoped row launches its profile as
                // configured, so only its frozen model and effort are shown.
                let mut preview_failed: Vec<String> = Vec::new();
                let mut preview_incomplete: Option<String> = None;
                for name in profile_names {
                    if tokio::time::Instant::now() >= discovery_deadline {
                        preview_incomplete = Some(format!(
                            "catalogue discovery budget elapsed; profile preview incomplete ({attempted} of {total} named profiles)"
                        ));
                        break;
                    }
                    attempted += 1;
                    let scoped = json!({"harness": harness, "profile": name});
                    let scoped_read =
                        tokio::time::timeout_at(discovery_deadline, read_models(scoped)).await;
                    let scoped_value = match scoped_read {
                        Ok(Ok(value)) => value,
                        Ok(Err(_)) => {
                            preview_failed.push(name.clone());
                            continue;
                        }
                        Err(_) => {
                            preview_incomplete = Some(format!(
                                "catalogue discovery budget elapsed; profile preview incomplete ({attempted} of {total} named profiles)"
                            ));
                            break;
                        }
                    };
                    let scoped_response = match serde_json::from_value::<
                        eliot_core::api::HarnessModelsResponse,
                    >(scoped_value)
                    {
                        Ok(response) => response,
                        Err(_) => {
                            preview_failed.push(name.clone());
                            continue;
                        }
                    };
                    let configured = self
                        .resolve_profile_cached(&name)
                        .ok()
                        .and_then(|snapshot| catalog::profile_frozen_selection(&snapshot));
                    // A named profile is a preset, not another ad-hoc model
                    // source. Profiles without a frozen model have no
                    // truthful picker row and must not advertise every model
                    // returned by their native reader.
                    let Some((configured_model, frozen_effort)) = configured else {
                        continue;
                    };
                    for model in scoped_response.models {
                        if model.id != configured_model {
                            continue;
                        }
                        models.push(catalog::profile_catalog_entry(
                            harness,
                            model,
                            &name,
                            frozen_effort.as_deref(),
                            now_ms(),
                        ));
                    }
                }
                if let Some(entry) = catalog::coalesced_preview_failure(
                    &preview_failed,
                    preview_incomplete.as_deref(),
                ) {
                    if let Some(existing) = failures
                        .iter_mut()
                        .find(|failure| failure.harness == "opencode")
                    {
                        catalog::append_failure_message(existing, &entry.message);
                    } else {
                        failures.push(entry);
                    }
                }
            }
            if let Some(quota) = ad_hoc_quota_for_cache.filter(|quota| quota.route_verified) {
                quotas.insert(harness.to_owned(), quota);
            }
        }
        models.sort_by(|left, right| {
            (
                left.harness.as_str(),
                left.id.as_str(),
                left.profile.as_deref(),
            )
                .cmp(&(
                    right.harness.as_str(),
                    right.id.as_str(),
                    right.profile.as_deref(),
                ))
        });
        let fetched_at_ms = now_ms();
        // A partial aggregate is never kept for the full TTL: it serves only
        // the rest of this listing's pages, and the next listing reads the
        // failed harness again (the others come from their per-scope
        // `harness.models` cache).
        let ttl_ms = if failures.is_empty() {
            catalog::CATALOG_TTL_MS
        } else {
            catalog::PARTIAL_AGGREGATE_TTL_MS
        };
        self.inner
            .catalog_cache
            .lock_recover()
            .insert_aggregate(catalog::CachedAggregate {
                key: key.clone(),
                harnesses: harnesses.clone(),
                models: models.clone(),
                quotas,
                failures: failures.clone(),
                fetched_at_ms,
                ttl_ms,
            });
        Self::catalog_page(
            &request,
            &harnesses,
            &models,
            &failures,
            &query,
            limit,
            after.as_deref(),
            fetched_at_ms,
            ttl_ms,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn catalog_page(
        request: &eliot_core::api::ModelsCatalogRequest,
        harnesses: &[eliot_core::api::HarnessEntry],
        models: &[eliot_core::api::ModelsCatalogEntry],
        failures: &[eliot_core::api::ModelsCatalogFailure],
        query: &str,
        limit: usize,
        after: Option<&str>,
        fetched_at_ms: i64,
        ttl_ms: u64,
    ) -> Result<Value> {
        use eliot_core::api::{ModelsCatalogResponse, catalog_matches};
        let mut filtered: Vec<eliot_core::api::ModelsCatalogEntry> = models
            .iter()
            .filter(|entry| catalog_matches(entry, query))
            .cloned()
            .collect();
        filtered.sort_by(|left, right| {
            (
                left.harness.as_str(),
                left.id.as_str(),
                left.profile.as_deref(),
            )
                .cmp(&(
                    right.harness.as_str(),
                    right.id.as_str(),
                    right.profile.as_deref(),
                ))
        });
        let start = match after {
            None => 0,
            Some(cursor) => filtered
                .iter()
                .position(|entry| entry.cursor() == cursor)
                .map(|index| index + 1)
                .ok_or_else(|| {
                    Error::new("invalid", eliot_core::api::CATALOG_CURSOR_STALE_MESSAGE)
                })?,
        };
        let page: Vec<eliot_core::api::ModelsCatalogEntry> =
            filtered.into_iter().skip(start).take(limit).collect();
        let next = if page.len() == limit {
            page.last().map(eliot_core::api::ModelsCatalogEntry::cursor)
        } else {
            None
        };
        let response = ModelsCatalogResponse {
            harnesses: harnesses.to_owned(),
            models: page,
            next,
            fetched_at_ms,
            ttl_ms,
            failures: failures.to_owned(),
        };
        request.validate_response(&response)?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "unified catalogue could not be encoded"))
    }
}

/// Maximum cached scopes per read kind.
const MAX_CACHED_SCOPES: usize = 32;

/// Serve cached catalogue/quota entries for this long after the native read.
pub(crate) const CATALOG_TTL_MS: u64 = HARNESS_READ_TTL_MS;

/// A partial aggregate (some harness read failed) serves only the
/// continuation pages of the listing that built it, for at most this long;
/// every first page rebuilds it, so the failed harness is read again.
pub(crate) const PARTIAL_AGGREGATE_TTL_MS: u64 = 30_000;
/// Provenance note for an OpenCode catalogue read: the picker and the
/// configure gate must never disagree, so the note always names which
/// session-true source produced the list.
fn opencode_catalog_note(provenance: &str) -> &'static str {
    match provenance {
        "session" => {
            "OpenCode catalogue from the live session GET /api/model; the picker lists exactly what configure admits for this Agent generation"
        }
        "preview" => {
            "OpenCode launch preview using the profile's provider settings and auth; workspace and live provider state can change availability"
        }
        _ => {
            "OpenCode preview from the canonical native stores; an isolated session may accept a different set"
        }
    }
}

/// Muse catalogue note: the native `isDefault` marker names a marked row
/// for display. An omitted `--model` always defers to the server default at
/// launch (MSP `session/start.modelId`: "server default when omitted"),
/// whether or not the catalogue marks a row.
fn muse_catalog_note(default: Option<&str>) -> String {
    match default {
        Some(marked) if !marked.trim().is_empty() => format!(
            "Muse model/list marks a row with the native isDefault marker ({marked}); an omitted --model still defers to the server default at launch"
        ),
        _ => "Muse model/list names no default marker; the server default applies at launch"
            .to_owned(),
    }
}

/// Universal external-route quota guard: a frozen profile that selects an
/// external provider route must never report CLI/keyring quota measured for
/// another route. Claude's markers are the core list shared with the Claude
/// reader; the daemon refuses before spawning any reader, so routing and
/// `harness.quota` share one decision. The one native fact it cannot see
/// stays in the plugin: user settings that select an external route (Claude
/// `settings.json`) are still refused by the Claude reader itself.
///
/// A Codex profile on an adopted proxy (`route_binding`) is such a route:
/// Codex's `account/rateLimits/read` reports the signed-in ChatGPT account
/// whatever provider the session uses, so its limits never describe the
/// proxy route (a free proxied model was refused as exhausted while the
/// ChatGPT weekly window was full).
pub(crate) fn external_quota_route(
    harness: &str,
    profile: &Value,
    environment: &Value,
) -> Option<&'static str> {
    // Claude matches its reader's lookup: exact on non-Windows, ASCII
    // case-insensitive on Windows.
    let present = |name: &str| {
        environment
            .as_object()
            .into_iter()
            .flatten()
            .any(|(key, value)| {
                (key == name || (cfg!(windows) && key.eq_ignore_ascii_case(name)))
                    && value.as_str().is_some_and(|text| !text.is_empty())
            })
    };
    match harness {
        "codex"
            if profile
                .get("route_binding")
                .is_some_and(|binding| !binding.is_null()) =>
        {
            Some(
                "Codex profile routes through an adopted proxy; ChatGPT account limits would describe the direct route",
            )
        }
        "claude"
            if CLAUDE_EXTERNAL_ROUTE_MARKERS
                .iter()
                .any(|marker| present(marker)) =>
        {
            Some(
                "Claude profile route is external or ambiguous; CLI OAuth quota would describe an unverified route",
            )
        }
        // Antigravity matches its reader's lookup exactly.
        "antigravity"
            if [
                "GEMINI_API_KEY",
                "GOOGLE_API_KEY",
                "GOOGLE_APPLICATION_CREDENTIALS",
            ]
            .iter()
            .any(|marker| {
                environment
                    .get(*marker)
                    .and_then(Value::as_str)
                    .is_some_and(|text| !text.is_empty())
            }) =>
        {
            Some(
                "Antigravity profile selects external auth; native keyring quota would describe another route",
            )
        }
        _ => None,
    }
}

/// Daemon-owned launch default for an omitted model, decided over the
/// daemon's own cached `harness.models` entry. Codex launches the native
/// `isDefault` id; Muse defers to the server default (the marker is
/// display-only), so an omitted Muse model resolves to no frozen value and
/// only an empty catalogue refuses. Explicit models stay with the plugins,
/// which validate them against their native entries at launch.
pub(crate) fn launch_model_default(
    harness: &str,
    response: &HarnessModelsResponse,
) -> Result<Option<String>> {
    match harness {
        "codex" => match response.default.clone() {
            Some(model) => Ok(Some(model)),
            None => Err(
                eliot_plugin::start_classification::harness_unavailable_model(
                    "codex",
                    "model_list",
                    "Codex model/list reported no default model; use an explicit model",
                ),
            ),
        },
        "muse" => {
            if response.models.is_empty() {
                return Err(Error::new(
                    "profile_error",
                    format!(
                        "Muse model catalogue (source {}, {} models) names no default model; use an explicit model",
                        response.catalogue,
                        response.models.len()
                    ),
                ));
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Outer bound for one `models.catalog` aggregate rebuild across all
/// installed harnesses. Version probes, reader slots and native processes
/// are each bounded, but a full pass over five harnesses still needs an
/// outer deadline so one listing cannot hold the coalescing lock
/// indefinitely. Harness reads past the deadline fail open into the partial
/// aggregate, which the next first-page listing rebuilds.
pub(super) fn quota_read_error(response: &mut HarnessQuotaResponse, error: eliot_core::Error) {
    if error.code == "unsupported" {
        response.status = "unsupported".to_owned();
        response.note = Some(match response.harness.as_str() {
            "muse" => "Installed Muse MSP does not support usage/read and has no retained subscription observation",
            "codex" => "Installed Codex app-server does not support account/rateLimits/read",
            _ => "Installed harness has no supported read-only quota method",
        }.to_owned());
    } else {
        let origin = if error.origin == Some(eliot_core::FailureOrigin::Provider) {
            "provider"
        } else {
            "environment"
        };
        response.details = Some(
            serde_json::json!({"state":"error","origin":origin,"error":"quota read failed; no values available"}),
        );
        response.note = Some(if error.code == "quota_unavailable" {
            eliot_core::bound_chars(&error.message, 512)
        } else {
            "Read-only native/provider quota unavailable; no inference or credential refresh attempted".to_owned()
        });
    }
}

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
    // The frozen account binding selects the native account: a profile edit
    // that renames it must not reuse the previous account's cache entry.
    if let Some(account) = snapshot
        .get("account")
        .and_then(Value::as_str)
        .filter(|name| !name.trim().is_empty())
    {
        let account = format!(" account:{account}");
        if scope.len() + account.len() <= eliot_core::api::MAX_HARNESS_SCOPE_BYTES {
            scope.push_str(&account);
        }
    }
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
            } else if let Some(provider) = snapshot.get("local_provider") {
                if let Some(id) = provider["id"].as_str() {
                    let suffix = format!(" provider:{id}");
                    if scope.len() + suffix.len() <= eliot_core::api::MAX_HARNESS_SCOPE_BYTES {
                        scope.push_str(&suffix);
                    }
                }
                if provider["api_key_env"].is_string() {
                    credential = "local-provider-env".to_owned();
                }
            }
        }
        "claude" => {
            credential = "claude-cli-oauth".to_owned();
        }
        "antigravity" => {
            credential = "antigravity-cli-keyring".to_owned();
        }
        "muse" => {
            credential = "ambient".to_owned();
        }
        _ => {}
    }
    (scope, credential)
}

/// Separate frozen routes using their verified environment identity and
/// the full configured provider endpoint (including path).
pub(crate) fn canonical_cache_identity(
    profile_label: &str,
    snapshot: &Value,
    verified: Option<&str>,
    environment: &Value,
) -> Option<String> {
    let verified = verified?;
    // Effective model pins also affect Claude's inventory. Include only the
    // classified metadata values from the verified reader environment.
    let model_pins = environment
        .as_object()
        .into_iter()
        .flatten()
        .filter(|(name, _)| crate::config::claude_model_selector(snapshot, name))
        .collect::<std::collections::BTreeMap<_, _>>();
    let route = json!({
        "label": profile_label,
        "harness": snapshot.get("harness"),
        "account": snapshot.get("account"),
        "auth": snapshot.get("auth_integration"),
        "local_provider": snapshot.get("local_provider"),
        "route": snapshot.get("route_binding"),
        "executable": snapshot.get("cmd"),
        "identity": verified,
        "model_pins": model_pins,
    });
    let digest = eliot_core::canonical_json_sha256(&route).ok()?;
    Some(digest.iter().map(|byte| format!("{byte:02x}")).collect())
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
    pub(crate) quotas: HashMap<String, ScopedQuota>,
    /// Installed harnesses whose read failed; non-empty makes it partial.
    pub(crate) failures: Vec<ModelsCatalogFailure>,
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
            .filter(|entry| quota_fresh(entry.fetched_at_ms, entry.ttl_ms, now))
            .cloned()
    }

    pub(crate) fn insert_models(&mut self, key: String, entry: HarnessModelsResponse) {
        evict_models(&mut self.models, entry.fetched_at_ms);
        self.models.insert(key, entry);
    }

    pub(crate) fn get_quota(&self, key: &str, now: i64) -> Option<HarnessQuotaResponse> {
        self.quotas
            .get(key)
            .filter(|entry| quota_fresh(entry.fetched_at_ms, entry.ttl_ms, now))
            .cloned()
    }

    /// Insert under the private canonical identity (`key`); the response
    /// keeps its public display `scope`. The two are separate on purpose:
    /// display is for operators, the key is for cache correctness.
    pub(crate) fn insert_quota(&mut self, key: String, entry: HarnessQuotaResponse) {
        evict_quotas(&mut self.quotas, entry.fetched_at_ms);
        self.quotas.insert(key, entry);
    }

    /// The fresh aggregate for `key`. A partial one answers only a
    /// continuation page (`after` set): a first page rebuilds it.
    pub(crate) fn get_aggregate(
        &self,
        key: &str,
        now: i64,
        continuation: bool,
    ) -> Option<CachedAggregate> {
        self.aggregate
            .as_ref()
            .filter(|entry| entry.key == key && entry.fresh(now))
            .filter(|entry| continuation || entry.failures.is_empty())
            .cloned()
            .map(|mut entry| {
                for model in &mut entry.models {
                    if model.availability == eliot_core::api::harness::CATALOG_AVAILABLE {
                        model.availability = availability(
                            Some(false),
                            entry.quotas.get(&model.harness),
                            &model.id,
                            now,
                        )
                        .to_owned();
                    }
                }
                entry
            })
    }

    pub(crate) fn insert_aggregate(&mut self, entry: CachedAggregate) {
        self.aggregate = Some(entry);
    }
}

fn claude_fallback_eligible(error: &Error) -> bool {
    matches!(error.code.as_str(), "unavailable" | "unsupported")
}

fn claude_alias_fallback(scope: String, credential: String) -> HarnessModelsResponse {
    HarnessModelsResponse {
        harness: "claude".to_owned(),
        catalogue: "aliases".to_owned(),
        models: crate::adhoc::claude_documented_models(),
        default: None,
        note: Some(
            "Claude native initialize inventory is unavailable for this scope. Entries are documented aliases and full model ids with unknown availability. The CLI default applies when no model is passed."
                .to_owned(),
        ),
        scope,
        fetched_at_ms: now_ms(),
        ttl_ms: CATALOG_TTL_MS,
        credential,
        configure: eliot_core::api::ConfigureSupport::for_harness("claude"),
    }
}

fn native_models_response(
    harness: &str,
    cmd: &str,
    scope: String,
    credential: String,
    provenance: &str,
    raw: &Value,
) -> Result<HarnessModelsResponse> {
    let models = raw
        .get("models")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let mut entries = Vec::new();
    for model in models {
        let id = model
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if id.trim().is_empty() {
            continue;
        }
        entries.push(eliot_core::api::HarnessModelEntry {
            id,
            display_name: model
                .get("display_name")
                .and_then(Value::as_str)
                .and_then(eliot_core::api::harness::native_model_display_name)
                .map(str::to_owned),
            default: model
                .get("default")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            efforts: model
                .get("efforts")
                .and_then(|efforts| serde_json::from_value(efforts.clone()).ok()),
            default_effort: model
                .get("default_effort")
                .and_then(Value::as_str)
                .map(str::to_owned),
            // Live native reads carry no provenance marker.
            source: None,
        });
    }
    if harness == "claude" && entries.is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Claude model.list returned no usable ids in its projected inventory",
        ));
    }
    let default = raw
        .get("default")
        .and_then(Value::as_str)
        .filter(|default| !default.trim().is_empty())
        .map(str::to_owned);
    let (catalogue, note) = if harness == "muse" && entries.is_empty() {
        // The launcher-marked Muse binary answered `model/list`
        // with an empty catalogue (observed on Muse 1.4.20's
        // empty `bundledCatalog`): no server default exists to
        // apply, so the read reports `empty` honestly instead of
        // the default-marker note. Discovery stays deterministic
        // on the launcher mark (no extra native launch beyond
        // this read); pass `--cmd` (or a profile pinning `cmd`)
        // naming a binary with a live catalogue, e.g. the
        // pinned 1.4.0 binary.
        (
            "empty".to_owned(),
            Some(format!(
                "Muse model/list from {cmd} returned an empty catalogue; ad-hoc launch needs an explicit model, or --cmd naming a binary with a live catalogue"
            )),
        )
    } else if harness == "muse" {
        (
            "native".to_owned(),
            Some(muse_catalog_note(default.as_deref())),
        )
    } else if harness == "opencode" {
        (
            "native".to_owned(),
            Some(opencode_catalog_note(provenance).to_owned()),
        )
    } else if harness == "antigravity" {
        (
            "native".to_owned(),
            Some(
                "Antigravity catalogue from `agy models`; effort-suffixed ids map to thought_level at launch"
                    .to_owned(),
            ),
        )
    } else {
        ("native".to_owned(), None)
    };
    Ok(HarnessModelsResponse {
        harness: harness.to_owned(),
        catalogue,
        models: entries,
        default,
        note,
        scope,
        fetched_at_ms: now_ms(),
        ttl_ms: CATALOG_TTL_MS,
        credential,
        configure: eliot_core::api::ConfigureSupport::for_harness(harness),
    })
}

/// The private native-read key includes executable/version identity without
/// changing the public account scope. A failed version probe cannot reuse
/// a previously cached native read.
pub(crate) fn models_key(scope: &str, cmd: &str, version: Option<&str>) -> Option<String> {
    version.map(|version| format!("{scope}\0{cmd}\0{version}"))
}

/// Build one unified `models.catalog` entry from a harness catalogue
/// entry: the provider derives from the id, the label uses the native display
/// name when available, and availability comes
/// from holds/quota (`unknown` without affirmative evidence), and the entry
/// `source` marker is copied verbatim.
pub(crate) fn catalog_entry(
    harness: &str,
    model: eliot_core::api::HarnessModelEntry,
    held: Option<bool>,
    quota: Option<&ScopedQuota>,
    now: i64,
) -> eliot_core::api::ModelsCatalogEntry {
    use eliot_core::api::catalog_provider;
    eliot_core::api::ModelsCatalogEntry {
        harness: harness.to_owned(),
        id: model.id.clone(),
        display_name: model.display_name().to_owned(),
        provider: catalog_provider(harness, &model.id),
        efforts: model.efforts,
        default_effort: model.default_effort,
        default: model.default,
        availability: availability(held, quota, &model.id, now).to_owned(),
        source: model.source,
        profile: None,
    }
}

/// Build one named-profile preset entry carrying only its frozen model and
/// effort. The picker launches this row with the named profile.
pub(crate) fn profile_catalog_entry(
    harness: &str,
    model: eliot_core::api::HarnessModelEntry,
    profile: &str,
    frozen_effort: Option<&str>,
    now: i64,
) -> eliot_core::api::ModelsCatalogEntry {
    let mut entry = catalog_entry(harness, model, None, None, now);
    entry.efforts = frozen_effort.map(|effort| vec![effort.to_owned()]);
    entry.default_effort = frozen_effort.map(str::to_owned);
    entry.default = false;
    entry.profile = Some(profile.to_owned());
    entry
}

/// The frozen model and effort a named profile launches. Profiles without a
/// configured model have no truthful preset row.
pub(crate) fn profile_frozen_selection(snapshot: &Value) -> Option<(String, Option<String>)> {
    let model = snapshot
        .get("model")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|model| !model.is_empty())
        .map(str::to_owned)?;
    let effort = snapshot
        .get("effort")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|effort| !effort.is_empty())
        .map(str::to_owned);
    Some((model, effort))
}

/// Coalesce named-preview failures into one bounded OpenCode entry naming the
/// affected profiles. Per-profile pushes would exceed the response failure
/// bound (failures count against harnesses, not profiles) and invalidate the
/// whole catalogue. Returns `None` when every named preview succeeded.
pub(crate) fn coalesced_preview_failure(
    failed: &[String],
    incomplete: Option<&str>,
) -> Option<ModelsCatalogFailure> {
    if failed.is_empty() && incomplete.is_none() {
        return None;
    }
    let mut message = String::new();
    if !failed.is_empty() {
        // Bound the name list well under the 512-byte failure cap; the
        // tail always says how many profiles failed.
        let mut names = String::new();
        let mut shown = 0usize;
        for name in failed {
            let trimmed = name.trim();
            if trimmed.is_empty() || trimmed.len() > 256 {
                continue;
            }
            let piece = if names.is_empty() {
                trimmed.to_owned()
            } else {
                format!(", {trimmed}")
            };
            if names.len() + piece.len() > 320 {
                break;
            }
            names.push_str(&piece);
            shown += 1;
        }
        let hidden = failed.len().saturating_sub(shown);
        message.push_str(&format!(
            "OpenCode named-profile preview failed for {} profile(s): {names}",
            failed.len()
        ));
        if hidden > 0 {
            message.push_str(&format!(" (and {hidden} more)"));
        }
    }
    if let Some(incomplete) = incomplete {
        if !message.is_empty() {
            message.push_str("; ");
        }
        message.push_str(incomplete);
    }
    Some(ModelsCatalogFailure::from_error(
        "opencode",
        &Error::new("unavailable", message),
    ))
}

/// Keep one bounded OpenCode failure when both ad-hoc and named previews fail.
pub(crate) fn append_failure_message(failure: &mut ModelsCatalogFailure, suffix: &str) {
    if suffix.trim().is_empty() {
        return;
    }
    let separator = if failure.message.trim().is_empty() {
        ""
    } else {
        "; "
    };
    let combined = format!("{suffix}{separator}{}", failure.message);
    let mut end = combined
        .len()
        .min(eliot_core::api::harness::MAX_CATALOG_FAILURE_MESSAGE_BYTES);
    while !combined.is_char_boundary(end) {
        end -= 1;
    }
    failure.message = combined[..end].to_owned();
}

/// Catalogue membership establishes identity, not account availability.
/// Only complete hold readback plus affirmative, fresh native quota signals
/// can establish availability. Unsupported and missing signals stay unknown.
/// Normalized provider quota projection: one typed, scoped observation
/// per read, consumed identically by routing (skip exhausted routes) and the
/// catalogue (availability needs affirmative headroom on every recognized
/// window). Callers consume only these typed fields: they never reparse raw
/// quota JSON into booleans.
///
/// - `provider`: the harness whose native schema was consumed.
/// - `scope`: the quota scope the observation covers (the response scope).
/// - `observed_at_ms`: the native observation time.
/// - `exhausted`: a recognized window is full with a future (or unknown)
///   reset; for Codex only the primary bucket's explicit limit state settles
///   exhaustion (`spendControlReached`, a non-null `rateLimitReachedType`, a
///   depleted `individualLimit`, or a full primary window without credits).
///   A full secondary window or a metered `rateLimitsByLimitId` bucket never
///   exhausts a route.
/// - `available`: every recognized window for this model shows headroom
///   (used below 100% with a future reset; Codex additionally requires
///   `ordinaryUsageAllowed` and per-model headroom when the provider scopes
///   them). Account quota alone never invents per-model entitlement: a
///   model-scoped window only constrains matching models.
/// - `mapping`: how the decisive bucket maps to the requested model (an
///   exact bucket id, a documented provider group, or unmapped).
/// - `provenance`: which provider shape was consumed (`muse:usage`,
///   `muse:windows`, `muse:window/weekly`, `codex:rateLimits`, `windows`).
///
/// Missing or unsupported signals yield `None`. Windows that exist but map
/// to no model yield an inert projection (`mapping: Unmapped`, neither
/// exhausted nor available). Either way callers keep `unknown` and never
/// skip a route or assert availability from them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum QuotaMapping {
    /// A bucket keyed by the exact requested model id.
    ExactId,
    /// A documented provider group window applying to this model
    /// (`account` for unlabeled account-scope windows).
    Group(&'static str),
    /// No documented mapping: the window constrains no model.
    Unmapped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct QuotaProjection {
    pub provider: String,
    pub scope: String,
    pub observed_at_ms: i64,
    pub exhausted: bool,
    pub available: bool,
    pub mapping: QuotaMapping,
    pub provenance: &'static str,
}

/// Typed quota observation: the provider whose native schema was consumed,
/// the quota scope it covers, and its native observation time. It travels
/// from the typed `HarnessQuotaResponse` into the projection untouched, so
/// neither the projectors nor their callers reparse raw quota JSON for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct QuotaObservation<'a> {
    pub provider: &'a str,
    pub scope: &'a str,
    pub observed_at_ms: i64,
}

/// Normalize provider-specific quota `details` into one projection.
/// A model scopes named windows. Without one only account windows apply.
/// Returns `None` when no signal maps to this observation's provider
/// (missing or unsupported shapes stay `unknown` at the caller).
pub(crate) fn project_quota(
    observation: QuotaObservation<'_>,
    details: &Value,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    if !details.is_object() {
        return None;
    }
    match observation.provider {
        "muse" => {
            let (windows, complete, provenance) = muse_window_set(details)?;
            windows_projection(observation, &windows, complete, model, now, provenance)
        }
        "codex" => codex_projection(observation, details, model, now),
        "opencode" => opencode_projection(observation, details, model, now),
        "claude" | "antigravity" => {
            let windows = details
                .get("windows")
                .and_then(Value::as_array)
                .filter(|windows| !windows.is_empty())?;
            let windows: Vec<&Value> = windows.iter().collect();
            windows_projection(observation, &windows, true, model, now, "windows")
        }
        _ => None,
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ScopedQuota {
    pub(crate) response: HarnessQuotaResponse,
    pub(crate) route_verified: bool,
}

/// One typed entry point for routing and the catalogue: a `supported`
/// response observed inside its freshness TTL projects, anything else
/// (missing, stale, unsupported) stays `unknown`. Both consumers share this
/// function, so a stale observation can neither skip a route nor mark a
/// model available.
pub(crate) fn projected_quota(
    quota: &ScopedQuota,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    if !quota.route_verified {
        return None;
    }
    let quota = &quota.response;
    if quota.status != eliot_core::api::HARNESS_QUOTA_SUPPORTED {
        return None;
    }
    if !quota_fresh(quota.fetched_at_ms, quota.ttl_ms, now) {
        return None;
    }
    let details = quota.details.as_ref()?;
    project_quota(
        QuotaObservation {
            provider: quota.harness.as_str(),
            scope: quota.scope.as_str(),
            observed_at_ms: quota.fetched_at_ms,
        },
        details,
        model,
        now,
    )
}

/// Muse contributes three shapes over time: retained `windows[]`
/// observations, live `usage{window,weekly}` reads, and the older top-level
/// `window`/`weekly` pair. The legacy pair is complete only when both halves
/// are present; a lone half is not headroom.
fn muse_window_set(details: &Value) -> Option<(Vec<&Value>, bool, &'static str)> {
    if let Some(windows) = details
        .get("windows")
        .and_then(Value::as_array)
        .filter(|windows| !windows.is_empty())
    {
        return Some((windows.iter().collect(), true, "muse:windows"));
    }
    if let Some(usage) = details.get("usage").and_then(Value::as_object) {
        let mut windows = Vec::new();
        for key in ["window", "weekly"] {
            if let Some(window) = usage.get(key).filter(|window| window.is_object()) {
                windows.push(window);
            }
        }
        if windows.is_empty() {
            return None;
        }
        let complete = windows.len() == 2;
        return Some((windows, complete, "muse:usage"));
    }
    let mut windows = Vec::new();
    for key in ["window", "weekly"] {
        if let Some(window) = details.get(key).filter(|window| window.is_object()) {
            windows.push(window);
        }
    }
    if windows.is_empty() {
        return None;
    }
    let complete = windows.len() == 2;
    Some((windows, complete, "muse:window/weekly"))
}

fn window_used_percent(window: &Value) -> Option<f64> {
    window
        .get("usedPercent")
        .and_then(Value::as_f64)
        .filter(|used| used.is_finite() && *used >= 0.0)
}

fn window_reset_ms(window: &Value) -> Option<i64> {
    window
        .get("resetsAtMs")
        .and_then(Value::as_i64)
        .filter(|reset| *reset >= 0)
        .or_else(|| {
            window
                .get("resetsAt")
                .and_then(Value::as_i64)
                .filter(|reset| *reset >= 0)
                .and_then(|reset| reset.checked_mul(1000))
        })
}

/// Full with a future (or unknown) reset: without reset evidence the window
/// may already have rolled, so callers treat it as exhausted only when the
/// provider gives no earlier reset.
fn window_exhausted(window: &Value, now: i64) -> bool {
    window_used_percent(window).is_some_and(|used| used >= 100.0)
        && window_reset_ms(window).is_none_or(|reset| reset > now)
}

/// Affirmative headroom: below 100% with a future reset. The future reset
/// is required: a full or reset-less window is never availability evidence.
fn window_headroom(window: &Value, now: i64) -> bool {
    window_used_percent(window).is_some_and(|used| used < 100.0)
        && window_reset_ms(window).is_some_and(|reset| reset > now)
}

/// Documented provider quota groups: the window-label family plus the model-id
/// tokens that prove membership. Group names come from the readers that emit
/// the labels (Antigravity `retrieveUserQuotaSummary` display names, Claude
/// CLI OAuth usage keys); a label whose family is not listed here is
/// unrecognized and maps to no model. Matching is on whole id tokens, never
/// substrings: the `Gemini 4 Argon` group requires an `argon` token, so an
/// exhausted Argon bucket never constrains a Hydrogen model even though both
/// contain `gemini`.
const WINDOW_MODEL_GROUPS: &[(&str, &[&str])] = &[
    ("gemini 4 argon", &["argon"]),
    ("claude/gpt", &["claude", "gpt"]),
    ("gemini", &["gemini"]),
    ("opus", &["opus"]),
    ("sonnet", &["sonnet"]),
    ("fable", &["fable"]),
    ("haiku", &["haiku"]),
];

/// Provider window-label periods emitted by the readers (`5-hour`, `Weekly`,
/// `Monthly`, Antigravity `quota`). A label that is exactly a period, or
/// carries no label at all, is account scope.
const WINDOW_LABEL_PERIODS: &[&str] = &["weekly", "5-hour", "monthly", "quota"];

fn documented_group(family: &str) -> Option<(&'static str, &'static [&'static str])> {
    WINDOW_MODEL_GROUPS
        .iter()
        .find(|(name, _)| *name == family)
        .map(|(name, tokens)| (*name, *tokens))
}

/// The provider group named by a window label, or `None` for account-scope
/// (bare-period) labels. An unrecognized family yields `Some` of itself so
/// the caller can report it unmapped.
fn window_family(label: &str) -> Option<String> {
    let lower = label.trim().to_ascii_lowercase();
    if lower.is_empty() {
        return None;
    }
    for period in WINDOW_LABEL_PERIODS {
        if lower == *period {
            return None;
        }
        if let Some(family) = lower.strip_suffix(period) {
            let family = family.trim().to_owned();
            if !family.is_empty() {
                return Some(family);
            }
        }
    }
    Some(lower)
}

fn model_token_present(model: &str, token: &str) -> bool {
    model
        .split(|c: char| !c.is_ascii_alphanumeric())
        .any(|part| part.eq_ignore_ascii_case(token))
}

/// How one window maps to the requested model: an exact label match, a
/// documented provider group the model belongs to, the unlabeled account
/// scope, or unmapped (an unrecognized provider label, or a recognized group
/// the model is not a member of). An unrecognized label applies to no model,
/// even on an unscoped read.
fn window_mapping(window: &Value, model: Option<&str>) -> QuotaMapping {
    let label = window.get("label").and_then(Value::as_str).unwrap_or("");
    if let Some(model) = model.filter(|model| !model.trim().is_empty())
        && label.eq_ignore_ascii_case(model.trim())
    {
        return QuotaMapping::ExactId;
    }
    let family = window_family(label);
    let Some(family) = family.as_deref() else {
        return QuotaMapping::Group("account");
    };
    let Some((group, tokens)) = documented_group(family) else {
        return QuotaMapping::Unmapped;
    };
    match model {
        Some(model) if tokens.iter().any(|token| model_token_present(model, token)) => {
            QuotaMapping::Group(group)
        }
        _ => QuotaMapping::Unmapped,
    }
}

fn windows_projection(
    observation: QuotaObservation<'_>,
    windows: &[&Value],
    complete: bool,
    model: Option<&str>,
    now: i64,
    provenance: &'static str,
) -> Option<QuotaProjection> {
    let applicable: Vec<(&Value, QuotaMapping)> = windows
        .iter()
        .map(|&window| (window, window_mapping(window, model)))
        .filter(|(_, mapping)| *mapping != QuotaMapping::Unmapped)
        .collect();
    summarize_windows(observation, &applicable, complete, now, provenance)
}

fn summarize_windows(
    observation: QuotaObservation<'_>,
    applicable: &[(&Value, QuotaMapping)],
    complete: bool,
    now: i64,
    provenance: &'static str,
) -> Option<QuotaProjection> {
    if applicable.is_empty() {
        // Windows exist but none maps to this model (for example an
        // unrecognized `Model group` label, or an Argon bucket read for a
        // Hydrogen model): report the inert unmapped projection so callers
        // stay unknown instead of skipping a route or asserting availability.
        return Some(QuotaProjection {
            provider: observation.provider.to_owned(),
            scope: observation.scope.to_owned(),
            observed_at_ms: observation.observed_at_ms,
            exhausted: false,
            available: false,
            mapping: QuotaMapping::Unmapped,
            provenance,
        });
    }
    let exhausted = applicable
        .iter()
        .any(|(window, _)| window_exhausted(window, now));
    // The decisive bucket drives the verdict: the first exhausted window
    // when anything is exhausted (preferring an exact id), else the exact
    // id when one matched, else the first applicable group window.
    let mapping = applicable
        .iter()
        .filter(|(window, _)| window_exhausted(window, now) == exhausted)
        .map(|(_, mapping)| *mapping)
        .find(|mapping| *mapping == QuotaMapping::ExactId)
        .or_else(|| {
            applicable
                .iter()
                .filter(|(window, _)| window_exhausted(window, now) == exhausted)
                .map(|(_, mapping)| *mapping)
                .next()
        })
        .unwrap_or(applicable[0].1);
    Some(QuotaProjection {
        provider: observation.provider.to_owned(),
        scope: observation.scope.to_owned(),
        observed_at_ms: observation.observed_at_ms,
        exhausted,
        available: complete
            && applicable
                .iter()
                .all(|(window, _)| window_headroom(window, now)),
        mapping,
        provenance,
    })
}

fn opencode_projection(
    observation: QuotaObservation<'_>,
    details: &Value,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    let model = model?;
    let (provider, _) = eliot_plugin::session::split_provider_model(model)?;
    match (details.get("integration").and_then(Value::as_str), provider) {
        (Some("opencode-go"), "opencode-go") => {
            let windows: Vec<_> = details.get("windows")?.as_array()?.iter().collect();
            windows_projection(observation, &windows, true, Some(model), now, "opencode:go")
        }
        (Some("opencode"), _) => {
            let applicable: Vec<_> = details
                .get("consoleWindows")?
                .as_array()?
                .iter()
                .filter_map(|window| {
                    if !matches!(window.get("scope")?.as_str()?, "each" | "combined") {
                        return None;
                    }
                    let mapping = match window.get("coverage")?.as_str()? {
                        "all" if provider == "opencode" => QuotaMapping::Group("console"),
                        "models"
                            if window
                                .get("models")?
                                .as_array()?
                                .iter()
                                .any(|id| id.as_str() == Some(model)) =>
                        {
                            QuotaMapping::ExactId
                        }
                        _ => return None,
                    };
                    Some((window, mapping))
                })
                .collect();
            summarize_windows(observation, &applicable, true, now, "opencode:console")
        }
        _ => None,
    }
}

fn codex_projection(
    observation: QuotaObservation<'_>,
    details: &Value,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    let rates = details
        .get("rateLimits")
        .filter(|rates| rates.is_object())?;
    // Only the primary bucket's explicit limit state settles exhaustion: the
    // deny signals, or a full primary window without credits. A full secondary
    // window or a metered `rateLimitsByLimitId` bucket never proves the
    // request's own quota is spent, so neither skips the route.
    let mut exhausted = rates.get("spendControlReached").and_then(Value::as_bool) == Some(true)
        || rates
            .get("rateLimitReachedType")
            .is_some_and(|reached| !reached.is_null())
        || rates
            .get("individualLimit")
            .and_then(|limit| limit.get("remainingPercent"))
            .and_then(Value::as_i64)
            .is_some_and(|remaining| remaining <= 0);
    let credits = &rates["credits"];
    let credit_available = credits["unlimited"].as_bool() == Some(true)
        || credits["hasCredits"].as_bool() == Some(true);
    exhausted |= !credit_available && window_exhausted(&rates["primary"], now);
    let available = !exhausted
        && details["ordinaryUsageAllowed"].as_bool() == Some(true)
        && window_headroom(&rates["primary"], now)
        && window_headroom(&rates["secondary"], now)
        && match details.get("rateLimitsByLimitId") {
            None | Some(Value::Null) => true,
            Some(Value::Object(limits)) if limits.is_empty() => true,
            Some(Value::Object(limits)) => model.is_some_and(|model| {
                limits.get(model).is_some_and(|rates| {
                    window_headroom(&rates["primary"], now)
                        && window_headroom(&rates["secondary"], now)
                })
            }),
            _ => false,
        };
    // Only the primary bucket can establish exhaustion. Per-model buckets
    // can establish the mapping for affirmative headroom.
    let mapping = match model {
        Some(model)
            if !exhausted
                && details
                    .get("rateLimitsByLimitId")
                    .and_then(|limits| limits.get(model))
                    .is_some() =>
        {
            QuotaMapping::ExactId
        }
        _ => QuotaMapping::Group("primary"),
    };
    Some(QuotaProjection {
        provider: observation.provider.to_owned(),
        scope: observation.scope.to_owned(),
        observed_at_ms: observation.observed_at_ms,
        exhausted,
        available,
        mapping,
        provenance: "codex:rateLimits",
    })
}

/// Catalogue membership establishes identity, not account availability.
/// Only complete hold readback plus affirmative, fresh native quota signals
/// can establish availability. Unsupported and missing signals stay unknown.
pub(crate) fn availability(
    held: Option<bool>,
    quota: Option<&ScopedQuota>,
    model: &str,
    now: i64,
) -> &'static str {
    use eliot_core::api::harness::{CATALOG_AVAILABLE, CATALOG_HELD, CATALOG_UNKNOWN};
    if held == Some(true) {
        return CATALOG_HELD;
    }
    let Some(quota) = quota.filter(|_| held == Some(false)) else {
        return CATALOG_UNKNOWN;
    };
    // One normalized projection serves every harness: affirmative headroom
    // on every recognized window establishes availability, anything else
    // stays unknown. Membership alone never proves availability. Freshness
    // shares the routing entry point below, so a stale observation can
    // neither skip a route nor mark a model available.
    if projected_quota(quota, Some(model), now).is_some_and(|projection| projection.available) {
        CATALOG_AVAILABLE
    } else {
        CATALOG_UNKNOWN
    }
}

/// Shared freshness gate for routing and the catalogue: an observation past
/// its TTL is rejected by both through [`projected_quota`].
pub(crate) fn quota_fresh(fetched_at_ms: i64, ttl_ms: u64, now: i64) -> bool {
    fetched_at_ms >= 0 && now >= 0 && (now.saturating_sub(fetched_at_ms) as u64) <= ttl_ms
}

fn evict_models(map: &mut HashMap<String, HarnessModelsResponse>, now: i64) {
    if map.len() < MAX_CACHED_SCOPES {
        return;
    }
    map.retain(|_, entry| quota_fresh(entry.fetched_at_ms, entry.ttl_ms, now));
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
    map.retain(|_, entry| quota_fresh(entry.fetched_at_ms, entry.ttl_ms, now));
    if map.len() >= MAX_CACHED_SCOPES
        && let Some(key) = map.keys().next().cloned()
    {
        map.remove(&key);
    }
}
