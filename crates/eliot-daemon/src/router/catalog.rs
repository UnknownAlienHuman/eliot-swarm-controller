//! Bounded cached catalogue/quota scope derivation for `harness.models`
//! and `harness.quota`.
//!
//! Live cache reuse requires a frozen route verified against the reader's
//! private process environment. Public scope labels contain only route names.

use super::{MODE_OPEN, Principal, Router, catalog, now_ms, require_owner};
use eliot_core::api::harness::NativeRoute;
use eliot_core::api::{
    HARNESS_READ_TTL_MS, HarnessCandidate, HarnessCapabilities, HarnessEntry, HarnessListResponse,
    HarnessModelsRequest, HarnessModelsResponse, HarnessQuotaRequest, HarnessQuotaResponse,
    ModelsCatalogEntry, ModelsCatalogFailure, ModelsCatalogRequest, ModelsCatalogResponse,
    SUPPORTED_HARNESSES,
};
use eliot_core::{Error, Result};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::{
    sync::{Arc, atomic::Ordering},
    time::Instant,
};

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
        let response =
            Self::harness_inventory(Instant::now() + crate::adhoc::DISCOVERY_BUDGET).await?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness list could not be encoded"))
    }

    async fn harness_inventory(deadline: Instant) -> Result<HarnessListResponse> {
        // Filesystem presence plus bounded `--version` probes is blocking
        // work: run it on the blocking pool so slow native CLIs never stall
        // async command/event service. Ranked candidates are enumerated once
        // and reused for both the entry and candidate list.
        let harnesses: Vec<HarnessEntry> = crate::adhoc::blocking_discovery(deadline, move || {
            let mut harnesses = Vec::new();
            for harness in SUPPORTED_HARNESSES {
                let ranked = crate::adhoc::has_ranked_install_candidates(harness);
                let ranked_candidates = if ranked {
                    crate::adhoc::discover_ranked_candidates(harness, deadline)
                } else {
                    Vec::new()
                };
                let entry = if ranked {
                    crate::adhoc::ranked_installation(harness, &ranked_candidates)
                } else {
                    crate::adhoc::harness_installation(harness, deadline)
                };
                let candidates = (!ranked_candidates.is_empty()).then_some(ranked_candidates);
                harnesses.push(HarnessEntry {
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
        Ok(HarnessListResponse { harnesses })
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
            let candidate = crate::adhoc::resolve_profile_command_until(
                name,
                harness,
                snapshot.get("cmd").and_then(Value::as_str),
                deadline,
            )
            .await?;
            crate::adhoc::write_profile_command_snapshot(&mut snapshot, candidate)?;
            return Ok((Some(snapshot), name.to_owned(), None));
        }
        let (cmd, discovered) = if !quota && crate::adhoc::has_ranked_install_candidates(harness) {
            match crate::adhoc::ranked_reader_executable(harness, deadline).await {
                Ok(candidate) => (Some(candidate.cmd.clone()), Some(candidate)),
                Err(error)
                    if HarnessCapabilities::of(harness).documented_models.is_some()
                        && catalog::documented_fallback_eligible(&error) =>
                {
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
        if quota
            && let Some(integration) = HarnessCapabilities::of(harness)
                .quota_readback
                .and_then(|readback| readback.adhoc_integration)
        {
            // Named launch routes bind their own integration.
            snapshot["auth_integration"] = Value::from(integration);
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
        let deadline = Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
        require_owner(principal)?;
        let request: HarnessModelsRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        let response = self.harness_models_until(request, deadline).await?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness models could not be encoded"))
    }

    async fn harness_models_until(
        &self,
        request: HarnessModelsRequest,
        deadline: Instant,
    ) -> Result<HarnessModelsResponse> {
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
        let response = if let Some((agent, generation)) = live_identity.filter(|_| {
            HarnessCapabilities::of(&harness)
                .manifest_extra_verbs
                .contains(&"models")
        }) {
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
        Ok(response)
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
        if HarnessCapabilities::of(harness).models_preview_auth_integrations
            && label == "adhoc"
            && snapshot.as_ref().is_some_and(|profile| {
                profile.get("auth_integration").is_none()
                    && !profile
                        .get("local_provider")
                        .is_some_and(|provider| !provider.is_null())
            })
        {
            return self
                .adhoc_models_by_integration(harness, snapshot, discovered, workspace, deadline)
                .await;
        }
        let (scope, credential) = catalog::describe_snapshot(label, harness, snapshot.as_ref());
        let documented_models = HarnessCapabilities::of(harness).documented_models;
        let read = async {
            let mut profile = snapshot.ok_or_else(|| {
                Error::new(
                    "unavailable",
                    "native model catalogue executable was not discovered",
                )
            })?;
            let route = crate::router::plugin_host::metadata_route(&profile)?;
            if let Some(documented) = documented_models
                && route.identity.is_none()
            {
                return Err(Error::new("unavailable", documented.unverified_route));
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
            Err(error) => {
                if let Some(documented) =
                    documented_models.filter(|_| catalog::documented_fallback_eligible(&error))
                {
                    Ok(catalog::alias_fallback(
                        harness, documented, scope, credential,
                    ))
                } else {
                    Err(error)
                }
            }
        }
    }

    /// A general OpenCode catalogue has no single launch model from which to
    /// select one integration. Preview each canonical integration in its own
    /// launch-shaped isolated store and union only the models the standalone
    /// API reports for that store.
    async fn adhoc_models_by_integration(
        &self,
        harness: &str,
        snapshot: Option<Value>,
        discovered: Option<HarnessCandidate>,
        workspace: Option<String>,
        deadline: Instant,
    ) -> Result<HarnessModelsResponse> {
        let snapshot = snapshot.ok_or_else(|| {
            Error::new(
                "unavailable",
                "OpenCode model catalogue executable was not discovered",
            )
        })?;
        let program = snapshot["cmd"].as_str().ok_or_else(|| {
            Error::new(
                "unavailable",
                "OpenCode model catalogue executable was not discovered",
            )
        })?;
        let program = program.to_owned();
        let integrations = crate::adhoc::blocking_discovery(deadline, move || {
            crate::adhoc::opencode_auth_ids(&program)
        })
        .await?
        .ok_or_else(|| {
            Error::new(
                "unavailable",
                "OpenCode auth integrations could not be read for an isolated model preview",
            )
        })?;
        if integrations.is_empty() {
            return Err(Error::new(
                "unavailable",
                "OpenCode has no auth integration for an isolated model preview",
            ));
        }

        let mut combined: Option<HarnessModelsResponse> = None;
        let mut models = Vec::new();
        let mut seen = HashSet::new();
        for integration in integrations {
            if Instant::now() >= deadline {
                break;
            }
            let integration = crate::adhoc::validate_adhoc_auth(harness, &integration)?;
            let mut selected = snapshot.clone();
            selected["auth_integration"] = json!(integration);
            let response = Box::pin(self.models_for_snapshot(
                harness,
                Some(selected),
                "adhoc",
                discovered.clone(),
                workspace.clone(),
                deadline,
            ))
            .await?;
            if combined.is_none() {
                let mut first = response.clone();
                first.models.clear();
                combined = Some(first);
            }
            for model in response.models {
                if seen.insert(model.id.clone()) {
                    models.push(model);
                }
            }
        }
        let mut response = combined.ok_or_else(|| {
            Error::new(
                "unavailable",
                "OpenCode auth integrations produced no isolated model preview",
            )
        })?;
        response.models = models;
        response.default = None;
        response.scope = format!("profile:adhoc harness:{harness} auth:available");
        response.credential = "auth:available".to_owned();
        response.note = Some("OpenCode launch previews from isolated stores, one integration per read. Use a named profile to see one exact auth selection.".to_owned());
        response.fetched_at_ms = now_ms();
        response.validate()?;
        Ok(response)
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
        let deadline = Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
        require_owner(principal)?;
        let request: HarnessQuotaRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        let (harness, snapshot, label) = self.resolve_quota_target(request, deadline).await?;
        let evidence = self
            .quota_for_snapshot(harness, snapshot.clone(), label.clone(), deadline)
            .await?;
        let now = now_ms();
        let reserve_readback = snapshot.as_ref().map(|snapshot| {
            let quota = super::routing::route_quota(Some(&evidence), snapshot, now);
            eliot_core::api::harness::QuotaReserveReadback {
                status: quota.status,
                unmapped: quota.unmapped,
                hold_unknown: crate::config::profile_quota_unknown_policy(snapshot)
                    == crate::config::QuotaUnknownPolicy::Hold,
            }
        });
        let mut response = evidence.response;
        response.reserve_readback = reserve_readback;
        if let Some(snapshot) = snapshot {
            response.effective_reserve_percent =
                crate::config::profile_quota_reserve_percent(&snapshot);
            if evidence.route_verified && quota_fresh(response.fetched_at_ms, response.ttl_ms, now)
            {
                let windows = quota_usage_windows(&response, now);
                if !windows.is_empty() {
                    let profile = if label == "adhoc" {
                        crate::adhoc::adhoc_profile_name(&response.harness)
                    } else {
                        label
                    };
                    response.observed_usage = self.inner.store.lock().await.summarize_quota_usage(
                        &response.harness,
                        &profile,
                        &snapshot,
                        &windows,
                    )?;
                }
            }
        }
        response.validate()?;
        serde_json::to_value(response).map_err(|_| Error::new("internal", "quota encoding failed"))
    }

    async fn harness_quota_until(
        &self,
        request: HarnessQuotaRequest,
        deadline: Instant,
    ) -> Result<catalog::ScopedQuota> {
        let (harness, snapshot, label) = self.resolve_quota_target(request, deadline).await?;
        self.quota_for_snapshot(harness, snapshot, label, deadline)
            .await
    }

    async fn resolve_quota_target(
        &self,
        request: HarnessQuotaRequest,
        deadline: Instant,
    ) -> Result<(String, Option<Value>, String)> {
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
        Ok((harness, snapshot, label))
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
        let route = crate::router::plugin_host::metadata_route(&profile)?;
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
                effective_reserve_percent: 0,
                reserve_readback: None,
                observed_usage: Vec::new(),
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
        if HarnessCapabilities::of(&harness).reader_executable_discovery {
            let candidate = crate::adhoc::resolve_profile_command_until(
                &label,
                &harness,
                profile["cmd"].as_str(),
                deadline,
            )
            .await?;
            crate::adhoc::write_profile_command_snapshot(&mut profile, candidate)?;
        }
        let cache_key = catalog::canonical_cache_identity(
            &label,
            &profile,
            route.identity.as_deref(),
            &route.environment,
        );
        let route_verified = cache_key.is_some();
        if eliot_core::api::HarnessCapabilities::of(&harness).reads_retained_quota
            && route_verified
            && let Some(observation) = self
                .inner
                .store
                .lock()
                .await
                .latest_quota_observation(&harness, &profile)?
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
                    effective_reserve_percent: 0,
                    reserve_readback: None,
                    observed_usage: Vec::new(),
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
            effective_reserve_percent: 0,
            reserve_readback: None,
            observed_usage: Vec::new(),
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
                    response.note = Some(
                        HarnessCapabilities::of(&harness)
                            .quota_readback
                            .map(|readback| (readback.description)(details))
                            .unwrap_or("Read-only native/provider quota values")
                            .to_owned(),
                    );
                    if HarnessCapabilities::of(&harness).reads_retained_quota {
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

    /// Page the installed harness catalogues for the current settings revision.
    pub(super) async fn models_catalog(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        let deadline = Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
        require_owner(principal)?;
        let request: ModelsCatalogRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        let response = self.models_catalog_until(&request, deadline).await?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "unified catalogue could not be encoded"))
    }

    async fn models_catalog_until(
        &self,
        request: &ModelsCatalogRequest,
        deadline: Instant,
    ) -> Result<ModelsCatalogResponse> {
        request.validate()?;
        if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
            return Err(Error::new(
                "unavailable",
                "daemon is shutting down and rejects new discovery",
            ));
        }
        let settings_revision = self.catalog_settings_revision();
        if let Some(snapshot) = self.cached_catalog(request, settings_revision.as_deref()) {
            return self.catalog_page(request, &snapshot, deadline).await;
        }
        let snapshot = {
            let _coalesce =
                tokio::time::timeout_at(deadline.into(), self.inner.catalog_aggregate_lock.lock())
                    .await
                    .map_err(|_| crate::adhoc::discovery_timeout())?;
            let settings_revision = self.catalog_settings_revision();
            match self.cached_catalog(request, settings_revision.as_deref()) {
                Some(snapshot) => snapshot,
                None => {
                    let snapshot = Arc::new(self.build_catalog(settings_revision, deadline).await?);
                    if self.catalog_settings_revision() == snapshot.settings_revision {
                        self.inner.catalog_cache.lock_recover().aggregate = Some(snapshot.clone());
                    }
                    snapshot
                }
            }
        };
        self.catalog_page(request, &snapshot, deadline).await
    }

    fn catalog_settings_revision(&self) -> Option<String> {
        crate::config::settings::current_revision(&self.inner.data_root)
            .ok()
            .flatten()
    }

    fn cached_catalog(
        &self,
        request: &ModelsCatalogRequest,
        settings_revision: Option<&str>,
    ) -> Option<Arc<catalog::CachedAggregate>> {
        self.inner
            .catalog_cache
            .lock_recover()
            .aggregate
            .as_ref()
            .filter(|entry| entry.settings_revision.as_deref() == settings_revision)
            .filter(|entry| quota_fresh(entry.fetched_at_ms, entry.ttl_ms, now_ms()))
            .filter(|entry| request.after.is_some() || entry.failures.is_empty())
            .cloned()
    }

    async fn build_catalog(
        &self,
        settings_revision: Option<String>,
        deadline: Instant,
    ) -> Result<catalog::CachedAggregate> {
        let discovery_deadline = tokio::time::Instant::from_std(deadline);
        let harnesses = Self::harness_inventory(deadline).await?.harnesses;
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
            let models_request = HarnessModelsRequest {
                harness: harness.to_owned(),
                profile: None,
                agent: None,
                workspace: None,
            };
            // A slow read past the request deadline fails open into the
            // partial aggregate instead of holding the coalescing lock.
            let models_read = tokio::time::timeout_at(
                discovery_deadline,
                self.harness_models_until(models_request, deadline),
            )
            .await;
            let ad_hoc_response = match models_read {
                Ok(Ok(response)) => Some(response),
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
            if let Some(response) = ad_hoc_response {
                if response.credential != "auth:available"
                    && let Ok(Ok(quota)) = tokio::time::timeout_at(
                        discovery_deadline,
                        self.harness_quota_until(
                            HarnessQuotaRequest {
                                harness: harness.to_owned(),
                                profile: None,
                            },
                            deadline,
                        ),
                    )
                    .await
                    && quota.route_verified
                {
                    quotas.insert(harness.to_owned(), quota);
                }
                models.extend(
                    response
                        .models
                        .into_iter()
                        .map(|model| catalog::catalog_entry(harness, model)),
                );
            }
            // Profile models are independent preset rows. They remain visible
            // when ad-hoc discovery returns the same id, and never inherit the
            // ad-hoc hold or quota.
            if HarnessCapabilities::of(harness).models_preview_auth_integrations {
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
                    let scoped = HarnessModelsRequest {
                        harness: harness.to_owned(),
                        profile: Some(name.clone()),
                        agent: None,
                        workspace: None,
                    };
                    let scoped_read = tokio::time::timeout_at(
                        discovery_deadline,
                        self.harness_models_until(scoped, deadline),
                    )
                    .await;
                    let scoped_response = match scoped_read {
                        Ok(Ok(response)) => response,
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
                        ));
                    }
                }
                if let Some(entry) = catalog::coalesced_preview_failure(
                    &preview_failed,
                    preview_incomplete.as_deref(),
                ) {
                    if let Some(existing) = failures
                        .iter_mut()
                        .find(|failure| failure.harness == harness)
                    {
                        catalog::append_failure_message(existing, &entry.message);
                    } else {
                        failures.push(entry);
                    }
                }
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
        Ok(catalog::CachedAggregate {
            settings_revision,
            harnesses,
            models,
            quotas,
            failures,
            fetched_at_ms,
            ttl_ms,
        })
    }

    /// Page the retained metadata and project availability from committed holds
    /// and fresh quota evidence.
    async fn catalog_page(
        &self,
        request: &ModelsCatalogRequest,
        snapshot: &catalog::CachedAggregate,
        deadline: Instant,
    ) -> Result<ModelsCatalogResponse> {
        use eliot_core::api::catalog_matches;

        let query = request.query.as_deref().unwrap_or("");
        let limit = request.effective_limit();
        let mut matched = snapshot
            .models
            .iter()
            .filter(|entry| catalog_matches(entry, query));
        if let Some(cursor) = request.after.as_deref()
            && !matched.any(|entry| entry.cursor() == cursor)
        {
            return Err(Error::new(
                "invalid",
                "catalogue cursor is no longer in the listing; restart from the first page",
            ));
        }
        let mut models: Vec<_> = matched.by_ref().take(limit).cloned().collect();
        let next = if matched.next().is_some() {
            models.last().map(ModelsCatalogEntry::cursor)
        } else {
            None
        };
        let scopes: HashMap<_, _> = models
            .iter()
            .filter(|model| model.profile.is_none())
            .map(|model| {
                let profile = crate::adhoc::adhoc_profile_name(&model.harness);
                (model.harness.clone(), format!("profile:{profile}"))
            })
            .collect();
        if !scopes.is_empty() {
            let exact_scopes: Vec<_> = scopes.values().cloned().collect();
            let holds = tokio::time::timeout_at(deadline.into(), self.inner.store.lock())
                .await
                .ok()
                .and_then(|store| store.provider_holds_for_scopes(&exact_scopes).ok());
            let now = now_ms();
            for model in models.iter_mut().filter(|model| model.profile.is_none()) {
                let held = holds
                    .as_ref()
                    .zip(scopes.get(&model.harness))
                    .map(|(holds, scope)| holds.contains_key(scope));
                model.availability =
                    availability(held, snapshot.quotas.get(&model.harness), &model.id, now)
                        .to_owned();
            }
        }
        let response = ModelsCatalogResponse {
            harnesses: snapshot.harnesses.clone(),
            models,
            next,
            fetched_at_ms: snapshot.fetched_at_ms,
            ttl_ms: snapshot.ttl_ms,
            failures: snapshot.failures.clone(),
        };
        request.validate_response(&response)?;
        Ok(response)
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
        "session" => "Advisory OpenCode catalogue from the live session GET /api/model",
        "preview" => {
            "OpenCode launch preview using the profile's provider settings and auth; workspace and live provider state can change availability"
        }
        _ => "Advisory OpenCode model catalogue",
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

/// Shared preflight for a quota reader whose frozen route is external or
/// ambiguous. The adapter declaration classifies only the frozen profile
/// and reader environment and returns a nonsecret refusal reason. The daemon
/// applies that refusal before cache access or a provider read, so routing
/// and `harness.quota` share the same decision.
pub(crate) fn external_quota_route(
    harness: &str,
    profile: &Value,
    environment: &Value,
) -> Option<&'static str> {
    let readback = HarnessCapabilities::of(harness).quota_readback?;
    readback.route_refusal?(profile, environment)
}

/// Daemon-owned launch default for an omitted model, decided over the
/// daemon's own cached `harness.models` entry. Codex launches the native
/// `isDefault` id; Muse defers to the server default (the marker is
/// display-only), so an omitted Muse model resolves to no frozen value and
/// only an empty catalogue refuses. Explicit models pass to the plugins.
pub(crate) fn launch_model_default(
    harness: &str,
    response: &HarnessModelsResponse,
) -> Result<Option<String>> {
    match HarnessCapabilities::of(harness).launch_default_policy {
        eliot_core::api::harness::LaunchDefaultPolicy::NativeMarkedDefault => {
            match response.default.clone() {
                Some(model) => Ok(Some(model)),
                None => Err(
                    eliot_plugin::start_classification::harness_unavailable_model(
                        harness,
                        "model_list",
                        "Codex model/list reported no default model; use an explicit model",
                    ),
                ),
            }
        }
        eliot_core::api::harness::LaunchDefaultPolicy::OmitForServerDefault => {
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
        eliot_core::api::harness::LaunchDefaultPolicy::None => Ok(None),
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
        response.note = Some(
            HarnessCapabilities::of(&response.harness)
                .quota_readback
                .and_then(|readback| readback.unsupported_note)
                .unwrap_or("Installed harness has no supported read-only quota method")
                .to_owned(),
        );
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

/// Display the nonsecret `(scope, credential)` pair for a frozen snapshot.
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
    // Show the account within the public scope field's byte budget.
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
    match HarnessCapabilities::of(harness).native_route {
        NativeRoute::ResponsesProxy { .. } => {
            match snapshot.get("route_binding").and_then(Value::as_object) {
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
            }
        }
        NativeRoute::AuthIntegration => {
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
        NativeRoute::FixedCredential(label) => credential = label.to_owned(),
        NativeRoute::Unspecified => {}
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
    aggregate: Option<Arc<CachedAggregate>>,
}

/// One sorted catalogue snapshot shared across pages. Holds and quota
/// freshness are evaluated when projecting each requested page.
struct CachedAggregate {
    settings_revision: Option<String>,
    harnesses: Vec<HarnessEntry>,
    models: Vec<ModelsCatalogEntry>,
    quotas: HashMap<String, ScopedQuota>,
    failures: Vec<ModelsCatalogFailure>,
    fetched_at_ms: i64,
    ttl_ms: u64,
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
}

fn documented_fallback_eligible(error: &Error) -> bool {
    matches!(error.code.as_str(), "unavailable" | "unsupported")
}

fn alias_fallback(
    harness: &str,
    documented: eliot_core::api::harness::DocumentedModelCatalog,
    scope: String,
    credential: String,
) -> HarnessModelsResponse {
    HarnessModelsResponse {
        harness: harness.to_owned(),
        catalogue: "aliases".to_owned(),
        models: (documented.entries)(),
        default: None,
        note: Some(documented.note.to_owned()),
        scope,
        fetched_at_ms: now_ms(),
        ttl_ms: CATALOG_TTL_MS,
        credential,
        configure: eliot_core::api::ConfigureSupport::for_harness(harness),
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
    if let Some(documented) = HarnessCapabilities::of(harness).documented_models
        && entries.is_empty()
    {
        return Err(Error::new(
            "source_integrity",
            documented.empty_native_inventory,
        ));
    }
    let default = raw
        .get("default")
        .and_then(Value::as_str)
        .filter(|default| !default.trim().is_empty())
        .map(str::to_owned);
    // These response labels describe the reader's provenance to the caller.
    let (catalogue, note) = if harness == "muse" && entries.is_empty() {
        (
            "empty".to_owned(),
            Some(format!(
                "Muse model/list from {cmd} returned an empty catalogue after provider discovery"
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

/// Project model metadata. An aggregate page sets availability for ad-hoc rows.
fn catalog_entry(harness: &str, model: eliot_core::api::HarnessModelEntry) -> ModelsCatalogEntry {
    use eliot_core::api::catalog_provider;
    eliot_core::api::ModelsCatalogEntry {
        harness: harness.to_owned(),
        id: model.id.clone(),
        display_name: model.display_name().to_owned(),
        provider: catalog_provider(harness, &model.id),
        efforts: model.efforts,
        default_effort: model.default_effort,
        default: model.default,
        availability: eliot_core::api::harness::CATALOG_UNKNOWN.to_owned(),
        source: model.source,
        profile: None,
    }
}

/// Build one named-profile preset entry carrying only its frozen model and
/// effort. The picker launches this row with the named profile.
fn profile_catalog_entry(
    harness: &str,
    model: eliot_core::api::HarnessModelEntry,
    profile: &str,
    frozen_effort: Option<&str>,
) -> ModelsCatalogEntry {
    let mut entry = catalog_entry(harness, model);
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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct QuotaProjection {
    pub provider: String,
    pub scope: String,
    pub observed_at_ms: i64,
    pub exhausted: bool,
    pub available: bool,
    pub mapping: QuotaMapping,
    pub provenance: &'static str,
    pub headroom: QuotaHeadroom,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct QuotaHeadroom {
    pub max_used: Option<f64>,
    pub weekly_used: Option<f64>,
}

impl QuotaHeadroom {
    fn observe(&mut self, window: &Value, now: i64, weekly: bool) {
        let Some(used) = window_used_percent(window)
            .filter(|_| window_reset_ms(window).is_none_or(|reset| reset > now))
        else {
            return;
        };
        self.max_used = Some(self.max_used.map_or(used, |prior| prior.max(used)));
        if weekly
            || window.get("windowDurationMins").and_then(Value::as_u64) == Some(10_080)
            || window
                .get("label")
                .and_then(Value::as_str)
                .and_then(|label| label.split_ascii_whitespace().last())
                .is_some_and(|period| period.eq_ignore_ascii_case("weekly"))
        {
            self.weekly_used = Some(self.weekly_used.map_or(used, |prior| prior.max(used)));
        }
    }
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

/// Normalize the adapter-declared quota `details` into one projection.
/// A model scopes named windows. Without one only account windows apply.
/// Returns `None` when no signal maps to this observation's route
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
    let readback = HarnessCapabilities::of(observation.provider).quota_readback?;
    match readback.projection {
        eliot_core::api::harness::QuotaProjectionFormat::UsagePair => {
            let (windows, complete, provenance) = usage_pair_window_set(details)?;
            let mut projection =
                windows_projection(observation, &windows, complete, model, now, provenance)?;
            if provenance != "muse:windows"
                && let Some(weekly) = details.get("usage").unwrap_or(details).get("weekly")
                && window_mapping(weekly, model) != QuotaMapping::Unmapped
            {
                projection.headroom.observe(weekly, now, true);
            }
            Some(projection)
        }
        eliot_core::api::harness::QuotaProjectionFormat::RateLimits => {
            rate_limits_projection(observation, details, model, now)
        }
        eliot_core::api::harness::QuotaProjectionFormat::IntegrationWindows => {
            integration_windows_projection(observation, details, model, now)
        }
        eliot_core::api::harness::QuotaProjectionFormat::Windows => {
            let windows = details
                .get("windows")
                .and_then(Value::as_array)
                .filter(|windows| !windows.is_empty())?;
            let windows: Vec<&Value> = windows.iter().collect();
            windows_projection(observation, &windows, true, model, now, "windows")
        }
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

/// Usage-pair readers contribute three shapes over time: retained `windows[]`
/// observations, live `usage{window,weekly}` reads, and the older top-level
/// `window`/`weekly` pair. The legacy pair is complete only when both halves
/// are present; a lone half is not headroom.
fn usage_pair_window_set(details: &Value) -> Option<(Vec<&Value>, bool, &'static str)> {
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

/// Only account-wide windows with explicit bounds can be joined to retained
/// Swarm usage. Model families can have different native windows and billing.
fn quota_usage_windows(
    response: &HarnessQuotaResponse,
    now: i64,
) -> Vec<eliot_core::api::harness::QuotaWindowBounds> {
    use eliot_core::api::harness::QuotaWindowBounds;

    let mut bounds = Vec::new();
    let Some(details) = response.details.as_ref() else {
        return bounds;
    };
    let Some(readback) = HarnessCapabilities::of(&response.harness).quota_readback else {
        return bounds;
    };
    let mut collect = |label: &str, window: &Value, duration: Option<i64>| {
        let Some((resets, minutes)) =
            window_reset_ms(window).zip(window["windowDurationMins"].as_i64().or(duration))
        else {
            return;
        };
        if window_used_percent(window).is_some()
            && minutes > 0
            && resets > now
            && let Some(starts_at_ms) = minutes
                .checked_mul(60_000)
                .and_then(|ms| resets.checked_sub(ms))
            && (0..now).contains(&starts_at_ms)
        {
            bounds.push(QuotaWindowBounds {
                label: label.to_owned(),
                starts_at_ms: Some(starts_at_ms),
                through_ms: now,
            });
        }
    };
    use eliot_core::api::harness::QuotaProjectionFormat;
    match readback.projection {
        QuotaProjectionFormat::RateLimits => {
            let rates = &details["rateLimits"];
            for (key, label) in [("primary", "Primary"), ("secondary", "Secondary")] {
                collect(label, &rates[key], None);
            }
        }
        QuotaProjectionFormat::UsagePair if details["windows"].as_array().is_none() => {
            let usage = details.get("usage").unwrap_or(details);
            collect("5-hour", &usage["window"], None);
            collect("Weekly", &usage["weekly"], Some(10_080));
        }
        QuotaProjectionFormat::UsagePair
        | QuotaProjectionFormat::IntegrationWindows
        | QuotaProjectionFormat::Windows => {
            if let Some(windows) = details["windows"].as_array() {
                for window in windows {
                    if let Some(label) = window["label"].as_str()
                        && window_family(label).is_none()
                    {
                        collect(label, window, None);
                    }
                }
            }
        }
    }
    bounds
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
            headroom: QuotaHeadroom::default(),
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
    let mut headroom = QuotaHeadroom::default();
    for (window, _) in applicable {
        headroom.observe(window, now, false);
    }
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
        headroom,
    })
}

fn integration_windows_projection(
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

fn rate_limits_projection(
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
    let mut headroom = QuotaHeadroom::default();
    for bucket in std::iter::once(rates)
        .chain(model.and_then(|model| details.get("rateLimitsByLimitId")?.get(model)))
    {
        for key in ["primary", "secondary"] {
            headroom.observe(&bucket[key], now, false);
        }
    }
    Some(QuotaProjection {
        provider: observation.provider.to_owned(),
        scope: observation.scope.to_owned(),
        observed_at_ms: observation.observed_at_ms,
        exhausted,
        available,
        mapping,
        provenance: "codex:rateLimits",
        headroom,
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
