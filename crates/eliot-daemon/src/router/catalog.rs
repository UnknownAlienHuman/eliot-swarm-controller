//! Bounded cached catalogue/quota scope derivation for `harness.models`
//! and `harness.quota`.
//!
//! The cache is keyed by the nonsecret native route/account scope, so one
//! live native read serves every caller on that scope until its TTL lapses.
//! Scope and credential identity derive only from frozen snapshot selectors
//! (harness, profile label, route/binding names); credential values,
//! `pass_env` resolutions and literal environment bodies are never read here.

use super::{MODE_OPEN, Principal, Router, catalog, now_ms, require_owner};
use eliot_core::api::{
    HARNESS_READ_TTL_MS, HarnessEntry, HarnessModelsResponse, HarnessQuotaResponse,
    ModelsCatalogEntry, ModelsCatalogFailure,
};
use eliot_core::{Error, Result};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::{path::PathBuf, sync::atomic::Ordering};

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
        let mut harnesses = Vec::new();
        for harness in crate::adhoc::SUPPORTED_HARNESSES {
            let entry = crate::adhoc::harness_installation(harness);
            // Claude keeps several native installs (PATH, user locations,
            // desktop-app bundles): report every candidate newest first so
            // the controller can see which install was preferred and why.
            let candidates = if *harness == "claude" {
                let candidates = crate::adhoc::discover_claude_candidates();
                (!candidates.is_empty()).then_some(candidates)
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
        let response = eliot_core::api::HarnessListResponse { harnesses };
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness list could not be encoded"))
    }
    /// Resolve the frozen snapshot for a catalogue/quota read. A named
    /// profile must exist and its frozen harness must match; otherwise the
    /// daemon defaults cover live harnesses (Codex/Muse/OpenCode, plus
    /// Antigravity when `live_antigravity` holds for `harness.models`, whose
    /// `agy models` reader needs a frozen cmd) while static harnesses (Claude,
    /// and Antigravity for `harness.quota`) need no snapshot at all.
    fn resolve_catalog_snapshot(
        &self,
        harness: &str,
        profile: Option<&str>,
        live_antigravity: bool,
    ) -> Result<(Option<Value>, String)> {
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
            return Ok((Some(snapshot), name.to_owned()));
        }
        if matches!(harness, "codex" | "muse" | "opencode")
            || (live_antigravity && harness == "antigravity")
        {
            let cmd = crate::adhoc::discover_cmd_for_harness(harness)?;
            let mut snapshot = crate::adhoc::adhoc_snapshot(
                harness,
                None,
                None,
                cmd,
                crate::adhoc::AdhocAuth::Skip,
                None,
            )?;
            crate::config::stamp_cloud_mcp_hostname(&mut snapshot, &self.inner.data_root);
            crate::config::capture_native_store_inputs(&mut snapshot, |name| {
                std::env::var_os(name)
            })?;
            return Ok((Some(snapshot), "adhoc".to_owned()));
        }
        Ok((None, "adhoc".to_owned()))
    }

    /// The route an existing Agent's `configure` draws from: its frozen
    /// launch snapshot, never the current profile file. The label carries the
    /// exact Agent generation so a later profile edit cannot share its cache
    /// entry. Observed Agents have no Eliot launch route.
    async fn agent_catalog_snapshot(
        &self,
        agent: &str,
        harness: &str,
    ) -> Result<(Option<Value>, String)> {
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
        Ok((Some(agent.profile_snapshot), label))
    }

    /// Read-only native model catalogue for a harness: no model turn, no
    /// inference. Codex/Muse/OpenCode/Antigravity use the harness's own
    /// read-only method through a dedicated metadata reader, cached per native
    /// route/account with freshness and credential identity; Claude reports
    /// its frozen documented aliases and full model ids plus the CLI
    /// default, each entry marked `documented_alias`. Native model ids are
    /// retained verbatim: no fallback model is ever selected.
    pub(super) async fn harness_models(
        &self,
        params: &Value,
        principal: &Principal,
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
        let (snapshot, label) = match request.agent.as_deref() {
            Some(agent) => self.agent_catalog_snapshot(agent, &harness).await?,
            None => self.resolve_catalog_snapshot(&harness, profile, true)?,
        };
        let (scope, credential) = catalog::describe_snapshot(&label, &harness, snapshot.as_ref());
        let cmd = snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.get("cmd"))
            .and_then(Value::as_str)
            .unwrap_or(&harness);
        let executable = if std::path::Path::new(cmd).is_absolute() {
            Some(PathBuf::from(cmd))
        } else {
            crate::adhoc::find_on_path(&format!("{cmd}.exe"))
                .or_else(|| crate::adhoc::find_on_path(cmd))
        };
        let cmd = executable
            .as_ref()
            .map(|path| path.to_string_lossy().into_owned())
            .unwrap_or_else(|| cmd.to_owned());
        let version = if harness == "claude" {
            Some("static-aliases".to_owned())
        } else {
            crate::adhoc::native_version_string(&cmd)
        };
        let cache_key = catalog::models_key(&scope, &cmd, version.as_deref());
        let now = now_ms();
        if let Some(hit) = cache_key
            .as_deref()
            .and_then(|key| self.inner.catalog_cache.lock_recover().get_models(key, now))
        {
            request.validate_response(&hit)?;
            return serde_json::to_value(hit)
                .map_err(|_| Error::new("internal", "harness models could not be encoded"));
        }
        let response = match harness.as_str() {
            "claude" => eliot_core::api::HarnessModelsResponse {
                harness: harness.clone(),
                catalogue: "aliases".to_owned(),
                models: crate::adhoc::claude_documented_models(),
                default: None,
                note: Some(
                    "Claude Code exposes no read-only model inventory (no models subcommand, \
                     --model names aliases or a full model name, initialize reports only the \
                     session model); entries are the documented aliases and full model ids from \
                     https://code.claude.com/docs/en/model-config (checked 2026-11-16, installed \
                     CLI 2.1.325), each marked source documented_alias with unknown availability; \
                     the CLI default applies when no model is passed"
                        .to_owned(),
                ),
                scope: scope.clone(),
                fetched_at_ms: now_ms(),
                ttl_ms: catalog::CATALOG_TTL_MS,
                credential: credential.clone(),
                configure: eliot_core::api::ConfigureSupport::for_harness(&harness),
            },
            "codex" | "muse" | "opencode" | "antigravity" => {
                let Some(snapshot) = snapshot else {
                    return Err(Error::new(
                        "internal",
                        "live harness catalogue requires a frozen snapshot",
                    ));
                };
                let snapshot_cmd = snapshot
                    .get("cmd")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let raw = self.inner.plugin_host.models(snapshot).await?;
                let models = raw
                    .get("models")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
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
                    let binary = snapshot_cmd.as_deref().unwrap_or("muse");
                    (
                        "empty".to_owned(),
                        Some(format!(
                            "Muse model/list from {binary} returned an empty catalogue; ad-hoc launch needs an explicit model, or --cmd naming a binary with a live catalogue"
                        )),
                    )
                } else if harness == "muse" {
                    (
                        "native".to_owned(),
                        Some(
                            "Muse model/list names no default marker; the server default applies at launch"
                                .to_owned(),
                        ),
                    )
                } else if harness == "opencode" {
                    (
                        "native".to_owned(),
                        Some(
                            "OpenCode catalogue from native `opencode models`; ids are provider/model verbatim, the frozen auth integration applies at launch"
                                .to_owned(),
                        ),
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
                eliot_core::api::HarnessModelsResponse {
                    harness: harness.clone(),
                    catalogue,
                    models: entries,
                    default,
                    note,
                    scope: scope.clone(),
                    fetched_at_ms: now_ms(),
                    ttl_ms: catalog::CATALOG_TTL_MS,
                    credential: credential.clone(),
                    configure: eliot_core::api::ConfigureSupport::for_harness(&harness),
                }
            }
            _ => {
                return Err(Error::new(
                    "unsupported",
                    format!("unsupported harness {harness}"),
                ));
            }
        };
        request.validate_response(&response)?;
        if let Some(key) = cache_key {
            self.inner
                .catalog_cache
                .lock_recover()
                .insert_models(key, response.clone());
        }
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness models could not be encoded"))
    }

    /// Provider-supported account quota readback: timestamp, scope and an
    /// explicit supported/unsupported/unknown status, cached per account.
    /// Starts no model turn, performs no inference and never polls per
    /// Agent. Codex reads `account/rateLimits/read` (plus `account/usage/read`
    /// when useful) and Muse reads `usage/read` through a dedicated metadata
    /// reader; OpenCode/Claude/Antigravity expose no read-only quota surface,
    /// so those report `unsupported` naming the checked surface. A method-not-
    /// found or schema mismatch stays `unsupported` or `unknown` naming the
    /// checked method; no numbers are fabricated. A quota failure never
    /// selects another model or account.
    pub(super) async fn harness_quota(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
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
        let (mut snapshot, label) = self.resolve_catalog_snapshot(&harness, profile, true)?;
        if harness == "claude" && snapshot.is_none() {
            let cmd = crate::adhoc::discover_cmd_for_harness(&harness)?;
            let mut resolved = crate::adhoc::adhoc_snapshot(
                &harness,
                None,
                None,
                cmd,
                crate::adhoc::AdhocAuth::Skip,
                None,
            )?;
            crate::config::capture_native_store_inputs(&mut resolved, |name| {
                std::env::var_os(name)
            })?;
            snapshot = Some(resolved);
        }
        self.quota_for_snapshot(harness, snapshot, label).await
    }

    pub(super) async fn quota_for_snapshot(
        &self,
        harness: String,
        snapshot: Option<Value>,
        label: String,
    ) -> Result<Value> {
        let request = eliot_core::api::HarnessQuotaRequest {
            harness: harness.clone(),
            profile: None,
        };
        let (scope, credential) = catalog::describe_snapshot(&label, &harness, snapshot.as_ref());
        if harness == "muse"
            && let Some(profile) = snapshot.as_ref()
            && let Some(observation) = self
                .inner
                .store
                .lock()
                .await
                .latest_quota_observation(profile)?
        {
            return serde_json::to_value(eliot_core::api::HarnessQuotaResponse {
                harness,scope,credential,fetched_at_ms:observation.observed_at_ms,ttl_ms:catalog::CATALOG_TTL_MS,
                status:"supported".to_owned(),note:Some("Muse subscription usage observed during a turn".to_owned()),
                details:Some(json!({"windows":observation.windows,"state":"ready","origin":"provider","source":"Muse MSP usage/changed"})),
            }).map_err(|_|Error::new("internal","quota encoding failed"));
        }
        if let Some(hit) = self
            .inner
            .catalog_cache
            .lock_recover()
            .get_quota(&scope, now_ms())
        {
            request.validate_response(&hit)?;
            return serde_json::to_value(hit)
                .map_err(|_| Error::new("internal", "quota encoding failed"));
        }
        let profile = snapshot
            .ok_or_else(|| Error::new("profile_error", "quota requires a native profile"))?;
        let raw = self.inner.plugin_host.quota(profile).await;
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
        self.inner
            .catalog_cache
            .lock_recover()
            .insert_quota(response.clone());
        serde_json::to_value(response).map_err(|_| Error::new("internal", "quota encoding failed"))
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
        let query = request.query.as_deref().unwrap_or("").to_owned();
        let limit = request.effective_limit();
        let after = request.after.clone();
        // Coalesce concurrent callers: the first rebuilds, the rest share
        // the fresh aggregate until its TTL lapses.
        let _coalesce = self.inner.catalog_aggregate_lock.lock().await;
        // Harness inventory with versions for the aggregate key.
        let harnesses_value = self.harness_list(&json!({}), principal).await?;
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
            let store = self.inner.store.lock().await;
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
        for entry in &harnesses {
            let harness = entry.harness.as_str();
            if !entry.installed {
                continue;
            }
            // Reuse the per-scope cached `harness.models` path so one live
            // native read serves every caller until its TTL lapses. Claude
            // contributes its frozen documented-alias entries the same way;
            // quota stays unsupported there, so their availability reads
            // unknown (membership alone never proves availability).
            let params = json!({"harness": harness});
            let response_value = match self.harness_models(&params, principal).await {
                Ok(value) => value,
                Err(error) => {
                    // The aggregate names the failed harness with its typed
                    // error instead of silently listing no models for it.
                    crate::logging::record(
                        &self.secret_policy_snapshot(),
                        "catalog.harness_failed",
                        &[("source", harness), ("method", "harness.models")],
                        Some(&error),
                    );
                    failures.push(ModelsCatalogFailure::from_error(harness, &error));
                    continue;
                }
            };
            let response: eliot_core::api::HarnessModelsResponse =
                serde_json::from_value(response_value)
                    .map_err(|_| Error::new("internal", "harness models could not be decoded"))?;
            let held = held_harness(harness);
            let quota = self
                .harness_quota(&params, principal)
                .await
                .ok()
                .and_then(|value| {
                    serde_json::from_value::<eliot_core::api::HarnessQuotaResponse>(value).ok()
                });
            for model in response.models {
                models.push(catalog::catalog_entry(
                    harness,
                    model,
                    held,
                    quota.as_ref(),
                    now_ms(),
                ));
            }
        }
        models.sort_by(|left, right| {
            (left.harness.as_str(), left.id.as_str())
                .cmp(&(right.harness.as_str(), right.id.as_str()))
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
        use eliot_core::api::{ModelsCatalogResponse, catalog_cursor, catalog_matches};
        let mut filtered: Vec<eliot_core::api::ModelsCatalogEntry> = models
            .iter()
            .filter(|entry| catalog_matches(entry, query))
            .cloned()
            .collect();
        filtered.sort_by(|left, right| {
            (left.harness.as_str(), left.id.as_str())
                .cmp(&(right.harness.as_str(), right.id.as_str()))
        });
        let start = match after {
            None => 0,
            Some(cursor) => filtered
                .iter()
                .position(|entry| catalog_cursor(&entry.harness, &entry.id) == cursor)
                .map(|index| index + 1)
                .unwrap_or(0),
        };
        let page: Vec<eliot_core::api::ModelsCatalogEntry> =
            filtered.into_iter().skip(start).take(limit).collect();
        let next = if page.len() == limit {
            page.last()
                .map(|entry| catalog_cursor(&entry.harness, &entry.id))
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

/// Build one unified `models.catalog` entry from a harness catalogue
/// entry: provider and display name derive from the id, availability comes
/// from holds/quota (`unknown` without affirmative evidence), and the entry
/// `source` marker is copied verbatim.
pub(crate) fn catalog_entry(
    harness: &str,
    model: eliot_core::api::HarnessModelEntry,
    held: Option<bool>,
    quota: Option<&HarnessQuotaResponse>,
    now: i64,
) -> eliot_core::api::ModelsCatalogEntry {
    use eliot_core::api::{catalog_display_name, catalog_provider};
    eliot_core::api::ModelsCatalogEntry {
        harness: harness.to_owned(),
        id: model.id.clone(),
        display_name: catalog_display_name(&model.id),
        provider: catalog_provider(harness, &model.id),
        efforts: model.efforts,
        default_effort: model.default_effort,
        default: model.default,
        availability: availability(held, quota, &model.id, now).to_owned(),
        source: model.source,
    }
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
    use super::super::tests::router;
    use super::*;
    use serde_json::json;

    #[test]
    fn quota_read_error_note_is_byte_bounded_on_a_char_boundary() {
        let mut response = HarnessQuotaResponse {
            harness: "claude".to_owned(),
            scope: "profile:adhoc harness:claude".to_owned(),
            fetched_at_ms: 1_000,
            ttl_ms: CATALOG_TTL_MS,
            credential: "none".to_owned(),
            status: "supported".to_owned(),
            note: None,
            details: None,
        };
        let message = "ж".repeat(400);
        quota_read_error(
            &mut response,
            eliot_core::Error::new("quota_unavailable", message),
        );
        let note = response.note.expect("quota read error carries a note");
        assert!(note.len() <= 512, "note is {} bytes", note.len());
        assert!(note.chars().all(|c| c == 'ж'));
    }

    #[test]
    fn quota_method_not_found_is_unsupported_and_names_checked_method() {
        for (harness, method) in [("codex", "account/rateLimits/read"), ("muse", "usage/read")] {
            let response = HarnessQuotaResponse {
                harness: harness.to_owned(),
                scope: "profile:p".to_owned(),
                credential: "ambient".to_owned(),
                fetched_at_ms: 1000,
                ttl_ms: CATALOG_TTL_MS,
                status: "supported".to_owned(),
                note: None,
                details: None,
            };
            let mut unsupported = response.clone();
            quota_read_error(
                &mut unsupported,
                eliot_core::Error::new("unsupported", "method not found"),
            );
            assert_eq!(unsupported.status, "unsupported");
            assert!(unsupported.note.unwrap().contains(method));
            assert!(unsupported.details.is_none());
            let mut environment = response.clone();
            quota_read_error(
                &mut environment,
                eliot_core::Error::new("unavailable", "reader failed"),
            );
            assert_eq!(environment.status, "supported");
            assert_eq!(environment.details.unwrap()["origin"], "environment");
            let mut provider = response;
            quota_read_error(
                &mut provider,
                eliot_core::Error::new("quota_unavailable", "upstream failed")
                    .with_origin(eliot_core::FailureOrigin::Provider, "provider read"),
            );
            assert_eq!(provider.status, "supported");
            assert_eq!(provider.details.unwrap()["origin"], "provider");
        }
    }

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
    fn catalog_entry_keeps_documented_source_with_unknown_availability() {
        use eliot_core::api::HarnessModelEntry;
        use eliot_core::api::harness::{CATALOG_HELD, CATALOG_UNKNOWN};
        let entry = HarnessModelEntry {
            id: "haiku".to_owned(),
            default: false,
            efforts: Some(vec!["low".to_owned()]),
            default_effort: None,
            source: Some("documented_alias".to_owned()),
        };
        // Claude quota stays unsupported: membership alone never proves
        // availability, and the provenance marker is copied verbatim.
        let unsupported = HarnessQuotaResponse {
            harness: "claude".to_owned(),
            scope: "profile:adhoc harness:claude".to_owned(),
            fetched_at_ms: 1_000,
            ttl_ms: CATALOG_TTL_MS,
            credential: "none".to_owned(),
            status: "unsupported".to_owned(),
            note: None,
            details: None,
        };
        let built = catalog_entry(
            "claude",
            entry.clone(),
            Some(false),
            Some(&unsupported),
            1_000,
        );
        assert_eq!(built.availability, CATALOG_UNKNOWN);
        assert_eq!(built.source.as_deref(), Some("documented_alias"));
        assert_eq!(built.provider, "anthropic");
        assert_eq!(built.efforts, entry.efforts);
        let held = catalog_entry("claude", entry, Some(true), None, 1_000);
        assert_eq!(held.availability, CATALOG_HELD);
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
            failures: Vec::new(),
            fetched_at_ms: 1_000,
            ttl_ms: CATALOG_TTL_MS,
        };
        cache.insert_aggregate(entry);
        assert_eq!(cache.aggregate_len(), 1);
        // Concurrent callers share the fresh entry: the same key hits.
        assert!(cache.get_aggregate(&key, 1_000, false).is_some());
        assert!(
            cache
                .get_aggregate(&key, 1_000 + CATALOG_TTL_MS as i64, false)
                .is_some()
        );
        assert!(
            cache
                .get_aggregate(&key, 1_000 + CATALOG_TTL_MS as i64 + 1, false)
                .is_none()
        );
        let upgraded = vec![harness_entry("codex", "codex", "1.1")];
        assert!(
            cache
                .get_aggregate(&aggregate_key(&upgraded), 1_000, false)
                .is_none()
        );
        // A rebuild replaces the single bounded entry, never grows it.
        cache.insert_aggregate(CachedAggregate {
            key: aggregate_key(&upgraded),
            harnesses: upgraded,
            models: Vec::new(),
            failures: Vec::new(),
            fetched_at_ms: 2_000,
            ttl_ms: CATALOG_TTL_MS,
        });
        assert_eq!(cache.aggregate_len(), 1);
    }

    #[test]
    fn partial_aggregate_serves_only_continuation_pages() {
        // k98: a failed Codex read was cached for the whole TTL and Codex
        // vanished from every picker. A partial aggregate keeps one
        // listing's pages consistent; every new listing reads again.
        let mut cache = CatalogCache::default();
        let harnesses = vec![harness_entry("codex", "codex", "1.0")];
        let key = aggregate_key(&harnesses);
        cache.insert_aggregate(CachedAggregate {
            key: key.clone(),
            harnesses,
            models: Vec::new(),
            failures: vec![ModelsCatalogFailure {
                harness: "codex".to_owned(),
                code: "busy".to_owned(),
                message: "metadata_readers in-flight bound reached (limit=4)".to_owned(),
                origin: eliot_core::FailureOrigin::Swarm,
            }],
            fetched_at_ms: 1_000,
            ttl_ms: PARTIAL_AGGREGATE_TTL_MS,
        });
        assert!(cache.get_aggregate(&key, 1_001, false).is_none());
        assert!(cache.get_aggregate(&key, 1_001, true).is_some());
        assert!(
            cache
                .get_aggregate(&key, 1_000 + PARTIAL_AGGREGATE_TTL_MS as i64 + 1, true)
                .is_none()
        );
    }
    #[tokio::test]
    async fn catalog_models_caches_static_harness_per_scope() {
        let router = router();
        // Claude needs no native installation: the aliases read never spawns
        // a reader, so this also proves the static path performs no I/O.
        let first = router
            .harness_models(&json!({"harness": "claude"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(first["catalogue"], json!("aliases"));
        assert_eq!(first["scope"], json!("profile:adhoc harness:claude"));
        assert_eq!(first["credential"], json!("none"));
        assert!(first["fetched_at_ms"].as_i64().unwrap_or(-1) >= 0);
        // The Claude group is no longer empty: documented aliases and full
        // ids, each marked with its provenance and the CLI effort
        // vocabulary, never a live availability claim.
        let models = first["models"].as_array().expect("claude entries");
        assert!(!models.is_empty());
        for model in models {
            assert_eq!(model["source"], json!("documented_alias"));
            assert!(model["id"].as_str().is_some_and(|id| !id.is_empty()));
            assert_eq!(
                model["efforts"].as_array().expect("claude efforts").len(),
                crate::adhoc::CLAUDE_EFFORT_LEVELS.len()
            );
        }
        let ids: Vec<&str> = models
            .iter()
            .filter_map(|model| model["id"].as_str())
            .collect();
        for expected in [
            "haiku",
            "sonnet",
            "opus",
            "opusplan",
            "claude-haiku-5-5",
            "claude-opus-5-5",
        ] {
            assert!(ids.contains(&expected), "missing {expected}");
        }
        let second = router
            .harness_models(&json!({"harness": "claude"}), &Principal::Owner)
            .await
            .unwrap();
        // Served from the per-scope cache: the freshness timestamp is kept.
        assert_eq!(first["fetched_at_ms"], second["fetched_at_ms"]);
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn catalog_quota_retains_supported_errors_with_scope_and_caches() {
        let router = router();
        // This check covers cache/error semantics only, without native/provider I/O.
        let (scope, credential) = catalog::describe_snapshot(
            "adhoc",
            "antigravity",
            Some(&json!({"harness":"antigravity"})),
        );
        router.inner.catalog_cache.lock_recover().insert_quota(
            eliot_core::api::HarnessQuotaResponse {
                harness: "antigravity".to_owned(),
                scope,
                credential,
                fetched_at_ms: now_ms(),
                ttl_ms: catalog::CATALOG_TTL_MS,
                status: "supported".to_owned(),
                note: Some("Native credential unavailable".to_owned()),
                details: Some(json!({"state":"error","origin":"environment"})),
            },
        );
        let first = router
            .harness_quota(&json!({"harness": "antigravity"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(first["status"], json!("supported"));
        assert_eq!(first["scope"], json!("profile:adhoc harness:antigravity"));
        assert_eq!(first["credential"], json!("antigravity-cli-keyring"));
        assert!(first["fetched_at_ms"].as_i64().unwrap_or(-1) >= 0);
        assert!(first["note"].as_str().is_some_and(|note| !note.is_empty()));
        let second = router
            .harness_quota(&json!({"harness": "antigravity"}), &Principal::Owner)
            .await
            .unwrap();
        assert_eq!(first["fetched_at_ms"], second["fetched_at_ms"]);
        assert_eq!(first, second);
        // Unknown profiles fail before any native work, and Agent callers
        // are rejected at the owner-only gate.
        let error = router
            .harness_quota(
                &json!({"harness": "codex", "profile": "missing"}),
                &Principal::Owner,
            )
            .await
            .unwrap_err();
        assert!(!error.code.is_empty());
        let agent = Principal::Agent {
            id: "a1".to_owned(),
            generation: 0,
        };
        let error = router
            .harness_quota(&json!({"harness": "codex"}), &agent)
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
        let error = router
            .harness_models(&json!({"harness": "codex"}), &agent)
            .await
            .unwrap_err();
        assert_eq!(error.code, "not_allowed");
    }
}
