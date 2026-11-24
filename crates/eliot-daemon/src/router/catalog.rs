//! Bounded cached catalogue/quota scope derivation for `harness.models`
//! and `harness.quota`.
//!
//! Live cache reuse requires a frozen route verified against the reader's
//! private process environment. Public scope labels contain only route names.

use super::{MODE_OPEN, Principal, Router, catalog, now_ms, require_owner};
use crate::adhoc::{DISCOVERY_RETRY_INTERVAL, DISCOVERY_RETRY_MAX, DiscoveryFailure};
use eliot_core::api::harness::{
    ModelLoginSource, ModelsCatalogWatchRequest, ModelsCatalogWatchResponse, NativeRoute,
    QuotaEvidence, QuotaWindowFacts,
};
use eliot_core::api::{
    HARNESS_READ_TTL_MS, HarnessCandidate, HarnessCapabilities, HarnessEntry, HarnessListResponse,
    HarnessModelsRequest, HarnessModelsResponse, HarnessQuotaRequest, HarnessQuotaResponse,
    ModelsCatalogEntry, ModelsCatalogFailure, ModelsCatalogRequest, ModelsCatalogResponse,
    SUPPORTED_HARNESSES,
};
use eliot_core::{Error, Result};
use eliot_plugin::protocol::ModelCatalogue;
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::sync::watch;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ModelReadMode {
    Serve,
    Refresh,
    Exact,
    Launch,
}

struct ModelReadOptions {
    workspace: Option<String>,
    deadline: Instant,
    mode: ModelReadMode,
}

impl Router {
    pub(super) async fn models_catalog_watch(
        &self,
        params: &Value,
        principal: &Principal,
    ) -> Result<Value> {
        require_owner(principal)?;
        let request: ModelsCatalogWatchRequest =
            eliot_core::api::discovery::decode_params(params.clone())?;
        request.validate()?;
        let mut updates = self.inner.catalog_updates.subscribe();
        let wait = async {
            loop {
                if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                    return Err(catalog_shutting_down());
                }
                let current = self
                    .inner
                    .catalog_cache
                    .lock_recover()
                    .aggregate
                    .as_ref()
                    .filter(|snapshot| snapshot.is_usable())
                    .map(|snapshot| snapshot.revision.clone());
                if let Some(current) = current
                    && Some(&current) != request.revision.as_ref()
                {
                    return Ok(Some(current));
                }
                updates
                    .changed()
                    .await
                    .map_err(|_| Error::new("unknown", "model catalogue refresh worker stopped"))?;
            }
        };
        let revision = tokio::time::timeout(Duration::from_secs(20), wait)
            .await
            .unwrap_or(Ok(None))?;
        serde_json::to_value(ModelsCatalogWatchResponse { revision })
            .map_err(|_| Error::new("internal", "catalogue revision could not be encoded"))
    }

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
            crate::config::capture_native_store_inputs(
                &mut snapshot,
                self.inner.stand_codex_home.as_deref(),
            )?;
            if snapshot.get("harness").and_then(Value::as_str) != Some(harness) {
                return Err(Error::new(
                    "invalid",
                    "harness does not match the selected profile",
                ));
            }
            let candidate = self
                .catalog_profile_command(name, harness, snapshot["cmd"].as_str(), deadline)
                .await?;
            crate::adhoc::write_profile_command_snapshot(&mut snapshot, candidate)?;
            return Ok((Some(snapshot), name.to_owned(), None));
        }
        let (cmd, discovered) = if !quota && crate::adhoc::has_ranked_install_candidates(harness) {
            let resolve = crate::adhoc::ranked_reader_executable(harness, deadline);
            match self
                .catalog_discovery(harness, CatalogDiscoverySource::Ranked, deadline, resolve)
                .await
            {
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
            let cmd = self
                .catalog_discovery(
                    harness,
                    CatalogDiscoverySource::Installed,
                    deadline,
                    crate::adhoc::discover_cmd_until(harness, deadline),
                )
                .await?;
            (cmd, None)
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
        crate::config::capture_native_store_inputs(
            &mut snapshot,
            self.inner.stand_codex_home.as_deref(),
        )?;
        Ok((Some(snapshot), "adhoc".to_owned(), discovered))
    }

    async fn catalog_profile_command(
        &self,
        label: &str,
        harness: &str,
        cmd: Option<&str>,
        deadline: Instant,
    ) -> Result<HarnessCandidate> {
        self.catalog_discovery(
            harness,
            CatalogDiscoverySource::Profile(label.to_owned(), cmd.map(str::to_owned)),
            deadline,
            crate::adhoc::resolve_profile_command_until(label, harness, cmd, deadline),
        )
        .await
    }

    /// Key the cooldown before resolving the executable, including failures
    /// for which no executable fingerprint can be constructed.
    async fn catalog_discovery<T>(
        &self,
        harness: &str,
        source: CatalogDiscoverySource,
        deadline: Instant,
        discover: impl Future<Output = Result<T>>,
    ) -> Result<T> {
        let gate = self
            .inner
            .catalog_cache
            .lock_recover()
            .discovery_gate((harness.to_owned(), source))?;
        let mut failure = tokio::time::timeout_at(deadline.into(), gate.lock())
            .await
            .map_err(|_| crate::adhoc::discovery_timeout())?;
        if let Some(failure) = failure
            .as_ref()
            .filter(|failure| Instant::now() < failure.until)
        {
            return Err(failure.error.clone());
        }
        let result = discover.await;
        if result
            .as_ref()
            .is_err_and(|error| crate::adhoc::caller_discovery_timed_out(error, deadline))
        {
            return result;
        }
        *failure = result
            .as_ref()
            .err()
            .map(|error| DiscoveryFailure::next(failure.as_ref(), error.clone()));
        result
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
        let mode = if request.workspace.is_some() {
            ModelReadMode::Exact
        } else {
            ModelReadMode::Serve
        };
        let response = self.harness_models_until(request, deadline, mode).await?;
        serde_json::to_value(response)
            .map_err(|_| Error::new("internal", "harness models could not be encoded"))
    }

    async fn harness_models_until(
        &self,
        request: HarnessModelsRequest,
        deadline: Instant,
        mode: ModelReadMode,
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
        let response = if let Some((agent, generation)) = live_identity.as_ref().filter(|_| {
            HarnessCapabilities::of(&harness)
                .manifest_extra_verbs
                .contains(&"models")
        }) {
            let (scope, credential) =
                catalog::describe_snapshot(&label, &harness, snapshot.as_ref());
            let raw = self
                .inner
                .plugin_host
                .agent_models(agent, *generation)
                .await?;
            catalog::native_models_response(&harness, "", scope, credential, raw)?
        } else {
            self.models_for_snapshot(
                &harness,
                snapshot,
                &label,
                discovered,
                ModelReadOptions {
                    workspace: request.workspace.clone(),
                    deadline,
                    mode,
                },
            )
            .await?
        };
        if let Some((agent, generation)) = live_identity
            && !self
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
        request.validate_response(&response)?;
        Ok(response)
    }

    pub(super) async fn launch_catalogue(
        &self,
        harness: &str,
        snapshot: Value,
        label: &str,
        deadline: Instant,
    ) -> Result<HarnessModelsResponse> {
        self.models_for_snapshot(
            harness,
            Some(snapshot),
            label,
            None,
            ModelReadOptions {
                workspace: None,
                deadline,
                mode: ModelReadMode::Launch,
            },
        )
        .await
    }

    async fn models_for_snapshot(
        &self,
        harness: &str,
        snapshot: Option<Value>,
        label: &str,
        discovered: Option<HarnessCandidate>,
        options: ModelReadOptions,
    ) -> Result<HarnessModelsResponse> {
        let ModelReadOptions {
            workspace,
            deadline,
            mode,
        } = options;
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
            let unverified = || {
                Error::new(
                    "unavailable",
                    documented_models
                        .map_or("model catalogue source could not be verified", |models| {
                            models.unverified_route
                        }),
                )
            };
            if route.identity.is_none() {
                return Err(unverified());
            }
            let workspace_key = if let Some(path) = workspace {
                Some(
                    crate::adhoc::blocking_discovery(deadline, move || {
                        let path = std::path::Path::new(&path);
                        let invalid_workspace = || {
                            Error::new(
                                "invalid",
                                "model preview workspace must be an existing absolute directory",
                            )
                        };
                        if !path.is_absolute() {
                            return Err(invalid_workspace());
                        }
                        let root = path.canonicalize().map_err(|_| invalid_workspace())?;
                        if !root.is_dir() {
                            return Err(invalid_workspace());
                        }
                        Ok(root)
                    })
                    .await??,
                )
            } else {
                None
            };
            let workspace = workspace_key
                .as_ref()
                .map(|path| {
                    path.to_str().map(str::to_owned).ok_or_else(|| {
                        Error::new("invalid", "model preview workspace path is not UTF-8")
                    })
                })
                .transpose()?;
            let cmd = profile["cmd"].as_str().unwrap_or(harness);
            let verify = |cmd: String, version: Option<String>| {
                let unversioned = || {
                    Error::new(
                        "unavailable",
                        "model catalogue executable version could not be verified",
                    )
                };
                let version = version.ok_or_else(unversioned)?;
                let executable =
                    crate::adhoc::VersionKey::read(&cmd, None).ok_or_else(unversioned)?;
                Ok((cmd, executable, version))
            };
            let (cmd, executable, version) = match discovered {
                Some(candidate) => verify(candidate.cmd, candidate.version)?,
                None => {
                    self.catalog_discovery(
                        harness,
                        CatalogDiscoverySource::Reader(cmd.to_owned()),
                        deadline,
                        async {
                            let (cmd, version) =
                                crate::adhoc::reader_executable(cmd, harness, deadline).await?;
                            verify(cmd, version)
                        },
                    )
                    .await?
                }
            };
            profile["cmd"] = Value::String(cmd.clone());
            let cache_key = ModelsCacheKey {
                scope: model_source_identity(
                    label,
                    &profile,
                    route.identity.as_deref(),
                    &route.environment,
                )
                .ok_or_else(unverified)?,
                executable,
                version,
                auth_revision: self.inner.catalog_cache.lock_recover().auth_revision,
                workspace: workspace_key,
            };
            let host = self.inner.plugin_host.clone();
            let router = self.clone();
            let harness = harness.to_owned();
            let scope = scope.clone();
            let credential = credential.clone();
            let login_tracked = cache_key.scope.login.is_tracked();
            let read = move |read_deadline: Instant| async move {
                let source = if workspace.is_some() {
                    "workspace"
                } else {
                    "profile"
                };
                crate::logging::record(
                    &router.secret_policy_snapshot(),
                    "catalog.source_read_started",
                    &[("harness", &harness), ("source", source)],
                    None,
                );
                let raw = host
                    .models(profile, route.environment, workspace, read_deadline.into())
                    .await?;
                let mut response =
                    catalog::native_models_response(&harness, &cmd, scope, credential, raw)?;
                if source == "workspace" {
                    response.ttl_ms = WORKSPACE_MODEL_TTL_MS;
                } else if !login_tracked {
                    response.ttl_ms = UNTRACKED_LOGIN_TTL_MS;
                }
                response.validate()?;
                Ok(response)
            };
            self.coalesced_models_read(cache_key, deadline, mode, read)
                .await
        }
        .await;
        match read {
            Ok(response) => Ok(response),
            Err(error) => {
                self.note_harness_auth_required(harness, Some(&error.code))
                    .await;
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

    async fn coalesced_models_read<F>(
        &self,
        key: ModelsCacheKey,
        wait_deadline: Instant,
        mode: ModelReadMode,
        read: impl FnOnce(Instant) -> F,
    ) -> Result<HarnessModelsResponse>
    where
        F: Future<Output = Result<HarnessModelsResponse>> + Send + 'static,
    {
        let mut result = {
            let mut cache = self.inner.catalog_cache.lock_recover();
            let now = now_ms();
            let cached = cache.models.get(&key);
            let tracked = key.scope.login.is_tracked();
            let lead = match mode {
                ModelReadMode::Refresh if tracked => CATALOG_REFRESH_LEAD_MS,
                ModelReadMode::Refresh => UNTRACKED_LOGIN_TTL_MS / 3,
                _ => 0,
            };
            let failure = cache
                .model_reads
                .get(&key)
                .and_then(|read| read.failure.clone());
            if failure.is_none()
                && let Some(hit) = cached.filter(|entry| {
                    quota_fresh(entry.fetched_at_ms, entry.ttl_ms.saturating_sub(lead), now)
                })
            {
                return Ok(hit.clone());
            }
            let stale = cached
                .filter(|_| mode == ModelReadMode::Serve && tracked)
                .filter(|_| {
                    failure
                        .as_ref()
                        .is_none_or(|failure| stale_after_failure(&failure.error))
                })
                .cloned()
                .map(|response| stale_models(response, failure.as_ref()));
            if let Some(failure) = &failure
                && Instant::now() < failure.until
            {
                return stale.ok_or_else(|| failure.error.clone());
            }
            let result = match cache.model_reads.get(&key).and_then(ModelRead::in_flight) {
                Some(in_flight) => in_flight,
                None => {
                    if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                        return Err(catalog_shutting_down());
                    }
                    if !cache.reserve_model_read(&key) {
                        return Err(Error::new(
                            "busy",
                            "model catalogue refresh capacity exhausted",
                        ));
                    }
                    let (publish, result) = watch::channel(None);
                    cache.model_reads.insert(
                        key.clone(),
                        ModelRead {
                            running: Some(result.clone()),
                            failure: failure.clone(),
                        },
                    );
                    let router = self.clone();
                    let read = read(Instant::now() + crate::adhoc::DISCOVERY_BUDGET);
                    // Own the read through native cleanup even if any caller drops
                    // its wait. The plugin host still tracks it for daemon shutdown.
                    tokio::spawn(async move {
                        let result = if router.inner.mode.load(Ordering::SeqCst) == MODE_OPEN {
                            read.await
                        } else {
                            Err(catalog_shutting_down())
                        }
                        .and_then(|response| {
                            if !key.executable.is_current() || !key.scope.login.is_current() {
                                return Err(catalog_source_changed());
                            }
                            Ok(response)
                        });
                        let mut cache = router.inner.catalog_cache.lock_recover();
                        if cache.auth_revision != key.auth_revision {
                            publish.send_replace(Some(Err(catalog_source_changed())));
                            return;
                        }
                        if let Ok(response) = &result {
                            cache.insert_models(key.clone(), response.clone());
                            cache.model_reads.remove(&key);
                        } else if let Err(error) = &result {
                            cache.model_reads.insert(
                                key,
                                ModelRead {
                                    running: None,
                                    failure: Some(DiscoveryFailure::next(
                                        failure.as_ref(),
                                        error.clone(),
                                    )),
                                },
                            );
                        }
                        publish.send_replace(Some(result));
                    });
                    result
                }
            };
            if let Some(stale) = stale {
                return Ok(stale);
            }
            result
        };
        let settled =
            tokio::time::timeout_at(wait_deadline.into(), result.wait_for(Option::is_some))
                .await
                .map_err(|_| crate::adhoc::discovery_timeout())?;
        settled
            .ok()
            .and_then(|result| result.as_ref().cloned())
            .unwrap_or_else(|| {
                Err(Error::new(
                    "unknown",
                    "shared model catalogue reader stopped",
                ))
            })
    }

    /// Provider-supported account quota readback: timestamp, scope and an
    /// explicit supported/unsupported/unknown status, cached per account.
    /// Starts no model turn, performs no inference and never polls per
    /// Agent. Codex reads `account/rateLimits/read` (plus `account/usage/read`
    /// when useful) and Muse reads `usage --json` through a dedicated metadata
    /// reader; Claude reads the CLI OAuth usage endpoint, OpenCode reads the
    /// Go account usage endpoint, and Antigravity reads
    /// `retrieveUserQuotaSummary`. Every reader projects provider quota
    /// signals into `details`: Codex keeps its `rateLimits` shape, the other
    /// four use `windows[]` with `usedPercent`/`resetsAtMs`. Routing and the
    /// catalogue consume all five through one normalized projection
    /// ([`catalog::projected_quota`]).
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
            let candidate = self
                .catalog_profile_command(&label, &harness, profile["cmd"].as_str(), deadline)
                .await?;
            crate::adhoc::write_profile_command_snapshot(&mut profile, candidate)?;
        }
        let retained_quota = HarnessCapabilities::of(&harness).retained_quota;
        let cache_key = if retained_quota.is_some() {
            None
        } else {
            model_source_identity(
                &label,
                &profile,
                route.identity.as_deref(),
                &route.environment,
            )
        };
        let route_verified = cache_key.is_some();
        if let Some(hit) = cache_key.as_ref().and_then(|key| {
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
        let (route_verified, raw) = if let Some(retained) = retained_quota {
            let (verified, failure) = self
                .read_allowance(profile, route, retained, &mut response, deadline.into())
                .await?;
            (verified, failure.map_or(Ok(None), Err))
        } else {
            let raw = self
                .inner
                .plugin_host
                .quota(profile, route.environment, deadline.into())
                .await;
            response.fetched_at_ms = now_ms();
            (route_verified, raw.map(Some))
        };
        let allowance_auth_reason = raw
            .as_ref()
            .err()
            .filter(|error| retained_quota.is_some() && error.code == "auth_required")
            .map(|error| eliot_core::bound_chars(&error.message, 512));
        match raw {
            Ok(None) => {}
            Ok(Some(raw)) => {
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
                } else {
                    response.details = Some(
                        json!({"state":"error","origin":"provider","error":"native quota schema unavailable"}),
                    );
                }
            }
            // A local reader-capacity refusal says nothing about the provider:
            // return it uncached so the next read tries again.
            Err(error) if retained_quota.is_none() && error.code == "busy" => return Err(error),
            Err(error) => {
                self.note_harness_auth_required(&harness, Some(&error.code))
                    .await;
                catalog::quota_read_error(&mut response, error);
            }
        }
        if let (Some(reason), Some(details)) = (allowance_auth_reason, response.details.as_mut()) {
            details["code"] = json!("auth_required");
            response.note = Some(reason);
        }
        request.validate_response(&response)?;
        if let Some(key) = cache_key.filter(|key| key.login.is_current()) {
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
        let mut updates = self.inner.catalog_updates.subscribe();
        let snapshot = {
            let mut cache = self.inner.catalog_cache.lock_recover();
            cache.aggregate_requested_at = Some(Instant::now());
            if let Some(error) = cache.aggregate_retry_error() {
                return Err(error);
            }
            cache.aggregate.clone()
        };
        let key = if snapshot.is_some() {
            self.catalog_key(deadline).await.ok()
        } else {
            None
        };
        let snapshot = match snapshot {
            Some(snapshot)
                if key.as_ref() == Some(&snapshot.key)
                    && snapshot.is_usable()
                    && (request.after.is_some() || !snapshot.refresh_due()) =>
            {
                snapshot
            }
            _ => {
                self.inner.catalog_refresh.notify_one();
                tokio::time::timeout_at(deadline.into(), updates.changed())
                    .await
                    .map_err(|_| crate::adhoc::discovery_timeout())?
                    .map_err(|_| Error::new("unknown", "model catalogue refresh worker stopped"))?;
                let snapshot = updates.borrow_and_update().clone().ok_or_else(|| {
                    Error::new("unknown", "model catalogue refresh produced no outcome")
                })??;
                if key.as_ref() != Some(&snapshot.key)
                    && snapshot.key != self.catalog_key(deadline).await?
                {
                    return Err(catalog_source_changed());
                }
                snapshot
            }
        };
        self.catalog_page(request, &snapshot, deadline).await
    }

    pub(crate) async fn run_catalog_refresh(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<()> {
        let mut tick = tokio::time::interval(CATALOG_REFRESH_INTERVAL);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !*shutdown.borrow() {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tick.tick() => {},
                _ = self.inner.catalog_refresh.notified() => {},
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                self.inner
                    .catalog_updates
                    .send_replace(Some(Err(catalog_shutting_down())));
                continue;
            }
            let auth_revision = {
                let cache = self.inner.catalog_cache.lock_recover();
                if cache.aggregate_retry_error().is_some()
                    || !cache
                        .aggregate_requested_at
                        .is_some_and(|at| at.elapsed() < CATALOG_INTEREST)
                {
                    continue;
                }
                cache.auth_revision
            };
            let result = tokio::select! {
                _ = shutdown.changed() => break,
                result = self.refresh_catalogue() => result,
            };
            if let Err(error) = &result {
                crate::logging::record(
                    &self.secret_policy_snapshot(),
                    "catalog.aggregate_refresh_failed",
                    &[],
                    Some(error),
                );
            }
            let mut cache = self.inner.catalog_cache.lock_recover();
            if cache.auth_revision == auth_revision {
                cache.aggregate_failure = result.as_ref().err().map(|error| {
                    DiscoveryFailure::next(cache.aggregate_failure.as_ref(), error.clone())
                });
            }
            self.inner.catalog_updates.send_replace(Some(result));
        }
        self.inner
            .catalog_updates
            .send_replace(Some(Err(catalog_shutting_down())));
        Ok(())
    }

    async fn refresh_catalogue(&self) -> Result<Arc<CachedAggregate>> {
        let deadline = Instant::now() + crate::adhoc::DISCOVERY_BUDGET;
        let mut key = self.catalog_key(deadline).await?;
        for _ in 0..2 {
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(catalog_shutting_down());
            }
            if let Some(snapshot) = self.cached_catalog(&key)
                && snapshot.is_usable()
                && !snapshot.refresh_due()
            {
                return Ok(snapshot);
            }
            // Reserve time to check the source keys again before publishing.
            let snapshot = self
                .build_catalog(key, deadline - Duration::from_secs(5))
                .await?;
            key = self.catalog_key(deadline).await?;
            let mut cache = self.inner.catalog_cache.lock_recover();
            if key != snapshot.key || cache.auth_revision != snapshot.key.auth_revision {
                continue;
            }
            if self.inner.mode.load(Ordering::SeqCst) != MODE_OPEN {
                return Err(catalog_shutting_down());
            }
            let snapshot = Arc::new(snapshot);
            cache.aggregate = Some(snapshot.clone());
            return Ok(snapshot);
        }
        Err(catalog_source_changed())
    }

    async fn catalog_key(&self, deadline: Instant) -> Result<catalog::CatalogKey> {
        let router = self.clone();
        crate::adhoc::blocking_discovery(deadline, move || {
            let root = &router.inner.data_root;
            let stand_codex_home = router.inner.stand_codex_home.as_deref();
            let auth_revision = router.inner.catalog_cache.lock_recover().auth_revision;
            let settings_revision = crate::config::settings::current_revision(root)?;
            let installations = SUPPORTED_HARNESSES
                .iter()
                .map(|harness| crate::adhoc::installation_fingerprint(harness, deadline))
                .collect::<Vec<_>>();
            let mut adhoc = Vec::new();
            let mut previews = Vec::new();
            for (harness, installation) in SUPPORTED_HARNESSES.iter().zip(&installations) {
                let cmd = installation
                    .as_ref()
                    .and_then(|entry| entry.first_program());
                let scope = cmd
                    .and_then(|cmd| {
                        crate::adhoc::adhoc_snapshot(
                            harness,
                            None,
                            None,
                            Some(cmd.to_owned()),
                            crate::adhoc::AdhocAuth::Skip,
                            None,
                        )
                        .ok()
                    })
                    .and_then(|snapshot| catalog_source_scope("adhoc", snapshot, stand_codex_home));
                adhoc.push(scope);
                if HarnessCapabilities::of(harness).models_preview_auth_integrations {
                    previews.extend(
                        crate::config::profile_names_for_harness(root, harness)
                            .into_iter()
                            .map(|name| {
                                let snapshot = router.resolve_profile_cached(&name).ok();
                                let executable = snapshot.as_ref().and_then(|snapshot| {
                                    crate::adhoc::VersionKey::for_reader(
                                        snapshot["cmd"].as_str()?,
                                        harness,
                                    )
                                });
                                let scope = snapshot.and_then(|snapshot| {
                                    catalog_source_scope(&name, snapshot, stand_codex_home)
                                });
                                PreviewSource {
                                    name,
                                    scope,
                                    executable,
                                }
                            }),
                    );
                }
            }
            Ok(catalog::CatalogKey {
                settings_revision,
                installations,
                adhoc,
                previews,
                auth_revision,
            })
        })
        .await?
    }

    fn cached_catalog(&self, key: &catalog::CatalogKey) -> Option<Arc<catalog::CachedAggregate>> {
        self.inner.catalog_cache.lock_recover().cached_catalog(key)
    }

    async fn build_catalog(
        &self,
        key: catalog::CatalogKey,
        deadline: Instant,
    ) -> Result<catalog::CachedAggregate> {
        let discovery_deadline = tokio::time::Instant::from_std(deadline);
        let previous = self.cached_catalog(&key);
        let mut retained = false;
        let harnesses = Self::harness_inventory(deadline).await?.harnesses;
        let mut models: Vec<ModelsCatalogEntry> = Vec::new();
        let mut failures: Vec<ModelsCatalogFailure> = Vec::new();
        let mut quotas = HashMap::new();
        let mut oldest = now_ms();
        let mut expires = i64::MAX;
        let mut hard_expires_at = None;
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
                self.harness_models_until(models_request, deadline, ModelReadMode::Refresh),
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
                    if let Some(previous) = &previous
                        && stale_after_failure(&error)
                    {
                        retained |= previous.retain_source(&mut models, harness, None);
                    }
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
                apply_source_age(&response, &mut oldest, &mut expires, &mut hard_expires_at);
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
                        self.harness_models_until(scoped, deadline, ModelReadMode::Refresh),
                    )
                    .await;
                    let scoped_response = match scoped_read {
                        Ok(Ok(response)) => response,
                        Ok(Err(error)) => {
                            if let Some(previous) = &previous
                                && stale_after_failure(&error)
                            {
                                retained |=
                                    previous.retain_source(&mut models, harness, Some(&name));
                            }
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
                    apply_source_age(
                        &scoped_response,
                        &mut oldest,
                        &mut expires,
                        &mut hard_expires_at,
                    );
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
                    harness,
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
        models.sort_by(compare_catalog_models);
        let completed = now_ms();
        if let Some(previous) = previous.as_ref().filter(|_| retained) {
            oldest = oldest.min(previous.fetched_at_ms);
            expires = expires.min(
                previous
                    .fetched_at_ms
                    .saturating_add(previous.ttl_ms as i64),
            );
        }
        if expires == i64::MAX {
            oldest = completed;
        }
        let ttl = if failures.is_empty() {
            CATALOG_TTL_MS
        } else {
            PARTIAL_AGGREGATE_TTL_MS
        };
        let retry_delay = previous
            .filter(|entry| {
                !failures.is_empty()
                    && entry
                        .failures
                        .iter()
                        .map(|failure| (&failure.harness, &failure.code))
                        .eq(failures
                            .iter()
                            .map(|failure| (&failure.harness, &failure.code)))
            })
            .map_or(DISCOVERY_RETRY_INTERVAL, |entry| {
                (entry.retry_delay * 2).min(DISCOVERY_RETRY_MAX)
            });
        let expires = expires.min(completed.saturating_add(ttl as i64));
        Ok(catalog::CachedAggregate {
            key,
            revision: ulid::Ulid::new().to_string(),
            harnesses,
            models,
            quotas,
            failures,
            fetched_at_ms: oldest,
            ttl_ms: expires.saturating_sub(oldest).max(1) as u64,
            completed_at: Instant::now(),
            retry_delay,
            hard_expires_at,
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

        if !snapshot.is_usable() {
            return Err(Error::new(
                "unavailable",
                "model catalogue login freshness expired during refresh",
            ));
        }
        let query = request.query.as_deref().unwrap_or("");
        let stale = !quota_fresh(snapshot.fetched_at_ms, snapshot.ttl_ms, now_ms());
        let limit = request.effective_limit();
        let mut matched = snapshot
            .models
            .iter()
            .filter(|entry| catalog_matches(entry, query));
        let revision = snapshot.revision.clone();
        if let Some(cursor) = request.after.as_deref()
            && !cursor.split_once(':').is_some_and(|(prefix, row)| {
                prefix == revision && matched.any(|entry| entry.cursor() == row)
            })
        {
            return Err(Error::new(
                "invalid",
                "catalogue cursor is no longer in the listing; restart from the first page",
            ));
        }
        let mut models: Vec<_> = matched.by_ref().take(limit).cloned().collect();
        let next = if matched.next().is_some() {
            models
                .last()
                .map(|model| format!("{revision}:{}", model.cursor()))
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
                .map_err(|_| crate::adhoc::discovery_timeout())?
                .provider_holds_for_scopes(&exact_scopes)?;
            let now = now_ms();
            for model in models.iter_mut().filter(|model| model.profile.is_none()) {
                let held = scopes
                    .get(&model.harness)
                    .map(|scope| holds.contains_key(scope));
                let quota = (!stale)
                    .then(|| snapshot.quotas.get(&model.harness))
                    .flatten();
                model.availability = availability(held, quota, &model.id, now).to_owned();
            }
        }
        let response = ModelsCatalogResponse {
            revision: Some(revision),
            harnesses: snapshot.harnesses.clone(),
            models,
            next,
            fetched_at_ms: snapshot.fetched_at_ms,
            ttl_ms: snapshot.ttl_ms,
            stale,
            failures: snapshot.failures.clone(),
        };
        request.validate_response(&response)?;
        Ok(response)
    }
}

/// Maximum cached scopes per read kind.
const MAX_CACHED_SCOPES: usize = 32;
const CATALOG_REFRESH_INTERVAL: Duration = Duration::from_secs(15);
const CATALOG_INTEREST: Duration = Duration::from_secs(5 * 60);
// Allow a complete discovery pass and one scheduler tick before expiry.
const CATALOG_REFRESH_LEAD_MS: u64 = 105_000;

/// Serve cached catalogue/quota entries for this long after the native read.
pub(crate) const CATALOG_TTL_MS: u64 = HARNESS_READ_TTL_MS;
const UNTRACKED_LOGIN_TTL_MS: u64 = 90_000;
const WORKSPACE_MODEL_TTL_MS: u64 = 30_000;

const PARTIAL_AGGREGATE_TTL_MS: u64 = DISCOVERY_RETRY_INTERVAL.as_millis() as u64;

fn catalog_shutting_down() -> Error {
    Error::new(
        "unavailable",
        "daemon is shutting down and rejects new discovery",
    )
}

fn catalog_source_changed() -> Error {
    Error::new(
        "unavailable",
        "model catalogue source changed during refresh",
    )
}

fn stale_after_failure(error: &Error) -> bool {
    matches!(
        error.code.as_str(),
        "unavailable" | "unsupported" | "timeout"
    )
}

fn stale_models(
    mut response: HarnessModelsResponse,
    failure: Option<&DiscoveryFailure>,
) -> HarnessModelsResponse {
    response.stale = true;
    response.refresh_failure =
        failure.map(|failure| ModelsCatalogFailure::from_error(&response.harness, &failure.error));
    response
}

fn apply_source_age(
    response: &HarnessModelsResponse,
    oldest: &mut i64,
    expires: &mut i64,
    hard_expires_at: &mut Option<i64>,
) {
    *oldest = (*oldest).min(response.fetched_at_ms);
    let source_expires = response
        .fetched_at_ms
        .saturating_add(response.ttl_ms as i64);
    *expires = (*expires).min(source_expires);
    if response.ttl_ms == UNTRACKED_LOGIN_TTL_MS {
        *hard_expires_at =
            Some(hard_expires_at.map_or(source_expires, |previous| previous.min(source_expires)));
    }
}

fn catalog_source_scope(
    label: &str,
    mut snapshot: Value,
    stand_codex_home: Option<&std::path::Path>,
) -> Option<ModelSourceIdentity> {
    crate::config::capture_native_store_inputs(&mut snapshot, stand_codex_home).ok()?;
    let route = crate::router::plugin_host::metadata_route(&snapshot).ok()?;
    model_source_identity(
        label,
        &snapshot,
        route.identity.as_deref(),
        &route.environment,
    )
}

fn model_source_identity(
    label: &str,
    profile: &Value,
    verified: Option<&str>,
    environment: &Value,
) -> Option<ModelSourceIdentity> {
    let scope = canonical_cache_identity(label, profile, verified, environment)?;
    let login = NativeLoginRevision::read(profile["harness"].as_str()?, environment);
    Some(ModelSourceIdentity { scope, login })
}

#[derive(PartialEq, Eq, Clone, Hash)]
struct ModelSourceIdentity {
    scope: String,
    login: NativeLoginRevision,
}

#[derive(PartialEq, Eq, Clone, Hash)]
enum NativeLoginRevision {
    Untracked,
    Files(Vec<(std::path::PathBuf, Option<crate::adhoc::VersionKey>)>),
    Keyring {
        harness: String,
        modified: Option<u64>,
    },
}

impl NativeLoginRevision {
    fn is_tracked(&self) -> bool {
        match self {
            // The first declared file is the sign-in store. Later entries
            // can also affect the catalogue, but do not establish login.
            Self::Files(files) => files
                .first()
                .is_some_and(|(_, revision)| revision.is_some()),
            Self::Keyring { modified, .. } => modified.is_some(),
            Self::Untracked => false,
        }
    }

    fn read(harness: &str, environment: &Value) -> Self {
        use std::path::PathBuf;
        let env_path = |name: &str| {
            environment
                .as_object()?
                .iter()
                .find(|(key, _)| {
                    key.as_str() == name || (cfg!(windows) && key.eq_ignore_ascii_case(name))
                })
                .and_then(|(_, value)| value.as_str())
                .filter(|value| !value.is_empty())
                .map(PathBuf::from)
        };
        let home = || {
            env_path(if cfg!(windows) { "USERPROFILE" } else { "HOME" })
                .or_else(|| eliot_plugin::quota::home().ok())
        };
        match HarnessCapabilities::of(harness).model_login_source {
            None => Self::Untracked,
            Some(ModelLoginSource::Keyring(read_revision)) => Self::Keyring {
                harness: harness.to_owned(),
                modified: read_revision(),
            },
            Some(ModelLoginSource::Files {
                root_env,
                default_root,
                files,
            }) => {
                let Some(root) = env_path(root_env)
                    .or_else(|| home().map(|home| home.join(default_root)))
                    .filter(|root| root.is_absolute())
                else {
                    return Self::Untracked;
                };
                Self::Files(
                    files
                        .iter()
                        .map(|name| {
                            let path = root.join(name);
                            let revision = Self::file_revision(&path);
                            (path, revision)
                        })
                        .collect(),
                )
            }
        }
    }

    fn file_revision(path: &std::path::Path) -> Option<crate::adhoc::VersionKey> {
        crate::adhoc::VersionKey::read(path.to_str()?, None)
    }

    fn is_current(&self) -> bool {
        match self {
            Self::Untracked => true,
            Self::Files(files) => files
                .iter()
                .all(|(path, revision)| &Self::file_revision(path) == revision),
            Self::Keyring { harness, modified } => {
                matches!(HarnessCapabilities::of(harness).model_login_source,
                    Some(ModelLoginSource::Keyring(read_revision)) if read_revision() == *modified)
            }
        }
    }
}

fn compare_catalog_models(
    left: &ModelsCatalogEntry,
    right: &ModelsCatalogEntry,
) -> std::cmp::Ordering {
    (&left.harness, &left.id, &left.profile).cmp(&(&right.harness, &right.id, &right.profile))
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
        response.note = Some(match error.code.as_str() {
            "auth_required" => "The vendor CLI requires sign-in.".to_owned(),
            "quota_unavailable" => eliot_core::bound_chars(&error.message, 512),
            _ => "Read-only native/provider quota unavailable; no inference or credential refresh attempted".to_owned(),
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
            } else if profile_label == "adhoc" {
                scope.push_str(" auth:available");
                credential = "auth:available".to_owned();
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

/// Bounded per-scope cache for catalogue and quota readback. Concurrent
/// model cache misses for the same verified source share one native read.
#[derive(Default)]
pub(crate) struct CatalogCache {
    models: HashMap<ModelsCacheKey, HarnessModelsResponse>,
    model_reads: HashMap<ModelsCacheKey, ModelRead>,
    discovery: HashMap<(String, CatalogDiscoverySource), DiscoveryGate>,
    quotas: HashMap<ModelSourceIdentity, HarnessQuotaResponse>,
    aggregate: Option<Arc<CachedAggregate>>,
    aggregate_failure: Option<DiscoveryFailure>,
    aggregate_requested_at: Option<Instant>,
    auth_revision: u64,
}

#[derive(Hash, PartialEq, Eq)]
enum CatalogDiscoverySource {
    Installed,
    Ranked,
    Profile(String, Option<String>),
    Reader(String),
}

type DiscoveryGate = Arc<tokio::sync::Mutex<Option<DiscoveryFailure>>>;

pub(super) type AggregateUpdate = Option<Result<Arc<CachedAggregate>>>;

struct ModelRead {
    running: Option<watch::Receiver<Option<Result<HarnessModelsResponse>>>>,
    failure: Option<DiscoveryFailure>,
}

impl ModelRead {
    fn in_flight(&self) -> Option<watch::Receiver<Option<Result<HarnessModelsResponse>>>> {
        self.running
            .as_ref()
            .filter(|read| read.has_changed().is_ok())
            .cloned()
    }
}

/// One sorted catalogue snapshot shared across pages. Holds and quota
/// freshness are evaluated when projecting each requested page.
pub(super) struct CachedAggregate {
    key: CatalogKey,
    revision: String,
    harnesses: Vec<HarnessEntry>,
    models: Vec<ModelsCatalogEntry>,
    quotas: HashMap<String, ScopedQuota>,
    failures: Vec<ModelsCatalogFailure>,
    fetched_at_ms: i64,
    ttl_ms: u64,
    completed_at: Instant,
    retry_delay: Duration,
    hard_expires_at: Option<i64>,
}

impl CachedAggregate {
    fn is_usable(&self) -> bool {
        self.hard_expires_at
            .is_none_or(|expires| now_ms() < expires)
    }

    fn refresh_due(&self) -> bool {
        let now = now_ms();
        if self.failures.is_empty() {
            !quota_fresh(
                self.fetched_at_ms,
                self.ttl_ms
                    .saturating_sub((self.ttl_ms / 3).min(CATALOG_REFRESH_LEAD_MS)),
                now,
            )
        } else {
            self.completed_at.elapsed() >= self.retry_delay
        }
    }

    fn retain_source(
        &self,
        models: &mut Vec<ModelsCatalogEntry>,
        harness: &str,
        profile: Option<&str>,
    ) -> bool {
        let scope = match profile {
            Some(name) => self
                .key
                .previews
                .iter()
                .find(|preview| preview.name == name)
                .and_then(|preview| preview.scope.as_ref()),
            None => SUPPORTED_HARNESSES
                .iter()
                .position(|name| *name == harness)
                .and_then(|index| self.key.adhoc[index].as_ref()),
        };
        if !scope.is_some_and(|scope| scope.login.is_tracked()) {
            return false;
        }
        let count = models.len();
        models.extend(
            self.models
                .iter()
                .filter(|model| model.harness == harness && model.profile.as_deref() == profile)
                .cloned(),
        );
        models.len() != count
    }
}

#[derive(PartialEq, Eq)]
struct CatalogKey {
    settings_revision: Option<String>,
    installations: Vec<Option<crate::adhoc::InstallationFingerprint>>,
    adhoc: Vec<Option<ModelSourceIdentity>>,
    previews: Vec<PreviewSource>,
    auth_revision: u64,
}

#[derive(PartialEq, Eq)]
struct PreviewSource {
    name: String,
    scope: Option<ModelSourceIdentity>,
    executable: Option<crate::adhoc::VersionKey>,
}

#[derive(Clone, Hash, PartialEq, Eq)]
struct ModelsCacheKey {
    scope: ModelSourceIdentity,
    executable: crate::adhoc::VersionKey,
    version: String,
    auth_revision: u64,
    workspace: Option<std::path::PathBuf>,
}

impl CatalogCache {
    fn aggregate_retry_error(&self) -> Option<Error> {
        self.aggregate_failure
            .as_ref()
            .filter(|failure| Instant::now() < failure.until)
            .map(|failure| failure.error.clone())
    }

    pub(super) fn invalidate_auth(&mut self) {
        *self = Self {
            auth_revision: self.auth_revision + 1,
            aggregate_requested_at: self.aggregate_requested_at,
            discovery: std::mem::take(&mut self.discovery),
            ..Self::default()
        };
    }

    fn discovery_gate(&mut self, key: (String, CatalogDiscoverySource)) -> Result<DiscoveryGate> {
        if let Some(gate) = self.discovery.get(&key) {
            return Ok(gate.clone());
        }
        if self.discovery.len() >= MAX_CACHED_SCOPES {
            self.discovery.retain(|_, gate| {
                Arc::strong_count(gate) > 1
                    || gate.try_lock().is_ok_and(|failure| {
                        failure
                            .as_ref()
                            .is_some_and(|failure| Instant::now() < failure.until)
                    })
            });
            if self.discovery.len() >= MAX_CACHED_SCOPES {
                return Err(Error::new(
                    "busy",
                    "catalogue executable discovery capacity exhausted",
                ));
            }
        }
        let gate = Arc::new(tokio::sync::Mutex::new(None));
        self.discovery.insert(key, gate.clone());
        Ok(gate)
    }

    fn cached_catalog(&self, key: &CatalogKey) -> Option<Arc<CachedAggregate>> {
        self.aggregate
            .as_ref()
            .filter(|entry| &entry.key == key)
            .cloned()
    }

    fn reserve_model_read(&mut self, key: &ModelsCacheKey) -> bool {
        if self.model_reads.contains_key(key) || self.model_reads.len() < MAX_CACHED_SCOPES {
            return true;
        }
        let evict = self
            .model_reads
            .iter()
            .filter(|(_, read)| {
                read.in_flight().is_none()
                    && read
                        .failure
                        .as_ref()
                        .is_none_or(|failure| Instant::now() >= failure.until)
            })
            .min_by_key(|(_, read)| read.failure.as_ref().map(|failure| failure.until))
            .map(|(key, _)| key.clone());
        evict.is_some_and(|key| self.model_reads.remove(&key).is_some())
    }

    fn insert_models(&mut self, key: ModelsCacheKey, entry: HarnessModelsResponse) {
        evict_models(&mut self.models, entry.fetched_at_ms);
        self.models.insert(key, entry);
    }

    fn get_quota(&self, key: &ModelSourceIdentity, now: i64) -> Option<HarnessQuotaResponse> {
        self.quotas
            .get(key)
            .filter(|entry| quota_fresh(entry.fetched_at_ms, entry.ttl_ms, now))
            .cloned()
    }

    /// Insert under the private canonical identity (`key`); the response
    /// keeps its public display `scope`. The two are separate on purpose:
    /// display is for operators, the key is for cache correctness.
    fn insert_quota(&mut self, key: ModelSourceIdentity, entry: HarnessQuotaResponse) {
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
        stale: false,
        refresh_failure: None,
        credential,
        configure: eliot_core::api::ConfigureSupport::for_harness(harness),
    }
}

fn native_models_response(
    harness: &str,
    cmd: &str,
    scope: String,
    credential: String,
    mut raw: ModelCatalogue,
) -> Result<HarnessModelsResponse> {
    raw.models.retain(|model| !model.id.trim().is_empty());
    for model in &mut raw.models {
        model.display_name = model
            .display_name
            .take()
            .filter(|name| eliot_core::api::harness::native_model_display_name(name).is_some());
        model.source = None;
    }
    let entries = raw.models;
    if let Some(documented) = HarnessCapabilities::of(harness).documented_models
        && entries.is_empty()
    {
        return Err(Error::new(
            "source_integrity",
            documented.empty_native_inventory,
        ));
    }
    let default = raw.default.filter(|default| !default.trim().is_empty());
    // Adapters own vendor-specific view notes; the daemon supplies only the
    // shared default presentation when an adapter declares no callback.
    let (catalogue, note) = HarnessCapabilities::of(harness)
        .model_catalogue_presentation
        .map(|present| {
            present(
                cmd,
                raw.provenance.as_str(),
                entries.is_empty(),
                default.as_deref(),
            )
        })
        .unwrap_or(("native", None));
    Ok(HarnessModelsResponse {
        harness: harness.to_owned(),
        catalogue: catalogue.to_owned(),
        models: entries,
        default,
        note,
        scope,
        fetched_at_ms: now_ms(),
        ttl_ms: CATALOG_TTL_MS,
        stale: false,
        refresh_failure: None,
        credential,
        configure: eliot_core::api::ConfigureSupport::for_harness(harness),
    })
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
fn coalesced_preview_failure(
    harness: &str,
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
        harness,
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

pub(crate) use eliot_core::api::harness::QuotaMapping;

#[derive(Clone, Debug, Default, PartialEq)]
pub(crate) struct QuotaHeadroom {
    pub max_used: Option<f64>,
    pub weekly_used: Option<f64>,
}

/// Scoped native evidence and the time used to settle a retained quota hold.
pub(crate) struct QuotaProjection {
    pub observed_at_ms: i64,
    pub exhausted: bool,
    pub available: bool,
    pub mapping: QuotaMapping,
    pub headroom: QuotaHeadroom,
}

#[derive(Clone, Debug)]
pub(crate) struct ScopedQuota {
    pub(crate) response: HarnessQuotaResponse,
    pub(crate) route_verified: bool,
}

/// Routing and catalogue availability share a single verified, fresh quota projection.
pub(crate) fn projected_quota(
    quota: &ScopedQuota,
    model: Option<&str>,
    now: i64,
) -> Option<QuotaProjection> {
    let response = &quota.response;
    if !quota.route_verified
        || response.status != eliot_core::api::HARNESS_QUOTA_SUPPORTED
        || !quota_fresh(response.fetched_at_ms, response.ttl_ms, now)
    {
        return None;
    }
    let details = response
        .details
        .as_ref()
        .filter(|details| details.is_object())?;
    let readback = HarnessCapabilities::of(&response.harness).quota_readback?;
    let evidence = (readback.evidence)(details, model)?;
    let (exhausted, available, mapping, headroom) = match evidence {
        QuotaEvidence::Windows {
            applicable,
            complete,
        } => {
            let exhausted = applicable
                .iter()
                .any(|(window, _)| window_exhausted(window, now));
            let mapping = applicable
                .iter()
                .filter(|(window, _)| window_exhausted(window, now) == exhausted)
                .map(|(_, mapping)| *mapping)
                .min_by_key(|mapping| *mapping != QuotaMapping::ExactId)
                .unwrap_or(QuotaMapping::Unmapped);
            let available = complete
                && !applicable.is_empty()
                && applicable
                    .iter()
                    .all(|(window, _)| window_has_headroom(window, now));
            let headroom = if complete {
                quota_headroom(applicable.iter().map(|(window, _)| window), now)
            } else {
                QuotaHeadroom::default()
            };
            (exhausted, available, mapping, headroom)
        }
        QuotaEvidence::Tiered(windows) => {
            // The primary account bucket and explicit native limit signals govern
            // exhaustion. Secondary and per-model buckets only affect headroom.
            let exhausted = windows.spend_control_reached
                || windows.limit_reached
                || windows
                    .remaining_percent
                    .is_some_and(|remaining| remaining <= 0)
                || !windows.credits_present && window_exhausted(&windows.account[0], now);
            let available = !exhausted
                && windows.ordinary_usage_allowed
                && windows
                    .account
                    .iter()
                    .all(|window| window_has_headroom(window, now))
                && (!windows.model_limits_reported
                    || windows.model.as_ref().is_some_and(|pair| {
                        pair.iter().all(|window| window_has_headroom(window, now))
                    }));
            let mapping = if !exhausted && windows.model.is_some() {
                QuotaMapping::ExactId
            } else {
                QuotaMapping::Group("primary")
            };
            let headroom = quota_headroom(
                windows.account.iter().chain(windows.model.iter().flatten()),
                now,
            );
            (exhausted, available, mapping, headroom)
        }
    };
    Some(QuotaProjection {
        observed_at_ms: response.fetched_at_ms,
        exhausted,
        available,
        mapping,
        headroom,
    })
}

fn window_exhausted(window: &QuotaWindowFacts, now: i64) -> bool {
    window.used_percent.is_some_and(|used| used >= 100.0)
        && window.reset_ms.is_none_or(|reset| reset > now)
}

fn window_has_headroom(window: &QuotaWindowFacts, now: i64) -> bool {
    window.used_percent.is_some_and(|used| used < 100.0)
        && window.reset_ms.is_some_and(|reset| reset > now)
}

fn quota_headroom<'a>(
    windows: impl Iterator<Item = &'a QuotaWindowFacts>,
    now: i64,
) -> QuotaHeadroom {
    let mut headroom = QuotaHeadroom::default();
    for window in windows {
        let Some(used) = window
            .used_percent
            .filter(|_| window.reset_ms.is_none_or(|reset| reset > now))
        else {
            continue;
        };
        headroom.max_used = Some(headroom.max_used.map_or(used, |prior| prior.max(used)));
        if window.weekly {
            headroom.weekly_used = Some(headroom.weekly_used.map_or(used, |prior| prior.max(used)));
        }
    }
    headroom
}

fn quota_usage_windows(
    response: &HarnessQuotaResponse,
    now: i64,
) -> Vec<eliot_core::api::harness::QuotaWindowBounds> {
    response
        .details
        .as_ref()
        .zip(HarnessCapabilities::of(&response.harness).quota_readback)
        .map_or_else(Vec::new, |(details, readback)| {
            (readback.usage_windows)(details, now)
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

fn evict_models(map: &mut HashMap<ModelsCacheKey, HarnessModelsResponse>, now: i64) {
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

fn evict_quotas(map: &mut HashMap<ModelSourceIdentity, HarnessQuotaResponse>, now: i64) {
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
