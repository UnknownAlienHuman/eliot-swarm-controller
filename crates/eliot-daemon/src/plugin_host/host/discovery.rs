//! A bounded read-only plugin instance, independent of every Agent identity.
use super::reader::{READER_CLOSE_BUDGET, ReaderTransport};
use super::{
    AgentKey, PluginHost, build_process_environment, metadata_secret_snapshot, redact_native_error,
    require_proxy_capability, signal_progress,
};
use eliot_core::{
    Error, Result, SecretPolicy, StderrSource,
    api::{DiscoverPage, HarnessCapabilities, harness::NativeArchiveBinding, is_supported_harness},
};
use eliot_plugin::protocol::{
    ModelCatalogue, ModelListParams, ModelListPart, NativeArchiveParams, NativeTurnEvidenceRequest,
    ProfileStoreParams, ProfileStoreResult,
};
use eliot_plugin::sync::LockExt;
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::{
    sync::{OwnedSemaphorePermit, watch},
    time::Instant,
};
use ulid::Ulid;

pub(super) const MAX_READERS: usize = 4;
// Launch preparation is independent of interactive discovery. Bound active
// read-only helpers while admitted commands wait fairly instead of refusing.
pub(super) const MAX_LAUNCH_READERS: usize = 32;
const READER_DEADLINE: Duration = Duration::from_secs(25);
/// How long a catalogue or quota read waits for an interactive reader slot.
const DISCOVERY_SLOT_WAIT: Duration = Duration::from_secs(8);
const MAX_CURSOR_BYTES: usize = 16 * 1024;
const MAX_PAGE_BYTES: usize = 900 * 1024;

/// The interactive readers are at their in-flight bound.
fn readers_busy() -> Error {
    Error::new("busy", "metadata_readers in-flight bound reached (limit=4)")
        .with_where("router.dispatch.discovery_readers")
        .with_origin(
            eliot_core::FailureOrigin::Swarm,
            "discovery readers at their in-flight bound",
        )
}

pub(super) struct Reader {
    _slot: OwnedSemaphorePermit,
    stop: watch::Sender<Option<Instant>>,
    done: watch::Receiver<ReaderCleanup>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReaderCleanup {
    Pending,
    Confirmed,
    Unknown,
}

struct MetadataRead<T> {
    result: Result<T>,
    cleanup: ReaderCleanup,
}

impl PluginHost {
    pub(crate) async fn historical_turn_evidence(
        &self,
        turn: &eliot_store::HistoricalTurn,
    ) -> Result<Option<eliot_core::api::TurnResolveStatus>> {
        let request = NativeTurnEvidenceRequest {
            native_scope: turn.native_scope.clone(),
            native_ref: turn.native_ref.clone(),
            turn_id: turn.turn_id.clone(),
            target_environment: history_environment(&turn.profile),
            profile: turn.profile.clone(),
        };
        let (id, stop, done) = self.register_metadata_reader()?;
        let host = self.clone();
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let completion = host.read_historical_turn(request, stop).await;
            host.finish_metadata_reader(&id, done, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "Native history reader stopped"))?
    }

    async fn read_historical_turn(
        &self,
        request: NativeTurnEvidenceRequest,
        mut stop: watch::Receiver<Option<Instant>>,
    ) -> MetadataRead<Option<eliot_core::api::TurnResolveStatus>> {
        let method = "native.turn_evidence";
        if stop.borrow().is_some() {
            return MetadataRead {
                result: Ok(None),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        let harness = request.profile["harness"].as_str().unwrap_or_default();
        let opened = self
            .open_reader_with_cleanup(
                &AgentKey {
                    id: "turn-evidence".to_owned(),
                    generation: 0,
                },
                harness,
                &request.profile,
                &request.target_environment,
                None,
            )
            .await;
        let mut reader = match opened.result {
            Ok(reader) => reader,
            Err(error) => return failed_metadata_open(error, opened.cleanup_confirmed, method),
        };
        let until = Instant::now() + READER_DEADLINE;
        let read = async {
            reader.initialize(harness, until).await?;
            let payload = serde_json::to_value(&request)
                .map_err(|_| Error::new("invalid", "Could not encode turn history binding"))?;
            let reply = reader.client.call(method, payload, until).await?;
            reject_reserved_metadata(&reply, &reader.metadata_secrets, method)?;
            if reply["native_scope"].as_str() != Some(request.native_scope.as_str())
                || reply["native_ref"].as_str() != Some(request.native_ref.as_str())
                || reply["turn_id"].as_str() != Some(request.turn_id.as_str())
            {
                return Err(Error::new(
                    "source_integrity",
                    "Native history reply changed its binding",
                ));
            }
            let outcome = reply.get("outcome").ok_or_else(|| {
                Error::new("source_integrity", "Native history omitted its outcome")
            })?;
            serde_json::from_value(outcome.clone())
                .map_err(|_| Error::new("source_integrity", "Native history outcome is malformed"))
        };
        let result = tokio::select! {
            result = tokio::time::timeout_at(until, read) =>
                result.unwrap_or_else(|_| Err(Error::new("unavailable", "Native history reader exceeded its deadline"))),
            _ = stop.changed() => Err(Error::new("cancelled", "Native history reader was cancelled")),
        };
        let closure = reader
            .close_until(self, None, metadata_reader_close_deadline(&stop))
            .await;
        MetadataRead {
            result: require_reader_cleanup(result, closure.closed, method),
            cleanup: cleanup_state(closure.closed),
        }
    }

    pub(crate) fn close_discovery_admission(&self) {
        // Shares the registration lock, so shutdown cannot miss a reader that
        // passed the Router check immediately before its admission mode changed.
        let _readers = self.state.readers.lock_recover();
        self.state.discovery_closed.store(true, Ordering::SeqCst);
        self.state.reader_slots.close();
        self.state.launch_reader_slots.close();
    }

    pub(crate) fn reader_count(&self) -> usize {
        let discoveries = metadata_reader_count(&self.state.readers);
        discoveries + self.observer_count()
    }

    pub(crate) async fn cancel_discoveries(&self) -> bool {
        let close_until = Instant::now() + READER_CLOSE_BUDGET;
        let readers = {
            let readers = self.state.readers.lock_recover();
            self.state.discovery_closed.store(true, Ordering::SeqCst);
            self.state.reader_slots.close();
            self.state.launch_reader_slots.close();
            readers
                .values()
                .map(|reader| (reader.stop.clone(), reader.done.clone()))
                .collect()
        };
        cancel_metadata_readers(readers, close_until).await
    }

    pub(crate) async fn discover(
        &self,
        profile: Value,
        native_scope: Option<String>,
        cursor: Option<String>,
        archived: bool,
        limit: usize,
    ) -> Result<Value> {
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new("invalid", "discovery limit must be 1..=1000"));
        }
        for (name, value, max) in [
            ("scope", native_scope.as_deref(), 512),
            ("cursor", cursor.as_deref(), MAX_CURSOR_BYTES),
        ] {
            if value.is_some_and(|value| {
                value.is_empty() || value.len() > max || value.chars().any(char::is_control)
            }) {
                return Err(Error::new("invalid", format!("invalid discovery {name}")));
            }
        }
        if !HarnessCapabilities::of(profile["harness"].as_str().unwrap_or_default())
            .native_discovery
        {
            return Err(Error::new(
                "unsupported",
                "profile does not support discovery",
            ));
        }
        // Resolved values are transported only to this owned process. No Agent
        // credentials or Store rows are created by discovery.
        let environment = Value::Object(build_process_environment(&profile)?);
        let (id, stop_rx, done_tx) = self.register_metadata_reader()?;
        let host = self.clone();
        // The owner task survives a disconnected RPC caller long enough to close
        // and reap its scoped process. Daemon force shutdown can cancel it.
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let completion = host
                .read_subject_page(
                    profile,
                    environment,
                    native_scope,
                    cursor,
                    archived,
                    limit,
                    stop_rx,
                )
                .await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "discovery reader stopped"))?
    }

    /// Archive one terminal native session through the harness's own
    /// documented method (Codex `thread/archive`, OpenCode
    /// `DELETE /api/session/{id}` on the retained per-Agent store). A
    /// dedicated metadata reader calls `native.archive` with no turn and no
    /// inference. Every other harness stays `unsupported` without spawning
    /// a reader, and the caller keeps the session. Never deletes raw native
    /// store files.
    pub(crate) async fn archive_native_session(
        &self,
        profile: Value,
        native_ref: &str,
        agent: &eliot_core::Agent,
    ) -> Result<()> {
        if HarnessCapabilities::of(profile["harness"].as_str().unwrap_or_default())
            .native_archive
            .is_none()
        {
            return Err(Error::new(
                "unsupported",
                "native archive needs a codex or opencode profile",
            ));
        }
        if native_ref.is_empty()
            || native_ref.len() > 512
            || native_ref.chars().any(char::is_control)
        {
            return Err(Error::new("invalid", "native session reference is invalid"));
        }
        let environment = Value::Object(build_process_environment(&profile)?);
        let (id, stop_rx, done_tx) = self.register_metadata_reader()?;
        let host = self.clone();
        let native_ref = native_ref.to_owned();
        let agent = agent.clone();
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let completion = host
                .read_archive(profile, environment, native_ref, &agent, stop_rx)
                .await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "archive reader stopped"))?
    }

    /// Reuse the native adapter's exact start-plan validator in a tracked
    /// reader. This lane cannot publish Agent facts or dispatch a native start.
    pub(crate) async fn validate_retained_start(
        &self,
        mut agent: eliot_core::Agent,
        source_state: Option<Value>,
    ) -> Result<()> {
        let is_resume = source_state.is_some();
        let caps = HarnessCapabilities::of(&agent.harness);
        let (overlay, rules_receipt) = {
            let store = self.state.store.lock().await;
            let overlay = if is_resume && caps.resume_config {
                store.get_agent_config_current(&agent.id, agent.generation)?
            } else {
                None
            };
            let rules = if caps.owner_rules_delivery
                != eliot_core::api::OwnerRulesDeliveryMethod::FirstInput
            {
                store.get_agent_owner_rules_delivery(&agent.id)?
            } else {
                None
            };
            (overlay, rules)
        };
        let owner_rules = rules_receipt
            .map(|receipt| {
                let policy = SecretPolicy::new(self.state.secrets.lock_recover().iter());
                crate::config::settings::owner_rules_for_delivery(
                    &self.state.data_root,
                    &receipt.digest,
                    &policy,
                )
            })
            .transpose()?;
        if is_resume {
            agent.generation = agent.generation.checked_add(1).ok_or_else(|| {
                Error::new("unknown", "no successor generation for readiness probe")
            })?;
        }
        let (environment, mcp) = super::build_target_environment(
            &self.state.data_root,
            self.state.stand_store_root.as_deref(),
            &agent,
            &Ulid::new().to_string(),
        )?;
        let mut params =
            eliot_core::AgentStartParams::new(&agent.id, agent.generation, Ulid::new().to_string());
        params.profile = Some(super::start_wire_profile(&agent));
        params.workspace = agent.workspace.clone();
        params.target_environment = Some(environment.clone());
        params.mcp_environment = if caps.data_root_only_mcp {
            Some(super::data_root_only_mcp_environment(&mcp)?)
        } else {
            Some(mcp)
        };
        params.agent_mcp_url = self.state.agent_mcp_url.clone();
        params.owner_rules = owner_rules;
        params.pass_agent_identity_to_shell = Some(
            agent
                .profile_snapshot
                .get("pass_agent_identity_to_shell")
                .and_then(Value::as_bool)
                .unwrap_or(true),
        );
        if is_resume {
            params.resume_from =
                Some(json!({"native_scope":agent.native_scope,"native_ref":agent.native_ref}));
            params.source_state = source_state;
        }
        if let Some(overlay) = overlay {
            params.model_override = overlay.model;
            params.effort_override = if caps.effort { overlay.effort } else { None };
            params.config_revision = Some(overlay.revision);
        }
        params.commit_roots = super::commit_roots_for(&agent, &self.state.store).await;
        let (id, stop_rx, done_tx) = self.wait_metadata_reader().await?;
        let host = self.clone();
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let completion = host
                .read_start_plan(agent.harness, environment, params, is_resume, stop_rx)
                .await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unknown", "start-plan reader stopped"))?
    }

    async fn read_start_plan(
        &self,
        harness: String,
        environment: Value,
        mut params: eliot_core::AgentStartParams,
        is_resume: bool,
        mut stop: watch::Receiver<Option<Instant>>,
    ) -> MetadataRead<()> {
        let method = "agent.validate_start";
        if stop.borrow().is_some() {
            return MetadataRead {
                result: Err(Error::new(
                    "unknown",
                    "start-plan reader cancelled before opening",
                )),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        let profile = params.profile.clone().unwrap_or_default();
        let opened = self
            .open_reader_with_cleanup(
                &AgentKey {
                    id: "update-plan".to_owned(),
                    generation: 0,
                },
                &harness,
                &profile,
                &environment,
                None,
            )
            .await;
        let mut reader = match opened.result {
            Ok(reader) => reader,
            Err(error) => return failed_metadata_open(error, opened.cleanup_confirmed, method),
        };
        let until = Instant::now() + READER_DEADLINE;
        let result = tokio::select! {
            result = async {
                let manifest = reader.initialize(&harness, until).await?;
                require_proxy_capability(&profile, &manifest)?;
                let verb = if is_resume { "resume" } else { "launch" };
                if !manifest.verbs.iter().any(|value| value == verb) {
                    return Err(Error::new("unsupported", "adapter has no matching managed start verb"));
                }
                params.source_plan = Some(super::manifest_source_plan(&manifest, &harness)?);
                let wire = serde_json::to_value(params).map_err(|_| Error::new("unknown", "cannot encode start plan"))?;
                let reply = reader.client.call(method, wire, until).await?;
                if reply != json!({"validated":true}) {
                    return Err(Error::new("unknown", "adapter did not confirm startup validation"));
                }
                Ok(())
            } => result,
            _ = stop.changed() => Err(Error::new("unknown", "start-plan reader cancelled")),
        };
        let secrets = reader.secrets.clone();
        let closure = reader
            .close_until(self, None, metadata_reader_close_deadline(&stop))
            .await;
        let result = require_reader_cleanup(result, closure.closed, method)
            .map_err(|error| redact_native_error(error, &secrets));
        MetadataRead {
            result,
            cleanup: if closure.closed {
                ReaderCleanup::Confirmed
            } else {
                ReaderCleanup::Unknown
            },
        }
    }

    pub(crate) async fn profile_store(
        &self,
        profile: Value,
        workspace: &str,
    ) -> Result<ProfileStoreResult> {
        if HarnessCapabilities::of(profile["harness"].as_str().unwrap_or_default())
            .frozen_store_environment
            .is_none()
        {
            return Err(Error::new(
                "unsupported",
                "profile.store requires a Codex profile",
            ));
        }
        if profile.get("pass_env_values").is_none() {
            return Err(Error::new(
                "unsupported",
                "legacy native store inputs cannot be reselected",
            ));
        }
        if !std::path::Path::new(workspace).is_dir() {
            return Err(Error::new(
                "invalid",
                "profile.store requires the prepared workspace",
            ));
        }
        let environment = Value::Object(build_process_environment(&profile)?);
        let params = ProfileStoreParams {
            profile,
            target_environment: environment,
            workspace: workspace.to_owned(),
        };
        let (id, stop_rx, done_tx) = self.wait_metadata_reader().await?;
        let host = self.clone();
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let completion = host.read_profile_store(params, stop_rx).await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "profile.store reader stopped"))?
    }

    /// Read-only native model catalogue for `harness.models`. A dedicated
    /// metadata reader calls the harness's own `model.list` with no session,
    /// no turn and no inference. The caller supplies a frozen profile
    /// snapshot (ad-hoc or named) whose `cmd` names the native executable.
    pub(crate) async fn models(
        &self,
        profile: Value,
        environment: Value,
        workspace: Option<String>,
        deadline: Instant,
    ) -> Result<ModelCatalogue> {
        let Some(harness) = profile
            .get("harness")
            .and_then(Value::as_str)
            .filter(|harness| is_supported_harness(harness))
        else {
            return Err(Error::new(
                "unsupported",
                "harness.models needs a codex, claude, muse, opencode or antigravity profile",
            ));
        };
        let options = HarnessCapabilities::of(harness).model_reader;
        let request = ModelListParams {
            target_environment: Some(environment),
            profile,
            workspace,
            part: ModelListPart::Models,
        };
        let slot = tokio::time::timeout_at(deadline, self.discovery_slot())
            .await
            .map_err(|_| crate::adhoc::discovery_timeout())??;
        let reader_budget = options
            .budget_seconds
            .map_or(READER_DEADLINE, Duration::from_secs);
        let until = deadline.min(Instant::now() + reader_budget);
        let (id, stop_rx, done_tx) = self.register_metadata_reader_slot(slot)?;
        let host = self.clone();
        let stop_tx = self.state.readers.lock_recover()[&id].stop.clone();
        let (mut reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let read = host.read_model_catalogue(request, stop_rx, until);
            tokio::pin!(read);
            let completion = tokio::select! {
                completion = &mut read => completion,
                _ = reply.closed() => {
                    stop_tx.send_if_modified(|stop| {
                        if stop.is_some() { return false; }
                        *stop = Some(Instant::now() + READER_CLOSE_BUDGET);
                        true
                    });
                    read.await
                }
            };
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response.await.map_err(|_| {
            Error::new(
                options.interruption_code.unwrap_or("unavailable"),
                "model.list reader stopped",
            )
        })?
    }

    /// Read-only native account quota for `harness.quota`. A dedicated
    /// metadata reader calls the harness's own `quota.read` with no session,
    /// no turn and no inference. Each adapter owns its native read and
    /// response translation.
    pub(crate) async fn quota(
        &self,
        profile: Value,
        environment: Value,
        deadline: Instant,
    ) -> Result<Value> {
        if !profile
            .get("harness")
            .and_then(Value::as_str)
            .is_some_and(is_supported_harness)
        {
            return Err(Error::new(
                "unsupported",
                "harness.quota needs a supported native profile",
            ));
        }
        let slot = tokio::time::timeout_at(deadline, self.discovery_slot())
            .await
            .map_err(|_| crate::adhoc::discovery_timeout())??;
        let until = deadline.min(Instant::now() + READER_DEADLINE);
        let (id, stop_rx, done_tx) = self.register_metadata_reader_slot(slot)?;
        let host = self.clone();
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let completion = host.read_quota(profile, environment, stop_rx, until).await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "quota.read reader stopped"))?
    }

    async fn read_quota(
        &self,
        profile: Value,
        environment: Value,
        mut stop: watch::Receiver<Option<Instant>>,
        until: Instant,
    ) -> MetadataRead<Value> {
        if stop.borrow().is_some() {
            return MetadataRead {
                result: Err(Error::new(
                    "cancelled",
                    "quota.read was cancelled before spawn",
                )),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        if Instant::now() >= until {
            return MetadataRead {
                result: Err(crate::adhoc::discovery_timeout()),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        let Some(harness) = profile["harness"].as_str() else {
            return MetadataRead {
                result: Err(Error::new("invalid", "missing harness")),
                cleanup: ReaderCleanup::Confirmed,
            };
        };
        let harness = harness.to_owned();
        let checked = HarnessCapabilities::of(&harness)
            .quota_readback
            .map_or("quota.read", |readback| readback.method)
            .to_owned();
        let opened = self
            .open_reader_with_cleanup(
                &AgentKey {
                    id: "quota-read".to_owned(),
                    generation: 0,
                },
                &harness,
                &profile,
                &environment,
                None,
            )
            .await;
        let mut reader = match opened.result {
            Ok(reader) => reader,
            Err(error) => {
                return failed_metadata_open(error, opened.cleanup_confirmed, "quota.read");
            }
        };
        let mut known: Vec<_> = self.state.secrets.lock_recover().iter().cloned().collect();
        known.extend([self.state.owner_token.clone(), reader.credential.clone()]);
        let metadata_secrets = metadata_secret_snapshot(&profile, &environment, known);
        let read = async {
            let manifest = reader.initialize(&harness, until).await?;
            require_proxy_capability(&profile, &manifest)?;
            let request = serde_json::to_value(eliot_plugin::protocol::QuotaReadParams {
                profile,
                environment,
            })
            .map_err(|_| Error::new("internal", "quota.read params encoding failed"))?;
            let value = reader.client.call("quota.read", request, until).await?;
            reject_reserved_metadata(&value, &metadata_secrets, "quota.read")?;
            let details = value
                .get("details")
                .filter(|details| details.is_object())
                .ok_or_else(|| {
                    Error::new("source_integrity", "quota.read details are malformed")
                })?;
            if serde_json::to_vec(details)
                .map(|bytes| bytes.len())
                .unwrap_or(usize::MAX)
                > 8192
            {
                return Err(Error::new(
                    "source_integrity",
                    "quota.read details exceed 8192 encoded bytes",
                ));
            }
            Ok(value)
        };
        let result = tokio::select! {
            result = tokio::time::timeout_at(until, read) =>
                result.unwrap_or_else(|_| Err(Error::new("timeout", "quota.read deadline exceeded"))),
            _ = stop.changed() => Err(Error::new("cancelled", "quota.read was cancelled")),
        };
        let result = result.map_err(|error| {
            let message = error.message.clone();
            if message.contains(&checked) {
                error
            } else {
                error.with_message(format!("{checked}: {message}"))
            }
        });
        self.close_metadata_reader(reader, &stop, result, "quota.read")
            .await
    }

    async fn read_archive(
        &self,
        profile: Value,
        environment: Value,
        native_ref: String,
        agent: &eliot_core::Agent,
        mut stop: watch::Receiver<Option<Instant>>,
    ) -> MetadataRead<()> {
        if stop.borrow().is_some() {
            return MetadataRead {
                result: Err(Error::new(
                    "cancelled",
                    "native.archive was cancelled before spawn",
                )),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        let Some(harness) = profile["harness"].as_str() else {
            return MetadataRead {
                result: Err(Error::new("invalid", "missing harness")),
                cleanup: ReaderCleanup::Confirmed,
            };
        };
        let harness = harness.to_owned();
        let opened = self
            .open_reader_with_cleanup(
                &AgentKey {
                    id: "native-archive".to_owned(),
                    generation: 0,
                },
                &harness,
                &profile,
                &environment,
                None,
            )
            .await;
        let mut reader = match opened.result {
            Ok(reader) => reader,
            Err(error) => {
                return failed_metadata_open(error, opened.cleanup_confirmed, "native.archive");
            }
        };
        let mut known: Vec<_> = self.state.secrets.lock_recover().iter().cloned().collect();
        known.extend([self.state.owner_token.clone(), reader.credential.clone()]);
        let metadata_secrets = metadata_secret_snapshot(&profile, &environment, known);
        let until = Instant::now() + READER_DEADLINE;
        let workspace_bound = HarnessCapabilities::of(&harness).native_archive
            == Some(NativeArchiveBinding::Workspace);
        let mut archive_dispatched = false;
        let read = async {
            let manifest = reader.initialize(&harness, until).await?;
            require_proxy_capability(&profile, &manifest)?;
            // The OpenCode reader proves its served store derives this exact
            // retained scope before the delete; the workspace binds the
            // server's directory context. Codex pins its store through the
            // frozen binding inside the profile.
            let request = NativeArchiveParams {
                target_environment: (!workspace_bound).then(|| history_environment(&profile)),
                profile,
                native_ref,
                agent_id: workspace_bound.then(|| agent.id.clone()),
                native_scope: workspace_bound
                    .then(|| agent.native_scope.clone())
                    .flatten(),
                workspace: workspace_bound.then(|| agent.workspace.clone()).flatten(),
            };
            let request = serde_json::to_value(&request)
                .map_err(|_| Error::new("invalid", "native.archive params cannot encode"))?;
            archive_dispatched = true;
            let value = reader.client.call("native.archive", request, until).await?;
            reject_reserved_metadata(&value, &metadata_secrets, "native.archive")?;
            let archived = value
                .get("archived")
                .and_then(Value::as_bool)
                .ok_or_else(|| {
                    Error::new("source_integrity", "native.archive result is malformed")
                })?;
            if !archived {
                return Err(Error::new(
                    "native_archive_failed",
                    "native archive did not confirm the session",
                ));
            }
            Ok(())
        };
        let result = tokio::select! {
            result = tokio::time::timeout_at(until, read) =>
                result.unwrap_or_else(|_| Err(Error::new("unavailable", "native.archive deadline exceeded"))),
            _ = stop.changed() => Err(Error::new("cancelled", "native.archive was cancelled")),
        };
        let result = result.map_err(|error| {
            let refused = workspace_bound && error.code == "native_archive_refused";
            let error = if archive_dispatched && !refused {
                Error::new("unknown", "native.archive may have taken effect; no replay")
            } else {
                error
            };
            let message = error.message.clone();
            if message.contains("native.archive") {
                error
            } else {
                error.with_message(format!("native.archive: {message}"))
            }
        });
        self.close_metadata_reader(reader, &stop, result, "native.archive")
            .await
    }

    async fn read_model_catalogue(
        &self,
        mut request: ModelListParams,
        stop: watch::Receiver<Option<Instant>>,
        until: Instant,
    ) -> MetadataRead<ModelCatalogue> {
        // OpenCode's two CLI operations must never share a reader lifetime:
        // closing the first Job proves its standalone server is gone before
        // starting the metadata command. Retain one admission slot throughout.
        let separate_variant_reader = request.profile["harness"].as_str().is_some_and(|harness| {
            HarnessCapabilities::of(harness)
                .model_reader
                .separate_variant_reader
        });
        let mut catalogue = self
            .read_model_catalogue_part(request.clone(), stop.clone(), until)
            .await;
        if !separate_variant_reader
            || catalogue.result.is_err()
            || catalogue.cleanup != ReaderCleanup::Confirmed
        {
            return catalogue;
        }
        request.part = ModelListPart::Variants;
        let metadata = self.read_model_catalogue_part(request, stop, until).await;
        // A missing optional inventory can degrade only after proven cleanup.
        // In particular a cancelled read or unknown family cannot become success.
        if metadata.cleanup != ReaderCleanup::Confirmed
            || metadata
                .result
                .as_ref()
                .is_err_and(|error| matches!(error.code.as_str(), "unknown" | "cancelled"))
        {
            return metadata;
        }
        if let (Ok(catalogue), Ok(metadata)) = (&mut catalogue.result, metadata.result) {
            catalogue.degraded |= metadata.degraded;
            overlay_model_metadata(catalogue, &metadata);
        }
        catalogue
    }

    async fn read_model_catalogue_part(
        &self,
        request: ModelListParams,
        mut stop: watch::Receiver<Option<Instant>>,
        until: Instant,
    ) -> MetadataRead<ModelCatalogue> {
        if stop.borrow().is_some() {
            return MetadataRead {
                result: Err(Error::new(
                    "cancelled",
                    "model.list was cancelled before spawn",
                )),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        if Instant::now() >= until {
            return MetadataRead {
                result: Err(Error::new(
                    "unavailable",
                    "model.list deadline exceeded before spawn",
                )),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        let profile = &request.profile;
        let environment = request.target_environment.as_ref().unwrap_or(&Value::Null);
        let Some(harness) = profile["harness"].as_str() else {
            return MetadataRead {
                result: Err(Error::new("invalid", "missing harness")),
                cleanup: ReaderCleanup::Confirmed,
            };
        };
        let harness = harness.to_owned();
        let opened = self
            .open_reader_with_cleanup(
                &AgentKey {
                    id: "model-list".to_owned(),
                    generation: 0,
                },
                &harness,
                profile,
                environment,
                None,
            )
            .await;
        let mut reader = match opened.result {
            Ok(reader) => reader,
            Err(error) => {
                return failed_metadata_open(error, opened.cleanup_confirmed, "model.list");
            }
        };
        let mut known: Vec<_> = self.state.secrets.lock_recover().iter().cloned().collect();
        known.extend([self.state.owner_token.clone(), reader.credential.clone()]);
        let metadata_secrets = metadata_secret_snapshot(profile, environment, known);
        let read = async {
            if HarnessCapabilities::of(&harness).models_preview_auth_integrations {
                reader.create_model_catalogue_dir()?;
            }
            let manifest = reader.initialize(&harness, until).await?;
            require_proxy_capability(profile, &manifest)?;
            let request = serde_json::to_value(&request)
                .map_err(|_| Error::new("internal", "model.list params encoding failed"))?;
            let value = reader.client.call("model.list", request, until).await?;
            reject_reserved_metadata(&value, &metadata_secrets, "model.list")?;
            ModelCatalogue::decode(value, None)
        };
        let result = tokio::select! {
            result = tokio::time::timeout_at(until, read) =>
                result.unwrap_or_else(|_| Err(Error::new(
                    HarnessCapabilities::of(&harness).model_reader.interruption_code.unwrap_or("unavailable"),
                    "model.list deadline exceeded",
                ))),
            _ = stop.changed() => Err(Error::new("cancelled", "model.list was cancelled")),
        };
        self.close_metadata_reader(reader, &stop, result, "model.list")
            .await
    }

    async fn close_metadata_reader<T>(
        &self,
        reader: ReaderTransport,
        stop: &watch::Receiver<Option<Instant>>,
        result: Result<T>,
        operation: &str,
    ) -> MetadataRead<T> {
        let secrets = reader.secrets.clone();
        let close_until = metadata_reader_close_deadline(stop);
        let closure = reader.close_until(self, None, close_until).await;
        let result = require_reader_cleanup(result, closure.closed, operation);
        let result = result.map_err(|error| {
            let error = redact_native_error(
                error.with_stderr(StderrSource::Plugin, closure.stderr),
                &secrets,
            );
            crate::logging::record(
                &SecretPolicy::new(secrets.iter()),
                &error.code,
                &[("operation", operation)],
                Some(&error),
            );
            error
        });
        MetadataRead {
            result,
            cleanup: cleanup_state(closure.closed),
        }
    }

    /// One interactive reader slot for a catalogue or quota read. The
    /// desktop asks every harness's quota and the unified catalogue together
    /// when it connects, so a read waits briefly for a slot within the
    /// four-reader bound instead of failing a transient full pool (the
    /// catalogue then lacked whole harnesses). After the wait it refuses
    /// `busy`, naming `metadata_readers` and its limit.
    async fn discovery_slot(&self) -> Result<OwnedSemaphorePermit> {
        match tokio::time::timeout(
            DISCOVERY_SLOT_WAIT,
            self.state.reader_slots.clone().acquire_owned(),
        )
        .await
        {
            Ok(Ok(slot)) => Ok(slot),
            Ok(Err(_)) => Err(Error::new("unavailable", "discovery admission is closed")),
            Err(_) => Err(readers_busy()),
        }
    }

    fn register_metadata_reader(
        &self,
    ) -> Result<(
        String,
        watch::Receiver<Option<Instant>>,
        watch::Sender<ReaderCleanup>,
    )> {
        let slot = self
            .state
            .reader_slots
            .clone()
            .try_acquire_owned()
            .map_err(|error| match error {
                tokio::sync::TryAcquireError::Closed => {
                    Error::new("unavailable", "discovery admission is closed")
                }
                tokio::sync::TryAcquireError::NoPermits => readers_busy(),
            })?;
        self.register_metadata_reader_slot(slot)
    }

    // Admitted launches wait fairly before opening any metadata process. The
    // permit stays with the reader until confirmed custody closure; unknown
    // cleanup never frees capacity. Closing admission wakes all queued waiters.
    async fn wait_metadata_reader(
        &self,
    ) -> Result<(
        String,
        watch::Receiver<Option<Instant>>,
        watch::Sender<ReaderCleanup>,
    )> {
        let slot = self
            .state
            .launch_reader_slots
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::new("unavailable", "discovery admission is closed"))?;
        self.register_metadata_reader_slot(slot)
    }

    fn register_metadata_reader_slot(
        &self,
        slot: OwnedSemaphorePermit,
    ) -> Result<(
        String,
        watch::Receiver<Option<Instant>>,
        watch::Sender<ReaderCleanup>,
    )> {
        let id = Ulid::new().to_string();
        let (stop_tx, stop_rx) = watch::channel(None);
        let (done_tx, done_rx) = watch::channel(ReaderCleanup::Pending);
        let mut readers = self.state.readers.lock_recover();
        if self.state.discovery_closed.load(Ordering::SeqCst) {
            return Err(Error::new("unavailable", "discovery admission is closed"));
        }
        readers.insert(
            id.clone(),
            Reader {
                _slot: slot,
                stop: stop_tx,
                done: done_rx,
            },
        );
        Ok((id, stop_rx, done_tx))
    }

    fn finish_metadata_reader(
        &self,
        id: &str,
        done: watch::Sender<ReaderCleanup>,
        cleanup: ReaderCleanup,
    ) {
        settle_metadata_reader(&self.state.readers, id, done, cleanup);
        signal_progress(&self.state);
    }

    async fn read_profile_store(
        &self,
        params: ProfileStoreParams,
        mut stop: watch::Receiver<Option<Instant>>,
    ) -> MetadataRead<ProfileStoreResult> {
        if stop.borrow().is_some() {
            return MetadataRead {
                result: Err(Error::new(
                    "cancelled",
                    "profile.store was cancelled before spawn",
                )),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        let Some(harness) = params.profile["harness"].as_str() else {
            return MetadataRead {
                result: Err(Error::new("invalid", "missing harness")),
                cleanup: ReaderCleanup::Confirmed,
            };
        };
        let opened = self
            .open_reader_with_cleanup(
                &AgentKey {
                    id: "profile-store".to_owned(),
                    generation: 0,
                },
                harness,
                &params.profile,
                &params.target_environment,
                None,
            )
            .await;
        let mut reader = match opened.result {
            Ok(reader) => reader,
            Err(error) => {
                return failed_metadata_open(error, opened.cleanup_confirmed, "profile.store");
            }
        };
        let mut known: Vec<_> = self.state.secrets.lock_recover().iter().cloned().collect();
        known.extend([self.state.owner_token.clone(), reader.credential.clone()]);
        let metadata_secrets =
            metadata_secret_snapshot(&params.profile, &params.target_environment, known);
        let until = Instant::now() + READER_DEADLINE;
        let read = async {
            let manifest = reader.initialize(harness, until).await?;
            require_proxy_capability(&params.profile, &manifest)?;
            let request = serde_json::to_value(&params)
                .map_err(|_| Error::new("internal", "cannot encode profile.store params"))?;
            let value = reader.client.call("profile.store", request, until).await?;
            reject_reserved_metadata(&value, &metadata_secrets, "profile.store")?;
            let preparation: ProfileStoreResult = serde_json::from_value(value).map_err(|_| {
                Error::new("source_integrity", "profile.store binding is malformed")
            })?;
            let binding = &preparation.binding;
            if [&binding.home, &binding.root, &binding.native_scope]
                .into_iter()
                .any(|value| value.is_empty() || value.chars().any(char::is_control))
                || !std::path::Path::new(&binding.home).is_absolute()
                || !std::path::Path::new(&binding.root).is_absolute()
                || preparation
                    .catalogue_digest
                    .as_deref()
                    .is_some_and(|digest| !eliot_core::is_lowercase_hex(digest, 64))
            {
                return Err(Error::new(
                    "source_integrity",
                    "profile.store binding is unverified",
                ));
            }
            Ok(preparation)
        };
        let result = tokio::select! {
            result = tokio::time::timeout_at(until, read) =>
                result.unwrap_or_else(|_| Err(Error::new("unavailable", "profile.store deadline exceeded"))),
            _ = stop.changed() => Err(Error::new("cancelled", "profile.store was cancelled")),
        };
        self.close_metadata_reader(reader, &stop, result, "profile.store")
            .await
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "The operation's distinct inputs remain explicit at its established call boundary."
    )]
    async fn read_subject_page(
        &self,
        profile: Value,
        environment: Value,
        native_scope: Option<String>,
        cursor: Option<String>,
        archived: bool,
        limit: usize,
        mut stop: watch::Receiver<Option<Instant>>,
    ) -> MetadataRead<Value> {
        if stop.borrow().is_some() {
            return MetadataRead {
                result: Err(Error::new(
                    "cancelled",
                    "discovery was cancelled before spawn",
                )),
                cleanup: ReaderCleanup::Confirmed,
            };
        }
        let Some(harness) = profile["harness"].as_str() else {
            return MetadataRead {
                result: Err(Error::new("invalid", "missing harness")),
                cleanup: ReaderCleanup::Confirmed,
            };
        };
        let opened = self
            .open_reader_with_policy(
                &AgentKey {
                    id: "discovery".to_owned(),
                    generation: 0,
                },
                harness,
                &profile,
                &environment,
                None,
                true,
            )
            .await;
        let mut reader = match opened.result {
            Ok(reader) => reader,
            Err(error) => {
                return failed_metadata_open(error, opened.cleanup_confirmed, "discovery");
            }
        };
        let until = Instant::now() + READER_DEADLINE;
        let read = async {
            let manifest = reader.initialize(harness, until).await?;
            require_proxy_capability(&profile, &manifest)?;
            if !manifest.features.discover {
                return Err(Error::new(
                    "unsupported",
                    "plugin does not advertise discovery",
                ));
            }
            let value = reader
                .client
                .call(
                    "discover",
                    json!({
                        "profile": profile, "target_environment": environment,
                        "native_scope": native_scope, "cursor": cursor,
                        "archived": archived, "limit": limit,
                    }),
                    until,
                )
                .await?;
            let page = validate_page(
                &value,
                harness,
                native_scope.as_deref(),
                cursor.as_deref(),
                limit,
            )?;
            serde_json::to_value(page)
                .map_err(|_| Error::new("source_integrity", "cannot encode discovery metadata"))
        };
        let result = tokio::select! {
            result = tokio::time::timeout_at(until, read) =>
                result.unwrap_or_else(|_| Err(Error::new("unavailable", "discovery deadline exceeded"))),
            _ = stop.changed() => Err(Error::new("cancelled", "discovery was cancelled")),
        };
        let secrets = reader.secrets.clone();
        let metadata_secrets = reader.metadata_secrets.clone();
        let close_until = metadata_reader_close_deadline(&stop);
        let closure = reader.close_until(self, None, close_until).await;
        let result = require_reader_cleanup(result, closure.closed, "discovery");
        let result = match result {
            Ok(mut value) => {
                if crate::redaction::redact_value(&mut value, &metadata_secrets) {
                    Err(Error::new(
                        "source_integrity",
                        "discovery metadata contains reserved credentials",
                    ))
                } else {
                    Ok(value)
                }
            }
            Err(error) => {
                let error = redact_native_error(
                    error.with_stderr(StderrSource::Plugin, closure.stderr),
                    &secrets,
                );
                crate::logging::record(
                    &SecretPolicy::new(secrets.iter()),
                    &error.code,
                    &[("operation", "discover")],
                    Some(&error),
                );
                Err(error)
            }
        };
        MetadataRead {
            result,
            cleanup: cleanup_state(closure.closed),
        }
    }
}

fn overlay_model_metadata(catalogue: &mut ModelCatalogue, metadata: &ModelCatalogue) {
    let by_id: HashMap<_, _> = metadata
        .models
        .iter()
        .rev()
        .map(|row| (row.id.as_str(), row))
        .collect();
    for model in &mut catalogue.models {
        if let Some(row) = by_id.get(model.id.as_str()) {
            if row.efforts.is_some() {
                model.efforts.clone_from(&row.efforts);
            }
            if row.display_name.is_some() {
                model.display_name.clone_from(&row.display_name);
            }
        }
    }
}

/// Refuse a metadata reply that echoes a reserved credential. Discovery
/// redacts instead of refusing, so it keeps its own `redact_value` path.
fn reject_reserved_metadata(value: &Value, secrets: &[String], method: &str) -> Result<()> {
    if SecretPolicy::new(secrets.iter()).contains_value(value) {
        return Err(Error::new(
            "source_integrity",
            format!("{method} metadata contains reserved credentials"),
        ));
    }
    Ok(())
}

fn metadata_reader_close_deadline(stop: &watch::Receiver<Option<Instant>>) -> Instant {
    (*stop.borrow()).unwrap_or_else(|| Instant::now() + READER_CLOSE_BUDGET)
}

fn failed_metadata_open<T>(
    error: Error,
    cleanup_confirmed: bool,
    operation: &str,
) -> MetadataRead<T> {
    MetadataRead {
        result: require_reader_cleanup(Err(error), cleanup_confirmed, operation),
        cleanup: cleanup_state(cleanup_confirmed),
    }
}

fn require_reader_cleanup<T>(
    result: Result<T>,
    cleanup_confirmed: bool,
    operation: &str,
) -> Result<T> {
    if cleanup_confirmed {
        result
    } else {
        Err(Error::new(
            "unknown",
            format!("{operation} reader cleanup is unconfirmed"),
        ))
    }
}

/// A historical read needs store selectors, never an inference credential or
/// a current provider route. The adapter rechecks the retained scope against
/// the resolved directories before it opens any history file.
fn history_environment(profile: &Value) -> Value {
    let capabilities = HarnessCapabilities::of(profile["harness"].as_str().unwrap_or_default());
    let native = capabilities.profile.environment;
    let names = ["HOME", "USERPROFILE"]
        .into_iter()
        .chain(
            capabilities
                .frozen_store_environment
                .into_iter()
                .flat_map(|store| store.selectors.iter().copied()),
        )
        .chain(native.store_paths.iter().copied())
        .chain(native.legacy_captured_paths.iter().copied())
        .chain(native.credential_directory);
    let mut environment = serde_json::Map::new();
    for name in names {
        let captured = profile["native_route_home"]
            .get(name)
            .or_else(|| profile["pass_env_values"].get(name));
        let admitted = matches!(name, "HOME" | "USERPROFILE")
            || profile["pass_env"].as_array().is_some_and(|names| {
                names
                    .iter()
                    .filter_map(Value::as_str)
                    .any(|key| key == name || cfg!(windows) && key.eq_ignore_ascii_case(name))
            });
        let path = match captured {
            Some(value) => value.as_str().map(str::to_owned),
            None if admitted => std::env::var(name).ok(),
            None => None,
        };
        if let Some(path) = path.filter(|path| std::path::Path::new(path).is_absolute()) {
            environment.insert(name.to_owned(), Value::String(path));
        }
    }
    Value::Object(environment)
}

fn cleanup_state(cleanup_confirmed: bool) -> ReaderCleanup {
    if cleanup_confirmed {
        ReaderCleanup::Confirmed
    } else {
        ReaderCleanup::Unknown
    }
}

fn metadata_reader_count(readers: &Mutex<HashMap<String, Reader>>) -> usize {
    readers.lock_recover().len()
}

fn settle_metadata_reader(
    readers: &Mutex<HashMap<String, Reader>>,
    id: &str,
    done: watch::Sender<ReaderCleanup>,
    cleanup: ReaderCleanup,
) {
    if cleanup == ReaderCleanup::Confirmed {
        readers.lock_recover().remove(id);
    }
    done.send_replace(cleanup);
}

async fn cancel_metadata_readers(
    readers: Vec<(
        watch::Sender<Option<Instant>>,
        watch::Receiver<ReaderCleanup>,
    )>,
    close_until: Instant,
) -> bool {
    for (stop, _) in &readers {
        stop.send_replace(Some(close_until));
    }

    let mut all_confirmed = true;
    for (_, mut done) in readers {
        match tokio::time::timeout_at(
            close_until,
            done.wait_for(|cleanup| *cleanup != ReaderCleanup::Pending),
        )
        .await
        {
            Ok(Ok(cleanup)) if *cleanup == ReaderCleanup::Confirmed => {}
            _ => all_confirmed = false,
        }
    }
    all_confirmed
}

fn validate_page(
    value: &Value,
    harness: &str,
    scope: Option<&str>,
    cursor: Option<&str>,
    limit: usize,
) -> Result<DiscoverPage> {
    let page: DiscoverPage = serde_json::from_value(value.clone())
        .map_err(|_| Error::new("source_integrity", "malformed discovery page"))?;
    if page.harness != harness
        || !valid_text(&page.native_scope, 512)
        || scope.is_some_and(|scope| scope != page.native_scope)
        || page.subjects.len() > limit
        || !valid_text(&page.coverage.kind, 128)
        || page.coverage.complete != page.next_cursor.is_none()
        || page
            .next_cursor
            .as_deref()
            .is_some_and(|next| !valid_text(next, MAX_CURSOR_BYTES) || Some(next) == cursor)
    {
        return Err(Error::new(
            "source_integrity",
            "discovery page binding or coverage is invalid",
        ));
    }
    let mut subjects = HashSet::new();
    for subject in &page.subjects {
        if !valid_text(&subject.native_ref, 512)
            || !valid_text(&subject.state, 128)
            || !subjects.insert(&subject.native_ref)
        {
            return Err(Error::new(
                "source_integrity",
                "invalid or duplicate discovery subject",
            ));
        }
    }
    if serde_json::to_vec(&page)
        .map_err(|_| Error::new("source_integrity", "cannot encode discovery page"))?
        .len()
        > MAX_PAGE_BYTES
    {
        return Err(Error::new(
            "frame_too_large",
            "discovery page exceeds 900 KiB",
        ));
    }
    Ok(page)
}

fn valid_text(value: &str, max: usize) -> bool {
    !value.is_empty() && value.len() <= max && !value.chars().any(char::is_control)
}
