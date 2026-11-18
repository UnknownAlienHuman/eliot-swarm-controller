//! A bounded read-only plugin instance, independent of every Agent identity.
use super::reader::READER_CLOSE_BUDGET;
use super::{
    AgentKey, PluginHost, build_process_environment, metadata_secret_snapshot, redact_native_error,
    require_proxy_capability, signal_progress,
};
use eliot_core::{Error, Result, SecretPolicy, StderrSource, api::DiscoverPage};
use eliot_plugin::protocol::{
    ModelListParams, ModelListPart, ProfileStoreParams, ProfileStoreResult,
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
        if !matches!(
            profile.get("harness").and_then(Value::as_str),
            Some("codex")
        ) {
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

    /// Archive one terminal Codex native session through the harness's own
    /// `thread/archive` method. A dedicated metadata reader calls
    /// `native.archive` with no turn and no inference. Only the codex
    /// harness documents a native archive surface; every other harness
    /// stays `unsupported` without spawning a reader, and the caller keeps
    /// the session. Never deletes raw native store files.
    pub(crate) async fn archive_native_session(
        &self,
        profile: Value,
        native_ref: &str,
    ) -> Result<()> {
        if profile.get("harness").and_then(Value::as_str) != Some("codex") {
            return Err(Error::new(
                "unsupported",
                "native archive needs a codex profile",
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
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let completion = host
                .read_archive(profile, environment, native_ref, stop_rx)
                .await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "archive reader stopped"))?
    }

    pub(crate) async fn profile_store(
        &self,
        profile: Value,
        workspace: &str,
    ) -> Result<ProfileStoreResult> {
        if profile.get("harness").and_then(Value::as_str) != Some("codex") {
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
        deadline: Instant,
    ) -> Result<Value> {
        if !profile
            .get("harness")
            .and_then(Value::as_str)
            .is_some_and(|harness| matches!(harness, "codex" | "muse" | "opencode" | "antigravity"))
        {
            return Err(Error::new(
                "unsupported",
                "harness.models needs a codex, muse, opencode or antigravity profile",
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
            let completion = host
                .read_model_catalogue(profile, environment, stop_rx, until)
                .await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "model.list reader stopped"))?
    }

    /// Read-only native account quota for `harness.quota`. A dedicated
    /// metadata reader calls the harness's own `quota.read` with no session,
    /// no turn and no inference. Only Codex (`account/rateLimits/read` plus
    /// `account/usage/read`) and Muse (`usage/read`) expose a read-only
    /// quota surface; other harnesses stay `unsupported` without spawning a
    /// reader.
    pub(crate) async fn quota(
        &self,
        profile: Value,
        environment: Value,
        deadline: Instant,
    ) -> Result<Value> {
        if !profile
            .get("harness")
            .and_then(Value::as_str)
            .is_some_and(|harness| {
                matches!(
                    harness,
                    "codex" | "muse" | "claude" | "opencode" | "antigravity"
                )
            })
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
        let checked = match harness.as_str() {
            "codex" => "account/rateLimits/read",
            "muse" => "usage/read",
            _ => "quota.read",
        }
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
            if SecretPolicy::new(metadata_secrets.iter()).contains_value(&value) {
                return Err(Error::new(
                    "source_integrity",
                    "quota.read metadata contains reserved credentials",
                ));
            }
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
                result.unwrap_or_else(|_| Err(Error::new("unavailable", "quota.read deadline exceeded"))),
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
        let secrets = reader.secrets.clone();
        let close_until = metadata_reader_close_deadline(&stop);
        let closure = reader.close_until(self, None, close_until).await;
        let result = require_reader_cleanup(result, closure.closed, "quota.read");
        let result = result.map_err(|error| {
            let error = redact_native_error(
                error.with_stderr(StderrSource::Plugin, closure.stderr),
                &secrets,
            );
            crate::logging::record(
                &SecretPolicy::new(secrets.iter()),
                &error.code,
                &[("operation", "quota.read")],
                Some(&error),
            );
            error
        });
        MetadataRead {
            result,
            cleanup: cleanup_state(closure.closed),
        }
    }

    async fn read_archive(
        &self,
        profile: Value,
        environment: Value,
        native_ref: String,
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
        let mut archive_dispatched = false;
        let read = async {
            let manifest = reader.initialize(&harness, until).await?;
            require_proxy_capability(&profile, &manifest)?;
            let request = serde_json::json!({"profile": profile, "native_ref": native_ref});
            archive_dispatched = true;
            let value = reader.client.call("native.archive", request, until).await?;
            if SecretPolicy::new(metadata_secrets.iter()).contains_value(&value) {
                return Err(Error::new(
                    "source_integrity",
                    "native.archive metadata contains reserved credentials",
                ));
            }
            let archived = value
                .get("archived")
                .and_then(Value::as_bool)
                .ok_or_else(|| {
                    Error::new("source_integrity", "native.archive result is malformed")
                })?;
            if !archived {
                return Err(Error::new(
                    "native_archive_failed",
                    "native archive did not confirm the thread",
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
            let error = if archive_dispatched {
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
        let secrets = reader.secrets.clone();
        let close_until = metadata_reader_close_deadline(&stop);
        let closure = reader.close_until(self, None, close_until).await;
        let result = require_reader_cleanup(result, closure.closed, "native.archive");
        let result = result.map_err(|error| {
            let error = redact_native_error(
                error.with_stderr(StderrSource::Plugin, closure.stderr),
                &secrets,
            );
            crate::logging::record(
                &SecretPolicy::new(secrets.iter()),
                &error.code,
                &[("operation", "native.archive")],
                Some(&error),
            );
            error
        });
        MetadataRead {
            result,
            cleanup: cleanup_state(closure.closed),
        }
    }

    async fn read_model_catalogue(
        &self,
        profile: Value,
        environment: Value,
        stop: watch::Receiver<Option<Instant>>,
        until: Instant,
    ) -> MetadataRead<Value> {
        // OpenCode's two CLI operations must never share a reader lifetime:
        // closing the first Job proves its standalone server is gone before
        // starting the metadata command. Retain one admission slot throughout.
        let opencode = profile["harness"] == "opencode";
        let mut catalogue = self
            .read_model_catalogue_part(
                profile.clone(),
                environment.clone(),
                stop.clone(),
                ModelListPart::Models,
                until,
            )
            .await;
        if !opencode || catalogue.result.is_err() || catalogue.cleanup != ReaderCleanup::Confirmed {
            return catalogue;
        }
        let metadata = self
            .read_model_catalogue_part(profile, environment, stop, ModelListPart::Variants, until)
            .await;
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
            overlay_model_metadata(catalogue, &metadata);
        }
        catalogue
    }

    async fn read_model_catalogue_part(
        &self,
        profile: Value,
        environment: Value,
        mut stop: watch::Receiver<Option<Instant>>,
        part: ModelListPart,
        until: Instant,
    ) -> MetadataRead<Value> {
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
                &profile,
                &environment,
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
        let metadata_secrets = metadata_secret_snapshot(&profile, &environment, known);
        let read = async {
            let manifest = reader.initialize(&harness, until).await?;
            require_proxy_capability(&profile, &manifest)?;
            let request = serde_json::to_value(ModelListParams { profile, part })
                .map_err(|_| Error::new("internal", "model.list params encoding failed"))?;
            let value = reader.client.call("model.list", request, until).await?;
            if SecretPolicy::new(metadata_secrets.iter()).contains_value(&value) {
                return Err(Error::new(
                    "source_integrity",
                    "model.list metadata contains reserved credentials",
                ));
            }
            let mut value: Value = value;
            if value.get("models").and_then(Value::as_array).is_none() {
                return Err(Error::new(
                    "source_integrity",
                    "model.list catalogue is malformed",
                ));
            }
            if let Some(models) = value.get_mut("models").and_then(Value::as_array_mut)
                && models.len() > 1024
            {
                return Err(Error::new(
                    "source_integrity",
                    "model.list catalogue exceeds 1024 entries",
                ));
            }
            Ok(value)
        };
        let result = tokio::select! {
            result = tokio::time::timeout_at(until, read) =>
                result.unwrap_or_else(|_| Err(Error::new("unavailable", "model.list deadline exceeded"))),
            _ = stop.changed() => Err(Error::new("cancelled", "model.list was cancelled")),
        };
        let secrets = reader.secrets.clone();
        let close_until = metadata_reader_close_deadline(&stop);
        let closure = reader.close_until(self, None, close_until).await;
        let result = require_reader_cleanup(result, closure.closed, "model.list");
        let result = result.map_err(|error| {
            let error = redact_native_error(
                error.with_stderr(StderrSource::Plugin, closure.stderr),
                &secrets,
            );
            crate::logging::record(
                &SecretPolicy::new(secrets.iter()),
                &error.code,
                &[("operation", "model.list")],
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
            if SecretPolicy::new(metadata_secrets.iter()).contains_value(&value) {
                return Err(Error::new(
                    "source_integrity",
                    "profile.store metadata contains reserved credentials",
                ));
            }
            let preparation: ProfileStoreResult = serde_json::from_value(value).map_err(|_| {
                Error::new("source_integrity", "profile.store binding is malformed")
            })?;
            let binding = &preparation.binding;
            if [&binding.home, &binding.root, &binding.native_scope]
                .into_iter()
                .any(|value| value.is_empty() || value.chars().any(char::is_control))
                || !std::path::Path::new(&binding.home).is_absolute()
                || !std::path::Path::new(&binding.root).is_absolute()
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
        let secrets = reader.secrets.clone();
        let close_until = metadata_reader_close_deadline(&stop);
        let closure = reader.close_until(self, None, close_until).await;
        let result = require_reader_cleanup(result, closure.closed, "profile.store");
        let result = result.map_err(|error| {
            let error = redact_native_error(
                error.with_stderr(StderrSource::Plugin, closure.stderr),
                &secrets,
            );
            crate::logging::record(
                &SecretPolicy::new(secrets.iter()),
                &error.code,
                &[("operation", "profile.store")],
                Some(&error),
            );
            error
        });
        MetadataRead {
            result,
            cleanup: cleanup_state(closure.closed),
        }
    }

    #[allow(clippy::too_many_arguments)]
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
            .open_reader_with_cleanup(
                &AgentKey {
                    id: "discovery".to_owned(),
                    generation: 0,
                },
                harness,
                &profile,
                &environment,
                None,
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

fn overlay_model_metadata(catalogue: &mut Value, metadata: &Value) {
    let (Some(models), Some(metadata)) = (
        catalogue.get_mut("models").and_then(Value::as_array_mut),
        metadata.get("models").and_then(Value::as_array),
    ) else {
        return;
    };
    for model in models {
        let Some(entry) = metadata
            .iter()
            .find(|entry| entry.get("id") == model.get("id"))
        else {
            continue;
        };
        for field in ["efforts", "display_name"] {
            if let Some(value) = entry.get(field).filter(|value| !value.is_null()) {
                model[field] = value.clone();
            }
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};

    #[test]
    fn unconfirmed_metadata_cleanup_overrides_success_and_known_failure() {
        for result in [
            Ok(json!({"models": []})),
            Err(Error::new("native_readback", "unreadable")),
        ] {
            let error = require_reader_cleanup(result, false, "model.list").unwrap_err();
            assert_eq!(error.code, "unknown");
            assert!(error.message.contains("cleanup is unconfirmed"));
        }
    }

    #[tokio::test]
    async fn cancelled_variants_remain_cancelled_when_read_deadline_also_expired() {
        let host = host();
        let until = Instant::now();
        let (_stop, stopped) = watch::channel(Some(until));
        let read = host
            .read_model_catalogue_part(
                json!({"harness": "opencode"}),
                json!({}),
                stopped,
                ModelListPart::Variants,
                until,
            )
            .await;
        assert_eq!(read.cleanup, ReaderCleanup::Confirmed);
        assert_eq!(read.result.unwrap_err().code, "cancelled");
    }

    fn host() -> PluginHost {
        use crate::{bus::Bus, monitor::Monitoring, plugin_host::host::PluginHostOptions};
        let store = Arc::new(tokio::sync::Mutex::new(
            eliot_store::Store::open(std::path::Path::new(":memory:")).unwrap(),
        ));
        let bus = Bus::new(64);
        let secrets = Arc::new(Mutex::new(HashSet::new()));
        let monitoring = Monitoring::new(store.clone(), bus.clone(), secrets.clone());
        let (progress, _) = watch::channel(0);
        PluginHost::new(
            PluginHostOptions::new(std::path::PathBuf::new(), "owner-test".into()),
            store,
            bus,
            secrets,
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashSet::new())),
            progress,
            monitoring,
        )
    }

    #[tokio::test]
    async fn concurrent_launches_above_four_wait_for_metadata_slots() {
        let host = host();
        let mut held = Vec::new();
        for _ in 0..MAX_READERS {
            held.push(host.register_metadata_reader().unwrap());
        }
        let error = host.register_metadata_reader().unwrap_err();
        assert_eq!(error.code, "busy");
        assert!(error.message.contains("metadata_readers"));
        assert!(error.message.contains("limit=4"));
        for _ in 0..MAX_LAUNCH_READERS {
            held.push(host.wait_metadata_reader().await.unwrap());
        }
        let mut launches = tokio::task::JoinSet::new();
        for _ in 0..50 {
            let host = host.clone();
            launches.spawn(async move {
                // Exercise real Store admission and the same pre-dispatch slot
                // acquisition as profile.store, without invoking a native agent.
                let request = eliot_core::SubmitCommand {
                    args: json!({"harness":"codex", "profile":"luna", "project":"k65-regression", "workspace":".",
                        "profile_snapshot":{"harness":"codex"}}),
                    ..eliot_core::SubmitCommand::new(Ulid::new().to_string(), "launch")
                };
                let admission = host
                    .state
                    .store
                    .lock()
                    .await
                    .submit("owner", request, crate::common::now_ms())
                    .unwrap();
                assert_eq!(admission.command.state, eliot_core::CommandState::Accepted);
                let (id, _, done) = host.wait_metadata_reader().await.unwrap();
                assert!(host.reader_count() <= MAX_READERS + MAX_LAUNCH_READERS);
                host.finish_metadata_reader(&id, done, ReaderCleanup::Confirmed);
            });
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), launches.join_next())
                .await
                .is_err()
        );
        for (id, _, done) in held {
            host.finish_metadata_reader(&id, done, ReaderCleanup::Confirmed);
        }
        let mut count = 0;
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(result) = launches.join_next().await {
                result.unwrap();
                count += 1;
            }
        })
        .await
        .unwrap();
        assert_eq!(count, 50);
        assert_eq!(host.reader_count(), 0);
    }

    #[tokio::test]
    async fn catalogue_read_waits_for_a_reader_slot_instead_of_refusing_busy() {
        // The desktop's five connect-time quota reads hold every slot when the
        // catalogue asks for Codex: the catalogue read waits for the first
        // reader to finish instead of failing (k98: Codex vanished).
        let host = host();
        let mut held = Vec::new();
        for _ in 0..MAX_READERS {
            held.push(
                host.discovery_slot()
                    .await
                    .map(|slot| host.register_metadata_reader_slot(slot).unwrap()),
            );
        }
        let waiting = host.discovery_slot();
        tokio::pin!(waiting);
        assert!(futures_util::poll!(&mut waiting).is_pending());
        let (id, _, done) = held.pop().unwrap().unwrap();
        host.finish_metadata_reader(&id, done, ReaderCleanup::Confirmed);
        let slot = tokio::time::timeout(Duration::from_secs(1), waiting)
            .await
            .expect("a freed slot admits the waiting read")
            .unwrap();
        let reader = host.register_metadata_reader_slot(slot).unwrap();
        assert_eq!(host.reader_count(), MAX_READERS);
        host.finish_metadata_reader(&reader.0, reader.2, ReaderCleanup::Confirmed);
        for (id, _, done) in held.into_iter().map(Result::unwrap) {
            host.finish_metadata_reader(&id, done, ReaderCleanup::Confirmed);
        }
        assert_eq!(host.reader_count(), 0);
        host.close_discovery_admission();
        assert_eq!(host.discovery_slot().await.unwrap_err().code, "unavailable");
    }

    #[tokio::test]
    async fn metadata_unknown_cleanup_retains_slot_and_shutdown_wakes_waiters() {
        let host = host();
        for _ in 0..MAX_LAUNCH_READERS {
            let (id, _, done) = host.wait_metadata_reader().await.unwrap();
            host.finish_metadata_reader(&id, done, ReaderCleanup::Unknown);
        }
        let wait = host.wait_metadata_reader();
        tokio::pin!(wait);
        assert!(futures_util::poll!(&mut wait).is_pending());
        assert_eq!(host.reader_count(), MAX_LAUNCH_READERS);
        host.close_discovery_admission();
        assert_eq!(wait.await.unwrap_err().code, "unavailable");
        assert_eq!(host.reader_count(), MAX_LAUNCH_READERS);
    }

    async fn cancel_after_cleanup(cleanup: ReaderCleanup) -> (bool, usize) {
        let readers = Arc::new(Mutex::new(HashMap::new()));
        let (stop_tx, mut stop_rx) = watch::channel(None);
        let (done_tx, done_rx) = watch::channel(ReaderCleanup::Pending);
        readers.lock_recover().insert(
            "reader".to_owned(),
            Reader {
                _slot: Arc::new(tokio::sync::Semaphore::new(1))
                    .try_acquire_owned()
                    .unwrap(),
                stop: stop_tx.clone(),
                done: done_rx.clone(),
            },
        );
        let owner_readers = Arc::clone(&readers);
        let owner = tokio::spawn(async move {
            let _ = stop_rx.changed().await;
            settle_metadata_reader(&owner_readers, "reader", done_tx, cleanup);
        });

        let confirmed = cancel_metadata_readers(
            vec![(stop_tx, done_rx)],
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        owner.await.expect("reader owner task");
        (confirmed, metadata_reader_count(&readers))
    }

    #[tokio::test]
    async fn metadata_reader_cancel_retains_unknown_blocker_and_releases_confirmed() {
        let (confirmed, blockers) = cancel_after_cleanup(cleanup_state(false)).await;
        assert!(!confirmed);
        assert_eq!(blockers, 1);

        let (confirmed, blockers) = cancel_after_cleanup(cleanup_state(true)).await;
        assert!(confirmed);
        assert_eq!(blockers, 0);

        let readers = Mutex::new(HashMap::new());
        let (stop_tx, _stop_rx) = watch::channel(None);
        let (done_tx, done_rx) = watch::channel(ReaderCleanup::Pending);
        readers.lock_recover().insert(
            "timed-out-reader".to_owned(),
            Reader {
                _slot: Arc::new(tokio::sync::Semaphore::new(1))
                    .try_acquire_owned()
                    .unwrap(),
                stop: stop_tx.clone(),
                done: done_rx.clone(),
            },
        );
        let timed_out = cancel_metadata_readers(vec![(stop_tx, done_rx)], Instant::now()).await;
        assert!(!timed_out);
        assert_eq!(metadata_reader_count(&readers), 1);
        drop(done_tx);
    }
}
