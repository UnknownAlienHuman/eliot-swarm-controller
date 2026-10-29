//! A bounded read-only plugin instance, independent of every Agent identity.
use super::reader::READER_CLOSE_BUDGET;
use super::{
    AgentKey, PluginHost, build_process_environment, metadata_secret_snapshot, redact_native_error,
    signal_progress,
};
use eliot_core::{Error, NativeStoreBinding, Result, SecretPolicy, StderrSource};
use eliot_plugin::protocol::ProfileStoreParams;
use eliot_plugin::{DiscoverPage, sync::LockExt};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::{Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::{sync::watch, time::Instant};
use ulid::Ulid;

const MAX_READERS: usize = 4;
const READER_DEADLINE: Duration = Duration::from_secs(25);
const MAX_CURSOR_BYTES: usize = 16 * 1024;
const MAX_PAGE_BYTES: usize = 900 * 1024;

pub(super) struct Reader {
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
            Some("fake" | "codex")
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
                .read_subject_page(profile, environment, native_scope, cursor, limit, stop_rx)
                .await;
            host.finish_metadata_reader(&id, done_tx, completion.cleanup);
            let _ = reply.send(completion.result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "discovery reader stopped"))?
    }

    pub(crate) async fn profile_store(
        &self,
        profile: Value,
        workspace: &str,
    ) -> Result<NativeStoreBinding> {
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
        let (id, stop_rx, done_tx) = self.register_metadata_reader()?;
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

    fn register_metadata_reader(
        &self,
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
        if readers.len() >= MAX_READERS {
            return Err(Error::new(
                "busy",
                "discovery readers are at their in-flight bound",
            ));
        }
        readers.insert(
            id.clone(),
            Reader {
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
    ) -> MetadataRead<NativeStoreBinding> {
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
            reader.initialize(harness, until).await?;
            let request = serde_json::to_value(&params)
                .map_err(|_| Error::new("internal", "cannot encode profile.store params"))?;
            let value = reader.client.call("profile.store", request, until).await?;
            if SecretPolicy::new(metadata_secrets.iter()).contains_value(&value) {
                return Err(Error::new(
                    "source_integrity",
                    "profile.store metadata contains reserved credentials",
                ));
            }
            let binding: NativeStoreBinding = serde_json::from_value(value).map_err(|_| {
                Error::new("source_integrity", "profile.store binding is malformed")
            })?;
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
            Ok(binding)
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
                        "native_scope": native_scope, "cursor": cursor, "limit": limit,
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
        let close_until = metadata_reader_close_deadline(&stop);
        let closure = reader.close_until(self, None, close_until).await;
        let result = require_reader_cleanup(result, closure.closed, "discovery");
        let result = match result {
            Ok(mut value) => {
                if crate::redaction::redact_value(&mut value, &secrets) {
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
            "unavailable",
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

    async fn cancel_after_cleanup(cleanup: ReaderCleanup) -> (bool, usize) {
        let readers = Arc::new(Mutex::new(HashMap::new()));
        let (stop_tx, mut stop_rx) = watch::channel(None);
        let (done_tx, done_rx) = watch::channel(ReaderCleanup::Pending);
        readers.lock_recover().insert(
            "reader".to_owned(),
            Reader {
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
