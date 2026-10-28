//! A bounded read-only plugin instance, independent of every Agent identity.
use super::{
    AgentKey, PluginHost, RPC_GRACE, STOP_GRACE, build_process_environment, redact_native_error,
    signal_progress,
};
use eliot_core::{Error, Result};
use eliot_plugin::DiscoverPage;
use serde_json::{Value, json};
use std::{collections::HashSet, sync::atomic::Ordering, time::Duration};
use tokio::{sync::watch, time::Instant};
use ulid::Ulid;

const MAX_READERS: usize = 4;
const READER_DEADLINE: Duration = Duration::from_secs(25);
const MAX_CURSOR_BYTES: usize = 16 * 1024;
const MAX_PAGE_BYTES: usize = 900 * 1024;

pub(super) struct Reader {
    stop: watch::Sender<bool>,
    done: watch::Receiver<bool>,
}

impl PluginHost {
    pub(crate) fn close_discovery_admission(&self) {
        // Shares the registration lock, so shutdown cannot miss a reader that
        // passed the Router check immediately before its admission mode changed.
        let _readers = self
            .state
            .readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.state.discovery_closed.store(true, Ordering::SeqCst);
    }

    pub(crate) fn reader_count(&self) -> usize {
        let discoveries = self
            .state
            .readers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len();
        discoveries + self.observer_count()
    }

    pub(crate) async fn cancel_discoveries(&self) {
        let readers: Vec<_> = {
            let readers = self
                .state
                .readers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.state.discovery_closed.store(true, Ordering::SeqCst);
            readers
                .values()
                .map(|reader| (reader.stop.clone(), reader.done.clone()))
                .collect()
        };
        for (stop, _) in &readers {
            stop.send_replace(true);
        }
        let limit = Instant::now() + STOP_GRACE + RPC_GRACE * 4;
        for (_, mut done) in readers {
            let _ = tokio::time::timeout_at(limit, done.wait_for(|done| *done)).await;
        }
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
        let id = Ulid::new().to_string();
        let (stop_tx, stop_rx) = watch::channel(false);
        let (done_tx, done_rx) = watch::channel(false);
        {
            let mut readers = self
                .state
                .readers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        }
        let host = self.clone();
        // The owner task survives a disconnected RPC caller long enough to close
        // and reap its scoped process. Daemon force shutdown can cancel it.
        let (reply, response) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = host
                .read_subject_page(profile, environment, native_scope, cursor, limit, stop_rx)
                .await;
            host.state
                .readers
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(&id);
            done_tx.send_replace(true);
            signal_progress(&host.state);
            let _ = reply.send(result);
        });
        response
            .await
            .map_err(|_| Error::new("unavailable", "discovery reader stopped"))?
    }

    #[allow(clippy::too_many_arguments)]
    async fn read_subject_page(
        &self,
        profile: Value,
        environment: Value,
        native_scope: Option<String>,
        cursor: Option<String>,
        limit: usize,
        mut stop: watch::Receiver<bool>,
    ) -> Result<Value> {
        if *stop.borrow() {
            return Err(Error::new(
                "cancelled",
                "discovery was cancelled before spawn",
            ));
        }
        let harness = profile["harness"]
            .as_str()
            .ok_or_else(|| Error::new("invalid", "missing harness"))?;
        let mut reader = self
            .open_reader(
                &AgentKey {
                    id: "discovery".to_owned(),
                    generation: 0,
                },
                harness,
                &environment,
                None,
            )
            .await?;
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
        if !reader.close(self, None).await {
            return Err(Error::new(
                "unavailable",
                "discovery reader cleanup is unconfirmed",
            ));
        }
        match result {
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
            Err(error) => Err(redact_native_error(error, &secrets)),
        }
    }
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
