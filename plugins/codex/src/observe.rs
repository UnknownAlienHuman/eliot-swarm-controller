//! Exact-subject, read-only Codex observation. This module never controls a subject.
//! Private history and normalized live facts have separate cursors.

#[path = "rollout.rs"]
mod rollout;

use crate::{
    discovery::{
        StoreContext, fingerprint, finish_reader, milliseconds, open_pinned_reader, probe_store,
        project_subject,
    },
    live_recovery::RecoveryPublisher,
    native::{
        NativeClient, NativeLaunch, NativeProcess,
        types::{NativeHistoryItemsResponse, NativeObservedThreadReadResponse},
    },
};
use eliot_core::{BatchAck, Error, NativeEvent, Result, SecretPolicy};
use eliot_plugin::{
    MAX_OBSERVATION_PAGE_BYTES, ObservationPage, ObservationQuery, ObserverAttachParams,
    ObserverDetachParams, ObserverSnapshotParams, PeerClient,
    protocol::MAX_OBSERVATION_CURSOR_BYTES,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    task::JoinHandle,
    time::{Instant, timeout_at},
};

const CURSOR_PREFIX: &str = "codex-history-v1:";
const MAX_NATIVE_CURSOR_BYTES: usize = 8 * 1024;
const MAX_ID_BYTES: usize = 256;
const MAX_PATH_BYTES: usize = 4096;
const MAX_PROFILE_BYTES: usize = 128 * 1024;
const MAX_LIVE_RECORDS_PER_POLL: usize = 32;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HistoryCursor {
    version: u8,
    binding: String,
    native_cursor: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryTurnsResponse {
    data: Vec<HistoryTurn>,
    #[serde(default)]
    next_cursor: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct HistoryTurn {
    id: String,
    status: String,
    #[serde(default)]
    started_at: Option<i64>,
    #[serde(default)]
    completed_at: Option<i64>,
    #[serde(default)]
    duration_ms: Option<i64>,
    items: Vec<Value>,
    #[serde(default)]
    items_view: Option<String>,
}

struct SubjectRead {
    projection: Value,
    rollout_path: Option<PathBuf>,
    cli_version: String,
}

enum PendingLive {
    Record {
        events: Vec<NativeEvent>,
        from: String,
        cursor: String,
    },
    Gap {
        reason: String,
        cursor: String,
    },
}

#[derive(Clone, PartialEq)]
enum LiveIntent {
    Begin(String),
    Recover(String),
    Gap {
        reason: String,
        checkpoint: Option<String>,
    },
    Health {
        source: String,
        health: String,
    },
}

pub(crate) struct ObserverSession {
    params: ObserverAttachParams,
    store: StoreContext,
    rollout_path: Option<PathBuf>,
    rollout_cli_version: String,
    secrets: Vec<String>,
    reader: Option<NativeProcess>,
    drainer: Option<JoinHandle<()>>,
    attach_result: Value,
    detach_command_id: Option<String>,
    detach_result: Option<Value>,
    reader_closed: bool,
    shutting_down: bool,
    live_policy: Arc<SecretPolicy>,
    live_rollout: Option<rollout::LiveRollout>,
    live_opening: Option<JoinHandle<Result<rollout::LiveRollout>>>,
    live_publisher: Option<RecoveryPublisher>,
    live_cursor: Option<String>,
    live_baseline: Option<String>,
    live_pending: Option<PendingLive>,
    live_started: bool,
    live_recovery_required: bool,
    live_stale: bool,
    live_reopen: bool,
    live_problem: Option<&'static str>,
    live_intent: Option<LiveIntent>,
}

impl ObserverSession {
    pub(crate) async fn attach(
        params: ObserverAttachParams,
        launch: NativeLaunch,
        secrets: Vec<String>,
        deadline: Instant,
    ) -> Result<(Self, Value)> {
        validate_attach(&params, &secrets)?;
        if encoded_len(&params.profile)? > MAX_PROFILE_BYTES
            || encoded_len(&params.target_environment)? > MAX_PROFILE_BYTES
        {
            return Err(too_large());
        }

        let store = probe_store(&launch, &params.target_environment, &secrets, deadline).await?;
        if store.scope() != params.native_scope {
            return Err(Error::new(
                "source_integrity",
                "Codex observation scope does not match its proven store",
            ));
        }

        let (mut reader, drainer) = open_pinned_reader(
            &launch,
            &params.target_environment,
            &secrets,
            &store,
            deadline,
        )
        .await?;
        let client = reader.client();
        let subject_read = match read_subject(&client, &params.native_ref, deadline).await {
            Ok(subject) => subject,
            Err(error) => {
                drop(client);
                finish_reader(&mut reader, Some(drainer)).await?;
                return Err(reader.client().diagnostic_error(error));
            }
        };
        drop(client);
        let SubjectRead {
            projection: subject,
            rollout_path,
            cli_version,
        } = subject_read;
        if let Err(error) = validate_live_codec(&cli_version) {
            finish_reader(&mut reader, Some(drainer)).await?;
            return Err(reader.client().diagnostic_error(error));
        }

        let metadata = match native_store_identity(&store).map(|identity| {
            json!({
                "subject": subject,
                "native_store_identity": identity,
            })
        }) {
            Ok(metadata) => metadata,
            Err(error) => {
                finish_reader(&mut reader, Some(drainer)).await?;
                return Err(reader.client().diagnostic_error(error));
            }
        };
        let attach_result = match bounded(
            json!({
                "agent_id": params.agent_id,
                "generation": params.generation,
                "command_id": params.command_id,
                "native_scope": params.native_scope,
                "native_ref": params.native_ref,
                "state": "attached",
                "observation_state": "unknown",
                "metadata": metadata,
                "coverage": {"kind": "native_metadata", "complete": true, "live": false},
            }),
            &secrets,
        ) {
            Ok(result) => result,
            Err(error) => {
                finish_reader(&mut reader, Some(drainer)).await?;
                return Err(reader.client().diagnostic_error(error));
            }
        };

        let live_policy = Arc::new(SecretPolicy::new(&secrets));
        let session = Self {
            params,
            store,
            rollout_path,
            rollout_cli_version: cli_version,
            secrets,
            reader: Some(reader),
            drainer: Some(drainer),
            attach_result: attach_result.clone(),
            detach_command_id: None,
            detach_result: None,
            reader_closed: false,
            shutting_down: false,
            live_policy,
            live_rollout: None,
            live_opening: None,
            live_publisher: None,
            live_cursor: None,
            live_baseline: None,
            live_pending: None,
            live_started: false,
            live_recovery_required: false,
            live_stale: false,
            live_reopen: false,
            live_problem: None,
            live_intent: None,
        };
        Ok((session, attach_result))
    }

    /// Return the original successful attach result for an exact command retry.
    pub(crate) fn repeat_attach(&self, params: &ObserverAttachParams) -> Result<Value> {
        self.binding(
            &params.agent_id,
            params.generation,
            &params.native_scope,
            &params.native_ref,
        )?;
        if params.command_id != self.params.command_id
            || params.mode != self.params.mode
            || params.profile != self.params.profile
            || params.target_environment != self.params.target_environment
            || params.source_plan != self.params.source_plan
            || params.source_state != self.params.source_state
        {
            return Err(Error::new(
                "id_conflict",
                "Codex observer attach id was reused with another binding",
            ));
        }
        Ok(self.attach_result.clone())
    }

    pub(crate) async fn snapshot(
        &mut self,
        params: &ObserverSnapshotParams,
        deadline: Instant,
    ) -> Result<Value> {
        let result = self.snapshot_inner(params, deadline).await;
        result.map_err(|error| match &self.reader {
            Some(reader) => reader.client().diagnostic_error(error),
            None => error,
        })
    }

    async fn snapshot_inner(
        &mut self,
        params: &ObserverSnapshotParams,
        deadline: Instant,
    ) -> Result<Value> {
        self.binding(
            &params.agent_id,
            params.generation,
            &params.native_scope,
            &params.native_ref,
        )?;
        if self.reader_closed || self.shutting_down || self.detach_command_id.is_some() {
            return Err(Error::new("not_found", "Codex observer reader is closed"));
        }
        params.observation.validate()?;
        let query = &params.observation;
        if !matches!(query.source.as_str(), "native" | "rollout") {
            return Err(unsupported());
        }
        match query.view.as_str() {
            "subject" if query.turn_id.is_some() || query.cursor.is_some() => {
                return Err(invalid());
            }
            "turns" if query.turn_id.is_some() => return Err(invalid()),
            "items" => validate_id(query.turn_id.as_deref().ok_or_else(invalid)?)?,
            _ => {}
        }
        if let Some(turn_id) = &query.turn_id {
            reject_immutable_secret(turn_id, &self.secrets)?;
        }

        let binding = self.page_binding(query)?;
        if Instant::now() >= deadline {
            return Err(Error::new(
                "native_not_dispatched",
                "Codex observation deadline elapsed before read dispatch",
            ));
        }
        let client = self
            .reader
            .as_ref()
            .map(|reader| reader.client())
            .ok_or_else(|| Error::new("not_found", "Codex observer reader is unavailable"))?;
        let (entries, next_cursor, coverage) = if query.source == "native" {
            let cursor = decode_cursor(query.cursor.as_deref(), &binding)?;
            if let Some(cursor) = &cursor {
                reject_immutable_secret(&cursor.native_cursor, &self.secrets)?;
            }
            let (entries, next_cursor) = self
                .read_native_entries(&client, query, cursor.as_ref(), &binding, deadline)
                .await?;
            let complete = next_cursor.is_none();
            (
                entries,
                next_cursor,
                json!({
                    "kind": if query.view == "subject" {"native_metadata"} else {"persisted_history"},
                    "complete": complete,
                    "live": false,
                }),
            )
        } else {
            let path = self.rollout_path.clone().ok_or_else(rollout_unavailable)?;
            let home = self.store.home().to_path_buf();
            let native_ref = self.params.native_ref.clone();
            let cli_version = self.rollout_cli_version.clone();
            let binding = binding.clone();
            let secrets = self.secrets.clone();
            let std_deadline = deadline.into_std();
            let request = rollout::RolloutReadRequest {
                path,
                codex_home: home,
                native_ref,
                cli_version,
                query: (*query).clone(),
                binding,
                secrets,
                deadline: std_deadline,
            };
            let page = tokio::task::spawn_blocking(move || rollout::read_page(request))
                .await
                .map_err(|_| Error::new("unknown", "Codex rollout reader task failed"))??;
            (page.entries, page.next_cursor, page.coverage)
        };
        let page = ObservationPage {
            agent_id: self.params.agent_id.clone(),
            generation: self.params.generation,
            native_scope: self.params.native_scope.clone(),
            native_ref: self.params.native_ref.clone(),
            source: query.source.clone(),
            view: query.view.clone(),
            entries,
            next_cursor,
            coverage,
        };
        page.validate()?;
        bounded(
            serde_json::to_value(page).map_err(|_| integrity())?,
            &self.secrets,
        )
    }

    pub(crate) fn live_poll_enabled(&self) -> bool {
        !self.reader_closed && !self.shutting_down && self.detach_command_id.is_none()
    }

    pub(crate) fn live_poll_timed_out(&mut self) {
        self.live_stale = true;
    }

    pub(crate) async fn poll_live(
        &mut self,
        peer: &PeerClient,
        credential: &str,
        deadline: Instant,
    ) -> Result<()> {
        if !self.live_poll_enabled() || Instant::now() >= deadline {
            return Ok(());
        }
        // observe opens during dispatch. Only the settled attach/fence proof
        // grants live publication; false is a wait, not an uncertain operation.
        if self.live_publisher.is_none() {
            self.live_publisher = Some(RecoveryPublisher::new(
                &self.params,
                peer.clone(),
                credential.to_owned(),
                self.live_policy.clone(),
            )?);
        }
        if !self
            .live_publisher
            .as_mut()
            .ok_or_else(live_unknown)?
            .wait_observer_ready(deadline)
            .await?
        {
            return Ok(());
        }
        let result = self.poll_live_inner(deadline).await;
        if result.is_err() {
            // Report after pending publication settles; never turn a lost ACK
            // into a different batch or a speculative gap operation.
            self.live_stale = true;
        }
        result
    }

    async fn poll_live_inner(&mut self, deadline: Instant) -> Result<()> {
        if let Some(ack) = self
            .live_publisher
            .as_mut()
            .ok_or_else(live_unknown)?
            .settle(deadline)
            .await?
        {
            self.live_cursor = Some(settled_record_cursor(self.live_pending.as_ref(), &ack)?);
            self.live_pending = None;
        }
        self.resume_live_intent(deadline).await?;
        self.finish_live_pending(deadline).await?;
        if let Some(reason) = self.live_problem {
            self.publish_live_intent(
                live_problem_intent(reason, self.live_started, self.live_recovery_required),
                deadline,
            )
            .await?;
        }
        if self.live_stale {
            self.publish_live_health(
                if self.live_started {
                    "transcript"
                } else {
                    "observe"
                },
                "stale",
                deadline,
            )
            .await?;
            self.live_stale = false;
        }
        if self.live_reopen {
            if let Some(tail) = self.live_rollout.as_mut() {
                tail.close().await?;
            }
            self.live_rollout = None;
            self.live_cursor = None;
            self.live_baseline = None;
            self.rollout_path = None;
            self.live_reopen = false;
        }
        self.open_live_rollout(deadline).await?;
        self.finish_live_baseline(deadline).await?;
        for _ in 0..MAX_LIVE_RECORDS_PER_POLL {
            if Instant::now() >= deadline {
                return Ok(());
            }
            let from = self.live_cursor.clone().ok_or_else(live_unknown)?;
            let read = self
                .live_rollout
                .as_mut()
                .ok_or_else(live_unknown)?
                .read_page(&from, 1, deadline.into_std())
                .await;
            let page = match read {
                Ok(page) => page,
                Err(error) if Instant::now() >= deadline => return Err(error),
                Err(error) => {
                    self.live_problem = Some("observer rollout identity or read is unavailable");
                    self.live_reopen = true;
                    return Err(error);
                }
            };
            if let Some(reason) = page.gap {
                if !page.records.is_empty() {
                    return Err(integrity());
                }
                self.live_pending = Some(PendingLive::Gap {
                    reason,
                    cursor: page.cursor,
                });
                self.finish_live_pending(deadline).await?;
                return Ok(());
            }
            if page.records.len() > 1 {
                return Err(integrity());
            }
            if let Some(record) = page.records.into_iter().next() {
                if record.cursor != page.cursor
                    || record.cursor == from
                    || record.events.is_empty()
                    || record
                        .events
                        .iter()
                        .any(|event| event.ts.is_none() || event.data.get("command_id").is_some())
                {
                    return Err(integrity());
                }
                // Latch BEFORE any await. Reader turn context may already have
                // advanced, so this record must settle before any further read.
                self.live_pending = Some(PendingLive::Record {
                    events: record.events,
                    from,
                    cursor: record.cursor,
                });
                self.finish_live_pending(deadline).await?;
            } else {
                if page.cursor == from && !page.at_end && !page.partial_tail {
                    return Err(integrity());
                }
                self.live_cursor = Some(page.cursor);
            }
            self.publish_live_health(
                "transcript",
                if self.live_recovery_required {
                    "stale"
                } else if page.at_end || page.partial_tail {
                    "ok"
                } else {
                    "slow"
                },
                deadline,
            )
            .await?;
            if page.at_end || page.partial_tail {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn finish_live_baseline(&mut self, deadline: Instant) -> Result<()> {
        if let Some(edge) = self.live_baseline.clone() {
            let intent = if self.live_started {
                LiveIntent::Recover(edge)
            } else {
                LiveIntent::Begin(edge)
            };
            self.publish_live_intent(intent, deadline).await?;
        }
        Ok(())
    }

    async fn publish_live_health(
        &mut self,
        source: &str,
        health: &str,
        deadline: Instant,
    ) -> Result<()> {
        self.publish_live_intent(
            LiveIntent::Health {
                source: source.to_owned(),
                health: health.to_owned(),
            },
            deadline,
        )
        .await
    }

    async fn publish_live_intent(&mut self, intent: LiveIntent, deadline: Instant) -> Result<()> {
        if self
            .live_intent
            .as_ref()
            .is_some_and(|pending| *pending != intent)
        {
            return Err(live_unknown());
        }
        self.live_intent = Some(intent);
        self.resume_live_intent(deadline).await
    }

    async fn resume_live_intent(&mut self, deadline: Instant) -> Result<()> {
        let Some(intent) = self.live_intent.clone() else {
            return Ok(());
        };
        let publisher = self.live_publisher.as_mut().ok_or_else(live_unknown)?;
        match &intent {
            LiveIntent::Begin(edge) => publisher.begin(edge, deadline).await?,
            LiveIntent::Recover(edge) => publisher.recover(edge, deadline).await?,
            LiveIntent::Gap { reason, checkpoint } => {
                let committed = publisher
                    .unavailable(reason, checkpoint.clone(), deadline)
                    .await?;
                if committed != *checkpoint {
                    return Err(live_unknown());
                }
            }
            LiveIntent::Health { source, health } => {
                publisher.health(source, health, deadline).await?
            }
        }
        // Nothing mutates the original intent or the pending record across the
        // await above. Apply its local checkpoint only after its exact proof.
        self.live_intent = None;
        acknowledge_live_outage(
            &intent,
            &mut self.live_problem,
            &mut self.live_recovery_required,
        );
        match intent {
            LiveIntent::Begin(edge) => {
                self.live_cursor = Some(edge);
                self.live_baseline = None;
                self.live_started = true;
            }
            LiveIntent::Recover(edge) => {
                if self.live_baseline.as_deref() == Some(edge.as_str()) {
                    self.live_cursor = Some(edge);
                    self.live_baseline = None;
                }
            }
            LiveIntent::Gap { reason, checkpoint } => {
                if let (
                    Some(PendingLive::Gap {
                        reason: pending_reason,
                        cursor,
                    }),
                    Some(checkpoint),
                ) = (self.live_pending.as_ref(), checkpoint)
                {
                    if *pending_reason != reason || *cursor != checkpoint {
                        return Err(live_unknown());
                    }
                    self.live_reopen |= self.live_cursor.as_deref() == Some(cursor.as_str());
                    self.live_cursor = Some(checkpoint);
                    self.live_pending = None;
                }
            }
            LiveIntent::Health { health, .. } => {
                if health == "stale" {
                    self.live_stale = false;
                }
            }
        }
        Ok(())
    }

    async fn finish_live_pending(&mut self, deadline: Instant) -> Result<()> {
        match self.live_pending.as_ref() {
            Some(PendingLive::Record {
                events,
                from,
                cursor,
            }) => {
                let events = events.clone();
                let from = from.clone();
                let cursor = cursor.clone();
                if self.live_recovery_required {
                    self.publish_live_intent(LiveIntent::Recover(from), deadline)
                        .await?;
                }
                let ack = self
                    .live_publisher
                    .as_mut()
                    .ok_or_else(live_unknown)?
                    .commit(events, cursor.clone(), deadline)
                    .await?;
                self.live_cursor = Some(settled_record_cursor(self.live_pending.as_ref(), &ack)?);
            }
            Some(PendingLive::Gap { reason, cursor }) => {
                let reason = reason.clone();
                let cursor = cursor.clone();
                self.publish_live_intent(
                    LiveIntent::Gap {
                        reason,
                        checkpoint: Some(cursor),
                    },
                    deadline,
                )
                .await?;
            }
            None => return Ok(()),
        }
        self.live_pending = None;
        Ok(())
    }

    async fn open_live_rollout(&mut self, deadline: Instant) -> Result<()> {
        if self.live_rollout.is_some() {
            return Ok(());
        }
        if self.live_opening.is_none() {
            if self.rollout_path.is_none() {
                let client = self.reader.as_ref().ok_or_else(live_unknown)?.client();
                let subject = match read_subject(&client, &self.params.native_ref, deadline).await {
                    Ok(subject) => subject,
                    Err(error) => {
                        self.live_problem = Some("observer exact subject metadata is unavailable");
                        return Err(error);
                    }
                };
                if subject.cli_version != self.rollout_cli_version {
                    self.live_problem = Some("observer rollout codec version changed");
                    return Err(integrity());
                }
                self.rollout_path = subject.rollout_path;
            }
            let Some(path) = self.rollout_path.clone() else {
                self.live_problem = Some("observer exact subject has no rollout path");
                return Err(rollout_unavailable());
            };
            let home = self.store.home().to_path_buf();
            let native_ref = self.params.native_ref.clone();
            let version = self.rollout_cli_version.clone();
            let binding = self.live_binding()?;
            let policy = self.live_policy.clone();
            self.live_opening = Some(tokio::task::spawn_blocking(move || {
                rollout::LiveRollout::open(
                    path,
                    home,
                    native_ref,
                    version,
                    binding,
                    policy,
                    deadline.into_std(),
                )
            }));
        }
        let opened = timeout_at(
            deadline,
            self.live_opening.as_mut().ok_or_else(live_unknown)?,
        )
        .await;
        match opened {
            Ok(result) => {
                self.live_opening = None;
                match result {
                    Ok(Ok(tail)) => {
                        self.live_baseline = Some(tail.baseline_cursor().to_owned());
                        self.live_rollout = Some(tail);
                        Ok(())
                    }
                    _ => {
                        self.live_problem = Some("observer rollout live edge is unavailable");
                        Err(rollout_unavailable())
                    }
                }
            }
            Err(_) => Err(Error::new("busy", "observer live opening deadline elapsed")),
        }
    }

    fn live_binding(&self) -> Result<String> {
        fingerprint(json!({
            "domain":"eliot/codex/observer-live/v1", "agent_id":self.params.agent_id,
            "generation":self.params.generation, "native_scope":self.store.scope(),
            "native_ref":self.params.native_ref, "profile":self.params.profile,
            "target_environment":self.params.target_environment, "store_binding":self.store.binding(),
            "cli_version":self.rollout_cli_version, "source":"transcript", "mechanism":"rollout_tail",
        }))
    }

    pub(crate) async fn detach(
        &mut self,
        params: &ObserverDetachParams,
        _deadline: Instant,
    ) -> Result<Value> {
        self.binding(
            &params.agent_id,
            params.generation,
            &params.native_scope,
            &params.native_ref,
        )?;
        validate_id(&params.command_id)?;
        reject_immutable_secret(&params.command_id, &self.secrets)?;

        if let Some(command_id) = &self.detach_command_id
            && command_id != &params.command_id
        {
            return Err(Error::new(
                "id_conflict",
                "Codex observer detach is bound to another command",
            ));
        }
        if let Some(result) = &self.detach_result {
            return Ok(result.clone());
        }
        self.detach_command_id = Some(params.command_id.clone());

        let state = if self.close_reader().await.is_ok() {
            "detached"
        } else {
            "unknown"
        };
        let result = bounded(
            json!({
                "agent_id": params.agent_id,
                "generation": params.generation,
                "command_id": params.command_id,
                "native_scope": params.native_scope,
                "native_ref": params.native_ref,
                "state": state,
                "observation_state": "unknown",
            }),
            &self.secrets,
        )?;
        self.detach_result = Some(result.clone());
        Ok(result)
    }

    pub(crate) async fn shutdown(&mut self, _deadline: Instant) -> Result<()> {
        self.shutting_down = true;
        self.close_reader().await
    }

    async fn close_reader(&mut self) -> Result<()> {
        let live_result = self.close_live().await;
        let Some(reader) = self.reader.as_mut() else {
            self.reader_closed = true;
            return live_result;
        };
        let drainer = self.drainer.take();
        let reader_result = finish_reader(reader, drainer).await;
        if reader_result.is_ok() {
            self.reader = None;
            self.reader_closed = true;
        }
        live_result.and(reader_result)
    }

    async fn close_live(&mut self) -> Result<()> {
        // Keep the opening handle in self while awaiting so another cancelled
        // cleanup still has its owner. A completed opening owns a file too.
        if let Some(opening) = self.live_opening.as_mut() {
            let result = opening.await;
            self.live_opening = None;
            if let Ok(Ok(tail)) = result {
                self.live_rollout = Some(tail);
            }
        }
        if let Some(tail) = self.live_rollout.as_mut() {
            tail.close().await?;
        }
        self.live_rollout = None;
        self.live_pending = None;
        Ok(())
    }

    fn binding(&self, agent: &str, generation: i64, scope: &str, native_ref: &str) -> Result<()> {
        if agent != self.params.agent_id || generation != self.params.generation {
            return Err(Error::new(
                "stale_generation",
                "Codex observer identity does not match its attachment",
            ));
        }
        if scope != self.params.native_scope || native_ref != self.params.native_ref {
            return Err(Error::new(
                "source_integrity",
                "Codex observer subject does not match its attachment",
            ));
        }
        Ok(())
    }

    fn page_binding(&self, query: &ObservationQuery) -> Result<String> {
        let method = match (query.source.as_str(), query.view.as_str()) {
            ("rollout", _) => "rollout/jsonl",
            (_, "subject") => "thread/read",
            (_, "turns") => "thread/turns/list",
            (_, "items") => "thread/items/list",
            _ => return Err(invalid()),
        };
        let direction = if query.source == "rollout" {
            "asc"
        } else if query.view == "turns" {
            "desc"
        } else {
            "asc"
        };
        let mut context = json!({
            "domain": "eliot/codex/history/v1",
            "agent_id": self.params.agent_id,
            "generation": self.params.generation,
            "native_scope": self.store.scope(),
            "native_ref": self.params.native_ref,
            "profile": self.params.profile,
            "target_environment": self.params.target_environment,
            "store_binding": self.store.binding(),
            "source": query.source,
            "method": method,
            "view": query.view,
            "turn_id": query.turn_id,
            "limit": query.limit,
            "direction": direction,
            "items_view": "notLoaded",
        });
        if query.source == "rollout" {
            if let Some(object) = context.as_object_mut() {
                object.remove("items_view");
            }
            context["rollout_cli_version"] = json!(self.rollout_cli_version);
        }
        fingerprint(context)
    }

    async fn read_native_entries(
        &self,
        client: &NativeClient,
        query: &ObservationQuery,
        cursor: Option<&HistoryCursor>,
        binding: &str,
        deadline: Instant,
    ) -> Result<(Vec<Value>, Option<String>)> {
        let (entries, next_cursor) = match query.view.as_str() {
            "subject" => (
                vec![
                    read_subject(client, &self.params.native_ref, deadline)
                        .await?
                        .projection,
                ],
                None,
            ),
            "turns" => self.read_turns(client, query, cursor, deadline).await?,
            "items" => self.read_items(client, query, cursor, deadline).await?,
            _ => return Err(invalid()),
        };
        let next_cursor = match next_cursor {
            Some(next) => {
                validate_native_cursor(&next)?;
                if cursor.is_some_and(|cursor| cursor.native_cursor == next) {
                    return Err(integrity());
                }
                reject_immutable_secret(&next, &self.secrets)?;
                let encoded = format!(
                    "{CURSOR_PREFIX}{}",
                    serde_json::to_string(&HistoryCursor {
                        version: 1,
                        binding: binding.to_owned(),
                        native_cursor: next,
                    })
                    .map_err(|_| integrity())?
                );
                if encoded.len() > MAX_OBSERVATION_CURSOR_BYTES {
                    return Err(too_large());
                }
                Some(encoded)
            }
            None => None,
        };
        Ok((entries, next_cursor))
    }

    async fn read_turns(
        &self,
        client: &NativeClient,
        query: &ObservationQuery,
        cursor: Option<&HistoryCursor>,
        deadline: Instant,
    ) -> Result<(Vec<Value>, Option<String>)> {
        let raw = client
            .call(
                "thread/turns/list",
                json!({
                    "threadId": self.params.native_ref,
                    "cursor": cursor.map(|cursor| &cursor.native_cursor),
                    "limit": query.limit,
                    "sortDirection": "desc",
                    "itemsView": "notLoaded",
                }),
                deadline,
            )
            .await
            .map_err(history_error)?;
        let response: HistoryTurnsResponse =
            serde_json::from_value(raw).map_err(|_| integrity())?;
        if response.data.len() > query.limit {
            return Err(integrity());
        }
        let mut ids = HashSet::with_capacity(response.data.len());
        let mut entries = Vec::with_capacity(response.data.len());
        for turn in response.data {
            validate_id(&turn.id)?;
            if !ids.insert(turn.id.clone())
                || turn
                    .items_view
                    .as_deref()
                    .is_some_and(|view| view != "notLoaded")
                || !turn.items.is_empty()
                || !matches!(
                    turn.status.as_str(),
                    "completed" | "failed" | "interrupted" | "inProgress"
                )
                || turn.duration_ms.is_some_and(|duration| duration < 0)
            {
                return Err(integrity());
            }
            reject_immutable_secret(&turn.id, &self.secrets)?;
            let value = json!({
                "turn_id": turn.id,
                "status": if turn.status == "inProgress" {"in_progress"} else {&turn.status},
                "started_at": turn.started_at.map(milliseconds).transpose()?,
                "completed_at": turn.completed_at.map(milliseconds).transpose()?,
                "duration_ms": turn.duration_ms,
            });
            entries.push(value);
        }
        Ok((entries, response.next_cursor))
    }

    async fn read_items(
        &self,
        client: &NativeClient,
        query: &ObservationQuery,
        cursor: Option<&HistoryCursor>,
        deadline: Instant,
    ) -> Result<(Vec<Value>, Option<String>)> {
        let turn_id = query.turn_id.as_deref().ok_or_else(invalid)?;
        let raw = client
            .call(
                "thread/items/list",
                json!({
                    "threadId": self.params.native_ref,
                    "turnId": turn_id,
                    "cursor": cursor.map(|cursor| &cursor.native_cursor),
                    "limit": query.limit,
                    "sortDirection": "asc",
                }),
                deadline,
            )
            .await
            .map_err(history_error)?;
        let response: NativeHistoryItemsResponse =
            serde_json::from_value(raw).map_err(|_| integrity())?;
        if response.data.len() > query.limit {
            return Err(integrity());
        }
        let mut ids = HashSet::with_capacity(response.data.len());
        let mut entries = Vec::with_capacity(response.data.len());
        for entry in response.data {
            validate_id(&entry.turn_id)?;
            validate_id(&entry.item.id)?;
            if entry.turn_id != turn_id || !ids.insert(entry.item.id.clone()) {
                return Err(integrity());
            }
            reject_immutable_secret(&entry.turn_id, &self.secrets)?;
            reject_immutable_secret(&entry.item.id, &self.secrets)?;
            let value = json!({
                "turn_id": entry.turn_id,
                "item_id": entry.item.id,
                "kind": item_kind(&entry.item.kind),
                "status": entry.item.status.as_deref().map(item_status),
                "started_at": entry.started_at_ms,
                "completed_at": entry.completed_at_ms,
            });
            entries.push(value);
        }
        Ok((entries, response.next_cursor))
    }
}

fn validate_attach(params: &ObserverAttachParams, secrets: &[String]) -> Result<()> {
    if params.mode != "observe"
        || !params.profile.is_object()
        || !params.target_environment.is_object()
        || params.generation <= 0
    {
        return Err(invalid());
    }
    for value in [
        params.agent_id.as_str(),
        params.command_id.as_str(),
        params.native_scope.as_str(),
        params.native_ref.as_str(),
    ] {
        validate_id(value)?;
        reject_immutable_secret(value, secrets)?;
    }
    Ok(())
}

async fn read_subject(
    client: &NativeClient,
    native_ref: &str,
    deadline: Instant,
) -> Result<SubjectRead> {
    let raw = client
        .call(
            "thread/read",
            json!({"threadId": native_ref, "includeTurns": false}),
            deadline,
        )
        .await
        .map_err(history_error)?;
    let cli_version = raw
        .pointer("/thread/cliVersion")
        .and_then(Value::as_str)
        .filter(|version| !version.trim().is_empty())
        .ok_or_else(integrity)?
        .to_owned();
    validate_id(&cli_version)?;
    let response: NativeObservedThreadReadResponse =
        serde_json::from_value(raw).map_err(|_| integrity())?;
    if response.thread.metadata.id != native_ref {
        return Err(integrity());
    }
    if let Some(path) = &response.thread.path {
        validate_path(path)?;
    }
    let native_path = response.thread.path;
    let rollout_path = native_path.as_deref().map(PathBuf::from);
    let mut subject = serde_json::to_value(project_subject(response.thread.metadata)?)
        .map_err(|_| integrity())?;
    let object = subject.as_object_mut().ok_or_else(integrity)?;
    object.insert("native_path".to_owned(), json!(native_path));
    object.insert("cli_version".to_owned(), json!(cli_version.clone()));
    Ok(SubjectRead {
        projection: subject,
        rollout_path,
        cli_version,
    })
}

fn native_store_identity(store: &StoreContext) -> Result<Value> {
    let home = store.home().to_str().ok_or_else(unverified_store)?;
    let root = store.root().to_str().ok_or_else(unverified_store)?;
    validate_path(home)?;
    validate_path(root)?;
    validate_id(store.scope())?;
    validate_id(store.binding())?;
    Ok(json!({
        "scope": store.scope(),
        "binding": store.binding(),
        "home": home,
        "root": root,
    }))
}

fn decode_cursor(raw: Option<&str>, binding: &str) -> Result<Option<HistoryCursor>> {
    let Some(raw) = raw else { return Ok(None) };
    if raw.len() > MAX_OBSERVATION_CURSOR_BYTES {
        return Err(too_large());
    }
    let cursor: HistoryCursor = serde_json::from_str(
        raw.strip_prefix(CURSOR_PREFIX)
            .ok_or_else(cursor_mismatch)?,
    )
    .map_err(|_| cursor_mismatch())?;
    if cursor.native_cursor.trim().is_empty() || cursor.native_cursor.chars().any(char::is_control)
    {
        return Err(cursor_mismatch());
    }
    if cursor.native_cursor.len() > MAX_NATIVE_CURSOR_BYTES {
        return Err(too_large());
    }
    if cursor.version != 1 || cursor.binding != binding {
        return Err(cursor_mismatch());
    }
    Ok(Some(cursor))
}

fn item_kind(kind: &str) -> &str {
    match kind {
        "userMessage"
        | "hookPrompt"
        | "agentMessage"
        | "functionCallOutput"
        | "plan"
        | "reasoning"
        | "commandExecution"
        | "fileChange"
        | "mcpToolCall"
        | "dynamicToolCall"
        | "collabAgentToolCall"
        | "subAgentActivity"
        | "webSearch"
        | "imageView"
        | "sleep"
        | "imageGeneration"
        | "enteredReviewMode"
        | "exitedReviewMode"
        | "contextCompaction" => kind,
        _ => "unknown",
    }
}

fn item_status(status: &str) -> &str {
    match status {
        "inProgress" => "in_progress",
        "completed" | "failed" | "declined" | "interrupted" => status,
        _ => "unknown",
    }
}

fn validate_id(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > MAX_ID_BYTES || value.chars().any(char::is_control)
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_native_cursor(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.chars().any(char::is_control) {
        return Err(integrity());
    }
    if value.len() > MAX_NATIVE_CURSOR_BYTES {
        return Err(too_large());
    }
    Ok(())
}

fn validate_path(value: &str) -> Result<()> {
    if value.trim().is_empty()
        || value.len() > MAX_PATH_BYTES
        || value.chars().any(char::is_control)
        || !Path::new(value).is_absolute()
    {
        return Err(integrity());
    }
    Ok(())
}

fn bounded(mut value: Value, secrets: &[String]) -> Result<Value> {
    sanitize_value(&mut value, secrets, false)?;
    if encoded_len(&value)? > MAX_OBSERVATION_PAGE_BYTES {
        return Err(too_large());
    }
    Ok(value)
}

fn sanitize_value(value: &mut Value, secrets: &[String], protected: bool) -> Result<()> {
    match value {
        Value::String(text) => sanitize_text(text, secrets, protected),
        Value::Array(values) => {
            for value in values {
                sanitize_value(value, secrets, protected)?;
            }
            Ok(())
        }
        Value::Object(values) => {
            for (key, value) in values {
                reject_immutable_secret(key, secrets)?;
                sanitize_value(value, secrets, protected || protected_key(key))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn sanitize_text(value: &mut String, secrets: &[String], protected: bool) -> Result<()> {
    let policy = SecretPolicy::new(secrets.iter().cloned());
    if protected && policy.contains_text(value) {
        return Err(secret_in_payload());
    }
    *value = policy.redact_text(value);
    Ok(())
}

fn reject_immutable_secret(value: &str, secrets: &[String]) -> Result<()> {
    let mut value = value.to_owned();
    sanitize_text(&mut value, secrets, true)
}

fn protected_key(key: &str) -> bool {
    matches!(
        key,
        "agent_id"
            | "command_id"
            | "native_scope"
            | "native_ref"
            | "scope"
            | "binding"
            | "store_binding"
            | "home"
            | "root"
            | "turn_id"
            | "item_id"
            | "next_cursor"
            | "cursor"
    )
}

fn encoded_len(value: &Value) -> Result<usize> {
    serde_json::to_vec(value)
        .map(|bytes| bytes.len())
        .map_err(|_| integrity())
}

fn history_error(error: Error) -> Error {
    let message = error.message.to_ascii_lowercase();
    if error.code == "native_rejected"
        && (message.contains("method not found") || message.contains("unknown method"))
    {
        return error
            .with_code("unsupported")
            .with_message("Codex native history read is unsupported by this app-server");
    }
    error
        .with_code("history_gap")
        .with_message("Codex exact persisted readback is unavailable or unsupported")
}

fn invalid() -> Error {
    Error::new("invalid", "Codex observation request is invalid")
}
fn integrity() -> Error {
    Error::new(
        "source_integrity",
        "Codex observation metadata does not match the exact subject or page",
    )
}
fn unsupported() -> Error {
    Error::new(
        "unsupported",
        "Codex observation source or native paging operation is unsupported",
    )
}
fn too_large() -> Error {
    Error::new(
        "frame_too_large",
        "Codex observation data exceeds its configured bound",
    )
}
fn cursor_mismatch() -> Error {
    Error::new(
        "cursor_mismatch",
        "Codex history cursor does not match its exact subject, generation, profile, store or query",
    )
}
fn rollout_unavailable() -> Error {
    Error::new(
        "history_gap",
        "Codex thread/read did not provide an exact rollout path",
    )
}

fn live_unknown() -> Error {
    Error::new(
        "unknown",
        "Codex observer live publication has no verified current boundary",
    )
}

fn live_problem_intent(reason: &str, started: bool, gap_acknowledged: bool) -> LiveIntent {
    if started && !gap_acknowledged {
        LiveIntent::Gap {
            reason: reason.to_owned(),
            checkpoint: None,
        }
    } else {
        // Before handoff there is no transcript. After gap acknowledgement,
        // retain that same uncovered interval until a verified file edge.
        LiveIntent::Health {
            source: if started { "transcript" } else { "observe" }.to_owned(),
            health: "stale".to_owned(),
        }
    }
}

fn acknowledge_live_outage(
    intent: &LiveIntent,
    problem: &mut Option<&'static str>,
    recovery_required: &mut bool,
) {
    match intent {
        LiveIntent::Gap { .. } => *recovery_required = true,
        LiveIntent::Begin(_) | LiveIntent::Recover(_) => {
            *problem = None;
            *recovery_required = false;
        }
        LiveIntent::Health { .. } => {}
    }
}

fn validate_live_codec(version: &str) -> Result<()> {
    if version != "0.161.0" {
        return Err(Error::new(
            "profile_error",
            "Codex live observation requires the supported stable 0.161.0 codec",
        ));
    }
    Ok(())
}

fn settled_record_cursor(pending: Option<&PendingLive>, ack: &BatchAck) -> Result<String> {
    match pending {
        Some(PendingLive::Record { cursor, .. }) if ack.cursor.as_ref() == Some(cursor) => {
            Ok(cursor.clone())
        }
        _ => Err(live_unknown()),
    }
}
fn secret_in_payload() -> Error {
    Error::new(
        "secret_in_payload",
        "Codex observation binding contains a known secret",
    )
}
fn unverified_store() -> Error {
    Error::new(
        "unverified_store",
        "Codex observation store identity could not be represented exactly",
    )
}

#[cfg(test)]
mod tests {
    use super::{
        LiveIntent, PendingLive, acknowledge_live_outage, live_problem_intent, sanitize_text,
        settled_record_cursor, validate_live_codec,
    };
    use eliot_core::{BatchAck, NativeEvent};
    use serde_json::json;

    #[test]
    fn acknowledged_outage_retains_problem_until_verified_edge() {
        let mut problem = Some("observer rollout live edge is unavailable");
        let mut recovery_required = false;
        let first = live_problem_intent(problem.unwrap(), true, recovery_required);
        assert!(matches!(
            first,
            LiveIntent::Gap {
                checkpoint: None,
                ..
            }
        ));
        // Local coordinator decisions only. The runtime calls this transition
        // after publisher success; transport ACK/nonce proofs live in its tests.
        acknowledge_live_outage(&first, &mut problem, &mut recovery_required);
        assert!(problem.is_some());
        assert!(recovery_required);

        for _ in 0..4 {
            // The real open/read failure path can assign the same problem again.
            problem = Some("observer rollout live edge is unavailable");
            let intent = live_problem_intent(problem.unwrap(), true, recovery_required);
            assert!(matches!(&intent, LiveIntent::Health { source, health }
                if source == "transcript" && health == "stale"));
            acknowledge_live_outage(&intent, &mut problem, &mut recovery_required);
            assert!(problem.is_some());
            assert!(recovery_required);
        }

        acknowledge_live_outage(
            &LiveIntent::Recover("verified-held-file-edge".to_owned()),
            &mut problem,
            &mut recovery_required,
        );
        assert!(problem.is_none());
        assert!(!recovery_required);

        problem = Some("observer rollout live edge is unavailable");
        let next = live_problem_intent(problem.unwrap(), true, recovery_required);
        assert!(matches!(
            next,
            LiveIntent::Gap {
                checkpoint: None,
                ..
            }
        ));
        acknowledge_live_outage(&next, &mut problem, &mut recovery_required);
        assert!(problem.is_some());
        assert!(recovery_required);
    }

    #[test]
    fn prebaseline_unavailability_stays_on_observe_until_verified_begin() {
        let mut problem = Some("observer exact subject has no rollout path");
        let mut recovery_required = false;
        let intent = live_problem_intent(problem.unwrap(), false, recovery_required);
        assert!(matches!(&intent, LiveIntent::Health { source, health }
            if source == "observe" && health == "stale"));
        acknowledge_live_outage(&intent, &mut problem, &mut recovery_required);
        assert!(problem.is_some());
        acknowledge_live_outage(
            &LiveIntent::Begin("verified-initial-edge".to_owned()),
            &mut problem,
            &mut recovery_required,
        );
        assert!(problem.is_none());
        assert!(!recovery_required);
    }

    #[test]
    fn uncertain_record_keeps_frozen_payload_and_cursor_until_exact_ack() {
        let events = vec![NativeEvent {
            kind: "message.completed".to_owned(),
            native_id: "held-file-record".to_owned(),
            ts: Some(1234),
            data: json!({"turn_id":"native-turn","text":"dummy"}),
        }];
        let pending = PendingLive::Record {
            events: events.clone(),
            from: "before-record".to_owned(),
            cursor: "after-record".to_owned(),
        };
        for cursor in [
            None,
            Some("before-record".to_owned()),
            Some("another-record".to_owned()),
        ] {
            let ack = BatchAck {
                first_seq: 1,
                last_seq: 1,
                cursor,
            };
            assert_eq!(
                settled_record_cursor(Some(&pending), &ack)
                    .unwrap_err()
                    .code,
                "unknown"
            );
        }
        let ack = BatchAck {
            first_seq: 1,
            last_seq: 1,
            cursor: Some("after-record".to_owned()),
        };
        assert_eq!(
            settled_record_cursor(Some(&pending), &ack).unwrap(),
            "after-record"
        );
        let PendingLive::Record {
            events: retained,
            from,
            cursor,
        } = pending
        else {
            panic!("record")
        };
        assert_eq!(retained, events);
        assert_eq!(from, "before-record");
        assert_eq!(cursor, "after-record");
    }

    #[test]
    fn live_codec_is_explicit_stable_0161_without_alpha_fallback() {
        validate_live_codec("0.161.0").unwrap();
        for version in ["", "0.164.0-alpha", "0.164.0", "0.161.0-alpha", "0.161.0 "] {
            assert_eq!(
                validate_live_codec(version).unwrap_err().code,
                "profile_error"
            );
        }
    }

    #[test]
    fn short_and_overlapping_secrets_reject_routing_ids_without_mutation() {
        let secrets = vec!["xy".to_owned(), "xy-long".to_owned(), String::new()];
        let mut identifier = "native-xy-long-id".to_owned();
        let original = identifier.clone();
        assert_eq!(
            sanitize_text(&mut identifier, &secrets, true)
                .expect_err("protected identity must reject")
                .code,
            "secret_in_payload"
        );
        assert_eq!(identifier, original);
        let mut text = "xy-long xy".to_owned();
        sanitize_text(&mut text, &secrets, false).expect("redact free text");
        assert_eq!(text, "[REDACTED] [REDACTED]");
    }
}
