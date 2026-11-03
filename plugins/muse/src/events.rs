use eliot_core::{Error, NativeEvent, ProviderRestriction, Result, SecretPolicy, SourceBatch};
use eliot_plugin::{CommitSink, MAX_EVENT_BYTES, PeerClient, protocol::MAX_COMMAND_ID_BYTES};
use serde::Serialize;
use serde_json::{Value, json};
use std::{collections::VecDeque, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time::{Instant, timeout_at},
};

use crate::native::{
    NativeClient, NativeFrame, StreamState,
    types::{
        NativeItemCompletedNotification, NativeItemStartedNotification,
        NativeSessionClosedNotification, NativeSessionStartedNotification,
        NativeTurnCompletedNotification, NativeTurnError, NativeTurnStartedNotification,
        NativeTurnTerminal,
    },
};

const EVENT_DEADLINE: Duration = Duration::from_secs(30);
const CONTROL_QUEUE: usize = 16;
const EARLY_EVENT_LIMIT: usize = 128;
const EARLY_EVENT_BYTES: usize = 256 * 1024;

#[derive(Clone, Debug, Default)]
pub(crate) struct EventStatus {
    pub ready: bool,
    pub session_id: Option<String>,
    pub active_command_id: Option<String>,
    pub active_turn_id: Option<String>,
    pub last_turn: Option<TurnFacts>,
    pub failure: Option<String>,
    pub batch_unknown: bool,
    pub source_unknown: bool,
    pub stopped: bool,
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct TurnFacts {
    pub command_id: String,
    pub turn_id: String,
    pub status: Option<String>,
    pub final_text: Option<String>,
    pub terminal: bool,
}

pub(crate) struct EventConfig {
    pub credential: String,
    pub agent_id: String,
    pub generation: i64,
    pub secrets: Vec<String>,
}

pub(crate) struct EventHandle {
    control: mpsc::Sender<Control>,
    status: watch::Receiver<EventStatus>,
}

enum Control {
    Ready {
        native_scope: String,
        session_id: String,
        effective: Value,
        reply: oneshot::Sender<Result<()>>,
    },
    AssociateTurn {
        command_id: String,
        turn_id: String,
        reply: oneshot::Sender<Result<()>>,
    },
    BeginStop {
        reply: oneshot::Sender<Result<()>>,
    },
    EndStop {
        reply: oneshot::Sender<Result<()>>,
    },
    Close {
        reply: oneshot::Sender<()>,
    },
}

#[derive(Clone)]
enum BufferedNativeFrame {
    Notification {
        method: String,
        params: Value,
    },
    Request {
        id: Value,
        method: String,
        params: Value,
    },
}

#[derive(Clone)]
struct BufferedFrame {
    frame: BufferedNativeFrame,
    size: usize,
}

struct EventProcessor {
    native: NativeClient,
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent_id: String,
    generation: i64,
    secrets: Vec<String>,
    status: EventStatus,
    buffer: VecDeque<BufferedFrame>,
    buffer_bytes: usize,
    gap_number: u64,
    accept_frames: bool,
    planned_stop: bool,
    loss_reason: Option<&'static str>,
    frames_drained: bool,
    pending_end_stop: Option<oneshot::Sender<Result<()>>>,
    writes_blocked: bool,
    gap_reported: bool,
    final_text: Option<String>,
}

pub(crate) fn spawn(
    frames: mpsc::Receiver<NativeFrame>,
    stream: watch::Receiver<StreamState>,
    native: NativeClient,
    host: PeerClient,
    config: EventConfig,
) -> EventHandle {
    let processor = EventProcessor::new(native, host, config);
    let (control, control_rx) = mpsc::channel(CONTROL_QUEUE);
    let (status_tx, status) = watch::channel(EventStatus::default());
    tokio::spawn(run(frames, stream, processor, control_rx, status_tx));
    EventHandle { control, status }
}

impl EventHandle {
    pub async fn ready(
        &self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        self.send(|reply| Control::Ready {
            native_scope,
            session_id,
            effective,
            reply,
        })
        .await
    }

    pub async fn associate_turn(&self, command_id: String, turn_id: String) -> Result<()> {
        self.send(|reply| Control::AssociateTurn {
            command_id,
            turn_id,
            reply,
        })
        .await
    }

    pub async fn begin_stop(&self) -> Result<()> {
        self.send(|reply| Control::BeginStop { reply }).await
    }

    pub async fn end_stop(&self) -> Result<()> {
        self.send(|reply| Control::EndStop { reply }).await
    }

    pub async fn close(&self) {
        let (reply, response) = oneshot::channel();
        if self.control.send(Control::Close { reply }).await.is_ok() {
            let _ = timeout_at(Instant::now() + EVENT_DEADLINE, response).await;
        }
    }

    pub fn status(&self) -> EventStatus {
        self.status.borrow().clone()
    }

    async fn send(&self, build: impl FnOnce(oneshot::Sender<Result<()>>) -> Control) -> Result<()> {
        let (reply, response) = oneshot::channel();
        timeout_at(
            Instant::now() + EVENT_DEADLINE,
            self.control.send(build(reply)),
        )
        .await
        .map_err(|_| Error::new("unknown", "Muse event-control queue deadline elapsed"))?
        .map_err(|_| Error::new("unknown", "Muse event processor stopped"))?;
        timeout_at(Instant::now() + EVENT_DEADLINE, response)
            .await
            .map_err(|_| Error::new("unknown", "Muse event-control outcome is unknown"))?
            .map_err(|_| Error::new("unknown", "Muse event-control response was lost"))?
    }
}

impl EventProcessor {
    fn new(native: NativeClient, host: PeerClient, config: EventConfig) -> Self {
        let sink = CommitSink::new(host.clone(), config.credential.clone());
        Self {
            native,
            host,
            sink,
            credential: config.credential,
            agent_id: config.agent_id,
            generation: config.generation,
            secrets: config.secrets,
            status: EventStatus::default(),
            buffer: VecDeque::new(),
            buffer_bytes: 0,
            gap_number: 0,
            accept_frames: true,
            planned_stop: false,
            loss_reason: None,
            frames_drained: false,
            pending_end_stop: None,
            writes_blocked: false,
            gap_reported: false,
            final_text: None,
        }
    }

    async fn handle_control(&mut self, control: Control) -> bool {
        match control {
            Control::Ready {
                native_scope,
                session_id,
                effective,
                reply,
            } => {
                let result = self.ready(native_scope, session_id, effective).await;
                let _ = reply.send(result);
                true
            }
            Control::AssociateTurn {
                command_id,
                turn_id,
                reply,
            } => {
                let result = self.associate_turn(command_id, turn_id).await;
                let _ = reply.send(result);
                true
            }
            Control::BeginStop { reply } => {
                self.planned_stop = true;
                let _ = reply.send(Ok(()));
                true
            }
            Control::EndStop { reply } => {
                if self.frames_drained {
                    let result = self.finish_stop().await;
                    let _ = reply.send(result);
                } else if self.pending_end_stop.is_some() {
                    let _ = reply.send(Err(Error::new(
                        "bad_state",
                        "Muse stop is already waiting for native output drain",
                    )));
                } else {
                    self.pending_end_stop = Some(reply);
                }
                true
            }
            Control::Close { reply } => {
                if let Some(pending) = self.pending_end_stop.take() {
                    let _ = pending.send(Err(Error::new(
                        "unknown",
                        "Muse native event queue closed before stop settlement",
                    )));
                }
                let _ = reply.send(());
                false
            }
        }
    }

    async fn handle_stream_state(&mut self, current: StreamState) {
        if current == StreamState::Open {
            return;
        }
        match current {
            StreamState::Closed => {
                if !self.planned_stop {
                    self.defer_gap("Muse session host event stream closed");
                }
            }
            StreamState::Overflow => self.defer_gap("Muse session host event queue overflowed"),
            StreamState::Malformed => {
                self.defer_gap("Muse session host stream contained an invalid frame")
            }
            StreamState::Open => {}
        }
    }

    async fn handle_frame(&mut self, frame: Option<NativeFrame>) {
        match frame {
            Some(NativeFrame::Request { id, method, params }) => {
                if !self.status.ready || self.status.active_turn_id.is_none() {
                    self.buffer_frame(BufferedNativeFrame::Request { id, method, params });
                } else if let Err(error) = self.process_request(id, method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported && !self.writes_blocked {
                        let _ = self
                            .record_gap("Muse native request could not be normalized")
                            .await;
                    }
                    if self.status.failure.is_none() {
                        self.status.failure = Some(error.message);
                    }
                }
            }
            Some(NativeFrame::Notification { method, params }) => {
                if self.should_buffer(&method) {
                    self.buffer_frame(BufferedNativeFrame::Notification { method, params });
                } else if let Err(error) = self.process_notification(method, params).await {
                    self.status.source_unknown = true;
                    if !self.gap_reported && !self.writes_blocked {
                        let _ = self
                            .record_gap("Muse notification could not be normalized")
                            .await;
                    }
                    if self.status.failure.is_none() {
                        self.status.failure = Some(error.message);
                    }
                }
            }
            None => {
                self.frames_drained = true;
                self.accept_frames = false;
                if !self.buffer.is_empty() {
                    self.buffer.clear();
                    self.buffer_bytes = 0;
                    self.defer_gap("Muse native events remained unassociated at stream end");
                }
                if !self.planned_stop && self.loss_reason.is_none() {
                    self.defer_gap("Muse native event reader closed");
                }
                self.report_gap_if_queue_empty(true).await;
                self.finish_pending_end_stop().await;
            }
        }
    }

    fn defer_gap(&mut self, reason: &'static str) {
        if self.loss_reason.is_none() {
            self.loss_reason = Some(reason);
        }
    }

    async fn report_gap_if_queue_empty(&mut self, queue_empty: bool) {
        if queue_empty
            && !self.gap_reported
            && !self.writes_blocked
            && let Some(reason) = self.loss_reason
        {
            let _ = self.record_gap(reason).await;
        }
    }

    fn should_buffer(&self, method: &str) -> bool {
        matches!(
            method,
            "turn/started" | "turn/completed" | "item/started" | "item/updated" | "item/completed"
        ) && (!self.status.ready || self.status.active_turn_id.is_none())
    }

    fn buffer_frame(&mut self, frame: BufferedNativeFrame) {
        let encoded = match &frame {
            BufferedNativeFrame::Notification { method, params } => {
                serde_json::to_vec(&json!({"method": method, "params": params}))
            }
            BufferedNativeFrame::Request { id, method, params } => {
                serde_json::to_vec(&json!({"id": id, "method": method, "params": params}))
            }
        };
        let size = encoded
            .map(|bytes| bytes.len())
            .unwrap_or_else(|_| EARLY_EVENT_BYTES.saturating_add(1));
        if self.buffer.len() >= EARLY_EVENT_LIMIT
            || self.buffer_bytes.saturating_add(size) > EARLY_EVENT_BYTES
        {
            self.buffer.clear();
            self.buffer_bytes = 0;
            self.defer_gap("Muse early-event buffer exceeded its bound");
            return;
        }
        self.buffer_bytes = self.buffer_bytes.saturating_add(size);
        self.buffer.push_back(BufferedFrame { frame, size });
    }

    async fn ready(
        &mut self,
        native_scope: String,
        session_id: String,
        effective: Value,
    ) -> Result<()> {
        if self.status.ready || session_id.trim().is_empty() || !effective.is_object() {
            return Err(Error::new(
                "invalid",
                "Muse readiness requires one exact session and effective settings",
            ));
        }
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|observed| observed != session_id)
        {
            self.record_gap("Muse session/start response disagreed with session/started")
                .await?;
            return Err(Error::new(
                "source_integrity",
                "Muse session identity did not match its start notification",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse event source acknowledgement is unresolved",
            ));
        }
        self.status.session_id = Some(session_id.clone());
        let native_id = format!("muse:{session_id}:session.ready");
        self.commit_event(
            "session.ready",
            native_id,
            json!({
                "native_scope": native_scope,
                "native_ref": session_id,
                "effective": effective,
            }),
        )
        .await?;
        self.status.ready = true;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Muse event source is incomplete before readiness",
            ));
        }
        self.flush_buffer().await?;
        if self.status.failure.is_some() {
            return Err(Error::new(
                "unknown",
                "Muse event stream has an unresolved gap",
            ));
        }
        Ok(())
    }

    async fn associate_turn(&mut self, command_id: String, turn_id: String) -> Result<()> {
        if !self.status.ready
            || self.status.session_id.is_none()
            || command_id.trim().is_empty()
            || turn_id.trim().is_empty()
        {
            return Err(Error::new(
                "invalid",
                "Muse turn association requires a ready exact session and turn",
            ));
        }
        if self.status.active_turn_id.is_some() {
            return Err(Error::new(
                "busy",
                "Muse already has an associated active turn",
            ));
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse event source acknowledgement is unresolved",
            ));
        }
        self.status.active_command_id = Some(command_id.clone());
        self.status.active_turn_id = Some(turn_id.clone());
        self.final_text = None;
        let session_id = self.status.session_id.as_deref().unwrap_or_default();
        let native_id = event_id(session_id, &turn_id, &turn_id, "turn.started")?;
        self.commit_event(
            "turn.started",
            native_id,
            json!({"command_id": command_id, "turn_id": turn_id}),
        )
        .await?;
        self.flush_buffer().await
    }

    async fn flush_buffer(&mut self) -> Result<()> {
        while let Some(frame) = self.buffer.pop_front() {
            self.buffer_bytes = self.buffer_bytes.saturating_sub(frame.size);
            let should_buffer = match &frame.frame {
                BufferedNativeFrame::Notification { method, .. } => self.should_buffer(method),
                BufferedNativeFrame::Request { .. } => {
                    !self.status.ready || self.status.active_turn_id.is_none()
                }
            };
            if should_buffer {
                self.buffer.push_front(frame);
                self.buffer_bytes = self
                    .buffer_bytes
                    .saturating_add(self.buffer.front().map_or(0, |item| item.size));
                break;
            }
            let result = match frame.frame {
                BufferedNativeFrame::Notification { method, params } => {
                    self.process_notification(method, params).await
                }
                BufferedNativeFrame::Request { id, method, params } => {
                    self.process_request(id, method, params).await
                }
            };
            if let Err(error) = result {
                self.status.source_unknown = true;
                if !self.gap_reported && !self.writes_blocked {
                    let _ = self
                        .record_gap("Buffered Muse native frame could not be normalized")
                        .await;
                }
                if self.status.failure.is_none() {
                    self.status.failure = Some(error.message);
                }
                return Err(Error::new(
                    "unknown",
                    "Buffered Muse event outcome is unresolved",
                ));
            }
            if self.writes_blocked {
                break;
            }
        }
        Ok(())
    }

    /// Server-initiated must-answer requests (`approval/request`,
    /// `userInput/request`) are unconnected in PR1: reject them with a typed
    /// error, never an invented default reply, and record that the native
    /// turn may be waiting on an unanswered request.
    async fn process_request(&mut self, id: Value, method: String, params: Value) -> Result<()> {
        let rejected = self
            .native
            .reject_unsupported_request(id, &method, &params, Instant::now() + EVENT_DEADLINE)
            .await;
        if rejected.is_err() {
            self.defer_gap("Muse unsupported native request could not be rejected");
        } else {
            self.defer_gap("Muse native request type is unconnected in this slice");
        }
        Ok(())
    }

    async fn process_notification(&mut self, method: String, params: Value) -> Result<()> {
        match method.as_str() {
            "turn/started" => {
                let notification: NativeTurnStartedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse turn/started did not match the 1.4 schema",
                        )
                    })?;
                validate_session_and_turn(
                    &self.status,
                    &notification.session_id,
                    &notification.turn_id,
                )?;
                if self.status.active_turn_id.as_deref() != Some(notification.turn_id.as_str()) {
                    return self
                        .record_gap("Muse turn/started did not match the exact submitted turn")
                        .await;
                }
            }
            "item/started" => {
                let notification: NativeItemStartedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/started did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
            }
            "item/updated" => {
                // Full re-emission at a higher revision; PR1 folds only the
                // authoritative terminal object from `item/completed`.
                let notification: NativeItemCompletedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/updated did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
            }
            "item/completed" => {
                let notification: NativeItemCompletedNotification =
                    serde_json::from_value(params.clone()).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse item/completed did not match the 1.4 schema",
                        )
                    })?;
                self.validate_item_turn(
                    &notification.session_id,
                    notification.item.turn_id.as_ref(),
                )?;
                if notification.item.kind == "agentMessage" {
                    let text = notification.item.text.as_deref().ok_or_else(|| {
                        Error::new("native_protocol", "Muse agentMessage omitted its text")
                    })?;
                    let turn_id = notification.item.turn_id.as_deref().unwrap_or_default();
                    self.complete_message(turn_id, &notification.item.item_id, text)
                        .await?;
                }
            }
            "turn/completed" => {
                self.complete_turn(params).await?;
            }
            "session/started" => {
                self.observe_session_started(&params).await?;
            }
            "session/closed" => {
                let notification: NativeSessionClosedNotification = serde_json::from_value(params)
                    .map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse session/closed did not match the 1.4 schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str())
                    && !self.planned_stop
                {
                    return self
                        .record_gap("Muse session closed outside a planned stop")
                        .await;
                }
            }
            "view/gap" => {
                let notification: crate::native::types::NativeViewGapNotification =
                    serde_json::from_value(params).map_err(|_| {
                        Error::new(
                            "native_protocol",
                            "Muse view/gap did not match the 1.4 schema",
                        )
                    })?;
                if self.status.session_id.as_deref() == Some(notification.session_id.as_str()) {
                    return self
                        .record_gap("Muse view stream reported a delivery gap")
                        .await;
                }
            }
            _ => {}
        }
        Ok(())
    }

    /// Items outside the exact active turn are gaps; `userShell` is the one
    /// kind outside any turn and is ignored in PR1.
    fn validate_item_turn(&self, session_id: &str, turn_id: Option<&String>) -> Result<()> {
        if self.status.session_id.as_deref() != Some(session_id) {
            return Err(Error::new(
                "source_integrity",
                "Muse notification did not belong to the owned native session",
            ));
        }
        match turn_id.map(String::as_str) {
            None => Ok(()),
            Some(turn_id) if turn_id.trim().is_empty() => Err(Error::new(
                "source_integrity",
                "Muse notification did not belong to the owned native session",
            )),
            Some(turn_id) => {
                if self.status.active_turn_id.as_deref() != Some(turn_id) {
                    return Err(Error::new(
                        "source_integrity",
                        "Muse item was outside the exact active turn",
                    ));
                }
                Ok(())
            }
        }
    }

    async fn complete_message(&mut self, turn_id: &str, item_id: &str, text: &str) -> Result<()> {
        if item_id.trim().is_empty() {
            return self
                .record_gap("Muse agentMessage omitted its item id")
                .await;
        }
        let text = SecretPolicy::new(self.secrets.iter().cloned()).redact_text(text);
        let data = json!({
            "command_id": self.status.active_command_id.as_deref().unwrap_or_default(),
            "turn_id": turn_id,
            "text": text,
        });
        let encoded_len = match serde_json::to_vec(&data) {
            Ok(bytes) => bytes.len(),
            Err(_) => usize::MAX,
        };
        if encoded_len > MAX_EVENT_BYTES {
            return self
                .record_gap("Muse completed message exceeded the persisted-event bound")
                .await;
        }
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            turn_id,
            item_id,
            "message.completed",
        )?;
        // The accumulated reply; a later `agentMessage` replaces an earlier one.
        self.final_text = Some(text);
        self.commit_event("message.completed", native_id, data)
            .await
    }

    async fn complete_turn(&mut self, params: Value) -> Result<()> {
        let notification: NativeTurnCompletedNotification = serde_json::from_value(params)
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse turn/completed did not match the 1.4 schema",
                )
            })?;
        validate_session_and_turn(
            &self.status,
            &notification.session_id,
            &notification.turn_id,
        )?;
        if self.status.active_turn_id.as_deref() != Some(notification.turn_id.as_str()) {
            return self
                .record_gap("Muse terminal notification did not match the exact active turn")
                .await;
        }
        let Some(status_text) = turn_status(&notification.terminal) else {
            return self
                .record_gap("Muse turn/completed carried an unknown terminal")
                .await;
        };
        let command_id = self
            .status
            .active_command_id
            .clone()
            .ok_or_else(|| Error::new("unknown", "Muse terminal turn has no Eliot command"))?;
        let provider_restriction = terminal_provider_restriction(&self.status, &notification);
        let secrets = SecretPolicy::new(self.secrets.iter().cloned());
        let mut data = json!({
            "command_id": command_id.clone(),
            "turn_id": notification.turn_id,
            "status": status_text,
        });
        // The semantic hold signal gets first claim on the bounded terminal data.
        if let Some(restriction) = provider_restriction {
            attach_provider_restriction(&mut data, restriction, &secrets);
        }
        if let Some(text) = self.final_text.as_ref() {
            attach_final_text(
                &mut data,
                text,
                notification.terminal == NativeTurnTerminal::Failed,
                &secrets,
            );
        }
        attach_native_error(&mut data, notification.error.as_ref(), &secrets);
        let native_id = event_id(
            self.status.session_id.as_deref().unwrap_or_default(),
            &notification.turn_id,
            &notification.turn_id,
            "turn.completed",
        )?;
        self.commit_event("turn.completed", native_id, data).await?;
        self.status.last_turn = Some(TurnFacts {
            command_id,
            turn_id: notification.turn_id,
            status: Some(status_text.to_owned()),
            final_text: self.final_text.take(),
            terminal: true,
        });
        self.status.active_turn_id = None;
        self.status.active_command_id = None;
        Ok(())
    }

    async fn observe_session_started(&mut self, params: &Value) -> Result<()> {
        let notification: NativeSessionStartedNotification = serde_json::from_value(params.clone())
            .map_err(|_| {
                Error::new(
                    "native_protocol",
                    "Muse session/started omitted its exact session id",
                )
            })?;
        if notification.session.session_id.trim().is_empty() {
            return Err(Error::new(
                "native_protocol",
                "Muse session/started omitted its exact session id",
            ));
        }
        if self
            .status
            .session_id
            .as_deref()
            .is_some_and(|existing| existing != notification.session.session_id)
        {
            return self
                .record_gap("Muse session host announced a second session in one Agent generation")
                .await;
        }
        self.status.session_id = Some(notification.session.session_id);
        Ok(())
    }

    async fn finish_stop(&mut self) -> Result<()> {
        if self.status.stopped {
            return Ok(());
        }
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse source acknowledgement is unresolved",
            ));
        }
        let native_id = format!(
            "muse:{}:session.ended",
            self.status.session_id.as_deref().unwrap_or("unresolved")
        );
        self.commit_event("session.ended", native_id, json!({}))
            .await?;
        if let (Some(command_id), Some(turn_id)) = (
            self.status.active_command_id.clone(),
            self.status.active_turn_id.clone(),
        ) {
            self.status.last_turn = Some(TurnFacts {
                command_id,
                turn_id,
                status: None,
                final_text: None,
                terminal: false,
            });
        }
        self.status.stopped = true;
        self.status.ready = false;
        self.status.active_command_id = None;
        self.status.active_turn_id = None;
        Ok(())
    }

    async fn finish_pending_end_stop(&mut self) {
        if !self.frames_drained {
            return;
        }
        if let Some(reply) = self.pending_end_stop.take() {
            let result = self.finish_stop().await;
            let _ = reply.send(result);
        }
    }

    async fn commit_event(&mut self, kind: &str, native_id: String, data: Value) -> Result<()> {
        if self.writes_blocked {
            return Err(Error::new(
                "unknown",
                "Muse event acknowledgement is unresolved",
            ));
        }
        let batch = SourceBatch {
            batch_id: format!("{native_id}:batch"),
            agent_id: self.agent_id.clone(),
            generation: self.generation,
            source: "stream".to_owned(),
            events: vec![NativeEvent {
                kind: kind.to_owned(),
                native_id,
                data,
                ts: None,
            }],
            cursor: None,
        };
        let error = match self
            .sink
            .commit(batch, Instant::now() + EVENT_DEADLINE)
            .await
        {
            Ok(_) => return Ok(()),
            Err(error) => error,
        };
        if self.sink.is_unknown() {
            self.writes_blocked = true;
            self.status.source_unknown = true;
            self.status.failure = Some("Muse event batch acknowledgement is unknown".to_owned());
            self.status.batch_unknown = true;
            return Err(Error::new(
                "unknown",
                "Muse event batch acknowledgement is unknown",
            ));
        }
        let reason = format!("{kind} could not be committed: {}", error.code);
        self.record_gap(&reason).await?;
        Err(Error::new(
            "unknown",
            "Muse native fact was not committed; no replay is allowed",
        ))
    }

    async fn record_gap(&mut self, reason: &str) -> Result<()> {
        if self.writes_blocked {
            self.status.source_unknown = true;
            return Err(Error::new(
                "unknown",
                "Muse event acknowledgement is unresolved; no source gap was issued",
            ));
        }
        if self.gap_reported {
            return Err(Error::new(
                "unknown",
                "Muse source already has an unresolved gap",
            ));
        }
        self.status.source_unknown = true;
        self.gap_reported = true;
        self.gap_number = self.gap_number.saturating_add(1);
        let gap_id = format!(
            "muse:{}:{}:gap-{}",
            self.agent_id, self.generation, self.gap_number
        );
        let ack_id = gap_id.clone();
        let result = self
            .host
            .call(
                "source.gap",
                json!({
                    "credential": self.credential,
                    "agent_id": self.agent_id,
                    "generation": self.generation,
                    "source": "stream",
                    "gap_id": gap_id,
                    "families": ["session", "turn", "message"],
                    "reason": reason,
                    "from": null,
                    "to": null,
                }),
                Instant::now() + EVENT_DEADLINE,
            )
            .await;
        match result {
            Ok(ack)
                if ack.get("gap_id").and_then(Value::as_str) == Some(ack_id.as_str())
                    && ack
                        .get("seq")
                        .and_then(Value::as_i64)
                        .is_some_and(|seq| seq > 0) =>
            {
                self.status.failure = Some(format!("{reason}; terminal outcome is unknown"));
                Ok(())
            }
            Ok(_) | Err(_) => {
                self.writes_blocked = true;
                self.status.failure = Some("Muse source gap acknowledgement is unknown".to_owned());
                Err(Error::new(
                    "unknown",
                    "Muse source gap acknowledgement is unknown",
                ))
            }
        }
    }
}

async fn run(
    mut frames: mpsc::Receiver<NativeFrame>,
    mut stream: watch::Receiver<StreamState>,
    mut processor: EventProcessor,
    mut controls: mpsc::Receiver<Control>,
    status_tx: watch::Sender<EventStatus>,
) {
    loop {
        let accept_frames = processor.accept_frames;
        tokio::select! {
            changed = stream.changed(), if accept_frames => {
                let current = match changed {
                    Ok(()) => *stream.borrow_and_update(),
                    Err(_) => StreamState::Closed,
                };
                processor.handle_stream_state(current).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
            control = controls.recv() => {
                match control {
                    Some(control) => {
                        if !processor.handle_control(control).await {
                            break;
                        }
                    }
                    None => break,
                }
            }
            frame = frames.recv(), if accept_frames => {
                if frame.is_none() {
                    let current = *stream.borrow_and_update();
                    processor.handle_stream_state(current).await;
                }
                processor.handle_frame(frame).await;
                processor.report_gap_if_queue_empty(frames.is_empty()).await;
            }
        }
        let _ = status_tx.send(processor.status.clone());
    }
}

fn attach_native_error(data: &mut Value, error: Option<&NativeTurnError>, policy: &SecretPolicy) {
    let Some(error) = error else { return };
    if ["native_error", "message", "truncated"]
        .into_iter()
        .any(|key| policy.contains_text(key))
    {
        return;
    }
    let Some(data) = data.as_object_mut() else {
        return;
    };
    let Ok(encoded) = serde_json::to_vec(&*data) else {
        return;
    };
    let budget = MAX_EVENT_BYTES
        .saturating_sub(encoded.len().saturating_add(r#","native_error":"#.len()))
        .min(4 * 1024);
    let mut message = policy.redact_text(&error.message);
    let mut truncated = false;
    loop {
        let diagnostic = json!({"message": &message, "truncated": truncated});
        let Ok(encoded) = serde_json::to_vec(&diagnostic) else {
            return;
        };
        if encoded.len() <= budget {
            data.insert("native_error".to_owned(), diagnostic);
            return;
        }
        if message.is_empty() {
            return;
        }
        let mut end = message.len() / 2;
        while !message.is_char_boundary(end) {
            end -= 1;
        }
        // Preserve complete masks across the host's repeated sanitization.
        let (safe, _) = policy.redact_prefix(&message.as_bytes()[..end], false);
        message = String::from_utf8_lossy(&safe).into_owned();
        truncated = true;
    }
}

fn validate_session_and_turn(status: &EventStatus, session_id: &str, turn_id: &str) -> Result<()> {
    if status.session_id.as_deref() != Some(session_id) || turn_id.trim().is_empty() {
        return Err(Error::new(
            "source_integrity",
            "Muse notification did not belong to the owned native session",
        ));
    }
    Ok(())
}

fn event_id(session_id: &str, turn_id: &str, item_id: &str, kind: &str) -> Result<String> {
    let native_id = format!("muse:{session_id}:{turn_id}:{item_id}:{kind}");
    if native_id.len() > MAX_COMMAND_ID_BYTES
        || session_id.trim().is_empty()
        || item_id.trim().is_empty()
    {
        return Err(Error::new(
            "source_integrity",
            "Muse native event identity exceeded its bound",
        ));
    }
    Ok(native_id)
}

/// Map the MSP terminal to the stored turn status. `cancelled` folds to
/// `interrupted`, matching the Codex path; an unknown wire terminal settles
/// nothing and the caller records a gap instead.
fn turn_status(terminal: &NativeTurnTerminal) -> Option<&'static str> {
    match terminal {
        NativeTurnTerminal::Completed => Some("completed"),
        NativeTurnTerminal::Failed => Some("failed"),
        NativeTurnTerminal::Cancelled => Some("interrupted"),
        NativeTurnTerminal::Unknown => None,
    }
}

fn terminal_provider_restriction(
    status: &EventStatus,
    notification: &NativeTurnCompletedNotification,
) -> Option<ProviderRestriction> {
    if notification.terminal != NativeTurnTerminal::Failed
        || status.session_id.as_deref() != Some(notification.session_id.as_str())
        || status.active_turn_id.as_deref() != Some(notification.turn_id.as_str())
        || status.active_command_id.is_none()
    {
        return None;
    }
    notification
        .error
        .as_ref()
        .and_then(NativeTurnError::provider_restriction)
}

fn attach_provider_restriction(
    data: &mut Value,
    restriction: ProviderRestriction,
    policy: &SecretPolicy,
) -> bool {
    let Ok(value) = serde_json::to_value(restriction) else {
        return false;
    };
    // The key and actual value define immutable semantics and may not be redacted.
    if policy.contains_text("provider_restriction")
        || policy.contains_value(&value)
        || data
            .as_object()
            .is_none_or(|fields| fields.contains_key("provider_restriction"))
    {
        return false;
    }
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return false;
    };
    fields.insert("provider_restriction".to_owned(), value.clone());
    if serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES) {
        return false;
    }
    if let Some(fields) = data.as_object_mut() {
        fields.insert("provider_restriction".to_owned(), value);
        return true;
    }
    false
}

fn attach_final_text(data: &mut Value, final_text: &str, failed: bool, policy: &SecretPolicy) {
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return;
    };
    fields.insert(
        "final_text".to_owned(),
        Value::String(final_text.to_owned()),
    );
    let exceeds_event_bound =
        serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES);
    if failed && exceeds_event_bound {
        attach_final_text_unavailable(data, policy);
        return;
    }
    *data = candidate;
}

fn attach_final_text_unavailable(data: &mut Value, policy: &SecretPolicy) {
    if ["final_text_unavailable", "event_budget"]
        .iter()
        .any(|literal| policy.contains_text(literal))
        || data
            .as_object()
            .is_none_or(|fields| fields.contains_key("final_text_unavailable"))
    {
        return;
    }
    let mut candidate = data.clone();
    let Some(fields) = candidate.as_object_mut() else {
        return;
    };
    fields.insert(
        "final_text_unavailable".to_owned(),
        Value::String("event_budget".to_owned()),
    );
    if serde_json::to_vec(&candidate).map_or(true, |encoded| encoded.len() > MAX_EVENT_BYTES) {
        return;
    }
    if let Some(fields) = data.as_object_mut() {
        fields.insert(
            "final_text_unavailable".to_owned(),
            Value::String("event_budget".to_owned()),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_mapping_cancels_to_interrupted() {
        assert_eq!(
            turn_status(&NativeTurnTerminal::Completed),
            Some("completed")
        );
        assert_eq!(turn_status(&NativeTurnTerminal::Failed), Some("failed"));
        assert_eq!(
            turn_status(&NativeTurnTerminal::Cancelled),
            Some("interrupted")
        );
        assert_eq!(turn_status(&NativeTurnTerminal::Unknown), None);
    }

    #[test]
    fn event_id_rejects_blank_or_oversize_identity() {
        assert!(event_id("", "turn", "item", "message.completed").is_err());
        assert!(event_id("session", "turn", "", "message.completed").is_err());
        let long = "x".repeat(MAX_COMMAND_ID_BYTES);
        assert!(event_id(&long, "turn", "item", "message.completed").is_err());
        assert_eq!(
            event_id("session", "turn", "item", "message.completed").unwrap(),
            "muse:session:turn:item:message.completed"
        );
    }

    #[test]
    fn failed_terminal_marks_unavailable_final_text_at_event_budget() {
        let mut data = json!({"command_id": "command", "turn_id": "turn", "status": "failed"});
        let oversize = "x".repeat(MAX_EVENT_BYTES);
        attach_final_text(&mut data, &oversize, true, &SecretPolicy::default());
        assert_eq!(data.get("final_text"), None);
        assert_eq!(
            data.get("final_text_unavailable"),
            Some(&Value::String("event_budget".to_owned()))
        );

        let mut data = json!({"command_id": "command", "turn_id": "turn", "status": "completed"});
        attach_final_text(&mut data, "final", false, &SecretPolicy::default());
        assert_eq!(
            data.get("final_text"),
            Some(&Value::String("final".to_owned()))
        );
    }

    #[test]
    fn provider_restriction_requires_the_exact_bound_failed_turn() {
        let status = EventStatus {
            session_id: Some("session-1".to_owned()),
            active_command_id: Some("command-1".to_owned()),
            active_turn_id: Some("turn-1".to_owned()),
            ..EventStatus::default()
        };
        let notification = NativeTurnCompletedNotification {
            session_id: "session-1".to_owned(),
            terminal: NativeTurnTerminal::Failed,
            turn_id: "turn-1".to_owned(),
            view_cursor: "cursor-1".to_owned(),
            error: Some(NativeTurnError {
                kind: "authRequired".to_owned(),
                message: "login expired".to_owned(),
                retryable: true,
            }),
            reason: None,
        };
        assert!(terminal_provider_restriction(&status, &notification).is_some());

        let mut other_session = notification.clone();
        other_session.session_id = "session-2".to_owned();
        assert_eq!(terminal_provider_restriction(&status, &other_session), None);
        let mut completed = notification.clone();
        completed.terminal = NativeTurnTerminal::Completed;
        assert_eq!(terminal_provider_restriction(&status, &completed), None);
    }
}
