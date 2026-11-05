use std::{collections::HashMap, sync::Arc};

use eliot_core::{Error, Result, SecretPolicy};
use eliot_plugin::{
    CommitSink, MAX_EVENT_BYTES, PeerClient,
    native_transport::StreamState,
    session::{StreamSource, attach_final_text, attach_native_error, event_id},
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::sync::{Mutex, mpsc, oneshot, watch};

use crate::native::{AgyFrame, ResultRecord, StepUpdate};

pub(crate) const FAMILIES: &[&str] = &["session", "turn", "message", "tool"];
pub(crate) type Shared = Arc<Mutex<State>>;

#[cfg(test)]
type CapturedEvents = Arc<std::sync::Mutex<Vec<(String, Value)>>>;

#[derive(Clone, Serialize)]
pub(crate) struct TurnFacts {
    pub command_id: String,
    pub turn_id: String,
    pub status: String,
    pub final_text: String,
    pub terminal: bool,
}

struct Pending {
    command: String,
    turn: String,
    associated: bool,
    reply: Option<oneshot::Sender<Result<()>>>,
}

pub(crate) struct State {
    host: PeerClient,
    sink: CommitSink,
    credential: String,
    agent: String,
    generation: i64,
    policy: SecretPolicy,
    pub conversation: String,
    pending: Option<Pending>,
    tool_events: HashMap<String, Value>,
    writes_blocked: bool,
    gap_reported: bool,
    gap_number: u64,
    pub failure: Option<String>,
    pub source_unknown: bool,
    pub batch_unknown: bool,
    pub stopping: bool,
    pub stopped: bool,
    pub last_turn: Option<TurnFacts>,
    pub usage: Option<crate::native::Usage>,
    #[cfg(test)]
    pub(crate) test_recorder: Option<CapturedEvents>,
}

impl State {
    pub fn new(
        host: PeerClient,
        credential: String,
        agent: String,
        generation: i64,
        policy: SecretPolicy,
        conversation: String,
    ) -> Self {
        Self {
            sink: CommitSink::new(host.clone(), credential.clone()),
            host,
            credential,
            agent,
            generation,
            policy,
            conversation,
            pending: None,
            tool_events: HashMap::new(),
            writes_blocked: false,
            gap_reported: false,
            gap_number: 0,
            failure: None,
            source_unknown: false,
            batch_unknown: false,
            stopping: false,
            stopped: false,
            last_turn: None,
            usage: None,
            #[cfg(test)]
            test_recorder: None,
        }
    }

    pub fn active_turn(&self) -> Option<&str> {
        self.pending.as_ref().map(|p| p.turn.as_str())
    }

    /// Register correlation before the write. This local turn id is never
    /// sent to AGY or presented as a vendor-supplied id.
    pub fn prepare(
        &mut self,
        command: String,
        turn: String,
    ) -> Result<oneshot::Receiver<Result<()>>> {
        if self.source_unknown || self.failure.is_some() {
            return Err(Error::new("unknown", "Antigravity stream is unresolved"));
        }
        if self.stopping || self.stopped {
            return Err(Error::new("bad_state", "Antigravity session is stopping"));
        }
        if self.pending.is_some() {
            return Err(Error::new("busy", "Antigravity has one outstanding input"));
        }
        let (reply, result) = oneshot::channel();
        self.pending = Some(Pending {
            command,
            turn,
            associated: false,
            reply: Some(reply),
        });
        self.tool_events.clear();
        Ok(result)
    }

    pub fn withdraw_unwritten(&mut self, command: &str) -> Result<()> {
        if self
            .pending
            .as_ref()
            .is_some_and(|p| p.command == command && !p.associated)
        {
            self.pending = None;
            return Ok(());
        }
        Err(Error::new(
            "unknown",
            "Antigravity emitted a turn despite an unwritten input",
        ))
    }

    pub async fn ready(&mut self, scope: &str, effective: Value) -> Result<()> {
        self.commit(
            "session.ready",
            "session",
            "ready",
            json!({
                "native_scope": scope, "native_ref": self.conversation, "effective": effective,
            }),
        )
        .await
    }

    async fn associate(&mut self) -> Result<()> {
        let pending = self.pending.as_ref().ok_or_else(|| {
            Error::new(
                "source_integrity",
                "Antigravity output has no outstanding input",
            )
        })?;
        if pending.associated {
            return Ok(());
        }
        let turn = pending.turn.clone();
        let data = json!({"command_id": pending.command, "turn_id": turn});
        let result = self.commit("turn.started", &turn, "started", data).await;
        if let Some(pending) = self.pending.as_mut() {
            pending.associated = result.is_ok();
            if let Some(reply) = pending.reply.take() {
                let _ = reply.send(result.clone());
            }
        }
        result
    }

    async fn observe(&mut self, frame: AgyFrame) -> Result<()> {
        if self.failure.is_some() {
            return Ok(());
        }
        match frame {
            AgyFrame::StepUpdate { step_update } => self.step(step_update).await,
            AgyFrame::Result { result } => self.complete(result).await,
            AgyFrame::Init { .. } => Err(Error::new(
                "source_integrity",
                "Antigravity sent a second init",
            )),
            AgyFrame::Unknown => Err(Error::new(
                "native_protocol",
                "Antigravity event is unconnected",
            )),
        }
    }

    fn check_conversation(&self, conversation: &str) -> Result<()> {
        if conversation == self.conversation {
            return Ok(());
        }
        Err(Error::new(
            "source_integrity",
            "Antigravity frame belongs to another conversation",
        ))
    }

    async fn step(&mut self, step: StepUpdate) -> Result<()> {
        self.check_conversation(&step.conversation_id)?;
        // Native bookkeeping may arrive before input, including on resume.
        // It supplies neither an input acknowledgement nor assistant output.
        if step.step_type == "system_message" {
            return Ok(());
        }
        match step.step_type.as_str() {
            "user_input" | "agent_response" => {
                self.associate().await?;
                Ok(())
            }
            "tool" => self.tool(step).await,
            _ => Err(self.unsupported_step_error(&step)),
        }
    }

    fn unsupported_step_error(&self, step: &StepUpdate) -> Error {
        let bounded = |value: &str| {
            let redacted = self.policy.redact_text(value);
            let mut chars = redacted.chars();
            let mut value: String = chars.by_ref().take(96).collect();
            if chars.next().is_some() {
                value.push_str("[TRUNCATED]");
            }
            value
        };
        // This reason reaches source.gap through lose(). Keep only structural
        // fields; never serialize the frame, prompt, response, or tool args.
        let details = json!({
            "event": "step_update",
            "conversation_id": bounded(&step.conversation_id),
            "step_index": step.step_index,
            "state": bounded(&step.state),
            "step_type": bounded(&step.step_type),
        });
        Error::new(
            "native_protocol",
            format!("Antigravity step type is unconnected: {details}"),
        )
    }

    async fn tool(&mut self, step: StepUpdate) -> Result<()> {
        let (kind, status) = match step.state.as_str() {
            "ACTIVE" => ("tool.started", "running"),
            "DONE" => ("tool.completed", "completed"),
            "ERROR" => ("tool.completed", "failed"),
            _ => {
                return Err(Error::new(
                    "native_protocol",
                    "Antigravity tool state is unknown",
                ));
            }
        };
        let tool = step
            .tool_name
            .filter(|s| !s.trim().is_empty())
            .ok_or_else(|| Error::new("native_protocol", "Antigravity tool name is absent"))?;
        self.associate().await?;
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        let turn = pending.turn.clone();
        let item = format!("step-{}", step.step_index);
        let mut data = json!({"command_id": pending.command, "turn_id": turn,
            "item_id": item, "tool": tool, "status": status, "native_status": step.state});
        if let Some(error) = step.tool_info.and_then(|info| info.error) {
            data["native_error_type"] = json!(self.policy.redact_text(&error.r#type));
            attach_native_error(&mut data, Some(&error.message), &self.policy);
        }
        self.policy.redact_values(&mut data);
        let key = format!("{item}:{kind}");
        if let Some(old) = self.tool_events.get(&key) {
            return if old == &data {
                Ok(())
            } else {
                Err(Error::new(
                    "source_integrity",
                    "Antigravity tool step changed after commitment",
                ))
            };
        }
        self.commit(kind, &turn, &item, data.clone()).await?;
        self.tool_events.insert(key, data);
        Ok(())
    }

    async fn complete(&mut self, result: ResultRecord) -> Result<()> {
        self.check_conversation(&result.conversation_id)?;
        let Some(status) = result.terminal_status()? else {
            return Ok(());
        };
        // A result alone suffices: no intermediate step or native turn id is required.
        self.associate().await?;
        let pending = self
            .pending
            .as_ref()
            .ok_or_else(|| Error::new("unknown", "Antigravity turn is absent"))?;
        let (command, turn) = (pending.command.clone(), pending.turn.clone());
        let failed = result.status != "SUCCESS";
        let text = self.policy.redact_text(&result.response);
        let mut data = json!({"command_id": command, "turn_id": turn,
            "status": status, "native_status": result.status, "denied_actions": result.denied_actions,
            "usage": result.usage, "num_turns": result.num_turns,
            "finished_reason": result.finished_reason});
        self.policy.redact_values(&mut data);
        attach_native_error(&mut data, result.error.as_deref(), &self.policy);
        attach_final_text(&mut data, &text, failed, &self.policy);
        let text_omitted = failed && data.get("final_text").is_none();
        if !text.is_empty() && !text_omitted {
            self.commit(
                "message.completed",
                &turn,
                "response",
                json!({
                    "command_id": command, "turn_id": turn, "text": text,
                }),
            )
            .await?;
        }
        self.commit("turn.completed", &turn, "completed", data.clone())
            .await?;
        if result.usage.is_some() {
            self.usage = result.usage; // Replace; never add cumulative results.
        }
        let final_text = data
            .get("final_text")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        self.last_turn = Some(TurnFacts {
            command_id: command,
            turn_id: turn,
            status: status.to_owned(),
            final_text,
            terminal: true,
        });
        self.pending = None;
        Ok(())
    }

    pub async fn ended(&mut self) -> Result<()> {
        if !self.stopped {
            if self.pending.is_some() {
                self.lose("Antigravity stopped with an unresolved input")
                    .await;
            }
            self.commit("session.ended", "session", "ended", json!({}))
                .await?;
            self.stopped = true;
        }
        // An unfinished input stays unresolved; process exit does not complete it.
        Ok(())
    }

    async fn commit(&mut self, kind: &str, turn: &str, item: &str, mut data: Value) -> Result<()> {
        self.policy.redact_values(&mut data);
        if serde_json::to_vec(&data).map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES) {
            return Err(Error::new(
                "frame_too_large",
                "Antigravity normalized fact exceeds event bound",
            ));
        }
        let id = event_id(&self.conversation, turn, item, kind, "agy", "Antigravity")?;
        #[cfg(test)]
        if let Some(recorder) = &self.test_recorder {
            recorder.lock().unwrap().push((kind.to_owned(), data));
            return Ok(());
        }
        self.source().commit_event(kind, id, data).await
    }

    pub async fn lose(&mut self, reason: &str) {
        self.source_unknown = true;
        if !self.gap_reported && !self.writes_blocked {
            let _ = self.source().record_gap(reason).await;
        }
        self.failure.get_or_insert_with(|| reason.to_owned());
        if let Some(reply) = self.pending.as_mut().and_then(|p| p.reply.take()) {
            let _ = reply.send(Err(Error::new(
                "unknown",
                "Antigravity input outcome is unknown",
            )));
        }
    }

    fn source(&mut self) -> StreamSource<'_> {
        StreamSource {
            sink: &mut self.sink,
            host: &self.host,
            credential: &self.credential,
            agent_id: &self.agent,
            generation: self.generation,
            harness: "Antigravity",
            id_prefix: "agy",
            families: FAMILIES,
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.source_unknown,
            batch_unknown: &mut self.batch_unknown,
            failure: &mut self.failure,
        }
    }
}

pub(crate) fn spawn(
    mut frames: mpsc::Receiver<AgyFrame>,
    stream: watch::Receiver<StreamState>,
    state: Shared,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(frame) = frames.recv().await {
            let mut state = state.lock().await;
            if *stream.borrow() == StreamState::Overflow {
                state.lose("Antigravity native queue overflowed").await;
            }
            if let Err(error) = state.observe(frame).await {
                state.lose(&error.message).await;
            }
        }
        // All queued stdout is consumed before concluding loss or planned EOF.
        let mut state = state.lock().await;
        if matches!(
            *stream.borrow(),
            StreamState::Overflow | StreamState::Malformed
        ) {
            state.lose("Antigravity output was incomplete").await;
        } else if !state.stopping {
            state
                .lose("Antigravity stream closed outside a planned stop")
                .await;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use eliot_plugin::Peer;

    fn test_state() -> State {
        let peer = Peer::stdio();
        State::new(
            peer.client(),
            "test-credential".to_owned(),
            "test-agent".to_owned(),
            1,
            SecretPolicy::default(),
            "conv-1".to_owned(),
        )
    }

    #[tokio::test]
    async fn unknown_step_leaves_input_ack_unknown() {
        let mut state = test_state();
        let mut reply_rx = state
            .prepare("cmd-1".to_owned(), "turn-1".to_owned())
            .expect("prepared input");
        let step = StepUpdate {
            conversation_id: "conv-1".to_owned(),
            step_index: 1,
            state: "RUNNING".to_owned(),
            step_type: "unknown_future_step".to_owned(),
            tool_name: None,
            tool_info: None,
        };
        let error = state
            .step(step)
            .await
            .expect_err("unknown step type rejects");
        assert_eq!(error.code, "native_protocol");
        assert!(!state.pending.as_ref().unwrap().associated);
        assert!(state.pending.as_ref().unwrap().reply.is_some());

        state.writes_blocked = true;
        state.lose(&error.message).await;
        let ack = reply_rx.try_recv().expect("reply received");
        assert_eq!(ack.expect_err("no ACK on unknown step").code, "unknown");
    }

    #[tokio::test]
    async fn system_message_stays_before_association() {
        let mut state = test_state();
        let step = StepUpdate {
            conversation_id: "conv-1".to_owned(),
            step_index: 0,
            state: "RUNNING".to_owned(),
            step_type: "system_message".to_owned(),
            tool_name: None,
            tool_info: None,
        };
        state.step(step).await.expect("system message ignored");
        assert!(state.pending.is_none());
    }

    #[tokio::test]
    async fn oversized_failed_terminal_omits_message_and_continues_lane() {
        let recorder = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut state = test_state();
        state.test_recorder = Some(Arc::clone(&recorder));
        let mut reply_rx = state
            .prepare("cmd-1".to_owned(), "turn-1".to_owned())
            .expect("prepared input");

        let oversize_response = "E".repeat(80 * 1024);
        let result = ResultRecord {
            conversation_id: "conv-1".to_owned(),
            status: "ERROR".to_owned(),
            response: oversize_response,
            error: Some("process failed with error".to_owned()),
            finished_reason: Some("error".to_owned()),
            num_turns: Some(1),
            usage: None,
            denied_actions: Vec::new(),
        };

        state
            .complete(result)
            .await
            .expect("oversized failed terminal settles without bound error");

        let ack = reply_rx.try_recv().expect("ack received");
        assert!(ack.is_ok());

        assert!(state.pending.is_none());
        assert_eq!(state.active_turn(), None);
        assert!(state.failure.is_none());
        assert!(!state.source_unknown);
        let last_turn = state.last_turn.as_ref().expect("last turn facts recorded");
        assert_eq!(last_turn.status, "failed");
        assert_eq!(last_turn.final_text, "");
        assert!(last_turn.terminal);

        let commits = recorder.lock().unwrap().clone();
        let kinds: Vec<&str> = commits.iter().map(|(kind, _)| kind.as_str()).collect();
        assert_eq!(kinds, vec!["turn.started", "turn.completed"]);

        let turn_completed = &commits[1].1;
        assert_eq!(
            turn_completed.get("final_text_unavailable"),
            Some(&Value::String("event_budget".to_owned()))
        );
        assert!(turn_completed.get("final_text").is_none());
    }

    #[tokio::test]
    async fn oversized_success_preserves_bound_failure() {
        let recorder = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut state = test_state();
        state.test_recorder = Some(Arc::clone(&recorder));
        let _reply_rx = state
            .prepare("cmd-1".to_owned(), "turn-1".to_owned())
            .expect("prepared input");

        let oversize_response = "S".repeat(80 * 1024);
        let result = ResultRecord {
            conversation_id: "conv-1".to_owned(),
            status: "SUCCESS".to_owned(),
            response: oversize_response,
            error: None,
            finished_reason: Some("stop".to_owned()),
            num_turns: Some(1),
            usage: None,
            denied_actions: Vec::new(),
        };

        let error = state
            .complete(result)
            .await
            .expect_err("oversized success must fail bound");
        assert_eq!(error.code, "frame_too_large");
    }

    #[tokio::test]
    async fn small_failed_terminal_retains_message_and_final_text() {
        let recorder = Arc::new(std::sync::Mutex::new(Vec::new()));
        let mut state = test_state();
        state.test_recorder = Some(Arc::clone(&recorder));
        let _reply_rx = state
            .prepare("cmd-1".to_owned(), "turn-1".to_owned())
            .expect("prepared input");

        let result = ResultRecord {
            conversation_id: "conv-1".to_owned(),
            status: "ERROR".to_owned(),
            response: "syntax error on line 42".to_owned(),
            error: Some("syntax error".to_owned()),
            finished_reason: Some("error".to_owned()),
            num_turns: Some(1),
            usage: None,
            denied_actions: Vec::new(),
        };

        state
            .complete(result)
            .await
            .expect("small failed terminal completes");
        let commits = recorder.lock().unwrap().clone();
        let kinds: Vec<&str> = commits.iter().map(|(kind, _)| kind.as_str()).collect();
        assert_eq!(
            kinds,
            vec!["turn.started", "message.completed", "turn.completed"]
        );
        let turn_completed = &commits[2].1;
        assert_eq!(
            turn_completed.get("final_text"),
            Some(&Value::String("syntax error on line 42".to_owned()))
        );
        assert!(turn_completed.get("final_text_unavailable").is_none());
    }
}
