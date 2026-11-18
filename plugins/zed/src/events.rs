use eliot_core::{Error, NativeEvent, Result, SecretPolicy, SourceBatch, limits::MAX_EVENT_BYTES};
use eliot_plugin::{CommitSink, PeerClient, native_stdio::NativeFrame, session::StreamSource};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::time::{Duration, Instant};

pub(crate) const FAMILIES: &[&str] = &["session", "turn", "message", "tool"];

#[derive(Deserialize)]
struct Event {
    seq: u64,
    session_id: String,
    turn_id: String,
    command_id: String,
    #[serde(flatten)]
    body: Body,
}

#[derive(Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
enum Body {
    Started {},
    AssistantText {
        text: String,
    },
    ToolCall {
        tool_id: String,
        title: Option<String>,
        status: Option<String>,
    },
    PermissionDenied {
        tool_id: String,
    },
    Error {
        code: String,
    },
    Terminal {
        stop_reason: String,
    },
}

pub(crate) struct Active {
    pub turn: String,
    pub command: String,
    pub started: bool,
    text: String,
    truncated: bool,
    error: bool,
}

impl Active {
    pub fn new(turn: String, command: String) -> Self {
        Self {
            turn,
            command,
            started: false,
            text: String::new(),
            truncated: false,
            error: false,
        }
    }
}

pub(crate) struct Events {
    pub agent: String,
    pub generation: i64,
    pub session: String,
    pub policy: SecretPolicy,
    pub active: Option<Active>,
    seq: u64,
    sink: CommitSink,
    peer: PeerClient,
    credential: String,
    gap_reported: bool,
    gap_number: u64,
    writes_blocked: bool,
    source_unknown: bool,
    batch_unknown: bool,
    failure: Option<String>,
}

impl Events {
    pub fn new(
        agent: String,
        generation: i64,
        session: String,
        policy: SecretPolicy,
        peer: &PeerClient,
        credential: &str,
    ) -> Self {
        Self {
            agent,
            generation,
            session,
            policy,
            active: None,
            seq: 0,
            sink: CommitSink::new(peer.clone(), credential),
            peer: peer.clone(),
            credential: credential.into(),
            gap_reported: false,
            gap_number: 0,
            writes_blocked: false,
            source_unknown: false,
            batch_unknown: false,
            failure: None,
        }
    }

    pub async fn commit(&mut self, suffix: &str, kind: &str, mut data: Value) -> Result<()> {
        self.policy.redact_values(&mut data);
        let event = NativeEvent {
            kind: kind.into(),
            native_id: format!("zed:{}:{suffix}", self.session),
            data,
            ts: None,
        };
        let batch = SourceBatch {
            batch_id: format!("{}:batch", event.native_id),
            agent_id: self.agent.clone(),
            generation: self.generation,
            source: "stream".into(),
            events: vec![event],
            cursor: None,
        };
        self.sink
            .commit(batch, Instant::now() + Duration::from_secs(15))
            .await
            .map(|_| ())
    }

    pub async fn loss(&mut self, reason: &'static str) {
        self.writes_blocked = self.sink.is_unknown(&self.agent, self.generation, "stream");
        let mut source = StreamSource {
            sink: &mut self.sink,
            host: &self.peer,
            credential: &self.credential,
            agent_id: &self.agent,
            generation: self.generation,
            harness: "Zed",
            id_prefix: "zed",
            families: FAMILIES,
            writes_blocked: &mut self.writes_blocked,
            gap_reported: &mut self.gap_reported,
            gap_number: &mut self.gap_number,
            source_unknown: &mut self.source_unknown,
            batch_unknown: &mut self.batch_unknown,
            failure: &mut self.failure,
        };
        let _ = source
            .record_gap_until(reason, Instant::now() + Duration::from_secs(5))
            .await;
    }

    pub async fn consume(&mut self, frame: NativeFrame) -> Result<()> {
        let NativeFrame::Notification { method, params } = frame else {
            return Err(invalid_event());
        };
        if method != "session.event" {
            return Err(invalid_event());
        }
        let event: Event = serde_json::from_value(params).map_err(|_| invalid_event())?;
        if event.session_id != self.session || self.seq.checked_add(1) != Some(event.seq) {
            return Err(invalid_event());
        }
        let active = self.active.as_mut().ok_or_else(invalid_event)?;
        if event.turn_id != active.turn || event.command_id != active.command {
            return Err(invalid_event());
        }
        if active.started == matches!(&event.body, Body::Started {}) {
            return Err(invalid_event());
        }
        self.seq = event.seq;
        let key = event.seq.to_string();
        let mut data = json!({"turn_id":event.turn_id, "command_id":event.command_id});
        match event.body {
            Body::Started {} => {
                active.started = true;
                self.commit(&key, "turn.started", data).await?;
            }
            Body::AssistantText { text } => {
                // Retain the whole bounded response for literal redaction,
                // including credentials split across native chunks.
                if !active.truncated && active.text.len() + text.len() <= MAX_EVENT_BYTES {
                    active.text.push_str(&text);
                } else {
                    active.text.clear();
                    active.truncated = true;
                }
            }
            Body::ToolCall {
                tool_id,
                title,
                status,
            } => {
                if tool_id.is_empty() {
                    return Err(invalid_event());
                }
                data["item_id"] = json!(tool_id);
                data["tool"] = json!(title);
                let (kind, state) = match status.as_deref() {
                    Some("completed") => ("tool.completed", "completed"),
                    Some("failed") => ("tool.completed", "failed"),
                    Some("cancelled") => ("tool.completed", "cancelled"),
                    Some("in_progress") => ("tool.started", "running"),
                    _ => return Ok(()),
                };
                data["status"] = json!(state);
                self.commit(&key, kind, data).await?;
            }
            Body::PermissionDenied { tool_id } => {
                data["item_id"] = json!(tool_id);
                data["status"] = json!("failed");
                data["permission"] = json!("denied");
                self.commit(&key, "tool.completed", data).await?;
            }
            Body::Error { code } => {
                if code != "native_execution_failed" {
                    return Err(invalid_event());
                }
                active.error = true;
            }
            Body::Terminal { stop_reason } => {
                let status = match stop_reason.as_str() {
                    "end_turn" if !active.error => "completed",
                    "cancelled" if !active.error => "interrupted",
                    "max_tokens" | "max_turn_requests" | "refusal" | "error" => "failed",
                    _ => return Err(invalid_event()),
                };
                let mut message = data.clone();
                message["text"] = json!(self.policy.redact_text(&active.text));
                if active.truncated
                    || serde_json::to_vec(&message)
                        .map_or(true, |bytes| bytes.len() > MAX_EVENT_BYTES)
                {
                    data["final_text_unavailable"] = json!("event_budget");
                } else if !active.text.is_empty() {
                    self.commit(&format!("{key}:message"), "message.completed", message)
                        .await?;
                } else if status == "completed" {
                    data["final_text_unavailable"] = json!("empty_native_response");
                }
                data["status"] = json!(status);
                data["outcome"] = json!(status);
                data["native_status"] = json!(stop_reason);
                if status == "failed" {
                    data["native_error"] =
                        json!({"code":"zed_turn_failed","message":"Zed native turn failed"});
                }
                self.commit(&key, "turn.completed", data).await?;
                self.active = None;
            }
        }
        Ok(())
    }
}

fn invalid_event() -> Error {
    Error::new(
        "unknown",
        "Zed native event order or binding could not be verified",
    )
}
