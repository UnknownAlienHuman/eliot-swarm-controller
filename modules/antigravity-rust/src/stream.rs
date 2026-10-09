use std::collections::{BTreeMap, VecDeque};

use futures_util::StreamExt;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::AsyncRead;
use tokio_util::codec::{FramedRead, LinesCodec};

pub const MAX_FRAME_BYTES: usize = 1_048_576;
pub const MAX_STEPS: usize = 200;
pub const MAX_CHILDREN: usize = 100;
pub const MAX_TURNS: usize = 32;
pub const MAX_TOOL_ERRORS: usize = 32;
pub const MAX_INIT_TOOLS: usize = 64;
pub const MAX_CHILD_STRINGS: usize = 8;
pub const MAX_CHILD_URI_CHARS: usize = 256;
pub const MAX_TEXT_CHARS_PER_STEP: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct UsageSnapshot {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub thinking_tokens: Option<u64>,
    pub cache_read_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
}

impl UsageSnapshot {
    fn from_value(value: &Value) -> Option<Self> {
        let usage = value.as_object()?;
        let known_fields = [
            "input_tokens",
            "output_tokens",
            "thinking_tokens",
            "cache_read_tokens",
            "total_tokens",
        ];
        if !known_fields.iter().any(|field| usage.contains_key(*field)) {
            return None;
        }
        Some(Self {
            input_tokens: usage.get("input_tokens").and_then(Value::as_u64),
            output_tokens: usage.get("output_tokens").and_then(Value::as_u64),
            thinking_tokens: usage.get("thinking_tokens").and_then(Value::as_u64),
            cache_read_tokens: usage.get("cache_read_tokens").and_then(Value::as_u64),
            total_tokens: usage.get("total_tokens").and_then(Value::as_u64),
        })
    }

    fn is_complete(self) -> bool {
        self.input_tokens.is_some()
            && self.output_tokens.is_some()
            && self.thinking_tokens.is_some()
            && self.cache_read_tokens.is_some()
            && self.total_tokens.is_some()
    }

    fn delta_from(self, previous: Self) -> Option<UsageDelta> {
        Some(UsageDelta {
            input_tokens: self.input_tokens?.checked_sub(previous.input_tokens?)?,
            output_tokens: self.output_tokens?.checked_sub(previous.output_tokens?)?,
            thinking_tokens: self
                .thinking_tokens?
                .checked_sub(previous.thinking_tokens?)?,
            cache_read_tokens: self
                .cache_read_tokens?
                .checked_sub(previous.cache_read_tokens?)?,
            total_tokens: self.total_tokens?.checked_sub(previous.total_tokens?)?,
        })
    }

    fn delta_from_zero(self) -> Option<UsageDelta> {
        Some(UsageDelta {
            input_tokens: self.input_tokens?,
            output_tokens: self.output_tokens?,
            thinking_tokens: self.thinking_tokens?,
            cache_read_tokens: self.cache_read_tokens?,
            total_tokens: self.total_tokens?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct UsageDelta {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub thinking_tokens: u64,
    pub cache_read_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageBasis {
    FreshProcessZero,
    PreviousTerminal,
    ResumedBaseline,
    ResetOrGap,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct TurnUsageEvidence {
    pub cumulative: Option<UsageSnapshot>,
    pub delta: Option<UsageDelta>,
    pub basis: UsageBasis,
}

#[derive(Debug, Clone)]
struct UsageBaseline {
    conversation_id: String,
    result_ordinal: u64,
    num_turns: u64,
    duration_seconds: f64,
    usage: UsageSnapshot,
    gaps: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    AwaitingInit,
    Ready,
    InitFailed,
    StreamEnded,
    StreamFailed,
}

#[derive(Debug, Clone, Serialize)]
pub struct Init {
    pub conversation_id: String,
    pub cwd: Option<String>,
    pub tools: Vec<String>,
    pub permission_mode: Option<String>,
    pub model: Option<String>,
    pub agent: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Step {
    pub step_index: u64,
    pub state: Option<String>,
    pub step_type: Option<String>,
    pub tool_name: Option<String>,
    pub text_chars: u64,
    pub usage: Option<UsageSnapshot>,
    pub tool_error_type: Option<String>,
    pub subagent_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Child {
    pub conversation_id: String,
    pub type_name: Option<String>,
    pub role: Option<String>,
    pub workspace_uris: Vec<String>,
    pub log_uri: Option<String>,
    pub status: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct Turn {
    pub status: String,
    pub conversation_id: Option<String>,
    pub result_ordinal: u64,
    pub response_sha256: Option<String>,
    pub response_chars: u64,
    pub num_turns: Option<u64>,
    pub duration_seconds: Option<f64>,
    pub duration_delta_seconds: Option<f64>,
    pub usage: TurnUsageEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TerminalDisposition {
    Applied,
    Rejected,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolError {
    pub step_index: u64,
    pub tool_name: Option<String>,
    pub error_type: String,
}

#[derive(Debug)]
pub struct StreamState {
    pub phase: Phase,
    pub init: Option<Init>,
    pub init_failure_status: Option<String>,
    pub execution: &'static str,
    pub steps: BTreeMap<u64, Step>,
    pub children: BTreeMap<String, Child>,
    pub turns: VecDeque<Turn>,
    pub result_ordinal: u64,
    pub tool_errors: VecDeque<ToolError>,
    pub native_events_seen: u64,
    pub malformed_lines: u64,
    pub other_events: u64,
    pub gaps: u64,
    pub stream_error_code: Option<&'static str>,
    pub exit_code: Option<i32>,
    resumed_baseline_pending: bool,
    last_terminal_usage: Option<UsageBaseline>,
}

impl StreamState {
    pub fn new(initial_result_ordinal: u64) -> Self {
        Self {
            phase: Phase::AwaitingInit,
            init: None,
            init_failure_status: None,
            execution: "not_started",
            steps: BTreeMap::new(),
            children: BTreeMap::new(),
            turns: VecDeque::new(),
            result_ordinal: initial_result_ordinal,
            tool_errors: VecDeque::new(),
            native_events_seen: 0,
            malformed_lines: 0,
            other_events: 0,
            gaps: 0,
            stream_error_code: None,
            exit_code: None,
            resumed_baseline_pending: false,
            last_terminal_usage: None,
        }
    }

    pub fn set_resume_baseline_pending(&mut self, pending: bool) {
        self.resumed_baseline_pending = pending;
        self.last_terminal_usage = None;
    }

    pub fn consume_line(&mut self, line: &[u8]) {
        if line.len() > MAX_FRAME_BYTES {
            self.malformed_lines = self.malformed_lines.saturating_add(1);
            self.gaps = self.gaps.saturating_add(1);
            return;
        }
        let event: Value = match serde_json::from_slice(line) {
            Ok(event) => event,
            Err(_) => {
                self.malformed_lines = self.malformed_lines.saturating_add(1);
                self.gaps = self.gaps.saturating_add(1);
                return;
            }
        };
        let Some(kind) = event.get("event").and_then(Value::as_str) else {
            self.malformed_lines = self.malformed_lines.saturating_add(1);
            self.gaps = self.gaps.saturating_add(1);
            return;
        };
        self.native_events_seen = self.native_events_seen.saturating_add(1);
        match kind {
            "init" => self.apply_init(&event),
            "step_update" => self.apply_step(&event),
            "result" => self.apply_result(&event),
            _ => self.other_events = self.other_events.saturating_add(1),
        }
    }

    pub fn note_stream_failure(&mut self) {
        self.phase = Phase::StreamFailed;
        self.execution = "stream_failed";
        self.stream_error_code = Some("NATIVE_STREAM_FAILED");
    }

    pub fn note_malformed_frame(&mut self) {
        self.malformed_lines = self.malformed_lines.saturating_add(1);
        self.gaps = self.gaps.saturating_add(1);
    }

    pub fn note_stream_end(&mut self, exit_code: Option<i32>) {
        self.phase = Phase::StreamEnded;
        self.execution = "stream_ended";
        self.exit_code = exit_code;
    }

    pub fn latest_turn(&self) -> Option<&Turn> {
        self.turns.back()
    }

    pub fn terminal_for(
        &self,
        expected_conversation: &str,
    ) -> Option<(&Turn, TerminalDisposition)> {
        let turn = self.latest_turn()?;
        if turn.conversation_id.as_deref() != Some(expected_conversation) {
            return None;
        }
        let disposition = match turn.status.as_str() {
            "SUCCESS" => TerminalDisposition::Applied,
            "ERROR" | "CANCELED" | "INTERRUPTED" => TerminalDisposition::Rejected,
            _ => return None,
        };
        Some((turn, disposition))
    }

    pub fn snapshot(&self) -> Value {
        json!({
            "phase": self.phase,
            "init": self.init,
            "init_failure_status": self.init_failure_status,
            "execution": self.execution,
            "steps": self.steps.values().collect::<Vec<_>>(),
            "children": self.children.values().collect::<Vec<_>>(),
            "turns": self.turns,
            "result_ordinal": self.result_ordinal,
            "tool_errors": self.tool_errors,
            "native_events_seen": self.native_events_seen,
            "malformed_lines": self.malformed_lines,
            "other_events": self.other_events,
            "gaps": self.gaps,
            "stream_error_code": self.stream_error_code,
            "exit_code": self.exit_code,
        })
    }

    fn apply_init(&mut self, event: &Value) {
        if self.init.is_some() {
            self.gaps = self.gaps.saturating_add(1);
            return;
        }
        let Some(conversation_id) = event
            .get("conversation_id")
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
        else {
            self.gaps = self.gaps.saturating_add(1);
            return;
        };
        let payload = event.get("init").unwrap_or(&Value::Null);
        let (tools, tools_truncated) =
            bounded_string_array(payload.get("tools"), MAX_INIT_TOOLS, 256);
        if tools_truncated {
            self.gaps = self.gaps.saturating_add(1);
        }
        self.init = Some(Init {
            conversation_id: bounded_string(conversation_id, 512),
            cwd: string_field(payload, "cwd", 2048),
            tools,
            permission_mode: string_field(payload, "permission_mode", 128),
            model: string_field(payload, "model", 256),
            agent: string_field(payload, "agent", 256),
        });
        if self.phase == Phase::AwaitingInit {
            self.phase = Phase::Ready;
        }
        if self.execution == "not_started" {
            self.execution = "running";
        }
    }

    fn apply_step(&mut self, event: &Value) {
        let Some(payload) = event.get("step_update").and_then(Value::as_object) else {
            self.gaps = self.gaps.saturating_add(1);
            return;
        };
        let Some(index) = payload.get("step_index").and_then(Value::as_u64) else {
            self.gaps = self.gaps.saturating_add(1);
            return;
        };
        if let (Some(root), Some(carrier)) = (
            self.init.as_ref().map(|init| init.conversation_id.as_str()),
            payload.get("conversation_id").and_then(Value::as_str),
        ) && carrier != root
        {
            self.gaps = self.gaps.saturating_add(1);
            return;
        }

        if !self.steps.contains_key(&index) && self.steps.len() >= MAX_STEPS {
            if let Some(oldest) = self.steps.keys().next().copied() {
                self.steps.remove(&oldest);
            }
            self.gaps = self.gaps.saturating_add(1);
        }
        let step = self.steps.entry(index).or_insert_with(|| Step {
            step_index: index,
            state: None,
            step_type: None,
            tool_name: None,
            text_chars: 0,
            usage: None,
            tool_error_type: None,
            subagent_ids: Vec::new(),
        });
        step.state = string_field_from_map(payload, "state", 64).or(step.state.take());
        step.step_type = string_field_from_map(payload, "step_type", 128).or(step.step_type.take());
        step.tool_name = string_field_from_map(payload, "tool_name", 256).or(step.tool_name.take());
        if let Some(delta) = payload.get("text_delta").and_then(Value::as_str) {
            step.text_chars = step
                .text_chars
                .saturating_add(delta.chars().count() as u64)
                .min(MAX_TEXT_CHARS_PER_STEP);
        }
        if let Some(raw_usage) = payload.get("usage") {
            if let Some(usage) = UsageSnapshot::from_value(raw_usage) {
                step.usage = Some(usage);
            } else {
                self.gaps = self.gaps.saturating_add(1);
            }
        }

        let tool_info = payload.get("tool_info").and_then(Value::as_object);
        let error = tool_info
            .and_then(|tool| tool.get("error"))
            .and_then(Value::as_object);
        if step.tool_error_type.is_none()
            && let Some(error) = error
        {
            let error_type = error
                .get("type")
                .and_then(Value::as_str)
                .map(|value| bounded_string(value, 128))
                .unwrap_or_else(|| "unknown".to_owned());
            step.tool_error_type = Some(error_type.clone());
            let tool_name = step.tool_name.clone().or_else(|| {
                tool_info
                    .and_then(|tool| tool.get("name"))
                    .and_then(Value::as_str)
                    .map(|value| bounded_string(value, 256))
            });
            if self.tool_errors.len() == MAX_TOOL_ERRORS {
                self.tool_errors.pop_front();
                self.gaps = self.gaps.saturating_add(1);
            }
            self.tool_errors.push_back(ToolError {
                step_index: index,
                tool_name,
                error_type,
            });
        }
        self.apply_children(index, payload.get("subagent_info"));
    }

    fn apply_children(&mut self, step_index: u64, info: Option<&Value>) {
        let Some(subagents) = info
            .and_then(|value| value.get("subagents"))
            .and_then(Value::as_array)
        else {
            return;
        };
        if subagents.len() > MAX_CHILDREN {
            self.gaps = self.gaps.saturating_add(1);
        }
        let mut ids = Vec::new();
        for entry in subagents.iter().take(MAX_CHILDREN) {
            let Some(id) = entry
                .get("conversation_id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            else {
                self.gaps = self.gaps.saturating_add(1);
                continue;
            };
            if !self.children.contains_key(id) && self.children.len() >= MAX_CHILDREN {
                self.gaps = self.gaps.saturating_add(1);
                continue;
            }
            let child = self
                .children
                .entry(bounded_string(id, 512))
                .or_insert_with(|| Child {
                    conversation_id: bounded_string(id, 512),
                    type_name: None,
                    role: None,
                    workspace_uris: Vec::new(),
                    log_uri: None,
                    status: "observed",
                });
            child.type_name = string_field(entry, "type_name", 256).or(child.type_name.take());
            child.role = string_field(entry, "role", 128).or(child.role.take());
            child.log_uri = string_field(entry, "log_uri", 2048).or(child.log_uri.take());
            let (workspace_uris, truncated) = bounded_string_array(
                entry.get("workspace_uris"),
                MAX_CHILD_STRINGS,
                MAX_CHILD_URI_CHARS,
            );
            if truncated {
                self.gaps = self.gaps.saturating_add(1);
            }
            child.workspace_uris = workspace_uris;
            ids.push(child.conversation_id.clone());
        }
        if let Some(step) = self.steps.get_mut(&step_index) {
            for id in ids {
                if !step.subagent_ids.contains(&id) {
                    step.subagent_ids.push(id);
                }
            }
        }
    }

    fn usage_evidence(
        &self,
        status: &str,
        conversation_id: Option<&str>,
        result_ordinal: u64,
        num_turns: Option<u64>,
        duration_seconds: Option<f64>,
        cumulative: Option<UsageSnapshot>,
    ) -> (TurnUsageEvidence, Option<f64>) {
        let reset = || {
            (
                TurnUsageEvidence {
                    cumulative,
                    delta: None,
                    basis: UsageBasis::ResetOrGap,
                },
                None,
            )
        };
        if !is_terminal_status(status) {
            return reset();
        }
        let (
            Some(init),
            Some(conversation_id),
            Some(num_turns),
            Some(duration_seconds),
            Some(usage),
        ) = (
            self.init.as_ref(),
            conversation_id,
            num_turns,
            duration_seconds,
            cumulative,
        )
        else {
            return reset();
        };
        if init.conversation_id.as_str() != conversation_id
            || !duration_seconds.is_finite()
            || duration_seconds < 0.0
            || !usage.is_complete()
        {
            return reset();
        }
        if let Some(previous) = &self.last_terminal_usage {
            if previous.conversation_id == conversation_id
                && previous.result_ordinal.checked_add(1) == Some(result_ordinal)
                && previous.num_turns.checked_add(1) == Some(num_turns)
                && previous.gaps == self.gaps
                && duration_seconds >= previous.duration_seconds
                && let Some(delta) = usage.delta_from(previous.usage)
            {
                return (
                    TurnUsageEvidence {
                        cumulative,
                        delta: Some(delta),
                        basis: UsageBasis::PreviousTerminal,
                    },
                    Some(duration_seconds - previous.duration_seconds),
                );
            }
            return reset();
        }
        if self.resumed_baseline_pending {
            return (
                TurnUsageEvidence {
                    cumulative,
                    delta: None,
                    basis: UsageBasis::ResumedBaseline,
                },
                None,
            );
        }
        if result_ordinal == 1 && num_turns == 1 && self.gaps == 0 {
            return (
                TurnUsageEvidence {
                    cumulative,
                    delta: usage.delta_from_zero(),
                    basis: UsageBasis::FreshProcessZero,
                },
                Some(duration_seconds),
            );
        }
        reset()
    }

    fn remember_terminal_usage(&mut self, turn: &Turn) {
        if !is_terminal_status(&turn.status) {
            return;
        }
        self.resumed_baseline_pending = false;
        let (
            Some(init),
            Some(conversation_id),
            Some(num_turns),
            Some(duration_seconds),
            Some(usage),
        ) = (
            self.init.as_ref(),
            turn.conversation_id.as_ref(),
            turn.num_turns,
            turn.duration_seconds,
            turn.usage.cumulative,
        )
        else {
            self.last_terminal_usage = None;
            return;
        };
        if init.conversation_id.as_str() != conversation_id.as_str() || !usage.is_complete() {
            self.last_terminal_usage = None;
            return;
        }
        self.last_terminal_usage = Some(UsageBaseline {
            conversation_id: conversation_id.clone(),
            result_ordinal: turn.result_ordinal,
            num_turns,
            duration_seconds,
            usage,
            gaps: self.gaps,
        });
    }

    fn apply_result(&mut self, event: &Value) {
        let Some(payload) = event.get("result").and_then(Value::as_object) else {
            self.gaps = self.gaps.saturating_add(1);
            return;
        };
        let Some(status) = payload.get("status").and_then(Value::as_str) else {
            self.gaps = self.gaps.saturating_add(1);
            return;
        };
        self.result_ordinal = self.result_ordinal.saturating_add(1);
        let status = bounded_string(status, 64);
        let response = payload.get("response").and_then(Value::as_str);
        let conversation_id = payload
            .get("conversation_id")
            .and_then(Value::as_str)
            .or_else(|| event.get("conversation_id").and_then(Value::as_str))
            .map(|value| bounded_string(value, 512));
        let num_turns = payload.get("num_turns").and_then(Value::as_u64);
        let duration_seconds = payload
            .get("duration_seconds")
            .and_then(nonnegative_finite_f64);
        let cumulative_usage = payload.get("usage").and_then(UsageSnapshot::from_value);
        let (usage, duration_delta_seconds) = self.usage_evidence(
            &status,
            conversation_id.as_deref(),
            self.result_ordinal,
            num_turns,
            duration_seconds,
            cumulative_usage,
        );
        let turn = Turn {
            status,
            conversation_id,
            result_ordinal: self.result_ordinal,
            response_sha256: response.map(sha256_hex),
            response_chars: response
                .map(|value| value.chars().count() as u64)
                .unwrap_or(0),
            num_turns,
            duration_seconds,
            duration_delta_seconds,
            usage,
        };
        if self.turns.len() == MAX_TURNS {
            self.turns.pop_front();
            self.gaps = self.gaps.saturating_add(1);
        }
        self.turns.push_back(turn.clone());
        if self.phase == Phase::AwaitingInit {
            if matches!(turn.status.as_str(), "ERROR" | "CANCELED" | "INTERRUPTED") {
                self.phase = Phase::InitFailed;
                self.init_failure_status = Some(turn.status);
                self.execution = "init_failed";
            } else {
                // A pre-init SUCCESS/WAITING/RUNNING/unknown result does not
                // prove that initialization failed or establish a session.
                self.gaps = self.gaps.saturating_add(1);
            }
            self.resumed_baseline_pending = false;
            self.last_terminal_usage = None;
            return;
        }
        self.execution = match turn.status.as_str() {
            "SUCCESS" => "turn_completed",
            "WAITING" => "turn_waiting",
            "RUNNING" => "turn_open",
            _ => "turn_failed",
        };
        self.remember_terminal_usage(&turn);
    }
}

fn is_terminal_status(status: &str) -> bool {
    matches!(
        status,
        "SUCCESS" | "ERROR" | "CANCELED" | "INTERRUPTED" | "INVALID"
    )
}

fn nonnegative_finite_f64(value: &Value) -> Option<f64> {
    let value = value.as_f64()?;
    (value.is_finite() && value >= 0.0).then_some(value)
}

pub struct NativeLineReader<R> {
    framed: FramedRead<R, LinesCodec>,
}

impl<R> NativeLineReader<R>
where
    R: AsyncRead + Unpin,
{
    pub fn new(reader: R) -> Self {
        Self {
            framed: FramedRead::new(reader, LinesCodec::new_with_max_length(MAX_FRAME_BYTES)),
        }
    }

    pub async fn next_line(&mut self) -> Option<Result<String, &'static str>> {
        match self.framed.next().await? {
            Ok(line) => Some(Ok(line)),
            Err(_) => Some(Err("NATIVE_FRAME_INVALID_OR_OVERSIZED")),
        }
    }
}

fn sha256_hex(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn string_field(value: &Value, key: &str, max_chars: usize) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(|value| bounded_string(value, max_chars))
}

fn string_field_from_map(
    value: &serde_json::Map<String, Value>,
    key: &str,
    max_chars: usize,
) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(|value| bounded_string(value, max_chars))
}

fn bounded_string_array(
    value: Option<&Value>,
    max_items: usize,
    max_chars: usize,
) -> (Vec<String>, bool) {
    let Some(items) = value.and_then(Value::as_array) else {
        return (Vec::new(), value.is_some());
    };
    let mut out = Vec::new();
    let mut truncated = items.len() > max_items;
    for item in items.iter().take(max_items) {
        let Some(value) = item.as_str() else {
            truncated = true;
            continue;
        };
        let bounded = bounded_string(value, max_chars);
        truncated |= bounded.chars().count() < value.chars().count();
        out.push(bounded);
    }
    (out, truncated)
}

fn bounded_string(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}
