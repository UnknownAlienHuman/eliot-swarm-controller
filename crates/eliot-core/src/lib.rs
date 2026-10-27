#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt::{self, Display, Formatter};

fn empty_object() -> Value {
    Value::Object(serde_json::Map::new())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Created,
    Starting,
    Idle,
    Busy,
    Waiting,
    Stopped,
    Failed,
    Lost,
}

impl AgentState {
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Stopped | Self::Failed | Self::Lost)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Agent {
    pub id: String,
    pub generation: i64,
    pub name: Option<String>,
    pub project: String,
    pub harness: String,
    pub profile: String,
    pub workspace: Option<String>,
    pub state: AgentState,
    pub active_turn: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Agent {
    pub fn new(
        id: impl Into<String>,
        project: impl Into<String>,
        harness: impl Into<String>,
        profile: impl Into<String>,
        now: i64,
    ) -> Self {
        Self {
            id: id.into(),
            generation: 0,
            name: None,
            project: project.into(),
            harness: harness.into(),
            profile: profile.into(),
            workspace: None,
            state: AgentState::Created,
            active_turn: None,
            created_at: now,
            updated_at: now,
        }
    }

    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandState {
    Accepted,
    Queued,
    Dispatched,
    Applied,
    Rejected,
    Failed,
    Unknown,
    Cancelled,
}

impl CommandState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Applied | Self::Rejected | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Command {
    pub id: String,
    pub principal: String,
    pub method: String,
    pub verb: String,
    pub target: Option<String>,
    pub expected_generation: Option<i64>,
    pub generation: Option<i64>,
    pub args: Value,
    pub state: CommandState,
    pub turn_id: Option<String>,
    pub outcome: Option<Value>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Event {
    pub seq: i64,
    pub ts: i64,
    pub kind: String,
    pub agent: Option<String>,
    pub generation: Option<i64>,
    pub source: String,
    pub native_id: Option<String>,
    pub data: Value,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnState {
    Running,
    Completed,
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Turn {
    pub agent: String,
    pub generation: i64,
    pub turn_id: String,
    pub state: TurnState,
    pub status: Option<String>,
    pub final_text: Option<String>,
    pub started_at: Option<i64>,
    pub completed_at: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SubmitCommand {
    pub id: String,
    pub target: Option<String>,
    pub verb: String,
    #[serde(default = "empty_object")]
    pub args: Value,
    pub expected_generation: Option<i64>,
    pub expected_turn_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct NativeEvent {
    pub kind: String,
    pub native_id: String,
    pub data: Value,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceBatch {
    pub batch_id: String,
    pub agent_id: String,
    pub generation: i64,
    pub source: String,
    pub events: Vec<NativeEvent>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct BatchAck {
    pub first_seq: i64,
    pub last_seq: i64,
    pub cursor: Option<String>,
}

/// Store-facing facts derived by the reducer; callers persist them atomically with
/// the event and next Agent projection.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "effect", rename_all = "snake_case")]
pub enum Effect {
    CommandTurnAssociated {
        command_id: String,
        turn_id: String,
    },
    RequestPending {
        request_id: String,
        turn_id: String,
        pending: bool,
    },
    MessageCompleted {
        command_id: String,
        turn_id: String,
        text: String,
    },
    TurnTerminal {
        command_id: Option<String>,
        turn_id: String,
        state: TurnState,
        status: Option<String>,
        final_text: Option<String>,
        completed_at: i64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Error {
    pub code: String,
    pub message: String,
}

impl Error {
    pub fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Reduce one already-fenced native fact. Generation and source ownership are
/// checked by the Store before this function is called.
pub fn apply(agent: &Agent, event: &NativeEvent, now: i64) -> Result<(Agent, Vec<Effect>)> {
    if agent.is_terminal() {
        return Err(Error::new(
            "bad_state",
            format!("Agent {} is already terminal", agent.id),
        ));
    }

    let mut next = agent.clone();
    let mut effects = Vec::new();

    match event.kind.as_str() {
        "session.ready" => {
            if agent.state != AgentState::Starting || agent.active_turn.is_some() {
                return Err(bad_state(event, "session.ready requires a starting Agent"));
            }
            next.state = AgentState::Idle;
        }
        "turn.started" => {
            if agent.state != AgentState::Idle || agent.active_turn.is_some() {
                return Err(bad_state(event, "turn.started requires an idle Agent"));
            }
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            next.state = AgentState::Busy;
            next.active_turn = Some(turn_id.clone());
            effects.push(Effect::CommandTurnAssociated {
                command_id,
                turn_id,
            });
        }
        "message.completed" => {
            require_live_turn(agent, event)?;
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let text = required_text(event, "text")?;
            effects.push(Effect::MessageCompleted {
                command_id,
                turn_id,
                text,
            });
        }
        "request.opened" => {
            require_live_turn(agent, event)?;
            let request_id = required_text(event, "request_id")?;
            let turn_id = required_text(event, "turn_id")?;
            next.state = AgentState::Waiting;
            effects.push(Effect::RequestPending {
                request_id,
                turn_id,
                pending: true,
            });
        }
        "request.resolved" => {
            require_live_turn(agent, event)?;
            if agent.state != AgentState::Waiting {
                return Err(bad_state(
                    event,
                    "request.resolved requires a waiting Agent",
                ));
            }
            let request_id = required_text(event, "request_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let last_pending = event
                .data
                .get("last_pending")
                .and_then(Value::as_bool)
                .ok_or_else(|| {
                    Error::new(
                        "invalid",
                        "request.resolved requires Store-derived last_pending boolean",
                    )
                })?;
            next.state = if last_pending {
                AgentState::Busy
            } else {
                AgentState::Waiting
            };
            effects.push(Effect::RequestPending {
                request_id,
                turn_id,
                pending: false,
            });
        }
        "turn.completed" => {
            require_live_turn(agent, event)?;
            let command_id = required_text(event, "command_id")?;
            let turn_id = required_text(event, "turn_id")?;
            let status = required_text(event, "status")?;
            let final_text = optional_text(event, "final_text")?;
            next.state = AgentState::Idle;
            next.active_turn = None;
            effects.push(Effect::TurnTerminal {
                command_id: Some(command_id),
                turn_id,
                state: TurnState::Completed,
                status: Some(status),
                final_text,
                completed_at: now,
            });
        }
        "session.ended" => {
            if !matches!(
                agent.state,
                AgentState::Starting | AgentState::Idle | AgentState::Busy | AgentState::Waiting
            ) {
                return Err(bad_state(event, "session.ended requires a live Agent"));
            }
            if let Some(turn_id) = &agent.active_turn {
                if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
                    return Err(bad_state(
                        event,
                        "an active turn requires a busy or waiting Agent",
                    ));
                }
                effects.push(Effect::TurnTerminal {
                    command_id: None,
                    turn_id: turn_id.clone(),
                    state: TurnState::Unknown,
                    status: None,
                    final_text: None,
                    completed_at: now,
                });
            }
            next.state = AgentState::Stopped;
            next.active_turn = None;
        }
        kind => {
            return Err(Error::new(
                "unsupported",
                format!("unsupported native event kind: {kind}"),
            ));
        }
    }

    next.updated_at = now;
    Ok((next, effects))
}

fn require_live_turn(agent: &Agent, event: &NativeEvent) -> Result<()> {
    let turn_id = required_text(event, "turn_id")?;
    if agent.active_turn.as_deref() != Some(turn_id.as_str()) {
        return Err(Error::new(
            "source_integrity",
            format!(
                "{} turn_id does not match Agent {} active turn",
                event.kind, agent.id
            ),
        ));
    }
    if !matches!(agent.state, AgentState::Busy | AgentState::Waiting) {
        return Err(bad_state(event, "event requires a busy or waiting Agent"));
    }
    Ok(())
}

fn required_text(event: &NativeEvent, field: &str) -> Result<String> {
    event
        .data
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| {
            Error::new(
                "invalid",
                format!("{} requires non-empty string field {field}", event.kind),
            )
        })
}

fn optional_text(event: &NativeEvent, field: &str) -> Result<Option<String>> {
    match event.data.get(field) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(Error::new(
            "invalid",
            format!("{} field {field} must be a string", event.kind),
        )),
    }
}

fn bad_state(event: &NativeEvent, message: &str) -> Error {
    Error::new("bad_state", format!("{}: {message}", event.kind))
}
