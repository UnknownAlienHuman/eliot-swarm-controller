//! A bounded, disposable view of committed live turns.

use eliot_core::{
    Event, SecretPolicy,
    api::{MAX_TURN_PREVIEW_BYTES, TurnPreview, TurnPreviewWatchRequest, valid_preview_id},
};
use eliot_plugin::{sync::LockExt, turn_preview::TurnPreviewChunk};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::watch;

const MAX_TURNS: usize = 64;
const COALESCE: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub(crate) struct TurnPreviews(Arc<Inner>);

struct Inner {
    state: Mutex<State>,
    updates: watch::Sender<u64>,
}

#[derive(Default)]
struct State {
    turns: HashMap<String, LiveTurn>,
    scheduled: bool,
    revision: u64,
}

struct LiveTurn {
    generation: i64,
    turn_id: String,
    message: Option<Message>,
    published: Option<Snapshot>,
    dirty: bool,
}

struct Message {
    id: String,
    next: u64,
    text: String,
    truncated: bool,
    credentials: Arc<[String]>,
}

#[derive(Clone)]
pub(crate) struct Snapshot {
    pub revision: u64,
    message_id: String,
    text: Arc<str>,
    credentials: Arc<[String]>,
    truncated: bool,
}

impl Snapshot {
    pub fn render(&self, global: &[String]) -> Option<TurnPreview> {
        let policy = SecretPolicy::new(self.credentials.iter().chain(global));
        if policy.contains_native_secret(&self.message_id) {
            return None;
        }
        // Keep the unfinished last word out of shape matching. Also withhold
        // any dangling prefix of a literal credential, even across spaces.
        let whole = self.text.trim_end_matches(|ch: char| !ch.is_whitespace());
        let (_, end) = policy.redact_prefix(whole.as_bytes(), false);
        let end = whole.floor_char_boundary(end);
        Some(TurnPreview {
            message_id: self.message_id.clone(),
            text: policy.redact_bounded(&whole[..end], MAX_TURN_PREVIEW_BYTES),
            truncated: self.truncated,
        })
    }
}

impl TurnPreviews {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            state: Mutex::new(State::default()),
            updates: watch::Sender::new(0),
        }))
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.0.updates.subscribe()
    }

    pub fn snapshot(&self, request: &TurnPreviewWatchRequest) -> Option<Snapshot> {
        let state = self.0.state.lock_recover();
        let turn = state.turns.get(&request.agent)?;
        if turn.generation != request.generation || turn.turn_id != request.turn_id {
            return None;
        }
        turn.published.clone()
    }

    pub fn committed(&self, event: &Event) {
        let (Some(agent), Some(generation)) = (&event.agent, event.generation) else {
            return;
        };
        let terminal = matches!(event.kind.as_str(), "turn.completed" | "turn.unknown");
        let generation_end = matches!(
            event.kind.as_str(),
            "agent.starting" | "agent.stopped" | "agent.failed" | "agent.lost" | "agent.deleted"
        );
        if !terminal && !generation_end && event.kind != "turn.started" {
            return;
        }
        let turn_id = event.data["turn_id"].as_str();
        {
            let mut state = self.0.state.lock_recover();
            if state.turns.get(agent).is_some_and(|live| {
                (generation_end && generation >= live.generation)
                    || (terminal && generation == live.generation && turn_id == Some(&live.turn_id))
            }) {
                state.turns.remove(agent);
            }
            if event.kind == "turn.started"
                && let Some(turn_id) = turn_id.filter(|id| valid_preview_id(id))
                && state.turns.get(agent).is_none_or(|live| {
                    generation >= live.generation
                        && (generation != live.generation || turn_id != live.turn_id)
                })
                && (state.turns.contains_key(agent) || state.turns.len() < MAX_TURNS)
            {
                state.turns.insert(
                    agent.clone(),
                    LiveTurn {
                        generation,
                        turn_id: turn_id.to_owned(),
                        message: None,
                        published: None,
                        dirty: false,
                    },
                );
            }
        }
        // Wake terminal watchers even if this turn never acquired a preview slot.
        if terminal || generation_end {
            self.notify();
        }
    }

    pub fn put(&self, chunk: TurnPreviewChunk, credentials: Arc<[String]>) {
        let mut state = self.0.state.lock_recover();
        let Some(turn) = state
            .turns
            .get_mut(&chunk.agent_id)
            .filter(|turn| turn.generation == chunk.generation && turn.turn_id == chunk.turn_id)
        else {
            return;
        };
        if turn
            .message
            .as_ref()
            .is_none_or(|msg| msg.id != chunk.message_id)
        {
            if chunk.sequence != 0 {
                return;
            }
            turn.message = Some(Message {
                id: chunk.message_id,
                next: 0,
                text: String::new(),
                truncated: false,
                credentials,
            });
        }
        let Some(message) = &mut turn.message else {
            return;
        };
        if message.truncated || chunk.sequence < message.next {
            return;
        }
        if chunk.sequence == message.next {
            let remaining = MAX_TURN_PREVIEW_BYTES - message.text.len();
            let accepted = chunk.text.floor_char_boundary(remaining);
            message.text.push_str(&chunk.text[..accepted]);
            message.truncated = accepted < chunk.text.len();
            message.next = message.next.saturating_add(1);
        } else {
            message.truncated = true;
        }
        turn.dirty = true;
        if !state.scheduled {
            state.scheduled = true;
            let hub = self.clone();
            let deadline = tokio::time::Instant::now() + COALESCE;
            tokio::spawn(async move {
                tokio::time::sleep_until(deadline).await;
                hub.flush();
            });
        }
    }

    fn flush(&self) {
        let mut state = self.0.state.lock_recover();
        state.scheduled = false;
        state.revision = state.revision.saturating_add(1);
        let revision = state.revision;
        let mut published = false;
        for turn in state.turns.values_mut().filter(|turn| turn.dirty) {
            turn.dirty = false;
            turn.published = turn.message.as_ref().map(|message| Snapshot {
                revision,
                message_id: message.id.clone(),
                text: Arc::from(message.text.as_str()),
                credentials: Arc::clone(&message.credentials),
                truncated: message.truncated,
            });
            published = true;
        }
        drop(state);
        if published {
            self.notify();
        }
    }

    fn notify(&self) {
        self.0
            .updates
            .send_modify(|tick| *tick = tick.wrapping_add(1));
    }
}
