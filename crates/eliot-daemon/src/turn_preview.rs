//! A bounded, disposable view of committed live turns.

use eliot_core::{
    Error, Event, Result, SecretPolicy,
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
const MAX_WAITERS: usize = 32;
const MAX_WAITERS_PER_PRINCIPAL: usize = 4;
// Known literals use exact prefix protection. The shape matcher sees the
// full retained prefix, while an unfinished ordinary word costs at most this tail.
// A trailing unbroken run longer than the tail is withheld whole until
// whitespace or the run cap; at the cap the complete run is redacted and shown.
const SHAPE_TAIL_BYTES: usize = 128;
const MAX_UNBROKEN_RUN_BYTES: usize = 4096;
const COALESCE: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub(crate) struct TurnPreviews(Arc<Inner>);

struct Inner {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    turns: HashMap<String, LiveTurn>,
    scheduled: bool,
    revision: u64,
    watches: HashMap<TurnKey, WatchGroup>,
    principal_waiters: HashMap<String, usize>,
    waiters: usize,
}

#[derive(Clone, Eq, Hash, PartialEq)]
struct TurnKey {
    agent: String,
    generation: i64,
    turn_id: String,
}

struct WatchGroup {
    waiters: usize,
    updates: watch::Sender<bool>,
}

pub(crate) struct PreviewWatch {
    hub: TurnPreviews,
    key: TurnKey,
    principal: String,
    pub updates: watch::Receiver<bool>,
}

impl Drop for PreviewWatch {
    fn drop(&mut self) {
        let mut state = self.hub.0.state.lock_recover();
        state.waiters -= 1;
        if let Some(count) = state.principal_waiters.get_mut(&self.principal) {
            *count -= 1;
            if *count == 0 {
                state.principal_waiters.remove(&self.principal);
            }
        }
        if let Some(group) = state.watches.get_mut(&self.key) {
            group.waiters -= 1;
            if group.waiters == 0 {
                state.watches.remove(&self.key);
            }
        }
    }
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
    pub fn render(&self) -> Option<TurnPreview> {
        let policy = SecretPolicy::new(self.credentials.iter());
        if policy.contains_native_secret(&self.message_id) {
            return None;
        }
        let (_, end) = policy.redact_prefix(self.text.as_bytes(), false);
        let mut text = policy.redact_text(&self.text[..self.text.floor_char_boundary(end)]);
        let unfinished = text.len() - text.trim_end_matches(|ch: char| !ch.is_whitespace()).len();
        let tail = unfinished.min(SHAPE_TAIL_BYTES);
        let withheld = if unfinished >= MAX_UNBROKEN_RUN_BYTES {
            0
        } else if unfinished > tail {
            unfinished
        } else {
            tail
        };
        let limit = (text.len() - withheld).min(MAX_TURN_PREVIEW_BYTES);
        text.truncate(text.floor_char_boundary(limit));
        Some(TurnPreview {
            message_id: self.message_id.clone(),
            text,
            truncated: self.truncated,
        })
    }
}

impl TurnPreviews {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            state: Mutex::new(State::default()),
        }))
    }

    pub fn subscribe(
        &self,
        request: &TurnPreviewWatchRequest,
        principal: String,
    ) -> Result<PreviewWatch> {
        let mut state = self.0.state.lock_recover();
        let count = state
            .principal_waiters
            .get(&principal)
            .copied()
            .unwrap_or(0);
        if state.waiters >= MAX_WAITERS || count >= MAX_WAITERS_PER_PRINCIPAL {
            return Err(Error::new("busy", "turn preview watch limit reached"));
        }
        let key = TurnKey {
            agent: request.agent.clone(),
            generation: request.generation,
            turn_id: request.turn_id.clone(),
        };
        let group = state
            .watches
            .entry(key.clone())
            .or_insert_with(|| WatchGroup {
                waiters: 0,
                updates: watch::Sender::new(false),
            });
        group.waiters += 1;
        let updates = group.updates.subscribe();
        state.waiters += 1;
        state.principal_waiters.insert(principal.clone(), count + 1);
        Ok(PreviewWatch {
            hub: self.clone(),
            key,
            principal,
            updates,
        })
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
            // A subscribed turn can lack a preview slot. Its exact terminal
            // still closes the watch without a reader querying Store on wake.
            for (key, group) in &state.watches {
                if key.agent == *agent
                    && ((generation_end && generation >= key.generation)
                        || (terminal
                            && generation == key.generation
                            && turn_id == Some(&key.turn_id))
                        || (event.kind == "turn.started"
                            && generation >= key.generation
                            && (generation != key.generation || turn_id != Some(&key.turn_id))))
                {
                    group.updates.send_replace(true);
                }
            }
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
        if chunk.sequence == message.next && !chunk.text.is_empty() {
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
        for turn in state.turns.values_mut().filter(|turn| turn.dirty) {
            turn.dirty = false;
            turn.published = turn.message.as_ref().map(|message| Snapshot {
                revision,
                message_id: message.id.clone(),
                text: Arc::from(message.text.as_str()),
                credentials: Arc::clone(&message.credentials),
                truncated: message.truncated,
            });
        }
        for (key, group) in &state.watches {
            if state.turns.get(&key.agent).is_some_and(|turn| {
                turn.generation == key.generation
                    && turn.turn_id == key.turn_id
                    && turn
                        .published
                        .as_ref()
                        .is_some_and(|snapshot| snapshot.revision == revision)
            }) {
                group.updates.send_modify(|_| {});
            }
        }
    }
}
