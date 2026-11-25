//! Shared best-effort transport for translated assistant deltas.

use crate::PeerClient;
use eliot_core::api::valid_preview_id;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::{sync::mpsc, time::Instant};

pub const PUT: &str = "turn.preview.put";
const MAX_CHUNK_BYTES: usize = 2 * 1024;
const QUEUE_CHUNKS: usize = 16;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TurnPreviewChunk {
    pub agent_id: String,
    pub generation: i64,
    pub turn_id: String,
    pub message_id: String,
    pub sequence: u64,
    pub text: String,
}

impl TurnPreviewChunk {
    pub fn valid(&self) -> bool {
        valid_preview_id(&self.turn_id)
            && valid_preview_id(&self.message_id)
            && self.text.len() <= MAX_CHUNK_BYTES
    }
}

pub struct PreviewSender {
    queue: mpsc::Sender<TurnPreviewChunk>,
    agent_id: String,
    generation: i64,
    current: Option<(String, String)>,
    next: u64,
    truncated: bool,
}

impl PreviewSender {
    pub fn new(peer: PeerClient, credential: String, agent_id: String, generation: i64) -> Self {
        let (queue, mut input) = mpsc::channel::<TurnPreviewChunk>(QUEUE_CHUNKS);
        tokio::spawn(async move {
            while let Some(chunk) = input.recv().await {
                if let Ok(mut params) = serde_json::to_value(chunk) {
                    params["credential"] = serde_json::json!(credential);
                    let _ = peer
                        .call(PUT, params, Instant::now() + Duration::from_millis(250))
                        .await;
                }
            }
        });
        Self {
            queue,
            agent_id,
            generation,
            current: None,
            next: 0,
            truncated: false,
        }
    }

    pub fn offer(&mut self, turn_id: &str, message_id: &str, mut text: &str) {
        if !self.bind(turn_id, message_id) || self.truncated {
            return;
        }
        while !text.is_empty() {
            // Keep one queue slot available for a gap even if no later delta
            // arrives to expose the missing sequence.
            if self.queue.capacity() <= 1 {
                self.gap(turn_id, message_id);
                return;
            }
            let (chunk, remaining) = text.split_at(text.floor_char_boundary(MAX_CHUNK_BYTES));
            text = remaining;
            self.enqueue(turn_id, message_id, chunk);
        }
    }

    /// An empty chunk marks the retained prefix truncated.
    pub fn gap(&mut self, turn_id: &str, message_id: &str) {
        if self.bind(turn_id, message_id) && !self.truncated {
            self.truncated = true;
            self.enqueue(turn_id, message_id, "");
        }
    }

    fn enqueue(&mut self, turn_id: &str, message_id: &str, text: &str) {
        let sequence = self.next;
        self.next = self.next.saturating_add(1);
        let _ = self.queue.try_send(TurnPreviewChunk {
            agent_id: self.agent_id.clone(),
            generation: self.generation,
            turn_id: turn_id.to_owned(),
            message_id: message_id.to_owned(),
            sequence,
            text: text.to_owned(),
        });
    }

    fn bind(&mut self, turn_id: &str, message_id: &str) -> bool {
        if !valid_preview_id(turn_id) || !valid_preview_id(message_id) {
            return false;
        }
        if self
            .current
            .as_ref()
            .is_none_or(|(turn, message)| turn != turn_id || message != message_id)
        {
            self.current = Some((turn_id.to_owned(), message_id.to_owned()));
            self.next = 0;
            self.truncated = false;
        }
        true
    }
}
