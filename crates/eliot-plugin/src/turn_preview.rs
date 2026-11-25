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
            && !self.text.is_empty()
            && self.text.len() <= MAX_CHUNK_BYTES
    }
}

pub struct PreviewSender {
    queue: mpsc::Sender<TurnPreviewChunk>,
    agent_id: String,
    generation: i64,
    current: Option<(String, String)>,
    next: u64,
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
        }
    }

    pub fn offer(&mut self, turn_id: &str, message_id: &str, mut text: &str) {
        if !valid_preview_id(turn_id) || !valid_preview_id(message_id) {
            return;
        }
        self.bind(turn_id, message_id);
        while !text.is_empty() {
            let (chunk, remaining) = text.split_at(text.floor_char_boundary(MAX_CHUNK_BYTES));
            text = remaining;
            let sequence = self.next;
            self.next = self.next.saturating_add(1);
            if self
                .queue
                .try_send(TurnPreviewChunk {
                    agent_id: self.agent_id.clone(),
                    generation: self.generation,
                    turn_id: turn_id.to_owned(),
                    message_id: message_id.to_owned(),
                    sequence,
                    text: chunk.to_owned(),
                })
                .is_err()
            {
                break;
            }
        }
    }

    /// A native pump drop consumes a sequence before any later text.
    pub fn gap(&mut self, turn_id: &str, message_id: &str) {
        self.bind(turn_id, message_id);
        self.next = self.next.saturating_add(1);
    }

    fn bind(&mut self, turn_id: &str, message_id: &str) {
        if self
            .current
            .as_ref()
            .is_none_or(|(turn, message)| turn != turn_id || message != message_id)
        {
            self.current = Some((turn_id.to_owned(), message_id.to_owned()));
            self.next = 0;
        }
    }
}
