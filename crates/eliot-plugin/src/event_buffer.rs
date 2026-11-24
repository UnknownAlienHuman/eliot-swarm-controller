use std::collections::VecDeque;

use serde::Serialize;
use serde_json::Value;

const MAX_EVENTS: usize = 128;
const MAX_BYTES: usize = 256 * 1024;

/// Native JSON-RPC facts held until the adapter can associate them.
#[derive(Clone, Serialize)]
#[serde(untagged)]
pub enum BufferedRpcFrame {
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

/// Holds early native events in arrival order. Adapters own admission to the
/// queue, association with a turn and the source transition on overflow.
pub struct EarlyEventBuffer<T> {
    events: VecDeque<(T, usize)>,
    bytes: usize,
}

impl<T> Default for EarlyEventBuffer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> EarlyEventBuffer<T> {
    pub fn new() -> Self {
        Self {
            events: VecDeque::new(),
            bytes: 0,
        }
    }

    /// An unencodable or over-limit event leaves the retained queue intact.
    pub fn try_push(&mut self, event: T) -> bool
    where
        T: Serialize,
    {
        if self.events.len() >= MAX_EVENTS {
            return false;
        }
        let Ok(encoded) = serde_json::to_vec(&event) else {
            return false;
        };
        let size = encoded.len();
        if size > MAX_BYTES - self.bytes {
            return false;
        }
        self.events.push_back((event, size));
        self.bytes += size;
        true
    }

    pub fn front(&self) -> Option<&T> {
        self.events.front().map(|(event, _)| event)
    }

    pub fn pop_front(&mut self) -> Option<T> {
        let (event, size) = self.events.pop_front()?;
        self.bytes -= size;
        Some(event)
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.events.iter().map(|(event, _)| event)
    }

    /// Take an event that can be associated independently of the head.
    pub fn take_first(&mut self, matches: impl FnMut(&T) -> bool) -> Option<T> {
        let index = self.iter().position(matches)?;
        let (event, size) = self.events.remove(index)?;
        self.bytes -= size;
        Some(event)
    }

    pub fn retain(&mut self, mut keep: impl FnMut(&T) -> bool) {
        self.events.retain(|(event, size)| {
            if keep(event) {
                return true;
            }
            self.bytes -= size;
            false
        });
    }

    pub fn clear(&mut self) {
        self.events.clear();
        self.bytes = 0;
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }
}
