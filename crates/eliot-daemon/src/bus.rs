//! In-process committed-event broadcast for L1 (`docs/spec/api.md`, `docs/spec/core.md`).
//!
//! The bus carries only Store-committed `eliot_core::Event` values in seq order.
//! Registration-before-high-water replay, lag accounting, and commit-before-publish
//! ordering live in the daemon router, not here. No Store or I/O in this module.
use tokio::sync::broadcast;
/// Bounded live-event broadcast. Clone to share; receivers lag explicitly.
#[derive(Clone)]
pub struct Bus(broadcast::Sender<eliot_core::Event>);
impl Bus {
    /// Create a bounded broadcast channel. Capacity is forced nonzero.
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (sender, _) = broadcast::channel(capacity);
        Self(sender)
    }
    /// Register a live receiver. Daemon calls this before capturing high-water.
    pub fn subscribe(&self) -> broadcast::Receiver<eliot_core::Event> {
        self.0.subscribe()
    }
    /// Publish committed events in supplied seq order. Ignores zero-subscriber sends.
    pub fn publish(&self, events: &[eliot_core::Event]) {
        for event in events {
            let _ = self.0.send(event.clone());
        }
    }
}
