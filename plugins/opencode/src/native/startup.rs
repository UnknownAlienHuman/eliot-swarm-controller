//! Metadata-only startup timings, routed through the existing debug channel.

use std::time::{Instant, SystemTime, UNIX_EPOCH};

pub(crate) struct Step {
    phase: &'static str,
    started: Instant,
    started_at_ms: u64,
}

impl Step {
    pub(crate) fn new(phase: &'static str) -> Self {
        Self {
            phase,
            started: Instant::now(),
            started_at_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        }
    }
}

impl Drop for Step {
    fn drop(&mut self) {
        eliot_plugin::debug::record(
            "opencode.start_step",
            &[("method", self.phase)],
            &[
                ("started_at_ms", self.started_at_ms),
                ("elapsed_us", self.started.elapsed().as_micros() as u64),
            ],
        );
    }
}
