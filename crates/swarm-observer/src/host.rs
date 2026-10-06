//! Lazy bridge from the host's bounded `swarm-telemetry` record stream into
//! the optional file recorder. Constructing this handle starts no worker and
//! creates no directory; the first real metadata line starts the frozen
//! recorder. The observer callback runs outside the Store/kernel caller.

use crate::{LiveConfigSource, Recorder, RecorderConfig, RecorderStats};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

#[derive(Clone, Copy, Debug, Default)]
pub struct HostRecorderStats {
    pub enabled: bool,
    pub started: bool,
    pub startup_failures: u64,
    pub startup_dropped_records: u64,
    pub startup_dropped_bytes: u64,
    pub callback_errors: u64,
    pub post_shutdown_dropped_records: u64,
    pub post_shutdown_dropped_bytes: u64,
    pub recorder: RecorderStats,
}

struct State {
    config: RecorderConfig,
    live_config: Option<LiveConfigSource>,
    recorder: Option<Recorder>,
    start_attempted: bool,
    closed: bool,
    startup_failures: u64,
    startup_dropped_records: u64,
    startup_dropped_bytes: u64,
    callback_errors: u64,
    post_shutdown_dropped_records: u64,
    post_shutdown_dropped_bytes: u64,
    last_error_code: Option<&'static str>,
    final_stats: Option<RecorderStats>,
}

/// A cloneable callback target. `observe_line` validates through
/// `Recorder::append_line`; it never accepts or stores free-form event text.
pub struct HostRecorder {
    state: Mutex<State>,
}

impl HostRecorder {
    /// Keep this constructor side-effect-free. The host may create the handle
    /// during startup even when no diagnostic event will ever be emitted.
    pub fn new(config: RecorderConfig) -> Self {
        Self::new_with_live_config(config, None)
    }

    /// Configure an optional pinned file, read by the existing writer only
    /// after the recorder has been lazily started by its first record.
    pub fn new_with_live_config(
        config: RecorderConfig,
        live_config: Option<LiveConfigSource>,
    ) -> Self {
        Self {
            state: Mutex::new(State {
                config,
                live_config,
                recorder: None,
                start_attempted: false,
                closed: false,
                startup_failures: 0,
                startup_dropped_records: 0,
                startup_dropped_bytes: 0,
                callback_errors: 0,
                post_shutdown_dropped_records: 0,
                post_shutdown_dropped_bytes: 0,
                last_error_code: None,
                final_stats: None,
            }),
        }
    }

    /// Create a closed recorder state after optional host config validation fails.
    /// It emits a fixed safe code and counts later offers as dropped records.
    pub fn disabled_for_invalid_config(config: RecorderConfig) -> Self {
        Self {
            state: Mutex::new(State {
                config,
                live_config: None,
                recorder: None,
                start_attempted: true,
                closed: false,
                startup_failures: 1,
                startup_dropped_records: 0,
                startup_dropped_bytes: 0,
                callback_errors: 0,
                post_shutdown_dropped_records: 0,
                post_shutdown_dropped_bytes: 0,
                last_error_code: Some("OBSERVER_CONFIG_INVALID"),
                final_stats: None,
            }),
        }
    }
    /// Offer one already-serialized telemetry line. The only potentially
    /// synchronous start work occurs on the dedicated telemetry writer thread;
    /// all subsequent file writes use the recorder's nonblocking bounded
    /// record-and-byte queue.
    pub fn observe_line(&self, line: &[u8]) -> bool {
        self.observe_line_with_manager_policy(line, None)
    }

    pub fn observe_line_with_manager_policy(
        &self,
        line: &[u8],
        manager_policy: Option<swarm_telemetry::ScopedPolicyOverride>,
    ) -> bool {
        let mut state = lock_recover(&self.state);
        if state.closed {
            state.post_shutdown_dropped_records =
                state.post_shutdown_dropped_records.saturating_add(1);
            state.post_shutdown_dropped_bytes = state
                .post_shutdown_dropped_bytes
                .saturating_add(line.len() as u64);
            return false;
        }

        if state.recorder.is_none() && !state.start_attempted {
            state.start_attempted = true;
            match Recorder::start_with_live_config(state.config.clone(), state.live_config.clone())
            {
                Ok(recorder) => state.recorder = Some(recorder),
                Err(error) => {
                    state.startup_failures = state.startup_failures.saturating_add(1);
                    state.startup_dropped_records = state.startup_dropped_records.saturating_add(1);
                    state.startup_dropped_bytes = state
                        .startup_dropped_bytes
                        .saturating_add(line.len() as u64);
                    state.last_error_code = Some(safe_error_code(&error.code));
                    return false;
                }
            }
        }

        let Some(recorder) = state.recorder.as_ref() else {
            state.startup_dropped_records = state.startup_dropped_records.saturating_add(1);
            state.startup_dropped_bytes = state
                .startup_dropped_bytes
                .saturating_add(line.len() as u64);
            return false;
        };
        match recorder.append_line_with_manager_policy(line, manager_policy) {
            Ok(()) => true,
            Err(error) => {
                state.callback_errors = state.callback_errors.saturating_add(1);
                state.last_error_code = Some(safe_error_code(&error.code));
                false
            }
        }
    }

    /// Gracefully drain the recorder's accepted queue and retain its final
    /// counters even when sync, retention, or worker shutdown reports failure.
    /// The caller must first drain `swarm-telemetry`, so no callback is still
    /// forwarding an admitted line.
    pub fn shutdown_with_status(&self) -> (HostRecorderStats, Option<&'static str>) {
        let mut state = lock_recover(&self.state);
        if state.final_stats.is_none() {
            state.closed = true;
            let (stats, result) = match state.recorder.take() {
                Some(recorder) => {
                    let (stats, result) = recorder.shutdown_with_timeout(Duration::from_secs(5));
                    if let Err(error) = &result {
                        state.last_error_code = Some(safe_error_code(&error.code));
                    }
                    (stats, result)
                }
                None => (RecorderStats::default(), Ok(())),
            };
            state.final_stats = Some(stats);
            if let Err(error) = result
                && state.last_error_code.is_none()
            {
                state.last_error_code = Some(safe_error_code(&error.code));
            }
        }
        let recorder = recorder_stats(&state);
        let result_code = state.last_error_code;
        (snapshot(&state, recorder), result_code)
    }

    pub fn stats(&self) -> HostRecorderStats {
        let state = lock_recover(&self.state);
        snapshot(&state, recorder_stats(&state))
    }

    /// Read counters without waiting for a callback that may be blocked in
    /// lazy filesystem setup. Host failure shutdown uses this after its
    /// producer-drain deadline and reports missing stats as unavailable.
    pub fn try_stats(&self) -> Option<HostRecorderStats> {
        let state = match self.state.try_lock() {
            Ok(state) => state,
            Err(std::sync::TryLockError::Poisoned(error)) => error.into_inner(),
            Err(std::sync::TryLockError::WouldBlock) => return None,
        };
        Some(snapshot(&state, recorder_stats(&state)))
    }
}

fn recorder_stats(state: &State) -> RecorderStats {
    state.final_stats.unwrap_or_else(|| {
        state
            .recorder
            .as_ref()
            .map_or_else(RecorderStats::default, Recorder::stats)
    })
}

fn snapshot(state: &State, recorder: RecorderStats) -> HostRecorderStats {
    HostRecorderStats {
        enabled: true,
        started: state.start_attempted && state.startup_failures == 0,
        startup_failures: state.startup_failures,
        startup_dropped_records: state.startup_dropped_records,
        startup_dropped_bytes: state.startup_dropped_bytes,
        callback_errors: state.callback_errors,
        post_shutdown_dropped_records: state.post_shutdown_dropped_records,
        post_shutdown_dropped_bytes: state.post_shutdown_dropped_bytes,
        recorder,
    }
}

fn lock_recover<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn safe_error_code(code: &str) -> &'static str {
    match code {
        "OBSERVER_CONFIG_INVALID" => "OBSERVER_CONFIG_INVALID",
        "OBSERVER_PATH_UNAVAILABLE" => "OBSERVER_PATH_UNAVAILABLE",
        "OBSERVER_PATH_INVALID" => "OBSERVER_PATH_INVALID",
        "OBSERVER_PERMISSION_FAILED" => "OBSERVER_PERMISSION_FAILED",
        "OBSERVER_START_FAILED" => "OBSERVER_START_FAILED",
        "OBSERVER_RECORD_TOO_LARGE" => "OBSERVER_RECORD_TOO_LARGE",
        "OBSERVER_RECORD_INVALID" => "OBSERVER_RECORD_INVALID",
        "OBSERVER_SCHEMA_UNSUPPORTED" => "OBSERVER_SCHEMA_UNSUPPORTED",
        "OBSERVER_QUEUE_FULL" => "OBSERVER_QUEUE_FULL",
        "OBSERVER_UNAVAILABLE" => "OBSERVER_UNAVAILABLE",
        "OBSERVER_SHUTDOWN_FAILED" => "OBSERVER_SHUTDOWN_FAILED",
        "OBSERVER_SHUTDOWN_TIMEOUT" => "OBSERVER_SHUTDOWN_TIMEOUT",
        "OBSERVER_LIVE_CONFIG_UNAVAILABLE" => "OBSERVER_LIVE_CONFIG_UNAVAILABLE",
        "OBSERVER_LIVE_CONFIG_SCOPE_MISMATCH" => "OBSERVER_LIVE_CONFIG_SCOPE_MISMATCH",
        "OBSERVER_LIVE_CONFIG_VERSION_REJECTED" => "OBSERVER_LIVE_CONFIG_VERSION_REJECTED",
        "OBSERVER_LIVE_CONFIG_INVALID" => "OBSERVER_LIVE_CONFIG_INVALID",
        _ => "OBSERVER_RECORDING_FAILED",
    }
}
