//! In-memory RPC observations and request-local Store acquisition timing.
use eliot_core::api::{
    RPC_LATENCY_SAMPLES_PER_METHOD, RPC_LATENCY_SLOW_CALLS, RPC_LATENCY_WINDOW_SECONDS,
    RpcLatencyPercentiles, RpcLatencyReport, RpcMethodLatency, SlowRpcCall,
};
use eliot_plugin::sync::LockExt;
use eliot_store::Store;
use std::{
    cell::Cell,
    collections::{BTreeMap, VecDeque},
    future::Future,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, MutexGuard, TryLockError};

const WINDOW: Duration = Duration::from_secs(RPC_LATENCY_WINDOW_SECONDS);

tokio::task_local! {
    static STORE_WAIT: Cell<Duration>;
}

pub(crate) async fn measure_store_wait<F: Future>(work: F) -> (F::Output, Duration) {
    STORE_WAIT
        .scope(Cell::new(Duration::ZERO), async {
            let result = work.await;
            (result, STORE_WAIT.with(Cell::get))
        })
        .await
}

pub(crate) type StoreHandle = Arc<StoreLock>;

pub(crate) struct StoreLock(AsyncMutex<Store>);

impl StoreLock {
    pub(crate) fn new(store: Store) -> Self {
        Self(AsyncMutex::new(store))
    }

    pub(crate) async fn lock(&self) -> MutexGuard<'_, Store> {
        // Dropping a timed-out acquisition counts the wait it actually spent.
        let _wait = STORE_WAIT.try_with(|_| StoreWait(Instant::now())).ok();
        self.0.lock().await
    }

    pub(crate) fn try_lock(&self) -> Result<MutexGuard<'_, Store>, TryLockError> {
        self.0.try_lock()
    }
}

struct StoreWait(Instant);

impl Drop for StoreWait {
    fn drop(&mut self) {
        let _ = STORE_WAIT.try_with(|wait| wait.set(wait.get().saturating_add(self.0.elapsed())));
    }
}

#[derive(Clone, Copy)]
struct Sample {
    finished: Instant,
    duration_us: u64,
    store_wait_us: u64,
}

#[derive(Default)]
pub(crate) struct RpcLatencies {
    // The only keys are static names from the daemon's method manifest.
    methods: Mutex<BTreeMap<&'static str, VecDeque<Sample>>>,
}

impl RpcLatencies {
    pub(crate) fn record(&self, method: &'static str, duration: Duration, store_wait: Duration) {
        let mut methods = self.methods.lock_recover();
        let sample = Sample {
            finished: Instant::now(),
            duration_us: micros(duration),
            store_wait_us: micros(store_wait),
        };
        let window = methods.entry(method).or_default();
        while window.front().is_some_and(|old| {
            sample.finished.saturating_duration_since(old.finished) > WINDOW
                || window.len() == RPC_LATENCY_SAMPLES_PER_METHOD
        }) {
            window.pop_front();
        }
        window.push_back(sample);
    }

    pub(crate) fn report(&self) -> RpcLatencyReport {
        let now = Instant::now();
        // Release the shared lock before percentile calculation and ordering.
        let samples: Vec<_> = self
            .methods
            .lock_recover()
            .iter()
            .map(|(&method, window)| {
                (
                    method,
                    window
                        .iter()
                        .copied()
                        .filter(|sample| now.saturating_duration_since(sample.finished) <= WINDOW)
                        .collect::<Vec<_>>(),
                )
            })
            .filter(|(_, samples)| !samples.is_empty())
            .collect();
        let mut methods = Vec::with_capacity(samples.len());
        let mut slowest = Vec::new();
        for (method, samples) in samples {
            let mut duration: Vec<_> = samples.iter().map(|sample| sample.duration_us).collect();
            let mut wait: Vec<_> = samples.iter().map(|sample| sample.store_wait_us).collect();
            duration.sort_unstable();
            wait.sort_unstable();
            methods.push(RpcMethodLatency {
                method: method.to_owned(),
                count: samples.len(),
                duration_us: percentiles(&duration),
                store_wait_us: percentiles(&wait),
            });
            slowest.extend(
                duration
                    .into_iter()
                    .rev()
                    .take(RPC_LATENCY_SLOW_CALLS)
                    .map(|duration_us| (method, duration_us)),
            );
        }
        slowest.sort_unstable_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(right.0)));
        RpcLatencyReport {
            window_seconds: RPC_LATENCY_WINDOW_SECONDS,
            samples_per_method: RPC_LATENCY_SAMPLES_PER_METHOD,
            methods,
            slowest: slowest
                .into_iter()
                .take(RPC_LATENCY_SLOW_CALLS)
                .map(|(method, duration_us)| SlowRpcCall {
                    method: method.to_owned(),
                    duration_us,
                })
                .collect(),
        }
    }
}

fn percentiles(sorted: &[u64]) -> RpcLatencyPercentiles {
    let rank = |percent: usize| (percent * sorted.len()).div_ceil(100) - 1;
    RpcLatencyPercentiles {
        p50: sorted[rank(50)],
        p95: sorted[rank(95)],
        max: sorted[sorted.len() - 1],
    }
}

fn micros(duration: Duration) -> u64 {
    duration.as_micros().try_into().unwrap_or(u64::MAX)
}
