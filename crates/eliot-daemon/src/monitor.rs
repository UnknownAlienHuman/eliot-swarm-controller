//! OS facts and ephemeral counters, independent of the native lifecycle source.
use crate::{bus::Bus, platform::CustodyObserver};
use eliot_core::{Agent, Error, ProcessRecord, ProcessSnapshot, ProcessState, Result};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex as AsyncMutex, watch},
    time::{MissedTickBehavior, interval},
};

const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
const SAMPLE_TTL_MS: i64 = 4_000;
const PAGE_LIMIT: usize = 1_000;

type Key = (String, i64);

#[derive(Clone)]
pub(crate) struct Monitoring {
    inner: Arc<State>,
}

struct State {
    store: Arc<AsyncMutex<eliot_store::Store>>,
    bus: Bus,
    targets: Mutex<HashMap<Key, Arc<Target>>>,
    unavailable: Mutex<Option<String>>,
}

struct Target {
    agent: String,
    generation: i64,
    owner: String,
    observer: CustodyObserver,
    sampling: AsyncMutex<()>,
    cache: Mutex<Cache>,
}

#[derive(Clone, Default)]
struct Cache {
    snapshot: Option<ProcessSnapshot>,
    sampled_at: Option<i64>,
    error: Option<String>,
}

impl Monitoring {
    pub(crate) fn new(store: Arc<AsyncMutex<eliot_store::Store>>, bus: Bus) -> Self {
        Self {
            inner: Arc::new(State {
                store,
                bus,
                targets: Mutex::new(HashMap::new()),
                unavailable: Mutex::new(None),
            }),
        }
    }

    pub(crate) fn mark_unavailable(&self, code: &str) {
        *self
            .inner
            .unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(code.to_owned());
    }

    pub(crate) async fn prepare_generation(&self, agent: &Agent) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = store.ensure_process_source(&agent.id, agent.generation, now_ms())?;
        self.inner.bus.publish(&events);
        Ok(())
    }

    pub(crate) async fn attach(&self, agent: &Agent, observer: CustodyObserver) -> Result<()> {
        let key = (agent.id.clone(), agent.generation);
        let target = Arc::new(Target {
            agent: agent.id.clone(),
            generation: agent.generation,
            owner: owner(&agent.id, agent.generation),
            observer,
            sampling: AsyncMutex::new(()),
            cache: Mutex::new(Cache::default()),
        });
        {
            let mut targets = self
                .inner
                .targets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if targets.contains_key(&key) {
                return Err(Error::new(
                    "bad_state",
                    "process observation is already attached",
                ));
            }
            targets.insert(key, Arc::clone(&target));
        }
        // A failed read-only probe leaves unknown observation; it does not fail native startup.
        let _ = self.sample_target(&target).await;
        Ok(())
    }

    pub(crate) async fn process_put(
        &self,
        agent: &str,
        generation: i64,
        pid: u32,
        role: &str,
    ) -> Result<Value> {
        if pid == 0 || role != "harness" {
            return Err(Error::new(
                "invalid",
                "process.put requires a nonzero harness PID",
            ));
        }
        let target = self.target(agent, generation).ok_or_else(|| {
            Error::new(
                "stale_generation",
                "process observation is not attached for this generation",
            )
        })?;
        let _sampling = target.sampling.lock().await;
        let observer = target.observer.clone();
        let process_owner = target.owner.clone();
        let sampled_at = now_ms();
        let sample = tokio::task::spawn_blocking(move || {
            observer.register_pid(pid, "harness", &process_owner, sampled_at)
        })
        .await
        .map_err(|_| Error::new("process_observation", "process verification task failed"))??;
        if sample.record.owner != target.owner
            || sample.record.identity.pid != pid
            || sample.record.identity.host_boot_id.is_empty()
            || sample.record.identity.start_time == 0
            || sample.record.role != "harness"
            || sample.record.state != ProcessState::Running
            || sample.record.exit_code.is_some()
        {
            return Err(Error::new(
                "not_allowed",
                "OS observation did not verify a live harness process",
            ));
        }
        {
            let mut store = self.inner.store.lock().await;
            let current = store
                .get_agent(agent)?
                .ok_or_else(|| Error::new("not_found", "process.put Agent was not found"))?;
            if current.generation != generation
                || current.is_terminal()
                || store.get_agent_token(agent, generation)?.is_none()
            {
                return Err(Error::new(
                    "stale_generation",
                    "process.put generation is no longer controlled",
                ));
            }
            let events = store.observe_processes(
                agent,
                generation,
                std::slice::from_ref(&sample.record),
                now_ms(),
            )?;
            self.inner.bus.publish(&events);
        }
        serde_json::to_value(sample.record)
            .map_err(|_| Error::new("internal", "process acknowledgement could not be encoded"))
    }

    /// Sample an exiting root before its native stdout is drained. Only the OS
    /// observer determines process facts; failed probes never synthesize an exit.
    pub(crate) async fn sample_generation(&self, agent: &Agent) -> Result<()> {
        match self.target(&agent.id, agent.generation) {
            Some(target) => self.sample_target(&target).await,
            None => Ok(()),
        }
    }

    pub(crate) async fn view(&self, agent: &Agent) -> Result<Value> {
        let cache = self
            .target(&agent.id, agent.generation)
            .map(|target| {
                target
                    .cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            })
            .unwrap_or_default();
        let unavailable = self
            .inner
            .unavailable
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let (processes, last_model_progress, source_healthy) = {
            let store = self.inner.store.lock().await;
            let processes =
                store.list_processes(&owner(&agent.id, agent.generation), None, PAGE_LIMIT)?;
            let progress = store.last_model_progress(&agent.id, agent.generation)?;
            let healthy = store
                .read_source_state(&agent.id, agent.generation, "stream")
                .ok()
                .map(|state| {
                    state.get("state").and_then(Value::as_str) == Some("open")
                        && state.get("health").and_then(Value::as_str) == Some("ok")
                });
            (processes, progress, healthy)
        };
        let now = now_ms();
        let sample_age_ms = cache.sampled_at.and_then(|at| age_ms(now, at));
        let sample_clock_regressed = cache.sampled_at.is_some_and(|at| at > now);
        let model_clock_regressed = last_model_progress.is_some_and(|at| at > now);
        let stale = unavailable.is_some()
            || cache.error.is_some()
            || sample_clock_regressed
            || sample_age_ms.is_some_and(|age| age > SAMPLE_TTL_MS);
        let available = cache.snapshot.is_some() && !stale;
        let coverage = match cache.snapshot.as_ref() {
            None => "unknown",
            Some(_) if stale => "stale",
            Some(snapshot) if snapshot.complete => "complete",
            Some(_) => "partial",
        };
        let samples = cache
            .snapshot
            .as_ref()
            .filter(|_| available)
            .map(|snapshot| &snapshot.samples);
        let total_samples = samples.filter(|_| cache.snapshot.as_ref().is_some_and(|s| s.complete));
        let cpu_time_ns = total_samples.and_then(|samples| {
            if samples.is_empty() {
                return None;
            }
            samples.iter().try_fold(0_u64, |total, sample| {
                total.checked_add(sample.cpu_time_ns?)
            })
        });
        let rss_bytes = total_samples.and_then(|samples| {
            if samples.is_empty() {
                return None;
            }
            samples
                .iter()
                .try_fold(0_u64, |total, sample| total.checked_add(sample.rss_bytes?))
        });
        Ok(json!({
            "processes_truncated": processes.len() == PAGE_LIMIT,
            "processes": processes,
            "stats": {
                "available": available,
                "sampled_at": cache.sampled_at,
                "age_ms": sample_age_ms,
                "ttl_ms": SAMPLE_TTL_MS,
                "clock_regressed": sample_clock_regressed,
                "coverage": coverage,
                "complete": cache.snapshot.as_ref().map(|snapshot| snapshot.complete),
                "cpu_time_ns": cpu_time_ns,
                "rss_bytes": rss_bytes,
                "samples": samples,
                "error": unavailable.or(cache.error),
            },
            "liveness": {
                "state": agent.state,
                "last_model_progress": last_model_progress,
                "age_ms": last_model_progress.and_then(|at| age_ms(now, at)),
                "clock_regressed": model_clock_regressed,
                "basis": "committed_native_facts",
                "source_healthy": source_healthy,
            },
        }))
    }

    pub(crate) async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut ticks = interval(SAMPLE_INTERVAL);
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = ticks.tick() => self.sample_all().await,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
            }
        }
        self.sample_all().await;
        Ok(())
    }

    fn target(&self, agent: &str, generation: i64) -> Option<Arc<Target>> {
        self.inner
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(agent.to_owned(), generation))
            .cloned()
    }

    async fn sample_all(&self) {
        let targets: Vec<_> = self
            .inner
            .targets
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect();
        for target in targets {
            let _ = self.sample_target(&target).await;
        }
    }

    async fn sample_target(&self, target: &Arc<Target>) -> Result<()> {
        let _sampling = target.sampling.lock().await;
        let observer = target.observer.clone();
        let process_owner = target.owner.clone();
        let sampled_at = now_ms();
        let result =
            tokio::task::spawn_blocking(move || observer.snapshot(&process_owner, sampled_at))
                .await
                .map_err(|_| Error::new("process_observation", "process sampling task failed"))
                .and_then(|result| result);
        let snapshot = match result {
            Ok(snapshot) => snapshot,
            Err(error) => {
                target
                    .cache
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .error = Some(error.code.clone());
                let _ = self.set_source_status(target, "error", false).await;
                return Err(error);
            }
        };
        let records: Vec<ProcessRecord> = snapshot
            .samples
            .iter()
            .map(|sample| sample.record.clone())
            .collect();
        let final_exit = snapshot.complete
            && snapshot
                .samples
                .iter()
                .all(|sample| sample.record.state == ProcessState::Exited);
        let observed = {
            let mut store = self.inner.store.lock().await;
            let mut result = Ok(());
            for page in records.chunks(PAGE_LIMIT) {
                match store.observe_processes(&target.agent, target.generation, page, now_ms()) {
                    Ok(events) => self.inner.bus.publish(&events),
                    Err(error) => {
                        result = Err(error);
                        break;
                    }
                }
            }
            result
        };
        if let Err(error) = observed {
            target
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .error = Some(error.code.clone());
            let _ = self.set_source_status(target, "error", false).await;
            return Err(error);
        }
        if let Err(error) = self.set_source_status(target, "ok", final_exit).await {
            target
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .error = Some(error.code.clone());
            return Err(error);
        }
        *target
            .cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Cache {
            snapshot: Some(snapshot),
            sampled_at: Some(sampled_at),
            error: None,
        };
        if final_exit {
            let key = (target.agent.clone(), target.generation);
            let mut targets = self
                .inner
                .targets
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if targets
                .get(&key)
                .is_some_and(|current| Arc::ptr_eq(current, target))
            {
                targets.remove(&key);
            }
        }
        Ok(())
    }

    async fn set_source_status(&self, target: &Target, health: &str, closed: bool) -> Result<()> {
        let mut store = self.inner.store.lock().await;
        let events = store.set_process_source_status(
            &target.agent,
            target.generation,
            health,
            closed,
            now_ms(),
        )?;
        self.inner.bus.publish(&events);
        Ok(())
    }
}

fn age_ms(now: i64, timestamp: i64) -> Option<i64> {
    now.checked_sub(timestamp).filter(|age| *age >= 0)
}

fn owner(agent: &str, generation: i64) -> String {
    format!("agent:{agent}#{generation}")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
