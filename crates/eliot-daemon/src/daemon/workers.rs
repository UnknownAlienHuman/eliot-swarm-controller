use crate::{logging, router::Router};
use eliot_core::{Error, Result, SecretPolicy, api::DaemonStatusResponse};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{sync::watch, task::JoinSet, time::Instant};

pub(crate) const WORKER_NAMES: [&str; DaemonStatusResponse::MAX_AUTOMATION_TASKS] = [
    "rule_worker",
    "goal_driver",
    "project_checks",
    "maintenance",
    "monitor",
    "git_sampler",
    "attention",
    "tool_failure_attention",
    "mail_reoffer",
    "agent_wake",
    "completion_digest",
];

const INITIAL_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
const HEALTHY_RUN_RESET: Duration = Duration::from_secs(60);
const WORKER_FAILURE_LIMIT: u32 = 5;

pub(crate) enum WorkerOutcome {
    Stopped,
    Failed(Error),
}

#[derive(Clone)]
pub(crate) struct WorkerReporter {
    router: Router,
    policy: Arc<SecretPolicy>,
}

impl WorkerReporter {
    pub(crate) fn new(router: Router, policy: Arc<SecretPolicy>) -> Self {
        Self { router, policy }
    }

    pub(crate) fn starting(&self, name: &str) {
        self.router.note_worker_task_starting(name);
    }

    pub(crate) fn running(&self, name: &str) {
        self.router.note_worker_task_running(name);
    }

    pub(crate) fn stopped(&self, name: &str) {
        self.router.note_worker_task_stopped(name);
    }

    pub(crate) fn failed(
        &self,
        name: &str,
        error: &Error,
        restart_count: u32,
        retry_after: Option<Duration>,
    ) {
        let code = bounded_failure_code(&error.code);
        self.router
            .note_worker_task_failed(name, code, restart_count);
        let restart_count = restart_count.to_string();
        let retry_ms = retry_after.map(|delay| delay.as_millis().to_string());
        let mut fields = vec![("worker", name), ("restart_count", restart_count.as_str())];
        if let Some(retry_ms) = retry_ms.as_deref() {
            fields.push(("retry_after_ms", retry_ms));
        }
        logging::record(
            &self.policy,
            if retry_after.is_some() {
                "daemon.worker_restart_scheduled"
            } else {
                "daemon.worker_failed"
            },
            &fields,
            Some(error),
        );
    }

    async fn attention(&self, name: &str, failures: u32, error: &Error) {
        let code = bounded_failure_code(&error.code);
        let attempt = failures.to_string();
        let fields = [("worker", name), ("attempt", attempt.as_str())];
        logging::record(
            &self.policy,
            "daemon.worker_attention",
            &fields,
            Some(error),
        );
        if let Err(error) = self.router.report_daemon_attention(format!(
            "The {name} worker stopped after {failures} consecutive failures ({code}). Check the daemon log before restarting Swarm."
        )).await {
            logging::record(&self.policy, "daemon.attention_delivery_failed", &fields, Some(&error));
        }
    }
}

pub(crate) async fn supervise<F, Fut>(
    name: &'static str,
    reporter: WorkerReporter,
    shutdown: watch::Receiver<bool>,
    run: F,
) where
    F: FnMut(watch::Receiver<bool>) -> Fut + Send + 'static,
    Fut: Future<Output = WorkerOutcome> + Send + 'static,
{
    supervise_with(name, reporter, shutdown, run, |_| {}).await;
}

pub(crate) async fn supervise_with<F, Fut, H>(
    name: &'static str,
    reporter: WorkerReporter,
    mut shutdown: watch::Receiver<bool>,
    mut run: F,
    mut on_failure: H,
) where
    F: FnMut(watch::Receiver<bool>) -> Fut + Send + 'static,
    Fut: Future<Output = WorkerOutcome> + Send + 'static,
    H: FnMut(&Error) + Send + 'static,
{
    let mut restart_count = 0_u32;
    let mut consecutive_failures = 0_u32;
    loop {
        if is_stopping(&shutdown) {
            reporter.stopped(name);
            return;
        }

        reporter.starting(name);
        let started = Instant::now();
        let mut worker = JoinSet::new();
        worker.spawn(run(shutdown.clone()));
        let joined = loop {
            tokio::select! {
                joined = worker.join_next() => break joined,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break worker.join_next().await;
                    }
                }
            }
        };
        let outcome = match joined {
            Some(Ok(outcome)) => outcome,
            Some(Err(error)) => {
                let code = if error.is_panic() {
                    "worker_panicked"
                } else if error.is_cancelled() {
                    "worker_cancelled"
                } else {
                    "worker_task_failed"
                };
                WorkerOutcome::Failed(Error::new(code, "background worker task failed"))
            }
            None => WorkerOutcome::Failed(Error::new(
                "worker_task_missing",
                "background worker task ended without an outcome",
            )),
        };

        if is_stopping(&shutdown) {
            match outcome {
                WorkerOutcome::Stopped => reporter.stopped(name),
                WorkerOutcome::Failed(error) => {
                    on_failure(&error);
                    reporter.failed(name, &error, restart_count, None);
                }
            }
            return;
        }

        match outcome {
            WorkerOutcome::Stopped => {
                reporter.stopped(name);
                return;
            }
            WorkerOutcome::Failed(error) => {
                on_failure(&error);
                if started.elapsed() >= HEALTHY_RUN_RESET {
                    consecutive_failures = 0;
                }
                consecutive_failures = consecutive_failures.saturating_add(1);
                if consecutive_failures >= WORKER_FAILURE_LIMIT {
                    reporter.failed(name, &error, restart_count, None);
                    reporter.attention(name, consecutive_failures, &error).await;
                    return;
                }
                restart_count = restart_count.saturating_add(1);
                let delay = retry_backoff(consecutive_failures);
                reporter.failed(name, &error, restart_count, Some(delay));
                if !wait_for_retry(&mut shutdown, delay).await {
                    reporter.stopped(name);
                    return;
                }
            }
        }
    }
}

pub(crate) fn from_result(result: Result<()>, shutdown: &watch::Receiver<bool>) -> WorkerOutcome {
    match result {
        Err(error) => WorkerOutcome::Failed(error),
        Ok(()) if is_stopping(shutdown) => WorkerOutcome::Stopped,
        Ok(()) => WorkerOutcome::Failed(Error::new(
            "worker_exited",
            "background worker exited unexpectedly",
        )),
    }
}

pub(crate) fn retry_backoff(attempt: u32) -> Duration {
    let shift = attempt.saturating_sub(1).min(7);
    let multiplier = 1_u64 << shift;
    INITIAL_BACKOFF
        .saturating_mul(multiplier as u32)
        .min(MAX_BACKOFF)
}

fn is_stopping(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

async fn wait_for_retry(shutdown: &mut watch::Receiver<bool>, delay: Duration) -> bool {
    let sleep = tokio::time::sleep(delay);
    tokio::pin!(sleep);
    loop {
        tokio::select! {
            _ = &mut sleep => return !is_stopping(shutdown),
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return false;
                }
            }
        }
    }
}

fn bounded_failure_code(code: &str) -> String {
    if code.is_empty() || code.len() > 128 {
        "worker_failed".to_owned()
    } else {
        code.to_owned()
    }
}
