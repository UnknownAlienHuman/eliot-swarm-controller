use std::{ops::ControlFlow, time::Duration};

use tokio::time::Instant;

/// Wait for an absolute deadline, or remain pending when no deadline exists.
///
/// The returned future is cancellation-safe: dropping it does not change the
/// deadline, so a caller can recreate it in a `select!` loop without extending
/// the wait.
pub async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

/// The adapter supplies native evidence. An unresolved effect alone never
/// authorizes another attempt.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum RetryFact {
    BeforeEffect,
    Idempotent,
    Unknown,
}

#[derive(Clone, Copy)]
pub enum RetryOperation {
    NativeStartup,
    SessionBinding,
    OutstandingOffer,
    HookPoll,
    ReadinessPoll,
    /// Settle a polled native readback that becomes observable
    /// asynchronously (installed MCP inventory): 50 ms doubling to 1 s.
    ReadinessSettle,
}

const READINESS_SETTLE_FIRST: Duration = Duration::from_millis(50);
const READINESS_SETTLE_MAX: Duration = Duration::from_secs(1);
const READINESS_SETTLE_BUDGET: Duration = Duration::from_secs(20);
pub const NATIVE_READINESS_BUDGET: Duration = Duration::from_secs(60);

/// Poll a native read or an explicitly idempotent bind. A break carries
/// the translated observation (including a definite refusal, if any).
pub async fn poll_idempotent<T>(
    operation: RetryOperation,
    deadline: Instant,
    mut observe: impl AsyncFnMut() -> ControlFlow<T>,
) -> Option<T> {
    let mut retry = Retry::new(operation);
    loop {
        if let ControlFlow::Break(value) = observe().await {
            return Some(value);
        }
        if !retry.wait(RetryFact::Idempotent, Some(deadline)).await {
            return None;
        }
    }
}

pub enum Readiness<T> {
    Pending(T),
    Settled(T),
    Unavailable,
}

/// Preserve the last successful pending observation if no terminal
/// observation is available, including on the final read at the cap.
pub async fn settle_readiness<T>(
    deadline: Instant,
    mut observe: impl AsyncFnMut(Instant) -> Readiness<T>,
) -> Option<T> {
    let cap = (Instant::now() + READINESS_SETTLE_BUDGET).min(deadline);
    let mut retry = Retry::new(RetryOperation::ReadinessSettle);
    let mut last = None;
    loop {
        match observe(cap).await {
            Readiness::Settled(value) => return Some(value),
            Readiness::Pending(value) => last = Some(value),
            Readiness::Unavailable => {}
        }
        if Instant::now() >= cap {
            return last;
        }
        retry.wait_settle(cap).await;
    }
}

fn readiness_settle_delay(retries: u32) -> Duration {
    READINESS_SETTLE_FIRST
        .saturating_mul(1 << retries.min(5))
        .min(READINESS_SETTLE_MAX)
}

enum StartupAfterFailure {
    Retry,
    Failed,
    Unresolved,
}

/// Evidence produced after the owned initialization attempt was reaped.
pub struct StartupFailureFact {
    pub initialize_request_failed: bool,
    pub initialize_response_received: bool,
    pub natural_failure_exit_observed: bool,
    pub output_complete: bool,
    pub unexpected_native_frame: bool,
    pub local_state_initialization_failed: bool,
}

pub struct StartupFailure {
    pub fact: StartupFailureFact,
    pub error: eliot_core::Error,
    pub pre_effect_error: eliot_core::Error,
}

pub enum StartupAttempt<T> {
    Ready(T),
    InitializationFailed(Box<StartupFailure>),
}

/// Retry one owned, read-only initialization before it issues any native
/// session or thread request. Later failures go straight to the caller.
pub async fn start_read_only_native<T, Attempt>(
    deadline: Instant,
    pre_spawn_refusal: impl Fn() -> eliot_core::Error,
    mut attempt: impl FnMut() -> Attempt,
) -> eliot_core::Result<T>
where
    Attempt: Future<Output = eliot_core::Result<StartupAttempt<T>>> + Send,
{
    let mut retry = Retry::new(RetryOperation::NativeStartup);
    loop {
        if Instant::now() >= deadline {
            return Err(pre_spawn_refusal());
        }
        let failure = match attempt().await? {
            StartupAttempt::Ready(ready) => return Ok(ready),
            StartupAttempt::InitializationFailed(failure) => failure,
        };
        let fact = &failure.fact;
        let no_effect = fact.initialize_request_failed
            && !fact.initialize_response_received
            && fact.output_complete
            && !fact.unexpected_native_frame;
        let eligible = no_effect
            && fact.natural_failure_exit_observed
            && fact.local_state_initialization_failed;
        match retry
            .after_start_failure(eligible, no_effect, deadline)
            .await
        {
            StartupAfterFailure::Retry => {}
            StartupAfterFailure::Failed => return Err(failure.pre_effect_error),
            StartupAfterFailure::Unresolved => return Err(failure.error),
        }
    }
}

pub struct Retry {
    operation: RetryOperation,
    retries: u32,
}

impl Retry {
    pub fn new(operation: RetryOperation) -> Self {
        Self {
            operation,
            retries: 0,
        }
    }

    fn exhausted(&self) -> bool {
        match self.operation {
            RetryOperation::NativeStartup => self.retries >= 1,
            RetryOperation::SessionBinding => self.retries >= 2,
            RetryOperation::OutstandingOffer
            | RetryOperation::HookPoll
            | RetryOperation::ReadinessPoll
            | RetryOperation::ReadinessSettle => false,
        }
    }

    /// Decide how to finish a failed initialization from native proof. If
    /// complete pre-effect evidence allowed a retry but the deadline cannot
    /// fit it, the attempted startup still failed before creating a session.
    async fn after_start_failure(
        &mut self,
        eligible: bool,
        proved_no_effect: bool,
        deadline: Instant,
    ) -> StartupAfterFailure {
        let fact = if eligible {
            RetryFact::BeforeEffect
        } else {
            RetryFact::Unknown
        };
        if self.wait(fact, Some(deadline)).await {
            StartupAfterFailure::Retry
        } else if proved_no_effect && (eligible || self.exhausted()) {
            StartupAfterFailure::Failed
        } else {
            StartupAfterFailure::Unresolved
        }
    }

    pub async fn wait(&mut self, fact: RetryFact, deadline: Option<Instant>) -> bool {
        if self.exhausted() || fact == RetryFact::Unknown {
            return false;
        }
        let delay = match self.operation {
            RetryOperation::NativeStartup if fact == RetryFact::BeforeEffect => {
                let random_byte = ulid::Ulid::new().to_bytes()[15];
                Duration::from_millis(25 + u64::from(random_byte) % 76)
            }
            RetryOperation::NativeStartup => return false,
            RetryOperation::SessionBinding => Duration::from_millis(100),
            RetryOperation::OutstandingOffer => {
                Duration::from_millis(250 * (1 << self.retries.min(3)))
            }
            RetryOperation::HookPoll | RetryOperation::ReadinessPoll => Duration::from_millis(250),
            RetryOperation::ReadinessSettle => readiness_settle_delay(self.retries),
        };
        let wake = Instant::now() + delay;
        if deadline.is_some_and(|end| wake >= end) {
            return false;
        }
        wait_until(Some(wake)).await;
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return false;
        }
        self.retries = self.retries.saturating_add(1);
        true
    }

    /// Wait one readiness-settle interval, clamped to `deadline` so a polled
    /// readback never runs past its budget. The caller re-reads after every
    /// wait, including the wait that lands exactly on the deadline, because
    /// the observation at the settle cap decides the result.
    async fn wait_settle(&mut self, deadline: Instant) {
        debug_assert!(matches!(self.operation, RetryOperation::ReadinessSettle));
        let delay = readiness_settle_delay(self.retries);
        wait_until(Some((Instant::now() + delay).min(deadline))).await;
        self.retries = self.retries.saturating_add(1);
    }
}
