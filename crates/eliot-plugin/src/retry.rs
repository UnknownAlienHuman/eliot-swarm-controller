use std::time::Duration;

use tokio::time::{Instant, sleep_until};

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
}

pub enum StartupAfterFailure {
    Retry,
    Failed,
    Unresolved,
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
            | RetryOperation::ReadinessPoll => false,
        }
    }

    /// Decide how to finish a failed initialization from native proof. If
    /// complete pre-effect evidence allowed a retry but the deadline cannot
    /// fit it, the attempted startup still failed before creating a session.
    pub async fn after_start_failure(
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
        };
        let wake = Instant::now() + delay;
        if deadline.is_some_and(|end| wake >= end) {
            return false;
        }
        sleep_until(wake).await;
        if deadline.is_some_and(|end| Instant::now() >= end) {
            return false;
        }
        self.retries = self.retries.saturating_add(1);
        true
    }
}
