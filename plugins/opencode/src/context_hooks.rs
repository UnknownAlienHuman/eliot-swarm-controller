//! Owned-generation longpoll for native context hooks.
//!
//! One task per bound OpenCode session: it `next`-polls the vendor-native
//! producer over the existing authenticated HTTP transport and forwards
//! each offered hook to the event task through a bounded channel. The
//! task carries no writer and never retries an ambiguous resolve: it only
//! reads. The event task owns Store claims, vendor resolves and durable
//! receipts; aborting this task at stop/drop ends the poll cleanly.

use std::time::Duration;

use eliot_plugin::retry::{Retry, RetryFact, RetryOperation};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};

use crate::native::{client::OpenCodeHttp, types::EliotContextHook};

/// One `next` wait. The producer clamps the wait (120s max) and admits a
/// single waiter; 30s keeps abort responsive without busy-polling.
pub(crate) const CONTEXT_NEXT_WAIT_MS: i64 = 30_000;

/// One offered context hook forwarded from the poll task to the event task.
pub(crate) struct ContextHookSignal {
    pub hook: EliotContextHook,
}

/// Spawn the owned-generation poll task. The caller aborts it at stop/drop;
/// dropping the returned handle aborts the task.
pub(crate) fn spawn_context_poll(
    http: OpenCodeHttp,
    session_id: String,
    tx: mpsc::Sender<crate::events::ContextHookMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut outstanding: Option<String> = None;
        let mut offer_retry = Retry::new(RetryOperation::OutstandingOffer);
        let mut poll_retry = Retry::new(RetryOperation::HookPoll);
        loop {
            let deadline =
                Instant::now() + Duration::from_millis(CONTEXT_NEXT_WAIT_MS as u64 + 10_000);
            match http
                .eliot_context_next(&session_id, CONTEXT_NEXT_WAIT_MS, deadline)
                .await
            {
                Ok(output) => {
                    poll_retry = Retry::new(RetryOperation::HookPoll);
                    let Some(hook) = output.hook else {
                        // No pending offer: the hook resolved, expired, or
                        // was never offered. The next poll re-engages the
                        // native long-poll wait, so no sleep is needed here.
                        outstanding = None;
                        offer_retry = Retry::new(RetryOperation::OutstandingOffer);
                        continue;
                    };
                    if hook.session_id != session_id {
                        // A foreign offer never binds: drop it; the next
                        // poll re-reads the bound session's queue.
                        continue;
                    }
                    let identical = outstanding.as_deref() == Some(hook.hook_id.as_str());
                    // Keep the current offer until the event loop can accept it.
                    if tx
                        .send(crate::events::ContextHookMsg::Arrived(ContextHookSignal {
                            hook: hook.clone(),
                        }))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    if identical {
                        // The event task deduplicates repeat offers, but the
                        // repeats still cost an HTTP round trip and queue
                        // traffic each. Back off while the same hook stays
                        // outstanding; a new hook or an empty poll resets.
                        if !offer_retry.wait(RetryFact::Idempotent, None).await {
                            return;
                        }
                    } else {
                        outstanding = Some(hook.hook_id.clone());
                        offer_retry = Retry::new(RetryOperation::OutstandingOffer);
                    }
                }
                Err(_) => {
                    // A poll carries no native effect: back off and re-read.
                    // The event task is gone when the channel closes.
                    if tx.is_closed() {
                        return;
                    }
                    if !poll_retry.wait(RetryFact::Idempotent, None).await {
                        return;
                    }
                }
            }
        }
    })
}

/// True when a `bind` output proves version-1 binding of exactly this
/// session. Only then may readiness advertise `turn_safe_point`.
pub(crate) fn bind_verified(
    output: &crate::native::types::EliotContextBindOutput,
    session_id: &str,
) -> bool {
    output.version == 1 && output.bound_session_id == session_id
}
