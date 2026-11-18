//! Owned-generation longpoll for native context hooks.
//!
//! One task per bound OpenCode session: it `next`-polls the vendor-native
//! producer over the existing authenticated HTTP transport and forwards
//! each offered hook to the event task through a bounded channel. The
//! task carries no writer and never retries an ambiguous resolve: it only
//! reads. The event task owns Store claims, vendor resolves and durable
//! receipts; aborting this task at stop/drop ends the poll cleanly.

use std::time::Duration;

use tokio::{sync::mpsc, task::JoinHandle, time::Instant};

use crate::native::{client::OpenCodeHttp, types::EliotContextHook};

/// One `next` wait. The producer clamps the wait (120s max) and admits a
/// single waiter; 30s keeps abort responsive without busy-polling.
pub(crate) const CONTEXT_NEXT_WAIT_MS: i64 = 30_000;
/// Backoff after a poll transport failure before retrying the read.
const NEXT_RETRY_BEAT: Duration = Duration::from_millis(250);
/// First backoff after re-observing the identical outstanding hook offer.
/// `next` answers a still-pending hook immediately, bypassing its
/// long-poll wait, so polling at once only repeats HTTP and queue traffic
/// while the first offer is claimed or resolved. Bounded well under the
/// native 10 s pending-hook deadline; the first offer is never delayed
/// (it forwards before any sleep).
const OFFER_RETRY_FIRST: Duration = Duration::from_millis(250);
/// Cap for the identical-offer backoff.
const OFFER_RETRY_MAX: Duration = Duration::from_secs(2);

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
        let mut repeats: u32 = 0;
        loop {
            let deadline =
                Instant::now() + Duration::from_millis(CONTEXT_NEXT_WAIT_MS as u64 + 10_000);
            match http
                .eliot_context_next(&session_id, CONTEXT_NEXT_WAIT_MS, deadline)
                .await
            {
                Ok(output) => {
                    let Some(hook) = output.hook else {
                        // No pending offer: the hook resolved, expired, or
                        // was never offered. The next poll re-engages the
                        // native long-poll wait, so no sleep is needed here.
                        outstanding = None;
                        repeats = 0;
                        continue;
                    };
                    if hook.session_id != session_id {
                        // A foreign offer never binds: drop it; the next
                        // poll re-reads the bound session's queue.
                        continue;
                    }
                    let identical = outstanding.as_deref() == Some(hook.hook_id.as_str());
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
                        repeats += 1;
                        tokio::time::sleep(offer_backoff(repeats)).await;
                    } else {
                        outstanding = Some(hook.hook_id.clone());
                        repeats = 0;
                    }
                }
                Err(_) => {
                    // A poll carries no native effect: back off and re-read.
                    // The event task is gone when the channel closes.
                    if tx.is_closed() {
                        return;
                    }
                    tokio::time::sleep(NEXT_RETRY_BEAT).await;
                }
            }
        }
    })
}

/// Bounded backoff for the Nth consecutive observation of the identical
/// outstanding hook offer (N >= 1): 250 ms doubling to a 2 s cap. Pure so
/// the pacing is pinned without a clock.
fn offer_backoff(repeats: u32) -> Duration {
    let shift = repeats.saturating_sub(1).min(3);
    OFFER_RETRY_FIRST
        .checked_mul(1 << shift)
        .unwrap_or(OFFER_RETRY_MAX)
        .min(OFFER_RETRY_MAX)
}

/// True when a `bind` output proves version-1 binding of exactly this
/// session. Only then may readiness advertise `turn_safe_point`.
pub(crate) fn bind_verified(
    output: &crate::native::types::EliotContextBindOutput,
    session_id: &str,
) -> bool {
    output.version == 1 && output.bound_session_id == session_id
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identical_offer_backoff_grows_to_a_bounded_cap() {
        assert_eq!(offer_backoff(1), Duration::from_millis(250));
        assert_eq!(offer_backoff(2), Duration::from_millis(500));
        assert_eq!(offer_backoff(3), Duration::from_secs(1));
        assert_eq!(offer_backoff(4), Duration::from_secs(2));
        assert_eq!(offer_backoff(100), Duration::from_secs(2));
    }
}
