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
        loop {
            let deadline =
                Instant::now() + Duration::from_millis(CONTEXT_NEXT_WAIT_MS as u64 + 10_000);
            match http
                .eliot_context_next(&session_id, CONTEXT_NEXT_WAIT_MS, deadline)
                .await
            {
                Ok(output) => {
                    let Some(hook) = output.hook else {
                        continue;
                    };
                    if hook.session_id != session_id {
                        // A foreign offer never binds: drop it; the next
                        // poll re-reads the bound session's queue.
                        continue;
                    }
                    if tx
                        .send(crate::events::ContextHookMsg::Arrived(ContextHookSignal {
                            hook,
                        }))
                        .await
                        .is_err()
                    {
                        return;
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

/// True when a `bind` output proves version-1 binding of exactly this
/// session. Only then may readiness advertise `turn_safe_point`.
pub(crate) fn bind_verified(
    output: &crate::native::types::EliotContextBindOutput,
    session_id: &str,
) -> bool {
    output.version == 1 && output.bound_session_id == session_id
}
