//! Retire a per-generation plugin only after its owned native endpoint exits.

use std::future::{Future, pending};

use eliot_core::{Error, Result};
use tokio::{
    sync::watch,
    time::{Instant, timeout_at},
};

use crate::{native_transport::NativeChild, session::EVENT_DEADLINE};

/// Subscribe to the existing child waiter. A reader fault or an unconfirmed
/// wait error is not process-death evidence. Cancellation never kills the child.
pub async fn wait_for_native_exit(child: Option<&mut NativeChild>) {
    if let Some(child) = child
        && child.wait_for_process_exit().await.is_ok()
    {
        return;
    }
    pending().await
}

/// Wait until the event owner has processed the native queue through EOF,
/// including its commit acknowledgements. Closure without that barrier is loss.
pub async fn wait_for_native_tail<S>(
    status: &watch::Receiver<S>,
    drained: fn(&S) -> bool,
) -> Result<()> {
    let mut status = status.clone();
    loop {
        let finished = drained(&status.borrow_and_update());
        if finished {
            return Ok(());
        }
        if status.changed().await.is_err() {
            // A final retained value may arrive with sender closure.
            return if drained(&status.borrow()) {
                Ok(())
            } else {
                Err(Error::new(
                    "unknown",
                    "native event owner ended before tail settlement",
                ))
            };
        }
    }
}

/// Called only after the same session's confirmed exit. Give the existing pump
/// and event owner one bounded settlement window; never manufacture a terminal
/// turn or run the ordinary stop path. The daemon separately proves plugin
/// custody exit and drains its own reader before settling loss.
pub async fn drain_native_exit(
    child: &mut NativeChild,
    events: impl Future<Output = Result<()>>,
) -> Result<()> {
    if !child.process_exit_observed() {
        return Err(Error::new("unknown", "native process exit is unconfirmed"));
    }
    let deadline = Instant::now() + EVENT_DEADLINE;
    let output = child.await_output_drained(deadline).await;
    // Even a faulty reader can have queued a terminal frame before its fault.
    // Always attempt event settlement before reporting either failure.
    timeout_at(deadline, events)
        .await
        .map_err(|_| Error::new("unknown", "native event tail settlement timed out"))??;
    output.map_err(|error| error.with_code("unknown"))
}

#[cfg(test)]
mod tests {
    use super::wait_for_native_tail;
    use tokio::sync::watch;

    #[tokio::test]
    async fn owner_closure_without_committed_tail_is_unknown() {
        let (owner, status) = watch::channel(false);
        drop(owner);
        let error = wait_for_native_tail(&status, |committed| *committed)
            .await
            .unwrap_err();
        assert_eq!(error.code, "unknown");
    }

    #[tokio::test]
    async fn committed_tail_wins_over_owner_closure() {
        let (owner, status) = watch::channel(false);
        owner.send_replace(true);
        drop(owner);
        wait_for_native_tail(&status, |committed| *committed)
            .await
            .unwrap();
    }
}
