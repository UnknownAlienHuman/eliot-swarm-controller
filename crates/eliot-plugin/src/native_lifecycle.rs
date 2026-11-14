//! Retire a per-generation plugin only after its owned native endpoint exits.

use std::{
    future::{Future, pending},
    time::Duration,
};

use eliot_core::{Error, Result};
use tokio::{
    io::AsyncWriteExt,
    sync::watch,
    time::{Instant, timeout_at},
};

use crate::{native_transport::NativeChild, session::EVENT_DEADLINE};

const PLUGIN_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(1);

/// Own the runtime for every stdio plugin entry. Tokio stdin starts an
/// uncancellable blocking read, so ordinary runtime Drop can wait forever even
/// after the adapter has drained its native tail and aborted its peer reader.
/// Flush acknowledged output before bounding runtime shutdown; process exit is
/// still proved separately by the daemon's owned child and reader.
pub fn run_plugin_entry<F: Future>(entry: F) -> Result<F::Output> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|_| Error::new("plugin_runtime", "plugin runtime could not be started"))?;
    let result = runtime.block_on(async {
        let result = entry.await;
        tokio::time::timeout(PLUGIN_SHUTDOWN_TIMEOUT, tokio::io::stdout().flush())
            .await
            .map_err(|_| Error::new("unknown", "plugin stdout flush timed out"))?
            .map_err(|_| Error::new("unknown", "plugin stdout flush failed"))?;
        Ok(result)
    });
    runtime.shutdown_timeout(PLUGIN_SHUTDOWN_TIMEOUT);
    result
}

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
    use super::{run_plugin_entry, wait_for_native_tail};
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

    #[test]
    fn plugin_entry_retires_with_stdin_open() {
        use std::{
            io::Read,
            process::{Command, Stdio},
            task::Poll,
            time::{Duration, Instant},
        };
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        const CHILD: &str = "ELIOT_STDIN_RETIREMENT_REGRESSION";
        if std::env::var_os(CHILD).is_some() {
            run_plugin_entry(async {
                let mut stdin = tokio::io::stdin();
                let mut byte = [0];
                let mut read = Box::pin(stdin.read(&mut byte));
                // Poll the real Tokio stdin once so its blocking read is armed,
                // then cancel the async owner as adapter retirement does.
                std::future::poll_fn(|cx| match read.as_mut().poll(cx) {
                    Poll::Pending => Poll::Ready(()),
                    Poll::Ready(_) => panic!("parent must keep plugin stdin open"),
                })
                .await;
                tokio::io::stdout()
                    .write_all(b"retirement_complete\n")
                    .await
                    .unwrap();
            })
            .unwrap();
            return;
        }

        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "native_lifecycle::tests::plugin_entry_retires_with_stdin_open",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let _open_stdin = child.stdin.take().unwrap();
        let deadline = Instant::now() + Duration::from_secs(8);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(20))
                }
                result => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("plugin entry did not retire with stdin open: {result:?}");
                }
            }
        };
        let mut stdout = String::new();
        child
            .stdout
            .take()
            .unwrap()
            .read_to_string(&mut stdout)
            .unwrap();
        assert!(status.success(), "{stdout}");
        assert!(stdout.contains("retirement_complete"), "{stdout}");
    }
}
